//go:build linux || darwin

package loop

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/DevNewbie1826/hoe/internal/poll"
	"github.com/DevNewbie1826/hoe/internal/sched"
)

// Group owns a set of loops, each with its own elastic worker shard.
type Group struct {
	loops     []*reactor
	handler   Handler
	next      atomic.Uint64
	tokens    atomic.Uint64
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// New starts a transport group.
func New(cfg Config) (*Group, error) {
	if cfg.Handler == nil || cfg.Loops < 0 {
		return nil, syscall.EINVAL
	}
	n := cfg.Loops
	if n == 0 {
		n = DefaultLoops()
	}
	g := &Group{handler: cfg.Handler}
	for range n {
		p, err := poll.New()
		if err != nil {
			for _, l := range g.loops {
				l.p.Close()
				l.pool.Close()
			}
			return nil, err
		}
		g.loops = append(g.loops, &reactor{g: g, p: p, pool: sched.New(cfg.IdleWorkerTimeout), done: make(chan struct{})})
	}
	for _, l := range g.loops {
		go l.run()
	}
	return g, nil
}

func (g *Group) owner() *reactor { return g.loops[(g.next.Add(1)-1)%uint64(len(g.loops))] }

// Listen transfers ownership of a nonblocking close-on-exec listener fd on
// success. The caller retains ownership if registration fails.
func (g *Group) Listen(fd int) error {
	if g.closed.Load() {
		return net.ErrClosed
	}
	return g.owner().call(&command{kind: listenCommand, fd: fd})
}

// Adopt takes ownership of an accepted nonblocking close-on-exec socket.
// It closes the descriptor even if registration fails.
func (g *Group) Adopt(fd int) error {
	c := &Conn{loop: g.owner(), fd: fd, token: g.tokens.Add(1), done: make(chan struct{})}
	cmd := &command{kind: adoptCommand, conn: c, reply: make(chan error, 1)}
	if err := c.loop.enqueue(cmd); err != nil {
		return errors.Join(err, syscall.Close(fd))
	}
	return <-cmd.reply
}

// Close closes owned descriptors and joins loops and workers. It must not
// be called from a Handler callback. Conn.Done closes before OnClose is queued,
// so blocking adapters can release their workers during shutdown.
func (g *Group) Close() error {
	g.closeOnce.Do(func() {
		g.closed.Store(true)
		for _, l := range g.loops {
			if err := l.call(&command{kind: stopCommand}); err != nil && !errors.Is(err, net.ErrClosed) {
				g.closeErr = errors.Join(g.closeErr, err)
			}
		}
		for _, l := range g.loops {
			<-l.done
			g.closeErr = errors.Join(g.closeErr, l.err)
		}
		for _, l := range g.loops {
			l.pool.Close()
		}
	})
	return g.closeErr
}

type entry struct {
	token uint64
	conn  *Conn // nil identifies a listener
}

type commandKind uint8

const (
	listenCommand commandKind = iota
	stopListenersCommand
	adoptCommand
	interestCommand
	closeCommand
	detachCommand
	stopCommand
)

// CloseListeners stops accepting without closing active connections.
// It is safe to call repeatedly, but not from a direct readiness callback.
func (g *Group) CloseListeners() error {
	var err error
	for _, l := range g.loops {
		err = errors.Join(err, l.call(&command{kind: stopListenersCommand}))
	}
	return err
}

type command struct {
	kind        commandKind
	fd          int
	conn        *Conn
	read, write bool
	reply       chan error
}

type reactor struct {
	g        *Group
	p        *poll.Poller
	pool     *sched.Pool
	table    []*entry
	mu       sync.Mutex
	commands []*command
	stopped  bool
	done     chan struct{}
	err      error
	tasks    []sched.Task
}

func (l *reactor) enqueue(cmd *command) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return net.ErrClosed
	}
	wake := len(l.commands) == 0
	l.commands = append(l.commands, cmd)
	if wake {
		if err := l.p.Wake(); err != nil {
			l.commands = l.commands[:len(l.commands)-1]
			return err
		}
	}
	return nil
}

func (l *reactor) call(cmd *command) error {
	cmd.reply = make(chan error, 1)
	if err := l.enqueue(cmd); err != nil {
		return err
	}
	return <-cmd.reply
}

func (l *reactor) at(fd int) *entry {
	if fd < 0 || fd >= len(l.table) {
		return nil
	}
	return l.table[fd]
}
func (l *reactor) put(fd int, e *entry) {
	if fd >= len(l.table) {
		table := make([]*entry, max(fd+1, len(l.table)*2, 128))
		copy(table, l.table)
		l.table = table
	}
	l.table[fd] = e
}

func (l *reactor) callback(c *Conn, fn func()) {
	l.tasks = append(l.tasks, sched.Task{Queue: &c.queue, Run: fn})
}
func (l *reactor) dispatch() {
	if len(l.tasks) != 0 {
		l.pool.Dispatch(l.tasks)
		clear(l.tasks)
		l.tasks = l.tasks[:0]
	}
}
