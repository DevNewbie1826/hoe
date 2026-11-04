//go:build linux || darwin

package loop

import (
	"errors"
	"net"
	"slices"
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
	c := &Conn{loop: g.owner(), fd: fd, done: make(chan struct{})}
	cmd := &command{kind: adoptCommand, conn: c, reply: make(chan error, 1)}
	if err := c.loop.enqueue(cmd); err != nil {
		return errors.Join(err, syscall.Close(fd))
	}
	return <-cmd.reply
}

// AdoptAsync transfers ownership and reports registration completion without
// making the caller wait for the destination reactor.
func (g *Group) AdoptAsync(fd int, complete func(error)) error {
	c := &Conn{loop: g.owner(), fd: fd, done: make(chan struct{})}
	if err := c.loop.enqueue(&command{kind: adoptCommand, conn: c, complete: complete}); err != nil {
		return errors.Join(err, syscall.Close(fd))
	}
	return nil
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
	conn  *Conn
}

// listener records one listening descriptor. Listeners stay outside the
// connection slots because their count is bounded by the caller, not by the
// descriptor values the process happens to reach.
type listener struct {
	fd    int
	token uint64
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
	complete    func(error)
}

// Connection registrations are addressed by a compact slot, never by
// descriptor value: descriptors are process-global and spread across reactors,
// so an fd-indexed table would hold one dense array per reactor sized by the
// highest descriptor seen. The token carried by poll events encodes the slot in
// its low bits and a registration sequence above it, so an event resolves in
// O(1) and a stale token never matches a recycled slot.
const (
	slotBits = 24
	slotMask = 1<<slotBits - 1
	// The slot index shifts left one bit inside the slot field, so tokens
	// address at most maxSlots connections, and the sequence stops at
	// maxSeq so it never wraps onto an earlier identity.
	maxSlots = 1 << (slotBits - 1)
	maxSeq   = 1<<(64-slotBits) - 1
)

// Registration fails cleanly when the token space is exhausted instead of
// publishing a token that decodes to another live registration.
var (
	errSlotsExhausted = errors.New("loop: connection slot table exhausted")
	errSeqExhausted   = errors.New("loop: registration sequence exhausted")
)

type reactor struct {
	g         *Group
	p         *poll.Poller
	pool      *sched.Pool
	slots     []*entry
	free      []int
	seq       uint64
	listeners []listener
	mu        sync.Mutex
	commands  []*command
	stopped   bool
	done      chan struct{}
	err       error
	tasks     []sched.Task
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

// mint returns the next registration token with low in its low bits. The
// caller must have verified the limits first; mint itself never wraps.
func (l *reactor) mint(low uint64) uint64 {
	l.seq++
	return l.seq<<slotBits | low
}

// register reserves a slot for e, mints its token, and publishes the entry.
// Exhaustion fails before any state changes: a failed registration consumes
// no slot and never publishes a token that decodes to another entry.
func (l *reactor) register(e *entry) (uint64, error) {
	if l.seq >= maxSeq {
		return 0, errSeqExhausted
	}
	slot := len(l.slots)
	if n := len(l.free); n != 0 {
		slot, l.free = l.free[n-1], l.free[:n-1]
	} else {
		if slot >= maxSlots {
			return 0, errSlotsExhausted
		}
		l.slots = append(l.slots, nil)
	}
	e.token = l.mint(uint64(slot) << 1)
	l.slots[slot] = e
	return e.token, nil
}

// registerConn registers a connection and republishes the token on it, so
// interest and removal commands carry the identity the poller reports. On
// exhaustion the connection keeps its zero token and owns no slot.
func (l *reactor) registerConn(c *Conn) (uint64, error) {
	token, err := l.register(&entry{conn: c})
	if err != nil {
		return 0, err
	}
	c.token = token
	return token, nil
}

// at resolves a poll token to its live registration.
func (l *reactor) at(token uint64) *entry {
	slot := int(token&slotMask) >> 1
	if slot >= len(l.slots) {
		return nil
	}
	if e := l.slots[slot]; e != nil && e.token == token {
		return e
	}
	return nil
}

// freeSlot releases the registration addressed by token.
func (l *reactor) freeSlot(token uint64) {
	slot := int(token&slotMask) >> 1
	if slot < len(l.slots) && l.slots[slot] != nil && l.slots[slot].token == token {
		l.slots[slot] = nil
		l.free = append(l.free, slot)
	}
}

func (l *reactor) listenerAt(fd int, token uint64) bool {
	for _, e := range l.listeners {
		if e.fd == fd && e.token == token {
			return true
		}
	}
	return false
}

func (l *reactor) hasListener(fd int) bool {
	for _, e := range l.listeners {
		if e.fd == fd {
			return true
		}
	}
	return false
}

// registerListener registers fd for read readiness and records it.
func (l *reactor) registerListener(fd int) error {
	if l.seq >= maxSeq {
		return errSeqExhausted
	}
	token := l.mint(0)
	if err := l.p.Add(fd, token, true, false); err != nil {
		return err
	}
	l.listeners = append(l.listeners, listener{fd: fd, token: token})
	return nil
}

// closeListeners unregisters every listener in descriptor order.
func (l *reactor) closeListeners() error {
	ls := slices.Clone(l.listeners)
	slices.SortFunc(ls, func(a, b listener) int { return a.fd - b.fd })
	var err error
	for _, e := range ls {
		err = errors.Join(err, l.p.Delete(e.fd), syscall.Close(e.fd))
	}
	l.listeners = nil
	return err
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
