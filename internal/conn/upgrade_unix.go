//go:build linux || darwin

package conn

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/DevNewbie1826/hoe/internal/http1"
	"github.com/DevNewbie1826/hoe/internal/poll"
	"github.com/DevNewbie1826/hoe/internal/sched"
)

// UpgradeGroup shares a reactor across upgraded connections. Its zero value
// is ready to use; the last connection releases the reactor and its workers.
type UpgradeGroup struct {
	mu     sync.Mutex
	active *upgradeReactor
}

type upgradeReactor struct {
	group    *UpgradeGroup
	poller   *poll.Poller
	pool     *sched.Pool
	mu       sync.Mutex
	commands []*upgradeCommand
	stopping bool
	done     chan struct{}
	conns    map[int]*UpgradeConn
	next     uint64
	users    int // guarded by group.mu, includes pending registrations
}

type upgradeCommand struct {
	run   func() error
	reply chan error
}

// IsUpgradeWriter recognizes the hoe HTTP transport through standard wrappers.
func IsUpgradeWriter(w http.ResponseWriter) bool {
	for {
		if _, ok := w.(*http1.Response); ok {
			return true
		}
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return false
		}
		w = u.Unwrap()
	}
}

// Adopt moves a hijacked runtime TCP socket to the upgrade reactor. Ownership of
// nc remains with the caller on error; on success it is closed after duplication.
func (g *UpgradeGroup) Adopt(nc net.Conn) (*UpgradeConn, error) {
	sc, ok := nc.(syscall.Conn)
	if !ok {
		return nil, http.ErrNotSupported
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return nil, err
	}
	fd := -1
	var dupErr error
	if err = raw.Control(func(n uintptr) {
		syscall.ForkLock.RLock()
		defer syscall.ForkLock.RUnlock()
		fd, dupErr = syscall.Dup(int(n))
		if dupErr == nil {
			syscall.CloseOnExec(fd)
		}
	}); err != nil {
		return nil, err
	}
	if dupErr != nil {
		return nil, dupErr
	}
	if err = syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		return nil, err
	}
	g.mu.Lock()
	r := g.active
	if r == nil {
		p, e := poll.New()
		if e != nil {
			g.mu.Unlock()
			syscall.Close(fd)
			return nil, e
		}
		r = &upgradeReactor{group: g, poller: p, pool: sched.New(time.Millisecond),
			done: make(chan struct{}), conns: make(map[int]*UpgradeConn)}
		g.active = r
		go r.run()
	}
	r.users++
	g.mu.Unlock()
	c := &UpgradeConn{reactor: r, fd: fd, done: make(chan struct{}), changed: make(chan struct{})}
	err = r.call(func() error {
		r.next++
		c.token = r.next
		if e := r.poller.Add(fd, c.token, true, false); e != nil {
			return e
		}
		r.conns[fd] = c
		return nil
	})
	if err != nil {
		syscall.Close(fd)
		r.release()
		return nil, err
	}
	if err = nc.Close(); err != nil {
		c.Close()
		return nil, err
	}
	return c, nil
}

func (r *upgradeReactor) enqueue(cmd *upgradeCommand) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping {
		return net.ErrClosed
	}
	r.commands = append(r.commands, cmd)
	return r.poller.Wake()
}

func (r *upgradeReactor) call(f func() error) error {
	cmd := &upgradeCommand{run: f, reply: make(chan error, 1)}
	if err := r.enqueue(cmd); err != nil {
		return err
	}
	select {
	case err := <-cmd.reply:
		return err
	case <-r.done:
		return net.ErrClosed
	}
}

func (r *upgradeReactor) release() {
	r.group.mu.Lock()
	r.users--
	last := r.users == 0
	if last {
		r.group.active = nil
	}
	r.group.mu.Unlock()
	if last {
		r.enqueue(&upgradeCommand{run: func() error {
			r.mu.Lock()
			r.stopping = true
			r.mu.Unlock()
			return nil
		}})
	}
}

func (r *upgradeReactor) run() {
	defer close(r.done)
	defer r.pool.Close()
	defer r.poller.Close()
	for {
		events, err := r.poller.Wait()
		if err != nil {
			for _, c := range r.conns {
				r.remove(c, err)
			}
			return
		}
		// Resolve tokens before commands can close and reuse descriptor numbers.
		for _, ev := range events {
			c := r.conns[ev.FD]
			if c == nil || c.token != ev.Token {
				continue
			}
			c.mu.Lock()
			if (ev.Write || ev.EOF) && c.writable != nil {
				close(c.writable)
				c.writable = nil
			}
			active := c.receive != nil && !c.closed
			c.mu.Unlock()
			if active && (ev.Read || ev.EOF) {
				c.schedule()
			}
		}
		r.mu.Lock()
		cmds := r.commands
		r.commands = nil
		r.mu.Unlock()
		for _, cmd := range cmds {
			e := cmd.run()
			if cmd.reply != nil {
				cmd.reply <- e
			}
		}
		r.mu.Lock()
		stop := r.stopping
		r.mu.Unlock()
		if stop {
			return
		}
	}
}

func (r *upgradeReactor) remove(c *UpgradeConn, cause error) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	err := errors.Join(r.poller.Delete(c.fd), syscall.Close(c.fd))
	close(c.done)
	close(c.changed)
	onClose := c.onClose
	c.mu.Unlock()
	delete(r.conns, c.fd)
	c.queue.Submit(r.pool, func() {
		if onClose != nil {
			onClose(errors.Join(cause, err))
		}
	})
	r.release()
	return err
}
