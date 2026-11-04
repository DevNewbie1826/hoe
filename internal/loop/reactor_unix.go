//go:build linux || darwin

package loop

import (
	"errors"
	"net"
	"slices"
	"syscall"

	"github.com/DevNewbie1826/hoe/internal/poll"
	"github.com/DevNewbie1826/hoe/internal/sys"
)

func (l *reactor) run() {
	defer close(l.done)
	for {
		events, err := l.p.Wait()
		if err != nil {
			l.finish(err)
			return
		}
		// Resolve all identities before commands/accepts can reuse descriptors.
		listeners := make([]int, 0)
		for _, e := range events {
			owner := l.at(e.Token)
			if owner == nil {
				if e.Read && l.listenerAt(e.FD, e.Token) {
					listeners = append(listeners, e.FD)
				}
				continue
			}
			c := owner.conn
			if h, ok := l.g.handler.(interface{ OnReadiness(*Conn, poll.Event) }); ok {
				// This optional nonblocking hook cannot issue loop commands.
				// Cancellation must not wait behind a blocked handler callback.
				h.OnReadiness(c, e)
			}
			if c.noteEvent(e) {
				l.callback(c, c.eventTask)
			}
		}
		if l.drain() {
			l.finish(nil)
			return
		}
		for _, fd := range listeners {
			if l.hasListener(fd) {
				l.accept(fd)
			}
		}
		l.dispatch()
	}
}

func (l *reactor) drain() bool {
	l.mu.Lock()
	commands := l.commands
	l.commands = nil
	l.mu.Unlock()
	stop := false
	for _, cmd := range commands {
		var err error
		if stop {
			err = net.ErrClosed
		} else {
			switch cmd.kind {
			case stopListenersCommand:
				err = errors.Join(err, l.closeListeners())
			case listenCommand:
				if l.g.closed.Load() {
					err = net.ErrClosed
					break
				}
				err = l.registerListener(cmd.fd)
			case adoptCommand:
				c := cmd.conn
				if l.g.closed.Load() {
					err = net.ErrClosed
				} else {
					// The compact slot, not the descriptor, identifies this
					// registration; republish its token before the poller sees
					// it. An exhausted token space fails before a slot is
					// taken, so the failure path below balances exactly as
					// for a rejected Add.
					var token uint64
					token, err = l.registerConn(c)
					if err == nil {
						err = l.p.Add(c.fd, token, true, false)
					}
				}
				if err != nil {
					c.mu.Lock()
					c.closed = true
					close(c.done)
					syscall.Close(c.fd)
					c.mu.Unlock()
					if c.token != 0 {
						l.freeSlot(c.token)
					}
				} else {
					l.callback(c, func() { l.g.handler.OnOpen(c) })
				}
			case interestCommand:
				c := cmd.conn
				if e := l.at(c.token); e == nil || e.conn != c {
					err = net.ErrClosed
				} else {
					err = l.p.Modify(c.fd, c.token, cmd.read, cmd.write)
				}
			case closeCommand, detachCommand:
				c := cmd.conn
				if e := l.at(c.token); e != nil && e.conn == c {
					err = l.remove(c, cmd.kind == closeCommand, nil)
				} else if cmd.kind == detachCommand {
					err = net.ErrClosed
				}
			case stopCommand:
				stop = true
			}
		}
		if cmd.kind == adoptCommand && stop {
			c := cmd.conn
			c.mu.Lock()
			c.closed = true
			close(c.done)
			syscall.Close(c.fd)
			c.mu.Unlock()
		}
		if cmd.reply != nil {
			cmd.reply <- err
		}
		if cmd.complete != nil {
			cmd.complete(err)
		}
	}
	return stop
}

func (l *reactor) accept(fd int) {
	for {
		n, _, err := sys.Accept(fd)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return
		} // EAGAIN ends this edge; listener errors are retried on its next edge.
		if err := syscall.SetsockoptInt(n, syscall.IPPROTO_TCP, syscall.TCP_NODELAY, 1); err != nil {
			syscall.Close(n)
			continue
		}
		target := l.g.owner(n)
		c := &Conn{loop: target, fd: n, done: make(chan struct{})}
		if err := target.enqueue(&command{kind: adoptCommand, conn: c}); err != nil {
			syscall.Close(n)
		}
	}
}

func (l *reactor) remove(c *Conn, closeFD bool, cause error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	err := l.p.Delete(c.fd)
	if err != nil && !closeFD {
		return err
	}
	l.freeSlot(c.token)
	c.closed = true
	close(c.done)
	if closeFD {
		err = errors.Join(err, syscall.Close(c.fd))
		closedCause := errors.Join(cause, err)
		l.callback(c, func() { l.g.handler.OnClose(c, closedCause) })
	}
	return err
}

func (l *reactor) finish(cause error) {
	l.mu.Lock()
	l.stopped = true
	remaining := l.commands
	l.commands = nil
	l.mu.Unlock()
	for _, cmd := range remaining {
		if cmd.kind == adoptCommand {
			syscall.Close(cmd.conn.fd)
			close(cmd.conn.done)
		}
		if cmd.reply != nil {
			cmd.reply <- net.ErrClosed
		}
		if cmd.complete != nil {
			cmd.complete(net.ErrClosed)
		}
	}
	// Close in descriptor order, as the fd-indexed sweep did, so shutdown
	// callback order stays independent of registration-slot reuse.
	type owned struct {
		fd   int
		conn *Conn
	}
	ownedFDs := make([]owned, 0, len(l.listeners)+len(l.slots))
	for _, e := range l.listeners {
		ownedFDs = append(ownedFDs, owned{fd: e.fd})
	}
	for _, e := range l.slots {
		if e != nil {
			ownedFDs = append(ownedFDs, owned{fd: e.conn.fd, conn: e.conn})
		}
	}
	slices.SortFunc(ownedFDs, func(a, b owned) int { return a.fd - b.fd })
	for _, o := range ownedFDs {
		if o.conn == nil {
			cause = errors.Join(cause, l.p.Delete(o.fd), syscall.Close(o.fd))
		} else {
			cause = errors.Join(cause, l.remove(o.conn, true, cause))
		}
	}
	l.listeners = nil
	l.dispatch()
	l.err = errors.Join(cause, l.p.Close())
}
