//go:build linux || darwin

package loop

import (
	"errors"
	"net"
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
			owner := l.at(e.FD)
			if owner == nil || owner.token != e.Token {
				continue
			}
			if owner.conn == nil {
				if e.Read {
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
			if e := l.at(fd); e != nil && e.conn == nil {
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
				for fd, e := range l.table {
					if e != nil && e.conn == nil {
						err = errors.Join(err, l.p.Delete(fd), syscall.Close(fd))
						l.table[fd] = nil
					}
				}
			case listenCommand:
				if l.g.closed.Load() {
					err = net.ErrClosed
					break
				}
				token := l.g.tokens.Add(1)
				err = l.p.Add(cmd.fd, token, true, false)
				if err == nil {
					l.put(cmd.fd, &entry{token: token})
				}
			case adoptCommand:
				c := cmd.conn
				if l.g.closed.Load() {
					err = net.ErrClosed
				} else {
					err = l.p.Add(c.fd, c.token, true, false)
				}
				if err != nil {
					c.mu.Lock()
					c.closed = true
					close(c.done)
					syscall.Close(c.fd)
					c.mu.Unlock()
				} else {
					l.put(c.fd, &entry{token: c.token, conn: c})
					l.callback(c, func() { l.g.handler.OnOpen(c) })
				}
			case interestCommand:
				c := cmd.conn
				if e := l.at(c.fd); e == nil || e.conn != c {
					err = net.ErrClosed
				} else {
					err = l.p.Modify(c.fd, c.token, cmd.read, cmd.write)
				}
			case closeCommand, detachCommand:
				c := cmd.conn
				if e := l.at(c.fd); e != nil && e.conn == c {
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
		target := l.g.owner()
		c := &Conn{loop: target, fd: n, token: l.g.tokens.Add(1), done: make(chan struct{})}
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
	l.table[c.fd] = nil
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
	for fd, e := range l.table {
		if e == nil {
			continue
		}
		if e.conn == nil {
			cause = errors.Join(cause, l.p.Delete(fd), syscall.Close(fd))
			l.table[fd] = nil
		} else {
			cause = errors.Join(cause, l.remove(e.conn, true, cause))
		}
	}
	l.dispatch()
	l.err = errors.Join(cause, l.p.Close())
}
