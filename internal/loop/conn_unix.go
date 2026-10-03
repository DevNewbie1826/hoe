//go:build linux || darwin

package loop

import (
	"net"
	"sync"
	"syscall"

	"github.com/DevNewbie1826/hoe/internal/poll"
	"github.com/DevNewbie1826/hoe/internal/sched"
	"github.com/DevNewbie1826/hoe/internal/sys"
)

// Conn identifies one lifetime of an accepted descriptor. It retains no I/O
// buffer while idle. Its I/O lock prevents close or detach during a syscall.
type Conn struct {
	loop                  *reactor
	fd                    int
	token                 uint64
	mu                    sync.Mutex
	closed                bool
	done                  chan struct{}
	queue                 sched.Queue
	readReady, writeReady chan struct{}
	pending               poll.Event
	eventScheduled        bool
	eventTask             func()
}

// Done closes as soon as the loop closes or detaches this descriptor, without
// waiting for a blocked callback to release its connection queue.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Read receives nonblocking socket bytes; adapters handle EAGAIN internally.
func (c *Conn) Read(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return sys.Read(c.fd, b)
}

// Write sends nonblocking socket bytes; a short count remains caller-owned.
func (c *Conn) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return sys.Write(c.fd, b)
}

// Writev sends a vector while excluding descriptor closure.
func (c *Conn) Writev(b [][]byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return sys.Writev(c.fd, b)
}

// Sendfile transfers file bytes while excluding descriptor closure.
func (c *Conn) Sendfile(in int, offset *int64, count int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return sys.Sendfile(c.fd, in, offset, count)
}

// Splice transfers pipe bytes to this socket on Linux.
func (c *Conn) Splice(in, count int) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, net.ErrClosed
	}
	return sys.Splice(c.fd, in, count)
}

// SetInterest synchronously changes readiness interests through the owner.
func (c *Conn) SetInterest(read, write bool) error {
	return c.loop.call(&command{kind: interestCommand, conn: c, read: read, write: write})
}

// Close closes this connection through its owner loop and is idempotent.
func (c *Conn) Close() error {
	select {
	case <-c.done:
		return nil
	default:
	}
	err := c.loop.call(&command{kind: closeCommand, conn: c})
	if err == net.ErrClosed {
		return nil
	}
	return err
}

// Detach removes the socket from the poller without closing it, transferring
// ownership of the raw fd to the caller for a runtime net.Conn handoff.
func (c *Conn) Detach() (int, error) {
	if err := c.loop.call(&command{kind: detachCommand, conn: c}); err != nil {
		return -1, err
	}
	return c.fd, nil
}

// CloseWrite sends a FIN while allowing a bounded read drain before closure.
func (c *Conn) CloseWrite() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	return syscall.Shutdown(c.fd, syscall.SHUT_WR)
}

// RelinquishWorker releases scheduler ownership after Hijack or forced closure.
// User code may continue blocking after its descriptor leaves the reactor.
func (c *Conn) RelinquishWorker() { c.queue.Detach() }

// LocalAddr returns a snapshot of the socket's local address.
func (c *Conn) LocalAddr() (syscall.Sockaddr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, net.ErrClosed
	}
	return syscall.Getsockname(c.fd)
}

// RemoteAddr returns a snapshot of the socket's peer address.
func (c *Conn) RemoteAddr() (syscall.Sockaddr, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, net.ErrClosed
	}
	return syscall.Getpeername(c.fd)
}
