//go:build linux || darwin

package conn

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sched"
	"github.com/DevNewbie1826/hoe/internal/sys"
)

// UpgradeConn provides serialized callbacks and blocking, bounded writes over
// a reactor-owned socket. No goroutine waits for input on an idle connection.
type UpgradeConn struct {
	reactor   *upgradeReactor
	fd        int
	token     uint64
	mu        sync.Mutex
	writeMu   sync.Mutex
	closed    bool
	scheduled bool
	done      chan struct{}
	changed   chan struct{}
	writable  chan struct{}
	deadline  time.Time
	queue     sched.Queue
	receive   func([]byte) error
	onClose   func(error)
	pending   []byte
}

// Start installs callbacks and schedules already-buffered and socket bytes.
// Call once, after the protocol's open callback has completed.
func (c *UpgradeConn) Start(pending []byte, receive func([]byte) error, onClose func(error)) {
	c.mu.Lock()
	c.pending, c.receive, c.onClose = pending, receive, onClose
	closed := c.closed
	c.mu.Unlock()
	if closed {
		onClose(net.ErrClosed)
		return
	}
	c.schedule()
}

func (c *UpgradeConn) schedule() {
	c.mu.Lock()
	if c.scheduled || c.closed {
		c.mu.Unlock()
		return
	}
	c.scheduled = true
	c.mu.Unlock()
	c.queue.Submit(c.reactor.pool, c.read)
}

func (c *UpgradeConn) read() {
	if len(c.pending) > 0 {
		p := c.pending
		c.pending = nil
		if err := c.receive(p); err != nil {
			c.fail(err)
			return
		}
	}
	var buf [16 << 10]byte
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return
		}
		n, err := sys.Read(c.fd, buf[:])
		// Clearing under the syscall lock prevents losing a concurrent edge.
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			c.scheduled = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if n > 0 {
			if e := c.receive(buf[:n]); e != nil {
				c.fail(e)
				return
			}
		}
		if err != nil || n == 0 {
			if err == nil {
				err = io.EOF
			}
			c.fail(err)
			return
		}
	}
}

func (c *UpgradeConn) fail(err error) {
	c.reactor.call(func() error { return c.reactor.remove(c, err) })
}

// Close releases the descriptor through its owning reactor.
func (c *UpgradeConn) Close() error {
	select {
	case <-c.done:
		return nil
	default:
	}
	return c.reactor.call(func() error { return c.reactor.remove(c, nil) })
}

// SetWriteDeadline wakes a blocked writer when its deadline changes.
func (c *UpgradeConn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	c.deadline = t
	close(c.changed)
	c.changed = make(chan struct{})
	return nil
}

// Write blocks on socket backpressure without accumulating an output queue.
func (c *UpgradeConn) Write(p []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	total := 0
	interest := false
	defer func() {
		if interest {
			c.reactor.call(func() error { return c.reactor.poller.Modify(c.fd, c.token, true, false) })
		}
	}()
	for len(p) > 0 {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return total, net.ErrClosed
		}
		d, change := c.deadline, c.changed
		if !d.IsZero() && !time.Now().Before(d) {
			c.mu.Unlock()
			return total, os.ErrDeadlineExceeded
		}
		if c.writable == nil {
			c.writable = make(chan struct{})
		}
		ready := c.writable
		n, err := sys.Write(c.fd, p)
		c.mu.Unlock()
		total += n
		p = p[n:]
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
			if !interest {
				if e := c.reactor.call(func() error { return c.reactor.poller.Modify(c.fd, c.token, true, true) }); e != nil {
					return total, e
				}
				interest = true
			}
			var timer *time.Timer
			var timeout <-chan time.Time
			if !d.IsZero() {
				timer = time.NewTimer(time.Until(d))
				timeout = timer.C
			}
			select {
			case <-ready:
			case <-change:
			case <-c.done:
				err = net.ErrClosed
			case <-timeout:
				err = os.ErrDeadlineExceeded
			}
			if timer != nil {
				timer.Stop()
			}
			if err != nil && !errors.Is(err, syscall.EAGAIN) && !errors.Is(err, syscall.EWOULDBLOCK) {
				return total, err
			}
			continue
		}
		if err != nil {
			return total, err
		}
		if n == 0 {
			return total, io.ErrNoProgress
		}
	}
	return total, nil
}
