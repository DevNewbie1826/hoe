//go:build linux || darwin

package loop

import (
	"net"

	"github.com/DevNewbie1826/hoe/internal/sys"
)

// EnableCork holds partial TCP packets in the kernel, not in a user-space buffer.
// The caller owns the hold deadline and must explicitly push at release points.
func (c *Conn) EnableCork() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	return sys.EnableCork(c.fd)
}

// PushCork forces queued bytes out, restoring the cork only when keep is true.
// It excludes writes, close and detach for the entire push/re-cork sequence.
// A failed push leaves the caller responsible for ending the connection.
func (c *Conn) PushCork(keep bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return net.ErrClosed
	}
	return sys.PushCork(c.fd, keep)
}

// CorkEnabled reads the kernel flag rather than a cached state, for test probes.
func (c *Conn) CorkEnabled() (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false, net.ErrClosed
	}
	return sys.CorkEnabled(c.fd)
}
