//go:build linux || darwin

package loop

import "github.com/DevNewbie1826/hoe/internal/poll"

// ReadReady subscribes before Read to the next read-readiness edge.
// After EAGAIN, wait on this channel, Done, or the adapter's deadline.
// Readiness is published by the loop even while this connection's callback
// is blocked, so streaming reads never depend on another queued callback.
func (c *Conn) ReadReady() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.done
	}
	if c.readReady == nil {
		c.readReady = make(chan struct{})
	}
	return c.readReady
}

// WriteReady subscribes before Write to the next write-readiness edge.
// After EAGAIN, enable write interest and wait on this channel, Done, or the
// adapter's deadline. Disable write interest again after draining output.
func (c *Conn) WriteReady() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return c.done
	}
	if c.writeReady == nil {
		c.writeReady = make(chan struct{})
	}
	return c.writeReady
}

func (c *Conn) noteEvent(e poll.Event) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if (e.Read || e.EOF) && c.readReady != nil {
		close(c.readReady)
		c.readReady = nil
	}
	if (e.Write || e.EOF) && c.writeReady != nil {
		close(c.writeReady)
		c.writeReady = nil
	}
	c.pending.FD, c.pending.Token = c.fd, c.token
	c.pending.Read = c.pending.Read || e.Read
	c.pending.Write = c.pending.Write || e.Write
	c.pending.EOF = c.pending.EOF || e.EOF
	if c.eventScheduled {
		return false
	}
	c.eventScheduled = true
	if c.eventTask == nil {
		c.eventTask = c.runEvents
	}
	return true
}

func (c *Conn) runEvents() {
	for {
		c.mu.Lock()
		if c.closed {
			c.pending = poll.Event{}
			c.eventScheduled = false
			c.mu.Unlock()
			return
		}
		e := c.pending
		c.pending = poll.Event{}
		c.mu.Unlock()
		c.loop.g.handler.OnEvent(c, e)
		c.mu.Lock()
		if !c.pending.Read && !c.pending.Write && !c.pending.EOF {
			c.eventScheduled = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}
