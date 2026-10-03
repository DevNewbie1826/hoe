//go:build linux || darwin

package conn

import (
	"sync"
	"time"
)

const deadlineQuantum = 5 * time.Millisecond
const deadlineSlots = 512

// One wheel services every connection; it sleeps when no deadlines are armed.
// Socket operations still check the exact timestamp before each syscall.
type deadlineWheel struct {
	mu    sync.Mutex
	slots [deadlineSlots]map[*Conn]struct{}
	count int
	wake  chan struct{}
	stop  chan struct{}
	done  chan struct{}
}

func (w *deadlineWheel) init() {
	w.wake = make(chan struct{}, 1)
	w.stop = make(chan struct{})
	w.done = make(chan struct{})
}
func (w *deadlineWheel) close() { close(w.stop); <-w.done }

// update is called with c.mu held; the wheel never takes c.mu while w.mu is held.
func (w *deadlineWheel) update(c *Conn, t time.Time) {
	w.mu.Lock()
	if c.timerSlot >= 0 {
		delete(w.slots[c.timerSlot], c)
		w.count--
		c.timerSlot = -1
		c.timerArmed.Store(false)
	}
	if !t.IsZero() && time.Now().Before(t) {
		tick := (t.UnixNano() + int64(deadlineQuantum) - 1) / int64(deadlineQuantum)
		slot := int(tick % deadlineSlots)
		if w.slots[slot] == nil {
			w.slots[slot] = make(map[*Conn]struct{})
		}
		w.slots[slot][c] = struct{}{}
		c.timerSlot, c.timerTick = slot, tick
		c.timerArmed.Store(true)
		w.count++
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (c *Conn) updateDeadlinesLocked() {
	if c.changed != nil {
		close(c.changed)
		c.changed = nil
	}
	if c.readDeadline.IsZero() && c.writeDeadline.IsZero() && !c.timerArmed.Load() {
		return
	}
	t := c.readDeadline
	now := time.Now()
	if !t.IsZero() && !now.Before(t) {
		t = time.Time{}
	}
	if d := c.writeDeadline; !d.IsZero() && now.Before(d) && (t.IsZero() || d.Before(t)) {
		t = d
	}
	c.engine.timers.update(c, t)
}

// SetDeadline changes the blocking read and write deadlines.
func (c *Conn) SetDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readDeadline.Equal(t) && c.writeDeadline.Equal(t) {
		return nil
	}
	c.readDeadline, c.writeDeadline = t, t
	c.updateDeadlinesLocked()
	return nil
}

// SetReadDeadline changes current and future reads, including blocked body reads.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readDeadline.Equal(t) {
		return nil
	}
	c.readDeadline = t
	c.updateDeadlinesLocked()
	return nil
}

// SetWriteDeadline changes current and future writes, including backpressure waits.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeDeadline.Equal(t) {
		return nil
	}
	c.writeDeadline = t
	c.updateDeadlinesLocked()
	return nil
}
func (c *Conn) stopDeadlines() { c.SetDeadline(time.Time{}) }

func after(d time.Duration) time.Time {
	if d <= 0 {
		return time.Time{}
	}
	return time.Now().Add(d)
}
func (c *Conn) startHead(idle bool) {
	d := c.engine.server.ReadHeaderTimeout
	if d == 0 {
		d = c.engine.server.ReadTimeout
	}
	if idle {
		d = c.engine.server.IdleTimeout
		if d == 0 {
			d = c.engine.server.ReadTimeout
		}
	}
	c.mu.Lock()
	c.serving = false
	if !idle && (c.engine.server.ReadTimeout > 0 || c.engine.server.ReadHeaderTimeout > 0) {
		c.headStart = time.Now()
	}
	c.readDeadline = after(d)
	c.writeDeadline = time.Time{}
	c.updateDeadlinesLocked()
	c.mu.Unlock()
}

func (c *Conn) expire() {
	c.mu.Lock()
	closeHead := !c.serving && !c.readDeadline.IsZero() && !time.Now().Before(c.readDeadline)
	c.updateDeadlinesLocked()
	c.mu.Unlock()
	if closeHead {
		c.Close()
	}
}

func (w *deadlineWheel) run() {
	defer close(w.done)
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	last := time.Now().UnixNano() / int64(deadlineQuantum)
	for {
		w.mu.Lock()
		armed := w.count > 0
		w.mu.Unlock()
		var tick <-chan time.Time
		if armed {
			now := time.Now()
			next := (now.UnixNano()/int64(deadlineQuantum) + 1) * int64(deadlineQuantum)
			timer.Reset(time.Duration(next - now.UnixNano()))
			tick = timer.C
		}
		select {
		case <-w.stop:
			return
		case <-w.wake:
			timer.Stop()
		case <-tick:
		}
		now := time.Now().UnixNano() / int64(deadlineQuantum)
		var expired []*Conn
		w.mu.Lock()
		for t := max(last+1, now-deadlineSlots+1); t <= now; t++ {
			for c := range w.slots[t%deadlineSlots] {
				if c.timerTick <= now {
					delete(w.slots[t%deadlineSlots], c)
					c.timerSlot = -1
					c.timerArmed.Store(false)
					w.count--
					expired = append(expired, c)
				}
			}
		}
		w.mu.Unlock()
		last = now
		for _, c := range expired {
			c.expire()
		}
	}
}
