//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

func (e *Engine) notifyLocked() {
	if e.changed != nil {
		close(e.changed)
		e.changed = nil
	}
}

func (e *Engine) watchLocked() <-chan struct{} {
	if e.changed == nil {
		e.changed = make(chan struct{})
	}
	return e.changed
}

// StopListening stops all acceptance without interrupting active responses.
// Listeners whose accept loop already runs publish the engine-wide stop as
// their loop ends; listeners that are reserved but not yet serving have no
// loop to observe the close, so their listener is ended here directly — a
// listener parked in BaseContext cannot stall engine shutdown.
func (e *Engine) StopListening() error {
	e.stopping.Store(true)
	e.mu.Lock()
	ls := make([]*net.TCPListener, 0, len(e.listeners))
	for l, st := range e.listeners {
		if st.serving {
			ls = append(ls, l)
		}
	}
	unserved := make([]*net.TCPListener, 0, len(e.listeners))
	for l, st := range e.listeners {
		if !st.serving {
			unserved = append(unserved, l)
		}
	}
	last := len(e.listeners) == 0 || len(unserved) == len(e.listeners)
	e.mu.Unlock()
	for _, l := range ls {
		e.noteListenerClose(l.Close())
	}
	for _, l := range unserved {
		e.endListener(l)
	}
	if last {
		e.stopOnce.Do(func() { close(e.stopped) })
	}
	e.mu.Lock()
	err := e.acceptErr
	e.mu.Unlock()
	return err
}

func (e *Engine) finishOpen() {
	e.mu.Lock()
	e.opening--
	e.notifyLocked()
	e.mu.Unlock()
}

// Close cancels requests, closes HTTP descriptors, and joins owned goroutines.
// Hijacked runtime connections remain the handler's responsibility.
func (e *Engine) Close() error {
	e.StopListening()
	err := e.closeResources(true)
	<-e.finalized
	return err
}

func (e *Engine) closeResources(force bool) error {
	e.closeOnce.Do(func() {
		e.closeErr = e.StopListening()
		e.mu.Lock()
		cs := make([]*Conn, 0, len(e.conns))
		for _, c := range e.conns {
			cs = append(cs, c)
		}
		e.mu.Unlock()
		for _, c := range cs {
			c.cancelRequest()
			e.closeErr = errors.Join(e.closeErr, c.Close())
			c.mu.Lock()
			active := force && c.serving && c.state.Load() == int32(http.StateActive)
			c.mu.Unlock()
			if active {
				// Like std Close, descriptor closure does not join user code
				// which may ignore cancellation. Its queued OnClose still
				// publishes StateClosed when the callback eventually returns.
				c.raw.RelinquishWorker()
			}
		}
		e.closeErr = errors.Join(e.closeErr, e.group.Close())
		e.timers.close()
		trackEngine(e, false)
		close(e.resourcesClosed)
	})
	return e.closeErr
}

// A timed-out Shutdown must not strand reactor resources after its last
// active handler eventually finishes. This owner also joins on Close.
func (e *Engine) finalize() {
	defer close(e.finalized)
	<-e.stopped
	for {
		e.mu.Lock()
		empty := len(e.conns) == 0 && e.opening == 0
		change := e.watchLocked()
		e.mu.Unlock()
		if empty {
			e.closeResources(false)
			return
		}
		select {
		case <-change:
		case <-e.resourcesClosed:
			return
		}
	}
}

// Shutdown closes idle connections and waits for active requests or ctx.
func (e *Engine) Shutdown(ctx context.Context) error {
	if err := e.StopListening(); err != nil && !errors.Is(err, net.ErrClosed) {
		return err
	}
	for {
		e.mu.Lock()
		cs := make([]*Conn, 0, len(e.conns))
		for _, c := range e.conns {
			cs = append(cs, c)
		}
		empty := len(cs) == 0 && e.opening == 0
		change := e.watchLocked()
		e.mu.Unlock()
		if empty {
			return e.Close()
		}
		for _, c := range cs {
			if c.state.Load() == int32(http.StateIdle) {
				c.Close()
			} else if c.state.Load() == int32(http.StateNew) {
				t, _ := c.deadline("read")
				grace := c.accepted.Add(5 * time.Second)
				if t.IsZero() || t.After(grace) {
					c.SetReadDeadline(grace)
				}
			}
		}
		select {
		case <-change:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CloseIdle closes connections parked between requests.
func (e *Engine) CloseIdle() {
	e.mu.Lock()
	cs := make([]*Conn, 0, len(e.conns))
	for _, c := range e.conns {
		if c.state.Load() == int32(http.StateIdle) {
			cs = append(cs, c)
		}
	}
	e.mu.Unlock()
	for _, c := range cs {
		c.Close()
	}
}
