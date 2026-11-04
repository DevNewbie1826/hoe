//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"
	"time"
)

// listenerState tracks one listener's acceptance inside the engine: the base
// context its connections carry, and the adoption failure that stopped it.
type listenerState struct {
	base     context.Context
	adoptErr error // guarded by Engine.mu
}

// Serve accepts on the caller's listener and transfers sockets to the
// shared loops; concurrent calls serve different listeners through one
// group. It returns when that listener's acceptance stops, independently of
// active request completion. Connections adopted here carry this listener's
// BaseContext, and an adoption failure stops only this listener.
func (e *Engine) Serve(l *net.TCPListener) (serveErr error) {
	base := context.Background()
	if e.server.BaseContext != nil {
		base = e.server.BaseContext(l)
		if base == nil {
			panic("BaseContext returned a nil context")
		}
	}
	base = context.WithValue(base, http.ServerContextKey, e.server)
	st := &listenerState{base: base}
	e.mu.Lock()
	if e.stopping.Load() {
		e.mu.Unlock()
		return http.ErrServerClosed
	}
	e.listeners[l] = st
	e.mu.Unlock()
	defer func() {
		e.adoptions.Wait()
		e.mu.Lock()
		adoptErr := st.adoptErr
		e.mu.Unlock()
		if adoptErr != nil {
			if e.stopping.Load() {
				serveErr = http.ErrServerClosed
			} else {
				serveErr = adoptErr
			}
		}
		e.endListener(l)
	}()
	var tempDelay time.Duration
	for {
		c, err := l.AcceptTCP()
		if err != nil {
			if e.stopping.Load() {
				return http.ErrServerClosed
			}
			var temporary net.Error
			if errors.As(err, &temporary) && temporary.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay = min(tempDelay*2, time.Second)
				}
				if logger := e.server.ErrorLog; logger != nil {
					logger.Printf("http: Accept error: %v; retrying in %v", err, tempDelay)
				} else {
					e.serverLog("http: Accept error: %v; retrying in %v", err, tempDelay)
				}
				time.Sleep(tempDelay)
				continue
			}
			return err
		}
		tempDelay = 0
		raw, err := c.SyscallConn()
		if err != nil {
			c.Close()
			return err
		}
		fd := -1
		var dupErr error
		err = raw.Control(func(socket uintptr) {
			// Dup preserves the accepted socket's nonblocking flag. Unlike
			// TCPConn.File, it never changes the runtime-owned file status.
			syscall.ForkLock.RLock()
			defer syscall.ForkLock.RUnlock()
			fd, dupErr = syscall.Dup(int(socket))
			if dupErr == nil {
				syscall.CloseOnExec(fd)
			}
		})
		closeErr := c.Close()
		if err = errors.Join(err, dupErr, closeErr); err != nil {
			if fd >= 0 {
				syscall.Close(fd)
			}
			return err
		}
		if err = e.adopt(fd, l); err != nil {
			return err
		}
	}
}

// endListener stops one listener's acceptance. When the engine's last
// listener ends, acceptance-stop is published for the finalizer.
func (e *Engine) endListener(l *net.TCPListener) {
	e.mu.Lock()
	delete(e.listeners, l)
	last := len(e.listeners) == 0
	e.mu.Unlock()
	e.noteListenerClose(l.Close())
	if last {
		e.stopOnce.Do(func() { close(e.stopped) })
	}
}

func (e *Engine) noteListenerClose(err error) {
	if err == nil || errors.Is(err, net.ErrClosed) {
		return
	}
	e.mu.Lock()
	e.acceptErr = errors.Join(e.acceptErr, err)
	e.mu.Unlock()
}

func (e *Engine) adopt(fd int, l *net.TCPListener) error {
	e.mu.Lock()
	st := e.listeners[l]
	e.opening++
	e.mu.Unlock()
	if st == nil {
		// Adoptions outside a running Serve carry no listener base context.
		st = &listenerState{base: context.Background()}
	}
	e.adoptions.Add(1)
	if err := e.group.AdoptAsync(fd, st.base, func(err error) { e.adopted(err, l) }); err != nil {
		e.adopted(err, l)
		return err
	}
	return nil
}

// adopted attributes a registration failure to the listener that accepted
// the socket: that listener's state latches the error and its acceptance
// stops, leaving the engine's other listeners untouched.
func (e *Engine) adopted(err error, l *net.TCPListener) {
	if err != nil {
		e.mu.Lock()
		if st := e.listeners[l]; st != nil && st.adoptErr == nil {
			st.adoptErr = err
		}
		e.mu.Unlock()
		e.finishOpen()
		l.Close()
	}
	e.adoptions.Done()
}
