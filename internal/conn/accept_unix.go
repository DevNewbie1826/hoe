//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"syscall"
	"time"
)

// listenerState tracks one listener's acceptance inside the engine: the base
// context its connections carry, the adoption failure that stopped it, the
// in-flight adoptions its accept loop must outlive, and whether its accept
// loop has started (a reserved listener with no loop yet is ended by
// StopListening, so a blocked BaseContext cannot stall engine shutdown).
type listenerState struct {
	base      context.Context
	adoptErr  error // guarded by Engine.mu
	adoptions sync.WaitGroup
	serving   bool // guarded by Engine.mu
}

// reserve creates l's listener state, calling BaseContext once, and
// registers it under one lock. Registering before any acceptance starts is
// what keeps a listener that ends immediately from retiring the engine
// while its siblings are still being reserved.
func (e *Engine) reserve(l *net.TCPListener) (*listenerState, error) {
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
	defer e.mu.Unlock()
	if e.stopping.Load() {
		return nil, http.ErrServerClosed
	}
	e.listeners[l] = st
	return st, nil
}

// Serve accepts on the caller's listener and transfers sockets to the
// shared loops; concurrent calls serve different listeners through one
// group. It returns when that listener's acceptance stops, independently of
// active request completion. Connections adopted here carry this listener's
// BaseContext, and an adoption failure stops only this listener.
func (e *Engine) Serve(l *net.TCPListener) error {
	st, err := e.reserve(l)
	if err != nil {
		return err
	}
	return e.serveReserved(l, st)
}

// ServeAll reserves every listener before any acceptance starts, then
// serves them concurrently, and returns the joined per-listener errors. A
// listener whose acceptance ends immediately cannot publish acceptance-stop
// while later listeners are still reserving.
func (e *Engine) ServeAll(ls ...*net.TCPListener) error {
	states := make([]*listenerState, len(ls))
	for i, l := range ls {
		st, err := e.reserve(l)
		if err != nil {
			for j := 0; j < i; j++ {
				e.endListener(ls[j])
			}
			return err
		}
		states[i] = st
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(ls))
	for i, l := range ls {
		wg.Add(1)
		go func(l *net.TCPListener, st *listenerState) {
			defer wg.Done()
			errs <- e.serveReserved(l, st)
		}(l, states[i])
	}
	wg.Wait()
	close(errs)
	var err error
	for serveErr := range errs {
		err = errors.Join(err, serveErr)
	}
	return err
}

// serveReserved runs one registered listener's accept loop to its end.
func (e *Engine) serveReserved(l *net.TCPListener, st *listenerState) (serveErr error) {
	e.mu.Lock()
	st.serving = true
	e.mu.Unlock()
	defer func() {
		// Only this listener's accept loop can add to its adoptions, and
		// the loop has ended here, so the wait cannot race a later Add.
		st.adoptions.Wait()
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
// listener ends, acceptance-stop is published for the finalizer. The state
// moves to retired rather than being dropped: an accept already returning a
// backlog connection when the close lands must still adopt it with this
// listener's base context.
func (e *Engine) endListener(l *net.TCPListener) {
	e.mu.Lock()
	st := e.listeners[l]
	delete(e.listeners, l)
	if st != nil && !e.retiredSealed {
		e.retired[l] = st
	}
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
	if st == nil {
		// A listener ended while its accept was already returning a backlog
		// connection: adopt with the retired state so the connection keeps
		// that listener's base context.
		st = e.retired[l]
	}
	if st == nil {
		// Adoptions outside a running Serve carry no listener base context.
		st = &listenerState{base: context.Background()}
	}
	e.opening++
	e.mu.Unlock()
	st.adoptions.Add(1)
	if err := e.group.AdoptAsync(fd, st.base, func(err error) { e.adopted(err, l, st) }); err != nil {
		e.adopted(err, l, st)
		return err
	}
	return nil
}

// adopted attributes a registration failure to the listener that accepted
// the socket: that listener's state latches the error and its acceptance
// stops, leaving the engine's other listeners untouched.
func (e *Engine) adopted(err error, l *net.TCPListener, st *listenerState) {
	if err != nil {
		e.mu.Lock()
		if cur := e.listeners[l]; cur != nil && cur.adoptErr == nil {
			cur.adoptErr = err
		}
		e.mu.Unlock()
		e.finishOpen()
		l.Close()
	}
	st.adoptions.Done()
}
