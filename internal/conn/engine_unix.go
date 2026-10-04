//go:build linux || darwin

// Package conn joins the HTTP/1 codec to loop-owned TCP descriptors.
package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/DevNewbie1826/hoe/internal/loop"
	"github.com/DevNewbie1826/hoe/internal/poll"
)

// Engine owns one listener's HTTP connections, workers, and deadline wheel.
type Engine struct {
	server          *http.Server
	keepAlive       func() bool
	base            context.Context
	group           *loop.Group
	listener        *net.TCPListener
	mu              sync.Mutex
	conns           map[*loop.Conn]*Conn
	changed         chan struct{}
	stopping        atomic.Bool
	pendingOutput   atomic.Int64
	stopOnce        sync.Once
	stopped         chan struct{}
	closeOnce       sync.Once
	closeErr        error
	finalized       chan struct{}
	resourcesClosed chan struct{}
	timers          deadlineWheel
}

// New prepares a TCP engine using the standard server configuration snapshot.
// The supplied keepAlive predicate includes shutdown and runtime policy.
func New(s *http.Server, l *net.TCPListener, keepAlive func() bool) (*Engine, error) {
	base := context.Background()
	if s.BaseContext != nil {
		base = s.BaseContext(l)
		if base == nil {
			panic("BaseContext returned a nil context")
		}
	}
	e := &Engine{server: s, keepAlive: keepAlive, base: context.WithValue(base, http.ServerContextKey, s),
		conns: make(map[*loop.Conn]*Conn), changed: make(chan struct{}), stopped: make(chan struct{}), finalized: make(chan struct{}), resourcesClosed: make(chan struct{})}
	e.timers.init()
	g, err := loop.New(loop.Config{Handler: e, IdleWorkerTimeout: time.Second})
	if err != nil {
		return nil, err
	}
	e.group = g
	e.listener = l
	go e.timers.run()
	trackEngine(e, true)
	go e.finalize()
	return e, nil
}

// Serve accepts on the caller's listener and transfers sockets to the loops.
// It returns when acceptance stops, independently of active request completion.
func (e *Engine) Serve(l *net.TCPListener) error {
	defer e.StopListening()
	for {
		c, err := l.AcceptTCP()
		if err != nil {
			if e.stopping.Load() {
				return http.ErrServerClosed
			}
			return err
		}
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
		if err = e.group.Adopt(fd); err != nil {
			if e.stopping.Load() {
				return http.ErrServerClosed
			}
			return err
		}
	}
}

// OnOpen creates the stable net.Conn identity and context chain.
func (e *Engine) OnOpen(raw *loop.Conn) {
	local, err := raw.LocalAddr()
	if err != nil {
		raw.Close()
		return
	}
	remote, err := raw.RemoteAddr()
	if err != nil {
		raw.Close()
		return
	}
	c := &Conn{engine: e, raw: raw, local: tcpAddr(local), remote: tcpAddr(remote), changed: make(chan struct{}), timerSlot: -1}
	c.accepted = time.Now()
	c.base = e.base
	if e.server.ConnContext != nil {
		c.base = e.server.ConnContext(c.base, c)
		if c.base == nil {
			panic("ConnContext returned a nil context")
		}
	}
	c.base = context.WithValue(c.base, http.LocalAddrContextKey, c.local)
	e.mu.Lock()
	e.conns[raw] = c
	raw.Data.Store(c)
	e.notifyLocked()
	e.mu.Unlock()
	c.setState(http.StateNew)
	// Like std conn.serve, snapshot after ConnContext and StateNew hooks.
	c.remoteName = c.remote.String()
	c.startHead(false)
	if e.stopping.Load() {
		c.Close()
	}
}

func (e *Engine) lookup(raw *loop.Conn) *Conn {
	c, _ := raw.Data.Load().(*Conn)
	return c
}

// OnReadiness signals disconnect directly, even while ServeHTTP is blocked.
// It never waits for a loop command or performs socket I/O.
func (e *Engine) OnReadiness(raw *loop.Conn, ev poll.Event) {
	if ev.EOF {
		if c := e.lookup(raw); c != nil {
			c.peerEOF()
		}
	}
}

// OnEvent reads only available header bytes, then runs the ready request inline.
func (e *Engine) OnEvent(raw *loop.Conn, ev poll.Event) {
	if c := e.lookup(raw); c != nil {
		c.round()
	}
}

// OnClose releases all retained input before publishing the terminal state.
func (e *Engine) OnClose(raw *loop.Conn, cause error) {
	c := e.lookup(raw)
	if c == nil {
		return
	}
	c.cancelRequest()
	c.releaseInput()
	c.stopDeadlines()
	c.setState(http.StateClosed)
	e.forget(c)
}

func (e *Engine) forget(c *Conn) {
	e.mu.Lock()
	delete(e.conns, c.raw)
	e.notifyLocked()
	e.mu.Unlock()
	resourceChanged()
}

func tcpAddr(sa syscall.Sockaddr) *net.TCPAddr {
	switch a := sa.(type) {
	case *syscall.SockaddrInet4:
		return &net.TCPAddr{IP: net.IP(a.Addr[:]), Port: a.Port}
	case *syscall.SockaddrInet6:
		addr := &net.TCPAddr{IP: net.IP(a.Addr[:]), Port: a.Port}
		if a.ZoneId != 0 {
			if intf, err := net.InterfaceByIndex(int(a.ZoneId)); err == nil {
				addr.Zone = intf.Name
			}
		}
		return addr
	default:
		return &net.TCPAddr{}
	}
}

// Supported reports whether plaintext TCP can use the event engine.
func Supported() bool { return true }
