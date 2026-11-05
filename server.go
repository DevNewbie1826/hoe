package hoe

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

// ErrServerClosed is returned by serving methods after Shutdown or Close.
var ErrServerClosed = http.ErrServerClosed

// Server defines the Go 1.25 http.Server configuration and lifecycle.
// Its zero value is ready to serve. Configure fields before starting serving;
// do not copy a Server after first use. BaseContext is the exception: it is
// read when each serving call starts, so assigning it between serving calls
// takes effect as in net/http. Assigning it concurrently with a running
// serving call is not supported, as in net/http. The other configuration
// fields are not re-read for each serving call: configure them before serving,
// and do not change them while a serving call is running.
//
// The HTTP/1 engine has two owner-approved compatibility exceptions, both
// for a MaxBytesReader that is not reachable from the served Request.Body:
// one kept only in a handler-local variable, and one installed on a copied
// request by http.MaxBytesHandler. After such an overflow the response and
// error match net/http, but the connection stays reusable instead of closing
// when at most 256 KiB of the body remains unread. Assign the wrapper to
// r.Body to preserve net/http's overflow-close behavior.
type Server struct {
	// Addr is the TCP listening address; empty means ":http".
	Addr string
	// Handler handles requests; nil uses http.DefaultServeMux.
	Handler http.Handler
	// DisableGeneralOptionsHandler forwards OPTIONS * to Handler if true.
	DisableGeneralOptionsHandler bool
	// TLSConfig configures TLS; ServeTLS clones it before use.
	TLSConfig *tls.Config
	// ReadTimeout limits reading the entire request, including its body.
	// Zero or negative disables the timeout.
	ReadTimeout time.Duration
	// ReadHeaderTimeout limits header reading; zero uses ReadTimeout.
	// Negative, or zero with nonpositive ReadTimeout, disables it.
	ReadHeaderTimeout time.Duration
	// WriteTimeout limits response writing; nonpositive disables it.
	// It is reset when a new request's headers are read.
	WriteTimeout time.Duration
	// IdleTimeout limits keep-alive waiting; zero uses ReadTimeout.
	// Negative, or zero with nonpositive ReadTimeout, disables it.
	IdleTimeout time.Duration
	// PipelineFlushDelay bounds kernel coalescing of buffered HTTP/1 responses.
	// Zero uses 1 ms, a positive value sets the cap, and a negative value
	// disables coalescing for per-response delivery. Each response is still
	// written before the next handler runs, but unlike net/http its delivery
	// may be delayed up to the cap while a later pipelined handler blocks.
	// Round completion and explicit flushes or handoffs push immediately.
	// This setting applies only to the native plaintext TCP HTTP/1 engine.
	PipelineFlushDelay time.Duration
	// MaxHeaderBytes limits request line and headers, not body size.
	// Zero uses http.DefaultMaxHeaderBytes.
	MaxHeaderBytes int
	// TLSNextProto takes over connections after ALPN negotiation.
	// A non-nil map disables automatic HTTP/2 setup unless Protocols enables it.
	TLSNextProto map[string]func(*http.Server, *tls.Conn, http.Handler)
	// ConnState observes standard connection state transitions.
	ConnState func(net.Conn, http.ConnState)
	// ErrorLog receives server errors; nil uses the standard logger.
	ErrorLog *log.Logger
	// BaseContext supplies a base context for each listener. It is read each
	// time a serving call starts (Serve, ServeAll, ServeTLS, ListenAndServeTLS),
	// once per listener, so assigning it between serving calls takes effect as
	// in net/http; assigning it concurrently with a running serving call is not
	// supported. Nil means context.Background, and a non-nil function must
	// return a non-nil context.
	BaseContext func(net.Listener) context.Context
	// ConnContext derives a non-nil context for a newly accepted connection.
	ConnContext func(context.Context, net.Conn) context.Context
	// HTTP2 configures HTTP/2 connections.
	HTTP2 *http.HTTP2Config
	// Protocols selects HTTP/1, HTTP/2 and unencrypted HTTP/2.
	// Nil follows net/http defaults.
	Protocols *http.Protocols

	once        sync.Once
	configured  sync.Once
	std         *http.Server
	mu          sync.Mutex
	engines     map[*conn.Engine]struct{}
	shutting    atomic.Bool
	noKeepAlive atomic.Bool
	usingEngine atomic.Bool
}

// ListenAndServe listens on s.Addr and serves s.Handler.
// It always returns a non-nil error, including ErrServerClosed after shutdown.
func (s *Server) ListenAndServe() error {
	if s.shutting.Load() {
		return ErrServerClosed
	}
	addr := s.Addr
	if addr == "" {
		addr = ":http"
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(l)
}

// ListenAndServeTLS listens on s.Addr with TLS. Certificate files may be empty
// when TLSConfig supplies certificates or GetCertificate.
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	return s.serving().ListenAndServeTLS(certFile, keyFile)
}

// engineEligible reports whether l can be served by the native HTTP/1 event
// engine. It is the single engine-versus-net/http decision shared by Serve
// and ServeAll: only plaintext TCP listeners on supported platforms whose
// protocol set is plain HTTP/1 qualify.
func (s *Server) engineEligible(l net.Listener) bool {
	if _, ok := l.(*net.TCPListener); !ok || !conn.Supported() {
		return false
	}
	return s.Protocols == nil || !s.Protocols.UnencryptedHTTP2() && s.Protocols.HTTP1()
}

// pruneWhenDone removes e from s.engines once the engine has finished, so a
// completed serving lifecycle leaves no engine state reachable from the
// Server. It returns immediately; the removal happens on its own goroutine.
func (s *Server) pruneWhenDone(e *conn.Engine) {
	go func() {
		<-e.Done()
		s.mu.Lock()
		delete(s.engines, e)
		s.mu.Unlock()
	}()
}

// Serve serves requests on l and closes l on return.
// It always returns a non-nil error.
func (s *Server) Serve(l net.Listener) error {
	if !s.engineEligible(l) {
		return s.serving().Serve(l)
	}
	tcp := l.(*net.TCPListener)
	defer l.Close()
	s.mu.Lock()
	if s.shutting.Load() {
		s.mu.Unlock()
		return ErrServerClosed
	}
	e, err := conn.New(s.serving(), conn.Policy{
		KeepAlive:          func() bool { return !s.noKeepAlive.Load() && !s.shutting.Load() },
		PipelineFlushDelay: s.PipelineFlushDelay,
	})
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if s.engines == nil {
		s.engines = make(map[*conn.Engine]struct{})
	}
	s.engines[e] = struct{}{}
	s.usingEngine.Store(true)
	s.mu.Unlock()
	err = e.Serve(tcp)
	if !errors.Is(err, ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		e.Close()
	}
	s.pruneWhenDone(e)
	return err
}

// ServeAll serves requests on every listener. Engine-eligible listeners
// share one native HTTP/1 engine — one loop group sized by
// runtime.GOMAXPROCS(0), one worker pool per reactor, one deadline wheel —
// so N listeners pay one engine's scheduling cost instead of N engines'.
// Every engine-eligible listener is reserved before any acceptance starts
// and is closed when ServeAll returns, like Serve; listeners Serve would
// hand to net/http are served by net/http and keep its listener semantics.
//
// BaseContext is read when ServeAll starts, once per listener before
// acceptance starts, and connections adopted from a listener carry that
// listener's base context. A permanent accept error on one listener stops
// only that listener's acceptance; ServeAll returns when every listener has
// stopped, with the errors joined. It always returns a non-nil error,
// ErrServerClosed after Shutdown or Close.
func (s *Server) ServeAll(listeners ...net.Listener) error {
	if len(listeners) == 0 {
		return errors.New("hoe: ServeAll requires at least one listener")
	}
	var engineListeners []*net.TCPListener
	var stdListeners []net.Listener
	for _, l := range listeners {
		if s.engineEligible(l) {
			engineListeners = append(engineListeners, l.(*net.TCPListener))
		} else {
			stdListeners = append(stdListeners, l)
		}
	}
	// Like Serve and like net/http's Serve, ServeAll takes over every handed
	// listener and closes it on return, the early returns below included.
	// The accept loops and net/http's Serve close them again on the normal
	// path; a second Close of an already-closed listener is harmless.
	defer func() {
		for _, l := range listeners {
			l.Close()
		}
	}()
	if s.shutting.Load() {
		return ErrServerClosed
	}
	var eng *conn.Engine
	if len(engineListeners) != 0 {
		s.mu.Lock()
		if s.shutting.Load() {
			s.mu.Unlock()
			return ErrServerClosed
		}
		var err error
		eng, err = conn.New(s.serving(), conn.Policy{
			KeepAlive:          func() bool { return !s.noKeepAlive.Load() && !s.shutting.Load() },
			PipelineFlushDelay: s.PipelineFlushDelay,
			Loops:              runtime.GOMAXPROCS(0),
		})
		if err != nil {
			s.mu.Unlock()
			return err
		}
		if s.engines == nil {
			s.engines = make(map[*conn.Engine]struct{})
		}
		s.engines[eng] = struct{}{}
		s.usingEngine.Store(true)
		s.mu.Unlock()
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(listeners))
	if eng != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- eng.ServeAll(engineListeners...)
		}()
	}
	for _, l := range stdListeners {
		wg.Add(1)
		go func(l net.Listener) {
			defer wg.Done()
			errs <- s.serving().Serve(l)
		}(l)
	}
	wg.Wait()
	close(errs)
	var err error
	for e := range errs {
		err = errors.Join(err, e)
	}
	if eng != nil && !errors.Is(err, ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
		eng.Close()
	}
	if eng != nil {
		s.pruneWhenDone(eng)
	}
	return err
}

// ServeTLS serves TLS requests on l and closes l on return.
// Certificate files may be empty when TLSConfig supplies certificates.
func (s *Server) ServeTLS(l net.Listener, certFile, keyFile string) error {
	return s.serving().ServeTLS(l, certFile, keyFile)
}

// Shutdown gracefully closes listeners and idle connections, waiting for
// active handlers until ctx expires. Hijacked connections are not closed.
func (s *Server) Shutdown(ctx context.Context) error {
	s.shutting.Store(true)
	err := s.fallback().Shutdown(ctx)
	for _, e := range s.engineSnapshot() {
		err = errors.Join(err, e.Shutdown(ctx))
	}
	return err
}

// Close immediately closes listeners and all non-hijacked connections.
func (s *Server) Close() error {
	s.shutting.Store(true)
	err := s.fallback().Close()
	for _, e := range s.engineSnapshot() {
		err = errors.Join(err, e.Close())
	}
	return err
}

// RegisterOnShutdown registers a nonblocking shutdown notification callback.
func (s *Server) RegisterOnShutdown(f func()) { s.fallback().RegisterOnShutdown(f) }

// SetKeepAlivesEnabled controls HTTP keep-alive; it is enabled by default.
func (s *Server) SetKeepAlivesEnabled(v bool) {
	s.noKeepAlive.Store(!v)
	s.fallback().SetKeepAlivesEnabled(v)
	if !v {
		for _, e := range s.engineSnapshot() {
			e.CloseIdle()
		}
	}
}

func (s *Server) engineKind() string {
	if s.usingEngine.Load() {
		return "event"
	}
	return "std"
}

func (s *Server) engineSnapshot() []*conn.Engine {
	s.mu.Lock()
	defer s.mu.Unlock()
	es := make([]*conn.Engine, 0, len(s.engines))
	for e := range s.engines {
		es = append(es, e)
	}
	return es
}
