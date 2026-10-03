package hoe

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

// ErrServerClosed is returned by serving methods after Shutdown or Close.
var ErrServerClosed = http.ErrServerClosed

// Server defines the Go 1.25 http.Server configuration and lifecycle.
// Its zero value is ready to serve. Configure fields before starting serving;
// do not copy a Server after first use.
//
// The HTTP/1 engine has one compatibility exception: a MaxBytesReader
// kept only in a handler-local variable cannot signal connection closure.
// Assign the wrapper to r.Body to preserve net/http's overflow-close behavior.
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
	// BaseContext supplies a non-nil base context for each listener.
	// Nil uses context.Background.
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

// Serve serves requests on l and closes l on return.
// It always returns a non-nil error.
func (s *Server) Serve(l net.Listener) error {
	tcp, ok := l.(*net.TCPListener)
	if !ok || !conn.Supported() || s.Protocols != nil && (s.Protocols.UnencryptedHTTP2() || !s.Protocols.HTTP1()) {
		return s.serving().Serve(l)
	}
	defer l.Close()
	s.mu.Lock()
	if s.shutting.Load() {
		s.mu.Unlock()
		return ErrServerClosed
	}
	e, err := conn.New(s.serving(), tcp, func() bool { return !s.noKeepAlive.Load() && !s.shutting.Load() })
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
	if !errors.Is(err, ErrServerClosed) {
		e.Close()
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
