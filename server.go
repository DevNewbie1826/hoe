package hoe

import (
	"context"
	"crypto/tls"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// ErrServerClosed is returned by serving methods after Shutdown or Close.
var ErrServerClosed = http.ErrServerClosed

// Server defines the Go 1.25 http.Server configuration and lifecycle.
// Its zero value is ready to serve. Configure fields before starting serving;
// do not copy a Server after first use.
//
// The future HTTP/1 engine has one compatibility exception: a MaxBytesReader
// kept only in a handler-local variable cannot signal connection closure.
// Assign the wrapper to r.Body to preserve net/http's overflow-close behavior.
// With the current standard-library delegation, even the local case matches.
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

	once sync.Once
	std  *http.Server
}

// ListenAndServe listens on s.Addr and serves s.Handler.
// It always returns a non-nil error, including ErrServerClosed after shutdown.
func (s *Server) ListenAndServe() error { return s.fallback().ListenAndServe() }

// ListenAndServeTLS listens on s.Addr with TLS. Certificate files may be empty
// when TLSConfig supplies certificates or GetCertificate.
func (s *Server) ListenAndServeTLS(certFile, keyFile string) error {
	return s.fallback().ListenAndServeTLS(certFile, keyFile)
}

// Serve serves requests on l and closes l on return.
// It always returns a non-nil error.
func (s *Server) Serve(l net.Listener) error { return s.fallback().Serve(l) }

// ServeTLS serves TLS requests on l and closes l on return.
// Certificate files may be empty when TLSConfig supplies certificates.
func (s *Server) ServeTLS(l net.Listener, certFile, keyFile string) error {
	return s.fallback().ServeTLS(l, certFile, keyFile)
}

// Shutdown gracefully closes listeners and idle connections, waiting for
// active handlers until ctx expires. Hijacked connections are not closed.
func (s *Server) Shutdown(ctx context.Context) error { return s.fallback().Shutdown(ctx) }

// Close immediately closes listeners and all non-hijacked connections.
func (s *Server) Close() error { return s.fallback().Close() }

// RegisterOnShutdown registers a nonblocking shutdown notification callback.
func (s *Server) RegisterOnShutdown(f func()) { s.fallback().RegisterOnShutdown(f) }

// SetKeepAlivesEnabled controls HTTP keep-alive; it is enabled by default.
func (s *Server) SetKeepAlivesEnabled(v bool) { s.fallback().SetKeepAlivesEnabled(v) }

func (s *Server) engineKind() string { return "std" }
