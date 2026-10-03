package hoe

import (
	"net"
	"net/http"
)

func (s *Server) fallback() *http.Server {
	s.once.Do(func() {
		if s.std == nil {
			s.std = &http.Server{}
		}
		// Assign configuration only; never copy http.Server's mutexes/state.
		s.std.Addr = s.Addr
		s.std.Handler = s.Handler
		s.std.DisableGeneralOptionsHandler = s.DisableGeneralOptionsHandler
		s.std.TLSConfig = s.TLSConfig
		s.std.ReadTimeout = s.ReadTimeout
		s.std.ReadHeaderTimeout = s.ReadHeaderTimeout
		s.std.WriteTimeout = s.WriteTimeout
		s.std.IdleTimeout = s.IdleTimeout
		s.std.MaxHeaderBytes = s.MaxHeaderBytes
		s.std.TLSNextProto = s.TLSNextProto
		s.std.ConnState = s.ConnState
		s.std.ErrorLog = s.ErrorLog
		s.std.BaseContext = s.BaseContext
		s.std.ConnContext = s.ConnContext
		s.std.HTTP2 = s.HTTP2
		s.std.Protocols = s.Protocols
	})
	return s.std
}

// FromStd wraps s without copying its lifecycle state or registered callbacks.
// Configure the returned Server before starting it, as with http.Server.
func FromStd(s *http.Server) *Server {
	return &Server{
		Addr: s.Addr, Handler: s.Handler, DisableGeneralOptionsHandler: s.DisableGeneralOptionsHandler,
		TLSConfig: s.TLSConfig, ReadTimeout: s.ReadTimeout, ReadHeaderTimeout: s.ReadHeaderTimeout,
		WriteTimeout: s.WriteTimeout, IdleTimeout: s.IdleTimeout, MaxHeaderBytes: s.MaxHeaderBytes,
		TLSNextProto: s.TLSNextProto, ConnState: s.ConnState, ErrorLog: s.ErrorLog,
		BaseContext: s.BaseContext, ConnContext: s.ConnContext, HTTP2: s.HTTP2, Protocols: s.Protocols, std: s,
	}
}

// ListenAndServe listens on addr and serves handler; nil uses DefaultServeMux.
func ListenAndServe(addr string, handler http.Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServe()
}

// ListenAndServeTLS listens with TLS using the certificate and key files.
func ListenAndServeTLS(addr, certFile, keyFile string, handler http.Handler) error {
	return (&Server{Addr: addr, Handler: handler}).ListenAndServeTLS(certFile, keyFile)
}

// Serve serves handler on l; nil uses DefaultServeMux.
func Serve(l net.Listener, handler http.Handler) error {
	return (&Server{Handler: handler}).Serve(l)
}

// ServeTLS serves handler with TLS on l using the certificate and key files.
func ServeTLS(l net.Listener, handler http.Handler, certFile, keyFile string) error {
	return (&Server{Handler: handler}).ServeTLS(l, certFile, keyFile)
}
