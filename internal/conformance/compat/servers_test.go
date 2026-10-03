package compat

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

type server interface {
	Serve(net.Listener) error
	Close() error
	Shutdown(context.Context) error
}

// Construct directly so missing field delegation cannot be hidden by FromStd
// retaining an already-configured standard server.
func hoeConfig(s *http.Server) *hoe.Server {
	return &hoe.Server{
		Addr: s.Addr, Handler: s.Handler, DisableGeneralOptionsHandler: s.DisableGeneralOptionsHandler,
		TLSConfig: s.TLSConfig, ReadTimeout: s.ReadTimeout, ReadHeaderTimeout: s.ReadHeaderTimeout,
		WriteTimeout: s.WriteTimeout, IdleTimeout: s.IdleTimeout, MaxHeaderBytes: s.MaxHeaderBytes,
		TLSNextProto: s.TLSNextProto, ConnState: s.ConnState, ErrorLog: s.ErrorLog,
		BaseContext: s.BaseContext, ConnContext: s.ConnContext, HTTP2: s.HTTP2, Protocols: s.Protocols,
	}
}

func launch(t *testing.T, own bool, cfg *http.Server) (string, server) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ErrorLog = log.New(io.Discard, "", 0)
	var s server = cfg
	if own {
		s = hoeConfig(cfg)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("serving loop leaked")
		}
	})
	return l.Addr().String(), s
}

func same(t *testing.T, want, got []byte) {
	t.Helper()
	if !bytes.Equal(want, got) {
		t.Fatalf("DIFF\nstd %q\nhoe %q", want, got)
	}
	t.Logf("SAME %q", got)
}

func signal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("event timeout")
	}
}
