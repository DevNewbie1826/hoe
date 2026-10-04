package hoe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

func TestConfigurationAfterLifecycleSetup(t *testing.T) {
	// Given a server with shutdown hooks and keep-alive settings registered
	// before its handler is configured, as permitted by net/http.
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "old") })}
	s.RegisterOnShutdown(func() {})
	s.SetKeepAlivesEnabled(false)
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "configured") })
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server leaked")
		}
	})
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	// When a real client reaches the serving path.
	r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	// Then the latest pre-Serve configuration is used.
	if string(p) != "configured" || !r.Close {
		t.Fatalf("body=%q close=%t", p, r.Close)
	}
}

func TestFromStdPreservesLifecycle(t *testing.T) {
	std := &http.Server{}
	shutdown := make(chan struct{})
	std.RegisterOnShutdown(func() { close(shutdown) })
	s := FromStd(std)
	if s.fallback() != std {
		t.Fatal("standard server identity lost")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-shutdown:
	case <-time.After(5 * time.Second):
		t.Fatal("registered callback did not run")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := s.Serve(l); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Shutdown: %v", err)
	}
}

func TestEngineInUse(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "engine") })}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	defer func() { s.Close(); l.Close(); <-done }()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || string(p) != "engine" {
		t.Fatalf("body=%q err=%v", p, err)
	}
	if s.engineKind() == "std" {
		t.Fatal("plaintext TCP still delegates to net/http; N3 engine required")
	}
}
