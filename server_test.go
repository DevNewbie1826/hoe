package hoe

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"testing"
)

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
	<-shutdown
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
	if os.Getenv("HOE_EXPECT_ENGINE") != "1" {
		t.Skip("N1 delegates; N3 must remove the opt-in guard")
	}
	s := &Server{}
	if s.engineKind() == "std" {
		t.Fatal("plaintext TCP still delegates to net/http; N3 engine required")
	}
}
