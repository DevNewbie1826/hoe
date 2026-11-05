package hoe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

type baseMarkerKey struct{}

func baseMarker(marker string) func(net.Listener) context.Context {
	return func(net.Listener) context.Context {
		return context.WithValue(context.Background(), baseMarkerKey{}, marker)
	}
}

// Deadlines bound the request so a stalled serving path fails fast rather
// than hanging the test.
func baseMarkerRequest(t *testing.T, network, addr string) string {
	t.Helper()
	c, err := net.Dial(network, addr)
	if err != nil {
		t.Fatalf("dial %s %s: %v", network, addr, err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()
	p, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(p)
}

// baseContextPerCall requires each listener to observe the hook current when
// its own Serve started: l2 sees v2, while the already-started l1 keeps v1.
func baseContextPerCall(t *testing.T, network string, l1, l2 net.Listener) {
	t.Helper()
	firstServed := make(chan struct{})
	var first sync.Once
	s := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			marker, _ := r.Context().Value(baseMarkerKey{}).(string)
			fmt.Fprint(w, marker)
			first.Do(func() { close(firstServed) })
		}),
		BaseContext: baseMarker("one"),
	}
	t.Cleanup(func() {
		s.Close()
		l1.Close()
		l2.Close()
	})

	serve1 := make(chan error, 1)
	go func() { serve1 <- s.Serve(l1) }()
	if got := baseMarkerRequest(t, network, l1.Addr().String()); got != "one" {
		t.Fatalf("first Serve request marker = %q, want %q", got, "one")
	}
	// Reassign only after the first Serve read its hook; concurrent assignment
	// is outside the supported contract.
	select {
	case <-firstServed:
	case <-time.After(5 * time.Second):
		t.Fatal("first request never reached its handler")
	}

	s.BaseContext = baseMarker("two")
	serve2 := make(chan error, 1)
	go func() { serve2 <- s.Serve(l2) }()
	if got := baseMarkerRequest(t, network, l2.Addr().String()); got != "two" {
		t.Fatalf("second Serve request marker = %q, want %q: BaseContext must be read when each serving call starts", got, "two")
	}
	// The already-running Serve read its hook at its own start; the later
	// assignment must not retroactively change it.
	if got := baseMarkerRequest(t, network, l1.Addr().String()); got != "one" {
		t.Fatalf("already-started Serve request marker = %q, want %q: its BaseContext was read at Serve start", got, "one")
	}

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, ch := range []chan error{serve1, serve2} {
		select {
		case err := <-ch:
			if !errors.Is(err, ErrServerClosed) {
				t.Fatalf("Serve %d = %v, want ErrServerClosed", i+1, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Serve %d did not return after Close", i+1)
		}
	}
}

// TestServeReadsBaseContextPerCall pins the net/http parity contract that
// BaseContext is read when each serving call starts, per listener, rather
// than snapshotted once per Server. It fails when a later Serve keeps using
// the hook assigned before the first Serve.
func TestServeReadsBaseContextPerCall(t *testing.T) {
	t.Run("tcp-engine", func(t *testing.T) {
		if !conn.Supported() {
			t.Skip("documented standard-library fallback platform")
		}
		baseContextPerCall(t, "tcp", listenTCP(t), listenTCP(t))
	})

	t.Run("unix-std", func(t *testing.T) {
		dir := t.TempDir()
		l1, err := net.Listen("unix", filepath.Join(dir, "one.sock"))
		if err != nil {
			t.Skipf("unix sockets unavailable: %v", err)
		}
		l2, err := net.Listen("unix", filepath.Join(dir, "two.sock"))
		if err != nil {
			l1.Close()
			t.Skipf("unix sockets unavailable: %v", err)
		}
		baseContextPerCall(t, "unix", l1, l2)
	})
}
