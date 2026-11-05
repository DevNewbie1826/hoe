//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"
)

// TestEngineRetiredClearedAfterClose holds the engine's end-of-life
// contract: once Close has finalized the engine, no retired listener state
// stays reachable, so ended listeners' base contexts cannot outlive the
// engine through the retired map. Serve returns only after its deferred
// endListener ran, so the map's post-close state is settled when Serve
// has returned.
func TestEngineRetiredClearedAfterClose(t *testing.T) {
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(&http.Server{}, Policy{})
	if err != nil {
		l.Close()
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- e.Serve(l) }()
	awaitRegistered(t, e, l)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("Serve after Close: %v", err)
	}
	select {
	case <-e.finalized:
	case <-time.After(5 * time.Second):
		t.Fatal("engine resources not finalized")
	}
	e.mu.Lock()
	retired := len(e.retired)
	e.mu.Unlock()
	if retired != 0 {
		t.Fatalf("engine keeps %d retired listener states after close", retired)
	}
}

// retiredMarkerKey tags a listener's base context so a test can tell the
// retired listener's context from a freshly constructed background one.
type retiredMarkerKey struct{}

// TestAdoptCarriesRetiredBaseContext drives the retired fallback in adopt
// deterministically: the listener is ended (its state moves to the retired
// map) before an adoption arrives for it, which is the exact order the
// backlog-accept race produces. The adopted connection must carry the ended
// listener's base context, not a background one, while the engine serves.
// Removing the retired fallback in adopt makes this test fail.
func TestAdoptCarriesRetiredBaseContext(t *testing.T) {
	const marker = "retired-listener-base"
	e, err := New(&http.Server{
		BaseContext: func(net.Listener) context.Context {
			return context.WithValue(context.Background(), retiredMarkerKey{}, marker)
		},
	}, Policy{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	// A second listener keeps the engine serving, so ending the first
	// retires its state without finalizing the engine's resources.
	keep, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		l.Close()
		t.Fatal(err)
	}
	defer keep.Close()
	served := make(chan error, 1)
	go func() { served <- e.Serve(l) }()
	go func() { served <- e.Serve(keep) }()
	awaitRegistered(t, e, l)
	awaitRegistered(t, e, keep)

	// End the first listener: its state retires carrying the marker context,
	// and the engine keeps serving the second listener.
	e.endListener(l)

	// A backlog adoption for the ended listener arrives as a dup of a live
	// connected TCP socket, exactly what the accept loop hands to adopt.
	peer, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	live, err := net.DialTCP("tcp", nil, peer.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := live.SyscallConn()
	if err != nil {
		live.Close()
		t.Fatal(err)
	}
	var fd int
	raw.Control(func(s uintptr) { fd, err = syscall.Dup(int(s)) })
	live.Close()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { syscall.Close(fd) })
	if err := e.adopt(fd, l); err != nil {
		t.Fatalf("adopt after listener end: %v", err)
	}

	// The engine registers the socket asynchronously; wait for the
	// connection to exist, then read the base it was adopted with.
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.Lock()
		var base context.Context
		for _, c := range e.conns {
			base = c.base
		}
		e.mu.Unlock()
		if base != nil {
			if got, _ := base.Value(retiredMarkerKey{}).(string); got != marker {
				t.Fatalf("adopted base context carries %q, want the retired listener's %q", got, marker)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("adoption never registered a connection")
		}
		time.Sleep(time.Millisecond)
	}
}
