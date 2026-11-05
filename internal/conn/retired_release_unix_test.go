//go:build linux || darwin

package conn

import (
	"errors"
	"net"
	"net/http"
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
