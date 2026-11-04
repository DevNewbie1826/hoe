//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/loop"
)

// awaitRegistered waits until Serve registered l in the engine, so a
// deliberate adoption failure attributes to that listener deterministically.
func awaitRegistered(t *testing.T, e *Engine, l *net.TCPListener) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		e.mu.Lock()
		ready := e.listeners[l] != nil
		e.mu.Unlock()
		if ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("Serve did not register its listener")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestAdoptFailureBalancesOpening(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(&http.Server{}, Policy{})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		engine.Close()
	})

	// Stall every reactor in a completion callback before starting adoption.
	// The invalid descriptors fail registration without creating connections.
	entered := make(chan struct{}, loop.DefaultLoops())
	for range loop.DefaultLoops() {
		if err := engine.group.AdoptAsync(-1, nil, func(error) {
			entered <- struct{}{}
			<-release
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range loop.DefaultLoops() {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("reactor did not reach barrier")
		}
	}

	// Adoption must return even though no reactor can complete registration.
	queued := make(chan error, 1)
	go func() { queued <- engine.adopt(-1, listener) }()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("enqueue adoption: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("adoption waited for the destination reactor")
	}

	// Serve registers the listener while every reactor is still stalled, so
	// the queued failure attributes to it once the reactors drain.
	served := make(chan error, 1)
	go func() { served <- engine.Serve(listener) }()
	awaitRegistered(t, engine, listener)
	releaseOnce.Do(func() { close(release) })

	// The invalid descriptor reaches the real poller registration failure path.
	select {
	case err := <-served:
		if !errors.Is(err, syscall.EBADF) {
			t.Fatalf("Serve returned %v, want registration error %v", err, syscall.EBADF)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not report registration failure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := engine.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown after failed registration: %v", err)
	}
	select {
	case <-engine.finalized:
	case <-time.After(5 * time.Second):
		t.Fatal("engine resources not finalized")
	}
}
