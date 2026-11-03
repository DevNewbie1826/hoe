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

func TestAdoptFailureBalancesOpening(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := New(&http.Server{}, listener, Policy{})
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
		if err := engine.group.AdoptAsync(-1, func(error) {
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
	go func() { queued <- engine.adopt(-1) }()
	select {
	case err := <-queued:
		if err != nil {
			t.Fatalf("enqueue adoption: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("adoption waited for the destination reactor")
	}
	releaseOnce.Do(func() { close(release) })

	// The invalid descriptor reaches the real poller registration failure path.
	served := make(chan error, 1)
	go func() { served <- engine.Serve(listener) }()
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
