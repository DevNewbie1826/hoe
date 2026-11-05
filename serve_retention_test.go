package hoe

import (
	"context"
	"errors"
	"net"
	"net/http"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
	"github.com/DevNewbie1826/hoe/internal/loop"
)

// servePayloadKey carries one lifecycle's heap payload through BaseContext.
type servePayloadKey struct{}

// TestServeRetainedStateReleased drives repeated one-listener Serve
// lifecycles on one Server, each carrying a 1 MiB BaseContext payload, and
// requires the server to drop every finished engine once the lifecycles have
// ended: Serve returns after its deferred endListener ran, so the engine's
// resources are settled at that point. Release is observed with
// runtime.AddCleanup (queue-and-signal): one forced GC enqueues the cleanups
// and the counter is awaited on a channel under a deadline, the same wait the
// runtime's own object-death tests use. No fixed sleeps.
func TestServeRetainedStateReleased(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	prev := runtime.GOMAXPROCS(loop.DefaultLoops())
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	const (
		lifecycles = 40
		payloadB   = 1 << 20
	)
	var released atomic.Int64
	releaseSignal := make(chan struct{}, 1)
	s := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		BaseContext: func(l net.Listener) context.Context {
			payload := new([payloadB]byte)
			runtime.AddCleanup(payload, func(struct{}) {
				if released.Add(1) == lifecycles {
					select {
					case releaseSignal <- struct{}{}:
					default:
					}
				}
			}, struct{}{})
			return context.WithValue(context.Background(), servePayloadKey{}, payload)
		},
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	for i := 0; i < lifecycles; i++ {
		l := listenTCP(t)
		served := make(chan error, 1)
		go func() { served <- s.Serve(l) }()
		postEcho(t, tr, l.Addr().String(), "ready")
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		// Serve returns only after its deferred endListener ran, so the
		// lifecycle's engine state is settled at this point.
		if err := <-served; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("lifecycle %d Serve: %v", i, err)
		}
		waitUntil(t, "engine release", func() bool {
			return conn.SnapshotResources().Engines == 0
		})
	}
	runtime.GC()
	deadline := time.After(5 * time.Second)
	for {
		if n := released.Load(); n == lifecycles {
			break
		} else if n > lifecycles {
			t.Fatalf("payload cleanups fired %d times, want %d", n, lifecycles)
		}
		select {
		case <-releaseSignal:
		case <-deadline:
			t.Fatalf("%d of %d lifecycle payloads released; %d retained through finished engines", released.Load(), lifecycles, lifecycles-released.Load())
		}
		runtime.GC()
	}
	// The server keeps no finished engines: the registry must not pin them.
	pruneDeadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		n := len(s.engines)
		s.mu.Unlock()
		if n == 0 {
			return
		}
		if time.Now().After(pruneDeadline) {
			t.Fatalf("%d Serve lifecycles left %d finished engines in Server.engines", lifecycles, n)
		}
		time.Sleep(time.Millisecond)
	}
}
