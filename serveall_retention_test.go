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

// serveAllPayloadKey carries one lifecycle's heap payload through BaseContext.
type serveAllPayloadKey struct{}

// TestServeAllRetainedStateReleased drives repeated one-listener ServeAll
// lifecycles on one Server, each carrying a 1 MiB BaseContext payload, and
// requires every payload to be collected once the lifecycles have ended: a
// finished engine must drop its retired listener states and the server must
// drop finished engines. Release is observed with runtime.AddCleanup
// (queue-and-signal): one forced GC enqueues the cleanups and the counter is
// awaited on a channel under a deadline, the same wait the runtime's own
// object-death tests use. No fixed sleeps.
func TestServeAllRetainedStateReleased(t *testing.T) {
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
			return context.WithValue(context.Background(), serveAllPayloadKey{}, payload)
		},
	}
	tr := &http.Transport{DisableKeepAlives: true}
	defer tr.CloseIdleConnections()
	for i := 0; i < lifecycles; i++ {
		l := listenTCP(t)
		served := make(chan error, 1)
		go func() { served <- s.ServeAll(l) }()
		postEcho(t, tr, l.Addr().String(), "ready")
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		// ServeAll returns only after every listener's deferred endListener
		// ran, so the lifecycle's engine state is settled at this point.
		if err := <-served; err != nil && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("lifecycle %d ServeAll: %v", i, err)
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
	waitUntil(t, "server engine prune", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.engines) == 0
	})
}
