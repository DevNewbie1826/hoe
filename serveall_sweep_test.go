package hoe

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

// dialSweep dials addr and closes the connection at once, queuing one
// backlog entry for the sweep race.
func dialSweep(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
	if err != nil {
		return err
	}
	return c.Close()
}

// F1 regression (adversarial finding): a connection whose accept returns
// from a listener's backlog while StopListening's sweep ends that listener
// must still carry the listener's BaseContext, not context.Background().
// The sweep ends reserved-but-unserved listeners directly (state moved to
// retired, fd closed) while their accept loop's first AcceptTCP is already
// returning a queued backlog connection; adopt must still find the state.
//
// Deterministic shape: the last listener's BaseContext parks ServeAll after
// every listener is reserved but before any accept loop starts, so a
// pre-dialed burst queues in every backlog. Releasing the park concurrent
// with Shutdown makes StopListening's serving snapshot race the freshly
// spawned accept loops — the only window where a listener loses its state
// while a backlog accept is in flight — while a short-dial storm keeps
// fresh backlog landing through the sweep. ConnContext observes every
// adopted connection's context: none may arrive without its listener's
// base context value.
func TestServeAllSweepKeepsBaseContext(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	var checkedTotal, missingTotal int64
	for round := range sweepRounds {
		checked, missing := sweepRound(t, round)
		checkedTotal += checked
		missingTotal += missing
	}
	if checkedTotal == 0 {
		t.Fatal("no connection contexts observed")
	}
	if missingTotal != 0 {
		t.Fatalf("%d of %d connections lost their listener's base context", missingTotal, checkedTotal)
	}
}

// Sweep shape parameters: listeners spread the StopListening sweep wide
// enough to overlap the accept-loop spawn burst; each round pre-queues
// burst backlog dials per listener and runs stormLoops short-dial lanes
// until Shutdown returns.
const (
	sweepListeners  = 48
	sweepRounds     = 50
	sweepBurst      = 8
	sweepStormLoops = 64
)

// sweepRound runs one park-release-sweep lifecycle on a fresh Server with
// fresh listeners and reports its ConnContext observations.
func sweepRound(t *testing.T, round int) (checked, missing int64) {
	t.Helper()
	var checkedN, missingN atomic.Int64
	ls := make([]net.Listener, 0, sweepListeners)
	for range sweepListeners {
		ls = append(ls, listenTCP(t))
	}
	// Park the last listener's BaseContext: ServeAll reserves listeners in
	// order, so once the last one parks, every listener is registered and
	// no accept loop exists yet.
	entered := make(chan struct{})
	release := make(chan struct{})
	parked := ls[len(ls)-1]
	s := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		BaseContext: func(l net.Listener) context.Context {
			if l == parked {
				close(entered)
				<-release
			}
			return context.WithValue(context.Background(), ctxKey{}, l.Addr().String())
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			checkedN.Add(1)
			if _, ok := ctx.Value(ctxKey{}).(string); !ok {
				missingN.Add(1)
			}
			return ctx
		},
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- s.ServeAll(ls...) }()
	select {
	case <-entered:
	case err := <-serveErr:
		t.Fatalf("round %d: ServeAll ended before the park: %v", round, err)
	case <-time.After(5 * time.Second):
		close(release)
		s.Close()
		t.Fatalf("round %d: parked BaseContext never ran", round)
	}
	// Queue backlog on every listener while no accept loop can drain it;
	// best effort, since the sweep race needs no fixed backlog depth.
	var burstWG sync.WaitGroup
	for _, l := range ls {
		for range sweepBurst {
			burstWG.Add(1)
			go func(addr string) {
				defer burstWG.Done()
				dialSweep(addr)
			}(l.Addr().String())
		}
	}
	burstWG.Wait()
	// Release the park concurrent with Shutdown; the dial storm keeps fresh
	// backlog arriving while the sweep ends listeners.
	close(release)
	shutdownErr := make(chan error, 1)
	shutdownReturned := make(chan struct{})
	go func() {
		defer close(shutdownReturned)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownErr <- s.Shutdown(ctx)
	}()
	stormDone := make(chan struct{})
	var dialErrs atomic.Int64
	go func() {
		defer close(stormDone)
		var wg sync.WaitGroup
		for i := range sweepStormLoops {
			wg.Add(1)
			go func(l net.Listener) {
				defer wg.Done()
				for {
					select {
					case <-shutdownReturned:
						return
					default:
					}
					if err := dialSweep(l.Addr().String()); err != nil {
						dialErrs.Add(1) // listener closed: this dialer is done
						return
					}
				}
			}(ls[i%len(ls)])
		}
		wg.Wait()
	}()
	select {
	case err := <-shutdownErr:
		if err != nil {
			t.Fatalf("round %d: Shutdown = %v", round, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("round %d: Shutdown never returned", round)
	}
	select {
	case <-stormDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("round %d: dial storm never drained", round)
	}
	select {
	case err := <-serveErr:
		if !errors.Is(err, ErrServerClosed) {
			t.Fatalf("round %d: ServeAll = %v, want ErrServerClosed", round, err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("round %d: ServeAll never returned after Shutdown", round)
	}
	return checkedN.Load(), missingN.Load()
}
