package sched

import (
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// TestSequentialIdleWaitsShareTimers pins the pool's timer-sharing contract:
// the pool's own free list (not a sync.Pool cache) carries idle timers
// between waits, so sequential idle waits allocate exactly one timer. The
// assertion is deterministic and holds under -race.
func TestSequentialIdleWaitsShareTimers(t *testing.T) {
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = prevRate })

	synctest.Test(t, func(t *testing.T) {
		const extra = 32
		p := New(time.Hour)
		defer p.Close()

		before := poolRunTimerAllocBytes()
		idleOnce(t, p)
		first := poolRunTimerAllocBytes() - before
		if first == 0 {
			t.Fatal("first idle wait allocated no Pool.run timer bytes; sharing guard is blind")
		}
		for range extra {
			idleOnce(t, p)
		}
		// The free list guarantees reuse: no later wait may arm a timer the
		// pool does not already hold.
		if extraBytes := poolRunTimerAllocBytes() - before - first; extraBytes != 0 {
			t.Fatalf("idle waits allocated %d more Pool.run timer bytes, want 0 (first=%d waits=%d)", extraBytes, first, extra)
		}
		// Fire the idle timer so the worker expires and drains the pool.
		time.Sleep(time.Hour + time.Second)
		synctest.Wait()
		if n := p.Workers(); n != 0 {
			t.Fatalf("expired idle worker retained: %d", n)
		}
	})
}

func idleOnce(t *testing.T, p *Pool) {
	t.Helper()
	done := make(chan struct{})
	if !p.Submit(func() { close(done) }) {
		t.Fatal("task rejected")
	}
	<-done
	synctest.Wait()
	if n := p.Workers(); n != 1 {
		t.Fatalf("workers=%d, want 1", n)
	}
}

func poolRunTimerAllocBytes() int64 {
	// MemProfile publishes allocs on a GC cycle, and the snapshot may lag
	// one more; two collections settle it. inuse=true keeps fully-freed
	// buckets in the profile - the runtime drops them when inuse=false -
	// so this cumulative sum only grows and deltas cannot go negative.
	runtime.GC()
	runtime.GC()
	var recs []runtime.MemProfileRecord
	for {
		n, ok := runtime.MemProfile(recs, true)
		if ok {
			recs = recs[:n]
			break
		}
		recs = make([]runtime.MemProfileRecord, n+16)
	}
	var sum int64
	for i := range recs {
		var run, timer bool
		frames := runtime.CallersFrames(recs[i].Stack())
		for {
			fr, more := frames.Next()
			if strings.Contains(fr.Function, "(*Pool).run") {
				run = true
			}
			if strings.Contains(fr.Function, "NewTimer") || strings.Contains(fr.Function, "newTimer") {
				timer = true
			}
			if !more {
				break
			}
		}
		if run && timer {
			sum += recs[i].AllocBytes
		}
	}
	return sum
}

// TestIdleTimerFreeListCapsAndClearsOnClose pins the free-list bound. A
// burst larger than the cap expires down to the cap, and Close leaves the
// list empty. The second bubble closes while workers still hold timers, so
// a release after Close must drop, not append. The expected bound is the
// literal 8, not a value read back from the production constant, and a
// separate check pins idleTimerFreeListCap to that same literal: raising
// the cap must edit the constant and this pin together.
// Sleep advances the synctest fake clock; it is not a wall-clock wait.
func TestIdleTimerFreeListCapsAndClearsOnClose(t *testing.T) {
	if idleTimerFreeListCap != 8 {
		t.Fatalf("idleTimerFreeListCap=%d, want 8 (raise the literal below with the constant)", idleTimerFreeListCap)
	}
	const burst = idleTimerFreeListCap + 8

	t.Run("expiry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := New(time.Hour)
			defer p.Close()
			submitIdleBurst(t, p, burst)
			if got := p.Workers(); got != burst {
				t.Fatalf("idle workers=%d, want %d", got, burst)
			}
			time.Sleep(time.Hour + time.Second)
			synctest.Wait()
			if got := p.Workers(); got != 0 {
				t.Fatalf("expired idle workers retained: %d", got)
			}
			if got := idleTimerFreeListLen(p); got != 8 {
				t.Fatalf("free list len=%d after %d concurrent expiries, want 8", got, burst)
			}
			p.Close()
			if got := idleTimerFreeListLen(p); got != 0 {
				t.Fatalf("free list len=%d after Close, want 0", got)
			}
		})
	})

	t.Run("release after Close", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := New(time.Hour)
			defer p.Close()
			submitIdleBurst(t, p, burst)
			if got := p.Workers(); got != burst {
				t.Fatalf("idle workers=%d, want %d", got, burst)
			}
			p.Close()
			if got := idleTimerFreeListLen(p); got != 0 {
				t.Fatalf("release after Close left %d timers, want 0", got)
			}
		})
	})
}

func submitIdleBurst(t *testing.T, p *Pool, n int) {
	t.Helper()
	// Park every task until all have entered. A tight Submit loop otherwise
	// reuses workers that already went idle, so the burst is not concurrent.
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	for range n {
		if !p.Submit(func() {
			entered <- struct{}{}
			<-release
		}) {
			t.Fatal("task rejected")
		}
	}
	for range n {
		<-entered
	}
	close(release)
	synctest.Wait()
}

func idleTimerFreeListLen(p *Pool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.timers)
}
