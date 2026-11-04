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
	// Publish frees before the next test reads in-use bytes.
	t.Cleanup(func() {
		runtime.GC()
		runtime.GC()
	})
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
		// Fire the idle timer. Stop leaves a heap zombie the next test still
		// counts as Pool.run in-use bytes.
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
	// MemProfile publishes allocs on a GC cycle.
	runtime.GC()
	var recs []runtime.MemProfileRecord
	for {
		n, ok := runtime.MemProfile(recs, false)
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
