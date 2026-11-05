package sched

import (
	"runtime"
	"testing"
	"time"
)

// TestBlockedWorkersRetainNoTimer pins the hold shape the worker-timer mass
// came from: a worker blocked inside a task retains no timer allocated by
// Pool.run. The old run loop armed time.NewTimer before the task.
func TestBlockedWorkersRetainNoTimer(t *testing.T) {
	prev := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = prev })

	release := make(chan struct{})
	p := New(time.Minute)
	t.Cleanup(func() {
		close(release)
		p.Close()
	})
	// Delta around the operation only, on the cumulative allocation profile:
	// fully-freed buckets stay in the profile, frees cannot shrink it, and
	// no sched test leaves a worker running (pools join on Close; none
	// detaches), so only this window can move the delta.
	before := poolRunTimerAllocBytes()
	entered := make(chan struct{}, 8)
	for range 8 {
		if !p.Submit(func() {
			entered <- struct{}{}
			<-release
		}) {
			t.Fatal("task rejected")
		}
	}
	for range 8 {
		await(t, entered)
	}
	if got := poolRunTimerAllocBytes() - before; got != 0 {
		t.Fatalf("blocked workers allocated %d Pool.run timer bytes, want 0", got)
	}
}
