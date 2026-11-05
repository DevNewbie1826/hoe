package sched

import (
	"runtime"
	"testing"
	"testing/synctest"
	"time"
)

// TestBlockedWorkersRetainNoTimer pins the hold shape the worker-timer mass
// came from: a worker blocked inside a task retains no timer allocated by
// Pool.run, on either path a worker reaches a task. The old run loop armed
// time.NewTimer before the task. Four pre-warmed workers park on an idle
// timer and take the task through the handoff branch, while four fresh
// workers spawn for it, so a timer held across a task lands in the delta
// whether it spawned the worker or was handed to a parked one. A trailing
// idle-wait probe then pins that the handoff workers released their parked
// timers into the free list rather than holding them: the probe's own park
// must reuse one, so it allocates nothing.
func TestBlockedWorkersRetainNoTimer(t *testing.T) {
	prevRate := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = prevRate })

	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		p := New(time.Minute)
		t.Cleanup(func() {
			close(release)
			p.Close()
		})

		// Pre-warm four workers. Every task blocks until all four have
		// entered, so no Submit can reuse a worker that already returned and
		// re-parked; while the gate is closed no worker is idle mid-loop, so
		// each Submit must spawn its own worker. Once the gate opens and the
		// workers park idle with armed timers, those allocations land before
		// the baseline and the four workers are available for handoff.
		warmEntered := make(chan struct{}, 4)
		warmGate := make(chan struct{})
		for range 4 {
			if !p.Submit(func() {
				warmEntered <- struct{}{}
				<-warmGate
			}) {
				t.Fatal("task rejected")
			}
		}
		for range 4 {
			await(t, warmEntered)
		}
		close(warmGate)
		synctest.Wait()
		if n := p.Workers(); n != 4 {
			t.Fatalf("pre-warmed workers=%d, want 4", n)
		}

		// Delta around the operation only, on the cumulative allocation
		// profile: fully-freed buckets stay in the profile, frees cannot
		// shrink it, and no sched test leaves a worker running (pools join
		// on Close; none detaches), so only this window can move the delta.
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

		// Probe the free list with one short task on top of the eight blocked
		// ones. Its worker parks when the task returns, and that idle wait
		// reuses a timer the handoff workers released before running their
		// blocking tasks. A hold across a task - newly armed or reused -
		// drains the free list, so the probe park has to call NewTimer and
		// the delta grows even though the blocking tasks allocated nothing.
		probeDone := make(chan struct{})
		if !p.Submit(func() { close(probeDone) }) {
			t.Fatal("task rejected")
		}
		await(t, probeDone)
		synctest.Wait()

		if got := poolRunTimerAllocBytes() - before; got != 0 {
			t.Fatalf("blocked workers allocated %d Pool.run timer bytes, want 0", got)
		}
	})
}
