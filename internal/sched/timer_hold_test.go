package sched

import (
	"runtime"
	"strings"
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
	// Delta, not an absolute zero: timers allocated by earlier tests stay in
	// the in-use profile across one GC (sync.Pool victim cache; under -race a
	// dropped Put). Publish until that baseline stops moving, then require
	// these blocked workers to add none.
	before := runTimerBytes()
	for range 4 {
		next := runTimerBytes()
		if next == before {
			break
		}
		before = next
	}
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
	after := runTimerBytes()
	if got := before - after; got != 0 {
		t.Fatalf("blocked workers retain %d bytes allocated by Pool.run, want 0", after-before)
	}
}

func runTimerBytes() int64 {
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
		if recs[i].InUseBytes() == 0 {
			continue
		}
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
			sum += recs[i].InUseBytes()
		}
	}
	return sum
}
