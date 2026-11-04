package sched

import (
	"testing"
	"time"
)

// The blocked-hold allocation shape: a worker spawned for a task holds no
// idle timer while the task blocks. Timers arm only at a real idle wait,
// so the count-verified Pool.run timer mass (pool.go time.NewTimer rows in
// the u1 n-map) cannot exist at the hold.
func TestBlockedTaskHoldsNoIdleTimer(t *testing.T) {
	base := idleTimersCreated.Load()
	p := New(time.Minute)
	entered := make(chan struct{}, 8)
	release := make(chan struct{})
	for range 8 {
		if !p.Submit(func() { entered <- struct{}{}; <-release }) {
			t.Fatal("task rejected")
		}
	}
	for range 8 {
		await(t, entered)
	}
	if n := idleTimersCreated.Load() - base; n != 0 {
		t.Fatalf("idle timers created while 8 tasks blocked: %d", n)
	}
	close(release)
	p.Close()
	// Workers finishing into a closed pool exit without arming; any that
	// idled before Close share at most one timer per worker.
	if n := idleTimersCreated.Load() - base; n > 8 {
		t.Fatalf("idle timers created=%d exceeds the 8 workers", n)
	}
}
