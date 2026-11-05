//go:build linux

package poll

import (
	"testing"
)

// TestWaitSteadyStateAllocations pins the cost of one wait with readiness
// already published. A caller-visible allocation per wait is pure overhead in
// the reactor's hot loop, so the wait itself must not allocate.
//
// The device is deterministic: Wake publishes an eventfd counter and Wait
// consumes it through epoll and collect, so neither call can block, no
// goroutine is involved, and no timing assumption is made. The wake-and-drain
// cost is measured separately and subtracted, leaving the wait path alone.
// One thousand iterations keep a stray background allocation well inside the
// tolerance while a per-wait allocation (three on the pre-fix path) cannot fit.
func TestWaitSteadyStateAllocations(t *testing.T) {
	if raceEnabled {
		t.Skip("race instrumentation allocates on every poll operation")
	}
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	}()

	const runs = 1000
	drain := testing.AllocsPerRun(runs, func() {
		if err := p.Wake(); err != nil {
			t.Fatalf("wake: %v", err)
		}
		if err := p.collect(1); err != nil {
			t.Fatalf("drain: %v", err)
		}
	})
	full := testing.AllocsPerRun(runs, func() {
		if err := p.Wake(); err != nil {
			t.Fatalf("wake: %v", err)
		}
		if _, err := p.Wait(); err != nil {
			t.Fatalf("wait: %v", err)
		}
	})
	if got := full - drain; got > 0.1 {
		t.Fatalf("wait allocates %.2f per call, want 0 (wake+drain=%.2f full=%.2f)", got, drain, full)
	}
}

// BenchmarkWaitReady measures one publish-and-consume cycle on the wait path.
// Wake publishes readiness before every Wait so epoll returns without
// blocking, which keeps the number comparable across machines.
func BenchmarkWaitReady(b *testing.B) {
	p, err := New()
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := p.Close(); err != nil {
			b.Error(err)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := p.Wake(); err != nil {
			b.Fatal(err)
		}
		if _, err := p.Wait(); err != nil {
			b.Fatal(err)
		}
	}
}
