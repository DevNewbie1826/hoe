package goroutinebudget

import (
	"strings"
	"testing"
	"time"
)

func withStacks(t *testing.T, pages ...string) {
	t.Helper()
	restore := readStacks
	t.Cleanup(func() { readStacks = restore })
	i := 0
	readStacks = func() []byte {
		p := pages[i]
		if i < len(pages)-1 {
			i++
		}
		return []byte(p)
	}
}

func userGoroutine(fn string) string {
	return "goroutine 7 [select]:\n" + fn + "\n\t/repo/file.go:10 +0x1\n\n"
}

func parkedWorker() string {
	return userGoroutine("github.com/DevNewbie1826/hoe/internal/sched.(*Pool).run(0x1, 0x2, 0x3)")
}

func runtimeGoroutine() string {
	return "goroutine 9 [GC worker (idle)]:\nruntime.gcBgMarkWorker()\n\t/usr/local/go/src/runtime/mgc.go:1280 +0x50\nruntime.goexit()\n\t/usr/local/go/src/runtime/asm_arm64.s:1 +0x4\n\n"
}

// TestParseExcludesRuntimeGoroutines pins the classification that matches
// runtime.NumGoroutine: a stack made only of runtime frames is not user work.
func TestParseExcludesRuntimeGoroutines(t *testing.T) {
	p := parse([]byte(userGoroutine("main.serve(0x0)") + runtimeGoroutine()))
	if p.user != 1 {
		t.Fatalf("user=%d, want 1", p.user)
	}
	if p.parked != 0 {
		t.Fatalf("parked=%d, want 0", p.parked)
	}
}

// TestParseCountsParkedElasticWorkers pins that a parked worker is counted as
// live capacity, not silently dropped.
func TestParseCountsParkedElasticWorkers(t *testing.T) {
	p := parse([]byte(userGoroutine("main.serve(0x0)") + parkedWorker() + runtimeGoroutine()))
	if p.user != 2 {
		t.Fatalf("user=%d, want 2", p.user)
	}
	if p.parked != 1 {
		t.Fatalf("parked=%d, want 1", p.parked)
	}
}

// TestSettleWaitsForParkedWorkersToDrain pins the drain condition: a stable
// count that still holds a parked worker is not settled.
func TestSettleWaitsForParkedWorkersToDrain(t *testing.T) {
	withStacks(t,
		userGoroutine("main.serve(0x0)")+parkedWorker(),
		userGoroutine("main.serve(0x0)")+parkedWorker(),
		userGoroutine("main.serve(0x0)"),
		userGoroutine("main.serve(0x0)"),
	)
	n, ok := Settle(time.Now().Add(5 * time.Second))
	if !ok || n != 1 {
		t.Fatalf("settle=(%d,%t), want (1,true)", n, ok)
	}
}

// TestSettleIgnoresMovingCount pins that a count still changing is not
// reported as the resting value.
func TestSettleIgnoresMovingCount(t *testing.T) {
	withStacks(t,
		userGoroutine("main.serve(0x0)")+parkedWorker(),
		userGoroutine("main.serve(0x0)")+userGoroutine("main.other(0x0)")+parkedWorker(),
		userGoroutine("main.serve(0x0)")+userGoroutine("main.other(0x0)"),
		userGoroutine("main.serve(0x0)")+userGoroutine("main.other(0x0)"),
	)
	n, ok := Settle(time.Now().Add(5 * time.Second))
	if !ok || n != 2 {
		t.Fatalf("settle=(%d,%t), want (2,true)", n, ok)
	}
}

// TestSettleGivesUpOnParkedWorker pins the bounded wait: a worker that never
// drains fails the census instead of passing as settled.
func TestSettleGivesUpOnParkedWorker(t *testing.T) {
	withStacks(t, userGoroutine("main.serve(0x0)")+parkedWorker())
	if n, ok := Settle(time.Now().Add(50 * time.Millisecond)); ok {
		t.Fatalf("settle accepted a parked worker: %d", n)
	}
}

// TestFloorReadsTheLiveRuntime exercises the real probe, so a broken read
// fails here rather than inside a conformance leak budget.
func TestFloorReadsTheLiveRuntime(t *testing.T) {
	if n := Floor(t); n < 1 {
		t.Fatalf("floor=%d, want at least the running goroutine", n)
	}
}

func TestParkedReadsTheLiveRuntime(t *testing.T) {
	if n := Parked(); n < 0 {
		t.Fatalf("parked=%d, want a non-negative count", n)
	}
}

var _ = strings.TrimSpace
