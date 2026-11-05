// Package goroutinebudget counts the goroutines a test may attribute to its
// own workload.
package goroutinebudget

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// elasticWorkerFrame identifies a worker parked between tasks in the engine's
// elastic pool. The pool keeps such a worker until its idle timer fires, so a
// sample taken while one is parked counts capacity that is already on its way
// out. A worker that never expires is a leak, so the census waits for the
// parked set to empty instead of discounting it.
const elasticWorkerFrame = "internal/sched.(*Pool).run"

// readStacks is a seam: a test drives the census from synthetic stacks
// instead of racing the scheduler.
var readStacks = func() []byte {
	size := 1 << 20
	for {
		buf := make([]byte, size)
		if n := runtime.Stack(buf, true); n < len(buf) {
			return buf[:n]
		}
		size *= 2
	}
}

// page is one consistent snapshot: the number of user goroutines, and how
// many of them are elastic workers parked between tasks.
type page struct {
	user   int
	parked int
}

// parse counts goroutines in one snapshot. A goroutine whose every frame is
// in package runtime is excluded, matching how runtime.NumGoroutine leaves
// out GC workers, the sweeper and the scavenger. runtime.Stack stops the
// world for the call, so the snapshot is internally consistent, unlike
// gcount, which sums counters that other goroutines mutate concurrently.
func parse(stacks []byte) page {
	var p page
	for _, block := range strings.Split(string(stacks), "\n\n") {
		lines := strings.Split(block, "\n")
		if len(lines) < 2 || !strings.HasPrefix(lines[0], "goroutine ") {
			continue
		}
		runtimeOwned, innermost := true, ""
		for _, line := range lines[1:] {
			if strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "created by ") {
				continue
			}
			if innermost == "" {
				innermost = line
			}
			if !strings.HasPrefix(line, "runtime.") {
				runtimeOwned = false
			}
		}
		if runtimeOwned {
			continue
		}
		p.user++
		if strings.Contains(innermost, elasticWorkerFrame) {
			p.parked++
		}
	}
	return p
}

// Settle returns the user goroutine count once no elastic worker is parked
// and two consecutive snapshots agree, or ok=false at the deadline. Waiting
// for the parked set to empty is the exact drain condition; requiring equal
// snapshots rules out a goroutine starting or finishing between scans.
// Nothing sleeps in place of an event.
func Settle(deadline time.Time) (int, bool) {
	previous, _ := snapshot()
	for {
		current, parked := snapshot()
		if current == previous && parked == 0 {
			return current, true
		}
		if time.Now().After(deadline) {
			return 0, false
		}
		previous = current
		// Each snapshot stops the world, so yield between attempts: a parked
		// worker frees itself from a timer callback that never runs while
		// this loop holds the processor.
		runtime.Gosched()
	}
}

func snapshot() (int, int) {
	p := parse(readStacks())
	return p.user, p.parked
}

// Floor is the settled user goroutine count, bounded by a ten-second deadline.
func Floor(t testing.TB) int {
	t.Helper()
	n, ok := Settle(time.Now().Add(10 * time.Second))
	if !ok {
		t.Fatal("goroutine census did not settle: an elastic worker stayed parked or the count kept moving")
	}
	return n
}

// Parked reports how many elastic workers are parked between tasks.
func Parked() int {
	_, parked := snapshot()
	return parked
}
