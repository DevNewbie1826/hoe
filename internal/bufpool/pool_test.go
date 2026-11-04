package bufpool

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestBufferAccounting(t *testing.T) {
	baseline := Outstanding()
	b := Get(1001)
	if Outstanding() != baseline+1 {
		t.Fatalf("borrowed=%d want %d", Outstanding(), baseline+1)
	}
	if len(b) != 1001 || cap(b) != 1024 {
		t.Fatalf("len/cap=%d/%d", len(b), cap(b))
	}
	Put(b)
	if Outstanding() != baseline {
		t.Fatalf("return leaked buffer: %d", Outstanding())
	}
}

func TestReserveSurvivesGC(t *testing.T) {
	b := Get(8192)
	b[0] = 73
	Put(b)
	before := Allocations()
	runtime.GC()
	runtime.GC()
	b = Get(8192)
	defer Put(b)
	if Allocations() != before {
		t.Fatal("GC discarded reserve: backing allocation required")
	}
	if b[0] != 73 {
		t.Fatal("borrow did not reuse reserved buffer")
	}
}

func TestOversizeAndEmpty(t *testing.T) {
	before := Outstanding()
	b := Get(100000)
	if len(b) != 100000 {
		t.Fatal("short buffer")
	}
	Put(b)
	if b := Get(0); b != nil {
		t.Fatal("empty borrow must be nil")
	}
	if Outstanding() != before {
		t.Fatal("accounting did not return to baseline")
	}
}

// gcAndWait ends the current garbage-collection epoch and waits until the
// tick that settles classes has advanced twice, so the next class operation
// runs against a fresh, already-settled epoch instead of a stale one.
func gcAndWait(tb testing.TB) {
	tb.Helper()
	want := gcEpoch.Load() + 2
	deadline := time.Now().Add(5 * time.Second)
	for gcEpoch.Load() < want {
		if time.Now().After(deadline) {
			tb.Fatalf("gc epoch stuck at %d, want %d", gcEpoch.Load(), want)
		}
		runtime.GC()
		runtime.Gosched()
	}
}

func borrowConcurrently(size, n int) [][]byte {
	bufs := make([][]byte, n)
	var start, done sync.WaitGroup
	start.Add(1)
	for i := range bufs {
		done.Add(1)
		go func(i int) {
			defer done.Done()
			start.Wait()
			bufs[i] = Get(size)
		}(i)
	}
	start.Done()
	done.Wait()
	return bufs
}

func release(bufs [][]byte) {
	for i := range bufs {
		Put(bufs[i])
		bufs[i] = nil
	}
}

func TestClassLookupMatchesSizeTable(t *testing.T) {
	for size := 1; size <= maxSize+1000; size++ {
		b := Get(size)
		if len(b) != size {
			t.Fatalf("size=%d len=%d", size, len(b))
		}
		if size > maxSize {
			if cap(b) != size {
				t.Fatalf("size=%d oversize cap=%d want %d", size, cap(b), size)
			}
		} else {
			want := 0
			for _, capacity := range sizes {
				if capacity >= size {
					want = capacity
					break
				}
			}
			if cap(b) != want {
				t.Fatalf("size=%d cap=%d want %d", size, cap(b), want)
			}
		}
		Put(b)
	}
	if b := Get(0); b != nil {
		t.Fatal("Get(0) must be nil")
	}
	if b := Get(-1); b != nil {
		t.Fatal("Get(-1) must be nil")
	}
}

func TestPutAcceptsChangedLength(t *testing.T) {
	const size = 200
	Put(Get(size))
	b := Get(size)
	b = b[:7]
	Put(b)
	before := Allocations()
	b = Get(size)
	if Allocations() != before {
		t.Fatal("buffer returned with a shorter length was not retained")
	}
	if len(b) != size || cap(b) != 208 {
		t.Fatalf("len/cap=%d/%d want %d/208", len(b), cap(b), size)
	}
	Put(b)
}

func TestOversizeNotRetained(t *testing.T) {
	size := maxSize + 100000
	b := Get(size)
	b[0] = 73
	Put(b)
	runtime.GC()
	runtime.GC()
	before := Allocations()
	b = Get(size)
	if Allocations() != before+1 {
		t.Fatal("oversize buffer was retained across collections")
	}
	if b[0] != 0 {
		t.Fatal("oversize borrow reused a retained buffer")
	}
	Put(b)
}

func TestReserveAdaptsToConcurrentDemand(t *testing.T) {
	const (
		size = 4096
		n    = 16
	)
	release(borrowConcurrently(size, n))
	gcAndWait(t)
	before := Allocations()
	bufs := borrowConcurrently(size, n)
	if got := Allocations() - before; got != 0 {
		t.Fatalf("reserve did not adapt to %d concurrent borrowers: %d backing allocations", n, got)
	}
	release(bufs)
}

func TestReserveGivesBackIdleDemand(t *testing.T) {
	const (
		size = 16384
		peak = 16
		next = 4
	)
	release(borrowConcurrently(size, peak))
	gcAndWait(t)
	for i := 0; i < peak+4; i++ {
		b := Get(size)
		Put(b)
	}
	gcAndWait(t)
	b := Get(size)
	Put(b)
	before := Allocations()
	bufs := borrowConcurrently(size, next)
	if got := Allocations() - before; got != next-1 {
		t.Fatalf("idle reserve kept slots: want %d backing allocations, got %d", next-1, got)
	}
	release(bufs)
}

func TestIdleClassKeepsOneReservedSlot(t *testing.T) {
	const size = 9472
	b := Get(size)
	gcAndWait(t)
	Put(b)
	gcAndWait(t)
	before := Allocations()
	b = Get(size)
	if Allocations() != before {
		t.Fatal("idle class lost its reserved slot across collections")
	}
	Put(b)
}

func TestReserveBudgetBoundsAggregate(t *testing.T) {
	const (
		size = 2048
		n    = 10
	)
	idx := int(classOf[(size+7)>>3])
	c := &classes[idx]
	// Repetitions in one process (-count) must see the same starting
	// state: refund the class's committed bytes and clear its adaptive
	// state so the budget below is derived from a fresh class, not from
	// whatever an earlier repetition of this test left behind.
	c.mu.Lock()
	committed.Add(int64(-c.target * size))
	c.target = 0
	c.reserve = nil
	c.hw = 0
	c.mu.Unlock()
	old := reserveBudget
	reserveBudget = committed.Load() + 4*size
	defer func() { reserveBudget = old }()
	release(borrowConcurrently(size, n))
	target := c.target
	if target >= n {
		t.Fatalf("reserve grew to %d slots past the %d-byte budget", target, reserveBudget)
	}
	if got := committed.Load(); got > reserveBudget {
		t.Fatalf("committed %d exceeds budget %d", got, reserveBudget)
	}
	gcAndWait(t)
	before := Allocations()
	bufs := borrowConcurrently(size, n)
	if got := Allocations() - before; got != uint64(n-target) {
		t.Fatalf("budget-bounded reserve served %d of %d: want %d backing allocations, got %d", n-int(got), n, n-target, got)
	}
	release(bufs)
}

func BenchmarkBufferReserveAcrossGC(b *testing.B) {
	const (
		size    = 16384
		slots   = 16
		gcEvery = 32
	)
	bufs := make([][]byte, slots)
	acquire := func() {
		for i := range bufs {
			bufs[i] = Get(size)
		}
	}
	releaseAll := func() {
		for i := range bufs {
			Put(bufs[i])
			bufs[i] = nil
		}
	}
	for i := 0; i < 2*gcEvery; i++ {
		acquire()
		releaseAll()
	}
	gcAndWait(b)
	before := Allocations()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		acquire()
		releaseAll()
		if i%gcEvery == gcEvery-1 {
			runtime.GC()
			runtime.GC()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(Allocations()-before)/float64(b.N), "backing_allocs/op")
}
