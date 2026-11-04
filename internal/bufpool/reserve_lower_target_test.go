package bufpool

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// waitForEpoch advances the collection epoch by n and reports whether it
// got there before a 5-second deadline. Unlike gcAndWait it never fails
// the test, so hammer workers may call it off the test goroutine.
func waitForEpoch(n int64) bool {
	want := gcEpoch.Load() + n
	deadline := time.Now().Add(5 * time.Second)
	for gcEpoch.Load() < want {
		if time.Now().After(deadline) {
			return false
		}
		runtime.GC()
		runtime.Gosched()
	}
	return true
}

// TestSettleLowerTargetNeverExposesNilSlots pins the panic the adversarial
// lane reported: when a collection lowers a class's target while more
// buffers are on loan than the reserve holds, the reserve slice must never
// grow past its length. Fill class 4096 until its target reaches 16,
// return everything, notice a collection, drain 12 of the 16 slots
// (len(reserve)=4, hw=12), notice the next collection — settle lowers the
// target 16 -> 12 — then borrow again. The settle once resliced the
// reserve to 12 here, exposing eight nil slots, and the Get panicked
// slicing one; the borrow must instead return a real buffer and every
// remaining reserve slot must be non-nil.
func TestSettleLowerTargetNeverExposesNilSlots(t *testing.T) {
	const size = 4096
	fill := make([][]byte, 16)
	for i := range fill {
		fill[i] = Get(size)
		if len(fill[i]) != size {
			t.Fatalf("fill %d: len=%d", i, len(fill[i]))
		}
	}
	for _, b := range fill {
		Put(b)
	}
	gcAndWait(t)
	held := make([][]byte, 12)
	for i := range held {
		held[i] = Get(size)
		if len(held[i]) != size {
			t.Fatalf("drain %d: len=%d", i, len(held[i]))
		}
	}
	gcAndWait(t)
	got := Get(size)
	if len(got) != size {
		t.Fatalf("Get after lowered target: len=%d", len(got))
	}
	got[0] = 73
	Put(got)
	idx := int(classOf[(size+7)>>3])
	c := &classes[idx]
	c.mu.Lock()
	for i := range c.reserve {
		if c.reserve[i] == nil {
			c.mu.Unlock()
			t.Fatalf("reserve slot %d is nil: len=%d cap=%d target=%d",
				i, len(c.reserve), cap(c.reserve), c.target)
		}
	}
	t.Logf("post-settle class state: target=%d reserve=%d cap=%d",
		c.target, len(c.reserve), cap(c.reserve))
	c.mu.Unlock()
	for _, b := range held {
		Put(b)
	}
}

// TestSettleLowerTargetNeverExposesNilSlotsHammer hammers the same path
// with a forced collection running — the conditions under which the
// adversarial lane hit the panic dozens of times per run: workers keep a
// class's loans above its held slots across settles. Every borrow must
// return a real, correctly sized buffer.
func TestSettleLowerTargetNeverExposesNilSlotsHammer(t *testing.T) {
	const (
		size    = 4096
		workers = 8
		rounds  = 25
	)
	stop := make(chan struct{})
	var collectors sync.WaitGroup
	collectors.Add(1)
	go func() {
		defer collectors.Done()
		for {
			select {
			case <-stop:
				return
			default:
				runtime.GC()
			}
		}
	}()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			hold := 4 + w%12
			for r := 0; r < rounds; r++ {
				if !waitForEpoch(1) {
					t.Error("gc epoch stalled")
					return
				}
				fill := make([][]byte, 16)
				for i := range fill {
					fill[i] = Get(size)
					if fill[i] == nil || len(fill[i]) != size {
						t.Errorf("worker %d round %d: bad fill borrow", w, r)
						return
					}
				}
				for _, b := range fill {
					Put(b)
				}
				if !waitForEpoch(1) {
					t.Error("gc epoch stalled")
					return
				}
				held := make([][]byte, hold)
				for i := range held {
					held[i] = Get(size)
					if held[i] == nil || len(held[i]) != size {
						t.Errorf("worker %d round %d: bad drain borrow", w, r)
						return
					}
					held[i][0] = 173
				}
				if !waitForEpoch(1) {
					t.Error("gc epoch stalled")
					return
				}
				got := Get(size)
				if got == nil || len(got) != size {
					t.Errorf("worker %d round %d: nil or short borrow after settle", w, r)
					return
				}
				got[0] = 173
				Put(got)
				for _, b := range held {
					Put(b)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	collectors.Wait()
}
