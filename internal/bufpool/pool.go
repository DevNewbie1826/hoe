// Package bufpool loans byte buffers only for active I/O rounds.
package bufpool

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Go's small-object allocation classes, followed by page-sized I/O classes.
var sizes = [...]int{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768, 49152, 65536}

var maxSize = sizes[len(sizes)-1]

// classOf maps a size rounded up to the next multiple of 8 — every class
// capacity is one — to the first class covering it, so Get and Put resolve
// their class with one index instead of scanning the sizes table. It is
// derived from sizes at init, never maintained by hand.
var classOf = make([]uint8, (maxSize>>3)+1)

// gcEpoch counts garbage collections so a class can tell its sync.Pool tier
// was emptied. A collection is noticed through a finalizer tick: the tick
// stays unreachable until a collection frees it, which runs the finalizer,
// bumps the epoch and arms the next tick. Reading the epoch never takes a
// class lock.
var gcEpoch atomic.Int64

// collectionTick is 32 bytes on purpose, above the 16-byte tiny block: a
// tick packed with other tiny objects is freed only when everything sharing
// it is, and would silently never tick.
type collectionTick struct{ _ [32]byte }

func armTick() {
	runtime.SetFinalizer(&collectionTick{}, func(*collectionTick) {
		gcEpoch.Add(1)
		armTick()
	})
}

func init() {
	ci := 0
	for i := range classOf {
		for sizes[ci] < i<<3 {
			ci++
		}
		classOf[i] = uint8(ci)
	}
	armTick()
}

// reserveBudget bounds the bytes every class's reserve may hold together,
// beyond each touched class's first slot. It is a variable only so the tests
// can tighten it.
var reserveBudget int64 = 32 << 20

// committed is what the classes' targets add up to in bytes.
var committed atomic.Int64

type class struct {
	mu       sync.Mutex
	reserve  [][]byte // strong references the collector leaves alone
	target   int      // slots the reserve aims to hold; at least 1 once touched
	borrowed int      // buffers of this class currently on loan
	hw       int      // peak borrowed since the last collection was noticed
	epoch    int64    // gcEpoch when hw was last reset
	cache    sync.Pool
}

var classes [len(sizes)]class
var outstanding atomic.Int64
var allocations atomic.Uint64

// settleLocked notices a collection: the sync.Pool tier was just emptied, so
// the reserve keeps only what the previous epoch's peak demand justifies,
// with a floor of one slot for a touched class. Slots past the new target
// are given back to the collector.
func (c *class) settleLocked(capacity int, epoch int64) {
	t := c.hw
	if t > c.target {
		t = c.target
	}
	if t < 1 {
		t = 1
	}
	if t < c.target {
		committed.Add(int64(t-c.target) * int64(capacity))
		for i := t; i < len(c.reserve); i++ {
			c.reserve[i] = nil
		}
		c.reserve = c.reserve[:t]
		c.target = t
	}
	c.hw = 0
	c.epoch = epoch
}

// growLocked raises the target by one slot under the aggregate budget. The
// first slot of a touched class is exempt, so the floor of one reserved slot
// stays affordable at any budget.
func (c *class) growLocked(capacity int) {
	if c.target == 0 {
		c.target = 1
		committed.Add(int64(capacity))
	} else {
		if committed.Add(int64(capacity)) > reserveBudget {
			committed.Add(-int64(capacity))
			return
		}
		c.target++
	}
	if cap(c.reserve) < c.target {
		grown := make([][]byte, len(c.reserve), 2*c.target)
		copy(grown, c.reserve)
		c.reserve = grown
	}
}

// Get borrows a buffer of exactly size bytes, with a size-class capacity.
// Nonpositive sizes return nil without borrowing.
func Get(size int) []byte {
	if size <= 0 {
		return nil
	}
	outstanding.Add(1)
	if size > maxSize {
		allocations.Add(1)
		return make([]byte, size)
	}
	idx := int(classOf[(size+7)>>3])
	capacity := sizes[idx]
	c := &classes[idx]
	c.mu.Lock()
	if e := gcEpoch.Load(); e != c.epoch {
		c.settleLocked(capacity, e)
	}
	c.borrowed++
	if c.borrowed > c.hw {
		c.hw = c.borrowed
	}
	if n := len(c.reserve); n != 0 {
		b := c.reserve[n-1]
		c.reserve[n-1] = nil
		c.reserve = c.reserve[:n-1]
		c.mu.Unlock()
		return b[:size]
	}
	// A miss is demand evidence: the class holds less than the program keeps
	// in flight, so the reserve may aim one slot higher.
	if c.target < c.borrowed {
		c.growLocked(capacity)
	}
	c.mu.Unlock()
	if b, ok := c.cache.Get().([]byte); ok {
		return b[:size]
	}
	allocations.Add(1)
	return make([]byte, size, capacity)
}

// Put returns a buffer obtained from Get exactly once. Its starting address
// and capacity must be unchanged; its length may change. The caller must not
// retain or access it afterward. Buffers above 64 KiB are not retained.
func Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	outstanding.Add(-1)
	capacity := cap(b)
	if capacity > maxSize {
		return
	}
	idx := int(classOf[capacity>>3])
	if sizes[idx] != capacity {
		return
	}
	c := &classes[idx]
	c.mu.Lock()
	if e := gcEpoch.Load(); e != c.epoch {
		c.settleLocked(capacity, e)
	}
	c.borrowed--
	if len(c.reserve) < c.target {
		c.reserve = append(c.reserve, b[:capacity])
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.cache.Put(b[:capacity])
}

// Outstanding counts buffers currently borrowed.
func Outstanding() int64 { return outstanding.Load() }

// Allocations counts backing buffers created by Get.
func Allocations() uint64 { return allocations.Load() }
