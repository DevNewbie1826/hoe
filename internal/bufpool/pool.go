// Package bufpool loans byte buffers only for active I/O rounds.
package bufpool

import (
	"sync"
	"sync/atomic"
)

// Go's small-object allocation classes, followed by page-sized I/O classes.
var sizes = [...]int{8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 144, 160, 176, 192, 208, 224, 240, 256, 288, 320, 352, 384, 416, 448, 480, 512, 576, 640, 704, 768, 896, 1024, 1152, 1280, 1408, 1536, 1792, 2048, 2304, 2688, 3072, 3200, 3456, 4096, 4864, 5376, 6144, 6528, 6784, 6912, 8192, 9472, 9728, 10240, 10880, 12288, 13568, 14336, 16384, 18432, 19072, 20480, 21760, 24576, 27264, 28672, 32768, 49152, 65536}

type class struct {
	mu      sync.Mutex
	reserve [2][]byte
	count   int
	cache   sync.Pool
}

var classes [len(sizes)]class
var outstanding atomic.Int64
var allocations atomic.Uint64

// Get borrows a buffer of exactly size bytes, with a size-class capacity.
// Nonpositive sizes return nil without borrowing.
func Get(size int) []byte {
	if size <= 0 {
		return nil
	}
	outstanding.Add(1)
	for i, capacity := range sizes {
		if size > capacity {
			continue
		}
		c := &classes[i]
		c.mu.Lock()
		if c.count != 0 {
			c.count--
			b := c.reserve[c.count]
			c.reserve[c.count] = nil
			c.mu.Unlock()
			return b[:size]
		}
		c.mu.Unlock()
		if b, ok := c.cache.Get().([]byte); ok {
			return b[:size]
		}
		allocations.Add(1)
		return make([]byte, size, capacity)
	}
	allocations.Add(1)
	return make([]byte, size)
}

// Put returns a buffer obtained from Get exactly once. Its starting address
// and capacity must be unchanged; its length may change. The caller must not
// retain or access it afterward. Buffers above 64 KiB are not retained.
func Put(b []byte) {
	if cap(b) == 0 {
		return
	}
	outstanding.Add(-1)
	for i, capacity := range sizes {
		if cap(b) != capacity {
			continue
		}
		b = b[:capacity]
		c := &classes[i]
		c.mu.Lock()
		if c.count < len(c.reserve) {
			c.reserve[c.count] = b
			c.count++
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
		c.cache.Put(b)
		return
	}
}

// Outstanding counts buffers currently borrowed.
func Outstanding() int64 { return outstanding.Load() }

// Allocations counts backing buffers created by Get.
func Allocations() uint64 { return allocations.Load() }
