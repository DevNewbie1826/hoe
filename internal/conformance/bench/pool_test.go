package bench

import (
	"runtime"
	"sync"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
)

func BenchmarkBufferReserveAcrossGC(b *testing.B) {
	for _, reserve := range []bool{false, true} {
		name := "sync_pool"
		if reserve {
			name = "hoe_reserve"
		}
		b.Run(name, func(b *testing.B) {
			// Compare plain sync.Pool to actual production reserve loans.
			var allocations uint64
			pool := sync.Pool{New: func() any {
				allocations++
				return new([4096]byte)
			}}
			round := func() {
				if reserve {
					p := bufpool.Get(4096)
					p[0] = 42
					bufpool.Put(p)
				} else {
					p := pool.Get().(*[4096]byte)
					p[0] = 42
					pool.Put(p)
				}
			}
			backing := func() uint64 {
				if reserve {
					return bufpool.Allocations()
				}
				return allocations
			}
			round()
			before := backing()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				runtime.GC()
				runtime.GC()
				b.StartTimer()
				round()
			}
			b.StopTimer()
			b.ReportMetric(float64(backing()-before)/float64(b.N), "backing_allocs/op")
		})
	}
}
