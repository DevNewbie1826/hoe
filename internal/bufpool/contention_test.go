package bufpool

import (
	"runtime"
	"testing"
)

func BenchmarkPoolContention(b *testing.B) {
	cases := []struct {
		name  string
		sizes []int
		hold  int
		mixed bool
	}{
		{name: "immediate-4096", sizes: []int{4096}, hold: 1},
		{name: "immediate-32768", sizes: []int{32768}, hold: 1},
		{name: "immediate-mixed", sizes: []int{4096, 32768}, hold: 1, mixed: true},
		{name: "immediate-65536", sizes: []int{65536}, hold: 1},
		{name: "held4-4096", sizes: []int{4096}, hold: 4},
		{name: "held4-32768", sizes: []int{32768}, hold: 4},
	}
	pars := []struct {
		label string
		n     int
	}{
		{label: "par1", n: 1},
		{label: "par4", n: 4},
	}
	for _, tc := range cases {
		for _, par := range pars {
			b.Run(tc.name+"/"+par.label, func(b *testing.B) {
				b.ReportAllocs()
				// runN resets parallelism to 1 before every call. Set it here,
				// or every row silently runs at parallelism 1.
				b.SetParallelism(par.n)
				warmContention(tc.sizes, par.n, tc.hold)
				before := Allocations()
				b.ResetTimer()
				switch {
				case tc.mixed:
					benchMixed(b, tc.sizes[0], tc.sizes[1])
				case tc.hold == 4:
					benchHeld4(b, tc.sizes[0])
				default:
					benchImmediate(b, tc.sizes[0])
				}
				b.StopTimer()
				b.ReportMetric(float64(Allocations()-before)/float64(b.N), "backing_allocs/op")
			})
		}
	}
}

// warmContention holds parallelism*GOMAXPROCS*hold loans, then returns them.
// A paired Get/Put never lifts borrowed above one, so the reserve stays at
// its floor and backing_allocs/op counts cold-start makes instead of steady state.
func warmContention(sizes []int, parallelism, hold int) {
	n := parallelism * runtime.GOMAXPROCS(0) * hold
	bufs := make([][]byte, n)
	for _, size := range sizes {
		for i := range bufs {
			bufs[i] = Get(size)
		}
		for i := range bufs {
			Put(bufs[i])
			bufs[i] = nil
		}
	}
}

func benchImmediate(b *testing.B, size int) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Put(Get(size))
		}
	})
}

func benchMixed(b *testing.B, even, odd int) {
	b.RunParallel(func(pb *testing.PB) {
		var i int
		for pb.Next() {
			size := even
			if i&1 == 1 {
				size = odd
			}
			i++
			Put(Get(size))
		}
	})
}

func benchHeld4(b *testing.B, size int) {
	b.RunParallel(func(pb *testing.PB) {
		var held [4][]byte
		for i := range held {
			held[i] = Get(size)
		}
		var i int
		for pb.Next() {
			slot := i & 3
			i++
			Put(held[slot])
			held[slot] = Get(size)
		}
		for i := range held {
			Put(held[i])
		}
	})
}
