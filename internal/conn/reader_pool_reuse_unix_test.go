//go:build linux || darwin

package conn

import "testing"

// TestReleasedSmallReaderRejoinsPool pins the small reader's return to the
// shared pool. The round releases its small reader through releaseRequestIO,
// and that release must both count on the connection and hand the reader
// back so the next acquire can take it again: dropping the small pool's Put
// makes this test fail.
//
// The identity probe is structural, not timing-based. A released reader is
// expected back on the following acquire; the bounded retries only tolerate
// the pool legitimately dropping an entry (a GC between release and acquire)
// or a probe goroutine migrating between private pools. Under the mutation a
// released reader is never offered back at all, and every acquire returns a
// newly constructed reader with a different address than the live released
// one, so no retry can observe reuse.
func TestReleasedSmallReaderRejoinsPool(t *testing.T) {
	c := &Conn{}

	first := acquireRequestIO(c)
	if !first.small {
		t.Fatalf("first acquisition is not a small reader")
	}
	if got := first.in.Size(); got != smallReaderSize {
		t.Fatalf("first acquisition reader size = %d, want %d", got, smallReaderSize)
	}
	if c.smallReaderLoans != 1 || c.smallReaderReleases != 0 {
		t.Fatalf("after first acquisition loans/releases = %d/%d, want 1/0", c.smallReaderLoans, c.smallReaderReleases)
	}

	releaseRequestIO(c, first)
	if c.smallReaderLoans != 1 || c.smallReaderReleases != 1 {
		t.Fatalf("after first release loans/releases = %d/%d, want 1/1", c.smallReaderLoans, c.smallReaderReleases)
	}

	const probes = 100
	released := first
	reused := false
	for i := 0; i < probes; i++ {
		// released was returned to the pool immediately before this acquire.
		next := acquireRequestIO(c)
		if next == released {
			reused = true
			// Return the reused loan so the counters stay balanced below.
			releaseRequestIO(c, next)
			break
		}
		releaseRequestIO(c, next)
		released = next
	}
	if !reused {
		t.Fatalf("a small reader returned to the pool was not handed back by any of %d subsequent acquisitions; the release did not reach the pool", probes)
	}
	if c.smallReaderLoans != c.smallReaderReleases {
		t.Fatalf("small reader loans/releases = %d/%d, want equal: every loan was released", c.smallReaderLoans, c.smallReaderReleases)
	}
	if c.smallReaderLoans < 2 {
		t.Fatalf("small reader loans = %d, want at least the initial loan and one reuse probe", c.smallReaderLoans)
	}
}
