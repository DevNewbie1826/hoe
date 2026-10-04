package bufpool

import (
	"runtime"
	"testing"
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
