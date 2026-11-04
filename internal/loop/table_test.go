//go:build linux || darwin

package loop

import "testing"

// TestRegistrationSlotsTrackLiveCount pins that registration storage follows
// the live registration count, never descriptor values.
func TestRegistrationSlotsTrackLiveCount(t *testing.T) {
	l := &reactor{}
	const count = 4096
	for i := range count {
		// An fd-indexed table would hold a dense 1<<20-slot array here.
		l.registerConn(&Conn{fd: 1 << 20, done: make(chan struct{})})
		if got, want := len(l.slots), i+1; got != want {
			t.Fatalf("slots=%d after %d registrations, want %d", got, i+1, want)
		}
	}
	if cap(l.slots) > 2*count {
		t.Fatalf("slot backing=%d for %d registrations, want <= %d", cap(l.slots), count, 2*count)
	}
	for _, e := range l.slots {
		if e == nil || e.conn == nil {
			t.Fatal("registered slot missing")
		}
	}
	for _, e := range l.slots {
		l.freeSlot(e.token)
	}
	if got := len(l.free); got != count {
		t.Fatalf("free slots=%d, want %d", got, count)
	}
	l.registerConn(&Conn{done: make(chan struct{})})
	if got := len(l.slots); got != count {
		t.Fatalf("recycled registration grew slots to %d, want %d", got, count)
	}
	if got := len(l.free); got != count-1 {
		t.Fatalf("free slots=%d after recycle, want %d", got, count-1)
	}
}

// TestRegistrationTokenGuardsSlotReuse pins token identity across slot reuse.
func TestRegistrationTokenGuardsSlotReuse(t *testing.T) {
	l := &reactor{}
	first := &Conn{done: make(chan struct{})}
	t1 := l.registerConn(first)
	if t1 == 0 {
		t.Fatal("zero registration token")
	}
	if e := l.at(t1); e == nil || e.conn != first {
		t.Fatal("live token did not resolve")
	}
	if e := l.at(t1 ^ 1<<slotBits); e != nil {
		t.Fatal("stale sequence resolved a live slot")
	}
	l.freeSlot(t1)
	if e := l.at(t1); e != nil {
		t.Fatal("released token still resolved")
	}
	second := &Conn{done: make(chan struct{})}
	t2 := l.registerConn(second)
	if t2 == t1 {
		t.Fatal("recycled slot reused the token")
	}
	if e := l.at(t1); e != nil {
		t.Fatal("stale token resolved the recycled slot")
	}
	if e := l.at(t2); e == nil || e.conn != second {
		t.Fatal("recycled token did not resolve")
	}
	l.freeSlot(t1)
	if e := l.at(t2); e == nil {
		t.Fatal("stale release freed the live slot")
	}
	l.freeSlot(t2)
	if e := l.at(t2); e != nil {
		t.Fatal("live token resolved after release")
	}
}

// TestListenerRegistrationsStayOutOfConnectionSlots pins listener isolation.
func TestListenerRegistrationsStayOutOfConnectionSlots(t *testing.T) {
	l := &reactor{}
	lt := l.mint(0)
	ct := l.registerConn(&Conn{done: make(chan struct{})})
	if lt == ct {
		t.Fatal("listener and connection share a token")
	}
	l.listeners = append(l.listeners, listener{fd: 7, token: lt})
	if !l.listenerAt(7, lt) {
		t.Fatal("listener identity missing")
	}
	if l.listenerAt(8, lt) || l.listenerAt(7, ct) {
		t.Fatal("listener matched a different registration")
	}
	if !l.hasListener(7) || l.hasListener(8) {
		t.Fatal("listener membership wrong")
	}
	if e := l.at(lt); e != nil {
		t.Fatal("listener token resolved as a connection")
	}
}
