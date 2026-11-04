//go:build linux || darwin

package loop

import (
	"errors"
	"testing"
)

// TestRegistrationSlotsTrackLiveCount pins that registration storage follows
// the live registration count, never descriptor values.
func TestRegistrationSlotsTrackLiveCount(t *testing.T) {
	l := &reactor{}
	const count = 4096
	for i := range count {
		// An fd-indexed table would hold a dense 1<<20-slot array here.
		if _, err := l.registerConn(&Conn{fd: 1 << 20, done: make(chan struct{})}); err != nil {
			t.Fatal(err)
		}
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
	if _, err := l.registerConn(&Conn{done: make(chan struct{})}); err != nil {
		t.Fatal(err)
	}
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
	t1, err := l.registerConn(first)
	if err != nil {
		t.Fatal(err)
	}
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
	t2, err := l.registerConn(second)
	if err != nil {
		t.Fatal(err)
	}
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
	ct, err := l.registerConn(&Conn{done: make(chan struct{})})
	if err != nil {
		t.Fatal(err)
	}
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

// TestRegisterFailsAtSlotLimit pins that a slot index never spills into the
// sequence field: the last usable index registers and resolves, the next
// registration fails without consuming state, and a freed slot recovers.
func TestRegisterFailsAtSlotLimit(t *testing.T) {
	l := &reactor{}
	// Nil entries reserve the indexes without allocating connections.
	l.slots = make([]*entry, maxSlots-1)
	c := &Conn{done: make(chan struct{})}
	token, err := l.registerConn(c)
	if err != nil {
		t.Fatalf("last slot index rejected: %v", err)
	}
	if slot := int(token&slotMask) >> 1; slot != maxSlots-1 {
		t.Fatalf("last token addresses slot %d, want %d", slot, maxSlots-1)
	}
	if e := l.at(token); e == nil || e.conn != c {
		t.Fatal("last slot token did not resolve")
	}
	if _, err := l.registerConn(&Conn{done: make(chan struct{})}); !errors.Is(err, errSlotsExhausted) {
		t.Fatalf("overlapping registration accepted: %v", err)
	}
	if got, want := len(l.slots), maxSlots; got != want {
		t.Fatalf("failed registration grew slots to %d, want %d", got, want)
	}
	if got := len(l.free); got != 0 {
		t.Fatalf("failed registration left %d free slots", got)
	}
	// The boundary recovers once a slot frees.
	l.freeSlot(token)
	if _, err := l.registerConn(&Conn{done: make(chan struct{})}); err != nil {
		t.Fatalf("freed boundary slot rejected: %v", err)
	}
}

// TestRegisterFailsAtSequenceLimit pins that the sequence field stops
// before wrapping: the final sequence mints a resolvable nonzero token,
// later registrations fail without consuming a slot, and listener
// registration fails the same way.
func TestRegisterFailsAtSequenceLimit(t *testing.T) {
	l := &reactor{seq: maxSeq - 1}
	c := &Conn{done: make(chan struct{})}
	token, err := l.registerConn(c)
	if err != nil {
		t.Fatalf("final sequence rejected: %v", err)
	}
	if got := token >> slotBits; got != maxSeq {
		t.Fatalf("final token sequence %d, want %d", got, maxSeq)
	}
	if token == 0 {
		t.Fatal("final token is zero; the poller rejects zero tokens")
	}
	if e := l.at(token); e == nil || e.conn != c {
		t.Fatal("final sequence token did not resolve")
	}
	l.freeSlot(token)
	if _, err := l.registerConn(&Conn{done: make(chan struct{})}); !errors.Is(err, errSeqExhausted) {
		t.Fatalf("wrapped registration accepted: %v", err)
	}
	if err := (&reactor{seq: maxSeq}).registerListener(0); !errors.Is(err, errSeqExhausted) {
		t.Fatalf("wrapped listener registration accepted: %v", err)
	}
	if got, want := len(l.slots), 1; got != want {
		t.Fatalf("failed registration changed slots to %d, want %d", got, want)
	}
	if got, want := len(l.free), 1; got != want {
		t.Fatalf("failed registration changed free slots to %d, want %d", got, want)
	}
}
