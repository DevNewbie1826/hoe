//go:build linux || darwin

package loop_test

import (
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/loop"
	"github.com/DevNewbie1826/hoe/internal/poll"
)

// TestHighFDRegistrationRetainsNoDenseTable pins registration storage to the
// live set. Adopting one descriptor at 1<<18 must not retain the fd-indexed
// pointer array that table grew to fd+1 (2 MiB). The guard reads the in-use
// delta across the adoption only, GC-settled at both samples, so allocations
// earlier tests leave behind cannot trip it; the bound is 1 MiB, half of that
// array, not a size read back from the reactor.
func TestHighFDRegistrationRetainsNoDenseTable(t *testing.T) {
	opened := make(chan struct{})
	g, err := loop.New(loop.Config{Loops: 1, Handler: &highFDHandler{opened: opened}})
	if err != nil {
		t.Fatal(err)
	}
	peer, high, err := highSocket(t)
	if err != nil {
		g.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		syscall.Close(peer)
		g.Close()
	})

	prev := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = prev })

	loopInUse := func() int64 {
		return inUseBytes(func(rec runtime.MemProfileRecord, fr runtime.Frame) bool {
			return strings.Contains(fr.File, "internal/loop/")
		})
	}
	before := loopInUse()
	if err := g.Adopt(high); err != nil {
		t.Fatal(err)
	}
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("adopted socket did not open")
	}

	const denseTable = 1 << 20
	if got := loopInUse() - before; got >= denseTable {
		t.Fatalf("high fd adoption grew internal/loop in-use by %d bytes, want < %d", got, denseTable)
	}
}

type highFDHandler struct{ opened chan struct{} }

func (h *highFDHandler) OnOpen(*loop.Conn) {
	select {
	case <-h.opened:
	default:
		close(h.opened)
	}
}
func (*highFDHandler) OnEvent(*loop.Conn, poll.Event) {}
func (*highFDHandler) OnClose(*loop.Conn, error)      {}

// highSocket returns a connected unix pair whose adopted end sits at 1<<18,
// so an fd-indexed table cannot hide inside a small descriptor.
func highSocket(t *testing.T) (peer, high int, err error) {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return 0, 0, err
	}
	high = 1 << 18
	if err = dupAt(fds[0], high); err != nil {
		syscall.Close(fds[0])
		syscall.Close(fds[1])
		return 0, 0, err
	}
	syscall.Close(fds[0])
	syscall.CloseOnExec(high)
	if err = syscall.SetNonblock(high, true); err != nil {
		syscall.Close(high)
		syscall.Close(fds[1])
		return 0, 0, err
	}
	return fds[1], high, nil
}

func inUseBytes(match func(runtime.MemProfileRecord, runtime.Frame) bool) int64 {
	// The in-use snapshot may lag one GC cycle; two collections settle it.
	runtime.GC()
	runtime.GC()
	var recs []runtime.MemProfileRecord
	for {
		n, ok := runtime.MemProfile(recs, false)
		if ok {
			recs = recs[:n]
			break
		}
		recs = make([]runtime.MemProfileRecord, n+16)
	}
	var sum int64
	for i := range recs {
		if recs[i].InUseBytes() == 0 {
			continue
		}
		frames := runtime.CallersFrames(recs[i].Stack())
		for {
			fr, more := frames.Next()
			if match(recs[i], fr) {
				sum += recs[i].InUseBytes()
				break
			}
			if !more {
				break
			}
		}
	}
	return sum
}
