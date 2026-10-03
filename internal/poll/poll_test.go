//go:build linux || darwin

package poll

import (
	"syscall"
	"testing"
	"time"
)

func openPoller(t *testing.T) *Poller {
	t.Helper()
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func pair(t *testing.T) [2]int {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if err := syscall.SetNonblock(fd, true); err != nil {
			t.Fatal(err)
		}
		syscall.CloseOnExec(fd)
		t.Cleanup(func() {
			if err := syscall.Close(fd); err != nil {
				t.Error(err)
			}
		})
	}
	return fds
}

func TestEventBatch1024(t *testing.T) {
	// Given: every descriptor is ready before the first wait.
	p := openPoller(t)
	for i := 0; i < 1024; i++ {
		fds := pair(t)
		if err := p.Add(fds[0], uint64(i+1), true, false); err != nil {
			t.Fatal(err)
		}
		if _, err := syscall.Write(fds[1], []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	// When
	events, err := p.Wait()
	if err != nil {
		t.Fatal(err)
	}
	// Then: kernel readiness, not just a configured batch size.
	if len(events) != 1024 {
		t.Fatalf("ready events=%d, want 1024", len(events))
	}
	seen := make(map[uint64]bool)
	for _, e := range events {
		if !e.Read || e.Token == 0 || seen[e.Token] {
			t.Fatalf("bad event %+v", e)
		}
		seen[e.Token] = true
	}
}

func TestWakeParkedWait(t *testing.T) {
	p := openPoller(t)
	done := make(chan error, 1)
	go func() {
		events, err := p.Wait()
		if len(events) != 0 {
			t.Errorf("wake returned data: %+v", events)
		}
		done <- err
	}()
	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wake did not release Wait")
	}
}

func TestRuntimeParking(t *testing.T) {
	p := openPoller(t)
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		first := true
		done <- p.raw.Read(func(uintptr) bool {
			n, err := p.waitReady()
			if first {
				close(entered)
				first = false
			}
			return n != 0 || err != nil
		})
	}()
	<-entered
	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime parked wait did not wake")
	}
}

func TestRegistrationTokenAndModify(t *testing.T) {
	p := openPoller(t)
	fds := pair(t)
	if err := p.Add(fds[0], 1<<40|7, false, true); err != nil {
		t.Fatal(err)
	}
	events, err := p.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Write || events[0].Token != 1<<40|7 {
		t.Fatalf("%+v", events)
	}
	if err := p.Modify(fds[0], 1<<40|7, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err := syscall.Write(fds[1], []byte("token")); err != nil {
		t.Fatal(err)
	}
	events, err = p.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || !events[0].Read || events[0].Token != 1<<40|7 {
		t.Fatalf("%+v", events)
	}
	if err := p.Delete(fds[0]); err != nil {
		t.Fatal(err)
	}
	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	events, err = p.Wait()
	if err != nil || len(events) != 0 {
		t.Fatalf("deleted event %+v, %v", events, err)
	}
}
