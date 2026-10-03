//go:build darwin

package poll

import (
	"syscall"
	"testing"
	"time"
)

func TestKqueueOwnsSingleDescriptor(t *testing.T) {
	// Given: /dev/fd omits kqueue descriptors, so count valid F_GETFD calls.
	var limit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &limit); err != nil {
		t.Fatal(err)
	}
	count := func() int {
		n := 0
		for fd := uint64(0); fd < limit.Cur; fd++ {
			_, _, err := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
			if err == 0 {
				n++
			} else if err != syscall.EBADF {
				t.Fatal(err)
			}
		}
		return n
	}
	before := count()
	// When
	openPoller(t)
	// Then: only the kqueue descriptor, with no wake or relay pipe.
	if got := count() - before; got != 1 {
		t.Fatalf("poller descriptors=%d, want one kqueue", got)
	}
}

func TestKqueueUserEventWake(t *testing.T) {
	// Given
	p := openPoller(t)
	// When: repeated wakes coalesce into one kernel user event.
	for range 64 {
		if err := p.Wake(); err != nil {
			t.Fatal(err)
		}
	}
	var events [MaxEvents]syscall.Kevent_t
	n, err := syscall.Kevent(p.fd, nil, events[:], &syscall.Timespec{})
	if err != nil {
		t.Fatal(err)
	}
	// Then
	if n != 1 || events[0].Filter != syscall.EVFILT_USER || events[0].Ident != 1 {
		t.Fatalf("wake events=%d first=%+v, want EVFILT_USER ident=1", n, events[0])
	}
}

func TestKqueueBlockingWaitUserEventWake(t *testing.T) {
	// Given: subscribe to completion before starting the native wait.
	p := openPoller(t)
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(entered)
		events, err := p.Wait()
		if len(events) != 0 {
			t.Errorf("user wake returned socket events: %+v", events)
		}
		done <- err
	}()
	<-entered
	// When
	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	// Then
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("user event did not release blocking kqueue wait")
	}
}
