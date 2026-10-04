//go:build linux

package sys_test

import (
	"syscall"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys"
	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestSpliceCounters(t *testing.T) {
	fds := socketpair(t)
	var pipe [2]int
	if err := syscall.Pipe2(pipe[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC); err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(pipe[0])
	defer syscall.Close(pipe[1])
	payload := []byte("splice-real-bytes")
	if _, err := syscall.Write(fds[0], payload); err != nil {
		t.Fatal(err)
	}
	systest.Reset()
	// Socket -> pipe -> socket, as a TCP-source ReadFrom adapter will use it.
	n, err := sys.Splice(pipe[1], fds[1], len(payload))
	if err != nil || n != len(payload) {
		t.Fatalf("socket to pipe=%d,%v", n, err)
	}
	n, err = sys.Splice(fds[1], pipe[0], len(payload))
	if err != nil || n != len(payload) {
		t.Fatalf("pipe to socket=%d,%v", n, err)
	}
	got := make([]byte, len(payload))
	n, err = syscall.Read(fds[0], got)
	if err != nil || n != len(payload) || string(got) != string(payload) {
		t.Fatalf("splice payload=%q,%v", got, err)
	}
	c := systest.Snapshot()
	if c.SpliceCalls != 2 || c.SpliceBytes != uint64(2*len(payload)) {
		t.Fatalf("counters=%+v", c)
	}
}
