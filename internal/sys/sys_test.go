//go:build linux || darwin

package sys_test

import (
	"bytes"
	"os"
	"syscall"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys"
	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func socketpair(t *testing.T) [2]int {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		if err := syscall.SetNonblock(fd, true); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := syscall.Close(fd); err != nil {
				t.Error(err)
			}
		})
	}
	return fds
}

func TestReadWrite(t *testing.T) {
	fds := socketpair(t)
	if n, err := sys.Write(fds[0], []byte("raw socket")); err != nil || n != 10 {
		t.Fatalf("write=%d,%v", n, err)
	}
	b := make([]byte, 32)
	n, err := sys.Read(fds[1], b)
	if err != nil || string(b[:n]) != "raw socket" {
		t.Fatalf("read=%q,%v", b[:n], err)
	}
}

func TestWritevCounters(t *testing.T) {
	fds := socketpair(t)
	systest.Reset()
	n, err := sys.Writev(fds[0], [][]byte{nil, []byte("head"), {}, []byte("body")})
	if err != nil || n != 8 {
		t.Fatalf("writev=%d,%v", n, err)
	}
	b := make([]byte, 8)
	n, err = syscall.Read(fds[1], b)
	if err != nil || n != 8 || string(b) != "headbody" {
		t.Fatalf("read=%q,%v", b, err)
	}
	if c := systest.Snapshot(); c.WritevCalls != 1 {
		t.Fatalf("counters=%+v", c)
	}
}

func TestSendfileCounters(t *testing.T) {
	// Darwin sendfile requires a TCP destination.
	ln, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(ln)
	if err := syscall.Bind(ln, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(ln, 1); err != nil {
		t.Fatal(err)
	}
	addr, err := syscall.Getsockname(ln)
	if err != nil {
		t.Fatal(err)
	}
	client, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(client)
	if err := syscall.Connect(client, addr); err != nil {
		t.Fatal(err)
	}
	server, _, err := syscall.Accept(ln)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Close(server)
	f, err := os.CreateTemp(t.TempDir(), "sendfile")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	payload := bytes.Repeat([]byte("sendfile"), 4096)
	if _, err := f.Write(payload); err != nil {
		t.Fatal(err)
	}
	systest.Reset()
	off := int64(0)
	n, err := sys.Sendfile(server, int(f.Fd()), &off, len(payload))
	if err != nil || n != len(payload) || off != int64(len(payload)) {
		t.Fatalf("sendfile=%d offset=%d err=%v", n, off, err)
	}
	got := make([]byte, len(payload))
	for left := got; len(left) > 0; {
		n, err := syscall.Read(client, left)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("unexpected EOF")
		}
		left = left[n:]
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("sendfile corrupted bytes")
	}
	c := systest.Snapshot()
	if c.SendfileCalls != 1 || c.SendfileBytes != uint64(len(payload)) {
		t.Fatalf("counters=%+v", c)
	}
}
