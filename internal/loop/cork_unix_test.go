//go:build linux || darwin

package loop

import (
	"errors"
	"io"
	"net"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestCorkPush(t *testing.T) {
	for _, keep := range []bool{true, false} {
		name := "release"
		if keep {
			name = "keep"
		}
		t.Run(name, func(t *testing.T) {
			// Given a real accepted loopback TCP socket with a kernel cork.
			h := &echoHandler{open: make(chan *Conn, 1)}
			_, addr := transport(t, h)
			client := dial(t, addr)
			c := nextConn(t, h.open)
			if err := c.EnableCork(); err != nil {
				t.Fatal(err)
			}
			assertCork(t, c, true)
			before := systest.Snapshot().PushCalls
			payload := []byte("held response")
			if n, err := c.Write(payload); err != nil || n != len(payload) {
				t.Fatalf("write=%d,%v", n, err)
			}
			if runtime.GOOS == "darwin" {
				if err := client.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
					t.Fatal(err)
				}
				var b [1]byte
				n, err := client.Read(b[:])
				var timeout net.Error
				if n != 0 || !errors.As(err, &timeout) || !timeout.Timeout() {
					t.Fatalf("cork leaked bytes before push: n=%d err=%v", n, err)
				}
			}

			// When held bytes are explicitly pushed, keeping or dropping cork.
			start := time.Now()
			if err := client.SetReadDeadline(start.Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if err := c.PushCork(keep); err != nil {
				t.Fatal(err)
			}

			// Then receipt is prompt, the actual socket flag matches keep,
			// and the boundary counter advances exactly once.
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(client, got); err != nil {
				t.Fatalf("receipt after push: %v", err)
			}
			if string(got) != string(payload) {
				t.Fatalf("receipt=%q want=%q", got, payload)
			}
			if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
				t.Fatalf("push receipt took %v", elapsed)
			}
			assertCork(t, c, keep)
			if delta := systest.Snapshot().PushCalls - before; delta != 1 {
				t.Fatalf("push counter delta=%d want=1", delta)
			}
			t.Logf("receipt within 100ms; keep=%v; counter delta=1", keep)
		})
	}
}

func assertCork(t *testing.T, c *Conn, want bool) {
	t.Helper()
	got, err := c.CorkEnabled()
	if err != nil || got != want {
		t.Fatalf("CorkEnabled=%v,%v want=%v", got, err, want)
	}
	option := 3 // Linux TCP_CORK.
	if runtime.GOOS == "darwin" {
		option = 0x4 // Darwin TCP_NOPUSH.
	}
	c.mu.Lock()
	value, err := syscall.GetsockoptInt(c.fd, syscall.IPPROTO_TCP, option)
	c.mu.Unlock()
	// Darwin returns the socket flag mask (4096), not normalized integer 1.
	if err != nil || (value != 0) != want {
		t.Fatalf("getsockopt=%d,%v want enabled=%v", value, err, want)
	}
}

func TestCorkClosedConnection(t *testing.T) {
	for _, detach := range []bool{false, true} {
		name := "close"
		if detach {
			name = "detach"
		}
		t.Run(name, func(t *testing.T) {
			// Given a connection whose descriptor lifetime has ended.
			h := &echoHandler{open: make(chan *Conn, 1)}
			_, addr := transport(t, h)
			dial(t, addr)
			c := nextConn(t, h.open)
			if detach {
				fd, err := c.Detach()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := syscall.Close(fd); err != nil {
						t.Error(err)
					}
				})
			} else if err := c.Close(); err != nil {
				t.Fatal(err)
			}

			// When cork operations are attempted after close or detach.
			before := systest.Snapshot().PushCalls
			enableErr := c.EnableCork()
			pushErr := c.PushCork(false)
			_, probeErr := c.CorkEnabled()

			// Then no syscall boundary is counted or stale fd accessed.
			for _, err := range []error{enableErr, pushErr, probeErr} {
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("closed cork operation=%v", err)
				}
			}
			if got := systest.Snapshot().PushCalls; got != before {
				t.Fatalf("closed push changed counter: %d -> %d", before, got)
			}
		})
	}
}
