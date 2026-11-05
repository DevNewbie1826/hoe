//go:build linux || darwin

package loop

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	"github.com/DevNewbie1826/hoe/internal/poll"
)

type echoHandler struct {
	open    chan *Conn
	closed  chan *Conn
	block   func(*Conn)
	mu      sync.Mutex
	pending map[*Conn][]byte
}

func (h *echoHandler) OnOpen(c *Conn) {
	if h.open != nil {
		h.open <- c
	}
}
func (h *echoHandler) OnClose(c *Conn, err error) {
	h.mu.Lock()
	delete(h.pending, c)
	h.mu.Unlock()
	if h.closed != nil {
		h.closed <- c
	}
}
func (h *echoHandler) OnEvent(c *Conn, e poll.Event) {
	if h.block != nil {
		h.block(c)
	}
	h.mu.Lock()
	out := h.pending[c]
	delete(h.pending, c)
	h.mu.Unlock()
	var readErr error
	if e.Read {
		b := bufpool.Get(16384)
		defer bufpool.Put(b)
		for {
			n, err := c.Read(b)
			out = append(out, b[:n]...)
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if err != nil {
				if !errors.Is(err, syscall.EAGAIN) {
					readErr = err
				}
				break
			}
			if n == 0 {
				readErr = io.EOF
				break
			}
		}
	}
	for len(out) > 0 {
		n, err := c.Write(out)
		out = out[n:]
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			h.mu.Lock()
			if h.pending == nil {
				h.pending = make(map[*Conn][]byte)
			}
			h.pending[c] = out
			h.mu.Unlock()
			c.SetInterest(true, true)
			return
		}
		if err != nil {
			c.Close()
			return
		}
	}
	if readErr != nil || e.EOF {
		c.Close()
		return
	}
	c.SetInterest(true, false)
}

func transport(t *testing.T, h Handler) (*Group, string) {
	t.Helper()
	g, err := New(Config{Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	syscall.CloseOnExec(fd)
	if err := syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 4096); err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	addr, err := syscall.Getsockname(fd)
	if err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	if err := g.Listen(fd); err != nil {
		syscall.Close(fd)
		t.Fatal(err)
	}
	return g, fmt.Sprintf("127.0.0.1:%d", addr.(*syscall.SockaddrInet4).Port)
}
func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}
func nextConn(t *testing.T, ch <-chan *Conn) *Conn {
	t.Helper()
	select {
	case c := <-ch:
		return c
	case <-time.After(10 * time.Second):
		t.Fatal("connection event missing")
		return nil
	}
}
func echo(t *testing.T, c net.Conn, payload string) {
	t.Helper()
	if _, err := io.WriteString(c, payload); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(c, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != payload {
		t.Fatalf("misrouted bytes: %q want %q", got, payload)
	}
}

func TestAcceptReadWriteEcho(t *testing.T) {
	_, addr := transport(t, &echoHandler{})
	c := dial(t, addr)
	for i := range 16 {
		echo(t, c, fmt.Sprintf("echo-round-%04d", i))
	}
}

// socketPair returns a connected AF_UNIX stream pair with the transport's
// descriptor flags. A descriptor pair instead of a TCP listener keeps the
// churn off the host's connection-tracking table: the invariant under test is
// registration identity, and real dials only make the test depend on host
// state that has nothing to do with the loop.
func socketPair(t *testing.T) [2]int {
	t.Helper()
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, fd := range fds {
		syscall.CloseOnExec(fd)
		if err := syscall.SetNonblock(fd, true); err != nil {
			syscall.Close(fds[0])
			syscall.Close(fds[1])
			t.Fatal(err)
		}
	}
	return fds
}

// peerConn wraps a socketpair end for the test side. Only the descriptor that
// Group.Adopt owns stays raw; this side is a regular net.Conn so reads can
// carry a deadline instead of relying on the schedule.
func peerConn(t *testing.T, fd int) net.Conn {
	t.Helper()
	f := os.NewFile(uintptr(fd), "loop-test-peer")
	c, err := net.FileConn(f)
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestCloseVsEventFDReuse proves a reused descriptor number cannot revive the
// command identity of an earlier closed connection. Each iteration adopts a
// socketpair end, closes it, and adopts the next pair, whose descriptor number
// the kernel recycles immediately. Stale close and interest commands aimed at
// the previous lifetime must resolve through its registration token only, so
// they must neither reach the new registration nor be able to change it.
func TestCloseVsEventFDReuse(t *testing.T) {
	h := &echoHandler{open: make(chan *Conn, 8), closed: make(chan *Conn, 8)}
	g, err := New(Config{Loops: 2, Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.Close(); err != nil {
			t.Error(err)
		}
	})

	type lifetime struct {
		conn *Conn
		peer net.Conn
	}
	last := make(map[int]*lifetime)
	reused := 0
	const rounds = 64
	for i := range rounds {
		fds := socketPair(t)
		if err := g.Adopt(fds[0]); err != nil {
			syscall.Close(fds[1])
			t.Fatal(err)
		}
		owned := nextConn(t, h.open)
		peer := peerConn(t, fds[1])
		payload := fmt.Sprintf("connection-%08d", i)
		echo(t, peer, payload)

		// Close through the owner and wait for its close callback, so the next
		// adoption below is guaranteed to see this descriptor released.
		if err := owned.Close(); err != nil {
			t.Fatal(err)
		}
		if closed := nextConn(t, h.closed); closed != owned {
			t.Fatal("wrong lifetime closed")
		}

		if stale := last[owned.fd]; stale != nil {
			reused++
			// Recycle the descriptor number and install the new registration
			// first. The stale commands below then run while the slot is
			// occupied by a different lifetime, which is the only ordering
			// that can exercise the identity check.
			next := socketPair(t)
			if next[0] != owned.fd {
				syscall.Close(next[0])
				syscall.Close(next[1])
				t.Fatalf("descriptor %d not recycled; got %d", owned.fd, next[0])
			}
			if err := g.Adopt(next[0]); err != nil {
				syscall.Close(next[1])
				t.Fatal(err)
			}
			fresh := nextConn(t, h.open)
			if fresh.fd != owned.fd {
				t.Fatalf("adopted fd=%d, want reused %d", fresh.fd, owned.fd)
			}
			if err := stale.conn.loop.call(&command{kind: closeCommand, conn: stale.conn}); err != nil {
				t.Fatal(err)
			}
			if err := stale.conn.SetInterest(true, true); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("stale lifetime changed interests: %v", err)
			}
			// The new registration must be untouched by the stale commands.
			select {
			case <-fresh.Done():
				t.Fatal("stale commands closed the reused registration")
			default:
			}
			freshPeer := peerConn(t, next[1])
			echo(t, freshPeer, payload+"-after-stale")
			last[owned.fd] = &lifetime{conn: fresh, peer: freshPeer}
			if err := fresh.Close(); err != nil {
				t.Fatal(err)
			}
			if closed := nextConn(t, h.closed); closed != fresh {
				t.Fatal("wrong lifetime closed")
			}
			continue
		}
		last[owned.fd] = &lifetime{conn: owned, peer: peer}
	}
	if reused == 0 {
		t.Fatal("churn did not exercise fd reuse")
	}
	t.Logf("%d rounds, %d reused descriptors, zero misrouted bytes", rounds, reused)
}

func TestIdleConnectionsHoldNoBuffers(t *testing.T) {
	before := bufpool.Outstanding()
	h := &echoHandler{open: make(chan *Conn, 128), closed: make(chan *Conn, 128)}
	_, addr := transport(t, h)
	for range 128 {
		client := dial(t, addr)
		owned := nextConn(t, h.open)
		echo(t, client, "active-to-idle")
		roundDone := make(chan struct{})
		if !owned.queue.Submit(owned.loop.pool, func() { close(roundDone) }) {
			t.Fatal("round barrier rejected")
		}
		select {
		case <-roundDone:
		case <-time.After(5 * time.Second):
			t.Fatal("I/O round did not return")
		}
	}
	if got := bufpool.Outstanding(); got != before {
		t.Fatalf("idle buffers=%d baseline=%d", got, before)
	}
}
