//go:build linux || darwin

package loop

import (
	"errors"
	"fmt"
	"io"
	"net"
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

func TestCloseVsEventFDReuse(t *testing.T) {
	h := &echoHandler{open: make(chan *Conn, 32), closed: make(chan *Conn, 32)}
	_, addr := transport(t, h)
	last := make(map[int]*Conn)
	reused := 0
	// Each fresh connection has bytes queued alongside old close/interest
	// commands. Reusing a descriptor must not reuse its command identity.
	for i := range 5000 {
		c := dial(t, addr)
		owned := nextConn(t, h.open)
		if stale := last[owned.fd]; stale != nil {
			reused++
			if err := stale.loop.call(&command{kind: closeCommand, conn: stale}); err != nil {
				t.Fatal(err)
			}
			if err := stale.SetInterest(true, true); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("stale lifetime changed interests: %v", err)
			}
		}
		last[owned.fd] = owned
		echo(t, c, fmt.Sprintf("connection-%08d", i))
		c.Close()
		if closed := nextConn(t, h.closed); closed != owned {
			t.Fatal("wrong lifetime closed")
		}
		if err := owned.SetInterest(true, true); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("stale interest=%v", err)
		}
		if err := owned.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if reused == 0 {
		t.Fatal("churn did not exercise fd reuse")
	}
	t.Logf("5000 connections, %d reused descriptors, zero misrouted bytes", reused)
}

func TestIdleConnectionsHoldNoBuffers(t *testing.T) {
	before := bufpool.Outstanding()
	h := &echoHandler{open: make(chan *Conn, 128), closed: make(chan *Conn, 128)}
	g, addr := transport(t, h)
	for range 128 {
		client := dial(t, addr)
		owned := nextConn(t, h.open)
		echo(t, client, "active-to-idle")
		roundDone := make(chan struct{})
		if !owned.queue.Submit(g.pool, func() { close(roundDone) }) {
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
