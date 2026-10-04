//go:build linux || darwin

package loop

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/poll"
)

type pressureHandler struct {
	ctx     context.Context
	open    chan *Conn
	waiting chan struct{}
	result  chan error
	started atomic.Bool
}

func (h *pressureHandler) OnOpen(c *Conn)     { h.open <- c }
func (*pressureHandler) OnClose(*Conn, error) {}
func (h *pressureHandler) OnEvent(c *Conn, e poll.Event) {
	if !h.started.CompareAndSwap(false, true) {
		return
	}
	chunk := bytes.Repeat([]byte{0x5a}, 16384)
	announced := false
	for range 32 {
		for pending := chunk; len(pending) > 0; {
			ready := c.WriteReady()
			n, err := c.Write(pending)
			pending = pending[n:]
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			if errors.Is(err, syscall.EAGAIN) {
				if err := c.SetInterest(false, true); err != nil {
					h.result <- err
					return
				}
				if !announced {
					close(h.waiting)
					announced = true
				}
				select {
				case <-ready:
					continue
				case <-c.Done():
					h.result <- io.EOF
					return
				case <-h.ctx.Done():
					h.result <- h.ctx.Err()
					c.Close()
					return
				}
			}
			if err != nil {
				h.result <- err
				c.Close()
				return
			}
		}
	}
	h.result <- c.SetInterest(true, false)
}

func TestWriteReadyReleasesBackpressuredCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := &pressureHandler{ctx: ctx, open: make(chan *Conn, 1), waiting: make(chan struct{}), result: make(chan error, 1)}
	_, addr := transport(t, h)
	client := dial(t, addr)
	// Keep the receiver closed to application reads until the writer reports
	// EAGAIN. The small server send buffer forces real kernel backpressure.
	owned := nextConn(t, h.open)
	owned.mu.Lock()
	err := syscall.SetsockoptInt(owned.fd, syscall.SOL_SOCKET, syscall.SO_SNDBUF, 4096)
	owned.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("go")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.waiting:
	case <-ctx.Done():
		t.Fatal("writer did not reach backpressure")
	}
	got := make([]byte, 524288)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, bytes.Repeat([]byte{0x5a}, 524288)) {
		t.Fatal("backpressured write corrupted bytes")
	}
	select {
	case err := <-h.result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
