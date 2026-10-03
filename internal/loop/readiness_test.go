//go:build linux || darwin

package loop

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/poll"
)

type streamingHandler struct {
	ctx     context.Context
	waiting chan struct{}
	fail    chan error
	started atomic.Bool
}

func (*streamingHandler) OnOpen(*Conn)         {}
func (*streamingHandler) OnClose(*Conn, error) {}
func (h *streamingHandler) OnEvent(c *Conn, e poll.Event) {
	if !h.started.CompareAndSwap(false, true) {
		return
	}
	b := make([]byte, 8)
	n := 0
	waitAnnounced := false
	for n < len(b) {
		ready := c.ReadReady() // Subscribe before the action; edges cannot be lost.
		count, err := c.Read(b[n:])
		n += count
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			if !waitAnnounced {
				close(h.waiting)
				waitAnnounced = true
			}
			select {
			case <-ready:
				continue
			case <-c.Done():
				h.fail <- io.EOF
				return
			case <-h.ctx.Done():
				h.fail <- h.ctx.Err()
				c.Close()
				return
			}
		}
		if err != nil || count == 0 {
			h.fail <- errors.Join(err, io.EOF)
			c.Close()
			return
		}
	}
	for len(b) > 0 {
		n, err := c.Write(b)
		if err != nil {
			h.fail <- err
			c.Close()
			return
		}
		b = b[n:]
	}
	h.fail <- nil
}

func TestSameConnectionReadinessWakesBlockedCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	h := &streamingHandler{ctx: ctx, waiting: make(chan struct{}), fail: make(chan error, 1)}
	_, addr := transport(t, h)
	client := dial(t, addr)
	if _, err := client.Write([]byte("head")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.waiting:
	case <-ctx.Done():
		t.Fatal("callback did not wait for streaming body")
	}
	// Same callback remains scheduled; a second queued callback cannot wake it.
	if _, err := client.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatalf("same-connection stream stalled: %v", err)
	}
	if string(got) != "headbody" {
		t.Fatalf("bytes=%q", got)
	}
	select {
	case err := <-h.fail:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

type stormHandler struct {
	open                  chan *Conn
	entered, gate, second chan struct{}
	events                atomic.Int64
}

func (h *stormHandler) OnOpen(c *Conn)     { h.open <- c }
func (*stormHandler) OnClose(*Conn, error) {}
func (h *stormHandler) OnEvent(c *Conn, e poll.Event) {
	switch h.events.Add(1) {
	case 1:
		close(h.entered)
		<-h.gate
	case 2:
		close(h.second)
	}
}

func TestReadinessStormCoalescesBlockedCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	h := &stormHandler{open: make(chan *Conn, 1), entered: make(chan struct{}), gate: make(chan struct{}), second: make(chan struct{})}
	release := sync.OnceFunc(func() { close(h.gate) })
	defer release()
	g, addr := transport(t, h)
	client := dial(t, addr)
	owned := nextConn(t, h.open)
	if _, err := client.Write([]byte("start")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var start [5]byte
	if n, err := owned.Read(start[:]); err != nil || n != len(start) {
		t.Fatalf("start read=%d,%v", n, err)
	}
	for i := range 1000 {
		ready := owned.ReadReady()
		if _, err := client.Write([]byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-ready:
		case <-ctx.Done():
			t.Fatal("loop did not directly publish readiness")
		}
		var b [1]byte
		for {
			ready = owned.ReadReady()
			n, err := owned.Read(b[:])
			if errors.Is(err, syscall.EAGAIN) {
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				continue
			}
			if err != nil || n != 1 || b[0] != byte(i) {
				t.Fatalf("storm byte %d = %d, %d, %v", i, b[0], n, err)
			}
			break
		}
	}
	release()
	select {
	case <-h.second:
	case <-ctx.Done():
		t.Fatal("coalesced callback missing")
	}
	drained := make(chan struct{})
	if !owned.queue.Submit(g.pool, func() { close(drained) }) {
		t.Fatal("queue barrier rejected")
	}
	select {
	case <-drained:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if count := h.events.Load(); count != 2 {
		t.Fatalf("1000 edges queued %d callbacks, want first + one coalesced callback", count)
	}
}
