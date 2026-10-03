//go:build linux || darwin

package loop

import (
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	"github.com/DevNewbie1826/hoe/internal/poll"
)

type lifecycleHandler struct {
	open    chan *Conn
	entered chan struct{}
	closed  atomic.Int64
}

func (h *lifecycleHandler) OnOpen(c *Conn)                { h.open <- c }
func (h *lifecycleHandler) OnEvent(c *Conn, e poll.Event) { close(h.entered); <-c.Done() }
func (h *lifecycleHandler) OnClose(c *Conn, err error)    { h.closed.Add(1) }

func TestCloseWakesBlockedConnectionCallback(t *testing.T) {
	h := &lifecycleHandler{open: make(chan *Conn, 1), entered: make(chan struct{})}
	g, addr := transport(t, h)
	client := dial(t, addr)
	owned := nextConn(t, h.open)
	if _, err := client.Write([]byte("wait for close")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("callback did not start")
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-owned.Done():
	default:
		t.Fatal("Done did not close")
	}
	if h.closed.Load() != 1 {
		t.Fatalf("close callbacks=%d", h.closed.Load())
	}
}

func TestDetachTransfersDescriptorOwnership(t *testing.T) {
	h := &echoHandler{open: make(chan *Conn, 1)}
	g, addr := transport(t, h)
	client := dial(t, addr)
	owned := nextConn(t, h.open)
	fd, err := owned.Detach()
	if err != nil {
		t.Fatal(err)
	}
	f := os.NewFile(uintptr(fd), "hoe-detach-test")
	server, err := net.FileConn(f)
	closeErr := f.Close()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if err := server.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// Closing the reactor must not close a descriptor handed to the runtime.
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(client, "detached"); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, 8)
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "detached" {
		t.Fatalf("bytes=%q", got)
	}
	if _, err := server.Write(got); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(client, got); err != nil {
		t.Fatal(err)
	}
	if string(got) != "detached" {
		t.Fatalf("reply=%q", got)
	}
}

func TestTransportFDsAndBuffersReturnToBaseline(t *testing.T) {
	// Go lazily opens its process-wide runtime poller. Establish that global
	// resource before counting this group's fds, independent of test order.
	warm, err := poll.New()
	if err != nil {
		t.Fatal(err)
	}
	if err := warm.Close(); err != nil {
		t.Fatal(err)
	}
	path := "/proc/self/fd"
	if runtime.GOOS == "darwin" {
		path = "/dev/fd"
	}
	count := func() int {
		files, err := os.ReadDir(path)
		if err != nil {
			t.Fatal(err)
		}
		return len(files)
	}
	before, buffers := count(), bufpool.Outstanding()
	h := &echoHandler{open: make(chan *Conn, 1)}
	g, addr := transport(t, h)
	client := dial(t, addr)
	nextConn(t, h.open)
	echo(t, client, "resource-round")
	client.Close()
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if got := count(); got != before {
		t.Fatalf("fds=%d baseline=%d", got, before)
	}
	if got := bufpool.Outstanding(); got != buffers {
		t.Fatalf("buffers=%d baseline=%d", got, buffers)
	}
}

func TestBlockingTaskDoesNotStall(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	started := make(chan struct{})
	var once sync.Once
	h := &echoHandler{open: make(chan *Conn, 2)}
	var blocked atomic.Pointer[Conn]
	h.block = func(c *Conn) {
		if c == blocked.Load() {
			once.Do(func() { close(started); <-gate })
		}
	}
	_, addr := transport(t, h)
	first := dial(t, addr)
	blocked.Store(nextConn(t, h.open))
	if _, err := first.Write([]byte("blocked")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("blocking callback missing")
	}
	second := dial(t, addr)
	nextConn(t, h.open)
	echo(t, second, "independent")
}
