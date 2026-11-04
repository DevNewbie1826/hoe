//go:build linux || darwin

package loop

import (
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/poll"
)

// placementCases drives every guard on both policies the seam exposes.
var placementCases = []struct{ name, env string }{
	{"roundrobin", "roundrobin"},
	{"fdmodulo", "fdmodulo"},
}

// countingHandler pins exactly-once totals. Counters increment before the
// channel send and are read only after the group has joined, so scheduling
// cannot race the assertions.
type countingHandler struct {
	echoHandler
	opens, closes atomic.Int64
}

func (h *countingHandler) OnOpen(c *Conn) {
	h.opens.Add(1)
	h.echoHandler.OnOpen(c)
}

func (h *countingHandler) OnClose(c *Conn, err error) {
	h.closes.Add(1)
	h.echoHandler.OnClose(c, err)
}

func TestPlacementEnvValues(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    placementPolicy
		wantErr bool
	}{
		{"", roundRobin, false},
		{"roundrobin", roundRobin, false},
		{"fdmodulo", fdModulo, false},
		{"RoundRobin", roundRobin, true},
		{"fd-modulo", roundRobin, true},
	} {
		t.Setenv("HOE_LOOP_PLACEMENT", tc.value)
		got, err := placementFromEnv()
		if tc.wantErr {
			if err == nil {
				t.Fatalf("HOE_LOOP_PLACEMENT=%q accepted", tc.value)
			}
			continue
		}
		if err != nil {
			t.Fatalf("HOE_LOOP_PLACEMENT=%q: %v", tc.value, err)
		}
		if got != tc.want {
			t.Fatalf("HOE_LOOP_PLACEMENT=%q policy=%d want %d", tc.value, got, tc.want)
		}
	}
}

func TestPlacementOwnerSelection(t *testing.T) {
	const n = 5
	loops := make([]*reactor, n)
	for i := range loops {
		loops[i] = &reactor{}
	}
	t.Run("fdmodulo", func(t *testing.T) {
		g := &Group{loops: loops, placement: fdModulo}
		for fd := range 4*n + 7 {
			want := fd % n
			if got := g.owner(fd); got != loops[want] {
				t.Fatalf("fd=%d owner=loops[%d] want loops[%d]", fd, slices.Index(loops, got), want)
			}
		}
		// Sentinel and large descriptors must also resolve inside the set.
		for _, fd := range []int{-1, -1 << 30, 1 << 30} {
			if !slices.Contains(loops, g.owner(fd)) {
				t.Fatalf("fd=%d resolved outside the loop set", fd)
			}
		}
		if g.next.Load() != 0 {
			t.Fatal("fd-modulo placement advanced the round-robin counter")
		}
	})
	t.Run("roundrobin", func(t *testing.T) {
		g := &Group{loops: loops} // zero value keeps the historical default
		for i, fd := range []int{0, 1 << 20, 7, 99, 3, 1} {
			if got := g.owner(fd); got != loops[i%n] {
				t.Fatalf("call=%d fd=%d owner=loops[%d] want loops[%d]: round-robin must ignore the descriptor", i, fd, slices.Index(loops, got), i%n)
			}
		}
	})
}

// TestPlacementPoliciesAdoptExactlyOnce guards the seam on the real accept
// path: one registration per adopted descriptor on the policy-selected
// reactor, one OnOpen and one OnClose per connection, and per-connection
// byte FIFO while interleaved traffic shares the reactors.
func TestPlacementPoliciesAdoptExactlyOnce(t *testing.T) {
	for _, p := range placementCases {
		t.Run(p.name, func(t *testing.T) {
			t.Setenv("HOE_LOOP_PLACEMENT", p.env)
			h := &countingHandler{echoHandler: echoHandler{open: make(chan *Conn, 64), closed: make(chan *Conn, 64)}}
			g, addr := transport(t, h)
			const conns = 32
			clients := make([]net.Conn, conns)
			for i := range clients {
				clients[i] = dial(t, addr)
			}
			adopted := make(map[*Conn]bool, conns)
			for range conns {
				c := nextConn(t, h.open)
				if adopted[c] {
					t.Fatalf("OnOpen fired twice for fd=%d", c.fd)
				}
				adopted[c] = true
				if p.env == "fdmodulo" && c.loop != g.loops[c.fd%len(g.loops)] {
					t.Fatalf("fd=%d adopted by loops[%d] want loops[%d]", c.fd, slices.Index(g.loops, c.loop), c.fd%len(g.loops))
				}
			}
			// Interleaved rounds keep each connection's callback and byte
			// order under cross-connection traffic on shared reactors.
			for round := range 8 {
				for i, c := range clients {
					echo(t, c, fmt.Sprintf("policy=%s/conn=%02d/round=%d", p.name, i, round))
				}
			}
			for _, c := range clients {
				c.Close()
			}
			for range conns {
				if closed := nextConn(t, h.closed); !adopted[closed] {
					t.Fatalf("OnClose for a connection that never opened: fd=%d", closed.fd)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if got := h.opens.Load(); got != conns {
				t.Fatalf("OnOpen fired %d times for %d adoptions", got, conns)
			}
			if got := h.closes.Load(); got != conns {
				t.Fatalf("OnClose fired %d times for %d closures", got, conns)
			}
		})
	}
}

// TestPlacementPoliciesBalanceFailedAdoption guards the enqueue-failure
// path: the caller gets the error, the descriptor is closed (EBADF) by
// AdoptAsync, and the completion callback never fires.
func TestPlacementPoliciesBalanceFailedAdoption(t *testing.T) {
	for _, p := range placementCases {
		t.Run(p.name, func(t *testing.T) {
			t.Setenv("HOE_LOOP_PLACEMENT", p.env)
			g, err := New(Config{Handler: &echoHandler{}})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
			if err != nil {
				t.Fatal(err)
			}
			completions := 0
			err = g.AdoptAsync(fd, func(error) { completions++ })
			if !errors.Is(err, net.ErrClosed) {
				t.Fatalf("closed-group adoption err=%v want net.ErrClosed", err)
			}
			if completions != 0 {
				t.Fatalf("failed adoption invoked complete %d times", completions)
			}
			var st syscall.Stat_t
			if err := syscall.Fstat(fd, &st); !errors.Is(err, syscall.EBADF) {
				t.Fatalf("failed adoption left the descriptor open: fstat err=%v", err)
			}
		})
	}
}

// sequencingHandler records each connection's callback sequence around the
// echo behavior, so order guards can assert OnOpen strictly precedes the
// first OnEvent on the connection's queue.
type sequencingHandler struct {
	echoHandler
	mu  sync.Mutex
	seq map[*Conn][]string
}

func (h *sequencingHandler) note(c *Conn, step string) {
	h.mu.Lock()
	h.seq[c] = append(h.seq[c], step)
	h.mu.Unlock()
}

func (h *sequencingHandler) OnOpen(c *Conn) { h.note(c, "open"); h.echoHandler.OnOpen(c) }
func (h *sequencingHandler) OnEvent(c *Conn, e poll.Event) {
	h.note(c, "event")
	h.echoHandler.OnEvent(c, e)
}
func (h *sequencingHandler) OnClose(c *Conn, err error) {
	h.note(c, "close")
	h.echoHandler.OnClose(c, err)
}

// TestPlacementPoliciesPipelineConnectionOrder guards the client-visible
// ordering contract: a connection that writes its whole numbered pipeline in
// one burst, immediately after dialing, must run OnOpen before its first
// OnEvent and receive every byte back in FIFO order.
func TestPlacementPoliciesPipelineConnectionOrder(t *testing.T) {
	for _, p := range placementCases {
		t.Run(p.name, func(t *testing.T) {
			t.Setenv("HOE_LOOP_PLACEMENT", p.env)
			h := &sequencingHandler{
				echoHandler: echoHandler{open: make(chan *Conn, 64), closed: make(chan *Conn, 64)},
				seq:         make(map[*Conn][]string),
			}
			_, addr := transport(t, h)
			const conns = 8
			want := make([]string, conns)
			clients := make([]net.Conn, conns)
			for i := range clients {
				c := dial(t, addr)
				clients[i] = c
				var pipeline strings.Builder
				for k := range 32 {
					fmt.Fprintf(&pipeline, "policy=%s/conn=%02d/msg=%03d\n", p.name, i, k)
				}
				want[i] = pipeline.String()
				// The burst is on the wire before this test observes any
				// OnOpen, so adoption and the first readiness edge race.
				if _, err := io.WriteString(c, want[i]); err != nil {
					t.Fatal(err)
				}
			}
			for range conns {
				nextConn(t, h.open)
			}
			for i := range clients {
				got := make([]byte, len(want[i]))
				if _, err := io.ReadFull(clients[i], got); err != nil {
					t.Fatal(err)
				}
				if string(got) != want[i] {
					t.Fatalf("conn=%d echoed pipeline out of order: %q want %q", i, got, want[i])
				}
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			if len(h.seq) != conns {
				t.Fatalf("recorded %d connections, want %d", len(h.seq), conns)
			}
			for c, steps := range h.seq {
				if steps[0] != "open" {
					t.Fatalf("fd=%d callbacks %v: OnOpen must precede every OnEvent", c.fd, steps)
				}
				for _, step := range steps[1:] {
					if step != "event" {
						t.Fatalf("fd=%d callbacks %v: only readiness may follow OnOpen", c.fd, steps)
					}
				}
			}
		})
	}
}

// gateHandler records callback sequences and parks the reactor inside
// OnReadiness, which runs on the reactor goroutine before the round's
// commands drain. Blocking it there lets a test enqueue a whole command
// batch that a single drain pass must process in order.
type gateHandler struct {
	mu      sync.Mutex
	seq     map[*Conn][]string
	entered chan struct{}
	release chan struct{}
	gated   sync.Once
	closes  chan *Conn
}

func (h *gateHandler) note(c *Conn, step string) {
	h.mu.Lock()
	h.seq[c] = append(h.seq[c], step)
	h.mu.Unlock()
}

func (h *gateHandler) OnOpen(c *Conn)                { h.note(c, "open") }
func (h *gateHandler) OnEvent(c *Conn, e poll.Event) { h.note(c, "event") }
func (h *gateHandler) OnClose(c *Conn, err error) {
	h.note(c, "close")
	select {
	case h.closes <- c:
	default:
	}
}

// OnReadiness holds the reactor until the test releases it, after its
// queued commands are in place for one drain pass.
func (h *gateHandler) OnReadiness(c *Conn, e poll.Event) {
	h.gated.Do(func() { close(h.entered) })
	<-h.release
}

// socketpair returns a connected descriptor pair shaped like an accepted
// nonblocking close-on-exec socket plus its peer; the group owns the first
// descriptor once adopted, the test owns the peer.
func socketpair(t *testing.T) (int, int) {
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
	t.Cleanup(func() { syscall.Close(fds[1]) })
	return fds[0], fds[1]
}

// newGatedGroup starts a one-loop group and parks its reactor through one
// gate connection whose readiness byte is already queued, returning once
// OnReadiness holds the reactor. One loop guarantees every policy routes
// the test's commands to the parked reactor.
func newGatedGroup(t *testing.T, env string) (*Group, *gateHandler) {
	t.Helper()
	t.Setenv("HOE_LOOP_PLACEMENT", env)
	h := &gateHandler{
		seq:     make(map[*Conn][]string),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		closes:  make(chan *Conn, 32),
	}
	g, err := New(Config{Loops: 1, Handler: h})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { g.Close() })
	gateFd, gatePeer := socketpair(t)
	if _, err := syscall.Write(gatePeer, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := g.AdoptAsync(gateFd, func(error) {}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("gate readiness never reached the reactor")
	}
	return g, h
}

// TestPlacementPoliciesDrainCommandFIFO guards the drain order: with the
// reactor parked mid-round, a back-to-back AdoptAsync burst shares one
// drain pass, and the completion callbacks (which run on the reactor, in
// drain order) must fire in submission order.
func TestPlacementPoliciesDrainCommandFIFO(t *testing.T) {
	for _, p := range placementCases {
		t.Run(p.name, func(t *testing.T) {
			g, h := newGatedGroup(t, p.env)
			const burst = 16
			completions := make(chan int, burst)
			for i := range burst {
				fd, _ := socketpair(t)
				if err := g.AdoptAsync(fd, func(error) { completions <- i }); err != nil {
					t.Fatal(err)
				}
			}
			// Every command above queued behind the parked reactor; releasing
			// it runs one drain pass over the whole batch.
			close(h.release)
			for i := range burst {
				select {
				case got := <-completions:
					if got != i {
						t.Fatalf("drain completed adoption %d at position %d: commands must complete in submission order", got, i)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("adoption %d never completed", i)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestPlacementPoliciesAdoptBeforeShutdownOrder guards a connection adopted
// in the same drain batch that stops the reactor: registration must succeed,
// and OnOpen must precede OnClose on that connection's queue even though
// shutdown removes the descriptor in the same round.
func TestPlacementPoliciesAdoptBeforeShutdownOrder(t *testing.T) {
	for _, p := range placementCases {
		t.Run(p.name, func(t *testing.T) {
			g, h := newGatedGroup(t, p.env)
			fd, _ := socketpair(t)
			completed := make(chan error, 1)
			if err := g.AdoptAsync(fd, func(err error) { completed <- err }); err != nil {
				t.Fatal(err)
			}
			// Adoption and stop share the parked reactor's next drain pass.
			if err := g.loops[0].enqueue(&command{kind: stopCommand}); err != nil {
				t.Fatal(err)
			}
			close(h.release)
			select {
			case err := <-completed:
				if err != nil {
					t.Fatalf("adoption racing shutdown completed with %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("adoption never completed")
			}
			for range 2 { // the gate connection and the adopted connection
				select {
				case <-h.closes:
				case <-time.After(10 * time.Second):
					t.Fatal("shutdown never closed the adopted connection")
				}
			}
			h.mu.Lock()
			defer h.mu.Unlock()
			for c, steps := range h.seq {
				if steps[0] != "open" || steps[len(steps)-1] != "close" {
					t.Fatalf("fd=%d callbacks %v: OnOpen must precede OnClose on the connection queue", c.fd, steps)
				}
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
