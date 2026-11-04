//go:build linux || darwin

package loop

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"sync/atomic"
	"syscall"
	"testing"
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
// path: the caller gets the error, the descriptor is closed exactly once by
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
