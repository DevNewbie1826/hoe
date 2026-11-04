package ws_test

import (
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestIdleDistinctUpgraders(t *testing.T) {
	const count = 20
	opened := make(chan *ws.Conn, count)
	closed := make(chan error, count)
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := &ws.Upgrader{}
		u.Upgrade(w, r, ws.Handler{OnOpen: func(c *ws.Conn) { opened <- c }, OnClose: func(c *ws.Conn, e error) { closed <- e }})
	}))
	clients := make([]net.Conn, 0, count)
	conns := make([]*ws.Conn, 0, count)
	defer func() {
		closeClientsAfterServers(t, clients, conns)
		for range conns {
			awaitClose(t, closed)
		}
	}()
	var baseline int
	for i := 0; i < count; i++ {
		if i == 1 {
			baseline = goroutineFloor(t)
		}
		nc, br, _ := rawClient(t, addr, "")
		clients = append(clients, nc)
		select {
		case c := <-opened:
			conns = append(conns, c)
		case <-time.After(5 * time.Second):
			t.Fatal("open missing")
		}
		if _, err := nc.Write(frame(9, true, false, []byte("ready"))); err != nil {
			t.Fatal(err)
		}
		op, p := readFrame(t, br)
		if op != 10 || string(p) != "ready" {
			t.Fatal("pong missing")
		}
	}
	// The shared upgrade reactor (created on first Adopt) and the engine's L+2 population sit in the baseline;
	// the 1ms upgrade worker and hijacked HTTP workers drain inside the 3s min-window;
	// the ceiling is host-size independent with a deterministic leak floor of 3 goroutines.
	if growth := goroutineFloor(t) - baseline; growth > 2 {
		t.Fatalf("20 distinct Upgraders added %d goroutines over the drained baseline (max 2)", growth)
	} else {
		t.Logf("20 distinct Upgraders added %d goroutines over the drained baseline", growth)
	}
}

// goroutineFloor polls runtime.NumGoroutine every 50ms across the full 3s window
// and returns the minimum. The iteration count is the window: it never exits early.
func goroutineFloor(t *testing.T) int {
	t.Helper()
	floor := runtime.NumGoroutine()
	for range 60 {
		time.Sleep(50 * time.Millisecond)
		if n := runtime.NumGoroutine(); n < floor {
			floor = n
		}
	}
	return floor
}
