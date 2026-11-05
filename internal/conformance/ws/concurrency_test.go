package ws_test

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conformance/internal/goroutinebudget"
	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestConcurrentWriters(t *testing.T) {
	opened := make(chan *ws.Conn, 1)
	u := &ws.Upgrader{}
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.Upgrade(w, r, ws.Handler{OnOpen: func(c *ws.Conn) { opened <- c }})
	}))
	_, br, _ := rawClient(t, addr, "")
	var c *ws.Conn
	select {
	case c = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("open callback missing")
	}
	defer c.Close()
	results := make(chan error, 128)
	for i := 0; i < 128; i++ {
		go func() { results <- c.WriteMessage(1, []byte(fmt.Sprintf("writer-%03d", i))) }()
	}
	seen := make(map[string]bool)
	for i := 0; i < 128; i++ {
		op, p := readFrame(t, br)
		if op != 1 || seen[string(p)] {
			t.Fatalf("corrupt or duplicate frame %d %q", op, p)
		}
		seen[string(p)] = true
	}
	for i := 0; i < 128; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 128; i++ {
		if !seen[fmt.Sprintf("writer-%03d", i)] {
			t.Fatal("missing writer", i)
		}
	}
}

func TestIdle5000Connections(t *testing.T) {
	const count = 5000
	opened := make(chan *ws.Conn, count)
	closed := make(chan error, count)
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u := &ws.Upgrader{}
		u.Upgrade(w, r, ws.Handler{
			OnOpen:  func(c *ws.Conn) { opened <- c },
			OnClose: func(c *ws.Conn, err error) { closed <- err },
		})
	}))
	clients := make([]net.Conn, 0, count)
	serverConns := make([]*ws.Conn, 0, count)
	defer func() {
		closeClientsAfterServers(t, clients, serverConns)
		for range serverConns {
			awaitClose(t, closed)
		}
	}()
	var baseline int
	for i := 0; i < count; i++ {
		if i == 1 {
			baseline = goroutinebudget.Floor(t)
		}
		nc, br, _ := rawClient(t, addr, "")
		clients = append(clients, nc)
		select {
		case c := <-opened:
			serverConns = append(serverConns, c)
		case <-time.After(5 * time.Second):
			t.Fatal("open callback missing")
		}
		// A wire pong is an event-driven barrier proving the upgraded
		// connection is active in its reader, not parked in HTTP OnOpen.
		if _, err := nc.Write(frame(9, true, false, []byte("ready"))); err != nil {
			t.Fatal(err)
		}
		op, p := readFrame(t, br)
		if op != 10 || string(p) != "ready" {
			t.Fatal("readiness pong missing")
		}
	}
	// The engine's L+2 startup population, the shared upgrade reactor sit in the baseline;
	// transient workers (including the one that served the first connection) have drained because the census waits for the count to rest;
	// the ceiling is host-size independent with a deterministic leak floor of 3 goroutines.
	if growth := goroutinebudget.Floor(t) - baseline; growth > 2 {
		t.Fatalf("5000 idle sockets added %d goroutines over the drained baseline (max 2)", growth)
	} else {
		t.Logf("5000 idle sockets added %d goroutines over the drained baseline", growth)
	}
}

func TestCloseDuringOpen(t *testing.T) {
	for _, engine := range []bool{true, false} {
		t.Run(fmt.Sprint(engine), func(t *testing.T) {
			closed := make(chan error, 1)
			u := &ws.Upgrader{}
			addr := serve(t, engine, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u.Upgrade(w, r, ws.Handler{OnOpen: func(c *ws.Conn) { c.Close() }, OnClose: func(c *ws.Conn, e error) { closed <- e }})
			}))
			rawClient(t, addr, "")
			awaitClose(t, closed)
		})
	}
}

func TestControlDeadlineWhileWriterBlocked(t *testing.T) {
	opened := make(chan *ws.Conn, 1)
	closed := make(chan error, 1)
	u := &ws.Upgrader{}
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.Upgrade(w, r, ws.Handler{OnOpen: func(c *ws.Conn) { opened <- c }, OnClose: func(c *ws.Conn, e error) { closed <- e }})
	}))
	nc, br, _ := rawClient(t, addr, "")
	var c *ws.Conn
	select {
	case c = <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("open missing")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); c.WriteMessage(2, make([]byte, 32<<20)) }()
	// Receiving the frame header proves WriteMessage has acquired its gate.
	header := make([]byte, 10)
	if _, err := br.Read(header[:2]); err != nil {
		t.Fatal(err)
	}
	err := c.WriteControl(9, nil, time.Now().Add(20*time.Millisecond))
	if err == nil {
		t.Fatal("blocked control did not time out")
	}
	nc.Close()
	c.Close()
	wg.Wait()
	awaitClose(t, closed)
}
