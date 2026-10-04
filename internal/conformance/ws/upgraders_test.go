package ws_test

import (
	"net"
	"net/http"
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
	baseline := goroutines()
	clients := make([]net.Conn, 0, count)
	conns := make([]*ws.Conn, 0, count)
	defer func() {
		closeClientsAfterServers(t, clients, conns)
		for range conns {
			awaitClose(t, closed)
		}
	}()
	for i := 0; i < count; i++ {
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
	if growth := goroutines() - baseline; growth > 12 {
		t.Fatalf("20 distinct Upgraders added %d goroutines (max 12)", growth)
	} else {
		t.Logf("20 distinct Upgraders added %d goroutines", growth)
	}
}
