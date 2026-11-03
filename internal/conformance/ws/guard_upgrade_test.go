package ws_test

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestGuardPackageUpgradeUsesDefaultUpgrader(t *testing.T) {
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws.Upgrade(w, r, ws.Handler{
			OnMessage: func(c *ws.Conn, kind int, p []byte) {
				if err := c.WriteMessage(kind, p); err != nil {
					c.Close()
				}
			},
		})
	}))
	nc, br, _ := rawClient(t, addr, "")
	if _, err := nc.Write(frame(1, true, false, []byte("plain"))); err != nil {
		t.Fatal(err)
	}
	op, p := readFrame(t, br)
	if op != 1 || string(p) != "plain" {
		t.Fatalf("package Upgrade echo %d %q", op, p)
	}
	req, err := http.NewRequest("GET", "http://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Origin", "https://evil.example")
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("default upgrader status=%d, want 403 for a foreign origin", resp.StatusCode)
	}
}

func TestGuardUpgraderConfigFrozenAtUpgrade(t *testing.T) {
	u := &ws.Upgrader{
		ReadLimit:    4,
		Subprotocols: []string{"frozen"},
		CheckOrigin:  func(*http.Request) bool { return true },
	}
	opened := make(chan *ws.Conn, 1)
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := u.Upgrade(w, r, ws.Handler{OnOpen: func(c *ws.Conn) { opened <- c }}); err != nil {
			fmt.Fprint(w, err)
		}
	}))
	nc, br, resp := rawClient(t, addr, "Origin: https://other.example\r\nSec-WebSocket-Protocol: frozen\r\n")
	if resp.Header.Get("Sec-WebSocket-Protocol") != "frozen" {
		t.Fatalf("subprotocol %q, want frozen", resp.Header.Get("Sec-WebSocket-Protocol"))
	}
	select {
	case <-opened:
	case <-time.After(5 * time.Second):
		t.Fatal("Upgrade did not finish")
	}
	u.ReadLimit = 1 << 20
	u.Subprotocols = nil
	if _, err := nc.Write(frame(1, true, false, []byte("12345"))); err != nil {
		t.Fatal(err)
	}
	op, p := readFrame(t, br)
	if op != 8 || len(p) < 2 || binary.BigEndian.Uint16(p) != 1009 {
		t.Fatalf("ReadLimit not frozen at Upgrade: op=%d payload=%x", op, p)
	}
}
