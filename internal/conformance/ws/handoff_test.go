package ws_test

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestUpgradePreservesBufferedFrame(t *testing.T) {
	addr := serve(t, true, echo(&ws.Upgrader{}, nil))
	nc, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if err = nc.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	head := fmt.Sprintf("GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", addr)
	wire := append([]byte(head), frame(1, true, false, []byte("already buffered"))...)
	if _, err = nc.Write(wire); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	response, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 101 {
		t.Fatal(response.Status)
	}
	op, p := readFrame(t, br)
	if op != 1 || string(p) != "already buffered" {
		t.Fatalf("echo %d %q", op, p)
	}
}

func TestOpenPanicReleasesSocket(t *testing.T) {
	for _, engine := range []bool{false, true} {
		t.Run(fmt.Sprint(engine), func(t *testing.T) {
			u := &ws.Upgrader{}
			addr := serve(t, engine, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u.Upgrade(w, r, ws.Handler{OnOpen: func(*ws.Conn) { panic(http.ErrAbortHandler) }})
			}))
			nc, br, _ := rawClient(t, addr, "")
			if err := nc.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := br.ReadByte(); err != io.EOF {
				t.Fatalf("socket not released after panic: %v", err)
			}
		})
	}
}
