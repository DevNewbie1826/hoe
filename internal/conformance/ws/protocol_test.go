package ws_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net/http"
	"testing"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestCloseEcho(t *testing.T) {
	for _, engine := range []bool{false, true} {
		t.Run(map[bool]string{false: "std", true: "hoe"}[engine], func(t *testing.T) {
			closed := make(chan error, 1)
			addr := serve(t, engine, echo(&ws.Upgrader{}, closed))
			nc, br, _ := rawClient(t, addr, "")
			p := ws.FormatCloseMessage(1000, "finished")
			if _, err := nc.Write(frame(8, true, false, p)); err != nil {
				t.Fatal(err)
			}
			op, got := readFrame(t, br)
			if op != 8 || !bytes.Equal(got, p) {
				t.Fatalf("close echo %d %x", op, got)
			}
			var ce *ws.CloseError
			if err := awaitClose(t, closed); !errors.As(err, &ce) || ce.Code != 1000 || ce.Text != "finished" {
				t.Fatalf("callback %v", err)
			}
		})
	}
}

func TestProtocolRejection(t *testing.T) {
	cases := []struct {
		name string
		wire []byte
		code int
	}{
		{"unmasked", []byte{129, 1, 'x'}, 1002},
		{"invalid-utf8", frame(1, true, false, []byte{255}), 1007},
		{"invalid-close-reason", frame(8, true, false, []byte{3, 232, 255}), 1007},
		{"invalid-close-status", frame(8, true, false, []byte{3, 237}), 1002},
		{"one-byte-close", frame(8, true, false, []byte{3}), 1002},
		{"fragmented-control", frame(9, false, false, nil), 1002},
		{"oversized-control", frame(9, true, false, make([]byte, 126)), 1002},
		{"unexpected-continuation", frame(0, true, false, nil), 1002},
		{"unnegotiated-compression", frame(1, true, true, nil), 1002},
		{"reserved-opcode", frame(3, true, false, nil), 1002},
		{"nonminimal-length", []byte{130, 254, 0, 1, 17, 29, 37, 51}, 1002},
		{"length-high-bit", []byte{130, 255, 128, 0, 0, 0, 0, 1, 0, 0, 17, 29, 37, 51}, 1002},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			closed := make(chan error, 1)
			addr := serve(t, true, echo(&ws.Upgrader{}, closed))
			nc, br, _ := rawClient(t, addr, "")
			if _, err := nc.Write(tc.wire); err != nil {
				t.Fatal(err)
			}
			op, p := readFrame(t, br)
			if op != 8 || len(p) < 2 || int(binary.BigEndian.Uint16(p)) != tc.code {
				t.Fatalf("opcode=%d close=%x want=%d", op, p, tc.code)
			}
			awaitClose(t, closed)
		})
	}
}

func TestPartialFrameAndFragmentedUTF8(t *testing.T) {
	addr := serve(t, true, echo(&ws.Upgrader{}, nil))
	nc, br, _ := rawClient(t, addr, "")
	first := frame(1, false, false, []byte{0xe2})
	last := frame(0, true, false, []byte{0x82, 0xac})
	wire := append(first, frame(9, true, false, []byte("ping"))...)
	wire = append(wire, last...)
	for _, b := range wire {
		if _, err := nc.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	op, p := readFrame(t, br)
	if op != 10 || string(p) != "ping" {
		t.Fatalf("pong %d %x", op, p)
	}
	op, p = readFrame(t, br)
	if op != 1 || string(p) != "\u20ac" {
		t.Fatalf("message %d %x", op, p)
	}
}

func TestDeclaredLengthRejectedBeforePayload(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, true, echo(&ws.Upgrader{ReadLimit: 1024}, closed))
	nc, br, _ := rawClient(t, addr, "")
	// No payload follows this 1TiB declaration. A preallocated frame or a
	// parser that waits for the payload cannot produce the required close.
	wire := []byte{130, 255}
	wire = binary.BigEndian.AppendUint64(wire, 1<<40)
	wire = append(wire, 17, 29, 37, 51)
	if _, err := nc.Write(wire); err != nil {
		t.Fatal(err)
	}
	op, p := readFrame(t, br)
	if op != 8 || len(p) < 2 || binary.BigEndian.Uint16(p) != 1009 {
		t.Fatalf("close %d %x", op, p)
	}
	awaitClose(t, closed)
}

func TestHandshakePolicyAndSubprotocol(t *testing.T) {
	u := &ws.Upgrader{Subprotocols: []string{"preferred", "secondary"}}
	addr := serve(t, true, echo(u, nil))
	nc, _, response := rawClient(t, addr, "Sec-WebSocket-Protocol: secondary, preferred\r\n")
	if response.Header.Get("Sec-WebSocket-Protocol") != "preferred" {
		t.Fatal(response.Header)
	}
	nc.Close()
	client := &http.Client{}
	r, err := http.NewRequest("GET", "http://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	r.Header.Set("Origin", "https://evil.invalid")
	got, err := client.Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Body.Close()
	if got.StatusCode != 403 {
		t.Fatalf("status=%d", got.StatusCode)
	}
}
