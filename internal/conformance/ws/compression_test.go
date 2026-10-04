package ws_test

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"net/http"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
	"github.com/gorilla/websocket"
)

func TestCompressionNegotiation(t *testing.T) {
	for _, engine := range []bool{false, true} {
		t.Run(fmt.Sprint(engine), func(t *testing.T) {
			addr := serve(t, engine, echo(&ws.Upgrader{EnableCompression: true}, nil))
			d := websocket.Dialer{EnableCompression: true, HandshakeTimeout: 5 * time.Second}
			c, r, err := d.Dial("ws://"+addr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if r.Header.Get("Sec-WebSocket-Extensions") == "" {
				t.Fatal("compression not negotiated")
			}
			want := bytes.Repeat([]byte("compress this message "), 1000)
			c.EnableWriteCompression(true)
			if err = c.WriteMessage(2, want); err != nil {
				t.Fatal(err)
			}
			if err = c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			op, p, err := c.ReadMessage()
			if err != nil || op != 2 || !bytes.Equal(p, want) {
				t.Fatalf("echo opcode=%d size=%d err=%v", op, len(p), err)
			}
		})
	}
}

func TestCompressionContextTakeover(t *testing.T) {
	addr := serve(t, true, echo(&ws.Upgrader{EnableCompression: true}, nil))
	nc, br, response := rawClient(t, addr, "Sec-WebSocket-Extensions: permessage-deflate\r\n")
	if response.Header.Get("Sec-WebSocket-Extensions") != "permessage-deflate; server_no_context_takeover" {
		t.Fatal(response.Header)
	}
	var compressed bytes.Buffer
	w, err := flate.NewWriter(&compressed, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	want := bytes.Repeat([]byte("dictionary-data-with-nontrivial-overlap-"), 200)
	for i := 0; i < 2; i++ {
		compressed.Reset()
		if _, err = w.Write(want); err != nil {
			t.Fatal(err)
		}
		if err = w.Flush(); err != nil {
			t.Fatal(err)
		}
		wire := frame(2, true, true, compressed.Bytes()[:compressed.Len()-4])
		if _, err = nc.Write(wire); err != nil {
			t.Fatal(err)
		}
		// The server compresses the response independently. Decode its
		// payload to prove input context retention, not just negotiation.
		op, p := readFrame(t, br)
		r := flate.NewReader(bytes.NewReader(append(p, 0, 0, 255, 255, 1, 0, 0, 255, 255)))
		var out bytes.Buffer
		_, err = out.ReadFrom(r)
		r.Close()
		if err != nil || op != 2 || !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("message %d decode=%v size=%d", i, err, out.Len())
		}
	}
}

func TestDecodedCompressionReadLimit(t *testing.T) {
	closed := make(chan error, 1)
	addr := serve(t, true, echo(&ws.Upgrader{EnableCompression: true, ReadLimit: 1024}, closed))
	nc, br, _ := rawClient(t, addr, "Sec-WebSocket-Extensions: permessage-deflate\r\n")
	var out bytes.Buffer
	w, err := flate.NewWriter(&out, flate.BestCompression)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = w.Write(make([]byte, 65536)); err != nil {
		t.Fatal(err)
	}
	if err = w.Flush(); err != nil {
		t.Fatal(err)
	}
	wire := frame(2, true, true, out.Bytes()[:out.Len()-4])
	w.Close()
	if _, err = nc.Write(wire); err != nil {
		t.Fatal(err)
	}
	op, p := readFrame(t, br)
	if op != 8 || len(p) < 2 || binary.BigEndian.Uint16(p) != 1009 {
		t.Fatalf("close %d %x", op, p)
	}
	awaitClose(t, closed)
}

func TestGorillaOverHoe(t *testing.T) {
	u := websocket.Upgrader{EnableCompression: true}
	addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := u.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			op, p, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err = c.WriteMessage(op, p); err != nil {
				return
			}
		}
	}))
	d := websocket.Dialer{EnableCompression: true, HandshakeTimeout: 5 * time.Second}
	c, _, err := d.Dial("ws://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, kind := range []int{1, 2} {
		want := bytes.Repeat([]byte("gorilla interoperability"), 1000)
		if err = c.WriteMessage(kind, want); err != nil {
			t.Fatal(err)
		}
		if err = c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		op, p, err := c.ReadMessage()
		if err != nil || op != kind || !bytes.Equal(p, want) {
			t.Fatalf("echo %d %d %v", op, len(p), err)
		}
	}
}
