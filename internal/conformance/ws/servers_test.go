package ws_test

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	ws "github.com/DevNewbie1826/hoe/websocket"
)

func serve(t *testing.T, engine bool, h http.Handler) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	var closeServer func() error
	if engine {
		s := &hoe.Server{Handler: h}
		closeServer = s.Close
		go func() { result <- s.Serve(l) }()
	} else {
		s := &http.Server{Handler: h}
		closeServer = s.Close
		go func() { result <- s.Serve(l) }()
	}
	t.Cleanup(func() {
		if err := closeServer(); err != nil {
			t.Error(err)
		}
		select {
		case err := <-result:
			if err != http.ErrServerClosed {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("server did not stop")
		}
	})
	return l.Addr().String()
}

func rawClient(t *testing.T, address string, extra string) (net.Conn, *bufio.Reader, *http.Response) {
	t.Helper()
	nc, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close() })
	if err = nc.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = fmt.Fprintf(nc, "GET / HTTP/1.1\r\nHost: %s\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n%s\r\n", address, extra); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(nc)
	response, err := http.ReadResponse(br, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 101 {
		t.Fatalf("status=%d", response.StatusCode)
	}
	return nc, br, response
}

func frame(op int, final bool, compressed bool, p []byte) []byte {
	header := byte(op)
	if final {
		header |= 128
	}
	if compressed {
		header |= 64
	}
	b := []byte{header}
	switch {
	case len(p) < 126:
		b = append(b, 128|byte(len(p)))
	case len(p) <= 65535:
		b = append(b, 254, byte(len(p)>>8), byte(len(p)))
	default:
		b = append(b, 255)
		b = binary.BigEndian.AppendUint64(b, uint64(len(p)))
	}
	mask := [4]byte{17, 29, 37, 51}
	b = append(b, mask[:]...)
	for i, v := range p {
		b = append(b, v^mask[i&3])
	}
	return b
}

func readFrame(t *testing.T, r io.Reader) (int, []byte) {
	t.Helper()
	var h [2]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		t.Fatal(err)
	}
	if h[1]&128 != 0 {
		t.Fatal("server frame is masked")
	}
	if h[0]&128 == 0 {
		t.Fatal("server frame is fragmented")
	}
	n := uint64(h[1] & 127)
	if n == 126 {
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			t.Fatal(err)
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	} else if n == 127 {
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			t.Fatal(err)
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	if n > 64<<20 {
		t.Fatal("unexpected large response")
	}
	p := make([]byte, int(n))
	if _, err := io.ReadFull(r, p); err != nil {
		t.Fatal(err)
	}
	return int(h[0] & 15), p
}

func echo(u *ws.Upgrader, closed chan error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := u.Upgrade(w, r, ws.Handler{
			OnMessage: func(c *ws.Conn, kind int, p []byte) {
				if err := c.WriteMessage(kind, p); err != nil {
					c.Close()
				}
			},
			OnClose: func(c *ws.Conn, err error) {
				if closed != nil {
					closed <- err
				}
			},
		})
		if err != nil {
			return
		}
	})
}

func awaitClose(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("close callback missing")
		return nil
	}
}

func goroutines() int { return runtime.NumGoroutine() }

// The server initiates close, then clients consume its FIN before closing.
// This keeps 5000 client ephemeral ports out of TIME_WAIT for neighboring
// localhost suites without relying on a timer or weakening any count.
func closeClientsAfterServers(t *testing.T, clients []net.Conn, servers []*ws.Conn) {
	t.Helper()
	for _, c := range servers {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
	}
	var b [1]byte
	for _, c := range clients {
		if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Error(err)
		} else if _, err := c.Read(b[:]); err != io.EOF {
			t.Errorf("server close did not produce EOF: %v", err)
		}
		c.Close()
	}
}
