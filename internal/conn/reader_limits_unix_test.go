//go:build linux || darwin

package conn

import (
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestPanicAfterChunkedPromotion guards the observable behavior of a handler
// panic after a chunked body promoted the reader: each aborted connection
// closes, and the engine keeps serving with a fresh small reader. It does not
// pin the exact release pointer, so a mutant that releases the pre-promotion
// reader survives it.
func TestPanicAfterChunkedPromotion(t *testing.T) {
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &parked{ln: ln, entered: make(chan struct{}), release: make(chan struct{})}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.TransferEncoding) > 0 {
			panic(http.ErrAbortHandler)
		}
		p.entered <- struct{}{}
		<-p.release
	})}
	engine, err := New(srv, Policy{KeepAlive: func() bool { return true }})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	p.engine = engine
	go engine.Serve(ln)
	defer p.stop()

	body := "400\r\n" + strings.Repeat("x", 1024) + "\r\n0\r\n\r\n"
	const rounds = 8
	for i := range rounds {
		c, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
		if err != nil {
			t.Fatalf("round %d dial: %v", i, err)
		}
		req := "POST /panic HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n" + body
		if _, err := c.Write([]byte(req)); err != nil {
			c.Close()
			t.Fatalf("round %d write: %v", i, err)
		}
		if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			c.Close()
			t.Fatalf("round %d deadline: %v", i, err)
		}
		if _, err := io.Copy(io.Discard, c); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				c.Close()
				t.Fatalf("round %d: aborted connection did not close", i)
			}
		}
		c.Close()
	}

	withAllocProfile(t)
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatalf("dial after panics: %v", err)
	}
	defer client.Close()
	writeRaw(t, client, "GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, p.entered)
	if got := serveReaderBytes(); got == 0 || got >= halfOldReader {
		t.Fatalf("connection after %d promoted panics retained %d serve-reader bytes, want in (0, %d)", rounds, got, halfOldReader)
	}
	p.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("request after panics status %q", s)
	}
}
