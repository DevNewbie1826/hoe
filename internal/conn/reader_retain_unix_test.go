//go:build linux || darwin

package conn

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestParkedSmallRequestRetainsSmallReader pins idle reader retention on the
// real engine: one parked one-line GET holds a single small serve reader and
// keeps that loan while parked.
func TestParkedSmallRequestRetainsSmallReader(t *testing.T) {
	srv, client := parkedServer(t)
	defer srv.stop()

	writeRaw(t, client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, srv.entered)
	c := waitConn(t, srv.conns)
	if c.readerBytes != 1024 {
		t.Fatalf("parked small request reader backing = %d, want 1024", c.readerBytes)
	}
	if c.smallReaderLoans != 1 || c.smallReaderReleases != 0 {
		t.Fatalf("parked small request loans/releases = %d/%d, want 1/0", c.smallReaderLoans, c.smallReaderReleases)
	}
	srv.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("small request status %q", s)
	}
}

// TestReaderGrowsAfterBodyBeyondSmall pins growth on a real body read. A
// 2048-byte body fills a 1024-byte reader, so the round grows to the
// 4096-byte reader: promotion releases the small reader and the next parked
// request serves from the full-size backing.
func TestReaderGrowsAfterBodyBeyondSmall(t *testing.T) {
	srv, client := parkedServer(t)
	defer srv.stop()

	writeRaw(t, client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, srv.entered)
	c := waitConn(t, srv.conns)
	srv.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("small request status %q", s)
	}

	const body = 2048
	writeRaw(t, client, "POST /body HTTP/1.1\r\nHost: t\r\nContent-Length: 2048\r\n\r\n"+strings.Repeat("x", body))
	if s := readStatus(t, client, "POST").Status; s != "200 OK" {
		t.Fatalf("body request status %q", s)
	}

	writeRaw(t, client, "GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, srv.entered)
	if c.readerBytes != 4096 {
		t.Fatalf("request after a beyond-small body reader backing = %d, want 4096 (loopback POST write may have been split across reads; growth needs one delivered body read >= 1024)", c.readerBytes)
	}
	srv.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("request after growth status %q", s)
	}
	// Whether the POST gets its own round (loans=2, releases=2) or joins
	// GET /small's round (loans=1, releases=1) depends on arrival timing;
	// both are legitimate. readerBytes above is written by the goroutine
	// serving /after before its handler parks, so it is settled at the
	// waitPark edge; the previous round's release increment has no such
	// edge to this goroutine, so the every-loan-returned invariant is
	// asserted only after the engine joins its workers below.
	srv.stop()
	if c.smallReaderReleases != c.smallReaderLoans || c.smallReaderReleases < 1 {
		t.Fatalf("settled small-reader loans/releases = %d/%d, want equal and >= 1 (the pre-promotion reader leaked)", c.smallReaderLoans, c.smallReaderReleases)
	}
}

type parked struct {
	engine  *Engine
	ln      *net.TCPListener
	entered chan struct{}
	release chan struct{}
	conns   chan *Conn
}

func (p *parked) stop() {
	select {
	case <-p.release:
	default:
		close(p.release)
	}
	p.engine.Close()
	p.ln.Close()
}

func parkedServer(t *testing.T) (*parked, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	p := &parked{entered: make(chan struct{}), release: make(chan struct{}), conns: make(chan *Conn, 8)}
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil {
				io.Copy(io.Discard, r.Body)
			}
			if r.Method == http.MethodPost {
				return
			}
			p.entered <- struct{}{}
			<-p.release
		}),
		ConnState: func(nc net.Conn, s http.ConnState) {
			if s == http.StateNew {
				// Drop, not block: a failing test that stops draining must not stall the accept path.
				select {
				case p.conns <- nc.(*Conn):
				default:
				}
			}
		},
	}
	engine, err := New(srv, Policy{KeepAlive: func() bool { return true }})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	p.engine = engine
	p.ln = ln
	go engine.Serve(ln)
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		p.stop()
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	return p, client
}

func writeRaw(t *testing.T, c *net.TCPConn, req string) {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
}

func waitPark(t *testing.T, entered chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach its park")
	}
}

func waitConn(t *testing.T, conns chan *Conn) *Conn {
	t.Helper()
	select {
	case c := <-conns:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not reported")
		return nil
	}
}

func readStatus(t *testing.T, c *net.TCPConn, method string) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: method})
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}
