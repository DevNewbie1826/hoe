//go:build linux || darwin

package conn

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// The reader demand policy's observable contract over the real engine
// surface: requests that stay within the small reader never mark demand,
// and one real beyond-small body read does. This governs the parked-round
// retention the u1 reader carrier compacts (the 10k hold shape).
func TestReaderDemandPolicyOnRealReads(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	conns := make(chan *Conn, 1)
	var seen sync.Once
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Errorf("read body: %v", err)
			}
			entered <- struct{}{}
			<-release
		}),
		ConnState: func(nc net.Conn, _ http.ConnState) {
			seen.Do(func() { conns <- nc.(*Conn) })
		},
	}
	engine, err := New(srv, listener, Policy{KeepAlive: func() bool { return true }})
	if err != nil {
		listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		engine.Close()
		listener.Close()
	})
	go engine.Serve(listener)

	client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })

	// A small request parks with the small reader: no demand.
	writeAll(t, client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	awaitPark(t, entered)
	c := <-conns
	if c.readerDemand.Load() {
		t.Fatal("small request marked reader demand")
	}
	release <- struct{}{}
	if s := readResponse(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("small request status %q", s)
	}

	// One body read beyond the small size marks demand: the next read
	// round on this connection takes the full-size reader.
	writeAll(t, client, "POST /big HTTP/1.1\r\nHost: t\r\nContent-Length: 8192\r\n\r\n"+strings.Repeat("x", 8192))
	awaitPark(t, entered)
	if !c.readerDemand.Load() {
		t.Fatal("beyond-small body read did not mark reader demand")
	}
	release <- struct{}{}
	if s := readResponse(t, client, "POST").Status; s != "200 OK" {
		t.Fatalf("big request status %q", s)
	}

	// Serving on the grown reader keeps its wire behavior.
	writeAll(t, client, "GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
	awaitPark(t, entered)
	release <- struct{}{}
	if s := readResponse(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("request after growth status %q", s)
	}
}

func writeAll(t *testing.T, c *net.TCPConn, req string) {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
}

func awaitPark(t *testing.T, entered chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not reach its park")
	}
}

func readResponse(t *testing.T, c *net.TCPConn, method string) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: method})
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp
}
