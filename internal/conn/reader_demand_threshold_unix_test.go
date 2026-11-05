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

// The reader demand threshold is pinned at 1024 bytes with literals, so a
// moved smallReaderSize fails on the observed size before demand behavior.

// thresholdServer delivers a POST head before the body and parks twice, so
// the test reads reader state across the handler's channel happens-before edge.
type thresholdServer struct {
	engine         *Engine
	ln             *net.TCPListener
	client         *net.TCPConn
	conns          chan *Conn
	entered        chan struct{}
	drained        chan struct{}
	releaseBody    chan struct{}
	releaseDrained chan struct{}
	readErr        chan error
}

func (s *thresholdServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	s.entered <- struct{}{}
	<-s.releaseBody
	if _, err := io.Copy(io.Discard, r.Body); err != nil {
		select {
		case s.readErr <- err:
		default:
		}
	}
	s.drained <- struct{}{}
	<-s.releaseDrained
	w.WriteHeader(http.StatusOK)
}

func newThresholdServer(t *testing.T) *thresholdServer {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &thresholdServer{
		ln:             ln,
		conns:          make(chan *Conn, 8),
		entered:        make(chan struct{}, 1),
		drained:        make(chan struct{}, 1),
		releaseBody:    make(chan struct{}),
		releaseDrained: make(chan struct{}),
		readErr:        make(chan error, 1),
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
		ConnState: func(nc net.Conn, state http.ConnState) {
			if state != http.StateNew {
				return
			}
			c, ok := nc.(*Conn)
			if !ok {
				return
			}
			select {
			case s.conns <- c:
			default:
			}
		},
	}
	engine, err := New(srv, Policy{KeepAlive: func() bool { return true }})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	s.engine = engine
	go engine.Serve(ln)
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		engine.Close()
		ln.Close()
		t.Fatal(err)
	}
	s.client = client
	t.Cleanup(func() {
		close(s.releaseBody)
		close(s.releaseDrained)
		client.Close()
		engine.Close()
		ln.Close()
	})
	return s
}

func thresholdAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not reach %s", what)
	}
}

func thresholdRelease(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case ch <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not accept %s", what)
	}
}

func thresholdWrite(t *testing.T, c *net.TCPConn, payload string) {
	t.Helper()
	if _, err := c.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
}

func thresholdConn(t *testing.T, conns <-chan *Conn) *Conn {
	t.Helper()
	select {
	case c := <-conns:
		return c
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not captured at StateNew")
		return nil
	}
}

func thresholdResponse(t *testing.T, c *net.TCPConn) *http.Response {
	t.Helper()
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp
}

// TestReaderDemandThreshold1024MarksDemand pins the threshold from below.
func TestReaderDemandThreshold1024MarksDemand(t *testing.T) {
	s := newThresholdServer(t)

	// Head only: the body write below must be a head-excluded read.
	thresholdWrite(t, s.client, "POST /b HTTP/1.1\r\nHost: t\r\nContent-Length: 1024\r\n\r\n")
	thresholdAwait(t, s.entered, "handler entry")
	c := thresholdConn(t, s.conns)

	thresholdWrite(t, s.client, strings.Repeat("x", 1024))
	thresholdRelease(t, s.releaseBody, "the body release")
	thresholdAwait(t, s.drained, "body drain")

	select {
	case err := <-s.readErr:
		t.Fatalf("read body: %v", err)
	default:
	}
	if !c.readerDemand.Load() {
		t.Fatal("a delivered 1024-byte body read did not mark reader demand")
	}
	if c.bodyReadMax != 1024 {
		t.Fatalf("largest head-excluded body read = %d, want exactly 1024", c.bodyReadMax)
	}
	if c.readerBytes != 1024 {
		t.Fatalf("reader serving the parked request = %d bytes, want exactly 1024", c.readerBytes)
	}

	thresholdRelease(t, s.releaseDrained, "the drained release")
	if resp := thresholdResponse(t, s.client); resp.StatusCode != http.StatusOK {
		t.Fatalf("response status = %q, want 200 OK", resp.Status)
	}
}

// TestReaderDemandThreshold1023KeepsSmall pins the threshold from above.
func TestReaderDemandThreshold1023KeepsSmall(t *testing.T) {
	s := newThresholdServer(t)

	thresholdWrite(t, s.client, "POST /b HTTP/1.1\r\nHost: t\r\nContent-Length: 1023\r\n\r\n")
	thresholdAwait(t, s.entered, "handler entry")
	c := thresholdConn(t, s.conns)

	thresholdWrite(t, s.client, strings.Repeat("x", 1023))
	thresholdRelease(t, s.releaseBody, "the body release")
	thresholdAwait(t, s.drained, "body drain")

	select {
	case err := <-s.readErr:
		t.Fatalf("read body: %v", err)
	default:
	}
	if c.readerDemand.Load() {
		t.Fatal("a delivered 1023-byte body read marked reader demand")
	}
	if c.bodyReadMax != 1023 {
		t.Fatalf("largest head-excluded body read = %d, want exactly 1023", c.bodyReadMax)
	}
	if c.readerBytes != 1024 {
		t.Fatalf("reader serving the parked request = %d bytes, want exactly 1024", c.readerBytes)
	}

	thresholdRelease(t, s.releaseDrained, "the drained release")
	if resp := thresholdResponse(t, s.client); resp.StatusCode != http.StatusOK {
		t.Fatalf("response status = %q, want 200 OK", resp.Status)
	}
}
