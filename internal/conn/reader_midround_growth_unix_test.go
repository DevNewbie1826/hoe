//go:build linux || darwin

package conn

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestReaderGrowsMidRoundForPipelinedRequest pins growth inside one read
// round: after a body read marks demand, the pipelined head following it in
// the same round must be served from the full-size reader. Disabling the
// mid-round growth in round() leaves that request on the small reader.
func TestReaderGrowsMidRoundForPipelinedRequest(t *testing.T) {
	s := newMidRoundGrowthServer(t)

	const bodySize = 2 * bigReaderSize

	midRoundGrowthWrite(t, s.client,
		"POST /body HTTP/1.1\r\nHost: t\r\nContent-Length: "+strconv.Itoa(bodySize)+"\r\n\r\n")
	midRoundGrowthAwait(t, s.bodyEntered, "the body handler")

	// The body is written only once the handler parks, so the whole body
	// waits in the socket and a delivered read reaches the small reader's
	// full size however the kernel splits it.
	midRoundGrowthWrite(t, s.client,
		strings.Repeat("x", bodySize)+"GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
	midRoundGrowthRelease(t, s.bodyRelease, "the body release")

	midRoundGrowthAwait(t, s.afterEntered, "the pipelined handler")
	if err := <-s.handlerErr; err != nil {
		t.Fatalf("body handler: %v", err)
	}

	c := s.connC
	if c.readerBytes != bigReaderSize {
		t.Fatalf("pipelined request reader backing = %d, want %d", c.readerBytes, bigReaderSize)
	}
	if !c.readerDemand.Load() {
		t.Fatal("body read did not mark reader demand")
	}
	if c.smallReaderLoans != 1 || c.smallReaderReleases != 1 {
		t.Fatalf("mid-round growth loans/releases = %d/%d, want 1/1", c.smallReaderLoans, c.smallReaderReleases)
	}

	midRoundGrowthRelease(t, s.afterRelease, "the pipelined release")
	if resp := midRoundGrowthResponse(t, s.client, http.MethodPost); resp.StatusCode != http.StatusOK {
		t.Fatalf("body request status = %q, want 200 OK", resp.Status)
	}
	if resp := midRoundGrowthResponse(t, s.client, http.MethodGet); resp.StatusCode != http.StatusOK {
		t.Fatalf("pipelined request status = %q, want 200 OK", resp.Status)
	}
}

type midRoundGrowthServer struct {
	engine       *Engine
	ln           *net.TCPListener
	client       *net.TCPConn
	connC        *Conn
	conns        chan *Conn
	bodyEntered  chan struct{}
	bodyRelease  chan struct{}
	afterEntered chan struct{}
	afterRelease chan struct{}
	handlerErr   chan error
}

func (s *midRoundGrowthServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/body":
		s.bodyEntered <- struct{}{}
		<-s.bodyRelease
		_, err := io.Copy(io.Discard, r.Body)
		s.handlerErr <- err
		if err != nil {
			return
		}
		w.WriteHeader(http.StatusOK)
	case "/after":
		s.afterEntered <- struct{}{}
		<-s.afterRelease
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func newMidRoundGrowthServer(t *testing.T) *midRoundGrowthServer {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &midRoundGrowthServer{
		ln:           ln,
		conns:        make(chan *Conn, 1),
		bodyEntered:  make(chan struct{}, 1),
		bodyRelease:  make(chan struct{}, 1),
		afterEntered: make(chan struct{}, 1),
		afterRelease: make(chan struct{}, 1),
		handlerErr:   make(chan error, 1),
	}
	srv := &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
		ConnState: func(nc net.Conn, state http.ConnState) {
			if state != http.StateNew {
				return
			}
			if c, ok := nc.(*Conn); ok {
				select {
				case s.conns <- c:
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
	s.engine = engine
	go engine.Serve(ln)
	client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		engine.Close()
		ln.Close()
		t.Fatal(err)
	}
	s.client = client
	t.Cleanup(func() { s.stop() })

	select {
	case c := <-s.conns:
		s.connC = c
	case <-time.After(5 * time.Second):
		t.Fatal("connection was not reported at StateNew")
	}
	return s
}

func (s *midRoundGrowthServer) stop() {
	select {
	case s.bodyRelease <- struct{}{}:
	default:
	}
	select {
	case s.afterRelease <- struct{}{}:
	default:
	}
	if s.client != nil {
		s.client.Close()
	}
	if s.engine != nil {
		s.engine.Close()
	}
	if s.ln != nil {
		s.ln.Close()
	}
}

func midRoundGrowthWrite(t *testing.T, c *net.TCPConn, payload string) {
	t.Helper()
	if _, err := c.Write([]byte(payload)); err != nil {
		t.Fatal(err)
	}
}

func midRoundGrowthAwait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not reach %s", what)
	}
}

func midRoundGrowthRelease(t *testing.T, ch chan struct{}, what string) {
	t.Helper()
	select {
	case ch <- struct{}{}:
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not accept %s", what)
	}
}

func midRoundGrowthResponse(t *testing.T, c *net.TCPConn, method string) *http.Response {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: method})
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
