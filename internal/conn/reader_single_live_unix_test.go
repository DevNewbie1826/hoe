//go:build linux || darwin

package conn

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// TestSingleLiveSmallReaderInvariantOnConnection pins liveSmall =
// smallReaderLoans - smallReaderReleases on one real connection: 0 while
// idle, exactly 1 while the small reader serves a request, exactly 0 while a
// promoted request is served by the full-size reader, and never 2+ or
// negative at any observation point.
func TestSingleLiveSmallReaderInvariantOnConnection(t *testing.T) {
	s := newSingleLiveServer(t)
	defer s.stop()

	// A first short request also covers the idle stage: loans/releases of 1/0
	// inside the handler mean the idle connection retained no small reader.
	singleLiveWrite(t, s.client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	s.awaitPath(t, "/small")
	singleLiveRead(t, s.client, http.MethodGet, "small request")

	// The 2048-byte body fills the 1024-byte reader; promotion happens after
	// this request finishes, so the handler still sees the small reader live.
	singleLiveWrite(t, s.client, "POST /demand HTTP/1.1\r\nHost: t\r\nContent-Length: 2048\r\n\r\n"+strings.Repeat("x", 2048))
	s.awaitPath(t, "/demand")
	singleLiveRead(t, s.client, http.MethodPost, "demand request")

	// The promoted full-size reader serves the next request with no small
	// reader live beside it.
	singleLiveWrite(t, s.client, "GET /after HTTP/1.1\r\nHost: t\r\n\r\n")
	s.awaitPath(t, "/after")
	singleLiveRead(t, s.client, http.MethodGet, "request after promotion")

	// Chunked framing promotes before the handler runs, another zero-small
	// observation while the big reader serves.
	singleLiveWrite(t, s.client, "POST /chunked HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n"+
		"400\r\n"+strings.Repeat("x", 1024)+"\r\n0\r\n\r\n")
	s.awaitPath(t, "/chunked")
	singleLiveRead(t, s.client, http.MethodPost, "chunked request")

	// Idle again: Engine.Close joins the loops and workers, so the counters
	// are settled.
	if err := s.engine.Close(); err != nil {
		t.Fatalf("engine close: %v", err)
	}
	loans, releases := s.conn.smallReaderLoans, s.conn.smallReaderReleases
	singleLiveCheck(t, singleLiveSample{stage: "idle after close", loans: loans, releases: releases}, 0, 0)
	if loans != releases {
		t.Fatalf("idle after close small-reader loans/releases = %d/%d, every loan must be returned", loans, releases)
	}
}

type singleLiveSample struct {
	stage    string
	loans    int
	releases int
	bytes    int
}

// singleLiveCheck enforces the invariant at one observation point; wantBytes
// of zero skips the backing-size check.
func singleLiveCheck(t *testing.T, s singleLiveSample, wantLive, wantBytes int) {
	t.Helper()
	live := s.loans - s.releases
	if live < 0 {
		t.Errorf("%s: live small readers = %d from loans/releases %d/%d, must never be negative", s.stage, live, s.loans, s.releases)
	}
	if live > 1 {
		t.Errorf("%s: live small readers = %d from loans/releases %d/%d, two small readers must never be live at once", s.stage, live, s.loans, s.releases)
	}
	if live != wantLive {
		t.Errorf("%s: live small readers = %d from loans/releases %d/%d, want %d", s.stage, live, s.loans, s.releases, wantLive)
	}
	if wantBytes > 0 && s.bytes != wantBytes {
		t.Errorf("%s: reader backing = %d bytes, want %d", s.stage, s.bytes, wantBytes)
	}
}

type singleLiveConnKey struct{}

type singleLiveServer struct {
	t       *testing.T
	engine  *Engine
	ln      *net.TCPListener
	client  *net.TCPConn
	conn    *Conn
	entered chan string
}

func newSingleLiveServer(t *testing.T) *singleLiveServer {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &singleLiveServer{t: t, ln: ln, entered: make(chan string, 8)}
	srv := &http.Server{
		ConnContext: func(ctx context.Context, nc net.Conn) context.Context {
			s.conn = nc.(*Conn)
			return context.WithValue(ctx, singleLiveConnKey{}, s.conn)
		},
		Handler: http.HandlerFunc(s.serveHTTP),
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
		client.Close()
		engine.Close()
		ln.Close()
	})
	return s
}

func (s *singleLiveServer) stop() {
	s.engine.Close()
	s.ln.Close()
}

func (s *singleLiveServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	c, _ := r.Context().Value(singleLiveConnKey{}).(*Conn)
	s.entered <- r.URL.Path
	switch r.URL.Path {
	case "/small":
		singleLiveCheck(s.t, singleLiveNow(c, "small request"), 1, smallReaderSize)
		if c.smallReaderLoans != 1 || c.smallReaderReleases != 0 {
			s.t.Errorf("small request: loans/releases = %d/%d, want 1/0 for a first small round", c.smallReaderLoans, c.smallReaderReleases)
		}
	case "/demand":
		singleLiveCheck(s.t, singleLiveNow(c, "demand request entry"), 1, smallReaderSize)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			s.t.Errorf("demand request body: %v", err)
		}
		singleLiveCheck(s.t, singleLiveNow(c, "demand request after body"), 1, smallReaderSize)
	case "/after":
		singleLiveCheck(s.t, singleLiveNow(c, "request after promotion"), 0, bigReaderSize)
	case "/chunked":
		singleLiveCheck(s.t, singleLiveNow(c, "chunked request entry"), 0, bigReaderSize)
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			s.t.Errorf("chunked request body: %v", err)
		}
		singleLiveCheck(s.t, singleLiveNow(c, "chunked request after body"), 0, bigReaderSize)
	default:
		s.t.Errorf("unexpected request path %q", r.URL.Path)
	}
	w.WriteHeader(http.StatusOK)
}

func singleLiveNow(c *Conn, stage string) singleLiveSample {
	return singleLiveSample{
		stage:    stage,
		loans:    c.smallReaderLoans,
		releases: c.smallReaderReleases,
		bytes:    c.readerBytes,
	}
}

func (s *singleLiveServer) awaitPath(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-s.entered:
		if got != want {
			t.Fatalf("handler entered %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("handler did not enter %q", want)
	}
}

func singleLiveWrite(t *testing.T, c *net.TCPConn, req string) {
	t.Helper()
	if _, err := c.Write([]byte(req)); err != nil {
		t.Fatalf("write request: %v", err)
	}
}

func singleLiveRead(t *testing.T, c *net.TCPConn, method, what string) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("%s deadline: %v", what, err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: method})
	if err != nil {
		t.Fatalf("%s response: %v", what, err)
	}
	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		resp.Body.Close()
		t.Fatalf("%s body: %v", what, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s status = %q, want 200 OK", what, resp.Status)
	}
}
