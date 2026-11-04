//go:build linux || darwin

package conn

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"
)

// halfOldReader is half the pre-compaction 4096-byte request buffer. An idle
// parked request that still holds that buffer fails this bound; the bound is
// not read from smallReaderSize.
const halfOldReader = 2048

// oldReaderBytes is the pre-compaction request buffer. Growth is observed
// when the next parked request holds one of these, not when a flag flips.
const oldReaderBytes = 4096

// TestParkedSmallRequestRetainsSmallReader pins idle reader retention on the
// real engine. One parked one-line GET must hold the small serve reader.
func TestParkedSmallRequestRetainsSmallReader(t *testing.T) {
	srv, client := parkedServer(t)
	defer srv.stop()
	withAllocProfile(t)

	writeRaw(t, client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, srv.entered)
	got := serveReaderBytes()
	if got == 0 || got >= halfOldReader {
		t.Fatalf("parked small request retained %d serve-reader bytes, want in (0, %d)", got, halfOldReader)
	}
	srv.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("small request status %q", s)
	}
}

// TestReaderGrowsAfterBodyBeyondSmall pins growth on a real body read. A
// 2048-byte body fills a 1024-byte reader and does not fill a 4096-byte one;
// the next parked request must hold the full buffer.
func TestReaderGrowsAfterBodyBeyondSmall(t *testing.T) {
	srv, client := parkedServer(t)
	defer srv.stop()
	withAllocProfile(t)

	writeRaw(t, client, "GET /small HTTP/1.1\r\nHost: t\r\n\r\n")
	waitPark(t, srv.entered)
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
	got := serveReaderBytes()
	if got < oldReaderBytes {
		t.Fatalf("request after a beyond-small body retained %d serve-reader bytes, want at least %d", got, oldReaderBytes)
	}
	srv.release <- struct{}{}
	if s := readStatus(t, client, "GET").Status; s != "200 OK" {
		t.Fatalf("request after growth status %q", s)
	}
}

type parked struct {
	engine  *Engine
	ln      *net.TCPListener
	entered chan struct{}
	release chan struct{}
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
	p := &parked{entered: make(chan struct{}), release: make(chan struct{})}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			io.Copy(io.Discard, r.Body)
		}
		if r.Method == http.MethodPost {
			return
		}
		p.entered <- struct{}{}
		<-p.release
	})}
	engine, err := New(srv, ln, Policy{KeepAlive: func() bool { return true }})
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

func withAllocProfile(t *testing.T) {
	t.Helper()
	prev := runtime.MemProfileRate
	runtime.MemProfileRate = 1
	t.Cleanup(func() { runtime.MemProfileRate = prev })
	runtime.GC()
	runtime.GC()
}

func serveReaderBytes() int64 {
	runtime.GC()
	runtime.GC()
	var recs []runtime.MemProfileRecord
	for {
		n, ok := runtime.MemProfile(recs, false)
		if ok {
			recs = recs[:n]
			break
		}
		recs = make([]runtime.MemProfileRecord, n+16)
	}
	var sum int64
	for i := range recs {
		if recs[i].InUseBytes() == 0 {
			continue
		}
		var serve, reader bool
		frames := runtime.CallersFrames(recs[i].Stack())
		for {
			fr, more := frames.Next()
			if strings.Contains(fr.File, "serve_unix.go") {
				serve = true
			}
			if strings.Contains(fr.Function, "NewReaderSize") {
				reader = true
			}
			if !more {
				break
			}
		}
		if serve && reader {
			sum += recs[i].InUseBytes()
		}
	}
	return sum
}
