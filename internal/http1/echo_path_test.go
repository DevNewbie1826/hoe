//go:build linux || darwin

package http1

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"runtime"
	"testing"
)

type allocTransport struct {
	out *bufio.Writer
}

func (t *allocTransport) AcquireResponseOutput() *bufio.Writer { return t.out }
func (t *allocTransport) GrowResponseOutput() *bufio.Writer    { return t.out }
func (t *allocTransport) ReleaseResponseOutput()               {}
func (t *allocTransport) SetResponseCleanup(func())            {}
func (t *allocTransport) Write(p []byte) (int, error)          { return t.out.Write(p) }
func (t *allocTransport) Read(p []byte) (int, error)           { return 0, net.ErrClosed }

var echoHead = "POST /echo HTTP/1.1\r\n" +
	"Host: 127.0.0.1:10301\r\n" +
	"User-Agent: go-http-client/1.1\r\n" +
	"Content-Type: application/octet-stream\r\n" +
	"Content-Length: 1024\r\n" +
	"Accept-Encoding: gzip\r\n" +
	"\r\n"

var (
	contentTypeValue = []string{"application/octet-stream"}
	lengthValue      = []string{"1024"}
)

// echoRound runs one request the way the connection layer does: parse the
// head, answer it, and release the response buffers for the next request.
func echoRound(tb testing.TB, br *bufio.Reader, payload []byte, out *bufio.Writer, tr *allocTransport, held []byte) {
	tb.Helper()
	br.Reset(bytes.NewReader(payload))
	req, err := ReadRequest(br, 0)
	if err != nil {
		tb.Fatal(err)
	}
	w := NewResponse(req, br, out, tr)
	w.Header()["Content-Type"] = contentTypeValue
	w.Header()["Content-Length"] = lengthValue
	if _, err := io.ReadFull(req.Body, held); err != nil {
		tb.Fatal(err)
	}
	if _, err := w.Write(held); err != nil {
		tb.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		tb.Fatal(err)
	}
	w.releaseBuffer()
	out.Reset(io.Discard)
}

func BenchmarkEchoPath(b *testing.B) {
	out := bufio.NewWriter(io.Discard)
	tr := &allocTransport{out: out}
	payload := append([]byte(echoHead), make([]byte, 1024)...)
	br := bufio.NewReaderSize(bytes.NewReader(payload), 4096)
	held := make([]byte, 1024)
	b.ReportAllocs()
	for b.Loop() {
		echoRound(b, br, payload, out, tr, held)
	}
}

func TestEchoPathAllocs(t *testing.T) {
	out := bufio.NewWriter(io.Discard)
	tr := &allocTransport{out: out}
	payload := append([]byte(echoHead), make([]byte, 1024)...)
	br := bufio.NewReaderSize(bytes.NewReader(payload), 4096)
	held := make([]byte, 1024)
	n := testing.AllocsPerRun(500, func() {
		echoRound(t, br, payload, out, tr, held)
	})
	t.Logf("allocs_per_request=%.2f", n)
}

// echoBytesPerRequest returns exact allocated bytes per request. TotalAlloc is
// an exact counter, so this needs no sampling: the reader and writer are reused
// across the run, leaving only what one request costs.
func echoBytesPerRequest(t *testing.T, rounds, per int) float64 {
	out := bufio.NewWriter(io.Discard)
	tr := &allocTransport{out: out}
	payload := append([]byte(echoHead), make([]byte, 1024)...)
	br := bufio.NewReaderSize(bytes.NewReader(payload), 4096)
	held := make([]byte, 1024)
	run := func(n int) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range n {
			echoRound(t, br, payload, out, tr, held)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	run(per)
	best := float64(1 << 62)
	for range rounds {
		if b := float64(run(per)) / float64(per); b < best {
			best = b
		}
	}
	return best
}
