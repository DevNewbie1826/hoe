//go:build linux || darwin

package http1

import (
	"bufio"
	"bytes"
	"io"
	"runtime"
	"testing"
)

var (
	budgetContentType = []string{"application/octet-stream"}
	budgetLength      = []string{"1024"}
)

// headCost measures exact allocated bytes per echo request for a handler that
// either does or does not set response headers before WriteHeader. TotalAlloc
// is an exact counter, so no sampling is involved.
func headCost(t *testing.T, setHeaders bool) float64 {
	t.Helper()
	body := make([]byte, 1024)
	out := bufio.NewWriter(io.Discard)
	tr := &allocTransport{out: out}
	payload := append([]byte(echoHead), body...)
	br := bufio.NewReaderSize(bytes.NewReader(payload), 4096)
	var held [1024]byte
	run := func(n int) uint64 {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range n {
			br.Reset(bytes.NewReader(payload))
			req, err := ReadRequest(br, 0)
			if err != nil {
				t.Fatal(err)
			}
			w := NewResponse(req, br, out, tr)
			if setHeaders {
				w.Header()["Content-Type"] = budgetContentType
				w.Header()["Content-Length"] = budgetLength
			}
			if _, err := io.ReadFull(req.Body, held[:]); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(held[:]); err != nil {
				t.Fatal(err)
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			w.releaseBuffer()
			out.Reset(io.Discard)
		}
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	const per = 2000
	run(per)
	best := float64(1 << 62)
	for range 3 {
		if b := float64(run(per)) / per; b < best {
			best = b
		}
	}
	return best
}

// TestHeaderSnapshotDoesNotAllocate pins the snapshot to steady-state zero
// allocations: after the first freeze the pooled header is reused, so later
// requests must not build a fresh map or value slices.
func TestHeaderSnapshotDoesNotAllocate(t *testing.T) {
	br := bufio.NewReaderSize(bytes.NewReader([]byte("GET / HTTP/1.1\r\nHost: t\r\n\r\n")), 4096)
	req, err := ReadRequest(br, 0)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(req, br, bufio.NewWriter(io.Discard), io.Discard)
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", "1024")
	h.Set("Connection", "keep-alive")
	h.Set("X-A", "1")
	h.Set("X-B", "2")
	h.Set("X-C", "3")
	w.freezeHeaders()

	allocs := testing.AllocsPerRun(200, func() {
		h["Content-Length"][0] = "1024"
		w.freezeHeaders()
	})
	if allocs > 0 {
		t.Fatalf("freezing committed headers allocates %.1f objects/op, want 0", allocs)
	}

	plain, withHeaders := headCost(t, false), headCost(t, true)
	t.Logf("plain=%.0f B/req withHeaders=%.0f B/req delta=%.0f", plain, withHeaders, withHeaders-plain)
	w.releaseSnapshot()
}

// TestHeaderSnapshotFrozenAfterWrite pins that late headers still do not
// reach the wire while the snapshot is reused.
func TestHeaderSnapshotFrozenAfterWrite(t *testing.T) {
	var out bytes.Buffer
	bw := bufio.NewWriter(&out)
	br := bufio.NewReaderSize(bytes.NewReader([]byte("GET / HTTP/1.1\r\nHost: t\r\n\r\n")), 4096)
	req, err := ReadRequest(br, 0)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(req, br, bw, bw)
	w.Header().Set("X-Frozen", "before")
	w.WriteHeader(201)
	w.Header().Set("X-Frozen", "after")
	w.Header().Set("X-Late", "not-on-wire")
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	head, _, _ := bytes.Cut(out.Bytes(), []byte("\r\n\r\n"))
	got := string(head)
	if !bytes.Contains(head, []byte("X-Frozen: before")) {
		t.Fatalf("missing frozen value in %q", got)
	}
	if bytes.Contains(head, []byte("X-Frozen: after")) || bytes.Contains(head, []byte("X-Late")) {
		t.Fatalf("headers set after WriteHeader reached the wire: %q", got)
	}
}
