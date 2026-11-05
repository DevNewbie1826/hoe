//go:build linux || darwin

package http1

import (
	"bufio"
	"bytes"
	"io"
	"testing"
)

var (
	budgetContentType = []string{"application/octet-stream"}
	budgetLength      = []string{"1024"}
)

// headCost returns exact allocated bytes per request with and without committed
// response headers. TotalAlloc is an exact counter, so no sampling is involved.
func headCost(t *testing.T, setHeaders bool) float64 {
	t.Helper()
	out := bufio.NewWriter(io.Discard)
	tr := &allocTransport{out: out}
	payload := append([]byte(echoHead), make([]byte, 1024)...)
	br := bufio.NewReaderSize(bytes.NewReader(payload), 4096)
	held := make([]byte, 1024)
	round := func() {
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
		if _, err := io.ReadFull(req.Body, held); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Write(held); err != nil {
			t.Fatal(err)
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		w.releaseBuffer()
		out.Reset(io.Discard)
	}
	return bytesPerRound(t, round, 3, 2000)
}

// TestCommittedHeaderSnapshotBudget pins what committing response headers
// costs in bytes. Cloning the handler map adds roughly 850 B per request on this
// path; a pooled snapshot costs about half that.
func TestCommittedHeaderSnapshotBudget(t *testing.T) {
	plain := headCost(t, false)
	committed := headCost(t, true)
	delta := committed - plain
	t.Logf("plain=%.0f B/req committed=%.0f B/req delta=%.0f", plain, committed, delta)
	if delta > 600 {
		t.Fatalf("committing response headers costs %.0f B/req over a headerless response, want <= 600: the snapshot is being cloned per request", delta)
	}
}

// TestHeaderSnapshotFrozenAfterWrite pins standard-library write semantics: a
// header set after WriteHeader must not reach the wire.
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
