package http1

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type copyTransport struct {
	bytes.Buffer
	calls int
}

func (c *copyTransport) ReadFrom(r io.Reader) (int64, error) {
	c.calls++
	return io.Copy(&c.Buffer, r)
}

func TestReadFromHook(t *testing.T) {
	for _, tc := range []struct {
		name, method string
		size         int
		length       int
		limited      bool
		wantHook     bool
	}{
		{"large", "GET", 32768, 32768, false, true},
		{"small", "GET", 1024, 1024, false, false},
		{"head", "HEAD", 32768, 32768, false, false},
		{"chunked", "GET", 32768, -1, false, false},
		{"overflow", "GET", 32768, 8192, false, true},
		{"limitedOverflow", "GET", 32768, 8192, true, true},
		{"limitedExact", "GET", 32768, 32768, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given: a real regular file and a hook-capable bounded transport.
			data := strings.Repeat("x", tc.size)
			path := filepath.Join(t.TempDir(), "body")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			f, err := os.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			c := new(copyTransport)
			r, err := ParseHead([]byte(get(tc.method, "/", false)), nil)
			if err != nil {
				t.Fatal(err)
			}
			w := NewResponse(r, nil, bufio.NewWriter(c), c)
			if tc.length >= 0 {
				w.Header().Set("Content-Length", fmt.Sprint(tc.length))
			}
			var src io.Reader = f
			if tc.limited {
				src = &io.LimitedReader{R: f, N: int64(tc.size)}
			}
			// When: ReadFrom consumes that source and Finish emits the framing.
			n, copyErr := w.ReadFrom(src)
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			// Then: bytes, EOF, source accounting, and hook selection are correct.
			if (c.calls > 0) != tc.wantHook {
				t.Fatalf("hook calls=%d", c.calls)
			}
			if tc.length >= 0 && tc.length < tc.size {
				if n != int64(tc.length) || copyErr != http.ErrContentLength || !w.CloseAfterReply() {
					t.Fatalf("overflow n=%d err=%v close=%t", n, copyErr, w.CloseAfterReply())
				}
				if lr, ok := src.(*io.LimitedReader); ok && lr.N != int64(tc.size-tc.length-1) {
					t.Fatalf("LimitedReader N=%d", lr.N)
				}
			} else if copyErr != nil || n != int64(tc.size) {
				t.Fatalf("copy=%d,%v", n, copyErr)
			}
			response, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(c.Bytes())), &http.Request{Method: tc.method})
			if err != nil {
				t.Fatal(err)
			}
			p, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			want := data
			if tc.method == "HEAD" {
				want = ""
			} else if tc.length >= 0 {
				want = want[:tc.length]
			}
			if string(p) != want {
				t.Fatalf("body length=%d expected=%d", len(p), len(want))
			}
		})
	}
}

func TestResponseControllerDeadlines(t *testing.T) {
	// Given: real blocking pipe transport.
	c, peer := net.Pipe()
	defer c.Close()
	defer peer.Close()
	r, err := ParseHead([]byte(get("GET", "/", false)), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(r, bufio.NewReader(c), bufio.NewWriter(c), c)
	controller := http.NewResponseController(w)
	// When: each deadline is already expired (no timing-based synchronization).
	if err := controller.SetReadDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("read did not time out")
	}
	if err := controller.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(w, "body")
	// Then: controller flushing observes the real transport timeout.
	err = controller.Flush()
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatalf("flush=%v", err)
	}
}

func TestResponseControllerFullDuplex(t *testing.T) {
	// Given: a body not consumed yet and an independent output transport.
	in := bufio.NewReader(strings.NewReader("POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 3\r\n\r\nabc"))
	r, err := ReadRequest(in, 0)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	w := NewResponse(r, in, bufio.NewWriter(&out), &out)
	// When: full duplex is enabled before flushing.
	if err := http.NewResponseController(w).EnableFullDuplex(); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	// Then: the handler can still consume the original body after the response.
	p, err := io.ReadAll(r.Body)
	if err != nil || string(p) != "abc" {
		t.Fatalf("duplex body=%q err=%v", p, err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
}

func TestDifferentialCorpusFields(t *testing.T) {
	for _, tc := range codecCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write(requestSnapshot(r))
			})
			want := wire(t, false, tc.raw, tc.maxHeader, h)
			got := wire(t, true, tc.raw, tc.maxHeader, h)
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF corpus fields\nstd %q\ncodec %q", want, got)
			}
			t.Log("SAME request fields and consumed trailers")
		})
	}
}
