package http1

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestBodyDrainPipeline(t *testing.T) {
	for _, size := range []int{0, 3, MaxDrainBytes - 1, MaxDrainBytes, MaxDrainBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			// Given: an unread body followed by the next request.
			raw := fmt.Sprintf("POST / HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n%sGET /next HTTP/1.1\r\nHost: t\r\n\r\n", size, strings.Repeat("x", size))
			in := bufio.NewReader(strings.NewReader(raw))
			r, err := ReadRequest(in, 0)
			if err != nil {
				t.Fatal(err)
			}
			// When: the handler closes its body.
			if err := r.Body.Close(); err != nil {
				t.Fatal(err)
			}
			// Then: only within-budget requests are reusable.
			if b, ok := r.Body.(*Body); ok && b.EarlyClosed() != (size > MaxDrainBytes) {
				t.Fatalf("earlyClose=%t size=%d", b.EarlyClosed(), size)
			}
			if size <= MaxDrainBytes {
				next, err := ReadRequest(in, 0)
				if err != nil || next.URL.Path != "/next" {
					t.Fatalf("next=%v err=%v", next, err)
				}
			}
		})
	}
}

func TestBodyChunkedTrailerPipeline(t *testing.T) {
	in := bufio.NewReader(strings.NewReader("POST / HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n3\r\nabc\r\n0\r\nX-End: done\r\n\r\nGET /next HTTP/1.1\r\nHost: t\r\n\r\n"))
	r, err := ReadRequest(in, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if r.Trailer.Get("X-End") != "done" {
		t.Fatalf("trailer=%v", r.Trailer)
	}
	next, err := ReadRequest(in, 0)
	if err != nil || next.URL.Path != "/next" {
		t.Fatalf("pipeline=%v err=%v", next, err)
	}
}

func TestBodyReadAfterClose(t *testing.T) {
	r, err := parseRaw([]byte("POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc"))
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Body.Read(make([]byte, 1)); err != http.ErrBodyReadAfterClose {
		t.Fatalf("Read after Close=%v", err)
	}
}

func TestBodyShortContentLength(t *testing.T) {
	r, err := parseRaw([]byte("POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabcd"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r.Body)
	if string(p) != "abcd" || err != io.ErrUnexpectedEOF {
		t.Fatalf("body=%q err=%v", p, err)
	}
}

func TestBodyChunkedDrainLimit(t *testing.T) {
	for _, size := range []int{MaxDrainBytes - 1, MaxDrainBytes, MaxDrainBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			raw := fmt.Sprintf("POST / HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n0\r\n\r\n", size, strings.Repeat("x", size))
			// A large caller-owned buffer permits the chunk decoder to return
			// terminal EOF together with the last bytes of the drain budget.
			in := bufio.NewReaderSize(strings.NewReader(raw), len(raw)+1)
			r, err := ReadRequest(in, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.Body.Close(); err != nil {
				t.Fatal(err)
			}
			if r.Body.(*Body).EarlyClosed() != (size > MaxDrainBytes) {
				t.Fatalf("chunked drain size=%d early=%t", size, r.Body.(*Body).EarlyClosed())
			}
		})
	}
}
