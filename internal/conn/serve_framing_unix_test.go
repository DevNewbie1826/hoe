//go:build linux || darwin

package conn

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
)

// The first request on a fresh connection parses through the small reader.
// Chunked framing is bounded by that reader's capacity — the chunk decoder
// reads chunk-extension lines with ReadSlice and trailers are scanned with
// Peek up to the buffer size — so a first-request chunked body must run on
// the full-size reader. Both wire forms below fit the pre-compaction
// 4096-byte reader and must stay accepted, with the body preserved.
func TestFirstRequestChunkedFramingOnFreshConnection(t *testing.T) {
	const valueBytes = 1500
	value := strings.Repeat("x", valueBytes)

	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("%s: chunked framing rejected: %v", r.URL.Path, err)
			return
		}
		if r.URL.Path == "/trailer" && len(r.Trailer.Get("X-Check")) != valueBytes {
			t.Errorf("/trailer: X-Check length %d, want %d", len(r.Trailer.Get("X-Check")), valueBytes)
		}
		w.Write(body)
	})}
	engine, err := New(srv, ln, Policy{KeepAlive: func() bool { return true }})
	if err != nil {
		ln.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		engine.Close()
		ln.Close()
	})
	go engine.Serve(ln)

	for _, framing := range []struct {
		name string
		head string
		wire string
	}{
		{
			name: "trailer_value",
			head: "POST /trailer HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nTrailer: X-Check\r\n\r\n",
			wire: "1\r\nz\r\n0\r\nX-Check: " + value + "\r\n\r\n",
		},
		{
			name: "chunk_extension",
			head: "POST /extension HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n",
			wire: "1;ext=" + value + "\r\nz\r\n0\r\n\r\n",
		},
	} {
		t.Run(framing.name, func(t *testing.T) {
			client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			writeRaw(t, client, framing.head+framing.wire)
			resp, err := http.ReadResponse(bufio.NewReader(client), &http.Request{Method: http.MethodPost})
			if err != nil {
				t.Fatalf("server rejected request: %v", err)
			}
			got, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				t.Fatalf("read response body: %v", err)
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status %d, want %d", resp.StatusCode, http.StatusOK)
			}
			if string(got) != "z" {
				t.Fatalf("echoed body %q, want %q", got, "z")
			}
		})
	}
}
