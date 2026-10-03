package http1

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func parseRaw(raw []byte) (*http.Request, error) {
	in := bufio.NewReader(bytes.NewReader(raw))
	var head []byte
	for {
		line, err := in.ReadBytes('\n')
		head = append(head, line...)
		if err != nil || bytes.Equal(line, []byte("\r\n")) || bytes.Equal(line, []byte("\n")) {
			break
		}
	}
	return ParseHead(head, in)
}

func requestSnapshot(r *http.Request) []byte {
	p, err := io.ReadAll(r.Body)
	readErr := ""
	if err != nil {
		readErr = err.Error()
	}
	result := struct {
		Method, URI, Proto, Host string
		Major, Minor             int
		URL                      url.URL
		Header, Trailer          http.Header
		Length                   int64
		TE                       []string
		Close                    bool
		Body, Error              string
	}{r.Method, r.RequestURI, r.Proto, r.Host, r.ProtoMajor, r.ProtoMinor,
		*r.URL, r.Header, r.Trailer, r.ContentLength, r.TransferEncoding, r.Close, string(p), readErr}
	encoded, err := json.Marshal(result)
	if err != nil {
		panic(err)
	}
	return encoded
}

func compareRequest(t testing.TB, raw []byte) {
	t.Helper()
	want, we := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
	got, ge := parseRaw(raw)
	if (we == nil) != (ge == nil) {
		t.Fatalf("accept/reject DIFF input=%q std=%v codec=%v", raw, we, ge)
	}
	if we != nil {
		return
	}
	w, g := requestSnapshot(want), requestSnapshot(got)
	if !bytes.Equal(w, g) {
		t.Fatalf("request DIFF input=%q\nstd %s\ncodec %s", raw, w, g)
	}
}

func TestDifferentialRequestFields(t *testing.T) {
	for _, raw := range []string{
		"GET / HTTP/1.1\r\nHost: t\r\n\r\n",
		"GET /a? HTTP/1.0\r\nHost: t\r\nConnection: keep-alive\r\nPragma: no-cache\r\n\r\n",
		"GET http://example.test/a%2Fb?q=1 HTTP/1.1\r\nHost: ignored\r\n\r\n",
		"OPTIONS * HTTP/1.1\r\nHost: t\r\n\r\n",
		"CONNECT example.test:443 HTTP/1.1\r\n\r\n",
		"POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 3\r\n\r\nabcGET /next HTTP/1.1\r\n\r\n",
		"POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc",
		"POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabc",
		"POST / HTTP/1.1\r\nContent-Length: +3\r\n\r\nabc",
		"POST / HTTP/1.1\r\nContent-Length:\r\n\r\n",
		"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\nTrailer: X-End\r\n\r\n3\r\nabc\r\n0\r\nX-End: done\r\nX-Other: extra\r\n\r\n",
		"POST / HTTP/1.0\r\nTransfer-Encoding: nonsense\r\nContent-Length: 3\r\n\r\nabc",
		"GET / HTTP/1.1\nHost: t\nX-Fold: a\n b\n\n",
		"GET / HTTP/1.1\r\nHost: t\r\nX-Dup: a\r\nX-Dup: b\r\n\r\n",
		"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
		"PRI * HTTP/2.0\r\n\r\n",
	} {
		t.Run(strings.ReplaceAll(strings.Split(raw, "\n")[0], "\r", ""), func(t *testing.T) {
			compareRequest(t, []byte(raw))
		})
	}
}

func TestRequestOwnsHead(t *testing.T) {
	// Given: reusable transport storage.
	p := []byte("GET /kept HTTP/1.1\r\nHost: kept\r\nX-Name: retained\r\n\r\n")
	r, err := ParseHead(p, nil)
	if err != nil {
		t.Fatal(err)
	}
	// When: the caller overwrites its buffer.
	clear(p)
	// Then: all fields still refer to immutable request-owned data.
	if r.URL.Path != "/kept" || r.Host != "kept" || r.Header.Get("X-Name") != "retained" {
		t.Fatalf("retained request corrupted: %+v", r)
	}
}

func FuzzReadRequest(f *testing.F) {
	for _, s := range []string{
		"GET / HTTP/1.1\r\nHost: t\r\n\r\n",
		"POST / HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc",
		"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n3\r\nabc\r\n0\r\nX-End: done\r\n\r\n",
		"GET http://example/a%2Fb?q=1 HTTP/1.0\r\nPragma: no-cache\r\n\r\n",
		"CONNECT x:443 HTTP/1.1\r\n\r\n", "PRI * HTTP/2.0\r\n\r\n", "GARBAGE\r\n\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > 1<<20 {
			return
		}
		compareRequest(t, raw)
	})
}

var retainedRequest *http.Request

func BenchmarkParseHead(b *testing.B) {
	head := []byte("GET / HTTP/1.1\r\nHost: localhost\r\nUser-Agent: bench\r\nAccept: */*\r\n\r\n")
	b.ReportAllocs()
	for b.Loop() {
		var err error
		retainedRequest, err = ParseHead(head, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
