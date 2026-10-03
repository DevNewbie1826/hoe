package compat

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

type wireCase struct {
	name, raw string
	status    int
	halfClose bool
	maxHeader int
}

func request(method, path string, close bool) string {
	h := method + " " + path + " HTTP/1.1\r\nHost: t\r\n"
	if close {
		h += "Connection: close\r\n"
	}
	return h + "\r\n"
}

func corpus() []wireCase {
	post := func(path, extra, body string) string {
		return fmt.Sprintf("POST %s HTTP/1.1\r\nHost: t\r\nConnection: close\r\n%sContent-Length: %d\r\n\r\n%s", path, extra, len(body), body)
	}
	return []wireCase{
		{"HEAD", request("HEAD", "/hello", true), 200, false, 0},
		{"HTTP10", "GET /hello HTTP/1.0\r\n\r\n", 200, false, 0},
		{"Early103", request("GET", "/early", true), 103, false, 0},
		{"Expect100", post("/echo", "Expect: 100-continue\r\n", "hello"), 100, false, 0},
		{"CLshort", "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nabcd", 200, true, 0},
		{"CLlong", "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nABCDEFGHIJKLMNOPQRST" + request("GET", "/hello", true), 200, true, 0},
		{"204", request("GET", "/nocontent", true), 204, false, 0},
		{"Trailer", request("GET", "/trailer", true), 200, false, 0},
		{"CtxKeys", request("GET", "/ctxkeys", true), 200, false, 0},
		{"CtxCancelAfterReturn", request("GET", "/store", false) + request("GET", "/report", true), 200, false, 0},
		{"CtxGoroutines", request("GET", "/goroutines", true), 200, false, 0},
		{"Hdr9KB", "GET /hello HTTP/1.1\r\nHost: t\r\nConnection: close\r\nX-Big: " + strings.Repeat("a", 9000) + "\r\n\r\n", 200, false, 0},
		{"Malformed", "GARBAGE\r\n\r\n", 400, false, 0},
		{"NoHost", "GET /hello HTTP/1.1\r\n\r\n", 400, false, 0},
		{"Pipelining", request("GET", "/hello", false) + request("GET", "/hello", true), 200, false, 0},
		{"ChunkedReq", "POST /echo HTTP/1.1\r\nHost: t\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n5\r\nhello\r\n0\r\nX-End: done\r\n\r\n", 200, false, 0},
		{"ConnClose", request("GET", "/hello", true), 200, false, 0},
		{"ServeFile", request("GET", "/file", true), 200, false, 0},
		{"ServeContentRange", "GET /content HTTP/1.1\r\nHost: t\r\nConnection: close\r\nRange: bytes=2-5\r\n\r\n", 206, false, 0},
		{"FileServerHEAD", request("HEAD", "/static/data.txt", true), 200, false, 0},
		{"CopyFile", request("GET", "/copy", true), 200, false, 0},
		{"FlushStreaming", request("GET", "/flush", true), 200, false, 0},
		{"HijackBytes", request("GET", "/hijack", false) + "already-buffered", 0, true, 0},
		{"431", "GET /hello HTTP/1.1\r\nHost: t\r\nX-Big: " + strings.Repeat("a", 9000) + "\r\n\r\n", 431, false, 1024},
		{"505", "GET /hello HTTP/2.0\r\nHost: t\r\n\r\n", 505, false, 0},
		{"MaxBytesReader", post("/max", "", "0123456789"), 200, false, 0},
		{"TrailerPrefix", request("GET", "/prefix", true), 200, false, 0},
		{"Form", post("/form?name=query&tag=qs", "Content-Type: application/x-www-form-urlencoded\r\n", "name=body&tag=body1&tag=b2"), 200, false, 0},
		{"AbsoluteURI", "GET http://example.test/a%2Fb?q=1 HTTP/1.1\r\nHost: ignored\r\nConnection: close\r\n\r\n", 200, false, 0},
		{"OptionsStar", "OPTIONS * HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n", 200, false, 0},
	}
}

func handlers(t *testing.T) http.Handler {
	t.Helper()
	dir := t.TempDir()
	data := strings.Repeat("0123456789abcdef", 2048)
	file := filepath.Join(dir, "data.txt")
	if err := os.WriteFile(file, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	mtime := time.Unix(1700000000, 0)
	if err := os.Chtimes(file, mtime, mtime); err != nil {
		t.Fatal(err)
	}
	var stored context.Context
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/early":
			w.Header().Set("Link", "</style.css>; rel=preload")
			w.WriteHeader(103)
			w.Header().Set("Link", "</final.css>; rel=preload")
			fmt.Fprint(w, "final")
		case "/echo":
			p, err := io.ReadAll(r.Body)
			fmt.Fprintf(w, "%s|err=%v", p, err)
		case "/nocontent":
			w.WriteHeader(204)
		case "/trailer":
			w.Header().Set("Trailer", "X-Checksum")
			fmt.Fprint(w, "hello")
			w.Header().Set("X-Checksum", "abc")
		case "/prefix":
			fmt.Fprint(w, "hello")
			w.(http.Flusher).Flush()
			w.Header().Set(http.TrailerPrefix+"X-Checksum", "abc")
		case "/ctxkeys":
			_, serverOK := r.Context().Value(http.ServerContextKey).(*http.Server)
			_, addrOK := r.Context().Value(http.LocalAddrContextKey).(interface{ String() string })
			fmt.Fprintf(w, "server=%t local=%t", serverOK, addrOK)
		case "/store":
			stored = r.Context()
			fmt.Fprint(w, "stored")
		case "/report":
			fmt.Fprintf(w, "ctxErr=%v", stored.Err())
		case "/goroutines":
			// Count only propagation goroutines, so unrelated server cleanup
			// cannot make the process-wide NumGoroutine delta nondeterministic.
			watchers := func() int {
				p := make([]byte, 1<<20)
				n := runtime.Stack(p, true)
				return bytes.Count(p[:n], []byte("created by context.(*cancelCtx).propagateCancel"))
			}
			before := watchers()
			cs := make([]context.CancelFunc, 100)
			for i := range cs {
				_, cs[i] = context.WithCancel(r.Context())
			}
			fmt.Fprintf(w, "delta=%d", watchers()-before)
			for _, cancel := range cs {
				cancel()
			}
		case "/file":
			http.ServeFile(w, r, file)
		case "/content":
			http.ServeContent(w, r, "data.txt", mtime, strings.NewReader(data))
		case "/static/data.txt":
			http.StripPrefix("/static/", http.FileServer(http.Dir(dir))).ServeHTTP(w, r)
		case "/copy":
			f, err := os.Open(file)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer f.Close()
			w.Header().Set("Content-Length", fmt.Sprint(len(data)))
			io.Copy(w, f)
		case "/flush":
			fmt.Fprint(w, "one")
			w.(http.Flusher).Flush()
			fmt.Fprint(w, "two")
		case "/hijack":
			c, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			defer c.Close()
			p := make([]byte, len("already-buffered"))
			if _, err := io.ReadFull(rw, p); err != nil {
				return
			}
			fmt.Fprintf(rw, "hijack:%s", p)
			rw.Flush()
		case "/max":
			r.Body = http.MaxBytesReader(w, r.Body, 8)
			io.ReadAll(r.Body)
			fmt.Fprint(w, "ok")
		case "/form":
			err := r.ParseForm()
			fmt.Fprintf(w, "form=%v post=%v err=%v", r.Form, r.PostForm, err)
		default:
			fmt.Fprint(w, "hello")
		}
	})
}

func exerciseWire(t *testing.T, addr string, tc wireCase) []byte {
	t.Helper()
	c := dial(t, addr)
	if _, err := io.WriteString(c, tc.raw); err != nil {
		t.Fatal(err)
	}
	if tc.halfClose {
		if err := c.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}
	p, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	p = normalize(p)
	if tc.status != 0 && !bytes.Contains(p, []byte(fmt.Sprintf(" %d ", tc.status))) {
		t.Fatalf("missing status %d: %q", tc.status, p)
	}
	return p
}

func TestDifferential(t *testing.T) {
	for _, tc := range corpus() {
		t.Run(tc.name, func(t *testing.T) {
			std, _ := launch(t, false, &http.Server{Handler: handlers(t), MaxHeaderBytes: tc.maxHeader})
			own, _ := launch(t, true, &http.Server{Handler: handlers(t), MaxHeaderBytes: tc.maxHeader})
			want := exerciseWire(t, std, tc)
			got := exerciseWire(t, own, tc)
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF\nstd %q\nhoe %q", want, got)
			}
			t.Logf("SAME %d raw bytes (Date only normalized)", len(got))
		})
	}
}
