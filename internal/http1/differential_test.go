package http1

import (
	"bytes"
	"context"
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

func get(method, path string, closeConn bool) string {
	closeHeader := ""
	if closeConn {
		closeHeader = "Connection: close\r\n"
	}
	return method + " " + path + " HTTP/1.1\r\nHost: t\r\n" + closeHeader + "\r\n"
}

func post(path, headers, body string) string {
	return fmt.Sprintf("POST %s HTTP/1.1\r\nHost: t\r\nConnection: close\r\n%sContent-Length: %d\r\n\r\n%s", path, headers, len(body), body)
}

type codecCase struct {
	name, raw string
	maxHeader int
}

func codecCorpus() []codecCase {
	return []codecCase{
		{"HEAD", get("HEAD", "/hello", true), 0},
		{"HTTP10", "GET /hello HTTP/1.0\r\n\r\n", 0},
		{"HTTP10KeepAlive", "GET /hello HTTP/1.0\r\nConnection: keep-alive\r\n\r\n" + get("GET", "/hello", true), 0},
		{"Early103", get("GET", "/early", true), 0},
		{"Expect100", post("/echo", "Expect: 100-continue\r\n", "hello"), 0},
		{"ExpectRejected", post("/echo", "Expect: unsupported\r\n", "hello"), 0},
		{"ExpectReadAfterHeader", post("/expect-final", "Expect: 100-continue\r\n", "hello"), 0},
		{"ExpectReadAfterHeaderPipeline", "POST /expect-final HTTP/1.1\r\nHost: t\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello" + get("GET", "/hello", true), 0},
		{"ExpectClosedBody", "POST /expect-close HTTP/1.1\r\nHost: t\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello" + get("GET", "/hello", true), 0},
		{"PostPadding", "POST /hello HTTP/1.1\r\nHost: t\r\nContent-Length: 3\r\n\r\nabc\r\n" + get("GET", "/hello", true), 0},
		{"CLshort", "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nabcd", 0},
		{"CLlong", "POST /echo HTTP/1.1\r\nHost: t\r\nContent-Length: 10\r\n\r\nABCDEFGHIJKLMNOPQRST" + get("GET", "/hello", true), 0},
		{"204", get("GET", "/nocontent", true), 0},
		{"304", get("GET", "/notmodified", true), 0},
		{"Trailer", get("GET", "/trailer", true), 0},
		{"TrailerPrefix", get("GET", "/prefix", true), 0},
		{"CtxKeys", get("GET", "/ctxkeys", true), 0},
		{"CtxCancelAfterReturn", get("GET", "/store", false) + get("GET", "/report", true), 0},
		{"CtxGoroutines", get("GET", "/goroutines", true), 0},
		{"Hdr9KB", "GET /hello HTTP/1.1\r\nHost: t\r\nConnection: close\r\nX-Big: " + strings.Repeat("a", 9000) + "\r\n\r\n", 0},
		{"Malformed", "GARBAGE\r\n\r\n", 0},
		{"NoHost", "GET /hello HTTP/1.1\r\n\r\n", 0},
		{"BadHost", "GET /hello HTTP/1.1\r\nHost: bad host\r\n\r\n", 0},
		{"BadField", "GET /hello HTTP/1.1\r\nHost: t\r\nBad Field: a\r\n\r\n", 0},
		{"Pipelining", get("GET", "/hello", false) + get("GET", "/hello", true), 0},
		{"MixedNewlinePipeline", "GET /hello HTTP/1.1\nHost: t\n\n" + get("GET", "/hello", true), 0},
		{"MixedBlankLinePipeline", "GET /hello HTTP/1.1\r\nHost: t\r\n\n" + get("GET", "/hello", true), 0},
		{"ChunkedReq", "POST /echo HTTP/1.1\r\nHost: t\r\nConnection: close\r\nTransfer-Encoding: chunked\r\nTrailer: X-End\r\n\r\n5\r\nhello\r\n0\r\nX-End: done\r\n\r\n", 0},
		{"ConnClose", get("GET", "/hello", true), 0},
		{"ServeFile", get("GET", "/file", true), 0},
		{"ServeContentRange", "GET /content HTTP/1.1\r\nHost: t\r\nConnection: close\r\nRange: bytes=2-5\r\n\r\n", 0},
		{"FileServerHEAD", get("HEAD", "/static/data.txt", true), 0},
		{"CopyFile", get("GET", "/copy", true), 0},
		{"FlushStreaming", get("GET", "/flush", true), 0},
		{"HijackBytes", get("GET", "/hijack", false) + "already-buffered", 0},
		{"431", "GET /hello HTTP/1.1\r\nHost: t\r\nX-Big: " + strings.Repeat("a", 9000) + "\r\n\r\n", 1024},
		{"501", "POST /hello HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: gzip\r\n\r\n", 0},
		{"505", "GET /hello HTTP/2.0\r\nHost: t\r\n\r\n", 0},
		{"MaxBytesReader", post("/max", "", "0123456789"), 0},
		{"Form", post("/form?name=query&tag=qs", "Content-Type: application/x-www-form-urlencoded\r\n", "name=body&tag=body1&tag=b2"), 0},
		{"AbsoluteURI", "GET http://example.test/a%2Fb?q=1 HTTP/1.1\r\nHost: ignored\r\nConnection: close\r\n\r\n", 0},
		{"OptionsStar", get("OPTIONS", "*", true), 0},
	}
}

func codecHandlers(t *testing.T) http.Handler {
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
		case "/expect-final":
			w.WriteHeader(200)
			p, err := io.ReadAll(r.Body)
			fmt.Fprintf(w, "%s|err=%v", p, err)
		case "/expect-close":
			err := r.Body.Close()
			fmt.Fprintf(w, "close=%v", err)
		case "/nocontent":
			w.WriteHeader(204)
		case "/notmodified":
			w.Header().Set("Content-Type", "removed")
			w.Header().Set("Content-Length", "19")
			w.WriteHeader(304)
		case "/trailer":
			w.Header().Set("Trailer", "X-Checksum")
			fmt.Fprint(w, "hello")
			w.Header().Set("X-Checksum", "abc")
		case "/prefix":
			fmt.Fprint(w, "hello")
			w.(http.Flusher).Flush()
			w.Header().Set(http.TrailerPrefix+"X-Checksum", "abc")
		case "/ctxkeys":
			_, server := r.Context().Value(http.ServerContextKey).(*http.Server)
			_, local := r.Context().Value(http.LocalAddrContextKey).(net.Addr)
			fmt.Fprintf(w, "server=%t local=%t", server, local)
		case "/store":
			stored = r.Context()
			fmt.Fprint(w, "stored")
		case "/report":
			fmt.Fprintf(w, "ctxErr=%v", stored.Err())
		case "/goroutines":
			for range 100 {
				_, cancel := context.WithCancel(r.Context())
				cancel()
			}
			fmt.Fprint(w, "contexts")
		case "/file":
			http.ServeFile(w, r, file)
		case "/content":
			http.ServeContent(w, r, "data.txt", mtime, strings.NewReader(data))
		case "/static/data.txt":
			http.StripPrefix("/static/", http.FileServer(http.Dir(dir))).ServeHTTP(w, r)
		case "/copy":
			f, err := os.Open(file)
			if err != nil {
				t.Error(err)
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
				t.Error(err)
				return
			}
			defer c.Close()
			p := make([]byte, len("already-buffered"))
			if _, err := io.ReadFull(rw, p); err != nil {
				t.Error(err)
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

func TestDifferential(t *testing.T) {
	for _, tc := range codecCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			want := wire(t, false, tc.raw, tc.maxHeader, codecHandlers(t))
			got := wire(t, true, tc.raw, tc.maxHeader, codecHandlers(t))
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF\nstd %q\ncodec %q", want, got)
			}
			if len(got) == 0 {
				t.Fatal("empty proof")
			}
			t.Logf("SAME %d raw bytes (Date only normalized)", len(got))
		})
	}
}
