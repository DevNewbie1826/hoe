//go:build linux || darwin

package bench

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func fixture(t testing.TB, size int) (string, []byte) {
	t.Helper()
	p := bytes.Repeat([]byte("0123456789abcdef"), (size+15)/16)[:size]
	name := filepath.Join(t.TempDir(), "body.txt")
	if err := os.WriteFile(name, p, 0600); err != nil {
		t.Fatal(err)
	}
	return name, p
}

func fileHandler(t testing.TB, path string, size int, chunked bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f, err := os.Open(path)
		if err != nil {
			t.Error(err)
			return
		}
		defer f.Close()
		if chunked {
			w.Header().Set("Transfer-Encoding", "chunked")
		} else {
			w.Header().Set("Content-Length", strconv.Itoa(size))
		}
		if n, err := io.Copy(w, f); err != nil || n != int64(size) {
			t.Errorf("io.Copy(file)=%d,%v", n, err)
		}
	})
}

func verifyBody(t testing.TB, raw, want []byte, method string) {
	t.Helper()
	r, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(raw)), &http.Request{Method: method})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	got, err := io.ReadAll(r.Body)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("body bytes=%d want=%d err=%v status=%d", len(got), len(want), err, r.StatusCode)
	}
}

func TestSendfileUsed(t *testing.T) {
	path, data := fixture(t, 32<<10)
	for _, kind := range []string{"ServeFile", "CopyFile", "FileServerRange"} {
		t.Run(kind, func(t *testing.T) {
			// Given: std and hoe serve identical real regular-file sources.
			h := fileHandler(t, path, len(data), false)
			request := "GET /body.txt HTTP/1.1\r\nHost: t\r\nConnection: close\r\n"
			expected := data
			if kind == "ServeFile" {
				h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.ServeFile(w, r, path) })
			} else if kind == "FileServerRange" {
				h = http.FileServer(http.Dir(filepath.Dir(path)))
				request += "Range: bytes=1024-25599\r\n"
				expected = data[1024:25600]
			}
			request += "\r\n"
			std, _ := launch(t, false, h)
			own, _ := launch(t, true, h)
			want := rawResponse(t, std, request)
			before := systest.Snapshot()
			// When: the live hoe response is read completely.
			got := rawResponse(t, own, request)
			after := systest.Snapshot()
			// Then: both wire bytes and actual kernel-transfer accounting agree.
			if !bytes.Equal(want, got) {
				t.Fatal("sendfile output differs from std")
			}
			verifyBody(t, got, expected, "GET")
			if after.SendfileCalls <= before.SendfileCalls || after.SendfileBytes <= before.SendfileBytes {
				t.Fatalf("sendfile not used: before=%+v after=%+v", before, after)
			}
			t.Logf("SAME sendfile calls=%d bytes=%d", after.SendfileCalls-before.SendfileCalls, after.SendfileBytes-before.SendfileBytes)
		})
	}
}

func tlsResponse(t *testing.T, own bool, h http.Handler, request string) []byte {
	t.Helper()
	cert := httptest.NewTLSServer(http.NotFoundHandler())
	cfg := cert.TLS.Clone()
	cert.Close()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var s interface {
		ServeTLS(net.Listener, string, string) error
		Close() error
	} = &http.Server{Handler: h, TLSConfig: cfg}
	if own {
		s = &hoe.Server{Handler: h, TLSConfig: cfg}
	}
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(l, "", "") }()
	defer func() {
		s.Close()
		l.Close()
		if err := <-done; err != http.ErrServerClosed {
			t.Errorf("ServeTLS=%v", err)
		}
	}()
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", l.Addr().String(), &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return dateLine.ReplaceAll(p, []byte("Date: normalized\r\n"))
}

func TestSendfileNotUsed(t *testing.T) {
	for _, kind := range []string{"chunked", "HEAD", "small", "TLS"} {
		t.Run(kind, func(t *testing.T) {
			// Given: a standard exclusion from file zero-copy.
			size := 32 << 10
			if kind == "small" {
				size = (16 << 10) - 1
			}
			path, data := fixture(t, size)
			h := fileHandler(t, path, size, kind == "chunked")
			method := "GET"
			if kind == "HEAD" {
				method, data = "HEAD", nil
			}
			request := method + " / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
			var want, got []byte
			if kind == "TLS" {
				want = tlsResponse(t, false, h, request)
			} else {
				addr, _ := launch(t, false, h)
				want = rawResponse(t, addr, request)
			}
			before := systest.Snapshot()
			// When: hoe handles the excluded response, including TLS delegation.
			if kind == "TLS" {
				got = tlsResponse(t, true, h, request)
			} else {
				addr, _ := launch(t, true, h)
				got = rawResponse(t, addr, request)
			}
			// Then: bytes match and no sendfile call escaped the exclusion.
			if !bytes.Equal(want, got) {
				t.Fatalf("%s output differs from std", kind)
			}
			verifyBody(t, got, data, method)
			if delta := systest.Snapshot().SendfileCalls - before.SendfileCalls; delta != 0 {
				t.Fatalf("excluded %s used sendfile %d times", kind, delta)
			}
		})
	}
}

func BenchmarkFileCopy(b *testing.B) {
	path, data := fixture(b, 32<<10)
	for _, own := range []bool{false, true} {
		name := "std"
		if own {
			name = "hoe"
		}
		b.Run(name, func(b *testing.B) {
			addr, _ := launch(b, own, fileHandler(b, path, len(data), false))
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Minute))
			in := bufio.NewReader(c)
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := io.WriteString(c, get); err != nil {
					b.Fatal(err)
				}
				r, err := http.ReadResponse(in, nil)
				if err != nil {
					b.Fatal(err)
				}
				p, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || !bytes.Equal(p, data) {
					b.Fatal("invalid file response", err)
				}
			}
		})
	}
}
