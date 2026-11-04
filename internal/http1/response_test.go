package http1

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type responseErrorWriter struct{ err error }

func (w responseErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestFlushErrorReleasesEmptyBuffer(t *testing.T) {
	wantErr := errors.New("transport failure")
	transport := responseErrorWriter{err: wantErr}
	r, err := ParseHead([]byte(get("GET", "/", false)), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(r, nil, bufio.NewWriter(transport), transport)
	if _, err := w.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushError(); !errors.Is(err, wantErr) {
		t.Fatalf("flush error=%v", err)
	}
	if w.buffer != nil {
		t.Fatal("empty response buffer retained after failed output flush")
	}
	if n, err := w.Write([]byte("after")); n != 5 || err != nil {
		t.Fatalf("buffered write after flush error=%d,%v", n, err)
	}
	if err := w.Finish(); !errors.Is(err, wantErr) {
		t.Fatalf("finish error=%v", err)
	}
}

func TestFlushReleasesEmptyBufferWithStickyError(t *testing.T) {
	wantErr := errors.New("transport failure")
	transport := responseErrorWriter{err: wantErr}
	r, err := ParseHead([]byte(get("HEAD", "/", false)), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(r, nil, bufio.NewWriter(transport), transport)
	w.WriteHeader(http.StatusEarlyHints)
	if _, err := w.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushError(); !errors.Is(err, wantErr) {
		t.Fatalf("flush error=%v", err)
	}
	if w.buffer != nil {
		t.Fatal("empty response buffer retained after sticky write error")
	}
	if n, err := w.Write([]byte("after")); n != 0 || !errors.Is(err, wantErr) {
		t.Fatalf("write after sticky buffer error=%d,%v", n, err)
	}
	if err := w.Finish(); !errors.Is(err, wantErr) {
		t.Fatalf("finish error=%v", err)
	}
}

func TestFlushReleasesEmptyBuffer(t *testing.T) {
	var output bytes.Buffer
	r, err := ParseHead([]byte(get("GET", "/", false)), nil)
	if err != nil {
		t.Fatal(err)
	}
	w := NewResponse(r, nil, bufio.NewWriter(&output), &output)
	if _, err := w.Write([]byte("before")); err != nil {
		t.Fatal(err)
	}
	if err := w.FlushError(); err != nil {
		t.Fatal(err)
	}
	if w.buffer != nil {
		t.Fatal("empty response buffer retained after flush")
	}
	if _, err := w.Write([]byte("after")); err != nil {
		t.Fatal(err)
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(&output), r)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "beforeafter" {
		t.Fatalf("body=%q err=%v", body, err)
	}
}

func TestDifferentialResponseRules(t *testing.T) {
	cases := []struct {
		name, method string
		fn           func(http.ResponseWriter, *http.Request)
	}{
		{"empty", "GET", func(w http.ResponseWriter, r *http.Request) {}},
		{"emptyHEAD", "HEAD", func(w http.ResponseWriter, r *http.Request) {}},
		{"zeroWrite204", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(204)
			w.Write(nil)
		}},
		{"headersFrozen", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Frozen", "before")
			w.WriteHeader(201)
			w.Header().Set("X-Frozen", "after")
			io.WriteString(w, "body")
		}},
		{"headerAfterWrite", "GET", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "body")
			w.Header().Set("X-Late", "not-on-wire")
		}},
		{"nilTypeDateLength", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header()["Content-Type"] = nil
			w.Header()["Date"] = nil
			w.Header()["Content-Length"] = nil
			fmt.Fprint(w, "body")
		}},
		{"encodedNoSniff", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Encoding", "gzip")
			fmt.Fprint(w, "body")
		}},
		{"identity", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Transfer-Encoding", "identity")
			fmt.Fprint(w, "body")
		}},
		{"lengthAndTransfer", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Transfer-Encoding", "chunked")
			fmt.Fprint(w, "body")
		}},
		{"invalidLength", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "invalid")
			fmt.Fprint(w, "body")
		}},
		{"responseCLshort", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "10")
			fmt.Fprint(w, "short")
		}},
		{"responseCLlong", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "3")
			w.Write([]byte("too long"))
		}},
		{"multiple1xx", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "4")
			w.WriteHeader(102)
			w.WriteHeader(103)
			fmt.Fprint(w, "body")
		}},
		{"customStatus", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(299)
			fmt.Fprint(w, "body")
		}},
		{"explicit204", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "kept")
			w.Header().Set("Content-Length", "4")
			w.Header().Set("Transfer-Encoding", "chunked")
			w.WriteHeader(204)
		}},
		{"switch101", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Connection", "Upgrade")
			w.Header().Set("Upgrade", "example")
			w.WriteHeader(101)
		}},
		{"trailerBeforeHeader", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Trailer", "X-End")
			w.Header().Set("X-End", "excluded")
			fmt.Fprint(w, "body")
			w.Header().Set("X-End", "final")
		}},
		{"prefixBeforeHeader", "GET", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(http.TrailerPrefix+"X-End", "trailer")
			fmt.Fprint(w, "body")
		}},
		{"largeHEAD", "HEAD", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strings.Repeat("x", 8192))
		}},
		{"buffer2048", "GET", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strings.Repeat("x", 2048))
		}},
		{"buffer2049", "GET", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strings.Repeat("x", 2049))
		}},
		{"multiWriteBuffer", "GET", func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, strings.Repeat("x", 1000))
			io.WriteString(w, strings.Repeat("y", 5000))
		}},
		{"noBodyRead", "GET", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "ok")
		}},
		{"hijackBuffered", "GET", func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "discarded")
			c, rw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			io.WriteString(rw, "hijacked")
			rw.Flush()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(tc.fn)
			raw := get(tc.method, "/", true)
			want, got := wire(t, false, raw, 0, h), wire(t, true, raw, 0, h)
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF\nstd %q\ncodec %q", want, got)
			}
			t.Log("SAME")
		})
	}
}

func TestResponseWriteErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		length string
		err    error
	}{{"noBody", 204, "", http.ErrBodyNotAllowed}, {"tooLong", 200, "1", http.ErrContentLength}} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			r, _ := ParseHead([]byte(get("GET", "/", false)), nil)
			w := NewResponse(r, nil, bufio.NewWriter(&output), &output)
			if tc.length != "" {
				w.Header().Set("Content-Length", tc.length)
			}
			w.WriteHeader(tc.status)
			n, err := w.Write([]byte("ab"))
			if n != 0 || err != tc.err {
				t.Fatalf("Write=%d,%v", n, err)
			}
			if err := w.Finish(); err != nil {
				t.Fatal(err)
			}
			if tc.length != "" && !w.CloseAfterReply() {
				t.Fatal("CL overflow allowed pipeline reuse")
			}
		})
	}
}

func TestMaxBytes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		size      int
		flush     bool
		header    bool
		localOnly bool
	}{{"below", 7, false, false, false}, {"exact", 8, false, false, false},
		{"over", 9, false, false, false}, {"ignoredError", 32, false, false, false},
		{"afterFlush", 32, true, false, false}, {"afterLogicalHeader", 32, false, true, false},
		{"localVariable", 32, false, false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			detected := 0
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/next" {
					fmt.Fprint(w, "next")
					return
				}
				b := http.MaxBytesReader(w, r.Body, 8)
				if !tc.localOnly {
					r.Body = b
				}
				if tc.flush {
					http.NewResponseController(w).EnableFullDuplex()
					w.(http.Flusher).Flush()
				}
				if tc.header {
					w.WriteHeader(200)
				}
				io.ReadAll(b)
				fmt.Fprint(w, "ok")
			})
			raw := fmt.Sprintf("POST /limit HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n%s", tc.size, strings.Repeat("x", tc.size)) + get("GET", "/next", true)
			want := wire(t, false, raw, 0, h)
			got := wire(t, true, raw, 0, h, func(w *Response) {
				if w.LimitHit() {
					detected++
				}
			})
			if !tc.localOnly {
				if !bytes.Equal(want, got) {
					t.Fatalf("DIFF\nstd %q\ncodec %q", want, got)
				}
				if bytes.Contains(got, []byte("next")) != (tc.size <= 8) {
					t.Fatal("overflow pipeline policy wrong")
				}
				expected := 0
				if tc.size > 8 {
					expected = 1
				}
				if detected != expected {
					t.Fatalf("G1 reflection detections=%d want=%d", detected, expected)
				}
				t.Log("SAME")
			} else {
				in := bufio.NewReader(bytes.NewReader(got))
				for _, expected := range []string{"ok", "next"} {
					r, err := http.ReadResponse(in, nil)
					if err != nil {
						t.Fatal(err)
					}
					p, err := io.ReadAll(r.Body)
					r.Body.Close()
					if err != nil || string(p) != expected {
						t.Fatalf("approved exception body=%q err=%v", p, err)
					}
				}
				if detected != 0 || bytes.Contains(want, []byte("next")) {
					t.Fatal("local-only baseline changed")
				}
				t.Log("approved local-variable exception: intact pipeline")
			}
		})
	}
}
