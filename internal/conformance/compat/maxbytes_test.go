package compat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/gate"
	"github.com/go-chi/chi/v5/middleware"
)

var dateLine = regexp.MustCompile(`(?m)^Date: [^\r]*\r$`)

func normalize(p []byte) []byte { return dateLine.ReplaceAll(p, []byte("Date: DATE\r")) }

type running struct {
	addr string
	stop func()
}

func startGate(t *testing.T, h http.Handler) (running, *gate.Server) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &gate.Server{Handler: h}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	stop := func() { s.Close(); l.Close(); <-done }
	t.Cleanup(stop)
	return running{l.Addr().String(), stop}, s
}

func startStd(t *testing.T, h http.Handler) running {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &http.Server{Handler: h}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() { s.Close(); l.Close(); <-done })
	return running{addr: l.Addr().String()}
}

func dial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

// capture preserves raw framing. The final pipeline request closes the socket.
func capture(t *testing.T, addr, raw string) []byte {
	t.Helper()
	c := dial(t, addr)
	if _, err := io.WriteString(c, raw); err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return normalize(p)
}

func maxHandler(local, flush, chi bool) http.Handler {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/next" {
			fmt.Fprint(w, "next")
			return
		}
		if !chi {
			b := http.MaxBytesReader(w, r.Body, 8)
			if !local {
				r.Body = b
			}
			if flush {
				http.NewResponseController(w).EnableFullDuplex()
				w.(http.Flusher).Flush()
			}
			io.ReadAll(b)
		} else {
			io.ReadAll(r.Body)
		}
		fmt.Fprint(w, "ok")
	})
	if chi {
		return middleware.RequestSize(8)(h)
	}
	return h
}

func maxRequest(n int) string {
	return fmt.Sprintf("POST /limit HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n%sGET /next HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n", n, strings.Repeat("x", n))
}

func TestMaxBytesFeasibility(t *testing.T) {
	for _, tc := range []struct {
		name       string
		n          int
		chi, flush bool
	}{
		{"below", 7, false, false}, {"exact", 8, false, false}, {"over", 9, false, false},
		{"ignored_error", 32, false, false}, {"chi_RequestSize", 32, true, false}, {"after_flush", 32, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := maxHandler(false, tc.flush, tc.chi)
			std := startStd(t, h)
			own, s := startGate(t, h)
			want := capture(t, std.addr, maxRequest(tc.n))
			got := capture(t, own.addr, maxRequest(tc.n))
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF\nstd %q\nhoe %q", want, got)
			}
			wantCount := int64(0)
			if tc.n > 8 {
				wantCount = 1
			}
			if s.Detections.Load() != wantCount {
				t.Fatalf("reflect count=%d want=%d", s.Detections.Load(), wantCount)
			}
			if bytes.Contains(got, []byte("next")) != (tc.n <= 8) {
				t.Fatal("pipeline reuse incorrect")
			}
			t.Logf("SAME bytes=%q reflect=%d", got, s.Detections.Load())
		})
	}
}

func TestMaxBytesLocalVariableException(t *testing.T) {
	h := maxHandler(true, false, false)
	std := capture(t, startStd(t, h).addr, maxRequest(32))
	own, s := startGate(t, h)
	got := capture(t, own.addr, maxRequest(32))
	if !bytes.Contains(std, []byte("Connection: close")) || bytes.Contains(std, []byte("next")) {
		t.Fatalf("std exception baseline %q", std)
	}
	// Parse both responses to prove that unread body did not corrupt the next one.
	br := bufio.NewReader(bytes.NewReader(got))
	for _, body := range []string{"ok", "next"} {
		r, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		p, err := io.ReadAll(r.Body)
		r.Body.Close()
		if err != nil || string(p) != body {
			t.Fatalf("body=%q err=%v", p, err)
		}
	}
	if s.Detections.Load() != 0 {
		t.Fatal("local wrapper became observable")
	}
	t.Logf("approved exception: std=%q hoe=%q reflect=0", std, got)
}
