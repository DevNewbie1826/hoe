package compat

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGuardReadTimeoutCoversBody(t *testing.T) {
	run := func(own bool) []byte {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.Copy(io.Discard, r.Body)
			fmt.Fprint(w, "late")
		})
		addr, _ := launch(t, own, &http.Server{Handler: h, ReadTimeout: 200 * time.Millisecond})
		c := dial(t, addr)
		// Read deadline only: a write deadline would close the socket and fake the server timeout.
		if err := c.SetDeadline(time.Time{}); err != nil {
			t.Fatal(err)
		}
		if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(c, "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 8\r\nConnection: close\r\n\r\nX"); err != nil {
			t.Fatal(err)
		}
		p, err := io.ReadAll(c)
		if err != nil {
			t.Fatalf("own=%v stalled body was not bounded by ReadTimeout: %v", own, err)
		}
		return p
	}
	same(t, run(false), run(true))
}

func TestGuardWriteTimeoutResetsPerRequest(t *testing.T) {
	const (
		budget = 400 * time.Millisecond
		work   = 250 * time.Millisecond
	)
	run := func(own bool) []byte {
		var n atomic.Int32
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := n.Add(1)
			time.Sleep(work)
			fmt.Fprintf(w, "req-%d", id)
		})
		addr, _ := launch(t, own, &http.Server{Handler: h, WriteTimeout: budget})
		c := dial(t, addr)
		br := bufio.NewReader(c)
		var got []byte
		for range 2 {
			if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			got = append(got, readResponse(t, br, "GET")...)
		}
		return got
	}
	want := run(false)
	got := run(true)
	if string(want) != "req-1req-2" {
		t.Fatalf("std keep-alive WriteTimeout reset produced %q", want)
	}
	same(t, want, got)

	// Enforcement, not deadline bookkeeping: a cleared write deadline still
	// serves both bodies above. A write, a wait past WriteTimeout, and a
	// second write must not deliver the late bytes.
	cut := func(own bool) {
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "early")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(250 * time.Millisecond)
			fmt.Fprint(w, "late-write")
		})
		addr, _ := launch(t, own, &http.Server{Handler: h, WriteTimeout: 50 * time.Millisecond})
		c := dial(t, addr)
		if _, err := io.WriteString(c, request("GET", "/", true)); err != nil {
			t.Fatal(err)
		}
		p, err := io.ReadAll(c)
		got := string(p)
		if strings.Contains(got, "late-write") {
			t.Fatalf("own=%v WriteTimeout did not cut the late write: %q", own, got)
		}
		if !strings.Contains(got, "early") {
			t.Fatalf("own=%v early write not delivered: %q err=%v", own, got, err)
		}
	}
	cut(false)
	cut(true)
}
