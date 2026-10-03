package compat

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"
)

func readResponse(t *testing.T, br *bufio.Reader, method string) []byte {
	t.Helper()
	r, err := http.ReadResponse(br, &http.Request{Method: method})
	if err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDifferentialExpect100(t *testing.T) {
	run := func(own bool) []byte {
		addr, _ := launch(t, own, &http.Server{Handler: handlers(t)})
		c := dial(t, addr)
		io.WriteString(c, "POST /echo HTTP/1.1\r\nHost: t\r\nExpect: 100-continue\r\nContent-Length: 5\r\nConnection: close\r\n\r\n")
		br := bufio.NewReader(c)
		var interim bytes.Buffer
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			interim.WriteString(line)
			if line == "\r\n" {
				break
			}
		}
		if interim.String() != "HTTP/1.1 100 Continue\r\n\r\n" {
			t.Fatalf("interim %q", interim.String())
		}
		// Body is sent only after observing the exact interim event.
		io.WriteString(c, "hello")
		p, err := io.ReadAll(br)
		if err != nil {
			t.Fatal(err)
		}
		return normalize(append(interim.Bytes(), p...))
	}
	same(t, run(false), run(true))
}

func TestDifferentialPoolPoison(t *testing.T) {
	run := func(own bool) []byte {
		addr, _ := launch(t, own, &http.Server{Handler: handlers(t)})
		var p []byte
		for i := 0; i < 50; i++ {
			bad := capture(t, addr, "GARBAGE\r\n\r\n")
			if !bytes.Contains(bad, []byte("400 Bad Request")) {
				t.Fatal("malformed request not rejected")
			}
			good := capture(t, addr, request("GET", "/hello", true))
			if !bytes.HasSuffix(good, []byte("hello")) {
				t.Fatal("valid request corrupted")
			}
			p = append(p, bad...)
			p = append(p, good...)
		}
		return p
	}
	want, got := run(false), run(true)
	if !bytes.Equal(want, got) {
		t.Fatal("DIFF repeated malformed/good sequence")
	}
	t.Log("SAME 50 malformed + 50 valid requests")
}

func TestDifferentialIdleKeepAliveClose(t *testing.T) {
	run := func(own bool) []byte {
		closed := make(chan struct{}, 1)
		addr, _ := launch(t, own, &http.Server{Handler: handlers(t), ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				closed <- struct{}{}
			}
		}})
		c := dial(t, addr)
		io.WriteString(c, request("GET", "/hello", false))
		body := readResponse(t, bufio.NewReader(c), "GET")
		c.Close()
		signal(t, closed)
		return append(body, capture(t, addr, request("GET", "/hello", true))...)
	}
	same(t, run(false), run(true))
}

func TestDifferentialTimeouts(t *testing.T) {
	for _, name := range []string{"ReadHeaderTimeout", "IdleTimeout", "WriteTimeout"} {
		t.Run(name, func(t *testing.T) {
			run := func(own bool) []byte {
				idle := make(chan struct{}, 1)
				expired := make(chan struct{})
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if name == "WriteTimeout" {
						// An actual deadline expiry is the behavior under test.
						select {
						case <-time.After(80 * time.Millisecond):
						case <-r.Context().Done():
						}
						close(expired)
					}
					fmt.Fprint(w, "hello")
				})
				cfg := &http.Server{Handler: h, ConnState: func(_ net.Conn, s http.ConnState) {
					if s == http.StateIdle {
						idle <- struct{}{}
					}
				}}
				switch name {
				case "ReadHeaderTimeout":
					cfg.ReadHeaderTimeout = 30 * time.Millisecond
				case "IdleTimeout":
					cfg.IdleTimeout = 30 * time.Millisecond
				case "WriteTimeout":
					cfg.WriteTimeout = 30 * time.Millisecond
				}
				addr, _ := launch(t, own, cfg)
				c := dial(t, addr)
				var first []byte
				switch name {
				case "ReadHeaderTimeout":
					io.WriteString(c, "GET / HTTP/1.1\r\nHost:")
				case "IdleTimeout":
					io.WriteString(c, request("GET", "/hello", false))
					first = readResponse(t, bufio.NewReader(c), "GET")
					signal(t, idle)
				case "WriteTimeout":
					io.WriteString(c, request("GET", "/hello", true))
					signal(t, expired)
				}
				p, err := io.ReadAll(c)
				if err != nil {
					t.Fatal(err)
				}
				if len(p) != 0 {
					t.Fatalf("timed-out connection produced %q", p)
				}
				return append(first, p...)
			}
			same(t, run(false), run(true))
		})
	}
}

func TestDifferentialShutdownWaitsActive(t *testing.T) {
	run := func(own bool) []byte {
		entered, release, shutdown := make(chan struct{}), make(chan struct{}), make(chan struct{})
		addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-release
			fmt.Fprint(w, "finished")
		})})
		s.(interface{ RegisterOnShutdown(func()) }).RegisterOnShutdown(func() { close(shutdown) })
		c := dial(t, addr)
		io.WriteString(c, request("GET", "/hello", true))
		signal(t, entered)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- s.Shutdown(ctx) }()
		signal(t, shutdown)
		select {
		case err := <-done:
			t.Fatalf("Shutdown returned while handler active: %v", err)
		default:
		}
		close(release)
		p, err := io.ReadAll(c)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if !bytes.HasSuffix(p, []byte("finished")) {
			t.Fatalf("lost active response %q", p)
		}
		return normalize(p)
	}
	same(t, run(false), run(true))
}

func TestDifferentialConnStateSequence(t *testing.T) {
	run := func(own bool) []http.ConnState {
		var mu sync.Mutex
		var states []http.ConnState
		closed := make(chan struct{})
		idle := make(chan struct{}, 1)
		addr, _ := launch(t, own, &http.Server{Handler: handlers(t), ConnState: func(_ net.Conn, s http.ConnState) {
			mu.Lock()
			states = append(states, s)
			mu.Unlock()
			if s == http.StateIdle {
				idle <- struct{}{}
			}
			if s == http.StateClosed {
				close(closed)
			}
		}})
		c := dial(t, addr)
		br := bufio.NewReader(c)
		io.WriteString(c, request("GET", "/hello", false))
		readResponse(t, br, "GET")
		signal(t, idle)
		io.WriteString(c, request("GET", "/hello", true))
		readResponse(t, br, "GET")
		signal(t, closed)
		mu.Lock()
		defer mu.Unlock()
		want := []http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive, http.StateClosed}
		if !reflect.DeepEqual(states, want) {
			t.Fatalf("sequence %v", states)
		}
		return append([]http.ConnState(nil), states...)
	}
	want, got := run(false), run(true)
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("DIFF %v %v", want, got)
	}
	t.Logf("SAME %v", got)
}
