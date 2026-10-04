package compat

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestCloseNotify(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
			// Given a handler subscribed before the peer disconnects.
			entered, notified, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			addr, _ := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				ch := w.(http.CloseNotifier).CloseNotify()
				close(entered)
				select {
				case value := <-ch:
					if !value {
						t.Error("CloseNotify delivered false")
					}
					close(notified)
				case <-time.After(5 * time.Second):
					t.Error("CloseNotify did not signal")
				}
			})})
			c := dial(t, addr)
			io.WriteString(c, request("GET", "/", false))
			signal(t, entered)
			// When the established HTTP peer closes.
			c.Close()
			// Then the signal arrives while the handler is still running.
			signal(t, notified)
			signal(t, returned)
		})
	}
}

func TestResponseControllerFullDuplex(t *testing.T) {
	run := func(own bool) []byte {
		addr, _ := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rc := http.NewResponseController(w)
			if err := rc.EnableFullDuplex(); err != nil {
				t.Error(err)
				return
			}
			var first [1]byte
			if _, err := io.ReadFull(r.Body, first[:]); err != nil {
				t.Error(err)
				return
			}
			fmt.Fprintf(w, "first:%s", first[:])
			if err := rc.Flush(); err != nil {
				t.Error(err)
				return
			}
			rest, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
				return
			}
			fmt.Fprintf(w, "second:%s", rest)
		})})
		// Given a chunked request whose tail has not yet been transmitted.
		c := dial(t, addr)
		io.WriteString(c, "POST / HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n1\r\na\r\n")
		r, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "POST"})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		first := make([]byte, len("first:a"))
		// When the response is flushed before the remaining request body.
		if _, err = io.ReadFull(r.Body, first); err != nil {
			t.Fatal(err)
		}
		if string(first) != "first:a" {
			t.Fatalf("first=%q", first)
		}
		io.WriteString(c, "2\r\nbc\r\n0\r\n\r\n")
		rest, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		// Then both interleaved segments are complete and correctly framed.
		if string(rest) != "second:bc" {
			t.Fatalf("tail=%q", rest)
		}
		return append(first, rest...)
	}
	same(t, run(false), run(true))
}

func TestResponseControllerDeadlines(t *testing.T) {
	for _, op := range []string{"read", "write"} {
		t.Run(op, func(t *testing.T) {
			for _, own := range []bool{false, true} {
				t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
					result := make(chan error, 1)
					addr, _ := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						rc := http.NewResponseController(w)
						if op == "read" {
							if err := rc.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
								result <- err
								return
							}
							var p [1]byte
							_, err := r.Body.Read(p[:])
							result <- err
						} else {
							if err := rc.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
								result <- err
								return
							}
							fmt.Fprint(w, "deadline")
							result <- rc.Flush()
						}
					})})
					c := dial(t, addr)
					raw := request("GET", "/", true)
					if op == "read" {
						raw = "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 1\r\nConnection: close\r\n\r\n"
					}
					io.WriteString(c, raw)
					select {
					case err := <-result:
						network, ok := err.(net.Error)
						if !ok || !network.Timeout() {
							t.Fatalf("%s deadline returned %T %v", op, err, err)
						}
						t.Logf("%s observed timeout: %v", op, err)
					case <-time.After(5 * time.Second):
						t.Fatal("deadline did not unblock operation")
					}
					c.Close()
				})
			}
		})
	}
}
