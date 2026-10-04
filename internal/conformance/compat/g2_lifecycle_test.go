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

func TestRegisterOnShutdown(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
			// Given registered callbacks which must not delay Shutdown.
			callback, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "ready") })})
			// Establish that serving has started before shutting down.
			capture(t, addr, request("GET", "/", true))
			s.(interface{ RegisterOnShutdown(func()) }).RegisterOnShutdown(func() {
				close(callback)
				<-release
				close(finished)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// When Shutdown is called.
			err := s.Shutdown(ctx)
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			// Then the callback ran, but its completion was not awaited.
			signal(t, callback)
			signal(t, finished)
		})
	}
}

func TestSetKeepAlivesEnabledFalse(t *testing.T) {
	t.Run("new_response", func(t *testing.T) {
		run := func(own bool) []byte {
			addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "close") })})
			s.(interface{ SetKeepAlivesEnabled(bool) }).SetKeepAlivesEnabled(false)
			p := capture(t, addr, request("GET", "/", false))
			if !bytes.Contains(p, []byte("Connection: close\r\n")) {
				t.Fatalf("missing close header: %q", p)
			}
			return p
		}
		same(t, run(false), run(true))
	})
	t.Run("existing_idle_connection", func(t *testing.T) {
		for _, own := range []bool{false, true} {
			t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
				idle := make(chan struct{}, 1)
				addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "idle") }),
					ConnState: func(_ net.Conn, state http.ConnState) {
						if state == http.StateIdle {
							idle <- struct{}{}
						}
					}})
				c := dial(t, addr)
				io.WriteString(c, request("GET", "/", false))
				readResponse(t, bufio.NewReader(c), "GET")
				signal(t, idle)
				// When keep-alive is disabled on an existing idle connection.
				s.(interface{ SetKeepAlivesEnabled(bool) }).SetKeepAlivesEnabled(false)
				var p [1]byte
				if n, err := c.Read(p[:]); n != 0 || err != io.EOF {
					t.Fatalf("idle not closed: n=%d err=%v", n, err)
				}
			})
		}
	})
}

func TestConnStateSequence(t *testing.T) {
	for _, scenario := range []string{"reuse", "malformed", "hijack", "shutdown_idle"} {
		t.Run(scenario, func(t *testing.T) {
			run := func(own bool) []http.ConnState {
				var mu sync.Mutex
				var states []http.ConnState
				var identity net.Conn
				idle, terminal := make(chan struct{}, 2), make(chan struct{})
				cfg := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if scenario == "hijack" {
						c, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							return
						}
						c.Close()
					} else {
						fmt.Fprint(w, "state")
					}
				}), ConnState: func(c net.Conn, state http.ConnState) {
					mu.Lock()
					if identity == nil {
						identity = c
					}
					if identity != c {
						t.Error("ConnState changed net.Conn identity")
					}
					states = append(states, state)
					mu.Unlock()
					if state == http.StateIdle {
						idle <- struct{}{}
					}
					if state == http.StateClosed || state == http.StateHijacked {
						close(terminal)
					}
				}}
				addr, s := launch(t, own, cfg)
				c := dial(t, addr)
				if scenario == "malformed" {
					io.WriteString(c, "GARBAGE\r\n\r\n")
					io.ReadAll(c)
					c.Close()
				} else {
					io.WriteString(c, request("GET", "/", false))
					if scenario != "hijack" {
						br := bufio.NewReader(c)
						readResponse(t, br, "GET")
						signal(t, idle)
						if scenario == "reuse" {
							io.WriteString(c, request("GET", "/", true))
							readResponse(t, br, "GET")
						} else {
							ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
							defer cancel()
							if err := s.Shutdown(ctx); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				signal(t, terminal)
				mu.Lock()
				defer mu.Unlock()
				want := []http.ConnState{http.StateNew, http.StateActive, http.StateClosed}
				switch scenario {
				case "reuse":
					want = []http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateActive, http.StateClosed}
				case "hijack":
					want = []http.ConnState{http.StateNew, http.StateActive, http.StateHijacked}
				case "shutdown_idle":
					want = []http.ConnState{http.StateNew, http.StateActive, http.StateIdle, http.StateClosed}
				}
				if !reflect.DeepEqual(states, want) {
					t.Fatalf("%s states=%v want=%v", scenario, states, want)
				}
				return append([]http.ConnState(nil), states...)
			}
			want, got := run(false), run(true)
			if !reflect.DeepEqual(want, got) {
				t.Fatalf("DIFF states std=%v hoe=%v", want, got)
			}
			t.Logf("SAME %s states=%v", scenario, got)
		})
	}
}

func TestBaseConnContextChain(t *testing.T) {
	type key string
	run := func(own bool) []byte {
		cfg := &http.Server{}
		var listenerAddr string
		cfg.BaseContext = func(l net.Listener) context.Context {
			listenerAddr = l.Addr().String()
			return context.WithValue(context.Background(), key("base"), "base")
		}
		cfg.ConnContext = func(ctx context.Context, c net.Conn) context.Context {
			if ctx.Value(http.LocalAddrContextKey) != nil {
				t.Error("LocalAddrContextKey installed before ConnContext")
			}
			if ctx.Value(key("base")) != "base" {
				t.Error("base value missing in ConnContext")
			}
			if _, ok := ctx.Value(http.ServerContextKey).(*http.Server); !ok {
				t.Error("ServerContextKey missing in ConnContext")
			}
			return context.WithValue(context.WithValue(ctx, key("conn"), "conn"), key("identity"), c)
		}
		cfg.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			c, ok := ctx.Value(key("identity")).(net.Conn)
			s, serverOK := ctx.Value(http.ServerContextKey).(*http.Server)
			addr, addrOK := ctx.Value(http.LocalAddrContextKey).(net.Addr)
			valid := ok && serverOK && addrOK && c.RemoteAddr().String() == r.RemoteAddr &&
				addr.String() == listenerAddr && c.LocalAddr().String() == listenerAddr && s.Handler != nil
			fmt.Fprintf(w, "base=%v conn=%v identity=%t", ctx.Value(key("base")), ctx.Value(key("conn")), valid)
		})
		addr, _ := launch(t, own, cfg)
		c := dial(t, addr)
		io.WriteString(c, request("GET", "/", true))
		body := readResponse(t, bufio.NewReader(c), "GET")
		if string(body) != "base=base conn=conn identity=true" {
			t.Fatalf("chain=%q", body)
		}
		return body
	}
	same(t, run(false), run(true))
}
