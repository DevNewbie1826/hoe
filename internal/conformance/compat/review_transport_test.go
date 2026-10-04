//go:build linux || darwin

package compat

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

func TestReviewTransportCallerListenerClose(t *testing.T) {
	for _, active := range []bool{false, true} {
		for _, own := range []bool{false, true} {
			t.Run(fmt.Sprintf("active=%t/hoe=%t", active, own), func(t *testing.T) {
				// Given a ready server and an existing reusable or active connection.
				beforeFD, beforeBuffers := fdCount(t), bufpool.Outstanding()
				before := engineconn.SnapshotResources()
				entered, release := make(chan struct{}), make(chan struct{})
				var releaseOnce sync.Once
				cfg := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/active" {
						close(entered)
						<-release
						fmt.Fprintf(w, "context=%v", r.Context().Err())
						return
					}
					fmt.Fprint(w, "ready")
				})}
				var s server = cfg
				if own {
					s = hoeConfig(cfg)
				}
				l, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- s.Serve(l) }()
				joined := false
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(release) })
					s.Close()
					l.Close()
					if !joined {
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							t.Error("Serve leaked")
						}
					}
				})
				c := dial(t, l.Addr().String())
				in := bufio.NewReader(c)
				if _, err = io.WriteString(c, request("GET", "/", false)); err != nil {
					t.Fatal(err)
				}
				if p := readResponse(t, in, "GET"); string(p) != "ready" {
					t.Fatalf("readiness body=%q", p)
				}
				if active {
					if _, err = io.WriteString(c, request("GET", "/active", true)); err != nil {
						t.Fatal(err)
					}
					signal(t, entered)
				}
				// When the caller closes its listener, not the server.
				if err = l.Close(); err != nil {
					t.Fatalf("caller listener Close: %v", err)
				}
				select {
				case err = <-done:
					joined = true
					if !errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
						t.Fatalf("external-close Serve error=%v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("caller listener Close did not unblock Serve")
				}
				// Then new dials fail while existing connections remain usable.
				next, dialErr := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
				if dialErr == nil {
					next.Close()
					t.Fatal("listener still accepts after caller Close")
				}
				want := "ready"
				if active {
					want = "context=<nil>"
					releaseOnce.Do(func() { close(release) })
				} else if _, err = io.WriteString(c, request("GET", "/", true)); err != nil {
					t.Fatal(err)
				}
				if p := readResponse(t, in, "GET"); string(p) != want {
					t.Fatalf("existing connection body=%q want=%q", p, want)
				}
				c.Close()
				if active {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err = s.Shutdown(ctx)
					cancel()
				} else {
					err = s.Close()
				}
				if err != nil {
					t.Fatal(err)
				}
				if own && (fdCount(t) != beforeFD || bufpool.Outstanding() != beforeBuffers || engineconn.SnapshotResources() != before) {
					t.Fatal("caller-close cleanup did not restore resource baseline")
				}
				t.Log("SAME listener Close, Serve error, refused dial, existing connection, cleanup")
			})
		}
	}
}

func TestReviewTransportCompleteHeads(t *testing.T) {
	for _, tc := range []struct{ name, prefix, suffix string }{
		{"crlf_control", request("GET", "/", true), ""},
		{"lf_control", "GET / HTTP/1.1\nHost: t\nConnection: close\n\n", ""},
		{"lf_crlf_blank", "GET / HTTP/1.1\nHost: t\nConnection: close\n\r\n", ""},
		{"split_lf_crlf_blank", "GET / HTTP/1.1\nHost: t\nConnection: close\n\r", "\n"},
		{"mixed_pipeline", "GET / HTTP/1.1\nHost: t\n\r\n" + request("GET", "/next", true), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := func(own bool) []byte {
				// Given a real server with no read timeout.
				active := make(chan struct{}, 2)
				addr, _ := launch(t, own, &http.Server{
					Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "next") }),
					ConnState: func(_ net.Conn, state http.ConnState) {
						if state == http.StateActive {
							active <- struct{}{}
						}
					},
				})
				c := dial(t, addr)
				// When a complete accepted head arrives with the write side open.
				if _, err := io.WriteString(c, tc.prefix); err != nil {
					t.Fatal(err)
				}
				if tc.suffix != "" {
					if own {
						signal(t, active)
					}
					if _, err := io.WriteString(c, tc.suffix); err != nil {
						t.Fatal(err)
					}
				}
				p, err := io.ReadAll(c)
				if err != nil {
					t.Fatalf("hoe=%t response=%q error=%v", own, p, err)
				}
				return normalize(p)
			}
			// Then the raw responses match, including mixed-style pipelines.
			same(t, run(false), run(true))
		})
	}
}

func TestReviewTransportOptionsLimitPipeline(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, size := range []int{4096, 4097} {
			t.Run(fmt.Sprintf("disabled=%t/bytes=%d", disabled, size), func(t *testing.T) {
				run := func(own bool) []byte {
					// Given the default OPTIONS handler or its disabled control.
					addr, _ := launch(t, own, &http.Server{DisableGeneralOptionsHandler: disabled,
						Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "next") })})
					c := dial(t, addr)
					raw := fmt.Sprintf("OPTIONS * HTTP/1.1\r\nHost: t\r\nContent-Length: %d\r\n\r\n", size) +
						strings.Repeat("x", size) + request("GET", "/next", true)
					// When the boundary body and a pipelined GET arrive together.
					if _, err := io.WriteString(c, raw); err != nil {
						t.Fatal(err)
					}
					p, err := io.ReadAll(c)
					if err != nil {
						t.Fatal(err)
					}
					return normalize(p)
				}
				want, got := run(false), run(true)
				// Then overflow closes before the pipeline; exact limit reuses.
				same(t, want, got)
				count := 2
				if !disabled && size == 4097 {
					count = 1
				}
				if n := bytes.Count(got, []byte("HTTP/1.1 200 OK\r\n")); n != count {
					t.Fatalf("responses=%d want=%d", n, count)
				}
			})
		}
	}
}
