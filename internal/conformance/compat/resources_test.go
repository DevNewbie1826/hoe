//go:build linux || darwin

package compat

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	"github.com/DevNewbie1826/hoe/internal/bufpool"
	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

func fdCount(t *testing.T) int {
	t.Helper()
	path := "/proc/self/fd"
	if runtime.GOOS == "darwin" {
		path = "/dev/fd"
	}
	fds, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	return len(fds)
}

func TestResourcesReturnToBaseline(t *testing.T) {
	for _, kind := range []string{"aborted_header", "aborted_body", "normal_close", "keep_alive_idle", "hijack_handoff", "shutdown"} {
		t.Run(kind, func(t *testing.T) {
			runtime.GC()
			beforeFD, beforeG, beforeBuffers := fdCount(t), runtime.NumGoroutine(), bufpool.Outstanding()
			beforeEngine := engineconn.SnapshotResources()
			closed, idle, entered, returned := make(chan struct{}, 1), make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
			s := &hoe.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				close(entered)
				if kind == "hijack_handoff" {
					c, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					c.Close()
					return
				}
				if kind == "aborted_body" {
					io.Copy(io.Discard, r.Body)
					return
				}
				fmt.Fprint(w, "resource")
			}), ConnState: func(_ net.Conn, state http.ConnState) {
				if state == http.StateClosed || state == http.StateHijacked {
					closed <- struct{}{}
				}
				if state == http.StateIdle {
					idle <- struct{}{}
				}
			}}
			l, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- s.Serve(l) }()
			c, err := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
			if err != nil {
				s.Close()
				l.Close()
				<-done
				t.Fatal(err)
			}
			c.SetDeadline(time.Now().Add(5 * time.Second))
			cleaned := false
			defer func() {
				if !cleaned {
					c.Close()
					s.Close()
					l.Close()
					<-done
				}
			}()
			hook := engineconn.WatchResources()
			switch kind {
			case "aborted_header":
				io.WriteString(c, "GET / HTTP/1.1\r\nHost:")
				c.Close()
			case "aborted_body":
				io.WriteString(c, "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 99999\r\n\r\nabc")
				signal(t, entered)
				c.Close()
				signal(t, returned)
			case "hijack_handoff":
				io.WriteString(c, request("GET", "/", false))
				signal(t, returned)
				io.ReadAll(c)
				c.Close()
			default:
				io.WriteString(c, request("GET", "/", kind == "normal_close"))
				if p := readResponse(t, bufio.NewReader(c), "GET"); string(p) != "resource" {
					t.Fatalf("body=%q", p)
				}
				signal(t, returned)
				if kind != "normal_close" {
					signal(t, idle)
					if loans := bufpool.Outstanding() - beforeBuffers; loans != 0 {
						t.Fatalf("HTTP-idle retains %d buffers", loans)
					}
					if res := engineconn.SnapshotResources(); res.IdleBuffers != 0 {
						t.Fatalf("idle buffers=%d", res.IdleBuffers)
					}
				}
				if kind == "shutdown" {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					err = s.Shutdown(ctx)
					cancel()
					if err != nil {
						t.Fatal(err)
					}
				}
				c.Close()
			}
			signal(t, closed)
			signal(t, hook)
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			l.Close()
			if err = <-done; err != http.ErrServerClosed {
				t.Fatalf("Serve: %v", err)
			}
			cleaned = true
			runtime.GC()
			afterFD, afterG, afterBuffers := fdCount(t), runtime.NumGoroutine(), bufpool.Outstanding()
			afterEngine := engineconn.SnapshotResources()
			if afterFD != beforeFD || afterG > beforeG || afterBuffers != beforeBuffers || afterEngine != beforeEngine {
				t.Fatalf("resource leak fd %d->%d goroutines %d->%d buffers %d->%d engine %+v->%+v", beforeFD, afterFD, beforeG, afterG, beforeBuffers, afterBuffers, beforeEngine, afterEngine)
			}
			t.Logf("PASS %s fd=%d goroutines=%d buffers=%d; cleanup all clients/listener/loops/workers/wheel joined", kind, afterFD, afterG, afterBuffers)
		})
	}
}
