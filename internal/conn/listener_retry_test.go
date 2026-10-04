//go:build linux || darwin

package conn_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	"github.com/DevNewbie1826/hoe/internal/bufpool"
	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

// Only the listener emits logs in this fixture. The writer is an event
// subscription, not a log wording assertion or timing-based absence check.
type acceptRetrySignal struct {
	once  sync.Once
	retry chan struct{}
}

func (s *acceptRetrySignal) Write(p []byte) (int, error) {
	s.once.Do(func() { close(s.retry) })
	return len(p), nil
}

func listenerEvent(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("listener event timeout")
	}
}

func listenerResponse(t *testing.T, c net.Conn) string {
	t.Helper()
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(p)
}

func TestTemporaryAcceptPreservesActiveRequest(t *testing.T) {
	for _, action := range []string{"recover", "close", "shutdown"} {
		for _, own := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hoe=%t", action, own), func(t *testing.T) {
				// Given an active request and a subscribed accept-error event.
				l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
				if err != nil {
					t.Fatal(err)
				}
				defer l.Close()
				beforeBuffers, before := bufpool.Outstanding(), engineconn.SnapshotResources()
				entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
				shutdownStarted := make(chan struct{})
				retry := &acceptRetrySignal{retry: make(chan struct{})}
				var releaseOnce sync.Once
				cfg := &http.Server{ErrorLog: log.New(retry, "", 0), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/active" {
						close(entered)
						defer close(returned)
						select {
						case <-release:
							fmt.Fprintf(w, "context=%v", r.Context().Err())
						case <-r.Context().Done():
						}
						return
					}
					fmt.Fprint(w, "next")
				})}
				var s interface {
					Serve(net.Listener) error
					Close() error
					Shutdown(context.Context) error
					RegisterOnShutdown(func())
				} = cfg
				if own {
					s = &hoe.Server{Handler: cfg.Handler, ErrorLog: cfg.ErrorLog}
				}
				s.RegisterOnShutdown(func() { close(shutdownStarted) })
				done := make(chan error, 1)
				go func() { done <- s.Serve(l) }()
				joined := false
				t.Cleanup(func() {
					releaseOnce.Do(func() { close(release) })
					s.Close()
					if !joined {
						select {
						case <-done:
						case <-time.After(5 * time.Second):
							t.Error("Serve leaked")
						}
					}
				})
				c, err := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				if err = c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
					t.Fatal(err)
				}
				if _, err = io.WriteString(c, "GET /active HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"); err != nil {
					t.Fatal(err)
				}
				listenerEvent(t, entered)
				// When the real listener's pending Accept hits a temporary timeout.
				if err = l.SetDeadline(time.Now().Add(-time.Second)); err != nil {
					t.Fatal(err)
				}
				select {
				case <-retry.retry:
				case err = <-done:
					joined = true
					t.Fatalf("Serve stopped on temporary accept error: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("temporary accept error was not observed")
				}
				wantServe := http.ErrServerClosed
				switch action {
				case "recover":
					if err = l.SetDeadline(time.Time{}); err != nil {
						t.Fatal(err)
					}
					next, dialErr := net.DialTimeout("tcp", l.Addr().String(), 5*time.Second)
					if dialErr != nil {
						t.Fatal(dialErr)
					}
					defer next.Close()
					next.SetDeadline(time.Now().Add(5 * time.Second))
					if _, err = io.WriteString(next, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"); err != nil {
						t.Fatal(err)
					}
					if p := listenerResponse(t, next); p != "next" {
						t.Fatalf("recovered response=%q", p)
					}
					if err = l.Close(); err != nil {
						t.Fatal(err)
					}
					wantServe = net.ErrClosed
				case "close":
					if err = s.Close(); err != nil {
						t.Fatal(err)
					}
				case "shutdown":
					stopped := make(chan error, 1)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					go func() { stopped <- s.Shutdown(ctx) }()
					listenerEvent(t, shutdownStarted)
					releaseOnce.Do(func() { close(release) })
					select {
					case err = <-stopped:
						if err != nil {
							t.Fatal(err)
						}
					case <-ctx.Done():
						t.Fatal("Shutdown did not join")
					}
				default:
					t.Fatalf("unknown stop action %q", action)
				}
				// Then Serve's error and the existing request follow std lifetime.
				select {
				case err = <-done:
					joined = true
					if !errors.Is(err, wantServe) {
						t.Fatalf("Serve=%v want=%v", err, wantServe)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("stop did not unblock Serve")
				}
				if action != "close" {
					releaseOnce.Do(func() { close(release) })
					if p := listenerResponse(t, c); p != "context=<nil>" {
						t.Fatalf("active response=%q", p)
					}
				}
				listenerEvent(t, returned)
				c.Close()
				if err = s.Close(); err != nil {
					t.Fatal(err)
				}
				if own && (bufpool.Outstanding() != beforeBuffers || engineconn.SnapshotResources() != before) {
					t.Fatal("retry/stop leaked engine resources")
				}
				t.Log("SAME temporary accept, active lifetime, Serve error, cleanup")
			})
		}
	}
}
