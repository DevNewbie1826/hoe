//go:build linux || darwin

package compat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

func TestReviewListenerContextCallerClose(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(fmt.Sprintf("hoe=%t", own), func(t *testing.T) {
			// Given a real accepted socket paused inside ConnContext.
			l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer l.Close()
			beforeFD, beforeBuffers := fdCount(t), bufpool.Outstanding()
			before := engineconn.SnapshotResources()
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			cfg := &http.Server{
				ConnContext: func(ctx context.Context, _ net.Conn) context.Context {
					close(entered)
					<-release
					return ctx
				},
				Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					fmt.Fprint(w, "accepted")
				}),
			}
			var s server = cfg
			if own {
				s = hoeConfig(cfg)
			}
			done := make(chan error, 1)
			go func() { done <- s.Serve(l) }()
			joined := false
			t.Cleanup(func() {
				unblock()
				if err := s.Close(); err != nil {
					t.Error(err)
				}
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("Serve did not join")
					}
				}
			})
			c := dial(t, l.Addr().String())
			signal(t, entered)
			if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"); err != nil {
				t.Fatal(err)
			}
			// When only the caller's listener closes, not the server.
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			// Std runs ConnContext on its accept goroutine. Hoe runs it on
			// a worker, so joining its accept owner first fixes the interleaving.
			// Neither ordering authorizes closing the already accepted socket.
			if !own {
				unblock()
			}
			select {
			case err := <-done:
				joined = true
				if !errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
					t.Fatalf("Serve error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not observe listener closure")
			}
			unblock()
			// Then the accepted connection must still serve its closing GET.
			r, err := http.ReadResponse(bufio.NewReader(c), nil)
			if err != nil {
				t.Fatalf("accepted connection was lost after caller listener Close: %v", err)
			}
			body, readErr := io.ReadAll(r.Body)
			closeErr := r.Body.Close()
			if err := errors.Join(readErr, closeErr); err != nil {
				t.Fatal(err)
			}
			if r.StatusCode != http.StatusOK || string(body) != "accepted" {
				t.Fatalf("response = %d %q", r.StatusCode, body)
			}
			c.Close()
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if own && (fdCount(t) != beforeFD-1 || bufpool.Outstanding() != beforeBuffers || engineconn.SnapshotResources() != before) {
				t.Fatal("initializing connection cleanup did not restore resource baseline")
			}
			t.Logf("SAME accepted connection survived: status=%d body=%q; Serve joined and resources restored", r.StatusCode, body)
		})
	}
}
