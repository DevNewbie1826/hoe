package compat

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
)

func TestShutdownLeavesHijackedHandler(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
			hijacked, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				c, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer c.Close()
				close(hijacked)
				<-release
			})})
			c := dial(t, addr)
			io.WriteString(c, request("GET", "/", false))
			signal(t, hijacked)
			done := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() { done <- s.Shutdown(ctx) }()
			var shutdownErr error
			completed := false
			select {
			case shutdownErr = <-done:
				completed = true
			case <-ctx.Done():
				shutdownErr = ctx.Err()
			}
			// Cleanup must release the handler even if the assertion fails.
			close(release)
			signal(t, returned)
			if !completed {
				<-done
			}
			if shutdownErr != nil {
				t.Fatalf("Shutdown waited for hijacked handler: %v", shutdownErr)
			}
			var p [1]byte
			if n, err := c.Read(p[:]); n != 0 || err != io.EOF {
				t.Fatalf("hijack close n=%d err=%v", n, err)
			}
		})
	}
}
