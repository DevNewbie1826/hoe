package compat

import (
	"io"
	"net/http"
	"testing"
	"time"
)

func TestCloseDoesNotWaitForActiveHandler(t *testing.T) {
	for _, own := range []bool{false, true} {
		t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
			entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			addr, s := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				close(entered)
				<-release
			})})
			c := dial(t, addr)
			io.WriteString(c, request("GET", "/", false))
			signal(t, entered)
			done := make(chan error, 1)
			go func() { done <- s.Close() }()
			var closeErr error
			completed := false
			select {
			case closeErr = <-done:
				completed = true
			case <-time.After(time.Second):
			}
			// Release and join the user handler before reporting either outcome.
			close(release)
			signal(t, returned)
			if !completed {
				closeErr = <-done
				t.Fatalf("Close waited for active handler, err=%v", closeErr)
			}
			if closeErr != nil {
				t.Fatal(closeErr)
			}
			var p [1]byte
			if n, err := c.Read(p[:]); n != 0 || err != io.EOF {
				t.Fatalf("closed socket n=%d err=%v", n, err)
			}
		})
	}
}
