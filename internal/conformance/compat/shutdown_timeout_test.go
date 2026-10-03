//go:build linux || darwin

package compat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

func TestShutdownTimeoutEventuallyReleasesResources(t *testing.T) {
	before := engineconn.SnapshotResources()
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	addr, s := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		close(entered)
		<-release
		fmt.Fprint(w, "finish")
	})})
	c := dial(t, addr)
	io.WriteString(c, request("GET", "/", false))
	signal(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := s.Shutdown(ctx)
	cancel()
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown while active: %v", err)
	}
	if p := readResponse(t, bufio.NewReader(c), "GET"); string(p) != "finish" {
		t.Fatalf("active response lost: %q", p)
	}
	signal(t, returned)
	// Subscribe before each observation: finalization must happen without a
	// second Shutdown or Close, including after the caller's context expires.
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		event := engineconn.WatchResources()
		if engineconn.SnapshotResources() == before {
			break
		}
		select {
		case <-event:
		case <-timeout.C:
			t.Fatal("reactor resources stranded after timed-out Shutdown")
		}
	}
	t.Log("PASS active response retained and engine finalized after Shutdown deadline")
}
