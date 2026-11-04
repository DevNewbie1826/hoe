//go:build linux || darwin

package conn

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// placementRecorder keeps per-connection lifecycle facts for the policy
// guards. The hooks run on reactor and worker goroutines; the assertions
// read only after Shutdown joined the engine.
type placementRecorder struct {
	mu     sync.Mutex
	opens  map[*Conn]int
	states map[*Conn][]http.ConnState
}

func (r *placementRecorder) opened(c *Conn) {
	r.mu.Lock()
	r.opens[c]++
	r.mu.Unlock()
}

func (r *placementRecorder) state(c *Conn, s http.ConnState) {
	r.mu.Lock()
	r.states[c] = append(r.states[c], s)
	r.mu.Unlock()
}

// TestPlacementPoliciesServeExactlyOnceInOrder guards the engine on both
// placement policies: each accepted connection is adopted exactly once
// (ConnContext once, StateNew once, StateClosed once, in lifecycle order),
// pipelined responses keep per-connection FIFO, and adoption accounting
// balances (opening back to zero, no live connections, no latched error).
func TestPlacementPoliciesServeExactlyOnceInOrder(t *testing.T) {
	for _, policy := range []string{"roundrobin", "fdmodulo"} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv("HOE_LOOP_PLACEMENT", policy)
			ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			rec := &placementRecorder{opens: make(map[*Conn]int), states: make(map[*Conn][]http.ConnState)}
			srv := &http.Server{
				ConnContext: func(ctx context.Context, c net.Conn) context.Context {
					rec.opened(c.(*Conn))
					return ctx
				},
				ConnState: func(c net.Conn, s http.ConnState) {
					rec.state(c.(*Conn), s)
				},
				Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					fmt.Fprint(w, r.URL.Path)
				}),
			}
			engine, err := New(srv, ln, Policy{KeepAlive: func() bool { return true }})
			if err != nil {
				ln.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				engine.Close()
				ln.Close()
			})
			serveDone := make(chan error, 1)
			go func() { serveDone <- engine.Serve(ln) }()

			const (
				conns    = 16
				requests = 3
			)
			clients := make([]net.Conn, 0, conns)
			for i := range conns {
				client, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
				if err != nil {
					t.Fatal(err)
				}
				if err := client.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
					t.Fatal(err)
				}
				clients = append(clients, client)
				var pipeline strings.Builder
				for k := range requests {
					fmt.Fprintf(&pipeline, "GET /policy=%s/conn=%02d/req=%d HTTP/1.1\r\nHost: t\r\n\r\n", policy, i, k)
				}
				if _, err := client.Write([]byte(pipeline.String())); err != nil {
					t.Fatal(err)
				}
				reader := bufio.NewReader(client)
				for k := range requests {
					resp, err := http.ReadResponse(reader, nil)
					if err != nil {
						t.Fatalf("conn=%d req=%d response: %v", i, k, err)
					}
					body, err := io.ReadAll(resp.Body)
					resp.Body.Close()
					if err != nil {
						t.Fatalf("conn=%d req=%d body: %v", i, k, err)
					}
					want := fmt.Sprintf("/policy=%s/conn=%02d/req=%d", policy, i, k)
					if string(body) != want {
						t.Fatalf("conn=%d req=%d body=%q want %q: per-connection FIFO broken", i, k, body, want)
					}
				}
			}
			for _, c := range clients {
				c.Close()
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := engine.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown: %v", err)
			}
			select {
			case err := <-serveDone:
				if !errors.Is(err, http.ErrServerClosed) {
					t.Fatalf("Serve=%v want %v", err, http.ErrServerClosed)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not return after Shutdown")
			}
			engine.mu.Lock()
			opening, live, adoptErr := engine.opening, len(engine.conns), engine.adoptErr
			engine.mu.Unlock()
			if opening != 0 || live != 0 || adoptErr != nil {
				t.Fatalf("adoption accounting: opening=%d live=%d adoptErr=%v", opening, live, adoptErr)
			}
			if got := len(rec.opens); got != conns {
				t.Fatalf("ConnContext saw %d connections, want %d", got, conns)
			}
			for c, opens := range rec.opens {
				if opens != 1 {
					t.Fatalf("ConnContext ran %d times for one connection", opens)
				}
				states := rec.states[c]
				if len(states) == 0 || states[0] != http.StateNew || states[len(states)-1] != http.StateClosed {
					t.Fatalf("connection states %v must open with StateNew and end with StateClosed", states)
				}
				var news, actives, closed, firstActive, lastClosed int
				for i, s := range states {
					switch s {
					case http.StateNew:
						news++
					case http.StateActive:
						actives++
						if firstActive == 0 {
							firstActive = i
						}
					case http.StateClosed:
						closed++
						lastClosed = i
					}
				}
				if news != 1 || closed != 1 || actives == 0 || firstActive > lastClosed {
					t.Fatalf("connection states %v violate lifecycle order", states)
				}
			}
		})
	}
}

// TestPlacementPoliciesBalanceAdoptionFailure guards PR #8's failure
// accounting under both policies: a descriptor that fails registration
// latches the first adoption error, balances the opening count, stops
// acceptance, and Serve reports the registration error.
func TestPlacementPoliciesBalanceAdoptionFailure(t *testing.T) {
	for _, policy := range []string{"roundrobin", "fdmodulo"} {
		t.Run(policy, func(t *testing.T) {
			t.Setenv("HOE_LOOP_PLACEMENT", policy)
			ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			engine, err := New(&http.Server{}, ln, Policy{})
			if err != nil {
				ln.Close()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				engine.Close()
				ln.Close()
			})
			serveDone := make(chan error, 1)
			go func() { serveDone <- engine.Serve(ln) }()
			// The invalid descriptor fails poller registration on the
			// policy-selected reactor without creating a connection.
			if err := engine.adopt(-1); err != nil {
				t.Fatalf("enqueue adoption: %v", err)
			}
			select {
			case err := <-serveDone:
				if !errors.Is(err, syscall.EBADF) {
					t.Fatalf("Serve=%v want registration error %v", err, syscall.EBADF)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Serve did not report the adoption failure")
			}
			engine.mu.Lock()
			opening, adoptErr := engine.opening, engine.adoptErr
			engine.mu.Unlock()
			if opening != 0 {
				t.Fatalf("opening=%d after adoption failure, want 0", opening)
			}
			if adoptErr == nil || !errors.Is(adoptErr, syscall.EBADF) {
				t.Fatalf("adoptErr=%v want %v", adoptErr, syscall.EBADF)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := engine.Shutdown(ctx); err != nil {
				t.Fatalf("Shutdown after failed registration: %v", err)
			}
			select {
			case <-engine.finalized:
			case <-time.After(5 * time.Second):
				t.Fatal("engine resources not finalized")
			}
		})
	}
}
