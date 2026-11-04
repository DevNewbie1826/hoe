//go:build linux || darwin

package compat

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
	"github.com/gorilla/websocket"
)

func TestIdleTenThousandConnections(t *testing.T) {
	const count = 10000
	idle := make(chan struct{}, count)
	addr, s := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "idle") }),
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateIdle {
				idle <- struct{}{}
			}
		}})
	beforeBuffers := bufpool.Outstanding()
	var before int
	cs := make([]net.Conn, 0, count)
	defer func() {
		for _, c := range cs {
			c.Close()
		}
		s.Close()
	}()
	// Given 10k real TCP clients, each with a completed keep-alive response.
	for i := 0; i < count; i++ {
		if i == 1 {
			before = goroutineFloor(t)
		}
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		cs = append(cs, c)
		// This mass fixture has no graceful TCP-close criterion. Resetting
		// after the idle observation avoids exhausting the host's ephemeral
		// ports with 10k TIME_WAIT sockets across consecutive test runs.
		if err = c.(*net.TCPConn).SetLinger(0); err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(30 * time.Second))
		if _, err = io.WriteString(c, request("GET", "/", false)); err != nil {
			t.Fatal(err)
		}
		if body := readResponse(t, bufio.NewReader(c), "GET"); string(body) != "idle" {
			t.Fatalf("response %q", body)
		}
		signal(t, idle)
	}
	// When all StateIdle events have completed, no sleeping guesses are needed.
	delta := goroutineFloor(t) - before
	res := engineconn.SnapshotResources()
	// Then parked HTTP connections consume neither goroutines nor I/O loans.
	// The engine's L+2 startup population and the warmup worker sit in the baseline;
	// transient workers drain inside the 3s min-window;
	// the ceiling is therefore host-size independent with a deterministic leak floor of 6 goroutines.
	if delta > 5 {
		t.Fatalf("10000 idle connections add %d server goroutines over the drained baseline, limit5", delta)
	}
	if res.Connections != count || res.IdleBuffers != 0 || bufpool.Outstanding() != beforeBuffers {
		t.Fatalf("idle resources=%+v buffer delta=%d", res, bufpool.Outstanding()-beforeBuffers)
	}
	t.Logf("PASS %d idle connections, drained-baseline server goroutines=%d, idle buffers=0", count, delta)
}

// goroutineFloor polls runtime.NumGoroutine every 50ms across the full 3s window
// and returns the minimum. The deadline is the window: it never exits early.
func goroutineFloor(t *testing.T) int {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	floor := runtime.NumGoroutine()
	for {
		select {
		case <-ticker.C:
			if n := runtime.NumGoroutine(); n < floor {
				floor = n
			}
		case <-deadline.C:
			if n := runtime.NumGoroutine(); n < floor {
				floor = n
			}
			return floor
		}
	}
}

func TestPartialHeaderAndAbortedBodyCPU(t *testing.T) {
	for _, kind := range []string{"partial_header", "aborted_body"} {
		t.Run(kind, func(t *testing.T) {
			active, entered, closed := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
			addr, _ := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				io.Copy(io.Discard, r.Body)
			}), ConnState: func(_ net.Conn, state http.ConnState) {
				if state == http.StateActive {
					active <- struct{}{}
				}
				if state == http.StateClosed {
					close(closed)
				}
			}})
			c := dial(t, addr)
			if kind == "partial_header" {
				io.WriteString(c, "GET / HTTP/1.1\r\nHost:")
				signal(t, active)
			} else {
				io.WriteString(c, "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 100000\r\n\r\nabc")
				signal(t, entered)
				c.Close()
				signal(t, closed)
			}
			start := cpu(t)
			// The measured 3s interval is the CPU behavior itself, not synchronization.
			<-time.NewTimer(3 * time.Second).C
			used := cpu(t) - start
			if used >= 0.05 {
				t.Fatalf("CPU spin: %.6fs over3s, limit0.05s", used)
			}
			c.Close()
			if kind == "partial_header" {
				signal(t, closed)
			}
			t.Logf("PASS %s CPU=%.6fs over3s, no panic", kind, used)
		})
	}
}

func TestDisconnectCancellation(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			entered, cancelled, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			addr, _ := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(returned)
				close(entered)
				<-r.Context().Done()
				close(cancelled)
			})})
			c := dial(t, addr)
			io.WriteString(c, request("GET", "/", false))
			signal(t, entered)
			start := time.Now()
			c.Close()
			signal(t, cancelled)
			elapsed := time.Since(start)
			signal(t, returned)
			if elapsed >= 50*time.Millisecond {
				t.Fatalf("cancel latency=%s, limit50ms", elapsed)
			}
			t.Logf("PASS disconnect cancel=%s", elapsed)
		})
	}
}

func TestGorillaHijackEcho(t *testing.T) {
	const count = 200
	var handlers sync.WaitGroup
	upgrader := websocket.Upgrader{}
	addr, _ := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlers.Add(1)
		defer handlers.Done()
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer c.Close()
		for {
			kind, p, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err = c.WriteMessage(kind, p); err != nil {
				return
			}
		}
	})})
	cs := make([]*websocket.Conn, 0, count)
	defer func() {
		for _, c := range cs {
			c.Close()
		}
		handlers.Wait()
	}()
	for i := 0; i < count; i++ {
		c, r, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
		if err != nil {
			t.Fatalf("upgrade %d: %v response=%v", i, err, r)
		}
		cs = append(cs, c)
	}
	for i, c := range cs {
		payload := fmt.Sprintf("hoe-gorilla-echo-%d", i)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
			t.Fatal(err)
		}
		kind, p, err := c.ReadMessage()
		if err != nil || kind != websocket.TextMessage || string(p) != payload {
			t.Fatalf("echo %d kind=%d body=%q err=%v", i, kind, p, err)
		}
	}
	t.Logf("PASS %d gorilla hijack connections, all payloads echoed", count)
}
