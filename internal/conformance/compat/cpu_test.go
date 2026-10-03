//go:build darwin || linux

package compat

import (
	"io"
	"net"
	"net/http"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func cpu(t *testing.T) float64 {
	t.Helper()
	var u syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &u); err != nil {
		t.Fatal(err)
	}
	return float64(u.Utime.Sec+u.Stime.Sec) + float64(u.Utime.Usec+u.Stime.Usec)/1e6
}

func TestDifferentialStalledCPU(t *testing.T) {
	for _, name := range []string{"PartialHeaderCPU", "AbortedBodyCPU"} {
		t.Run(name, func(t *testing.T) {
			for _, own := range []bool{false, true} {
				t.Run(map[bool]string{false: "std", true: "hoe"}[own], func(t *testing.T) {
					closed := make(chan struct{})
					h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body) })
					addr, _ := launch(t, own, &http.Server{Handler: h, ReadTimeout: 300 * time.Millisecond,
						ConnState: func(_ net.Conn, s http.ConnState) {
							if s == http.StateClosed {
								close(closed)
							}
						}})
					c := dial(t, addr)
					beforeG, beforeCPU := runtime.NumGoroutine(), cpu(t)
					raw := "GET / HTTP/1.1\r\nHost:"
					if name == "AbortedBodyCPU" {
						raw = "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 100000\r\n\r\nabc"
					}
					io.WriteString(c, raw)
					if name == "AbortedBodyCPU" {
						// Actually abort the body, then observe the connection-closed event.
						c.Close()
					}
					signal(t, closed)
					if name == "AbortedBodyCPU" {
						// CPU over a defined observation interval is the behavior under test,
						// not a delay used to guess whether a goroutine has stopped.
						<-time.After(300 * time.Millisecond)
					}
					c.Close()
					elapsedCPU := cpu(t) - beforeCPU
					if elapsedCPU > 0.08 {
						t.Fatalf("CPU spin: %.4fs while stalled", elapsedCPU)
					}
					t.Logf("SAME no-spin CPU=%.4fs goroutine delta=%d", elapsedCPU, runtime.NumGoroutine()-beforeG)
				})
			}
		})
	}
}
