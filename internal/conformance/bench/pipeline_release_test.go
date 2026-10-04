//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestPipelineRelease(t *testing.T) {
	for _, point := range []string{"Flush", "FlushError", "1xx", "Hijack", "ReadFrom", "Close", "panic"} {
		t.Run(point, func(t *testing.T) {
			// Given A has finished into a held socket; expiry is controlled so
			// only B's release point can make A arrive.
			release := make(chan struct{})
			acted := make(chan struct{})
			path, data := fixture(t, 32<<10)
			transferBefore := systest.Snapshot().SendfileCalls
			rig := pipelineServer(t, pipelineConfig{controlled: true,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					pipelineProbe(t, pipelineTransport(r), true)
					if r.URL.Path == "/A" {
						io.WriteString(w, "/A")
						return
					}
					switch point {
					case "Flush":
						w.Header().Set("Content-Length", "1")
						io.WriteString(w, "B")
						w.(http.Flusher).Flush()
					case "FlushError":
						w.Header().Set("Content-Length", "1")
						io.WriteString(w, "B")
						if err := http.NewResponseController(w).Flush(); err != nil {
							t.Error(err)
						}
					case "1xx":
						w.WriteHeader(103)
					case "Hijack":
						nc, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
							close(acted)
							return
						}
						defer nc.Close()
						io.WriteString(nc, "handoff")
					case "ReadFrom":
						f, err := os.Open(path)
						if err != nil {
							t.Error(err)
							close(acted)
							return
						}
						defer f.Close()
						w.Header().Set("Content-Length", "32768")
						if _, err := w.(io.ReaderFrom).ReadFrom(f); err != nil {
							t.Error(err)
						}
					case "Close":
						if err := pipelineTransport(r).Close(); err != nil {
							t.Error(err)
						}
					case "panic":
						panic(http.ErrAbortHandler)
					}
					close(acted)
					<-release
				})})
			t.Cleanup(func() { close(release) })
			// When B executes a release before blocking (or panicking).
			pipelineSend(t, rig.client, "/A", "/B")
			arm := pipelineEvent(t, rig.arms)
			rig.client.SetReadDeadline(time.Now().Add(pipelineTolerance))
			// Then receipt and kernel state prove a push without timer expiry.
			pipelineReceipt(t, rig, "/A")
			if event := pipelineReleased(t, rig); event.expired {
				t.Fatal("release relied on cap expiry")
			}
			if point != "panic" {
				pipelineEvent(t, acted)
			}
			switch point {
			case "Flush", "FlushError":
				pipelineReceipt(t, rig, "B")
			case "1xx":
				r, err := http.ReadResponse(rig.reader, nil)
				if err != nil || r.StatusCode != 103 {
					t.Fatalf("informational receipt=%v err=%v", r, err)
				}
				r.Body.Close()
				t.Logf("HTTP/1.1 %s; headers=%v", r.Status, r.Header)
			case "Hijack":
				p := make([]byte, 7)
				if _, err := io.ReadFull(rig.reader, p); err != nil || string(p) != "handoff" {
					t.Fatalf("handoff receipt=%q err=%v", p, err)
				}
			case "ReadFrom":
				pipelineReceipt(t, rig, string(data))
				if systest.Snapshot().SendfileCalls <= transferBefore {
					t.Fatal("ReadFrom did not use sendfile")
				}
			case "Close", "panic":
				if _, err := rig.reader.ReadByte(); err != io.EOF {
					t.Fatalf("terminal receipt not EOF: %v", err)
				}
			}
			if point == "Hijack" || point == "Close" || point == "panic" {
				// A queued callback after Stop must be invalid after fd handoff.
				before := systest.Snapshot().PushCalls
				arm.expire()
				if systest.Snapshot().PushCalls != before {
					t.Fatal("stale timer touched a released descriptor")
				}
			}
			t.Logf("PASS %s: immediate receipt and cleared kernel hold; timer suppressed", point)
		})
	}
}
