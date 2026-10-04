//go:build linux || darwin

package bench

import (
	"io"
	"net"
	"net/http"
	"sync"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestPipelineRunningExpiry(t *testing.T) {
	for _, hijack := range []bool{false, true} {
		name := "Close"
		if hijack {
			name = "Hijack"
		}
		t.Run(name, func(t *testing.T) {
			// Given a live AfterFunc has entered, but has not acquired the
			// pipeline lock; Stop will not be able to cancel this callback.
			entered := make(chan struct{})
			resume := make(chan struct{})
			expiredDone := make(chan struct{})
			act := make(chan struct{})
			acted := make(chan struct{})
			var resumeOnce, actOnce sync.Once
			unblockExpiry := func() { resumeOnce.Do(func() { close(resume) }) }
			unblockHandler := func() { actOnce.Do(func() { close(act) }) }
			var handoff net.Conn
			rig := pipelineServer(t, pipelineConfig{
				expiryGate: func() { close(entered); <-resume },
				expiryDone: func() { close(expiredDone) },
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/A" {
						io.WriteString(w, "/A")
						return
					}
					<-act
					if hijack {
						nc, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Error(err)
						} else {
							handoff = nc
						}
					} else if err := pipelineTransport(r).Close(); err != nil {
						t.Error(err)
					}
					close(acted)
				}),
			})
			t.Cleanup(func() {
				unblockHandler()
				unblockExpiry()
				pipelineEvent(t, acted)
				if handoff != nil {
					handoff.Close()
				}
				pipelineEvent(t, expiredDone)
				t.Log("cleanup: handler and running timer callback joined; handoff closed")
			})
			pipelineSend(t, rig.client, "/A", "/B")
			pipelineEvent(t, rig.arms)
			pipelineEvent(t, entered)
			// When descriptor lifetime ends with an already-running expiry.
			unblockHandler()
			pipelineEvent(t, acted)
			pipelineReceipt(t, rig, "/A")
			if event := pipelineReleased(t, rig); event.expired {
				t.Fatal("callback pushed before lifetime release")
			}
			before := systest.Snapshot().PushCalls
			unblockExpiry()
			pipelineEvent(t, expiredDone)
			// Then the stale callback never issues a syscall on the handed-off
			// or closed fd; it completed without racing Close/Hijack.
			if systest.Snapshot().PushCalls != before {
				t.Fatal("running callback pushed after lifetime ended")
			}
			t.Logf("PASS %s while expiry running: stale callback syscall delta=0", name)
		})
	}
}
