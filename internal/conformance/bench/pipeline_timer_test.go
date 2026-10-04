//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestPipelineTimerReuse(t *testing.T) {
	// Given the first runtime expiry is dispatched but paused before its lock.
	entered, resume := make(chan struct{}), make(chan struct{})
	completed := make(chan struct{}, 8)
	atD := make(chan struct{})
	finishB, finishD, finishF := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	var resumeOnce, bOnce, dOnce, fOnce sync.Once
	resumeExpiry := func() { resumeOnce.Do(func() { close(resume) }) }
	releaseB := func() { bOnce.Do(func() { close(finishB) }) }
	releaseD := func() { dOnce.Do(func() { close(finishD) }) }
	releaseF := func() { fOnce.Do(func() { close(finishF) }) }
	rig := pipelineServer(t, pipelineConfig{
		expiryGate: func() {
			if first.CompareAndSwap(false, true) {
				close(entered)
				<-resume
			}
		},
		expiryDone: func() { completed <- struct{}{} },
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/B":
				<-finishB
			case "/C":
				pipelineProbe(t, pipelineTransport(r), false)
			case "/D":
				close(atD)
				<-finishD
			case "/F":
				<-finishF
			}
			io.WriteString(w, r.URL.Path)
		}),
	})
	joined := false
	t.Cleanup(func() {
		releaseB()
		releaseD()
		releaseF()
		resumeExpiry()
		if !joined && first.Load() {
			pipelineEvent(t, completed)
		}
		t.Log("cleanup: paused expiry joined; all handlers released")
	})
	pipelineSend(t, rig.client, "/A", "/B")
	pipelineEvent(t, rig.arms)
	pipelineEvent(t, entered)
	releaseB()
	pipelineReleased(t, rig)
	pipelineReceipt(t, rig, "/A")
	pipelineReceipt(t, rig, "/B")

	// When a new round arrives before Stop's dispatched callback finishes.
	pipelineSend(t, rig.client, "/C", "/D")
	pipelineEvent(t, atD)

	// Then the intervening round is uncorked, not adopted by the old callback.
	pipelineProbe(t, rig.raw, false)
	pipelineReceipt(t, rig, "/C")
	before := systest.Snapshot().PushCalls
	resumeExpiry()
	pipelineEvent(t, completed)
	joined = true
	if systest.Snapshot().PushCalls != before {
		t.Fatal("old expiry pushed an intervening round")
	}
	releaseD()
	pipelineReceipt(t, rig, "/D")

	// A later round must reuse the timer and still release A while B blocks.
	pipelineSend(t, rig.client, "/E", "/F")
	pipelineEvent(t, rig.arms)
	pipelineReceipt(t, rig, "/E")
	if event := pipelineReleased(t, rig); !event.expired {
		t.Fatal("reused timer did not enforce the cap")
	}
	pipelineEvent(t, completed)
	releaseF()
	pipelineReceipt(t, rig, "/F")
	t.Log("PASS Stop/running callback: uncorked intervening round; reused cap expires")
}
