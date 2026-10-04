//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"testing"
	"time"
)

const pipelineTolerance = 100 * time.Millisecond

func TestPipelineCap(t *testing.T) {
	for _, delay := range []time.Duration{0, 20 * time.Millisecond, 200 * time.Microsecond} {
		t.Run(delay.String(), func(t *testing.T) {
			// Given A produces bytes and B cannot finish until the client reads A.
			release := make(chan struct{})
			blocked := make(chan struct{})
			rig := pipelineServer(t, pipelineConfig{delay: delay,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/B" {
						close(blocked)
						<-release
					}
					io.WriteString(w, r.URL.Path)
				})})
			t.Cleanup(func() { close(release) })
			// When both requests arrive before the handler serves A.
			pipelineSend(t, rig.client, "/A", "/B")
			pipelineEvent(t, blocked)
			arm := pipelineEvent(t, rig.arms)
			want := delay
			if want == 0 {
				want = time.Millisecond
			}
			if arm.delay != want {
				t.Errorf("armed duration=%v want=%v", arm.delay, want)
			}
			rig.client.SetReadDeadline(arm.start.Add(want + pipelineTolerance))
			// Then A arrives within cap+tolerance while B is still blocked,
			// and the precise timer's expiry performed an actual push.
			pipelineReceipt(t, rig, "/A")
			if event := pipelineEvent(t, rig.pushes); !event.expired || event.err != nil {
				t.Fatalf("cap expiry push=%+v", event)
			}
			pipelineProbe(t, rig.raw, false)
			t.Logf("PASS arm=%v expiry push=true receipt=%v", arm.delay, time.Since(arm.start))
		})
	}
}

func TestPipelineAfterExpiry(t *testing.T) {
	// Given B waits for A's expiry and C waits for B's receipt.
	finishB := make(chan struct{})
	finishC := make(chan struct{})
	atC := make(chan struct{})
	rig := pipelineServer(t, pipelineConfig{handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/B":
			<-finishB
		case "/C":
			close(atC)
			<-finishC
		}
		io.WriteString(w, r.URL.Path)
	})})
	bClosed := false
	t.Cleanup(func() {
		if !bClosed {
			close(finishB)
		}
		close(finishC)
	})
	pipelineSend(t, rig.client, "/A", "/B", "/C")
	arm := pipelineEvent(t, rig.arms)
	rig.client.SetReadDeadline(arm.start.Add(time.Millisecond + pipelineTolerance))
	pipelineReceipt(t, rig, "/A")
	if event := pipelineEvent(t, rig.pushes); !event.expired || event.err != nil {
		t.Fatalf("first expiry=%+v", event)
	}
	// When B finishes after the cap has already released A.
	start := time.Now()
	close(finishB)
	bClosed = true
	pipelineEvent(t, atC)
	// Then B is not re-corked without a fresh cap while C remains blocked.
	pipelineProbe(t, rig.raw, false)
	rig.client.SetReadDeadline(start.Add(time.Millisecond + pipelineTolerance))
	pipelineReceipt(t, rig, "/B")
	t.Log("PASS B received after expiry with round uncorked while C blocks")
}

func TestPipelineOff(t *testing.T) {
	// Given negative delay and a later handler that blocks indefinitely.
	release := make(chan struct{})
	blocked := make(chan struct{})
	rig := pipelineServer(t, pipelineConfig{delay: -time.Second, controlled: true,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pipelineProbe(t, pipelineTransport(r), false)
			if r.URL.Path == "/B" {
				close(blocked)
				<-release
			}
			io.WriteString(w, r.URL.Path)
		})})
	t.Cleanup(func() { close(release) })
	// When A and B arrive together.
	pipelineSend(t, rig.client, "/A", "/B")
	pipelineEvent(t, blocked)
	rig.client.SetReadDeadline(time.Now().Add(pipelineTolerance))
	// Then A is delivered with no hold or timer.
	pipelineReceipt(t, rig, "/A")
	select {
	case arm := <-rig.arms:
		t.Fatalf("off setting armed timer %v", arm.delay)
	default:
	}
	t.Log("PASS negative setting: no hold, no timer, A received while B blocks")
}
