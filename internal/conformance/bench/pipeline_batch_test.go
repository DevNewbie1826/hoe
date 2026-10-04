//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestPipelineBatch(t *testing.T) {
	for _, off := range []bool{false, true} {
		name := "on"
		if off {
			name = "off"
		}
		t.Run(name, func(t *testing.T) {
			// Given sixteen buffered requests and a controlled cap timer.
			states := make(chan bool, 16)
			delay := time.Duration(0)
			if off {
				delay = -1
			}
			rig := pipelineServer(t, pipelineConfig{delay: delay, controlled: true,
				handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					held, err := pipelineTransport(r).CorkEnabled()
					if err != nil {
						t.Error(err)
					}
					states <- held
					io.WriteString(w, "batch")
				})})
			paths := make([]string, 16)
			for i := range paths {
				paths[i] = "/"
			}
			before := systest.Snapshot().PushCalls
			// When the real TCP client submits the entire pipeline in one write.
			pipelineSend(t, rig.client, paths...)
			if off {
				// No cork is armed, so the existing TCP_NODELAY writes must
				// deliver every response without a cap wait.
				rig.client.SetReadDeadline(time.Now().Add(pipelineTolerance))
			}
			for range 16 {
				pipelineReceipt(t, rig, "batch")
			}
			// Then all handlers observed the real kernel flag. Coalescing
			// pushes once; off mode pushes nothing.
			for range 16 {
				if held := pipelineEvent(t, states); held != !off {
					t.Errorf("handler kernel hold=%v want=%v", held, !off)
				}
			}
			want := uint64(1)
			if off {
				want = 0
			}
			for range want {
				event := pipelineEvent(t, rig.pushes)
				if event.expired || event.err != nil {
					t.Fatalf("round push=%+v", event)
				}
			}
			if delta := systest.Snapshot().PushCalls - before; delta != want {
				t.Fatalf("push boundaries=%d want=%d", delta, want)
			}
			if off {
				select {
				case event := <-rig.pushes:
					t.Fatalf("off mode pushed %+v", event)
				default:
				}
			}
			pipelineProbe(t, rig.raw, false)
			t.Logf("PASS push boundaries=%d; hold during=%v, after=false", want, !off)
		})
	}
}
