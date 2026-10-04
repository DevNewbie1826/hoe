//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func TestPipelineSingleKeepAlive(t *testing.T) {
	// Given a single request, with no next request buffered.
	rig := pipelineServer(t, pipelineConfig{controlled: true,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pipelineProbe(t, pipelineTransport(r), false)
			io.WriteString(w, "single")
		})})
	before := systest.Snapshot().PushCalls

	// When the keep-alive client sends exactly one GET and reads its response.
	pipelineSend(t, rig.client, "/")
	pipelineReceipt(t, rig, "single")

	// Then normal traffic needs neither a cork, a cap timer nor a push.
	pipelineProbe(t, rig.raw, false)
	if delta := systest.Snapshot().PushCalls - before; delta != 0 {
		t.Fatalf("single-request push boundaries=%d want=0", delta)
	}
	select {
	case <-rig.arms:
		t.Fatal("single request armed a cap timer")
	default:
	}
	t.Log("PASS single keep-alive GET: pushes=0; cork=false; no timer")
}
