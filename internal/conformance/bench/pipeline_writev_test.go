//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

// TestPipelineRoundOneWritev is the device for write batching: a pipelined
// round of sixteen responses must reach the kernel in one vector write. The
// counter is the real syscall, so a build that flushes per response fails.
func TestPipelineRoundOneWritev(t *testing.T) {
	rig := pipelineServer(t, pipelineConfig{delay: 0, controlled: true,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "batch")
		})})
	paths := make([]string, 16)
	for i := range paths {
		paths[i] = "/"
	}
	before := systest.Snapshot().WritevCalls
	pipelineSend(t, rig.client, paths...)
	for range 16 {
		pipelineReceipt(t, rig, "batch")
	}
	if calls := systest.Snapshot().WritevCalls - before; calls != 1 {
		t.Fatalf("writev calls=%d, want 1 for the whole round", calls)
	}
}
