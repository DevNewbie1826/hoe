package http1

import "testing"

func TestGuardExpectCloseBranchDirect(t *testing.T) {
	w := &Response{continuePending: true, body: &Body{}}
	w.disableContinue(true)
	if !closeUnsentContinue {
		t.Fatal("running toolchain selected the pre-1.27 expect-close branch")
	}
	if !w.closeAfter || !w.skipDrain || !w.body.expectClosed.Load() {
		t.Fatalf("unsent continue left open: closeAfter=%v skipDrain=%v expectClosed=%v", w.closeAfter, w.skipDrain, w.body.expectClosed.Load())
	}
}
