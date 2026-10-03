//go:build linux

package poll

import (
	"testing"
	"time"
)

func TestRuntimeParking(t *testing.T) {
	p := openPoller(t)
	entered := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		first := true
		done <- p.raw.Read(func(uintptr) bool {
			n, err := p.waitReady()
			if first {
				close(entered)
				first = false
			}
			return n != 0 || err != nil
		})
	}()
	<-entered
	if err := p.Wake(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runtime parked wait did not wake")
	}
}
