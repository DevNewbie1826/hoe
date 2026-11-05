//go:build linux || darwin

package conn

import (
	"net/http"
	"testing"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

// TestHeadReadFillsInputDirectly pins that a request head reaches the retained
// input buffer in the socket read itself. Staging it through a scratch buffer
// first costs one copy of every head byte and shows up here as zero direct
// fills.
func TestHeadReadFillsInputDirectly(t *testing.T) {
	srv, client := parkedServer(t)
	defer srv.stop()

	req := "POST /head HTTP/1.1\r\nHost: t\r\nContent-Length: 0\r\n\r\n"
	for _, split := range []int{0, 17} {
		before := systest.Snapshot()
		if split == 0 {
			writeRaw(t, client, req)
		} else {
			writeRaw(t, client, req[:split])
			writeRaw(t, client, req[split:])
		}
		if s := readStatus(t, client, http.MethodPost).Status; s != "200 OK" {
			t.Fatalf("split %d status %q", split, s)
		}
		after := systest.Snapshot()
		calls := after.ReadIntoCalls - before.ReadIntoCalls
		bytes := after.ReadIntoBytes - before.ReadIntoBytes
		if calls == 0 || bytes < uint64(len(req)) {
			t.Fatalf("split %d read %d head bytes into the input buffer over %d socket reads, want the whole %d-byte head", split, bytes, calls, len(req))
		}
	}
}
