package http1

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
)

// A request line whose bytes before the line ending exactly fill the reader
// leaves the line ending as its own fragment. That fragment terminates the
// request line, not the head, so the parser must keep reading the headers.
func TestReadRequestLineFillsReader(t *testing.T) {
	const (
		small = 1024
		large = 4096
	)
	for _, tc := range []struct {
		line int
		size int
	}{
		{small - 1, small},
		{small, small},
		{small + 1, small},
		{large - 1, large},
		{large, large},
		{large + 1, large},
	} {
		name := fmt.Sprintf("%dB/%dB", tc.line, tc.size)
		t.Run(name, func(t *testing.T) {
			targetLen := tc.line - len("GET ") - len(" HTTP/1.1")
			target := "/" + strings.Repeat("a", targetLen-2) + "z"
			raw := "GET " + target + " HTTP/1.1" + "\r\n" + "Host: t\r\n\r\n"
			r, err := ReadRequest(bufio.NewReaderSize(strings.NewReader(raw), tc.size), 1<<20)
			if err != nil {
				t.Fatalf("ReadRequest: %v", err)
			}
			got := r.RequestURI
			if len(got) != targetLen {
				t.Fatalf("request-target length %d, want %d", len(got), targetLen)
			}
			if !strings.HasPrefix(got, "/a") {
				t.Fatalf("request-target prefix %q, want %q", got[:2], "/a")
			}
			if !strings.HasSuffix(got, "z") {
				t.Fatalf("request-target suffix %q, want %q", got[len(got)-1:], "z")
			}
			if got != target {
				t.Fatalf("request-target does not match the sent bytes")
			}
		})
	}
}
