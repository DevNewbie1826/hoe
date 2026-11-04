package http1

import (
	"bufio"
	"fmt"
	"strings"
	"testing"
)

// A header line whose bytes before the line ending exactly fill the reader
// leaves a lone "\r\n" (or "\n") fragment that is the end of that line, not
// a blank line ending the head. The small first-request reader makes this
// boundary ordinary.
func TestReadRequestHeaderLineFillsReader(t *testing.T) {
	const size = 1024
	for _, eol := range []string{"\r\n", "\n"} {
		for _, n := range []int{size, 2 * size} {
			name := fmt.Sprintf("%q/%dx", eol, n/size)
			t.Run(name, func(t *testing.T) {
				value := strings.Repeat("x", n-len("X-Long: "))
				raw := "GET / HTTP/1.1" + eol + "Host: t" + eol + "X-Long: " + value + eol + "X-After: ok" + eol + eol
				r, err := ReadRequest(bufio.NewReaderSize(strings.NewReader(raw), size), 1<<20)
				if err != nil {
					t.Fatalf("ReadRequest: %v", err)
				}
				if got := r.Header.Get("X-Long"); got != value {
					t.Fatalf("X-Long length %d, want %d", len(got), len(value))
				}
				if got := r.Header.Get("X-After"); got != "ok" {
					t.Fatalf("header after the full-reader line was lost: X-After=%q", got)
				}
			})
		}
	}
}
