//go:build linux || darwin

package bench

import (
	"bytes"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func pipelineBytes(t *testing.T, addr, request string) []byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPipelineDifferential(t *testing.T) {
	for _, closeAt := range []int{15, 7} {
		t.Run(strconv.Itoa(closeAt), func(t *testing.T) {
			// Given identical handlers and fixed Date for exact byte comparison.
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Date", "Sun, 04 Oct 2026 00:00:00 GMT")
				io.WriteString(w, r.URL.Path)
			})
			std, _ := launch(t, false, h)
			own, _ := launch(t, true, h)
			request := ""
			for i := range 16 {
				request += "GET /" + strconv.Itoa(i) + " HTTP/1.1\r\nHost: t\r\n"
				if i == closeAt {
					request += "Connection: close\r\n"
				}
				request += "\r\n"
			}
			t.Logf("TCP Write(%q)", request)
			// When the real servers receive the same sixteen-deep pipeline.
			want := pipelineBytes(t, std, request)
			got := pipelineBytes(t, own, request)
			// Then every byte agrees, including close-at-middle truncation.
			if !bytes.Equal(want, got) {
				t.Fatalf("pipeline differs\nstd=%q\nhoe=%q", want, got)
			}
			if responses := bytes.Count(got, []byte("HTTP/1.1 200 OK")); responses != closeAt+1 {
				t.Fatalf("responses=%d want=%d", responses, closeAt+1)
			}
			t.Logf("PASS exact pipeline bytes=%q", got)
		})
	}
}
