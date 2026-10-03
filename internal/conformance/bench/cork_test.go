//go:build linux || darwin

package bench

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

var dateLine = regexp.MustCompile(`(?m)^Date: [^\r]*\r\n`)

func rawResponse(t testing.TB, addr, request string) []byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err = io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return dateLine.ReplaceAll(p, []byte("Date: normalized\r\n"))
}

func TestCorkedResponseUsesOneWritev(t *testing.T) {
	// Given: multiple writes totaling 32KiB, below the bounded cork watermark.
	data := strings.Repeat("x", 32<<10)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		for start := 0; start < len(data); start += 4096 {
			io.WriteString(w, data[start:start+4096])
		}
	})
	std, _ := launch(t, false, h)
	own, _ := launch(t, true, h)
	request := "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
	want := rawResponse(t, std, request)
	before := systest.Snapshot()
	// When: the real event engine emits the complete response.
	got := rawResponse(t, own, request)
	// Then: byte-identical output was corked into one kernel vector write.
	if !bytes.Equal(want, got) {
		t.Fatal("corked response differs from std")
	}
	if calls := systest.Snapshot().WritevCalls - before.WritevCalls; calls != 1 {
		t.Fatalf("writev calls=%d, want one", calls)
	}
}

func TestPipelineResponsePrecedesBlockedHandler(t *testing.T) {
	// Given: the second handler cannot complete until the first body is read.
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/second" {
			<-release
		}
		io.WriteString(w, hello)
	})
	addr, _ := launch(t, true, h)
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	in := bufio.NewReader(c)
	// When: both requests arrive in one read round.
	_, err = io.WriteString(c, strings.Replace(get, "GET / ", "GET /first ", 1)+strings.Replace(get, "GET / ", "GET /second ", 1))
	if err != nil {
		t.Fatal(err)
	}
	// Then: corking never hides an earlier response behind a blocking handler.
	if err := readHello(in); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := readHello(in); err != nil {
		t.Fatal(err)
	}
}
