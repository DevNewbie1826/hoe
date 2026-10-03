//go:build linux

package bench

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/sys/systest"
)

func tcpSource(t testing.TB, data []byte) (*net.TCPListener, func()) {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var writers sync.WaitGroup
	go func() {
		defer close(done)
		for {
			c, err := l.AcceptTCP()
			if err != nil {
				return
			}
			writers.Add(1)
			go func() {
				defer writers.Done()
				defer c.Close()
				c.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := c.Write(data); err != nil {
					t.Error(err)
				}
			}()
		}
	}()
	return l, func() {
		l.Close()
		<-done
		writers.Wait()
	}
}

func socketHandler(t testing.TB, l *net.TCPListener, size int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		src, err := net.DialTCP("tcp", nil, l.Addr().(*net.TCPAddr))
		if err != nil {
			t.Error(err)
			return
		}
		defer src.Close()
		src.SetReadDeadline(time.Now().Add(5 * time.Second))
		w.Header().Set("Content-Length", strconv.Itoa(size))
		if n, err := io.Copy(w, src); err != nil || n != int64(size) {
			t.Errorf("io.Copy(TCPConn)=%d,%v", n, err)
		}
	})
}

func TestSpliceUsed(t *testing.T) {
	// Given: a real TCP source, and identical explicit-length std/hoe handlers.
	data := bytes.Repeat([]byte("0123456789abcdef"), 2048)
	l, cleanup := tcpSource(t, data)
	defer cleanup()
	h := socketHandler(t, l, len(data))
	std, _ := launch(t, false, h)
	own, _ := launch(t, true, h)
	request := "GET / HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
	want := rawResponse(t, std, request)
	before := systest.Snapshot()
	// When: io.Copy(w, *net.TCPConn) runs through the real engine.
	got := rawResponse(t, own, request)
	after := systest.Snapshot()
	// Then: byte-identical output used Linux socket->pipe->socket splice.
	if !bytes.Equal(want, got) {
		t.Fatal("splice output differs from std")
	}
	verifyBody(t, got, data, "GET")
	if after.SpliceCalls <= before.SpliceCalls || after.SpliceBytes <= before.SpliceBytes {
		t.Fatalf("splice not used: before=%+v after=%+v", before, after)
	}
	t.Logf("SAME splice calls=%d bytes=%d", after.SpliceCalls-before.SpliceCalls, after.SpliceBytes-before.SpliceBytes)
}

func BenchmarkSocketCopy(b *testing.B) {
	data := bytes.Repeat([]byte("0123456789abcdef"), 2048)
	l, cleanup := tcpSource(b, data)
	defer cleanup()
	for _, own := range []bool{false, true} {
		name := "std"
		if own {
			name = "hoe"
		}
		b.Run(name, func(b *testing.B) {
			addr, _ := launch(b, own, socketHandler(b, l, len(data)))
			c, err := net.DialTimeout("tcp", addr, 5*time.Second)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			c.SetDeadline(time.Now().Add(time.Minute))
			in := bufio.NewReader(c)
			b.SetBytes(int64(len(data)))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := io.WriteString(c, get); err != nil {
					b.Fatal(err)
				}
				r, err := http.ReadResponse(in, nil)
				if err != nil {
					b.Fatal(err)
				}
				p, err := io.ReadAll(r.Body)
				r.Body.Close()
				if err != nil || !bytes.Equal(p, data) {
					b.Fatal("invalid socket response", err)
				}
			}
		})
	}
}
