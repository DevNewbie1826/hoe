package bench

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

const hello = "Hello Benchmark"
const get = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"

type server interface {
	Serve(net.Listener) error
	Close() error
	Shutdown(context.Context) error
}

func launch(t testing.TB, own bool, h http.Handler) (string, server) {
	return launchConfig(t, own, &http.Server{Handler: h})
}

func launchConfig(t testing.TB, own bool, cfg *http.Server) (string, server) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var s server = cfg
	if own {
		s = &hoe.Server{Handler: cfg.Handler, ConnContext: cfg.ConnContext, ConnState: cfg.ConnState}
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		l.Close()
		select {
		case err := <-done:
			if err != http.ErrServerClosed {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Serve not joined")
		}
	})
	return l.Addr().String(), s
}

func readHello(in *bufio.Reader) error {
	first := true
	for {
		line, err := in.ReadSlice('\n')
		if err != nil {
			return err
		}
		if first && !bytes.Equal(line, []byte("HTTP/1.1 200 OK\r\n")) {
			return io.ErrUnexpectedEOF
		}
		first = false
		if bytes.Equal(line, []byte("\r\n")) {
			break
		}
	}
	body, err := in.Peek(len(hello))
	if err != nil {
		return err
	}
	if string(body) != hello {
		return io.ErrUnexpectedEOF
	}
	_, err = in.Discard(len(hello))
	return err
}

func BenchmarkKeepAlive(b *testing.B) {
	for _, own := range []bool{false, true} {
		name := "std"
		if own {
			name = "hoe"
		}
		for _, depth := range []int{1, 16} {
			suffix := "/hello"
			if depth == 16 {
				suffix = "/pipeline16"
			}
			b.Run(name+suffix, func(b *testing.B) {
				addr, _ := launch(b, own, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					io.WriteString(w, hello)
				}))
				c, err := net.DialTimeout("tcp", addr, 5*time.Second)
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(2 * time.Minute))
				in := bufio.NewReader(c)
				request := bytes.Repeat([]byte(get), depth)
				// Warm transport pools and the Date cache before counting.
				if _, err := c.Write(request); err != nil {
					b.Fatal(err)
				}
				for range depth {
					if err := readHello(in); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, err := c.Write(request); err != nil {
						b.Fatal(err)
					}
					for range depth {
						if err := readHello(in); err != nil {
							b.Fatal(err)
						}
					}
				}
				b.StopTimer()
			})
		}
	}
}
