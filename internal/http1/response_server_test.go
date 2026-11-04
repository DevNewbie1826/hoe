//go:build linux || darwin

package http1_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func responseServer(t *testing.T, native bool, handler http.Handler) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	var closeServer func() error
	if native {
		server := &hoe.Server{Handler: handler}
		closeServer = server.Close
		go func() { done <- server.Serve(listener) }()
	} else {
		server := &http.Server{Handler: handler}
		closeServer = server.Close
		go func() { done <- server.Serve(listener) }()
	}
	t.Cleanup(func() {
		if err := closeServer(); err != nil {
			t.Error(err)
		}
		if err := <-done; !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve: %v", err)
		}
	})
	return listener.Addr().String()
}

func responseWire(t *testing.T, addr, request string) []byte {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
	result, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFlushReacquireWireAndPipeline(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", "Mon, 02 Jan 2006 15:04:05 GMT")
		for _, part := range []string{"before", r.URL.Path, "after"} {
			if _, err := io.WriteString(w, part); err != nil {
				t.Error(err)
				return
			}
			if err := http.NewResponseController(w).Flush(); err != nil {
				t.Error(err)
				return
			}
		}
	})
	std := responseServer(t, false, handler)
	native := responseServer(t, true, handler)
	for _, tc := range []struct {
		name    string
		request string
	}{
		{"single", "GET /one HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"},
		{"pipeline", "GET /one HTTP/1.1\r\nHost: t\r\n\r\nGET /two HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := responseWire(t, std, tc.request)
			got := responseWire(t, native, tc.request)
			if !bytes.Equal(got, want) {
				t.Fatalf("wire mismatch\nstd: %q\nhoe: %q", want, got)
			}
		})
	}
}

func TestFlushReacquireWriteDeadline(t *testing.T) {
	result := make(chan error, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if p := recover(); p != nil {
				result <- fmt.Errorf("handler panic: %v", p)
			}
		}()
		controller := http.NewResponseController(w)
		if _, err := io.WriteString(w, "before"); err != nil {
			result <- err
			return
		}
		if err := controller.Flush(); err != nil {
			result <- err
			return
		}
		if err := controller.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
			result <- err
			return
		}
		if _, err := io.WriteString(w, "after"); err != nil {
			result <- err
			return
		}
		flushErr := controller.Flush()
		if err := controller.SetWriteDeadline(time.Time{}); err != nil {
			result <- err
			return
		}
		if flushErr != nil {
			if _, err := w.Write(make([]byte, 4096)); !errors.Is(err, flushErr) {
				result <- fmt.Errorf("large write after flush error: %v", err)
				return
			}
		}
		result <- flushErr
	})
	addr := responseServer(t, true, handler)
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	select {
	case err := <-result:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("flush after expired deadline: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not report the write deadline")
	}
	body, err := io.ReadAll(resp.Body)
	if string(body) != "before" || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("body after write error=%q err=%v", body, err)
	}
}
