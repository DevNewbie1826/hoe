//go:build linux || darwin

package compat

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	"golang.org/x/net/http2"
)

type fallbackBaseKey struct{}

func fallbackBase(marker string, listenerAddr *string) func(net.Listener) context.Context {
	return func(l net.Listener) context.Context {
		if listenerAddr != nil {
			*listenerAddr = l.Addr().String()
		}
		return context.WithValue(context.Background(), fallbackBaseKey{}, marker)
	}
}

func observeBase(w http.ResponseWriter, r *http.Request) {
	v, _ := r.Context().Value(fallbackBaseKey{}).(string)
	fmt.Fprintf(w, "path=%s base=%s", r.URL.Path, v)
}

// TestFallbackServeKeepsBaseContext covers the paths net/http serves instead of
// the native engine: a unix listener, ServeTLS, and h2c. Each path gets its own
// marker, so a regression that stopped delegating BaseContext names the route it
// broke instead of failing one shared value.
func TestFallbackServeKeepsBaseContext(t *testing.T) {
	t.Run("unix-listener", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "hoe-unix-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		sock := dir + "/fallback.sock"
		l, err := net.Listen("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		s := &hoe.Server{Handler: http.HandlerFunc(observeBase), BaseContext: fallbackBase("unix-marker", nil)}
		done := make(chan error, 1)
		go func() { done <- s.Serve(l) }()
		t.Cleanup(func() { s.Close(); l.Close() })

		c, err := net.Dial("unix", sock)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(c, request("GET", "/unix", true)); err != nil {
			t.Fatal(err)
		}
		if got := readResponse(t, bufio.NewReader(c), "GET"); string(got) != "path=/unix base=unix-marker" {
			t.Fatalf("unix listener request: %q", got)
		}
		stopFallback(t, s, l, done, "Serve returned nil", "unix serving loop leaked")
	})

	t.Run("serve-tls", func(t *testing.T) {
		var listenerAddr string
		cfg := &http.Server{
			Handler:     http.HandlerFunc(observeBase),
			BaseContext: fallbackBase("tls-marker", &listenerAddr),
			TLSConfig:   &tls.Config{Certificates: []tls.Certificate{tlsCertificate()}},
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := hoeConfig(cfg)
		done := make(chan error, 1)
		go func() { done <- s.ServeTLS(l, "", "") }()
		t.Cleanup(func() { s.Close(); l.Close() })

		roots := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		clientTLS := roots.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
		roots.Close()
		clientTLS.ServerName = "example.com"
		tr := &http.Transport{TLSClientConfig: clientTLS}
		t.Cleanup(tr.CloseIdleConnections)
		resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("https://" + l.Addr().String() + "/tls")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if got := string(body); got != "path=/tls base=tls-marker" {
			t.Fatalf("ServeTLS request: %q", got)
		}
		if listenerAddr == "" {
			t.Fatal("BaseContext never ran on the TLS path")
		}
		stopFallback(t, s, l, done, "ServeTLS returned nil", "TLS serving loop leaked")
	})

	t.Run("h2c", func(t *testing.T) {
		var listenerAddr string
		p := new(http.Protocols)
		p.SetHTTP1(true)
		p.SetUnencryptedHTTP2(true)
		cfg := &http.Server{
			Handler:     http.HandlerFunc(observeBase),
			BaseContext: fallbackBase("h2c-marker", &listenerAddr),
			Protocols:   p,
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		s := hoeConfig(cfg)
		done := make(chan error, 1)
		go func() { done <- s.Serve(l) }()
		t.Cleanup(func() { s.Close(); l.Close() })

		tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		t.Cleanup(tr.CloseIdleConnections)
		resp, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + l.Addr().String() + "/h2c")
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if resp.ProtoMajor != 2 {
			t.Fatalf("h2c negotiated proto=%s", resp.Proto)
		}
		if got := string(body); got != "path=/h2c base=h2c-marker" {
			t.Fatalf("h2c request: %q", got)
		}
		if listenerAddr == "" {
			t.Fatal("BaseContext never ran on the h2c path")
		}
		stopFallback(t, s, l, done, "h2c Serve returned nil", "h2c serving loop leaked")
	})
}

func stopFallback(t *testing.T, s *hoe.Server, l net.Listener, done chan error, nilMsg, leakMsg string) {
	t.Helper()
	s.Close()
	l.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Error(nilMsg)
		} else if !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			t.Errorf("serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Error(leakMsg)
	}
}
