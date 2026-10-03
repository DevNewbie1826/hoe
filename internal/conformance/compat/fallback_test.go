package compat

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

func tlsCertificate() tls.Certificate {
	s := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer s.Close()
	return s.TLS.Certificates[0]
}

func fallbackTLS(t *testing.T, own, h2 bool) (string, *tls.Config) {
	t.Helper()
	cert := tlsCertificate()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetHTTP2(h2)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Compare stable negotiated fields, never random session key material.
		json.NewEncoder(w).Encode(struct {
			Proto                    int
			Version, Cipher          uint16
			Complete, Resume, Mutual bool
			ALPN, ServerName         string
			PeerCerts, Chains        int
		}{r.ProtoMajor, r.TLS.Version, r.TLS.CipherSuite, r.TLS.HandshakeComplete, r.TLS.DidResume,
			r.TLS.NegotiatedProtocolIsMutual, r.TLS.NegotiatedProtocol, r.TLS.ServerName, len(r.TLS.PeerCertificates), len(r.TLS.VerifiedChains)})
	})
	cfg := &http.Server{Handler: h, Protocols: p, TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}}}
	var srv interface {
		Close() error
		ServeTLS(net.Listener, string, string) error
	} = cfg
	if own {
		srv = hoeConfig(cfg)
	}
	done := make(chan error, 1)
	go func() { done <- srv.ServeTLS(l, "", "") }()
	t.Cleanup(func() {
		srv.Close()
		l.Close()
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("TLS server leaked")
		}
	})
	roots := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	// httptest's test certificate is shared; use its trusted client roots.
	clientTLS := roots.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	roots.Close()
	clientTLS.ServerName = "example.com"
	return l.Addr().String(), clientTLS
}

func tlsResult(t *testing.T, own, h2 bool) []byte {
	t.Helper()
	addr, cfg := fallbackTLS(t, own, h2)
	if !h2 {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", addr, cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(c, request("GET", "/", true)); err != nil {
			t.Fatal(err)
		}
		p, err := io.ReadAll(c)
		if err != nil {
			t.Fatal(err)
		}
		return normalize(p)
	}
	tr := &http.Transport{TLSClientConfig: cfg, ForceAttemptHTTP2: h2}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	r, err := client.Get("https://" + addr + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	want := 1
	if h2 {
		want = 2
	}
	if r.ProtoMajor != want {
		t.Fatalf("protocol=%s want major=%d", r.Proto, want)
	}
	if h2 && r.TLS.NegotiatedProtocol != "h2" {
		t.Fatal("ALPN h2 not negotiated")
	}
	t.Logf("response %s TLS ALPN=%q body=%s", r.Status, r.TLS.NegotiatedProtocol, p)
	return p
}

func TestFallbackTLS(t *testing.T)   { same(t, tlsResult(t, false, false), tlsResult(t, true, false)) }
func TestFallbackHTTP2(t *testing.T) { same(t, tlsResult(t, false, true), tlsResult(t, true, true)) }

func TestFallbackH2C(t *testing.T) {
	run := func(own bool) []byte {
		p := new(http.Protocols)
		p.SetUnencryptedHTTP2(true)
		addr, _ := launch(t, own, &http.Server{Protocols: p, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "proto=%d", r.ProtoMajor) })})
		tr := &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		t.Cleanup(tr.CloseIdleConnections)
		r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + addr + "/")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Body.Close()
		pb, err := io.ReadAll(r.Body)
		if err != nil || r.ProtoMajor != 2 || string(pb) != "proto=2" {
			t.Fatalf("H2C proto=%s body=%q err=%v", r.Proto, pb, err)
		}
		return pb
	}
	same(t, run(false), run(true))
}

func TestFallbackUnixListener(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix listener unavailable")
	}
	run := func(own bool) []byte {
		// Darwin Unix socket paths have a short maximum length.
		dir, err := os.MkdirTemp("", "hoe-unix-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		l, err := net.Listen("unix", filepath.Join(dir, "s"))
		if err != nil {
			t.Fatal(err)
		}
		cfg := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "unix") })}
		var s server = cfg
		if own {
			s = hoeConfig(cfg)
		}
		done := make(chan error, 1)
		go func() { done <- s.Serve(l) }()
		t.Cleanup(func() { s.Close(); l.Close(); <-done })
		c, err := net.Dial("unix", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		c.SetDeadline(time.Now().Add(5 * time.Second))
		io.WriteString(c, request("GET", "/", true))
		p, err := io.ReadAll(c)
		if err != nil {
			t.Fatal(err)
		}
		return normalize(p)
	}
	same(t, run(false), run(true))
}

func TestEngineInUse(t *testing.T) {
	writer := make(chan string, 1)
	addr, _ := launch(t, true, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writer <- reflect.TypeOf(w).String()
		fmt.Fprint(w, "engine")
	})})
	capture(t, addr, request("GET", "/", true))
	if typ := <-writer; typ == "*http.response" {
		t.Fatal("plaintext TCP uses std writer; N3 engine required")
	}
}
