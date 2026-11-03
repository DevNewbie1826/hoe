package compat

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestGuardTLSNextProtoDisablesAutoH2(t *testing.T) {
	body := tlsNextProtoMajor(t, false)
	if body != "major=1" {
		t.Fatalf("non-nil TLSNextProto without Protocols negotiated %s, want HTTP/1.1", body)
	}
}

func TestGuardTLSNextProtoProtocolsEnablesH2(t *testing.T) {
	body := tlsNextProtoMajor(t, true)
	if body != "major=2" {
		t.Fatalf("Protocols HTTP/2 with non-nil TLSNextProto negotiated %s, want HTTP/2", body)
	}
}

func tlsNextProtoMajor(t *testing.T, protocolsH2 bool) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "major=%d", r.ProtoMajor)
	})
	cfg := &http.Server{
		Handler:      h,
		TLSConfig:    &tls.Config{Certificates: []tls.Certificate{tlsCertificate()}},
		TLSNextProto: map[string]func(*http.Server, *tls.Conn, http.Handler){},
	}
	if protocolsH2 {
		p := new(http.Protocols)
		p.SetHTTP1(true)
		p.SetHTTP2(true)
		cfg.Protocols = p
	}
	s := hoeConfig(cfg)
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(l, "", "") }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("TLS server leaked")
		}
	})
	tr := &http.Transport{TLSClientConfig: trustedClientTLS(t), ForceAttemptHTTP2: true}
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	var last error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("https://" + l.Addr().String() + "/")
		if err != nil {
			last = err
			continue
		}
		defer resp.Body.Close()
		p, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		if protocolsH2 && resp.TLS.NegotiatedProtocol != "h2" {
			t.Fatalf("ALPN = %q, want h2", resp.TLS.NegotiatedProtocol)
		}
		if !protocolsH2 && resp.TLS.NegotiatedProtocol == "h2" {
			t.Fatal("ALPN h2 negotiated although TLSNextProto disables automatic HTTP/2")
		}
		return string(p)
	}
	t.Fatalf("tls request: %v", last)
	return ""
}
