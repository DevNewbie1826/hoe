package compat

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller")
	}
	dir := filepath.Dir(file)
	for {
		if _, err := os.Stat(filepath.Join(dir, "server.go")); err == nil {
			if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("module root not found")
		}
		dir = parent
	}
}

func trustedClientTLS(t *testing.T) *tls.Config {
	t.Helper()
	roots := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer roots.Close()
	cfg := roots.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	cfg.ServerName = "example.com"
	return cfg
}

func writeCertFiles(t *testing.T) (string, string) {
	t.Helper()
	cert := tlsCertificate()
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]}), 0o600); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}

func dialUntil(t *testing.T, addr string, budget time.Duration) net.Conn {
	t.Helper()
	deadline := time.Now().Add(budget)
	var last error
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			t.Cleanup(func() { c.Close() })
			if err := c.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			return c
		}
		last = err
	}
	t.Fatalf("dial %s: %v", addr, last)
	return nil
}

func serveHoe(t *testing.T, s *hoe.Server, l net.Listener) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Serve leaked")
		}
	})
}
