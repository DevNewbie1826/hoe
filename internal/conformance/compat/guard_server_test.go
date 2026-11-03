package compat

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func TestGuardNilHandlerUsesDefaultServeMux(t *testing.T) {
	const path = "/hoe-guard-nil-handler"
	mux := http.NewServeMux()
	kind := make(chan string, 1)
	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		kind <- reflect.TypeOf(w).String()
		fmt.Fprint(w, "mux-hit")
	})
	orig := http.DefaultServeMux
	http.DefaultServeMux = mux
	t.Cleanup(func() { http.DefaultServeMux = orig })

	addr, _ := launch(t, true, &http.Server{})
	c := dial(t, addr)
	if _, err := io.WriteString(c, request("GET", path, true)); err != nil {
		t.Fatal(err)
	}
	body := readResponse(t, bufio.NewReader(c), "GET")
	if string(body) != "mux-hit" {
		t.Fatalf("nil Handler body = %q, want mux-hit", body)
	}
	select {
	case got := <-kind:
		if got == "*http.response" {
			t.Fatal("nil Handler was served by net/http, not the native engine")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("DefaultServeMux handler did not run")
	}
}

func TestGuardEmptyAddrListensHTTP(t *testing.T) {
	const want = "guard-empty-addr"
	s := &hoe.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, want)
	})}
	errc := make(chan error, 1)
	go func() { errc <- s.ListenAndServe() }()
	t.Cleanup(func() {
		s.Close()
		select {
		case <-errc:
		case <-time.After(5 * time.Second):
			t.Error("ListenAndServe leaked port 80")
		}
	})

	var conn net.Conn
	var last error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && conn == nil {
		select {
		case err := <-errc:
			t.Fatalf("ListenAndServe empty Addr: %v", err)
		default:
		}
		conn, last = net.DialTimeout("tcp", "127.0.0.1:80", 50*time.Millisecond)
	}
	if conn == nil {
		t.Fatalf("empty Addr did not listen on :http (port 80): %v", last)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, request("GET", "/", true)); err != nil {
		t.Fatal(err)
	}
	body := readResponse(t, bufio.NewReader(conn), "GET")
	if string(body) != want {
		t.Fatalf("port 80 body = %q, want %s", body, want)
	}
}

func TestGuardListenAndServeClosedAfterShutdown(t *testing.T) {
	// The address is already bound. A pre-check returns ErrServerClosed without
	// listening; skipping it fails with address already in use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	s := &hoe.Server{Addr: ln.Addr().String(), Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := s.ListenAndServe(); !errors.Is(err, hoe.ErrServerClosed) {
		t.Fatalf("ListenAndServe after Shutdown = %v, want ErrServerClosed before bind", err)
	}
	if err := s.ListenAndServeTLS("", ""); !errors.Is(err, hoe.ErrServerClosed) {
		t.Fatalf("ListenAndServeTLS after Shutdown = %v, want ErrServerClosed before bind", err)
	}
}

func TestGuardPackageEntryPoints(t *testing.T) {
	if mode := os.Getenv("HOE_GUARD_CHILD"); mode != "" {
		servePackageChild(mode)
		return
	}
	t.Run("Serve", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		errc := make(chan error, 1)
		go func() {
			errc <- hoe.Serve(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, "pkg-serve")
			}))
		}()
		t.Cleanup(func() {
			l.Close()
			select {
			case <-errc:
			case <-time.After(5 * time.Second):
				t.Error("package Serve leaked")
			}
		})
		if string(packageBody(t, l.Addr().String())) != "pkg-serve" {
			t.Fatal("package Serve did not serve the handler")
		}
	})
	t.Run("ServeTLS", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		cert, key := writeCertFiles(t)
		errc := make(chan error, 1)
		go func() {
			errc <- hoe.ServeTLS(l, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, "pkg-servetls")
			}), cert, key)
		}()
		t.Cleanup(func() {
			l.Close()
			select {
			case <-errc:
			case <-time.After(5 * time.Second):
				t.Error("package ServeTLS leaked")
			}
		})
		if got := tlsBody(t, l.Addr().String()); got != "pkg-servetls" {
			t.Fatalf("package ServeTLS body = %q", got)
		}
	})
	t.Run("ListenAndServe", func(t *testing.T) {
		addr := freeAddr(t)
		startPackageChild(t, "listen", addr, "", "")
		if got := string(packageBody(t, addr)); got != "pkg-listen" {
			t.Fatalf("package ListenAndServe body = %q", got)
		}
	})
	t.Run("ListenAndServeTLS", func(t *testing.T) {
		addr := freeAddr(t)
		cert, key := writeCertFiles(t)
		startPackageChild(t, "listentls", addr, cert, key)
		if got := tlsBody(t, addr); got != "pkg-listentls" {
			t.Fatalf("package ListenAndServeTLS body = %q", got)
		}
	})
}

func servePackageChild(mode string) {
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "pkg-"+mode)
	})
	var err error
	switch mode {
	case "listen":
		err = hoe.ListenAndServe(os.Getenv("HOE_GUARD_ADDR"), h)
	case "listentls":
		err = hoe.ListenAndServeTLS(os.Getenv("HOE_GUARD_ADDR"), os.Getenv("HOE_GUARD_CERT"), os.Getenv("HOE_GUARD_KEY"), h)
	default:
		os.Exit(2)
	}
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func startPackageChild(t *testing.T, mode, addr, cert, key string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestGuardPackageEntryPoints$", "-test.count=1")
	cmd.Env = append(os.Environ(), "HOE_GUARD_CHILD="+mode, "HOE_GUARD_ADDR="+addr, "HOE_GUARD_CERT="+cert, "HOE_GUARD_KEY="+key)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		cmd.Wait()
	})
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

func packageBody(t *testing.T, addr string) []byte {
	t.Helper()
	c := dialUntil(t, addr, 3*time.Second)
	if _, err := io.WriteString(c, request("GET", "/", true)); err != nil {
		t.Fatal(err)
	}
	return readResponse(t, bufio.NewReader(c), "GET")
}

func tlsBody(t *testing.T, addr string) string {
	t.Helper()
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: trustedClientTLS(t)}, Timeout: 5 * time.Second}
	var last error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("https://" + addr + "/")
		if err != nil {
			last = err
			continue
		}
		defer resp.Body.Close()
		p, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(p)
	}
	t.Fatalf("tls dial %s: %v", addr, last)
	return ""
}
