package compat

import (
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func TestGuardErrorLogReceivesNativePanic(t *testing.T) {
	var buf bytes.Buffer
	kind := make(chan string, 1)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &hoe.Server{
		ErrorLog: log.New(&buf, "", 0),
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			kind <- reflect.TypeOf(w).String()
			panic("guard-boom")
		}),
	}
	serveHoe(t, s, l)
	c := dial(t, l.Addr().String())
	if _, err := io.WriteString(c, request("GET", "/", true)); err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(c)
	select {
	case got := <-kind:
		if got == "*http.response" {
			t.Fatal("panic was served by net/http, not the native engine")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not run")
	}
	logged := buf.String()
	if !strings.Contains(logged, "http: panic serving") || !strings.Contains(logged, "guard-boom") {
		t.Fatalf("ErrorLog = %q, want native panic", logged)
	}
}
