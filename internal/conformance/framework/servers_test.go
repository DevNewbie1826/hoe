package framework

import (
	"bytes"
	"errors"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func serve(t *testing.T, h http.Handler, own bool) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	std := &http.Server{Handler: h}
	var s interface {
		Serve(net.Listener) error
		Close() error
	} = std
	if own {
		s = &hoe.Server{Handler: h}
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case err := <-done:
			if !errors.Is(err, http.ErrServerClosed) {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server leaked")
		}
	})
	return "http://" + l.Addr().String()
}

func response(t *testing.T, req *http.Request) (int, string) {
	t.Helper()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return r.StatusCode, string(p)
}

func compare(t *testing.T, h http.Handler, method, path, ct, body, want string) {
	t.Helper()
	var previous string
	var status int
	for _, own := range []bool{false, true} {
		base := serve(t, h, own)
		req, err := http.NewRequest(method, base+path, bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", ct)
		req.Header.Set("X-Trace", "abc123")
		req.Host = "api.local"
		code, p := response(t, req)
		if !own {
			previous, status = p, code
		} else if p != previous || code != status {
			t.Fatalf("DIFF status %d/%d body %q/%q", status, code, previous, p)
		}
		if want != "" && (p != want || code != 200) {
			t.Fatalf("want 200 %q got %d %q", want, code, p)
		}
	}
}

type upload struct{ field, name, body string }

func multipartBody(t *testing.T, fields [][2]string, files []upload) (string, string) {
	t.Helper()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	if err := w.SetBoundary("hoe-framework-boundary"); err != nil {
		t.Fatal(err)
	}
	for _, kv := range fields {
		if err := w.WriteField(kv[0], kv[1]); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range files {
		p, err := w.CreateFormFile(f.field, f.name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(p, f.body); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return w.FormDataContentType(), b.String()
}
