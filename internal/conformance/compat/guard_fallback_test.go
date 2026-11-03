package compat

import (
	"bufio"
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
)

func TestGuardOtherGOOSFallsBack(t *testing.T) {
	root := moduleRoot(t)
	scratch := t.TempDir()
	copyTree(t, root, scratch)
	out := filepath.Join(t.TempDir(), "conn.a")
	cmd := exec.Command("go", "build", "-o", out, "./internal/conn")
	cmd.Dir = scratch
	cmd.Env = append(os.Environ(), "GOOS=windows", "GOARCH=amd64", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("GOOS=windows build of internal/conn: %v\n%s", err, b)
	}
	if _, err := os.Stat(out); err != nil {
		t.Fatal(err)
	}
	pinSupportedFalse(t, filepath.Join(scratch, "internal/conn/engine_other.go"))
	pinFallbackWhenUnsupported(t, filepath.Join(scratch, "server.go"))
}

func TestGuardPipelineFlushDelayNoopOnFallback(t *testing.T) {
	if os.PathSeparator == '\\' {
		t.Skip("Unix listener unavailable")
	}
	run := func(own bool) string {
		dir, err := os.MkdirTemp("", "hoe-delay-")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		l, err := net.Listen("unix", filepath.Join(dir, "s"))
		if err != nil {
			t.Fatal(err)
		}
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/b" {
				<-release
			}
			fmt.Fprint(w, r.URL.Path)
		})
		done := make(chan error, 1)
		var stop func() error
		if own {
			s := &hoe.Server{Handler: h, PipelineFlushDelay: 5 * time.Second}
			stop = s.Close
			go func() { done <- s.Serve(l) }()
		} else {
			s := &http.Server{Handler: h}
			stop = s.Close
			go func() { done <- s.Serve(l) }()
		}
		t.Cleanup(func() {
			stop()
			l.Close()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("fallback server leaked")
			}
		})
		var c net.Conn
		var last error
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) && c == nil {
			c, last = net.DialTimeout("unix", l.Addr().String(), 50*time.Millisecond)
		}
		if c == nil {
			t.Fatalf("dial unix: %v", last)
		}
		defer c.Close()
		if _, err := io.WriteString(c, "GET /a HTTP/1.1\r\nHost: t\r\n\r\nGET /b HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
			t.Fatal(err)
		}
		if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		res, err := http.ReadResponse(bufio.NewReader(c), &http.Request{Method: "GET"})
		if err != nil {
			t.Fatalf("own=%v first pipelined response: %v", own, err)
		}
		defer res.Body.Close()
		p, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return string(p)
	}
	got := run(true)
	want := run(false)
	if want != "/a" || got != want {
		t.Fatalf("fallback PipelineFlushDelay body hoe=%q std=%q, want /a immediately", got, want)
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Name() == ".git" {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		return os.WriteFile(target, b, info.Mode().Perm())
	})
	if err != nil {
		t.Fatal(err)
	}
}

func pinSupportedFalse(t *testing.T, path string) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "Supported" || fn.Body == nil || len(fn.Body.List) != 1 {
			continue
		}
		ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			t.Fatal("Supported does not return one value")
		}
		id, ok := ret.Results[0].(*ast.Ident)
		if !ok || id.Name != "false" {
			t.Fatal("other-GOOS Supported() must return false so Serve falls back to net/http")
		}
		found = true
	}
	if !found {
		t.Fatal("engine_other.go Supported() not found")
	}
}

func pinFallbackWhenUnsupported(t *testing.T, path string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte("!conn.Supported()")) {
		t.Fatal("Serve must fall back to net/http when conn.Supported() is false")
	}
}
