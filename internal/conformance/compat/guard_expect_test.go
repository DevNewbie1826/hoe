package compat

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestGuardExpectCloseBranch(t *testing.T) {
	root := moduleRoot(t)
	http1 := filepath.Join(root, "internal", "http1")
	archive := filepath.Join(t.TempDir(), "expect.a")
	compile := exec.Command("go", "tool", "compile", "-p", "github.com/DevNewbie1826/hoe/internal/http1", "-o", archive, "expect.go", "expect_pre_go127.go")
	compile.Dir = http1
	if b, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("expect.go and expect_pre_go127.go must both compile: %v\n%s", err, b)
	}
	build := exec.Command("go", "build", "-o", filepath.Join(t.TempDir(), "http1.a"), "./internal/http1")
	build.Dir = root
	if b, err := build.CombinedOutput(); err != nil {
		t.Fatalf("running toolchain must build internal/http1: %v\n%s", err, b)
	}
	direct := exec.Command("go", "test", "-count=1", "-run", "^TestGuardExpectCloseBranchDirect$", "./internal/http1")
	direct.Dir = root
	direct.Env = os.Environ()
	if b, err := direct.CombinedOutput(); err != nil {
		t.Fatalf("expect-close branch on the running toolchain: %v\n%s", err, b)
	}
}
