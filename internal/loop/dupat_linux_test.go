//go:build linux

package loop_test

import "syscall"

// dupAt duplicates from onto to at an exact descriptor number.
func dupAt(from, to int) error { return syscall.Dup3(from, to, 0) }
