//go:build darwin

package loop_test

import "syscall"

// dupAt duplicates from onto to at an exact descriptor number; darwin has no
// dup3, and dup2 matches dup3 with flags 0 whenever from != to (always here).
func dupAt(from, to int) error { return syscall.Dup2(from, to) }
