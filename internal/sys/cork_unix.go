//go:build linux || darwin

package sys

import "syscall"

// EnableCork enables the platform's kernel TCP cork.
func EnableCork(fd int) error {
	return syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, tcpCork, 1)
}

// CorkEnabled reports the actual kernel TCP cork flag.
func CorkEnabled(fd int) (bool, error) {
	value, err := syscall.GetsockoptInt(fd, syscall.IPPROTO_TCP, tcpCork)
	return value != 0, err
}
