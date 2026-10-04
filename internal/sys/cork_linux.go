//go:build linux

package sys

import "syscall"

const tcpCork = 3 // TCP_CORK.

// PushCork counts one push attempt; clearing TCP_CORK pushes queued output.
func PushCork(fd int, keep bool) error {
	pushCalls.Add(1)
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, tcpCork, 0); err != nil {
		return err
	}
	if keep {
		return EnableCork(fd)
	}
	return nil
}
