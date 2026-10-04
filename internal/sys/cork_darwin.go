//go:build darwin

package sys

import "syscall"

const tcpCork = 0x4 // TCP_NOPUSH.

// PushCork counts one push attempt and forces queued output to leave the kernel.
// Clearing TCP_NOPUSH alone does not push Darwin's held partial packet.
func PushCork(fd int, keep bool) error {
	pushCalls.Add(1)
	if err := syscall.SetsockoptInt(fd, syscall.IPPROTO_TCP, tcpCork, 0); err != nil {
		return err
	}
	// Do not use Write: it intentionally skips zero-length buffers.
	if _, err := syscall.Write(fd, nil); err != nil {
		return err
	}
	if keep {
		return EnableCork(fd)
	}
	return nil
}
