//go:build linux

package sys

import (
	"runtime"
	"syscall"
	"unsafe"
)

// Accept accepts a nonblocking close-on-exec socket.
func Accept(fd int) (int, syscall.Sockaddr, error) {
	return syscall.Accept4(fd, syscall.SOCK_NONBLOCK|syscall.SOCK_CLOEXEC)
}

// Read receives bytes from a nonblocking socket without scheduler handoff.
func Read(fd int, b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n, _, errno := syscall.RawSyscall6(syscall.SYS_RECVFROM, uintptr(fd), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), 0, 0, 0)
	runtime.KeepAlive(b)
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

// Write sends bytes without scheduler handoff or process-directed SIGPIPE.
func Write(fd int, b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n, _, errno := syscall.RawSyscall6(syscall.SYS_SENDTO, uintptr(fd), uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), syscall.MSG_NOSIGNAL, 0, 0)
	runtime.KeepAlive(b)
	if errno != 0 {
		return 0, errno
	}
	return int(n), nil
}

func sendfile(out, in int, offset *int64, count int) (int, error) {
	return syscall.Sendfile(out, in, offset, count)
}

// SplicePipe creates a nonblocking close-on-exec pipe for socket transfers.
// The caller must close both descriptors after the transfer finishes.
func SplicePipe() ([2]int, error) {
	var fds [2]int
	err := syscall.Pipe2(fds[:], syscall.O_NONBLOCK|syscall.O_CLOEXEC)
	return fds, err
}

// Splice moves bytes between a socket and a pipe, or between pipes, without
// blocking. At least one descriptor must be a pipe; callers own both ends.
func Splice(out, in, count int) (int, error) {
	if count < 0 {
		return 0, syscall.EINVAL
	}
	if count == 0 {
		return 0, nil
	}
	spliceCalls.Add(1)
	n, _, errno := syscall.Syscall6(syscall.SYS_SPLICE, uintptr(in), 0, uintptr(out), 0, uintptr(count), 2|1)
	if errno != 0 {
		return 0, errno
	}
	spliceBytes.Add(uint64(n))
	return int(n), nil
}
