//go:build linux || darwin

package sys

import (
	"runtime"
	"syscall"
	"unsafe"
)

// Writev writes up to 64 nonempty buffers in order. A short count leaves the
// remainder with the caller, including buffers beyond this syscall's batch.
func Writev(fd int, buffers [][]byte) (int, error) {
	var vectors [64]syscall.Iovec
	n := 0
	for _, b := range buffers {
		if len(b) == 0 {
			continue
		}
		vectors[n].Base = &b[0]
		vectors[n].SetLen(len(b))
		n++
		if n == len(vectors) {
			break
		}
	}
	if n == 0 {
		return 0, nil
	}
	writevCalls.Add(1)
	var count uintptr
	var errno syscall.Errno
	if runtime.GOOS == "linux" {
		// Transport sockets are nonblocking; EAGAIN is handled by the
		// adapter, so this call must not hand a P to the runtime scheduler.
		count, _, errno = syscall.RawSyscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&vectors[0])), uintptr(n))
	} else {
		count, _, errno = syscall.Syscall(syscall.SYS_WRITEV, uintptr(fd), uintptr(unsafe.Pointer(&vectors[0])), uintptr(n))
	}
	runtime.KeepAlive(buffers)
	if errno != 0 {
		return 0, errno
	}
	return int(count), nil
}

// Sendfile transfers file bytes, advancing offset by the actual count on both
// supported platforms, including partial transfers returned with an error.
// Offset must be non-nil. The input file's seek position is not changed.
func Sendfile(out, in int, offset *int64, count int) (int, error) {
	if offset == nil || count < 0 {
		return 0, syscall.EINVAL
	}
	if count == 0 {
		return 0, nil
	}
	sendfileCalls.Add(1)
	n, err := sendfile(out, in, offset, count)
	if n > 0 {
		sendfileBytes.Add(uint64(n))
	}
	return n, err
}
