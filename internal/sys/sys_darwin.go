//go:build darwin

package sys

import "syscall"

// Accept accepts a socket, applying nonblocking and close-on-exec before use.
func Accept(fd int) (int, syscall.Sockaddr, error) {
	syscall.ForkLock.RLock()
	n, addr, err := syscall.Accept(fd)
	if err == nil {
		syscall.CloseOnExec(n)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return -1, nil, err
	}
	if err := syscall.SetNonblock(n, true); err != nil {
		syscall.Close(n)
		return -1, nil, err
	}
	// BSD writev/sendfile use this per-socket option to suppress SIGPIPE.
	if err := syscall.SetsockoptInt(n, syscall.SOL_SOCKET, syscall.SO_NOSIGPIPE, 1); err != nil {
		syscall.Close(n)
		return -1, nil, err
	}
	return n, addr, nil
}

// Read receives bytes from a nonblocking socket.
func Read(fd int, b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n, err := syscall.Read(fd, b)
	if n < 0 {
		n = 0
	}
	return n, err
}

// Write sends bytes on a nonblocking socket.
func Write(fd int, b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	n, err := syscall.Write(fd, b)
	if n < 0 {
		n = 0
	}
	return n, err
}

func sendfile(out, in int, offset *int64, count int) (int, error) {
	n, err := syscall.Sendfile(out, in, offset, count)
	*offset += int64(n)
	return n, err
}

// Splice reports that Darwin has no splice syscall.
func Splice(out, in, count int) (int, error) { return 0, ErrUnsupported }
