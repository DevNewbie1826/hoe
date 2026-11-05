//go:build linux

package poll

import (
	"encoding/binary"
	"errors"
	"os"
	"syscall"
)

type backend struct {
	fd, wake int
	events   [MaxEvents]syscall.EpollEvent
	file     *os.File
	raw      syscall.RawConn
	// waitFunc is the RawConn.Read callback. It is a method value set once
	// in open, so a wait reuses it instead of allocating a closure per call.
	waitFunc func(uintptr) bool
	// waitN and waitErr carry the callback's result out of waitFunc. RawConn
	// may invoke the callback more than once, so every call overwrites them.
	waitN   int
	waitErr error
}

func (p *Poller) open() error {
	fd, err := syscall.EpollCreate1(syscall.EPOLL_CLOEXEC)
	if err != nil {
		return err
	}
	p.fd = fd
	w, _, errno := syscall.Syscall(syscall.SYS_EVENTFD2, 0, syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if errno != 0 {
		syscall.Close(fd)
		return errno
	}
	p.wake = int(w)
	if err := syscall.EpollCtl(fd, syscall.EPOLL_CTL_ADD, p.wake, &syscall.EpollEvent{Events: syscall.EPOLLIN}); err != nil {
		syscall.Close(p.wake)
		syscall.Close(fd)
		return err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		syscall.Close(p.wake)
		syscall.Close(fd)
		return err
	}
	p.file = os.NewFile(uintptr(fd), "hoe-epoll")
	raw, err := p.file.SyscallConn()
	if err != nil {
		p.closeFDs()
		return err
	}
	p.raw = raw
	p.waitFunc = p.waitReadyOnce
	return nil
}

func (p *Poller) register(r *registration, add bool) error {
	mask := uint32(1<<31) | syscall.EPOLLRDHUP
	if r.read {
		mask |= syscall.EPOLLIN
	}
	if r.write {
		mask |= syscall.EPOLLOUT
	}
	op := syscall.EPOLL_CTL_MOD
	if add {
		op = syscall.EPOLL_CTL_ADD
	}
	ev := syscall.EpollEvent{Events: mask, Fd: int32(r.token), Pad: int32(r.token >> 32)}
	return syscall.EpollCtl(p.fd, op, r.fd, &ev)
}

func (p *Poller) remove(r *registration) error {
	return syscall.EpollCtl(p.fd, syscall.EPOLL_CTL_DEL, r.fd, nil)
}
func (p *Poller) waitReady() (int, error) { return syscall.EpollWait(p.fd, p.events[:], 0) }

// waitReadyOnce performs one epoll wait and caches its result for wait. The
// callback returns false after EINTR so RawConn retries without blocking.
func (p *Poller) waitReadyOnce(uintptr) bool {
	n, err := p.waitReady()
	p.waitN, p.waitErr = n, err
	if errors.Is(err, syscall.EINTR) {
		p.waitErr = nil
		return false
	}
	return n > 0 || err != nil
}

func (p *Poller) wait() (int, error) {
	p.waitN, p.waitErr = 0, nil
	if err := p.raw.Read(p.waitFunc); err != nil {
		return 0, err
	}
	return p.waitN, p.waitErr
}

func (p *Poller) collect(n int) error {
	for _, e := range p.events[:n] {
		token := uint64(uint32(e.Fd)) | uint64(uint32(e.Pad))<<32
		if token == 0 {
			var b [8]byte
			for {
				_, err := syscall.Read(p.wake, b[:])
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if err != nil && !errors.Is(err, syscall.EAGAIN) {
					return err
				}
				break
			}
			continue
		}
		r := p.tokens[token]
		if r == nil {
			continue
		}
		p.out = append(p.out, Event{FD: r.fd, Token: token,
			Read:  e.Events&(syscall.EPOLLIN|syscall.EPOLLRDHUP|syscall.EPOLLHUP|syscall.EPOLLERR) != 0,
			Write: e.Events&syscall.EPOLLOUT != 0, EOF: e.Events&(syscall.EPOLLRDHUP|syscall.EPOLLHUP|syscall.EPOLLERR) != 0})
	}
	return nil
}

func (p *Poller) signal() error {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 1)
	for {
		_, err := syscall.Write(p.wake, b[:])
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		return err
	}
}
func (p *Poller) closeFDs() error { return errors.Join(syscall.Close(p.wake), p.file.Close()) }
