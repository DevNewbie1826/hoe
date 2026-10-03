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
}

func (p *Poller) pollFile() (*os.File, error) {
	if err := syscall.SetNonblock(p.fd, true); err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(p.fd), "hoe-epoll"), nil
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
func (p *Poller) closeFDs() error { return syscall.Close(p.wake) }
