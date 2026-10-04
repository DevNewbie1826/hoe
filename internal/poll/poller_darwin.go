//go:build darwin

package poll

import (
	"errors"
	"syscall"
	"unsafe"
)

type backend struct {
	fd     int
	events [MaxEvents]syscall.Kevent_t
	// kqueue stores Go pointer values, but does not own references for the GC.
	// Retain deleted registrations until all events from the next wait resolve.
	retired []*registration
}

func (p *Poller) open() error {
	syscall.ForkLock.RLock()
	fd, err := syscall.Kqueue()
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return err
	}
	p.fd = fd
	ev := syscall.Kevent_t{Ident: 1, Filter: syscall.EVFILT_USER, Flags: syscall.EV_ADD | syscall.EV_CLEAR}
	_, err = syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
	if err != nil {
		p.closeFDs()
	}
	return err
}

func (p *Poller) register(r *registration, _ bool) error {
	var changes [2]syscall.Kevent_t
	for i, filter := range []int16{syscall.EVFILT_READ, syscall.EVFILT_WRITE} {
		flags := uint16(syscall.EV_ADD | syscall.EV_CLEAR | syscall.EV_DISABLE)
		if (i == 0 && r.read) || (i == 1 && r.write) {
			flags = syscall.EV_ADD | syscall.EV_CLEAR | syscall.EV_ENABLE
		}
		changes[i] = syscall.Kevent_t{Ident: uint64(r.fd), Filter: filter, Flags: flags, Udata: (*byte)(unsafe.Pointer(&r.token))}
	}
	_, err := syscall.Kevent(p.fd, changes[:], nil, nil)
	return err
}

func (p *Poller) remove(r *registration) error {
	// Each filter is independently removed: one missing filter must not prevent
	// the other filter from being deregistered.
	p.retired = append(p.retired, r)
	var result error
	for _, filter := range []int16{syscall.EVFILT_READ, syscall.EVFILT_WRITE} {
		ev := syscall.Kevent_t{Ident: uint64(r.fd), Filter: filter, Flags: syscall.EV_DELETE}
		_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
		if err != nil && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.EBADF) {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (p *Poller) wait() (int, error) {
	for {
		n, err := syscall.Kevent(p.fd, nil, p.events[:], nil)
		if !errors.Is(err, syscall.EINTR) {
			return n, err
		}
	}
}

func (p *Poller) collect(n int) error {
	for _, e := range p.events[:n] {
		if e.Filter == syscall.EVFILT_USER {
			continue
		}
		if e.Udata == nil {
			continue
		}
		token := *(*uint64)(unsafe.Pointer(e.Udata))
		r := p.tokens[token]
		if r == nil {
			continue
		}
		p.out = append(p.out, Event{FD: r.fd, Token: token, Read: e.Filter == syscall.EVFILT_READ,
			Write: e.Filter == syscall.EVFILT_WRITE, EOF: e.Flags&(syscall.EV_EOF|syscall.EV_ERROR) != 0})
	}
	clear(p.retired)
	p.retired = p.retired[:0]
	// Do not leave untraced pointers to dead registrations in the scratch array.
	clear(p.events[:n])
	return nil
}

func (p *Poller) signal() error {
	ev := syscall.Kevent_t{Ident: 1, Filter: syscall.EVFILT_USER, Fflags: syscall.NOTE_TRIGGER}
	for {
		_, err := syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		return err
	}
}
func (p *Poller) closeFDs() error { return syscall.Close(p.fd) }
