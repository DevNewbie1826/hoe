//go:build darwin

package poll

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

type backend struct {
	fd                      int
	pipe                    [2]int
	events                  [MaxEvents]syscall.Kevent_t
	notifyFD, notifyWriteFD int
	notifyWriter            *os.File
	results                 chan kqueueResult
	begin                   chan struct{}
	requested               bool
	stop                    chan struct{}
	joined                  chan struct{}
	// kqueue stores Go pointer values, but does not own references for the GC.
	// Retain deleted registrations until all events from the next wait resolve.
	retired []*registration
}

func (p *Poller) pollFile() (*os.File, error) {
	// XNU does not expose kqueue as a nonblocking file. One helper waits
	// in kevent and relays batches to this runtime-polled pipe. There are
	// no per-connection waiters, and Close joins the helper.
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	for _, target := range []struct {
		file *os.File
		fd   *int
	}{{r, &p.notifyFD}, {w, &p.notifyWriteFD}} {
		raw, err := target.file.SyscallConn()
		if err != nil {
			r.Close()
			w.Close()
			return nil, err
		}
		if err := raw.Control(func(fd uintptr) { *target.fd = int(fd) }); err != nil {
			r.Close()
			w.Close()
			return nil, err
		}
	}
	p.notifyWriter = w
	p.results = make(chan kqueueResult, 1)
	p.begin = make(chan struct{}, 1)
	p.stop = make(chan struct{})
	p.joined = make(chan struct{})
	go p.relay()
	return r, nil
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
	if err := syscall.Pipe(p.pipe[:]); err != nil {
		syscall.Close(fd)
		return err
	}
	for _, fd := range p.pipe {
		syscall.CloseOnExec(fd)
		if err := syscall.SetNonblock(fd, true); err != nil {
			p.closeFDs()
			syscall.Close(p.fd)
			return err
		}
	}
	ev := syscall.Kevent_t{Ident: uint64(p.pipe[0]), Filter: syscall.EVFILT_READ, Flags: syscall.EV_ADD | syscall.EV_CLEAR}
	_, err = syscall.Kevent(p.fd, []syscall.Kevent_t{ev}, nil, nil)
	if err != nil {
		p.closeFDs()
		syscall.Close(p.fd)
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

func (p *Poller) waitReady() (int, error) {
	if !p.requested {
		p.requested = true
		p.begin <- struct{}{}
	}
	var b [1]byte
	n, err := syscall.Read(p.notifyFD, b[:])
	if errors.Is(err, syscall.EAGAIN) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, os.ErrClosed
	}
	result := <-p.results
	copy(p.events[:], result.events)
	p.requested = false
	return len(result.events), result.err
}

func (p *Poller) collect(n int) error {
	for _, e := range p.events[:n] {
		if int(e.Ident) == p.pipe[0] {
			var b [256]byte
			for {
				_, err := syscall.Read(p.pipe[0], b[:])
				if errors.Is(err, syscall.EINTR) {
					continue
				}
				if errors.Is(err, syscall.EAGAIN) {
					break
				}
				if err != nil {
					return err
				}
			}
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
	for {
		_, err := syscall.Write(p.pipe[1], []byte{1})
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EAGAIN) {
			return nil
		}
		return err
	}
}
func (p *Poller) closeFDs() error {
	var err error
	if p.stop != nil {
		close(p.stop)
		err = p.signal()
		<-p.joined
		err = errors.Join(err, p.notifyWriter.Close())
		err = errors.Join(err, syscall.Close(p.fd))
	}
	return errors.Join(err, syscall.Close(p.pipe[0]), syscall.Close(p.pipe[1]))
}
