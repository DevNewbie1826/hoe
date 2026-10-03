//go:build linux || darwin

package poll

import (
	"errors"
	"os"
	"sync"
	"syscall"
)

type registration struct {
	fd          int
	token       uint64
	read, write bool
}

// Poller is owned by one loop. Only Wake may run concurrently.
type Poller struct {
	backend
	file   *os.File
	raw    syscall.RawConn
	regs   map[int]*registration
	tokens map[uint64]*registration
	out    []Event
	wakeMu sync.Mutex
	closed bool
}

// New opens a kernel poller and registers it with Go's network poller.
func New() (*Poller, error) {
	p := &Poller{regs: make(map[int]*registration), tokens: make(map[uint64]*registration), out: make([]Event, 0, MaxEvents)}
	if err := p.open(); err != nil {
		return nil, err
	}
	file, err := p.pollFile()
	if err != nil {
		p.closeFDs()
		syscall.Close(p.fd)
		return nil, err
	}
	p.file = file
	raw, err := p.file.SyscallConn()
	if err != nil {
		p.closeFDs()
		p.file.Close()
		return nil, err
	}
	p.raw = raw
	return p, nil
}

// Add registers a descriptor with a nonzero, unique token.
func (p *Poller) Add(fd int, token uint64, read, write bool) error {
	if token == 0 || p.regs[fd] != nil || p.tokens[token] != nil {
		return syscall.EINVAL
	}
	r := &registration{fd: fd, token: token, read: read, write: write}
	if err := p.register(r, true); err != nil {
		// kqueue may apply one filter before rejecting another. Roll back
		// and retain its udata until the next batch before releasing r.
		removeErr := p.remove(r)
		if errors.Is(removeErr, syscall.ENOENT) || errors.Is(removeErr, syscall.EBADF) {
			removeErr = nil
		}
		return errors.Join(err, removeErr)
	}
	p.regs[fd], p.tokens[token] = r, r
	return nil
}

// Modify changes a descriptor's interests without changing its identity.
func (p *Poller) Modify(fd int, token uint64, read, write bool) error {
	r := p.regs[fd]
	if r == nil || r.token != token {
		return syscall.ENOENT
	}
	oldRead, oldWrite := r.read, r.write
	r.read, r.write = read, write
	if err := p.register(r, false); err != nil {
		r.read, r.write = oldRead, oldWrite
		return err
	}
	return nil
}

// Delete removes a registration.
func (p *Poller) Delete(fd int) error {
	r := p.regs[fd]
	if r == nil {
		return nil
	}
	if err := p.remove(r); err != nil && !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.EBADF) {
		return err
	}
	delete(p.regs, fd)
	delete(p.tokens, r.token)
	return nil
}

// Wait parks until readiness or a wake; events live until the next Wait.
func (p *Poller) Wait() ([]Event, error) {
	var n int
	var waitErr error
	err := p.raw.Read(func(uintptr) bool {
		n, waitErr = p.waitReady()
		if errors.Is(waitErr, syscall.EINTR) {
			waitErr = nil
			return false
		}
		return n > 0 || waitErr != nil
	})
	if err != nil {
		return nil, err
	}
	if waitErr != nil {
		return nil, waitErr
	}
	p.out = p.out[:0]
	if err := p.collect(n); err != nil {
		return nil, err
	}
	return p.out, nil
}

// Wake interrupts Wait; concurrent wakes coalesce in the kernel.
func (p *Poller) Wake() error {
	p.wakeMu.Lock()
	defer p.wakeMu.Unlock()
	if p.closed {
		return os.ErrClosed
	}
	return p.signal()
}

// Close releases the poller after its owner has stopped waiting.
func (p *Poller) Close() error {
	p.wakeMu.Lock()
	defer p.wakeMu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return errors.Join(p.closeFDs(), p.file.Close())
}
