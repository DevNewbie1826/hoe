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
	regs   map[int]*registration
	tokens map[uint64]*registration
	out    []Event
	wakeMu sync.Mutex
	closed bool
}

// New opens an edge-triggered kernel poller. Linux registers the epoll
// descriptor with Go's network poller; Darwin uses a blocking kqueue wait.
func New() (*Poller, error) {
	p := &Poller{regs: make(map[int]*registration), tokens: make(map[uint64]*registration), out: make([]Event, 0, MaxEvents)}
	if err := p.open(); err != nil {
		return nil, err
	}
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

// Wait waits until readiness or a wake; events live until the next Wait.
// Linux parks through syscall.RawConn.Read without blocking an OS thread.
// Darwin blocks the owner in kevent until readiness or an EVFILT_USER wake.
func (p *Poller) Wait() ([]Event, error) {
	n, err := p.wait()
	if err != nil {
		return nil, err
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
	return p.closeFDs()
}
