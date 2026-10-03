//go:build darwin

package poll

import (
	"errors"
	"syscall"
)

type kqueueResult struct {
	events []syscall.Kevent_t
	err    error
}

func (p *Poller) relay() {
	defer close(p.joined)
	var events [MaxEvents]syscall.Kevent_t
	for {
		select {
		case <-p.stop:
			return
		case <-p.begin:
		}
		// Do not prefetch: preserve the batch available at the owner's Wait.
		clear(events[:])
		var n int
		var err error
		for {
			n, err = syscall.Kevent(p.fd, nil, events[:], nil)
			if !errors.Is(err, syscall.EINTR) {
				break
			}
		}
		if n < 0 {
			n = 0
		}
		select {
		case <-p.stop:
			return
		case p.results <- kqueueResult{events: events[:n], err: err}:
		}
		for {
			_, writeErr := syscall.Write(p.notifyWriteFD, []byte{1})
			if errors.Is(writeErr, syscall.EINTR) {
				continue
			}
			if writeErr != nil {
				return
			}
			break
		}
		// The next begin is sent only after the owner copies this batch.
		if err != nil {
			return
		}
	}
}
