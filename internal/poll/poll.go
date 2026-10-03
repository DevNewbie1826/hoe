// Package poll provides edge-triggered readiness notifications.
// Linux parks the epoll descriptor in Go's runtime network poller and wakes
// through eventfd. Darwin blocks the owning loop goroutine in kevent and wakes
// through EVFILT_USER; it uses no pipe or relay goroutine.
package poll

import "errors"

// MaxEvents is the maximum number of events returned by one wait.
const MaxEvents = 1024

// ErrUnsupported reports an unavailable kernel backend.
var ErrUnsupported = errors.New("hoe: unsupported poller")

// Event identifies a registration, not merely a reusable descriptor.
type Event struct {
	// FD is the registered descriptor.
	FD int
	// Token identifies this descriptor's registration lifetime.
	Token uint64
	// Read indicates readable bytes or a read-side error.
	Read bool
	// Write indicates writable capacity.
	Write bool
	// EOF indicates half-close, hangup, or a kernel error.
	EOF bool
}
