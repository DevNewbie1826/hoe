//go:build !linux && !darwin

package poll

// Poller is unavailable on platforms served by the standard server.
type Poller struct{}

// New reports an unsupported kernel.
func New() (*Poller, error) { return nil, ErrUnsupported }

// Add reports an unsupported kernel.
func (*Poller) Add(int, uint64, bool, bool) error { return ErrUnsupported }

// Modify reports an unsupported kernel.
func (*Poller) Modify(int, uint64, bool, bool) error { return ErrUnsupported }

// Delete reports an unsupported kernel.
func (*Poller) Delete(int) error { return ErrUnsupported }

// Wait reports an unsupported kernel.
func (*Poller) Wait() ([]Event, error) { return nil, ErrUnsupported }

// Wake reports an unsupported kernel.
func (*Poller) Wake() error { return ErrUnsupported }

// Close has no resources to release.
func (*Poller) Close() error { return nil }
