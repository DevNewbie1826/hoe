//go:build !linux && !darwin

package loop

import "github.com/DevNewbie1826/hoe/internal/poll"

// Group is unavailable on standard-server fallback platforms.
type Group struct{}

// Conn is unavailable on standard-server fallback platforms.
type Conn struct{}

// ReadReady has no active descriptor on this platform.
func (c *Conn) ReadReady() <-chan struct{} { return c.Done() }

// WriteReady has no active descriptor on this platform.
func (c *Conn) WriteReady() <-chan struct{} { return c.Done() }

// New reports an unsupported platform.
func New(Config) (*Group, error) { return nil, poll.ErrUnsupported }

// Listen reports an unsupported platform.
func (*Group) Listen(int) error { return poll.ErrUnsupported }

// Close has no resources to release.
func (*Group) Close() error { return nil }

// Done has no active connection on this platform.
func (*Conn) Done() <-chan struct{} { ch := make(chan struct{}); close(ch); return ch }

// Read reports an unsupported platform.
func (*Conn) Read([]byte) (int, error) { return 0, poll.ErrUnsupported }

// Write reports an unsupported platform.
func (*Conn) Write([]byte) (int, error) { return 0, poll.ErrUnsupported }

// Writev reports an unsupported platform.
func (*Conn) Writev([][]byte) (int, error) { return 0, poll.ErrUnsupported }

// Sendfile reports an unsupported platform.
func (*Conn) Sendfile(int, *int64, int) (int, error) { return 0, poll.ErrUnsupported }

// Splice reports an unsupported platform.
func (*Conn) Splice(int, int) (int, error) { return 0, poll.ErrUnsupported }

// EnableCork is a no-op on standard-server fallback platforms.
func (*Conn) EnableCork() error { return nil }

// PushCork is a no-op on standard-server fallback platforms.
func (*Conn) PushCork(bool) error { return nil }

// CorkEnabled reports no cork on standard-server fallback platforms.
func (*Conn) CorkEnabled() (bool, error) { return false, nil }

// SetInterest reports an unsupported platform.
func (*Conn) SetInterest(bool, bool) error { return poll.ErrUnsupported }

// Close has no resources to release.
func (*Conn) Close() error { return nil }

// Detach reports an unsupported platform.
func (*Conn) Detach() (int, error) { return -1, poll.ErrUnsupported }
