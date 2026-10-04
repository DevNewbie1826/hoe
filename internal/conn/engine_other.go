//go:build !linux && !darwin

// Package conn joins the HTTP/1 codec to loop-owned TCP descriptors.
package conn

import (
	"context"
	"net"
	"net/http"

	"github.com/DevNewbie1826/hoe/internal/poll"
)

// Engine is unavailable on standard-library fallback platforms.
type Engine struct{}

// Supported reports whether plaintext TCP can use the event engine.
func Supported() bool { return false }

// New reports an unsupported event engine platform.
func New(*http.Server, *net.TCPListener, Policy) (*Engine, error) {
	return nil, poll.ErrUnsupported
}

// Serve reports an unsupported event engine platform.
func (*Engine) Serve(*net.TCPListener) error { return poll.ErrUnsupported }

// Shutdown has no engine resources on this platform.
func (*Engine) Shutdown(context.Context) error { return nil }

// Close has no engine resources on this platform.
func (*Engine) Close() error { return nil }

// CloseIdle has no engine resources on this platform.
func (*Engine) CloseIdle() {}
