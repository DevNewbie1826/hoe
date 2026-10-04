//go:build !linux && !darwin

package conn

import (
	"net"
	"net/http"
	"time"
)

// UpgradeGroup is unavailable on platforms served by net/http.
type UpgradeGroup struct{}

// UpgradeConn is unavailable on platforms served by net/http.
type UpgradeConn struct{}

// IsUpgradeWriter returns false for standard-library fallback writers.
func IsUpgradeWriter(http.ResponseWriter) bool { return false }

// Adopt reports that a reactor is unavailable.
func (*UpgradeGroup) Adopt(net.Conn) (*UpgradeConn, error) { return nil, http.ErrNotSupported }

// Start is unreachable for an unsupported reactor.
func (*UpgradeConn) Start([]byte, func([]byte) error, func(error)) {}

// Close reports that a reactor is unavailable.
func (*UpgradeConn) Close() error { return http.ErrNotSupported }

// Write reports that a reactor is unavailable.
func (*UpgradeConn) Write([]byte) (int, error) { return 0, http.ErrNotSupported }

// SetWriteDeadline reports that a reactor is unavailable.
func (*UpgradeConn) SetWriteDeadline(time.Time) error { return http.ErrNotSupported }
