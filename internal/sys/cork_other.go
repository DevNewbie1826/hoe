//go:build !linux && !darwin

package sys

// EnableCork is a no-op on standard-server fallback platforms.
func EnableCork(int) error { return nil }

// PushCork is a no-op on standard-server fallback platforms.
func PushCork(int, bool) error { return nil }

// CorkEnabled reports no cork on standard-server fallback platforms.
func CorkEnabled(int) (bool, error) { return false, nil }
