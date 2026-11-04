//go:build !linux && !darwin

package conn

// ResourceSnapshot exposes owned resources to internal conformance proofs.
// On fallback platforms the native engine does not exist, so every count is
// zero and tests observe exactly that.
type ResourceSnapshot struct {
	// Engines counts registered transport owners.
	Engines int
	// Connections counts owned HTTP connection lifetimes.
	Connections int
	// IdleBuffers counts idle connections retaining an input loan.
	IdleBuffers int
	// PeerEOF counts owned connections whose read half has closed.
	PeerEOF int
}

// WatchResources has no engine lifecycle to subscribe to on this platform.
func WatchResources() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// SnapshotResources reports zero engine resources on this platform.
func SnapshotResources() ResourceSnapshot {
	return ResourceSnapshot{}
}
