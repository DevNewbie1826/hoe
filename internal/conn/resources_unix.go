//go:build linux || darwin

package conn

import (
	"net/http"
	"sync"
)

var resources = struct {
	sync.Mutex
	engines map[*Engine]struct{}
	changed chan struct{}
}{engines: make(map[*Engine]struct{}), changed: make(chan struct{})}

// ResourceSnapshot exposes owned resources to internal conformance proofs.
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

func trackEngine(e *Engine, add bool) {
	resources.Lock()
	if add {
		resources.engines[e] = struct{}{}
	} else {
		delete(resources.engines, e)
	}
	close(resources.changed)
	resources.changed = make(chan struct{})
	resources.Unlock()
}
func resourceChanged() {
	resources.Lock()
	close(resources.changed)
	resources.changed = make(chan struct{})
	resources.Unlock()
}

// WatchResources subscribes before an action to the next internal lifecycle event.
func WatchResources() <-chan struct{} {
	resources.Lock()
	defer resources.Unlock()
	return resources.changed
}

// SnapshotResources counts live HTTP engine resources and idle buffer loans.
func SnapshotResources() ResourceSnapshot {
	resources.Lock()
	es := make([]*Engine, 0, len(resources.engines))
	for e := range resources.engines {
		es = append(es, e)
	}
	resources.Unlock()
	var result ResourceSnapshot
	result.Engines = len(es)
	for _, e := range es {
		e.mu.Lock()
		cs := make([]*Conn, 0, len(e.conns))
		for _, c := range e.conns {
			cs = append(cs, c)
		}
		e.mu.Unlock()
		result.Connections += len(cs)
		for _, c := range cs {
			c.mu.Lock()
			if c.eof {
				result.PeerEOF++
			}
			c.mu.Unlock()
			c.readMu.Lock()
			if c.state.Load() == int32(http.StateIdle) && len(c.input) > 0 {
				result.IdleBuffers++
			}
			c.readMu.Unlock()
		}
	}
	return result
}
