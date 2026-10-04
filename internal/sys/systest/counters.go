// Package systest exposes internal transport accounting to engine tests.
package systest

import "github.com/DevNewbie1826/hoe/internal/sys"

// Counters is a snapshot of syscall attempts and transferred bytes.
type Counters = sys.Counters

// Snapshot reads the process-wide counters.
func Snapshot() Counters { return sys.SnapshotCounters() }

// Reset clears counters while no transfers are running.
func Reset() { sys.ResetCounters() }
