// Package sys implements the nonblocking transport's syscall boundary.
package sys

import (
	"errors"
	"sync/atomic"
)

// ErrUnsupported reports an unavailable syscall.
var ErrUnsupported = errors.New("hoe: unsupported syscall")

// Counters records attempted zero-copy/vector calls and transferred bytes.
type Counters struct {
	// SendfileCalls counts attempted nonempty sendfile syscalls.
	SendfileCalls uint64
	// SendfileBytes counts bytes actually sent by sendfile.
	SendfileBytes uint64
	// SpliceCalls counts attempted nonempty Linux splice syscalls.
	SpliceCalls uint64
	// SpliceBytes counts bytes actually moved by splice.
	SpliceBytes uint64
	// WritevCalls counts attempted nonempty writev syscalls.
	WritevCalls uint64
}

var sendfileCalls, sendfileBytes, spliceCalls, spliceBytes, writevCalls atomic.Uint64

// SnapshotCounters reads process-wide syscall accounting.
// Concurrent snapshots may span transfers; quiesce to compare exact totals.
func SnapshotCounters() Counters {
	return Counters{sendfileCalls.Load(), sendfileBytes.Load(), spliceCalls.Load(), spliceBytes.Load(), writevCalls.Load()}
}

// ResetCounters resets accounting; callers must quiesce transfers first.
func ResetCounters() {
	sendfileCalls.Store(0)
	sendfileBytes.Store(0)
	spliceCalls.Store(0)
	spliceBytes.Store(0)
	writevCalls.Store(0)
}
