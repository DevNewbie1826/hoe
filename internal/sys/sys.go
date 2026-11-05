// Package sys implements the nonblocking transport's syscall boundary.
package sys

import (
	"errors"
	"sync/atomic"
)

// ErrUnsupported reports an unavailable syscall.
var ErrUnsupported = errors.New("hoe: unsupported syscall")

// Counters records attempted transport calls and transferred bytes.
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
	// ReadIntoCalls counts socket reads that filled the retained input buffer
	// directly instead of staging through a scratch copy.
	ReadIntoCalls uint64
	// ReadIntoBytes counts bytes those reads delivered.
	ReadIntoBytes uint64
	// PushCalls counts explicit kernel cork push attempts, not enable/probe calls.
	PushCalls uint64
}

var sendfileCalls, sendfileBytes, spliceCalls, spliceBytes, writevCalls atomic.Uint64
var readIntoCalls, readIntoBytes atomic.Uint64
var pushCalls atomic.Uint64

// SnapshotCounters reads process-wide syscall accounting.
// Concurrent snapshots may span transfers; quiesce to compare exact totals.
func SnapshotCounters() Counters {
	return Counters{
		SendfileCalls: sendfileCalls.Load(),
		SendfileBytes: sendfileBytes.Load(),
		SpliceCalls:   spliceCalls.Load(),
		SpliceBytes:   spliceBytes.Load(),
		WritevCalls:   writevCalls.Load(),
		ReadIntoCalls: readIntoCalls.Load(),
		ReadIntoBytes: readIntoBytes.Load(),
		PushCalls:     pushCalls.Load(),
	}
}

// ResetCounters resets accounting; callers must quiesce transfers first.
func ResetCounters() {
	sendfileCalls.Store(0)
	sendfileBytes.Store(0)
	spliceCalls.Store(0)
	spliceBytes.Store(0)
	writevCalls.Store(0)
	readIntoCalls.Store(0)
	readIntoBytes.Store(0)
	pushCalls.Store(0)
}

// CountReadInto records a socket read that filled the retained input buffer
// directly.
func CountReadInto(n int) {
	readIntoCalls.Add(1)
	readIntoBytes.Add(uint64(n))
}
