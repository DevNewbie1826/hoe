// Package loop owns transport descriptors and dispatches connection readiness.
package loop

import (
	"runtime"
	"time"

	"github.com/DevNewbie1826/hoe/internal/poll"
)

// Handler receives serial callbacks for each connection. OnEvent performs
// nonblocking I/O; HTTP-level blocking semantics belong to the connection
// adapter. A handler may wait without blocking any other connection.
type Handler interface {
	// OnOpen announces an accepted, registered socket.
	OnOpen(*Conn)
	// OnEvent receives coalesced readiness; bytes retain socket FIFO ordering.
	OnEvent(*Conn, poll.Event)
	// OnClose follows prior callbacks after loop-owned closure.
	OnClose(*Conn, error)
}

// Config selects loop and worker settings.
type Config struct {
	// Loops defaults to DefaultLoops when zero.
	Loops int
	// IdleWorkerTimeout defaults to one minute when nonpositive.
	IdleWorkerTimeout time.Duration
	// Handler receives transport callbacks and must be non-nil.
	Handler Handler
}

// DefaultLoops is the reactor count used when Config.Loops is zero.
func DefaultLoops() int { return max(1, runtime.NumCPU()/4) }
