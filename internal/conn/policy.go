package conn

import "time"

// Policy supplies the event engine's runtime keep-alive and pipeline settings.
type Policy struct {
	KeepAlive          func() bool
	PipelineFlushDelay time.Duration
	// Loops selects the engine's reactor count; zero uses loop.DefaultLoops.
	// Sharing one engine across many listeners sizes it by GOMAXPROCS instead.
	Loops int
}
