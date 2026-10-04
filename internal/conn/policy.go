package conn

import "time"

// Policy supplies the event engine's runtime keep-alive and pipeline settings.
type Policy struct {
	KeepAlive          func() bool
	PipelineFlushDelay time.Duration
}
