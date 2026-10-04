//go:build linux || darwin

package conn

import "time"

// PipelineHooks observes the kernel boundary in conformance tests.
// Install once in ConnContext, before any request is served. Callbacks must
// not block or change the pipeline lifecycle. Timer returns true to replace the
// runtime timer with a test-controlled invocation of expire.
type PipelineHooks struct {
	Timer  func(delay time.Duration, expire func()) bool
	Pushed func(expired bool, err error)
	// BeforeExpiry and AfterExpiry bracket the callback outside its lock.
	// Tests can pause an already-running callback before releasing its fd.
	BeforeExpiry func()
	AfterExpiry  func()
}

// SetPipelineHooks installs per-connection conformance instrumentation.
func (c *Conn) SetPipelineHooks(h PipelineHooks) { c.pipelineHooks = h }

// CorkEnabled probes the kernel flag, not the connection's cached hold state.
func (c *Conn) CorkEnabled() (bool, error) { return c.raw.CorkEnabled() }
