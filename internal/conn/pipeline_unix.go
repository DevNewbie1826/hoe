//go:build linux || darwin

package conn

import (
	"sync"
	"time"
)

// pipelineHold serializes hold epochs without taking writeMu: a writer can
// wait for readiness while the cap callback must still push earlier bytes.
type pipelineHold struct {
	mu              sync.Mutex
	holding         bool
	terminal        bool
	armed           bool
	generation      uint64
	timer           *time.Timer
	timerGeneration uint64
	timerPending    bool
}

func (c *Conn) beginPipeline() error {
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal || p.timerPending || c.engine.pipelineDelay < 0 {
		return nil
	}
	if err := c.raw.EnableCork(); err != nil {
		return err
	}
	p.holding = true
	return nil
}

// pipelineWrite writes each response into the kernel before the next handler.
// The timer starts only after the first successful held write, never while an
// idle connection or a handler with no output is waiting.
func (c *Conn) pipelineWrite(b []byte) (int, error) {
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	n, err := c.raw.Writev([][]byte{b})
	if n > 0 && p.holding && !p.armed {
		p.armed = true
		p.generation++
		generation := p.generation
		controlled := false
		if hook := c.pipelineHooks.Timer; hook != nil {
			controlled = hook(c.engine.pipelineDelay, func() { c.expirePipeline(generation, false) })
		}
		if !controlled {
			p.timerGeneration = generation
			p.timerPending = true
			if p.timer == nil {
				p.timer = time.AfterFunc(c.engine.pipelineDelay, c.pipelineTimerExpired)
			} else {
				p.timer.Reset(c.engine.pipelineDelay)
			}
		}
	}
	return n, err
}

func (c *Conn) pipelineTimerExpired() {
	p := &c.pipeline
	p.mu.Lock()
	generation := p.timerGeneration
	p.mu.Unlock()
	c.expirePipeline(generation, true)
}

func (c *Conn) expirePipeline(generation uint64, runtimeTimer bool) {
	if hook := c.pipelineHooks.AfterExpiry; hook != nil {
		defer hook()
	}
	if hook := c.pipelineHooks.BeforeExpiry; hook != nil {
		hook()
	}
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if runtimeTimer {
		p.timerPending = false
	}
	// A Stop that loses to expiry leaves timerPending set until this invocation
	// takes the lock. Intervening rounds stay uncorked, so Reset cannot let an
	// old invocation adopt a newer epoch, even if it only starts after Stop.
	if !p.armed || !p.holding || p.terminal || p.generation != generation {
		return
	}
	if err := c.pushPipelineLocked(true); err != nil {
		c.raw.Close()
	}
}

func (c *Conn) pushPipelineLocked(expired bool) error {
	p := &c.pipeline
	p.holding, p.armed = false, false
	p.generation++
	if p.timer != nil && p.timer.Stop() {
		p.timerPending = false
	}
	err := c.raw.PushCork(false)
	if hook := c.pipelineHooks.Pushed; hook != nil {
		hook(expired, err)
	}
	return err
}

// PushPipeline releases kernel-held output before an explicit flush or wait.
// The rest of the current read round remains uncorked.
func (c *Conn) PushPipeline() error {
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.holding {
		return nil
	}
	return c.pushPipelineLocked(false)
}

// finishPipelineResponse does not push. Non-negative delays coalesce in the
// kernel and release at cap expiry or round end. A negative delay never
// enables the cork; each response already leaves on the TCP_NODELAY write.
// Pushing here would issue TCP_CORK/TCP_NOPUSH syscalls the pre-branch
// server never made.
func (c *Conn) finishPipelineResponse() error {
	return nil
}

// endPipelineLifetime invalidates callbacks before descriptor close/detach.
func (c *Conn) endPipelineLifetime() error {
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	p.terminal = true
	if !p.holding {
		return nil
	}
	return c.pushPipelineLocked(false)
}

func (c *Conn) endPipelineRound() {
	if err := c.PushPipeline(); err != nil {
		c.Close()
	}
}
