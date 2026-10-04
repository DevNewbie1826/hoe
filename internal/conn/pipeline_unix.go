//go:build linux || darwin

package conn

import (
	"sync"
	"time"
)

// pipelineHold serializes hold epochs without taking writeMu: a writer can
// wait for readiness while the cap callback must still push earlier bytes.
type pipelineHold struct {
	mu         sync.Mutex
	holding    bool
	terminal   bool
	armed      bool
	generation uint64
	timer      *time.Timer
}

func (c *Conn) beginPipeline() error {
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal || c.engine.pipelineDelay < 0 {
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
		expire := func() {
			if hook := c.pipelineHooks.AfterExpiry; hook != nil {
				defer hook()
			}
			if hook := c.pipelineHooks.BeforeExpiry; hook != nil {
				hook()
			}
			p.mu.Lock()
			defer p.mu.Unlock()
			// Stop cannot join an already-running AfterFunc. The epoch guard
			// makes that callback inert after release, close or a new round.
			if !p.armed || !p.holding || p.terminal || p.generation != generation {
				return
			}
			if err := c.pushPipelineLocked(true); err != nil {
				c.raw.Close()
			}
		}
		controlled := false
		if hook := c.pipelineHooks.Timer; hook != nil {
			controlled = hook(c.engine.pipelineDelay, expire)
		}
		if !controlled {
			p.timer = time.AfterFunc(c.engine.pipelineDelay, expire)
		}
	}
	return n, err
}

func (c *Conn) pushPipelineLocked(expired bool) error {
	p := &c.pipeline
	p.holding, p.armed = false, false
	p.generation++
	if p.timer != nil {
		p.timer.Stop()
		p.timer = nil
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

func (c *Conn) finishPipelineResponse() error {
	if c.engine.pipelineDelay >= 0 {
		return nil
	}
	p := &c.pipeline
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.terminal {
		return nil
	}
	return c.pushPipelineLocked(false)
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
