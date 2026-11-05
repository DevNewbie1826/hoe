//go:build linux || darwin

package conn

import "sync"

const holdBufferSize = 64 << 10

var holdBufferPool = sync.Pool{New: func() any {
	b := make([]byte, 0, holdBufferSize)
	return &b
}}

// roundHold batches one corked round's bytes below the codec's writer, so the
// codec's per-response flush fills memory and one kernel write carries the
// round. The cap timer, round end, close, and hijack drain it, which bounds
// per-response delivery. Lock order: hold, then the connection write lock,
// then pipeline. Nothing takes the hold while holding either of those.
type roundHold struct {
	c   *Conn
	mu  sync.Mutex
	buf *[]byte
}

func (h *roundHold) start(c *Conn) {
	h.c = c
	h.buf = holdBufferPool.Get().(*[]byte)
	*h.buf = (*h.buf)[:0]
}

func (h *roundHold) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		return len(p), nil
	}
	if len(p) > 0 && len(*h.buf) == 0 {
		h.c.armPipeline()
	}
	*h.buf = append(*h.buf, p...)
	// Once the cap has released the round, later responses belong on the wire
	// immediately, exactly as an uncorked round delivers them.
	if len(*h.buf) >= holdBufferSize || !h.c.pipelineHolding() {
		return len(p), h.flushLocked()
	}
	return len(p), nil
}

// flushBestEffort drains held bytes without blocking. The close path cannot
// wait on a peer that is not reading: a stalled close would starve the
// reactor that accepts new connections.
func (h *roundHold) flushBestEffort() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil || len(*h.buf) == 0 {
		return
	}
	h.c.writeHeldNow(*h.buf)
	*h.buf = (*h.buf)[:0]
}

// Flush writes held bytes to the kernel in arrival order.
func (h *roundHold) Flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.flushLocked()
}

func (h *roundHold) flushLocked() error {
	if h.buf == nil || len(*h.buf) == 0 {
		return nil
	}
	_, err := h.c.Write(*h.buf)
	*h.buf = (*h.buf)[:0]
	return err
}

func (h *roundHold) release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.buf == nil {
		return
	}
	*h.buf = (*h.buf)[:0]
	holdBufferPool.Put(h.buf)
	h.buf = nil
}
