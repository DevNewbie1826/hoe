package http1

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// requestContext lives in the immutable request allocation. Native cancellation
// is created only when its channels, values, or callbacks are observed.
type requestContext struct {
	parent   context.Context
	mu       sync.Mutex
	native   context.Context
	cancel   context.CancelFunc
	err      error
	manual   bool
	response *Response
	head     string
}

func (c *requestContext) ensure() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native == nil {
		parent := c.parent
		if c.manual {
			// A later parent cancellation must not overwrite an earlier
			// request cancellation's Cause. Values still come from parent.
			parent = context.WithoutCancel(parent)
		}
		c.native, c.cancel = context.WithCancel(parent)
		if c.err != nil {
			c.cancel()
		}
	}
	return c.native
}

func (c *requestContext) Deadline() (time.Time, bool) { return c.parent.Deadline() }
func (c *requestContext) Done() <-chan struct{}       { return c.ensure().Done() }
func (c *requestContext) Value(key any) any           { return c.ensure().Value(key) }
func (c *requestContext) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.native != nil {
		return c.native.Err()
	}
	if c.err != nil {
		return c.err
	}
	return c.parent.Err()
}

// Cancel preserves native first-cancellation semantics without allocating
// when the handler never observes request-context cancellation.
func (c *requestContext) Cancel() {
	c.mu.Lock()
	if c.err == nil {
		c.err = c.parent.Err()
		if c.err == nil {
			c.err, c.manual = context.Canceled, true
		}
	}
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// AfterFunc attaches a callback through native cancellation without a watcher.
func (c *requestContext) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.ensure(), f)
}

func (b *requestBlock) attachContext() {
	b.context.parent = context.Background()
	b.context.response = &b.response
	b.request = *b.request.WithContext(&b.context)
}

// BindContext installs the connection's context chain before a request is
// published to the handler. Each request is bound exactly once; retained
// requests and their contexts are never reused.
func BindContext(r *http.Request, parent context.Context) interface{ Cancel() } {
	block := r.Context().(*requestContext)
	block.parent = parent
	return block
}
