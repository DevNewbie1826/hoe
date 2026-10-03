package http1

import (
	"context"
	"net/http"
)

// requestContext lives in the immutable request allocation, not a connection
// pool. Native cancellation retains Cause semantics and direct child attachment.
type requestContext struct{ context.Context }

// AfterFunc attaches a callback through native cancellation without a watcher.
func (c *requestContext) AfterFunc(f func()) func() bool {
	return context.AfterFunc(c.Context, f)
}

func (b *requestBlock) attachContext() {
	b.context.Context = context.Background()
	b.request = *b.request.WithContext(&b.context)
}

// BindContext installs the connection's context chain before a request is
// published to the handler. Each request is bound exactly once; retained
// requests and their contexts are never reused.
func BindContext(r *http.Request, parent context.Context) context.CancelFunc {
	ctx, cancel := context.WithCancel(parent)
	if block, ok := r.Context().(*requestContext); ok {
		block.Context = ctx
	} else {
		*r = *r.WithContext(ctx)
	}
	return cancel
}
