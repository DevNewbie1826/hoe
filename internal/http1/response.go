package http1

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"sync"
)

// Response serializes one HTTP/1 response. A handler uses its standard
// ResponseWriter interfaces, then the connection owner calls Finish and
// consults CloseAfterReply/Hijacked before scheduling another request.
// Reader and writer buffers belong to the caller and are never pooled here.
type Response struct {
	req              *http.Request
	body             *Body
	in               *bufio.Reader
	out              *bufio.Writer
	transport        io.Writer
	header           http.Header
	snapshot         http.Header
	calledHeader     bool
	status           int
	written          int64
	contentLength    int64
	wire             bool
	chunked          bool
	done             bool
	hijacked         bool
	closeAfter       bool
	wantsClose       bool
	wants10KeepAlive bool
	disableKeepAlive bool
	duplex           bool
	limitHit         bool
	skipDrain        bool
	trailers         []string
	err              error
	buffer           *bufio.Writer
	bufferErr        error
	chunks           responseChunks
	continueMu       sync.Mutex
	continuePending  bool
	expect           bool
}

var responseBufferPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 2048)
}}

// headerSnapshotPool recycles the frozen copy of a handler's response headers.
// A snapshot is internal to the codec: it is written once and never handed back
// to a handler, so it can return to the pool when the response is released.
var headerSnapshotPool = sync.Pool{New: func() any {
	return make(http.Header, 8)
}}

func (w *Response) freezeHeaders() {
	s := w.snapshot
	if s == nil {
		s = headerSnapshotPool.Get().(http.Header)
		w.snapshot = s
	}
	for k := range s {
		if _, keep := w.header[k]; !keep {
			delete(s, k)
		}
	}
	for k, vv := range w.header {
		dst := s[k]
		if cap(dst) < len(vv) {
			dst = make([]string, len(vv))
		}
		dst = dst[:len(vv)]
		copy(dst, vv)
		s[k] = dst
	}
}

func (w *Response) releaseSnapshot() {
	if w.snapshot == nil {
		return
	}
	// A large snapshot would pin its header bytes in the pool between
	// requests; give those back to the collector instead of retaining them.
	bytes := 0
	for k, vv := range w.snapshot {
		bytes += len(k)
		for _, v := range vv {
			bytes += len(v)
		}
	}
	if bytes <= 4096 && len(w.snapshot) <= 32 {
		headerSnapshotPool.Put(w.snapshot)
	}
	w.snapshot = nil
}

type responseOutputOwner interface {
	AcquireResponseOutput() *bufio.Writer
	GrowResponseOutput() *bufio.Writer
	ReleaseResponseOutput()
	SetResponseCleanup(func())
}

// NewResponse binds a request to blocking, bounded transport I/O. transport
// may implement io.ReaderFrom, deadline methods, CloseNotify, and the Hijack
// hook described on Response.Hijack. No goroutine is started by the codec.
func NewResponse(r *http.Request, in *bufio.Reader, out *bufio.Writer, transport io.Writer) *Response {
	var w *Response
	if block, ok := r.Context().(*requestContext); ok {
		w = block.response
	} else {
		w = new(Response)
	}
	*w = Response{
		req: r, in: in, out: out, transport: transport,
		contentLength: -1, wantsClose: r.Close,
		wants10KeepAlive: r.ProtoMajor == 1 && r.ProtoMinor == 0 && hasToken(r.Header.Values("Connection"), "keep-alive"),
	}
	w.body, _ = r.Body.(*Body)
	if w.body != nil {
		// Callers commonly use WithContext before handing the request to a
		// handler. Undeclared trailers must update that request, not its copy.
		w.body.req = r
	}
	w.chunks.w = w
	w.buffer = responseBufferPool.Get().(*bufio.Writer)
	w.buffer.Reset(&w.chunks)
	if owner, ok := transport.(responseOutputOwner); ok {
		owner.SetResponseCleanup(w.releaseBuffer)
	}
	if w.body != nil && hasToken(r.Header.Values("Expect"), "100-continue") && r.ProtoAtLeast(1, 1) {
		w.expect, w.continuePending = true, true
		w.body.beforeRead = w.writeContinue
	}
	return w
}

// Header returns the handler's mutable response headers.
func (w *Response) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	if w.status != 0 && !w.wire && w.snapshot == nil {
		w.freezeHeaders()
	}
	w.calledHeader = true
	return w.header
}

// WriteHeader sends informational responses immediately, or commits the final
// status and header snapshot. Invalid status codes panic exactly like net/http.
func (w *Response) WriteHeader(code int) {
	if w.hijacked || w.status != 0 {
		return
	}
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code))
	}
	w.detectLimit()
	if code == 100 || code >= 200 {
		w.disableContinue(code >= 200)
	}
	if code < 200 && code != 101 {
		w.record(w.pushPipeline())
		w.acquireOutput()
		writeStatus(w.out, w.req.ProtoAtLeast(1, 1), code)
		w.record(w.header.WriteSubset(w.out, map[string]bool{"Content-Length": true, "Transfer-Encoding": true}))
		_, err := w.out.WriteString("\r\n")
		w.record(err)
		w.record(w.out.Flush())
		w.record(w.pushPipeline())
		return
	}
	w.status = code
	if w.calledHeader {
		w.freezeHeaders()
	}
	if cl := w.header.Get("Content-Length"); cl != "" {
		n, err := strconv.ParseInt(cl, 10, 64)
		if err == nil && n >= 0 {
			w.contentLength = n
		} else {
			w.header.Del("Content-Length")
		}
	}
}

// Write buffers up to 2048 bytes before deciding content length or chunking.
func (w *Response) Write(p []byte) (int, error) {
	if err := w.prepareWrite(len(p)); err != nil {
		return 0, err
	}
	if w.bufferErr != nil {
		return 0, w.bufferErr
	}
	return w.acquireBuffer().Write(p)
}

// WriteString implements io.StringWriter with the same framing as Write.
func (w *Response) WriteString(s string) (int, error) {
	if err := w.prepareWrite(len(s)); err != nil {
		return 0, err
	}
	if w.bufferErr != nil {
		return 0, w.bufferErr
	}
	return w.acquireBuffer().WriteString(s)
}

func (w *Response) acquireBuffer() *bufio.Writer {
	if w.buffer == nil {
		w.buffer = responseBufferPool.Get().(*bufio.Writer)
		w.buffer.Reset(&w.chunks)
	}
	return w.buffer
}

func (w *Response) acquireOutput() {
	if w.out == nil {
		w.out = w.transport.(responseOutputOwner).AcquireResponseOutput()
	}
}

func (w *Response) prepareWrite(n int) error {
	if w.hijacked {
		return http.ErrHijacked
	}
	w.disableContinue(true)
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if n == 0 {
		return nil
	}
	if !bodyAllowed(w.status) {
		return http.ErrBodyNotAllowed
	}
	w.written += int64(n)
	if w.contentLength >= 0 && w.written > w.contentLength {
		return http.ErrContentLength
	}
	return nil
}

// Finish commits buffered bytes, final chunk/trailers, and bounded body drain.
// It must run once after the handler returns, not after a successful Hijack.
func (w *Response) Finish() error {
	if w.hijacked {
		return http.ErrHijacked
	}
	if w.done {
		return w.err
	}
	defer w.releaseBuffer()
	w.detectLimit()
	w.done = true
	if w.status == 0 {
		w.WriteHeader(200)
	}
	w.acquireOutput()
	if w.buffer != nil {
		w.record(w.buffer.Flush())
	}
	if !w.wire {
		w.sendHead(nil)
	}
	if w.chunked {
		_, err := w.out.WriteString("0\r\n")
		w.record(err)
		w.record(w.finalTrailers().Write(w.out))
		_, err = w.out.WriteString("\r\n")
		w.record(err)
	}
	w.record(w.out.Flush())
	if !w.limitHit && !w.skipDrain && w.body != nil {
		if err := w.body.Close(); err != nil {
			w.closeAfter = true
		}
		w.closeAfter = w.closeAfter || w.body.EarlyClosed()
	}
	if w.req.Method != "HEAD" && bodyAllowed(w.status) && w.contentLength >= 0 && w.written != w.contentLength {
		w.closeAfter = true
	}
	if w.req.MultipartForm != nil {
		w.record(w.req.MultipartForm.RemoveAll())
	}
	return w.err
}

func (w *Response) releaseBuffer() {
	if w.buffer != nil {
		w.buffer.Reset(io.Discard)
		responseBufferPool.Put(w.buffer)
		w.buffer = nil
	}
	w.releaseSnapshot()
}

// CloseAfterReply reports whether the owner must stop HTTP reuse.
func (w *Response) CloseAfterReply() bool { return w.closeAfter || w.err != nil }

// Hijacked reports that transport ownership has moved to the handler.
func (w *Response) Hijacked() bool { return w.hijacked }

// DisableKeepAlives requests standard Connection: close response semantics.
func (w *Response) DisableKeepAlives() { w.disableKeepAlive = true }

// LimitHit reports assigned MaxBytesReader overflow (G1) or an unread body
// exceeding the standard pre-response drain budget.
func (w *Response) LimitHit() bool { return w.limitHit }

func (w *Response) record(err error) {
	if w.err == nil {
		w.err = err
	}
}

func (w *Response) detectLimit() {
	switch w.req.Body.(type) {
	case *Body:
		return
	}
	v := reflect.ValueOf(w.req.Body)
	if !v.IsValid() || v.Type().String() != "*http.maxBytesReader" || v.IsNil() {
		return
	}
	f := v.Elem().FieldByName("err")
	if !f.IsValid() || f.IsNil() || f.Elem().Type() != reflect.TypeFor[*http.MaxBytesError]() {
		return
	}
	w.limitHit, w.closeAfter = true, true
	if w.status == 0 {
		w.Header().Set("Connection", "close")
	}
}

func bodyAllowed(status int) bool {
	return status >= 200 && status != 204 && status != 304
}
