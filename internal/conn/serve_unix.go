//go:build linux || darwin

package conn

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	"github.com/DevNewbie1826/hoe/internal/http1"
)

const (
	// smallReaderSize starts every connection's read rounds small; the
	// reader grows to bigReaderSize only after a read fills the small
	// buffer, so parked rounds retain one small backing.
	smallReaderSize = 1024
	bigReaderSize   = 4096
)

type requestIO struct {
	in    *bufio.Reader
	small bool
}

var smallRequestIOPool = sync.Pool{New: func() any {
	return &requestIO{in: bufio.NewReaderSize(nil, smallReaderSize), small: true}
}}

var bigRequestIOPool = sync.Pool{New: func() any {
	return &requestIO{in: bufio.NewReaderSize(nil, bigReaderSize)}
}}

func acquireRequestIO(c *Conn) *requestIO {
	if c.readerDemand.Load() {
		return bigRequestIOPool.Get().(*requestIO)
	}
	return smallRequestIOPool.Get().(*requestIO)
}

func releaseRequestIO(b *requestIO) {
	if b.small {
		smallRequestIOPool.Put(b)
		return
	}
	bigRequestIOPool.Put(b)
}

// source wraps small readers so real demand grows the connection; big
// readers read the connection directly.
func (b *requestIO) source(c *Conn) io.Reader {
	if !b.small {
		return c
	}
	return &demandReader{c: c}
}

// demandReader flags the connection the first time a read delivers the
// small reader's full size: only then has a request read beyond the small
// retention. Promoted full-size readers read through it too, so a body
// moved off the small reader still marks demanding connections.
type demandReader struct {
	c *Conn
}

func (d *demandReader) Read(p []byte) (int, error) {
	n, err := d.c.Read(p)
	if n >= smallReaderSize {
		d.c.readerDemand.Store(true)
	}
	return n, err
}

var corkWriterPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 64<<10)
}}

var outputWriterPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 4096)
}}

const maxPendingOutput = 64 << 20

// roundOutput owns the writer loan across responses in one read round.
// Flush can return the loan while the handler and round remain active.
type roundOutput struct {
	*Conn
	out     *bufio.Writer
	corked  bool
	cleanup func()
}

func (r *roundOutput) acquireCork() {
	if r.out != nil {
		return
	}
	if r.engine.pendingOutput.Add(64<<10) <= maxPendingOutput {
		r.corked = true
		r.out = corkWriterPool.Get().(*bufio.Writer)
		r.out.Reset(r.Conn)
	} else {
		r.engine.pendingOutput.Add(-(64 << 10))
	}
}

func (r *roundOutput) AcquireResponseOutput() *bufio.Writer {
	if r.out == nil {
		r.out = outputWriterPool.Get().(*bufio.Writer)
		r.out.Reset(r.Conn)
	}
	return r.out
}

func (r *roundOutput) GrowResponseOutput() *bufio.Writer {
	// The current writer must be empty: bufio.Writer cannot transfer pending
	// bytes to a larger buffer without sending them prematurely.
	if r.out.Size() >= 64<<10 || r.out.Buffered() != 0 {
		return r.out
	}
	r.ReleaseResponseOutput()
	r.acquireCork()
	return r.AcquireResponseOutput()
}

func (r *roundOutput) ReleaseResponseOutput() {
	if r.out == nil {
		return
	}
	r.out.Reset(io.Discard)
	if r.corked {
		corkWriterPool.Put(r.out)
		r.engine.pendingOutput.Add(-(64 << 10))
		r.corked = false
	} else {
		outputWriterPool.Put(r.out)
	}
	r.out = nil
}

func (r *roundOutput) SetResponseCleanup(cleanup func()) {
	r.cleanup = cleanup
}

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}
func interrupted(err error) bool { return errors.Is(err, syscall.EINTR) }

func completeHead(p []byte) bool {
	// Match the codec's headEnd: an LF followed by an LF or CRLF.
	return bytes.Contains(p, []byte("\n\n")) || bytes.Contains(p, []byte("\n\r\n"))
}

// stashBuffered moves p ahead of any pending input under readMu so the
// next read serves p first.
func (c *Conn) stashBuffered(p []byte) {
	c.readMu.Lock()
	b := bufpool.Get(max(4096, len(p)+len(c.input)-c.offset))
	n := copy(b, p)
	n += copy(b[n:], c.input[c.offset:])
	c.releaseInputLocked()
	c.input = b[:n]
	c.readMu.Unlock()
}

// growRequestIO swaps the round's reader for a full-size one. Buffered
// bytes move ahead of pending input so parsing results stay identical.
func (c *Conn) growRequestIO(buffers *requestIO, source io.Reader) *requestIO {
	grown := bigRequestIOPool.Get().(*requestIO)
	if carry := buffers.in.Buffered(); carry > 0 {
		p, _ := buffers.in.Peek(carry)
		c.stashBuffered(p)
	}
	grown.in.Reset(source)
	buffers.in.Reset(nil)
	releaseRequestIO(buffers)
	return grown
}

func (c *Conn) round() {
	var buffers *requestIO
	var in *bufio.Reader
	output := &roundOutput{Conn: c}
	defer func() {
		if buffers != nil {
			buffers.in.Reset(nil)
			output.ReleaseResponseOutput()
			releaseRequestIO(buffers)
		}
	}()
	defer func() {
		if p := recover(); p != nil {
			if p != http.ErrAbortHandler {
				if l := c.engine.server.ErrorLog; l != nil {
					l.Printf("http: panic serving %s: %v\n%s", c.remote, p, debug.Stack())
				} else {
					c.engine.serverLog("http: panic serving %s: %v\n%s", c.remote, p, debug.Stack())
				}
			}
			c.cancelRequest()
			c.Close()
		}
	}()
	defer c.endPipelineRound()
	for {
		select {
		case <-c.raw.Done():
			return
		default:
		}
		ready := false
		if in != nil && in.Buffered() > 0 {
			p, _ := in.Peek(in.Buffered())
			ready = completeHead(p)
			if ready {
				c.startHead(false)
				c.setState(http.StateActive)
			} else {
				c.stashBuffered(p)
				in.Discard(len(p))
			}
		}
		var err error
		if !ready {
			ready, err = c.readHead()
		}
		if err != nil {
			if writeErr := http1.WriteError(c, err); writeErr != nil {
				c.cancelRequest()
			}
			if pushErr := c.PushPipeline(); pushErr != nil {
				c.Close()
				return
			}
			if c.raw.CloseWrite() == nil {
				// A FIN makes the error readable even if rejected bytes remain
				// in the socket. Wait for peer EOF, bounded like std's linger.
				c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				io.Copy(io.Discard, c)
			}
			c.Close()
			return
		}
		if !ready {
			return
		}
		first := buffers == nil
		if first {
			buffers = acquireRequestIO(c)
			in = buffers.in
			in.Reset(buffers.source(c))
		}
		served := c.serveRequest(&buffers, output, first)
		in = buffers.in
		if !served {
			return
		}
		if buffers.small && c.readerDemand.Load() {
			// A read filled the small buffer: this connection needs the
			// full-size reader.
			buffers = c.growRequestIO(buffers, c)
			in = buffers.in
		}
	}
}

// readHead never waits for data. Partial heads keep a bounded loan, not a worker.
func (c *Conn) readHead() (bool, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	var tmp [4096]byte
	maxHead := c.engine.server.MaxHeaderBytes
	if maxHead <= 0 {
		maxHead = http.DefaultMaxHeaderBytes
	}
	for {
		t, _ := c.deadline("read")
		if !t.IsZero() && !time.Now().Before(t) {
			return false, c.timeoutError("read")
		}
		p := c.input[c.offset:]
		if len(p) > 0 && c.state.Load() != int32(http.StateActive) {
			if c.state.Load() == int32(http.StateIdle) {
				c.startHead(false)
			}
			c.setState(http.StateActive)
		}
		if c.postPadding > 0 && len(p) > 0 {
			for len(p) > 0 && c.postPadding > 0 && (p[0] == '\r' || p[0] == '\n') {
				c.offset++
				c.postPadding--
				p = p[1:]
			}
			if len(p) > 0 {
				c.postPadding = 0
			}
		}
		if completeHead(p) {
			return true, nil
		}
		if len(p) > maxHead+4096 {
			return false, &http1.HeaderError{Status: 431}
		}
		if i := bytes.IndexByte(p, '\n'); i >= 0 {
			// Invalid completed request lines must not wait for a blank line.
			_, err := http.ReadRequest(bufio.NewReader(io.MultiReader(bytes.NewReader(p[:i+1]), strings.NewReader("\r\n"))))
			if err != nil {
				return false, err
			}
		}
		n, err := c.raw.Read(tmp[:])
		if n > 0 {
			c.pending(tmp[:n])
			continue
		}
		if interrupted(err) {
			continue
		}
		if wouldBlock(err) {
			return false, nil
		}
		c.peerEOF()
		if err != nil {
			return false, err
		}
		if len(p) > 0 {
			// Let the codec reproduce std's response to truncated headers.
			return true, nil
		}
		return false, io.EOF
	}
}

// serveRequest publishes a promoted reader through bp before the handler
// runs, so a panic unwinds with round() owning exactly the live reader.
func (c *Conn) serveRequest(bp **requestIO, output *roundOutput, first bool) bool {
	in := (*bp).in
	r, err := http1.ReadRequest(in, c.engine.server.MaxHeaderBytes)
	if err != nil {
		http1.WriteError(c, err)
		c.Close()
		return false
	}
	// Only hold a round when another complete head is already buffered.
	// Skip a known-length body; chunked bodies have no known next-head offset.
	// Eligibility is checked once, so a release never re-corks this round.
	if first && r.ContentLength >= 0 && r.ContentLength < int64(in.Buffered()) {
		p, _ := in.Peek(in.Buffered())
		if completeHead(p[r.ContentLength:]) {
			if err := c.beginPipeline(); err != nil {
				c.Close()
				return false
			}
			output.acquireCork()
		}
	}
	if (*bp).small && len(r.TransferEncoding) > 0 {
		// Chunked framing is bounded by the reader's capacity: chunk
		// extension lines and trailers parse through ReadSlice and Peek on
		// this reader, so the small reader would reject framing the
		// pre-compaction full-size reader accepted. Promote before the
		// first body byte is read; bodyless and fixed-length requests keep
		// the small reader and its retention win.
		*bp = c.growRequestIO(*bp, &demandReader{c})
		in = (*bp).in
		http1.RebindBodyReader(r, in)
	}
	cancel := http1.BindContext(r, c.base)
	r.RemoteAddr = c.remoteName
	c.mu.Lock()
	c.cancel = cancel
	c.bodyDone = r.Body == http.NoBody
	c.closeNotify = nil
	c.serving = true
	eof := c.eof
	c.mu.Unlock()
	if r.Body != http.NoBody {
		http1.OnBodyReadDone(r, c.bodyReadDone)
	}
	if eof && r.Body == http.NoBody {
		cancel.Cancel()
	}
	readDeadline := time.Time{}
	if c.engine.server.ReadTimeout > 0 {
		readDeadline = c.headStart.Add(c.engine.server.ReadTimeout)
	}
	c.SetReadDeadline(readDeadline)
	c.SetWriteDeadline(after(c.engine.server.WriteTimeout))
	w := http1.NewResponse(r, in, output.AcquireResponseOutput(), output)
	defer func() {
		output.cleanup()
		output.cleanup = nil
	}()
	if !c.engine.keepAlive() {
		w.DisableKeepAlives()
	}
	if w.CheckExpect() {
		h := c.engine.server.Handler
		if h == nil {
			h = http.DefaultServeMux
		}
		if r.Method == "OPTIONS" && r.RequestURI == "*" && !c.engine.server.DisableGeneralOptionsHandler {
			w.Header().Set("Content-Length", "0")
			if r.Body != http.NoBody {
				r.Body = http.MaxBytesReader(w, r.Body, 4096)
				io.Copy(io.Discard, r.Body)
			}
		} else {
			h.ServeHTTP(w, r)
		}
	}
	cancel.Cancel()
	c.mu.Lock()
	c.cancel = nil
	c.mu.Unlock()
	if w.Hijacked() {
		return false
	}
	err = w.Finish()
	if err == nil {
		err = c.finishPipelineResponse()
	}
	if err != nil || w.CloseAfterReply() || !c.engine.keepAlive() {
		c.Close()
		return false
	}
	if r.Method == "POST" {
		c.postPadding = 4
		// The shared round reader may own leading padding/pipeline bytes.
		if in.Buffered() > 0 {
			p, _ := in.Peek(in.Buffered())
			n := 0
			for n < len(p) && c.postPadding > 0 && (p[n] == '\r' || p[n] == '\n') {
				n++
				c.postPadding--
			}
			in.Discard(n)
			if in.Buffered() > 0 {
				c.postPadding = 0
			}
		}
	}
	c.startHead(true)
	c.setState(http.StateIdle)
	return true
}

func (e *Engine) serverLog(format string, args ...any) {
	// Standard panic reporting uses the process logger when ErrorLog is nil.
	fmt.Fprintf(logWriter{}, format+"\n", args...)
}
