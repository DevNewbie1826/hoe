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

type requestIO struct {
	in *bufio.Reader
}

var requestIOPool = sync.Pool{New: func() any {
	return &requestIO{in: bufio.NewReaderSize(nil, 4096)}
}}

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

func (c *Conn) round() {
	var buffers *requestIO
	var in *bufio.Reader
	output := &roundOutput{Conn: c}
	defer func() {
		if buffers != nil {
			in.Reset(nil)
			output.ReleaseResponseOutput()
			requestIOPool.Put(buffers)
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
				c.readMu.Lock()
				b := bufpool.Get(max(4096, len(p)+len(c.input)-c.offset))
				n := copy(b, p)
				n += copy(b[n:], c.input[c.offset:])
				c.releaseInputLocked()
				c.input = b[:n]
				c.readMu.Unlock()
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
			buffers = requestIOPool.Get().(*requestIO)
			in = buffers.in
			in.Reset(c)
		}
		if !c.serveRequest(in, output, first) {
			return
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

func (c *Conn) serveRequest(in *bufio.Reader, output *roundOutput, first bool) bool {
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
