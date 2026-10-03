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
	in  *bufio.Reader
	out *bufio.Writer
}

var requestIOPool = sync.Pool{New: func() any {
	return &requestIO{bufio.NewReaderSize(nil, 4096), bufio.NewWriterSize(io.Discard, 4096)}
}}

var corkWriterPool = sync.Pool{New: func() any {
	return bufio.NewWriterSize(io.Discard, 64<<10)
}}

const maxPendingOutput = 64 << 20

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}
func interrupted(err error) bool { return errors.Is(err, syscall.EINTR) }

func completeHead(p []byte) bool {
	return bytes.Contains(p, []byte("\r\n\r\n")) || bytes.Contains(p, []byte("\n\n"))
}

func (c *Conn) round() {
	var buffers *requestIO
	var in *bufio.Reader
	var out *bufio.Writer
	corked := false
	defer func() {
		if buffers != nil {
			in.Reset(nil)
			out.Reset(io.Discard)
			if corked {
				corkWriterPool.Put(out)
				c.engine.pendingOutput.Add(-(64 << 10))
			}
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
		if buffers == nil {
			buffers = requestIOPool.Get().(*requestIO)
			in, out = buffers.in, buffers.out
			// Reserve cork capacity, not only bytes already pending. When
			// the server budget is full, retain std's 4KiB blocking path.
			corked = c.engine.pendingOutput.Add(64<<10) <= maxPendingOutput
			if corked {
				out = corkWriterPool.Get().(*bufio.Writer)
			} else {
				c.engine.pendingOutput.Add(-(64 << 10))
			}
			in.Reset(c)
			out.Reset(c)
		}
		if !c.serveRequest(in, out) {
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

func (c *Conn) serveRequest(in *bufio.Reader, out *bufio.Writer) bool {
	r, err := http1.ReadRequest(in, c.engine.server.MaxHeaderBytes)
	if err != nil {
		http1.WriteError(c, err)
		c.Close()
		return false
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
		cancel()
	}
	readDeadline := time.Time{}
	if c.engine.server.ReadTimeout > 0 {
		readDeadline = c.headStart.Add(c.engine.server.ReadTimeout)
	}
	c.SetReadDeadline(readDeadline)
	c.SetWriteDeadline(after(c.engine.server.WriteTimeout))
	w := http1.NewResponse(r, in, out, c)
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
				io.CopyN(io.Discard, r.Body, 4096)
			}
		} else {
			h.ServeHTTP(w, r)
		}
	}
	cancel()
	c.mu.Lock()
	c.cancel = nil
	c.mu.Unlock()
	if w.Hijacked() {
		return false
	}
	err = w.Finish()
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
