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

func wouldBlock(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}
func interrupted(err error) bool { return errors.Is(err, syscall.EINTR) }

func completeHead(p []byte) bool {
	return bytes.Contains(p, []byte("\r\n\r\n")) || bytes.Contains(p, []byte("\n\n"))
}

func (c *Conn) round() {
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
		ready, err := c.readHead()
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
		if !c.serveRequest() {
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

func (c *Conn) serveRequest() bool {
	buffers := requestIOPool.Get().(*requestIO)
	in, out := buffers.in, buffers.out
	in.Reset(c)
	out.Reset(c)
	defer func() {
		in.Reset(nil)
		out.Reset(io.Discard)
		requestIOPool.Put(buffers)
	}()
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
	if n := in.Buffered(); n > 0 {
		p, _ := in.Peek(n)
		c.readMu.Lock()
		b := bufpool.Get(n + len(c.input) - c.offset)
		copy(b, p)
		copy(b[n:], c.input[c.offset:])
		c.releaseInputLocked()
		c.input = b
		c.readMu.Unlock()
	}
	if r.Method == "POST" {
		c.postPadding = 4
	}
	c.startHead(true)
	c.setState(http.StateIdle)
	return true
}

func (e *Engine) serverLog(format string, args ...any) {
	// Standard panic reporting uses the process logger when ErrorLog is nil.
	fmt.Fprintf(logWriter{}, format+"\n", args...)
}
