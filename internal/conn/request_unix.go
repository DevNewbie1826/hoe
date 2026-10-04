//go:build linux || darwin

package conn

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/DevNewbie1826/hoe/internal/http1"
)

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
		cancel.Cancel()
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
