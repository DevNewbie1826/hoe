package http1

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"
)

type responseChunks struct{ w *Response }

func (c responseChunks) Write(p []byte) (int, error) {
	w := c.w
	w.sendHead(p)
	if w.req.Method == "HEAD" {
		return len(p), w.err
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.chunked {
		var scratch [18]byte
		frame := strconv.AppendInt(scratch[:0], int64(len(p)), 16)
		frame = append(frame, '\r', '\n')
		if _, err := w.out.Write(frame); err != nil {
			w.record(err)
			return 0, err
		}
	}
	n, err := w.out.Write(p)
	if err == nil && w.chunked {
		_, err = w.out.WriteString("\r\n")
	}
	w.record(err)
	return n, err
}

func (w *Response) writeContinue() error {
	w.continueMu.Lock()
	defer w.continueMu.Unlock()
	if !w.continuePending {
		return nil
	}
	w.continuePending = false
	if err := w.pushPipeline(); err != nil {
		return err
	}
	if _, err := w.out.WriteString("HTTP/1.1 100 Continue\r\n\r\n"); err != nil {
		return err
	}
	if err := w.out.Flush(); err != nil {
		return err
	}
	return w.pushPipeline()
}

func (w *Response) disableContinue(skip bool) {
	w.continueMu.Lock()
	defer w.continueMu.Unlock()
	if w.continuePending {
		w.continuePending = false
		if skip && closeUnsentContinue {
			w.skipDrain, w.closeAfter = true, true
			w.body.expectClosed.Store(true)
		}
	}
}

// Flush sends pending body bytes and commits headers without a final length.
func (w *Response) Flush() { w.record(w.FlushError()) }

// FlushError implements ResponseController flushing and returns write errors.
func (w *Response) FlushError() error {
	if w.hijacked {
		return http.ErrHijacked
	}
	w.record(w.pushPipeline())
	if w.status == 0 {
		w.WriteHeader(200)
	}
	w.record(w.buffer.Flush())
	w.sendHead(nil)
	w.record(w.out.Flush())
	w.record(w.pushPipeline())
	return w.err
}

func (w *Response) pushPipeline() error {
	if c, ok := w.transport.(interface{ PushPipeline() error }); ok {
		return c.PushPipeline()
	}
	return nil
}

// EnableFullDuplex suppresses the pre-response request-body drain.
func (w *Response) EnableFullDuplex() error { w.duplex = true; return nil }

// SetReadDeadline delegates to the transport, or returns ErrNotSupported.
func (w *Response) SetReadDeadline(t time.Time) error {
	if c, ok := w.transport.(interface{ SetReadDeadline(time.Time) error }); ok {
		return c.SetReadDeadline(t)
	}
	return http.ErrNotSupported
}

// SetWriteDeadline delegates to the transport, or returns ErrNotSupported.
func (w *Response) SetWriteDeadline(t time.Time) error {
	if c, ok := w.transport.(interface{ SetWriteDeadline(time.Time) error }); ok {
		return c.SetWriteDeadline(t)
	}
	return http.ErrNotSupported
}

// CloseNotify delegates disconnect notification to the connection owner.
func (w *Response) CloseNotify() <-chan bool {
	if c, ok := w.transport.(interface{ CloseNotify() <-chan bool }); ok {
		return c.CloseNotify()
	}
	return nil
}

// Hijack first flushes already-committed output. Event-loop transports implement
// Hijack(*bufio.Reader) (net.Conn, *bufio.ReadWriter, error) to deregister and
// transfer a real runtime net.Conn with all buffered input preserved. A plain
// net.Conn needs no handoff and is returned directly.
func (w *Response) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if w.hijacked {
		return nil, nil, http.ErrHijacked
	}
	w.disableContinue(false)
	if w.status != 0 {
		// std flushes the chunk writer, not its 2048-byte handler buffer.
		// Buffered body bytes are abandoned when ownership is hijacked.
		w.sendHead(nil)
		if err := w.out.Flush(); err != nil {
			return nil, nil, err
		}
	}
	if c, ok := w.transport.(interface {
		Hijack(*bufio.Reader) (net.Conn, *bufio.ReadWriter, error)
	}); ok {
		conn, rw, err := c.Hijack(w.in)
		if err == nil {
			w.hijacked = true
			w.releaseBuffer()
		}
		return conn, rw, err
	}
	if c, ok := w.transport.(net.Conn); ok {
		if err := c.SetDeadline(time.Time{}); err != nil {
			return nil, nil, err
		}
		w.hijacked = true
		w.releaseBuffer()
		return c, bufio.NewReadWriter(w.in, w.out), nil
	}
	return nil, nil, http.ErrNotSupported
}

// CheckExpect rejects unsupported expectations before invoking a handler.
// False means a 417 response is committed; the owner must call Finish and close.
func (w *Response) CheckExpect() bool {
	if w.req.Header.Get("Expect") == "" || hasToken(w.req.Header.Values("Expect"), "100-continue") {
		return true
	}
	w.Header().Set("Connection", "close")
	w.closeAfter, w.skipDrain = true, true
	w.WriteHeader(http.StatusExpectationFailed)
	return false
}

type writerOnly struct{ io.Writer }

// ReadFrom uses the transport's io.ReaderFrom hook after sniffing and header
// flush, only for unchunked permitted bodies. File sources below 16KiB use the
// bounded copy path. The transport owns sendfile/splice and blocking flow control.
func (w *Response) ReadFrom(src io.Reader) (n int64, err error) {
	rf, ok := w.transport.(io.ReaderFrom)
	eligible := ok
	raw := src
	if lr, ok := raw.(*io.LimitedReader); ok {
		raw = lr.R
	}
	file := false
	if _, ok := raw.(*os.File); ok {
		file = true
	} else if typ := reflect.TypeOf(raw); typ != nil {
		file = typ.PkgPath() == "os" && typ.Name() == "fileWithoutWriteTo"
	}
	if file {
		f := raw.(interface {
			Stat() (os.FileInfo, error)
			Seek(int64, int) (int64, error)
		})
		info, statErr := f.Stat()
		if statErr != nil {
			return 0, statErr
		}
		offset, seekErr := f.Seek(0, io.SeekCurrent)
		if seekErr != nil {
			eligible = false
		} else {
			size := info.Size() - offset
			if lr, ok := src.(*io.LimitedReader); ok {
				size = min(size, lr.N)
			}
			eligible = eligible && info.Mode().IsRegular() && size >= 16<<10
		}
	}
	if !eligible {
		return io.Copy(writerOnly{w}, src)
	}
	if !w.wire {
		n, err = io.Copy(writerOnly{w}, io.LimitReader(src, 512))
		if err != nil || n < 512 {
			return n, err
		}
	}
	if err = w.FlushError(); err != nil {
		return n, err
	}
	if w.chunked || !bodyAllowed(w.status) || w.req.Method == "HEAD" {
		n0, e := io.Copy(writerOnly{w}, src)
		return n + n0, e
	}
	original := src
	var restoreLimit func()
	if w.contentLength >= 0 {
		left := max(int64(0), w.contentLength-w.written)
		if lr, ok := src.(*io.LimitedReader); ok {
			diff := max(int64(0), lr.N-left)
			lr.N -= diff
			restoreLimit = func() { lr.N += diff }
		} else {
			src = io.LimitReader(src, left)
		}
	}
	n0, err := rf.ReadFrom(src)
	w.written += n0
	n += n0
	w.record(err)
	if restoreLimit != nil {
		restoreLimit()
	}
	if err == nil && w.contentLength >= 0 && w.written == w.contentLength {
		var extra [1]byte
		if count, _ := original.Read(extra[:]); count > 0 {
			err = http.ErrContentLength
			w.closeAfter = true
		}
	}
	return n, err
}
