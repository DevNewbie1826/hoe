package http1

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httputil"
	"net/textproto"
	"sync"
	"sync/atomic"
)

// MaxDrainBytes is the standard server's unread request body drain budget.
const MaxDrainBytes = 256 << 10

// Body provides a blocking streaming request body with bounded Close draining.
// It owns framing state, not transport buffers. Close and Read are serialized
// like the standard body; connection cancellation remains the caller's job.
type Body struct {
	mu           sync.Mutex
	in           *bufio.Reader
	source       io.Reader
	req          *http.Request
	remaining    int64
	chunked      bool
	closing      bool
	closed       bool
	sawEOF       bool
	earlyClose   bool
	beforeRead   func() error
	expectClosed atomic.Bool
	expectEOF    atomic.Bool
}

func attachBody(r *http.Request, in *bufio.Reader, b *Body) {
	if r.Body == http.NoBody || r.ContentLength == 0 && len(r.TransferEncoding) == 0 {
		r.Body = http.NoBody
		return
	}
	b.in, b.req, b.remaining, b.closing = in, r, r.ContentLength, r.Close
	b.chunked = len(r.TransferEncoding) > 0
	if b.chunked {
		b.source = httputil.NewChunkedReader(in)
	}
	r.Body = b
}

// Read consumes body bytes, never pipeline bytes, and validates chunk trailers.
func (b *Body) Read(p []byte) (int, error) {
	if b.expectClosed.Load() {
		return 0, http.ErrBodyReadAfterClose
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0, http.ErrBodyReadAfterClose
	}
	if b.beforeRead != nil {
		if err := b.beforeRead(); err != nil {
			return 0, err
		}
	}
	n, err := b.readLocked(p)
	if b.beforeRead != nil && errors.Is(err, io.EOF) {
		b.expectEOF.Store(true)
	}
	return n, err
}

func (b *Body) readLocked(p []byte) (n int, err error) {
	if b.sawEOF {
		return 0, io.EOF
	}
	if !b.chunked {
		if b.remaining == 0 {
			b.sawEOF = true
			return 0, io.EOF
		}
		if int64(len(p)) > b.remaining {
			p = p[:b.remaining]
		}
		n, err = b.in.Read(p)
		b.remaining -= int64(n)
		if errors.Is(err, io.EOF) {
			b.sawEOF = true
			if b.remaining > 0 {
				err = io.ErrUnexpectedEOF
			}
		}
		if err == nil && b.remaining == 0 {
			b.sawEOF, err = true, io.EOF
		}
		return n, err
	}
	n, err = b.source.Read(p)
	if errors.Is(err, io.EOF) {
		if err = b.readTrailer(); err != nil {
			b.closed = true
			return n, err
		}
		b.sawEOF = true
		return n, io.EOF
	}
	return n, err
}

func (b *Body) readTrailer() error {
	p, err := b.in.Peek(2)
	if bytes.Equal(p, []byte("\r\n")) {
		_, err = b.in.Discard(2)
		return err
	}
	if len(p) < 2 {
		return errors.New("http: unexpected EOF reading trailer")
	}
	if err != nil {
		return err
	}
	// std limits trailers to the reader's capacity, not an unbounded
	// textproto allocation. Peek does not consume the next request.
	found := false
	for n := 4; n <= b.in.Size(); n++ {
		p, err = b.in.Peek(n)
		if bytes.HasSuffix(p, []byte("\r\n\r\n")) {
			found = true
			break
		}
		if err != nil {
			break
		}
	}
	if !found {
		return errors.New("http: suspiciously long trailer after chunked body")
	}
	h, err := textproto.NewReader(b.in).ReadMIMEHeader()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("http: unexpected EOF reading trailer")
		}
		return err
	}
	if b.req.Trailer == nil {
		b.req.Trailer = http.Header(h)
	} else {
		for k, v := range h {
			b.req.Trailer[k] = v
		}
	}
	return nil
}

// Close discards at most MaxDrainBytes+1 unread bytes. EarlyClosed tells the
// connection owner whether framing prevented safe keep-alive reuse.
func (b *Body) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if b.sawEOF || b.closing && !b.chunked {
		return nil
	}
	if !b.chunked && b.remaining > MaxDrainBytes {
		b.earlyClose = true
		return nil
	}
	var scratch [8192]byte
	var consumed int64
	for consumed <= MaxDrainBytes {
		want := min(int64(len(scratch)), MaxDrainBytes+1-consumed)
		n, err := b.readLocked(scratch[:want])
		consumed += int64(n)
		if errors.Is(err, io.EOF) {
			// io.CopyN's standard drain treats a full budget+1 copy as
			// early close even if its last Read also reported terminal EOF.
			b.earlyClose = consumed > MaxDrainBytes
			return nil
		}
		if err != nil {
			b.earlyClose = true
			return err
		}
	}
	b.earlyClose = true
	return nil
}

// Remaining returns unread length, or -1 for chunked bodies.
func (b *Body) Remaining() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remaining
}

// EOF reports whether body framing and trailers have been consumed.
func (b *Body) EOF() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sawEOF
}

// EarlyClosed reports that Close did not consume the complete body.
func (b *Body) EarlyClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.earlyClose || b.closed && !b.sawEOF && !b.closing
}
