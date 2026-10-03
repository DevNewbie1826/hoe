// Package gate contains a deliberately limited HTTP/1 serving path for the
// MaxBytesReader feasibility proof. It is not used by hoe.Server.
package gate

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Server runs one goroutine per connection and counts reflect detections.
type Server struct {
	Handler    http.Handler
	Detections atomic.Int64
	mu         sync.Mutex
	listener   net.Listener
	conns      map[net.Conn]struct{}
	closed     bool
	wg         sync.WaitGroup
}

// Serve accepts connections until Close shuts down the listener.
func (s *Server) Serve(l net.Listener) error {
	s.mu.Lock()
	s.listener = l
	s.conns = make(map[net.Conn]struct{})
	s.mu.Unlock()
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return net.ErrClosed
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.wg.Done()
			defer func() { c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }()
			s.serveConn(c)
		}()
	}
}

// Close closes the listener and connections and joins all workers.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	if s.listener != nil {
		s.listener.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) serveConn(c net.Conn) {
	in, out := bufio.NewReader(c), bufio.NewWriter(c)
	for {
		r, err := http.ReadRequest(in)
		if err != nil {
			return
		}
		w := &writer{req: r, original: r.Body, out: out, header: make(http.Header), server: s}
		s.Handler.ServeHTTP(w, r)
		w.detect()
		if !w.sent {
			w.send(true)
		}
		if w.chunked {
			fmt.Fprint(out, "0\r\n\r\n")
		}
		if out.Flush() != nil {
			return
		}
		if w.close || r.Close {
			return
		}
		if _, err := io.Copy(io.Discard, w.original); err != nil {
			return
		}
		if err := w.original.Close(); err != nil {
			return
		}
	}
}

type writer struct {
	req                                    *http.Request
	original                               io.ReadCloser
	out                                    *bufio.Writer
	header                                 http.Header
	buffer                                 bytes.Buffer
	server                                 *Server
	status                                 int
	sent, chunked, close, duplex, detected bool
}

func (w *writer) Header() http.Header { return w.header }
func (w *writer) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
}
func (w *writer) detect() {
	v := reflect.ValueOf(w.req.Body)
	if !v.IsValid() || v.Type().String() != "*http.maxBytesReader" {
		return
	}
	f := v.Elem().FieldByName("err")
	overflow := f.IsValid() && !f.IsNil() && f.Elem().Type() == reflect.TypeFor[*http.MaxBytesError]()
	if overflow {
		w.close = true
		if !w.sent {
			w.header.Set("Connection", "close")
		}
		if !w.detected {
			w.server.Detections.Add(1)
			w.detected = true
		}
	}
}
func (w *writer) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	if !w.sent {
		return w.buffer.Write(p)
	}
	if w.chunked {
		if _, err := fmt.Fprintf(w.out, "%x\r\n", len(p)); err != nil {
			return 0, err
		}
		n, err := w.out.Write(p)
		if err != nil {
			return n, err
		}
		_, err = w.out.WriteString("\r\n")
		return n, err
	}
	return w.out.Write(p)
}
func (w *writer) send(done bool) {
	w.detect()
	if !w.close && !w.duplex {
		io.Copy(io.Discard, w.original)
	}
	if w.status == 0 {
		w.status = 200
	}
	w.sent = true
	fmt.Fprintf(w.out, "HTTP/1.1 %d %s\r\n", w.status, http.StatusText(w.status))
	// Handler-supplied headers precede the automatic headers, as in std.
	w.header.Write(w.out)
	if w.header.Get("Date") == "" {
		fmt.Fprintf(w.out, "Date: %s\r\n", time.Now().UTC().Format(http.TimeFormat))
	}
	if w.header.Get("Content-Length") == "" {
		if done {
			fmt.Fprintf(w.out, "Content-Length: %s\r\n", strconv.Itoa(w.buffer.Len()))
		} else {
			w.chunked = true
		}
	}
	if w.buffer.Len() > 0 && w.header.Get("Content-Type") == "" {
		fmt.Fprintf(w.out, "Content-Type: %s\r\n", http.DetectContentType(w.buffer.Bytes()))
	}
	if (w.close || w.req.Close) && w.header.Get("Connection") != "close" {
		fmt.Fprint(w.out, "Connection: close\r\n")
	}
	if w.chunked {
		fmt.Fprint(w.out, "Transfer-Encoding: chunked\r\n")
	}
	fmt.Fprint(w.out, "\r\n")
	if w.buffer.Len() > 0 {
		p := append([]byte(nil), w.buffer.Bytes()...)
		w.buffer.Reset()
		w.Write(p)
	}
}
func (w *writer) Flush() {
	if !w.sent {
		w.send(false)
	}
	w.out.Flush()
}
func (w *writer) EnableFullDuplex() error { w.duplex = true; return nil }
