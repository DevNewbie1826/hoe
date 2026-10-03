package http1

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"regexp"
	"sync"
	"testing"
	"time"
)

type pipeListener struct {
	conn net.Conn
	once sync.Once
	done chan struct{}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	var c net.Conn
	l.once.Do(func() { c = l.conn })
	if c != nil {
		return c, nil
	}
	<-l.done
	return nil, net.ErrClosed
}
func (l *pipeListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}
func (l *pipeListener) Addr() net.Addr { return l.conn.LocalAddr() }

// finiteInput provides the request-side half-close net.Pipe lacks. The bytes
// still cross a real blocking pipe; EOF is injected only after all input bytes.
type finiteInput struct {
	net.Conn
	left int
}

func (c *finiteInput) Read(p []byte) (int, error) {
	if c.left == 0 {
		return 0, io.EOF
	}
	p = p[:min(len(p), c.left)]
	n, err := c.Conn.Read(p)
	c.left -= n
	return n, err
}

var datePattern = regexp.MustCompile(`(?m)^Date: [^\r]*\r$`)

func wire(t *testing.T, own bool, raw string, maxHeader int, handler http.Handler, finished ...func(*Response)) []byte {
	return wireInput(t, own, raw, maxHeader, handler, len(raw), finished...)
}

func wireInput(t *testing.T, own bool, raw string, maxHeader int, handler http.Handler, inputLength int, finished ...func(*Response)) []byte {
	t.Helper()
	client, server := net.Pipe()
	c := &finiteInput{server, inputLength}
	deadline := time.Now().Add(5 * time.Second)
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := server.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var stop func()
	if own {
		go func() {
			defer close(done)
			defer server.Close()
			in, out := bufio.NewReader(c), bufio.NewWriter(c)
			base := context.WithValue(context.Background(), http.ServerContextKey, &http.Server{})
			base = context.WithValue(base, http.LocalAddrContextKey, c.LocalAddr())
			for {
				r, err := ReadRequest(in, maxHeader)
				if err != nil {
					if err := WriteError(out, err); err != nil {
						return
					}
					out.Flush()
					return
				}
				ctx, cancel := context.WithCancel(base)
				r = r.WithContext(ctx)
				w := NewResponse(r, in, out, c)
				if !w.CheckExpect() {
					// The codec committed a standard 417 instead of a handler.
				} else if r.Method == "OPTIONS" && r.RequestURI == "*" {
					w.Header().Set("Content-Length", "0")
				} else {
					handler.ServeHTTP(w, r)
				}
				cancel()
				if w.Hijacked() {
					return
				}
				err = w.Finish()
				for _, fn := range finished {
					fn(w)
				}
				if err != nil || w.CloseAfterReply() {
					return
				}
				if r.Method == "POST" {
					if err := DiscardPostPadding(in); err != nil {
						return
					}
				}
			}
		}()
		stop = func() { server.Close() }
	} else {
		l := &pipeListener{conn: c, done: make(chan struct{})}
		closed := make(chan struct{})
		s := &http.Server{Handler: handler, MaxHeaderBytes: maxHeader, ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed || state == http.StateHijacked {
				close(closed)
			}
		}}
		go func() { defer close(done); s.Serve(l) }()
		stop = func() { s.Close(); l.Close(); <-closed }
	}
	written := make(chan struct{})
	go func() { defer close(written); io.WriteString(client, raw) }()
	t.Cleanup(func() {
		client.Close()
		stop()
		<-written
		<-done
	})
	output, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("wire read: %v, partial=%q", err, output)
	}
	return datePattern.ReplaceAll(output, []byte("Date: DATE\r"))
}

func TestDifferentialMalformedLineNoEOF(t *testing.T) {
	// Given: a malformed completed line, but neither EOF nor a blank line.
	raw := "GARBAGE\r\n"
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("malformed request reached handler") })
	// When: the peer remains open waiting for a response.
	want := wireInput(t, false, raw, 0, h, len(raw)+1)
	got := wireInput(t, true, raw, 0, h, len(raw)+1)
	// Then: both servers reject immediately with the same 400 bytes.
	if string(got) != string(want) {
		t.Fatalf("DIFF immediate400: std=%q codec=%q", want, got)
	}
}
