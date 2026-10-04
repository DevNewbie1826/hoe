//go:build linux || darwin

package conn

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DevNewbie1826/hoe/internal/bufpool"
	"github.com/DevNewbie1826/hoe/internal/loop"
)

// Conn is the stable net.Conn identity reported throughout an HTTP lifetime.
// Blocking I/O waits for loop-published edges; idle sockets retain no buffers.
type Conn struct {
	engine                      *Engine
	raw                         *loop.Conn
	local, remote               *net.TCPAddr
	remoteName                  string
	base                        context.Context
	state                       atomic.Int32
	mu                          sync.Mutex
	cancel                      interface{ Cancel() }
	eof                         bool
	bodyDone                    bool
	serving                     bool
	headStart                   time.Time
	accepted                    time.Time
	readDeadline, writeDeadline time.Time
	changed                     chan struct{}
	closeNotify                 chan bool
	readMu                      sync.Mutex
	writeMu                     sync.Mutex
	input                       []byte
	offset                      int
	postPadding                 int
	timerSlot                   int // guarded by wheel.mu
	timerTick                   int64
	timerArmed                  atomic.Bool
	pipelineHooks               PipelineHooks
	pipeline                    pipelineHold
}

func (c *Conn) setState(s http.ConnState) {
	c.state.Store(int32(s))
	if f := c.engine.server.ConnState; f != nil {
		f(c, s)
	}
	c.engine.mu.Lock()
	c.engine.notifyLocked()
	c.engine.mu.Unlock()
	resourceChanged()
}

func (c *Conn) peerEOF() {
	c.mu.Lock()
	c.eof = true
	cancel := c.cancel
	if !c.bodyDone {
		cancel = nil
	}
	if c.bodyDone && c.closeNotify != nil {
		select {
		case c.closeNotify <- true:
		default:
		}
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel.Cancel()
	}
	resourceChanged()
}

func (c *Conn) bodyReadDone() {
	c.mu.Lock()
	c.bodyDone = true
	eof := c.eof
	c.mu.Unlock()
	if eof {
		c.peerEOF()
	}
}

func (c *Conn) cancelRequest() {
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel.Cancel()
	}
}

// Close asks the owner loop to close the descriptor.
func (c *Conn) Close() error {
	c.cancelRequest()
	err := c.endPipelineLifetime()
	return errors.Join(err, c.raw.Close())
}

// LocalAddr returns the immutable local TCP address snapshot.
func (c *Conn) LocalAddr() net.Addr { return c.local }

// RemoteAddr returns the immutable peer TCP address snapshot.
func (c *Conn) RemoteAddr() net.Addr { return c.remote }

// CloseNotify returns a buffered disconnect signal for the current request.
func (c *Conn) CloseNotify() <-chan bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closeNotify == nil {
		c.closeNotify = make(chan bool, 1)
		if c.eof && c.bodyDone {
			c.closeNotify <- true
		}
	}
	return c.closeNotify
}

func (c *Conn) releaseInput() {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	c.releaseInputLocked()
}
func (c *Conn) releaseInputLocked() {
	bufpool.Put(c.input)
	c.input = nil
	c.offset = 0
}

func (c *Conn) pending(p []byte) {
	if len(p) == 0 {
		return
	}
	need := len(c.input) - c.offset + len(p)
	if c.offset == 0 && need <= cap(c.input) {
		c.input = append(c.input, p...)
		return
	}
	b := bufpool.Get(max(4096, need))
	n := copy(b, c.input[c.offset:])
	n += copy(b[n:], p)
	c.releaseInputLocked()
	c.input = b[:n]
}

func (c *Conn) timeoutError(op string) error {
	return &net.OpError{Op: op, Net: "tcp", Source: c.local, Addr: c.remote, Err: os.ErrDeadlineExceeded}
}

func (c *Conn) deadline(op string) (time.Time, <-chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.readDeadline
	if op == "write" {
		t = c.writeDeadline
	}
	return t, c.changed
}

func (c *Conn) wait(op string, ready <-chan struct{}) error {
	if err := c.PushPipeline(); err != nil {
		return err
	}
	for {
		c.mu.Lock()
		if c.changed == nil {
			c.changed = make(chan struct{})
		}
		t, changed := c.readDeadline, c.changed
		if op == "write" {
			t = c.writeDeadline
		}
		c.mu.Unlock()
		if !t.IsZero() && !time.Now().Before(t) {
			return c.timeoutError(op)
		}
		select {
		case <-ready:
			return nil
		case <-c.raw.Done():
			return net.ErrClosed
		case <-changed:
			// Updates and wheel expirations wake blocked operations.
		}
	}
}

// Read provides net.Conn's blocking read semantics over nonblocking syscalls.
func (c *Conn) Read(p []byte) (int, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	for {
		t, _ := c.deadline("read")
		if !t.IsZero() && !time.Now().Before(t) {
			c.cancelRequest()
			return 0, c.timeoutError("read")
		}
		if c.offset < len(c.input) {
			n := copy(p, c.input[c.offset:])
			c.offset += n
			if c.offset == len(c.input) {
				c.releaseInputLocked()
			}
			return n, nil
		}
		ready := c.raw.ReadReady()
		n, err := c.raw.Read(p)
		if n > 0 {
			return n, nil
		}
		if interrupted(err) {
			continue
		}
		if wouldBlock(err) {
			if err = c.wait("read", ready); err != nil {
				c.cancelRequest()
				return 0, err
			}
			continue
		}
		c.peerEOF()
		c.cancelRequest()
		if err == nil {
			err = io.EOF
		}
		return 0, err
	}
}
