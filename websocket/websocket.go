// Package websocket implements callback-based RFC 6455 server connections.
// On hoe plaintext TCP servers, idle connections wait in a shared event loop;
// other HTTP servers use a single reader goroutine per hijacked connection.
package websocket

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Message opcodes defined by RFC 6455.
const (
	TextMessage   = 1
	BinaryMessage = 2
	CloseMessage  = 8
	PingMessage   = 9
	PongMessage   = 10
)

// Close status codes defined by RFC 6455 and the IANA registry.
const (
	CloseNormalClosure           = 1000
	CloseGoingAway               = 1001
	CloseProtocolError           = 1002
	CloseUnsupportedData         = 1003
	CloseNoStatusReceived        = 1005
	CloseAbnormalClosure         = 1006
	CloseInvalidFramePayloadData = 1007
	ClosePolicyViolation         = 1008
	CloseMessageTooBig           = 1009
	CloseMandatoryExtension      = 1010
	CloseInternalServerErr       = 1011
	CloseServiceRestart          = 1012
	CloseTryAgainLater           = 1013
	CloseTLSHandshake            = 1015
)

// ErrCloseSent means a close frame has already been sent.
var ErrCloseSent = errors.New("websocket: close already sent")

// ErrReadLimit means a frame or decoded message exceeds the read limit.
var ErrReadLimit = errors.New("websocket: read limit exceeded")

// ErrInvalidMessage means an outgoing opcode or payload is invalid.
var ErrInvalidMessage = errors.New("websocket: invalid message")

// CloseError describes a received close frame. Code 1005 denotes an empty
// close payload; code 1006 denotes transport closure without a close frame.
type CloseError struct {
	Code int
	Text string
}

// Error describes the close status and reason.
func (e *CloseError) Error() string { return fmt.Sprintf("websocket: close %d: %s", e.Code, e.Text) }

// Handler receives serial callbacks for one connection. Different connections
// may call it concurrently. Payload slices belong to the application and may
// be retained. Ping is answered automatically before OnPing is called.
type Handler struct {
	// OnOpen runs before Upgrade returns and before input callbacks begin.
	OnOpen func(*Conn)
	// OnMessage receives a complete, validated text or binary message.
	OnMessage func(*Conn, int, []byte)
	// OnClose reports peer or transport closure after OnOpen returns.
	OnClose func(*Conn, error)
	// OnPing observes a ping after its automatic pong has been written.
	OnPing func(*Conn, []byte)
	// OnPong receives a peer pong.
	OnPong func(*Conn, []byte)
}

type transport interface {
	io.Writer
	io.Closer
	SetWriteDeadline(time.Time) error
}

// Conn is created by Upgrade and represents a server-side connection.
// WriteMessage, WriteControl,
// Close and SetReadLimit are safe to call concurrently with callbacks.
type Conn struct {
	socket          transport
	handler         Handler
	protocol        string
	compression     bool
	clientNoContext bool
	limit           atomic.Int64
	closed          atomic.Bool
	closeOnce       sync.Once
	writeGate       chan struct{}
	closeSent       bool        // guarded by writeGate
	parser          frameParser // only the serial input callback accesses this state
	dictionary      []byte
}

// Subprotocol returns the negotiated subprotocol, or an empty string.
func (c *Conn) Subprotocol() string { return c.protocol }

// SetReadLimit sets the maximum encoded and decoded message size. A nonpositive
// limit disables the bound. Upgrader defaults to a 64MiB limit.
func (c *Conn) SetReadLimit(n int64) { c.limit.Store(n) }

// Close closes the socket immediately; send WriteControl(CloseMessage, ...)
// first to initiate a graceful close handshake.
func (c *Conn) Close() error {
	c.closed.Store(true)
	return c.socket.Close()
}

func (c *Conn) finish(err error) {
	c.closed.Store(true)
	c.closeOnce.Do(func() {
		if err == nil || errors.Is(err, io.EOF) {
			err = &CloseError{Code: CloseAbnormalClosure}
		}
		if c.handler.OnClose != nil {
			c.handler.OnClose(c, err)
		}
	})
}

// FormatCloseMessage encodes a status and UTF-8 reason for a close frame.
// CloseNoStatusReceived produces the empty close payload.
func FormatCloseMessage(code int, text string) []byte {
	if code == CloseNoStatusReceived {
		return nil
	}
	p := make([]byte, 2+len(text))
	binary.BigEndian.PutUint16(p, uint16(code))
	copy(p[2:], text)
	return p
}
