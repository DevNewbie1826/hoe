package websocket

import (
	"encoding/binary"
	"io"
	"net"
	"os"
	"time"
	"unicode/utf8"
)

// WriteMessage writes one text or binary message. Messages and controls from
// concurrent writers never interleave their headers or payloads.
func (c *Conn) WriteMessage(kind int, p []byte) error {
	if kind != TextMessage && kind != BinaryMessage {
		return ErrInvalidMessage
	}
	if kind == TextMessage && !utf8.Valid(p) {
		return ErrInvalidMessage
	}
	return c.writeFrame(kind, p, time.Time{})
}

// WriteControl writes a close, ping or pong frame. The deadline bounds both
// waiting for another writer and socket backpressure. A zero deadline disables
// the bound. Close frames are sent once; subsequent writes return ErrCloseSent.
func (c *Conn) WriteControl(kind int, p []byte, deadline time.Time) error {
	if kind != CloseMessage && kind != PingMessage && kind != PongMessage || len(p) > 125 {
		return ErrInvalidMessage
	}
	if kind == CloseMessage {
		_, text, valid := parseClose(p)
		if !valid || !utf8.ValidString(text) {
			return ErrInvalidMessage
		}
	}
	return c.writeFrame(kind, p, deadline)
}

func (c *Conn) writeFrame(kind int, p []byte, deadline time.Time) error {
	var timer *time.Timer
	var timeout <-chan time.Time
	if !deadline.IsZero() {
		if !time.Now().Before(deadline) {
			return os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		timeout = timer.C
	}
	select {
	case <-c.writeGate:
	case <-timeout:
		return os.ErrDeadlineExceeded
	}
	defer func() { c.writeGate <- struct{}{} }()
	if c.closed.Load() {
		return net.ErrClosed
	}
	if c.closeSent {
		return ErrCloseSent
	}
	if err := c.socket.SetWriteDeadline(deadline); err != nil {
		return err
	}
	compressed := c.compression && kind < 8
	if compressed {
		var err error
		p, err = deflate(p)
		if err != nil {
			return err
		}
	}
	var header [10]byte
	header[0] = 128 | byte(kind)
	if compressed {
		header[0] |= 64
	}
	n := 2
	switch {
	case len(p) < 126:
		header[1] = byte(len(p))
	case len(p) <= 65535:
		header[1] = 126
		binary.BigEndian.PutUint16(header[2:4], uint16(len(p)))
		n = 4
	default:
		header[1] = 127
		binary.BigEndian.PutUint64(header[2:], uint64(len(p)))
		n = 10
	}
	if kind == CloseMessage {
		c.closeSent = true
	}
	for _, part := range [][]byte{header[:n], p} {
		for len(part) > 0 {
			written, err := c.socket.Write(part)
			if err != nil {
				c.Close()
				return err
			}
			if written == 0 {
				c.Close()
				return io.ErrNoProgress
			}
			part = part[written:]
		}
	}
	return nil
}
