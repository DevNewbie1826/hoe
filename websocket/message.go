package websocket

import (
	"encoding/binary"
	"errors"
	"time"
	"unicode/utf8"
)

func (c *Conn) endFrame() error {
	f := &c.parser
	switch f.opcode {
	case PingMessage:
		p := f.control
		f.control = nil
		if err := c.WriteControl(PongMessage, p, time.Now().Add(5*time.Second)); err != nil {
			return err
		}
		if c.handler.OnPing != nil {
			c.handler.OnPing(c, p)
		}
	case PongMessage:
		p := f.control
		f.control = nil
		if c.handler.OnPong != nil {
			c.handler.OnPong(c, p)
		}
	case CloseMessage:
		p := f.control
		f.control = nil
		code, text, valid := parseClose(p)
		if !valid {
			return c.protocolFailure(1002, "invalid close status")
		}
		if !utf8.ValidString(text) {
			return c.protocolFailure(1007, "invalid close reason")
		}
		if err := c.WriteControl(CloseMessage, p, time.Now().Add(5*time.Second)); err != nil && !errors.Is(err, ErrCloseSent) {
			return err
		}
		return &CloseError{Code: code, Text: text}
	case 0, TextMessage, BinaryMessage:
		if !f.final {
			return nil
		}
		p := f.message
		f.message = nil
		if f.compressed {
			var err error
			p, err = c.inflate(p)
			if errors.Is(err, ErrReadLimit) {
				return c.protocolFailure(1009, "read limit exceeded")
			}
			if err != nil {
				return c.protocolFailure(1007, "invalid compressed payload")
			}
		}
		if f.messageType == TextMessage && (!utf8.Valid(p) || len(f.textTail) > 0) {
			return c.protocolFailure(1007, "invalid UTF-8")
		}
		kind := f.messageType
		f.messageType = 0
		f.compressed = false
		f.textTail = nil
		if c.handler.OnMessage != nil {
			c.handler.OnMessage(c, kind, p)
		}
	}
	return nil
}

func (c *Conn) protocolFailure(code int, text string) error {
	if err := c.WriteControl(CloseMessage, FormatCloseMessage(code, text), time.Now().Add(5*time.Second)); err != nil && !errors.Is(err, ErrCloseSent) {
		return err
	}
	return &CloseError{Code: code, Text: text}
}

func parseClose(p []byte) (int, string, bool) {
	if len(p) == 0 {
		return CloseNoStatusReceived, "", true
	}
	if len(p) == 1 {
		return 0, "", false
	}
	code := int(binary.BigEndian.Uint16(p))
	valid := code >= 3000 && code <= 4999 || code >= 1000 && code <= 1014 && code != 1004 && code != 1005 && code != 1006
	return code, string(p[2:]), valid
}
