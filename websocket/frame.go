package websocket

import (
	"encoding/binary"
	"net"
	"unicode/utf8"
)

type frameParser struct {
	header              [14]byte
	used, need          int
	remaining, position uint64
	mask                [4]byte
	opcode              int
	final               bool
	control             []byte
	message             []byte
	messageType         int
	compressed          bool
	textTail            []byte
}

func (c *Conn) feed(p []byte) error {
	f := &c.parser
	for len(p) > 0 {
		if c.closed.Load() {
			return net.ErrClosed
		}
		if f.need == 0 {
			f.need = 2
		}
		if f.used < f.need {
			n := copy(f.header[f.used:f.need], p)
			f.used += n
			p = p[n:]
			if f.used < f.need {
				return nil
			}
			if f.need == 2 {
				length := int(f.header[1] & 127)
				if f.header[1]&128 == 0 {
					return c.protocolFailure(1002, "unmasked client frame")
				}
				if f.header[0]&15 >= 8 && (f.header[0]&128 == 0 || length > 125) {
					return c.protocolFailure(1002, "invalid control frame")
				}
				f.need = 6
				if length == 126 {
					f.need = 8
				} else if length == 127 {
					f.need = 14
				}
				continue
			}
			if err := c.beginFrame(); err != nil {
				return err
			}
		}
		n := len(p)
		if uint64(n) > f.remaining {
			n = int(f.remaining)
		}
		var dst []byte
		if f.opcode >= 8 {
			start := len(f.control)
			f.control = append(f.control, p[:n]...)
			dst = f.control[start:]
		} else {
			start := len(f.message)
			f.message = append(f.message, p[:n]...)
			dst = f.message[start:]
		}
		for i := range dst {
			dst[i] ^= f.mask[(f.position+uint64(i))&3]
		}
		if f.opcode < 8 && f.messageType == TextMessage && !f.compressed {
			if !f.validText(dst) {
				return c.protocolFailure(1007, "invalid UTF-8")
			}
		}
		f.remaining -= uint64(n)
		f.position += uint64(n)
		p = p[n:]
		if f.remaining > 0 {
			continue
		}
		if err := c.endFrame(); err != nil {
			return err
		}
		f.used, f.need = 0, 0
	}
	return nil
}

func (c *Conn) beginFrame() error {
	f := &c.parser
	b := f.header[0]
	f.opcode = int(b & 15)
	f.final = b&128 != 0
	f.position = 0
	rsv1 := b&64 != 0
	if b&48 != 0 || f.header[1]&128 == 0 {
		return c.protocolFailure(1002, "reserved bits or unmasked client frame")
	}
	switch f.opcode {
	case 0:
		if f.messageType == 0 || rsv1 {
			return c.protocolFailure(1002, "unexpected continuation")
		}
	case TextMessage, BinaryMessage:
		if f.messageType != 0 {
			return c.protocolFailure(1002, "interleaved message")
		}
		if rsv1 && !c.compression {
			return c.protocolFailure(1002, "unnegotiated compression")
		}
		f.messageType, f.compressed = f.opcode, rsv1
	case CloseMessage, PingMessage, PongMessage:
		if !f.final || rsv1 {
			return c.protocolFailure(1002, "invalid control frame")
		}
	default:
		return c.protocolFailure(1002, "unknown opcode")
	}
	length := uint64(f.header[1] & 127)
	maskOffset := 2
	switch length {
	case 126:
		length = uint64(binary.BigEndian.Uint16(f.header[2:4]))
		maskOffset = 4
		if length < 126 {
			return c.protocolFailure(1002, "nonminimal length")
		}
	case 127:
		length = binary.BigEndian.Uint64(f.header[2:10])
		maskOffset = 10
		if length < 65536 || length>>63 != 0 {
			return c.protocolFailure(1002, "invalid length")
		}
	}
	if f.opcode >= 8 && length > 125 {
		return c.protocolFailure(1002, "oversized control frame")
	}
	if f.opcode < 8 {
		limit := c.limit.Load()
		if limit > 0 && (length > uint64(limit) || uint64(len(f.message)) > uint64(limit)-length) {
			return c.protocolFailure(1009, "read limit exceeded")
		}
	}
	copy(f.mask[:], f.header[maskOffset:maskOffset+4])
	f.remaining = length
	return nil
}

func (f *frameParser) validText(p []byte) bool {
	if len(f.textTail) > 0 {
		for !utf8.FullRune(f.textTail) && len(p) > 0 {
			f.textTail = append(f.textTail, p[0])
			p = p[1:]
		}
		if !utf8.FullRune(f.textTail) {
			return true
		}
		r, size := utf8.DecodeRune(f.textTail)
		if r == utf8.RuneError && size == 1 {
			return false
		}
		f.textTail = nil
	}
	for len(p) > 0 {
		if !utf8.FullRune(p) {
			f.textTail = append([]byte(nil), p...)
			return true
		}
		r, n := utf8.DecodeRune(p)
		if r == utf8.RuneError && n == 1 {
			return false
		}
		p = p[n:]
	}
	return true
}
