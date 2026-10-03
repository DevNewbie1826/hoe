package websocket

import (
	"bytes"
	"compress/flate"
	"io"
	"math"
	"strconv"
	"strings"
)

// negotiate supports a 15-bit server compressor with no context takeover.
// Declining a smaller server window is permitted by RFC 7692; the response
// must not promise a window size the standard flate compressor cannot honor.
func negotiate(values []string) (string, bool) {
	for _, value := range values {
		for _, offer := range strings.Split(value, ",") {
			parts := strings.Split(offer, ";")
			if strings.TrimSpace(parts[0]) != "permessage-deflate" {
				continue
			}
			seen := make(map[string]bool)
			valid, clientNo, clientWindow, serverWindow := true, false, "", ""
			for _, part := range parts[1:] {
				name, v, hasValue := strings.Cut(strings.TrimSpace(part), "=")
				name = strings.TrimSpace(name)
				v = strings.Trim(strings.TrimSpace(v), "\"")
				if seen[name] {
					valid = false
					break
				}
				seen[name] = true
				switch name {
				case "server_no_context_takeover":
					valid = valid && !hasValue
				case "client_no_context_takeover":
					valid = valid && !hasValue
					clientNo = true
				case "server_max_window_bits":
					n, e := strconv.Atoi(v)
					valid = valid && hasValue && e == nil && n == 15
					serverWindow = "15"
				case "client_max_window_bits":
					if hasValue {
						n, e := strconv.Atoi(v)
						valid = valid && e == nil && n >= 8 && n <= 15
						clientWindow = v
					} else {
						clientWindow = "15"
					}
				default:
					valid = false
				}
			}
			if !valid {
				continue
			}
			response := "permessage-deflate; server_no_context_takeover"
			if clientNo {
				response += "; client_no_context_takeover"
			}
			if serverWindow != "" {
				response += "; server_max_window_bits=" + serverWindow
			}
			if clientWindow != "" {
				response += "; client_max_window_bits=" + clientWindow
			}
			return response, clientNo
		}
	}
	return "", false
}

func deflate(p []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.DefaultCompression)
	if err != nil {
		return nil, err
	}
	if _, err = w.Write(p); err != nil {
		w.Close()
		return nil, err
	}
	if err = w.Flush(); err != nil {
		w.Close()
		return nil, err
	}
	n := b.Len() - 4
	out := append([]byte(nil), b.Bytes()[:n]...)
	if err = w.Close(); err != nil {
		return nil, err
	}
	return out, nil
}

func (c *Conn) inflate(p []byte) ([]byte, error) {
	// The sync-flush trailer was removed on the wire. A final empty block
	// terminates the stream so truncated/invalid compressed data is an error.
	r := flate.NewReaderDict(io.MultiReader(bytes.NewReader(p), bytes.NewReader([]byte{0, 0, 255, 255, 1, 0, 0, 255, 255})), c.dictionary)
	defer r.Close()
	var source io.Reader = r
	limit := c.limit.Load()
	if limit > 0 && limit < math.MaxInt64 {
		source = io.LimitReader(r, limit+1)
	}
	out, err := io.ReadAll(source)
	if err != nil {
		return nil, err
	}
	if limit > 0 && int64(len(out)) > limit {
		return nil, ErrReadLimit
	}
	if !c.clientNoContext {
		if len(out) >= 32768 {
			c.dictionary = append(c.dictionary[:0], out[len(out)-32768:]...)
		} else {
			c.dictionary = append(c.dictionary, out...)
			if len(c.dictionary) > 32768 {
				copy(c.dictionary, c.dictionary[len(c.dictionary)-32768:])
				c.dictionary = c.dictionary[:32768]
			}
		}
	}
	return out, nil
}
