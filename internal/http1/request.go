package http1

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strings"
)

// HeaderError describes a protocol error and its standard server response.
type HeaderError struct {
	// Status is the standard HTTP error status.
	Status int
	// Detail is included in status-error responses such as missing Host.
	Detail string
}

// Error returns the HTTP status and protocol diagnostic.
func (e *HeaderError) Error() string {
	return fmt.Sprintf("%d %s: %s", e.Status, http.StatusText(e.Status), e.Detail)
}

// WriteError writes the same raw error response as net/http.Server. EOF,
// unexpected EOF, and network read failures receive no response.
func WriteError(out io.Writer, err error) error {
	var network net.Error
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &network) {
		return nil
	}
	code, detail := 400, ""
	var h *HeaderError
	if errors.As(err, &h) {
		code, detail = h.Status, h.Detail
	}
	status := fmt.Sprintf("%d %s", code, http.StatusText(code))
	message := status
	if code == 501 {
		message = "Unsupported transfer encoding"
	} else if detail != "" {
		status += ": " + detail
		message = status
	}
	_, writeErr := fmt.Fprintf(out, "HTTP/1.1 %s\r\nContent-Type: text/plain; charset=utf-8\r\nConnection: close\r\n\r\n%s", status, message)
	return writeErr
}

// ReadRequest reads one server request. maxHeaderBytes has the same default
// and 4096-byte read-ahead allowance as net/http.Server.MaxHeaderBytes.
// The supplied reader remains the sole owner of buffered pipeline bytes.
func ReadRequest(in *bufio.Reader, maxHeaderBytes int) (*http.Request, error) {
	if maxHeaderBytes <= 0 {
		maxHeaderBytes = http.DefaultMaxHeaderBytes
	}
	limit := int64(maxHeaderBytes) + 4096
	// Already buffered ordinary heads need no intermediate head allocation.
	if _, err := in.Peek(1); err != nil {
		return nil, err
	}
	p, _ := in.Peek(in.Buffered())
	if n := headEnd(p); n > 0 {
		if int64(n) > limit {
			return nil, &HeaderError{Status: 431}
		}
		r, err := ParseHead(p[:n], in)
		if err == nil {
			err = validateServerHead(p[:n], r)
		}
		if _, discardErr := in.Discard(n); discardErr != nil {
			return nil, discardErr
		}
		return r, classifyHeadError(err)
	}
	var head []byte
	firstLine := true
	for {
		fragment, err := in.ReadSlice('\n')
		if int64(len(head))+int64(len(fragment)) > limit {
			return nil, &HeaderError{Status: 431}
		}
		head = append(head, fragment...)
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(head) > 0 {
				r, parseErr := ParseHead(head, in)
				if parseErr == nil {
					parseErr = validateServerHead(head, r)
				}
				return r, classifyHeadError(parseErr)
			}
			return nil, err
		}
		if firstLine {
			firstLine = false
			// Reject a malformed completed request line before waiting for
			// more headers. A blank synthetic head lets std validate method,
			// version and URL without consuming transport body/pipeline bytes.
			probe := io.MultiReader(bytes.NewReader(head), strings.NewReader("\r\n"))
			if _, err := http.ReadRequest(bufio.NewReader(probe)); err != nil {
				return nil, classifyHeadError(err)
			}
		}
		if bytes.Equal(fragment, []byte("\r\n")) || bytes.Equal(fragment, []byte("\n")) {
			break
		}
	}
	r, err := ParseHead(head, in)
	if err == nil {
		err = validateServerHead(head, r)
	}
	return r, classifyHeadError(err)
}

func headEnd(p []byte) int {
	// Choose the first terminator even when a following pipelined request
	// uses a different accepted newline style.
	for i := 0; i+1 < len(p); i++ {
		if p[i] != '\n' {
			continue
		}
		if p[i+1] == '\n' {
			return i + 2
		}
		if i+2 < len(p) && p[i+1] == '\r' && p[i+2] == '\n' {
			return i + 3
		}
	}
	return 0
}

func classifyHeadError(err error) error {
	if err == nil {
		return nil
	}
	var h *HeaderError
	if errors.As(err, &h) {
		return err
	}
	// The standard parser's unsupportedTEError is package-private.
	if strings.HasPrefix(err.Error(), "unsupported transfer encoding:") ||
		strings.HasPrefix(err.Error(), "too many transfer encodings:") {
		return &HeaderError{Status: 501}
	}
	return err
}

func validateServerHead(head []byte, r *http.Request) error {
	upgrade := r.Method == "PRI" && r.RequestURI == "*" && r.ProtoMajor == 2 && r.ProtoMinor == 0
	if r.ProtoMajor != 1 && !upgrade {
		return &HeaderError{505, "unsupported protocol version"}
	}
	// Host is deleted by the public ReadRequest. Inspect the original field,
	// including absolute-form requests whose URL authority overrides Host.
	s := string(head)
	_, headers, _ := strings.Cut(s, "\n")
	haveHost := false
	for line := range strings.SplitSeq(headers, "\n") {
		k, v, ok := strings.Cut(strings.TrimSuffix(line, "\r"), ":")
		if ok && strings.EqualFold(k, "Host") {
			haveHost = true
			if !validHost(strings.Trim(v, " \t")) {
				return &HeaderError{400, "malformed Host header"}
			}
		}
	}
	if r.ProtoAtLeast(1, 1) && !haveHost && !upgrade && r.Method != "CONNECT" {
		return &HeaderError{400, "missing required Host header"}
	}
	for k, vv := range r.Header {
		if !validToken(k) {
			return &HeaderError{400, "invalid header name"}
		}
		for _, v := range vv {
			if !validValue(v) {
				return &HeaderError{400, "invalid header value"}
			}
		}
	}
	// Folded Host values are uncommon; validate their unfolded value too.
	if haveHost && bytes.Contains(head, []byte("\n ")) || haveHost && bytes.Contains(head, []byte("\n\t")) {
		h, err := textproto.NewReader(bufio.NewReader(strings.NewReader(headers))).ReadMIMEHeader()
		if err != nil {
			return err
		}
		if !validHost(h.Get("Host")) {
			return &HeaderError{400, "malformed Host header"}
		}
	}
	return nil
}

func validHost(s string) bool {
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if !strings.ContainsRune("!$%&'()*+,-.:;=[]_~", rune(c)) {
			return false
		}
	}
	return true
}

// DiscardPostPadding tolerates up to four leading CR/LF bytes between a POST
// body and the next request, exactly like net/http's connection reader.
// The owner calls it only when the preceding request's method was POST.
func DiscardPostPadding(in *bufio.Reader) error {
	p, _ := in.Peek(4)
	n := 0
	for _, c := range p {
		if c != '\r' && c != '\n' {
			break
		}
		n++
	}
	if _, discardErr := in.Discard(n); discardErr != nil {
		return discardErr
	}
	return nil
}
