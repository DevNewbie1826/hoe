// Package http1 implements the HTTP/1 wire codec independently of a transport.
// Callers supply blocking, bounded readers and writers and own connection
// scheduling, deadlines, contexts, and the lifecycle of their I/O buffers.
package http1

import (
	"bufio"
	"bytes"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unsafe"
)

type requestBlock struct {
	request  http.Request
	url      url.URL
	values   [8]string
	body     Body
	context  requestContext
	response Response
	headData [128]byte
}

// ParseHead parses a complete request head, including its terminating blank
// line. It copies the input once: retained requests never alias transport
// buffers. body is the connection's reader positioned immediately after head;
// it is not read until the handler reads or closes Request.Body.
// Uncommon heads use http.ReadRequest; ordinary requests share one block for
// their Request, URL, header values, and body state.
func ParseHead(head []byte, body *bufio.Reader) (*http.Request, error) {
	b := new(requestBlock)
	var owned string
	if len(head) <= len(b.headData) {
		copy(b.headData[:], head)
		// The bytes belong to this fresh, never-pooled request allocation.
		// They are never changed after publication; retained URL/header
		// strings keep the containing allocation alive.
		owned = unsafe.String(&b.headData[0], len(head))
	} else {
		owned = string(head)
	}
	if r := fastHead(owned, body, b); r != nil {
		return r, nil
	}
	r, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(head)))
	if err != nil {
		return nil, err
	}
	b.request = *r
	b.attachContext()
	attachBody(&b.request, body, &b.body)
	return &b.request, nil
}

func fastHead(s string, in *bufio.Reader, b *requestBlock) *http.Request {
	line, rest, ok := strings.Cut(s, "\r\n")
	if !ok {
		return nil
	}
	method, line, ok := strings.Cut(line, " ")
	if !ok || !validToken(method) {
		return nil
	}
	target, proto, ok := strings.Cut(line, " ")
	if !ok || (proto != "HTTP/1.1" && proto != "HTTP/1.0") {
		return nil
	}
	// Delegate escaping, CONNECT, absolute-form, fragments, and unusual bytes
	// to the standard URL parser rather than widening the fast-path contract.
	if target != "*" && !strings.HasPrefix(target, "/") {
		return nil
	}
	for i := range len(target) {
		if target[i] <= ' ' || target[i] >= 127 || target[i] == '%' || target[i] == '#' {
			return nil
		}
	}
	r := &b.request
	r.Method, r.RequestURI, r.Proto = internMethod(method), target, proto
	r.ProtoMajor, r.ProtoMinor = 1, 1
	if proto == "HTTP/1.0" {
		r.ProtoMinor = 0
	}
	r.URL = &b.url
	r.URL.Path, r.URL.RawQuery, r.URL.ForceQuery = splitTarget(target)
	r.Header = make(http.Header)
	count := 0
	haveHost := false
	for {
		line, rest, ok = strings.Cut(rest, "\r\n")
		if !ok {
			return nil
		}
		if line == "" {
			if rest != "" {
				return nil
			}
			break
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok || !validToken(k) || !validValue(v) || count == len(b.values) {
			return nil
		}
		k = commonKey(k)
		if k == "Host" {
			if haveHost {
				return nil
			}
			haveHost = true
			r.Host = strings.Trim(v, " \t")
			continue
		}
		if _, exists := r.Header[k]; exists {
			return nil
		}
		// Transfer-Encoding, folded fields and exotic framing stay on std.
		if k == "Transfer-Encoding" {
			return nil
		}
		b.values[count] = strings.Trim(v, " \t")
		r.Header[k] = b.values[count : count+1 : count+1]
		count++
	}
	if v, exists := r.Header["Content-Length"]; exists {
		n, err := strconv.ParseUint(v[0], 10, 63)
		if err != nil {
			return nil
		}
		r.ContentLength = int64(n)
	}
	if r.Header.Get("Pragma") == "no-cache" {
		if _, exists := r.Header["Cache-Control"]; !exists {
			r.Header["Cache-Control"] = []string{"no-cache"}
		}
	}
	r.Close = hasToken(r.Header.Values("Connection"), "close") ||
		(r.ProtoMinor == 0 && !hasToken(r.Header.Values("Connection"), "keep-alive"))
	attachBody(r, in, &b.body)
	b.context.head = s
	b.attachContext()
	return r
}

func splitTarget(target string) (path, query string, force bool) {
	path, query, found := strings.Cut(target, "?")
	return path, query, found && query == ""
}

func internMethod(s string) string {
	switch s {
	case "GET":
		return http.MethodGet
	case "HEAD":
		return http.MethodHead
	case "POST":
		return http.MethodPost
	case "PUT":
		return http.MethodPut
	case "DELETE":
		return http.MethodDelete
	case "OPTIONS":
		return http.MethodOptions
	case "PATCH":
		return http.MethodPatch
	case "CONNECT":
		return http.MethodConnect
	case "TRACE":
		return http.MethodTrace
	default:
		return s
	}
}

func commonKey(s string) string {
	// CanonicalHeaderKey avoids allocations for already canonical names.
	k := http.CanonicalHeaderKey(s)
	switch k {
	case "Host":
		return "Host"
	case "Content-Length":
		return "Content-Length"
	case "Content-Type":
		return "Content-Type"
	case "Connection":
		return "Connection"
	case "User-Agent":
		return "User-Agent"
	case "Accept":
		return "Accept"
	case "Accept-Encoding":
		return "Accept-Encoding"
	case "Transfer-Encoding":
		return "Transfer-Encoding"
	default:
		return k
	}
}

func validToken(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			continue
		}
		if !strings.ContainsRune("!#$%&'*+-.^_`|~", rune(c)) {
			return false
		}
	}
	return true
}

func validValue(s string) bool {
	for i := range len(s) {
		if s[i] == 127 || s[i] < 32 && s[i] != '\t' {
			return false
		}
	}
	return true
}

func hasToken(values []string, token string) bool {
	for _, v := range values {
		for part := range strings.SplitSeq(v, ",") {
			if strings.EqualFold(strings.Trim(part, " \t"), token) {
				return true
			}
		}
	}
	return false
}
