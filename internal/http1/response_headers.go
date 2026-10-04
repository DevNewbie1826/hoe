package http1

import (
	"bufio"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var dateCache struct {
	sync.Mutex
	current atomic.Pointer[dateValue]
}

type dateValue struct {
	second int64
	value  string
}

func cachedDate() string {
	second := time.Now().Unix()
	if value := dateCache.current.Load(); value != nil && value.second == second {
		return value.value
	}
	dateCache.Lock()
	defer dateCache.Unlock()
	value := dateCache.current.Load()
	if value == nil || value.second != second {
		value = &dateValue{second, time.Unix(second, 0).UTC().Format(http.TimeFormat)}
		dateCache.current.Store(value)
	}
	return value.value
}

func (w *Response) sendHead(p []byte) {
	if w.wire {
		return
	}
	w.detectLimit()
	w.wire = true
	h := w.snapshot
	if h == nil {
		h = w.header
	}
	exclude := make(map[string]bool)
	var date, length, contentType, connection, transfer string
	hasTrailers := false
	for k := range h {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			exclude[k], hasTrailers = true, true
		}
	}
	for _, v := range h.Values("Trailer") {
		hasTrailers = true
		for key := range strings.SplitSeq(v, ",") {
			key = http.CanonicalHeaderKey(strings.Trim(key, " \t"))
			if validTrailer(key) {
				w.trailers = append(w.trailers, key)
			}
		}
	}
	te := h.Get("Transfer-Encoding")
	hasTE := te != ""
	_, hasLengthKey := h["Content-Length"]
	isHead := w.req.Method == "HEAD"
	if w.done && !hasTrailers && !hasTE && bodyAllowed(w.status) && !hasLengthKey && (!isHead || len(p) > 0) {
		w.contentLength = int64(len(p))
		length = strconv.Itoa(len(p))
	}
	hasCL := w.contentLength >= 0
	if w.wants10KeepAlive && !w.disableKeepAlive && h.Get("Content-Length") != "" && h.Get("Connection") == "keep-alive" {
		w.closeAfter = false
	}
	if w.wants10KeepAlive && (isHead || hasCL || !bodyAllowed(w.status)) {
		if _, ok := h["Connection"]; !ok {
			connection = "keep-alive"
		}
	} else if !w.req.ProtoAtLeast(1, 1) || w.wantsClose {
		w.closeAfter = true
	}
	if h.Get("Connection") == "close" || w.disableKeepAlive {
		w.closeAfter = true
	}
	if w.expect && w.body != nil {
		sawEOF := w.body.expectEOF.Load()
		if closeUnsentContinue {
			sawEOF = w.body.EOF()
		}
		if !sawEOF {
			w.closeAfter = true
		}
	}
	if !w.closeAfter && !w.duplex && w.body != nil {
		if w.body.Remaining() >= MaxDrainBytes {
			w.limitHit, w.closeAfter = true, true
			exclude["Connection"], connection = true, "close"
		} else {
			if err := w.body.Close(); err != nil {
				w.closeAfter = true
			}
			w.closeAfter = w.closeAfter || w.body.EarlyClosed()
		}
	}
	if bodyAllowed(w.status) {
		_, haveType := h["Content-Type"]
		if !haveType && h.Get("Content-Encoding") == "" && !hasTE && len(p) > 0 {
			contentType = http.DetectContentType(p)
		}
	} else {
		exclude["Content-Length"], exclude["Transfer-Encoding"] = true, true
		if w.status == 304 {
			exclude["Content-Type"] = true
		}
	}
	if _, ok := h["Date"]; !ok {
		date = cachedDate()
	}
	if hasCL && hasTE && te != "identity" {
		exclude["Content-Length"] = true
		hasCL = false
	}
	switch {
	case isHead || !bodyAllowed(w.status):
		exclude["Transfer-Encoding"] = true
	case hasCL:
		exclude["Transfer-Encoding"] = true
	case w.req.ProtoAtLeast(1, 1) && te == "identity":
		w.closeAfter, exclude["Transfer-Encoding"] = true, true
	case w.req.ProtoAtLeast(1, 1):
		w.chunked, transfer = true, "chunked"
		if te == "chunked" {
			exclude["Transfer-Encoding"] = true
		}
	default:
		w.closeAfter, exclude["Transfer-Encoding"] = true, true
	}
	if w.chunked {
		exclude["Content-Length"] = true
	}
	switching := w.status == 101 && h.Get("Upgrade") != "" && hasToken(h.Values("Connection"), "upgrade")
	if w.closeAfter && (w.disableKeepAlive || !hasToken(h.Values("Connection"), "close")) && !switching {
		exclude["Connection"] = true
		if w.req.ProtoAtLeast(1, 1) {
			connection = "close"
		}
	}
	writeStatus(w.out, w.req.ProtoAtLeast(1, 1), w.status)
	w.record(h.WriteSubset(w.out, exclude))
	for _, field := range [][2]string{{"Date", date}, {"Content-Length", length}, {"Content-Type", contentType}, {"Connection", connection}, {"Transfer-Encoding", transfer}} {
		if field[1] != "" {
			_, err := w.out.WriteString(field[0])
			w.record(err)
			_, err = w.out.WriteString(": ")
			w.record(err)
			_, err = w.out.WriteString(field[1])
			w.record(err)
			_, err = w.out.WriteString("\r\n")
			w.record(err)
		}
	}
	_, err := w.out.WriteString("\r\n")
	w.record(err)
}

func writeStatus(out *bufio.Writer, is11 bool, status int) {
	version := "HTTP/1.0"
	if is11 {
		version = "HTTP/1.1"
	}
	text := http.StatusText(status)
	if text == "" {
		text = fmt.Sprintf("status code %d", status)
	}
	out.WriteString(version)
	out.WriteByte(' ')
	out.WriteByte(byte('0' + status/100))
	out.WriteByte(byte('0' + status/10%10))
	out.WriteByte(byte('0' + status%10))
	out.WriteByte(' ')
	out.WriteString(text)
	out.WriteString("\r\n")
}

func validTrailer(k string) bool {
	if !validToken(k) || strings.HasPrefix(k, "If-") {
		return false
	}
	switch k {
	case "Authorization", "Cache-Control", "Connection", "Content-Encoding", "Content-Length",
		"Content-Range", "Content-Type", "Expect", "Host", "Keep-Alive", "Max-Forwards",
		"Pragma", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Range",
		"Realm", "Te", "Trailer", "Transfer-Encoding", "Www-Authenticate":
		return false
	default:
		return true
	}
}

func (w *Response) finalTrailers() http.Header {
	h := make(http.Header)
	for k, vv := range w.header {
		if strings.HasPrefix(k, http.TrailerPrefix) {
			h[strings.TrimPrefix(k, http.TrailerPrefix)] = vv
		}
	}
	for _, k := range w.trailers {
		for _, v := range w.header[k] {
			h.Add(k, v)
		}
	}
	return h
}
