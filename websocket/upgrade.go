package websocket

import (
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
)

// Upgrader configures a server handshake. The zero value permits requests with
// no Origin or a same-host Origin, no compression, and messages up to 64MiB.
// Configuration must not change while Upgrade is being called.
type Upgrader struct {
	// CheckOrigin authorizes an Origin; nil enforces same-host origins.
	CheckOrigin func(*http.Request) bool
	// Subprotocols lists supported protocols in server preference order.
	Subprotocols []string
	// EnableCompression permits permessage-deflate negotiation.
	EnableCompression bool
	// ReadLimit bounds both encoded and decoded messages; zero selects 64MiB.
	ReadLimit int64
}

var defaultUpgrader Upgrader
var upgradeGroup conn.UpgradeGroup

// Upgrade performs a handshake using the default Upgrader.
func Upgrade(w http.ResponseWriter, r *http.Request, h Handler) (*Conn, error) {
	return defaultUpgrader.Upgrade(w, r, h)
}

// Upgrade validates the handshake, transfers ownership and invokes OnOpen.
// On success the HTTP handler may return immediately. Handshake validation
// failures write an HTTP error; after hijacking, failures close the socket.
func (u *Upgrader) Upgrade(w http.ResponseWriter, r *http.Request, h Handler) (*Conn, error) {
	fail := func(status int, reason string) (*Conn, error) {
		http.Error(w, reason, status)
		return nil, fmt.Errorf("websocket: handshake: %s", reason)
	}
	if r.Method != "GET" {
		w.Header().Set("Allow", "GET")
		return fail(405, "GET required")
	}
	if !r.ProtoAtLeast(1, 1) || !token(r.Header.Values("Connection"), "upgrade") || !token(r.Header.Values("Upgrade"), "websocket") {
		return fail(400, "WebSocket upgrade required")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		w.Header().Set("Sec-WebSocket-Version", "13")
		return fail(426, "WebSocket version 13 required")
	}
	keys := r.Header.Values("Sec-WebSocket-Key")
	if len(keys) != 1 {
		return fail(400, "invalid WebSocket key")
	}
	key := strings.TrimSpace(keys[0])
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err != nil || len(decoded) != 16 {
		return fail(400, "invalid WebSocket key")
	}
	check := u.CheckOrigin
	if check == nil {
		check = sameOrigin
	}
	if !check(r) {
		return fail(403, "origin not allowed")
	}
	protocol := ""
	for _, supported := range u.Subprotocols {
		if tokenExact(r.Header.Values("Sec-WebSocket-Protocol"), supported) {
			protocol = supported
			break
		}
	}
	extension, clientNoContext := "", false
	if u.EnableCompression {
		extension, clientNoContext = negotiate(r.Header.Values("Sec-WebSocket-Extensions"))
	}
	isHoe := conn.IsUpgradeWriter(w)
	nc, rw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return nil, err
	}
	success := false
	var owner transport = nc
	defer func() {
		if !success {
			owner.Close()
		}
	}()
	if err = nc.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	if _, err = fmt.Fprintf(rw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n", base64.StdEncoding.EncodeToString(sum[:])); err != nil {
		return nil, err
	}
	if protocol != "" {
		if _, err = fmt.Fprintf(rw, "Sec-WebSocket-Protocol: %s\r\n", protocol); err != nil {
			return nil, err
		}
	}
	if extension != "" {
		if _, err = fmt.Fprintf(rw, "Sec-WebSocket-Extensions: %s\r\n", extension); err != nil {
			return nil, err
		}
	}
	if _, err = rw.WriteString("\r\n"); err != nil {
		return nil, err
	}
	if err = rw.Flush(); err != nil {
		return nil, err
	}
	c := &Conn{socket: nc, handler: h, protocol: protocol, compression: extension != "", clientNoContext: clientNoContext, writeGate: make(chan struct{}, 1)}
	c.writeGate <- struct{}{}
	limit := u.ReadLimit
	if limit == 0 {
		limit = 64 << 20
	}
	c.limit.Store(limit)
	if isHoe {
		var pending []byte
		if n := rw.Reader.Buffered(); n > 0 {
			p, e := rw.Reader.Peek(n)
			if e != nil {
				return nil, e
			}
			pending = append(pending, p...)
		}
		ac, e := upgradeGroup.Adopt(nc)
		if e != nil {
			return nil, e
		}
		c.socket = ac
		owner = ac
		if h.OnOpen != nil {
			h.OnOpen(c)
		}
		ac.Start(pending, c.feed, c.finish)
	} else {
		if h.OnOpen != nil {
			h.OnOpen(c)
		}
		go func() {
			defer nc.Close()
			var buf [16 << 10]byte
			for {
				n, e := rw.Read(buf[:])
				if n > 0 {
					if e2 := c.feed(buf[:n]); e2 != nil {
						c.finish(e2)
						return
					}
				}
				if e != nil {
					c.finish(e)
					return
				}
			}
		}()
	}
	success = true
	return c, nil
}

func token(values []string, wanted string) bool {
	for _, v := range values {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), wanted) {
				return true
			}
		}
	}
	return false
}
func tokenExact(values []string, wanted string) bool {
	for _, v := range values {
		for _, t := range strings.Split(v, ",") {
			if strings.TrimSpace(t) == wanted {
				return true
			}
		}
	}
	return false
}
func sameOrigin(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if len(values) == 0 {
		return true
	}
	if len(values) != 1 {
		return false
	}
	u, err := url.Parse(values[0])
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, r.Host)
}
