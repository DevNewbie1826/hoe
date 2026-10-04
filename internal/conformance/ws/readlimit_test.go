package ws_test

import (
	"errors"
	"math"
	"net/http"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
	"github.com/gorilla/websocket"
)

func TestSetReadLimitBoundary(t *testing.T) {
	for _, limit := range []int64{4, math.MaxInt64} {
		t.Run(map[bool]string{true: "maximum", false: "small"}[limit == math.MaxInt64], func(t *testing.T) {
			u := &ws.Upgrader{EnableCompression: true}
			addr := serve(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				u.Upgrade(w, r, ws.Handler{
					OnOpen:    func(c *ws.Conn) { c.SetReadLimit(limit) },
					OnMessage: func(c *ws.Conn, k int, p []byte) { c.WriteMessage(k, p) },
				})
			}))
			d := websocket.Dialer{EnableCompression: true, HandshakeTimeout: 5 * time.Second}
			c, _, err := d.Dial("ws://"+addr, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			c.EnableWriteCompression(true)
			if err = c.WriteMessage(2, []byte("12345")); err != nil {
				t.Fatal(err)
			}
			if err = c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			kind, p, err := c.ReadMessage()
			if limit == 4 {
				var closed *websocket.CloseError
				if !errors.As(err, &closed) || closed.Code != 1009 {
					t.Fatalf("limit4 close: %v", err)
				}
			} else if err != nil || kind != 2 || string(p) != "12345" {
				t.Fatalf("maximum limit echo: %d %q %v", kind, p, err)
			}
		})
	}
}
