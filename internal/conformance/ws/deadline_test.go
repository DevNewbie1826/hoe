package ws_test

import (
	"net/http"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestUpgradeClearsHTTPDeadline(t *testing.T) {
	for _, engine := range []bool{true, false} {
		t.Run(map[bool]string{true: "hoe", false: "std"}[engine], func(t *testing.T) {
			u := &ws.Upgrader{}
			addr := serve(t, engine, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := http.NewResponseController(w).SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
					t.Error(err)
					return
				}
				u.Upgrade(w, r, ws.Handler{OnMessage: func(c *ws.Conn, kind int, p []byte) { c.WriteMessage(kind, p) }})
			}))
			nc, br, _ := rawClient(t, addr, "")
			if _, err := nc.Write(frame(1, true, false, []byte("deadline cleared"))); err != nil {
				t.Fatal(err)
			}
			op, p := readFrame(t, br)
			if op != 1 || string(p) != "deadline cleared" {
				t.Fatalf("echo %d %q", op, p)
			}
		})
	}
}
