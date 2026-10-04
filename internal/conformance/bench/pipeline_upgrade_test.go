//go:build linux || darwin

package bench

import (
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	ws "github.com/DevNewbie1826/hoe/websocket"
)

func TestPipelineReleaseWebSocket(t *testing.T) {
	// Given a held HTTP response followed by a native WebSocket handoff.
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	opened := make(chan *ws.Conn, 1)
	closed := make(chan error, 1)
	rig := pipelineServer(t, pipelineConfig{controlled: true,
		handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			pipelineProbe(t, pipelineTransport(r), true)
			if r.URL.Path == "/A" {
				io.WriteString(w, "/A")
				return
			}
			u := &ws.Upgrader{}
			_, err := u.Upgrade(w, r, ws.Handler{
				OnOpen: func(c *ws.Conn) {
					opened <- c
					<-release
				},
				OnClose: func(c *ws.Conn, err error) { closed <- err },
			})
			if err != nil {
				t.Error(err)
			}
		})})
	var upgraded *ws.Conn
	t.Cleanup(func() {
		unblock()
		if upgraded == nil {
			upgraded = pipelineEvent(t, opened)
		}
		if err := upgraded.Close(); err != nil {
			t.Error(err)
		}
		pipelineEvent(t, closed)
		t.Log("cleanup: WebSocket closed and OnClose joined, including failure paths")
	})
	// When the upgrade and A are submitted in the same TCP write.
	request := "GET /A HTTP/1.1\r\nHost: t\r\n\r\n" +
		"GET /ws HTTP/1.1\r\nHost: t\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n" +
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"
	t.Logf("TCP Write(%q)", request)
	if _, err := io.WriteString(rig.client, request); err != nil {
		t.Fatal(err)
	}
	pipelineEvent(t, rig.arms)
	rig.client.SetReadDeadline(time.Now().Add(pipelineTolerance))
	// Then A and the 101 are delivered while OnOpen remains blocked,
	// and the handoff cleared kernel hold independently of the timer.
	pipelineReceipt(t, rig, "/A")
	pipelineReleased(t, rig)
	response, err := http.ReadResponse(rig.reader, nil)
	if err != nil || response.StatusCode != 101 {
		t.Fatalf("upgrade receipt=%v err=%v", response, err)
	}
	response.Body.Close()
	t.Logf("HTTP/1.1 %s; headers=%v", response.Status, response.Header)
	upgraded = pipelineEvent(t, opened)
}
