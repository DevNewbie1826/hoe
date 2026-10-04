package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	url := flag.String("url", "", "WebSocket echo URL")
	n := flag.Int("n", 1, "simultaneous validated WebSocket connections")
	flag.Parse()
	if err := run(*url, *n); err != nil {
		log.Fatal(err)
	}
}

func run(url string, n int) error {
	if url == "" || n <= 0 {
		return fmt.Errorf("provide an echo URL and a positive connection count")
	}
	conns := make([]*websocket.Conn, 0, n)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	payload := []byte("Hello Benchmark")
	for range n {
		c, res, err := websocket.DefaultDialer.Dial(url, nil)
		if err != nil {
			return err
		}
		if res.StatusCode != 101 {
			c.Close()
			return fmt.Errorf("upgrade status=%d", res.StatusCode)
		}
		conns = append(conns, c)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := c.WriteMessage(websocket.TextMessage, payload); err != nil {
			return err
		}
		kind, p, err := c.ReadMessage()
		if err != nil || kind != websocket.TextMessage || !bytes.Equal(p, payload) {
			return fmt.Errorf("echo kind=%d body=%q error=%v", kind, p, err)
		}
	}
	for _, c := range conns {
		if err := c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
			return err
		}
		if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
			return fmt.Errorf("close handshake: %v", err)
		}
	}
	fmt.Printf("PASS websocket connections=%d errors=0 all_bodies_validated=true close_handshakes=true\n", len(conns))
	return nil
}
