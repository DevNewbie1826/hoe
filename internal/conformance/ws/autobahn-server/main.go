package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/DevNewbie1826/hoe"
	ws "github.com/DevNewbie1826/hoe/websocket"
	"github.com/gorilla/websocket"
)

func gorillaEcho(w http.ResponseWriter, r *http.Request) {
	u := websocket.Upgrader{EnableCompression: true, CheckOrigin: func(*http.Request) bool { return true }}
	c, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	for {
		kind, p, err := c.ReadMessage()
		if err != nil {
			return
		}
		if err = c.WriteMessage(kind, p); err != nil {
			return
		}
	}
}

func main() {
	u := &ws.Upgrader{EnableCompression: true, CheckOrigin: func(*http.Request) bool { return true }}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.Upgrade(w, r, ws.Handler{OnMessage: func(c *ws.Conn, kind int, p []byte) {
			if err := c.WriteMessage(kind, p); err != nil {
				c.Close()
			}
		}})
	})
	servers := []interface {
		Serve(net.Listener) error
		Close() error
	}{
		&hoe.Server{Handler: h},
		&hoe.Server{Handler: http.HandlerFunc(gorillaEcho)},
		&http.Server{Handler: http.HandlerFunc(gorillaEcho)},
	}
	listeners := make([]net.Listener, 0, len(servers))
	for i := range servers {
		l, err := net.Listen("tcp", fmt.Sprintf(":%d", 9001+i))
		if err != nil {
			log.Fatal(err)
		}
		listeners = append(listeners, l)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, os.Interrupt)
	defer signal.Stop(signals)
	var wg sync.WaitGroup
	for i, s := range servers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Serve(listeners[i]); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Print(err)
			}
		}()
	}
	fmt.Println("READY hoe/websocket=9001 gorilla-over-hoe=9002 gorilla-over-std=9003")
	<-signals
	for _, s := range servers {
		if err := s.Close(); err != nil {
			log.Print(err)
		}
	}
	wg.Wait()
}
