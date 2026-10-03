package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	"github.com/DevNewbie1826/hoe"
	"github.com/gin-gonic/gin"
	"github.com/go-chi/chi/v5"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:18080", "listening address")
	flag.Parse()
	mux := http.NewServeMux()
	c := chi.NewRouter()
	c.Get("/users/{id}", func(w http.ResponseWriter, r *http.Request) { fmt.Fprintf(w, "id=%s", chi.URLParam(r, "id")) })
	mux.Handle("/chi/", http.StripPrefix("/chi", c))
	gin.SetMode(gin.ReleaseMode)
	g := gin.New()
	g.GET("/users/:id", func(c *gin.Context) { c.String(200, "id=%s", c.Param("id")) })
	mux.Handle("/gin/", http.StripPrefix("/gin", g))
	e := echo.New()
	e.GET("/users/:id", func(c echo.Context) error { return c.String(200, "id="+c.Param("id")) })
	mux.Handle("/echo/", http.StripPrefix("/echo", e))
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.Dir("."))))
	upgrader := websocket.Upgrader{}
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			kind, p, err := c.ReadMessage()
			if err != nil {
				return
			}
			if err := c.WriteMessage(kind, p); err != nil {
				return
			}
		}
	})
	log.Fatal(hoe.ListenAndServe(*addr, mux))
}
