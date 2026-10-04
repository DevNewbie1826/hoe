package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/DevNewbie1826/hoe"
	"github.com/gorilla/websocket"
)

type server interface {
	Serve(net.Listener) error
	Close() error
	Shutdown(context.Context) error
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	impl := flag.String("impl", "hoe", "std or hoe")
	addr := flag.String("addr", "127.0.0.1:0", "HTTP listen address")
	track := flag.Bool("track-states", false, "track idle connections for resource measurement")
	profile := flag.String("cpuprofile", "", "optional server CPU profile")
	mutex := flag.String("mutexprofile", "", "optional server mutex profile")
	flag.Parse()
	if *impl != "std" && *impl != "hoe" {
		return fmt.Errorf("invalid implementation %q", *impl)
	}
	if *profile != "" {
		f, err := os.Create(*profile)
		if err != nil {
			return err
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	if *mutex != "" {
		runtime.SetMutexProfileFraction(1)
		defer func() {
			f, err := os.Create(*mutex)
			if err != nil {
				log.Print(err)
				return
			}
			defer f.Close()
			if err := pprof.Lookup("mutex").WriteTo(f, 0); err != nil {
				log.Print(err)
			}
		}()
	}
	var requests atomic.Uint64
	var stateMu sync.Mutex
	idle := 0
	states := make(map[net.Conn]http.ConnState)
	var changed chan struct{}
	state := func(c net.Conn, next http.ConnState) {
		if !*track {
			return
		}
		stateMu.Lock()
		if states[c] == http.StateIdle {
			idle--
		}
		if next == http.StateClosed || next == http.StateHijacked {
			delete(states, c)
		} else {
			states[c] = next
		}
		if next == http.StateIdle {
			idle++
		}
		if changed != nil {
			close(changed)
			changed = nil
		}
		stateMu.Unlock()
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		io.WriteString(w, "Hello Benchmark")
	})
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
	var s server = &http.Server{Handler: mux, ConnState: state}
	if *impl == "hoe" {
		s = &hoe.Server{Handler: mux, ConnState: state}
	}
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	defer l.Close()
	adminListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer adminListener.Close()
	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		var want int
		fmt.Sscanf(r.URL.Query().Get("idle"), "%d", &want)
		for {
			stateMu.Lock()
			current := idle
			if current >= want {
				stateMu.Unlock()
				var mem runtime.MemStats
				runtime.ReadMemStats(&mem)
				json.NewEncoder(w).Encode(struct {
					Mallocs    uint64
					Requests   uint64
					Goroutines int
					Idle       int
				}{mem.Mallocs, requests.Load(), runtime.NumGoroutine(), current})
				return
			}
			if changed == nil {
				changed = make(chan struct{})
			}
			event := changed
			stateMu.Unlock()
			select {
			case <-event:
			case <-r.Context().Done():
				return
			}
		}
	})
	admin := &http.Server{Handler: adminMux}
	served, administered := make(chan error, 1), make(chan error, 1)
	go func() { served <- s.Serve(l) }()
	go func() { administered <- admin.Serve(adminListener) }()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)
	if err := json.NewEncoder(os.Stdout).Encode(struct {
		Address, Admin string
		PID            int
	}{l.Addr().String(), adminListener.Addr().String(), os.Getpid()}); err != nil {
		s.Close()
		admin.Close()
		<-served
		<-administered
		return err
	}
	<-stop
	closeErr := s.Close()
	adminErr := admin.Close()
	serveErr, adminServeErr := <-served, <-administered
	if serveErr != http.ErrServerClosed || adminServeErr != http.ErrServerClosed {
		return fmt.Errorf("HTTP Serve=%v admin Serve=%v", serveErr, adminServeErr)
	}
	if closeErr != nil {
		return closeErr
	}
	return adminErr
}
