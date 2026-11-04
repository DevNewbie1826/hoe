package hoe

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/conn"
	"github.com/DevNewbie1826/hoe/internal/loop"
)

func TestConfigurationAfterLifecycleSetup(t *testing.T) {
	// Given a server with shutdown hooks and keep-alive settings registered
	// before its handler is configured, as permitted by net/http.
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "old") })}
	s.RegisterOnShutdown(func() {})
	s.SetKeepAlivesEnabled(false)
	s.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "configured") })
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		s.Close()
		l.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server leaked")
		}
	})
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	// When a real client reaches the serving path.
	r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	// Then the latest pre-Serve configuration is used.
	if string(p) != "configured" || !r.Close {
		t.Fatalf("body=%q close=%t", p, r.Close)
	}
}

func TestFromStdPreservesLifecycle(t *testing.T) {
	std := &http.Server{}
	shutdown := make(chan struct{})
	std.RegisterOnShutdown(func() { close(shutdown) })
	s := FromStd(std)
	if s.fallback() != std {
		t.Fatal("standard server identity lost")
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-shutdown:
	case <-time.After(5 * time.Second):
		t.Fatal("registered callback did not run")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := s.Serve(l); !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Shutdown: %v", err)
	}
}

func TestEngineInUse(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "engine") })}
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	defer func() { s.Close(); l.Close(); <-done }()
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()
	r, err := (&http.Client{Transport: tr, Timeout: 5 * time.Second}).Get("http://" + l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || string(p) != "engine" {
		t.Fatalf("body=%q err=%v", p, err)
	}
	if s.engineKind() == "std" {
		t.Fatal("plaintext TCP still delegates to net/http; N3 engine required")
	}
}

// goroutineFrames counts frames matching substr across every goroutine's
// stack; it is the census primitive for the shared-engine proofs.
func goroutineFrames(substr string) int {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return strings.Count(string(buf[:n]), substr)
		}
		buf = make([]byte, 2*len(buf))
	}
}

// waitUntil polls cond under a deadline. Goroutine start and exit expose no
// event to subscribe to, so a bounded condition poll is the deterministic
// wait for census counts.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

func listenTCP(t *testing.T) *net.TCPListener {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// postEcho POSTs body to addr through tr and returns the response body.
func postEcho(t *testing.T, tr *http.Transport, addr, body string) string {
	t.Helper()
	client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
	resp, err := client.Post("http://"+addr, "text/plain", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST http://%s status=%d", addr, resp.StatusCode)
	}
	p, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(p)
}

// writeAll sends one raw HTTP request head.
func writeAll(t *testing.T, c net.Conn, req string) {
	t.Helper()
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
}

// readResponseBody reads exactly one response body from a raw connection.
func readResponseBody(t *testing.T, c net.Conn) string {
	t.Helper()
	r, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	p, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(p)
}

func TestServeAll(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	// Given GOMAXPROCS pinned to DefaultLoops, so ServeAll's GOMAXPROCS-sized
	// shared group and Serve's DefaultLoops group hold the same census.
	prev := runtime.GOMAXPROCS(loop.DefaultLoops())
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
	const payload = "serveall echo"
	echo := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
			return
		}
		w.Write(p)
	})
	tr := &http.Transport{}
	defer tr.CloseIdleConnections()

	// Given a one-listener Serve engine as the census comparison shape.
	one := listenTCP(t)
	s1 := &Server{Handler: echo}
	d1 := make(chan error, 1)
	go func() { d1 <- s1.Serve(one) }()
	if got := postEcho(t, tr, one.Addr().String(), payload); got != payload {
		t.Fatalf("one-listener body=%q want %q", got, payload)
	}
	waitUntil(t, "one-listener reactor census", func() bool {
		return goroutineFrames("internal/loop.(*reactor).run(") == loop.DefaultLoops()
	})
	single := goroutineFrames("internal/loop.(*reactor).run(")
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-d1; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("Serve after Close: %v", err)
	}
	waitUntil(t, "one-listener engine release", func() bool {
		return conn.SnapshotResources().Engines == 0
	})

	// When three listeners share one ServeAll engine.
	ls := make([]net.Listener, 3)
	addrs := make([]string, len(ls))
	for i := range ls {
		ls[i] = listenTCP(t)
		addrs[i] = ls[i].Addr().String()
	}
	s := &Server{Handler: echo}
	d := make(chan error, 1)
	go func() { d <- s.ServeAll(ls...) }()
	for i, addr := range addrs {
		if got := postEcho(t, tr, addr, payload); got != payload {
			t.Fatalf("listener %d body=%q want %q", i, got, payload)
		}
	}

	// Then one engine serves every listener, with the one-listener reactor
	// census, one deadline wheel, and one finalizer.
	if engines := conn.SnapshotResources().Engines; engines != 1 {
		t.Fatalf("engines=%d want 1", engines)
	}
	waitUntil(t, "ServeAll reactor census", func() bool {
		return goroutineFrames("internal/loop.(*reactor).run(") == single
	})
	if wheels := goroutineFrames("internal/conn.(*deadlineWheel).run("); wheels != 1 {
		t.Fatalf("deadline wheels=%d want 1", wheels)
	}
	if finalizers := goroutineFrames("internal/conn.(*Engine).finalize("); finalizers != 1 {
		t.Fatalf("engine finalizers=%d want 1", finalizers)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-d; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ServeAll after Close: %v", err)
	}
}

func TestServeAllShutdown(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	// Given two listeners served through one shared engine, one connection
	// held active inside its handler and one parked idle after a request.
	ls := []net.Listener{listenTCP(t), listenTCP(t)}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hold" {
			once.Do(func() { close(entered) })
			<-release
		}
		fmt.Fprint(w, r.URL.Path)
	})}
	d := make(chan error, 1)
	go func() { d <- s.ServeAll(ls...) }()

	idle, err := net.Dial("tcp", ls[0].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	writeAll(t, idle, "GET /idle HTTP/1.1\r\nHost: t\r\n\r\n")
	if got := readResponseBody(t, idle); got != "/idle" {
		t.Fatalf("idle warmup body=%q", got)
	}
	active, err := net.Dial("tcp", ls[1].Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer active.Close()
	writeAll(t, active, "GET /hold HTTP/1.1\r\nHost: t\r\n\r\n")
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("held request never reached its handler")
	}

	// When Shutdown runs while one request is active on another listener.
	shutdownErr := make(chan error, 1)
	go func() { shutdownErr <- s.Shutdown(context.Background()) }()

	// Then the idle connection closes while the held request stays live,
	// finishes, and only then does ServeAll return ErrServerClosed.
	if err := idle.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := idle.Read(make([]byte, 1)); err != io.EOF {
		t.Fatalf("idle connection read after Shutdown: %v, want EOF", err)
	}
	close(release)
	if got := readResponseBody(t, active); got != "/hold" {
		t.Fatalf("held request body=%q", got)
	}
	if err := <-shutdownErr; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := <-d; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ServeAll after Shutdown: %v", err)
	}
	waitUntil(t, "engine release", func() bool { return conn.SnapshotResources().Engines == 0 })
}

func TestServeAllClose(t *testing.T) {
	if !conn.Supported() {
		t.Skip("documented standard-library fallback platform")
	}
	// Given three listeners sharing one engine, each with a keep-alive
	// connection parked idle after one request.
	ls := make([]net.Listener, 3)
	for i := range ls {
		ls[i] = listenTCP(t)
	}
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})}
	before := runtime.NumGoroutine()
	d := make(chan error, 1)
	go func() { d <- s.ServeAll(ls...) }()
	var idle []net.Conn
	defer func() {
		for _, c := range idle {
			c.Close()
		}
	}()
	for _, l := range ls {
		c, err := net.Dial("tcp", l.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		idle = append(idle, c)
		writeAll(t, c, "GET / HTTP/1.1\r\nHost: t\r\n\r\n")
		if got := readResponseBody(t, c); got != "ok" {
			t.Fatalf("warmup body=%q", got)
		}
	}
	waitUntil(t, "shared engine census", func() bool { return conn.SnapshotResources().Engines == 1 })

	// When Close runs with every connection idle.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-d; !errors.Is(err, ErrServerClosed) {
		t.Fatalf("ServeAll after Close: %v, want ErrServerClosed", err)
	}

	// Then every listener is freed, no engine goroutine remains behind
	// (reactor, wheel, finalizer, worker), and the goroutine count returns
	// to its pre-serve baseline.
	for _, l := range ls {
		if c, err := net.DialTimeout("tcp", l.Addr().String(), time.Second); err == nil {
			c.Close()
			t.Fatalf("listener %s still accepts after Close", l.Addr())
		}
	}
	waitUntil(t, "reactor release", func() bool { return goroutineFrames("internal/loop.(*reactor).run(") == 0 })
	waitUntil(t, "deadline wheel release", func() bool { return goroutineFrames("internal/conn.(*deadlineWheel).run(") == 0 })
	waitUntil(t, "finalizer release", func() bool { return goroutineFrames("internal/conn.(*Engine).finalize(") == 0 })
	waitUntil(t, "worker release", func() bool { return goroutineFrames("internal/sched.(*Pool).run(") == 0 })
	waitUntil(t, "engine registry release", func() bool { return conn.SnapshotResources().Engines == 0 })
	waitUntil(t, "goroutine baseline", func() bool { return runtime.NumGoroutine() <= before })
}

// A-1 regression (adversarial finding A-1): a listener whose acceptance
// ends immediately must not retire the shared engine while a later listener
// is still being reserved. The dead listener's accept loop ends at once; the
// live listener's BaseContext delays its own reservation past that point.
func TestServeAllSurvivesStartupListenerOrder(t *testing.T) {
	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	live, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()
	if err := dead.Close(); err != nil {
		t.Fatal(err)
	}

	var delayed atomic.Bool
	s := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		BaseContext: func(l net.Listener) context.Context {
			if l.Addr().String() == live.Addr().String() && !delayed.Swap(true) {
				time.Sleep(100 * time.Millisecond)
			}
			return context.Background()
		},
	}
	errc := make(chan error, 1)
	go func() { errc <- s.ServeAll(dead, live) }()

	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://" + live.Addr().String() + "/echo")
	if err != nil {
		t.Fatalf("live listener abandoned by the shared engine: %v", err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", res.StatusCode)
	}
	s.Close()
	// The dead listener's accept error is joined into ServeAll's return;
	// this test's contract is only that the live listener kept serving.
	<-errc
}

// A-2 regression (sweep row S / adversarial A-2): ServeAll closes every
// engine-eligible listener on every return path, like Serve.
func TestServeAllEarlyReturnClosesListeners(t *testing.T) {
	var s Server
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	// A unix listener is engine-ineligible and rides the net/http branch;
	// ServeAll must close it on the early return too, like net/http's Serve.
	l3, err := net.Listen("unix", t.TempDir()+"/s.sock")
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err := s.ServeAll(l1, l2, l3); err != ErrServerClosed {
		t.Fatalf("ServeAll = %v, want ErrServerClosed", err)
	}
	for i, l := range []net.Listener{l1, l2} {
		d := net.Dialer{Timeout: 500 * time.Millisecond}
		c, dialErr := d.Dial("tcp", l.Addr().String())
		if dialErr == nil {
			c.Close()
			t.Errorf("listener %d still accepts after ServeAll returned", i)
		}
	}
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	if c, dialErr := d.Dial("unix", l3.Addr().String()); dialErr == nil {
		c.Close()
		t.Error("unix listener still accepts after ServeAll returned")
	}
}

// A-3 regression (adversarial finding): one listener's accept-loop exit must
// not wait on a WaitGroup its siblings still add to — per-listener adoption
// tracking. Closing listeners under load used to panic with
// "sync: WaitGroup is reused before previous Wait has returned".
func TestServeAllListenerExitUnderSiblingAdoption(t *testing.T) {
	ls := make([]net.Listener, 0, 6)
	for range 6 {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls = append(ls, l)
	}
	s := &Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	errc := make(chan error, 1)
	go func() { errc <- s.ServeAll(ls...) }()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for _, l := range ls {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client := &http.Client{Timeout: time.Second}
			for {
				select {
				case <-stop:
					return
				default:
				}
				res, err := client.Get("http://" + addr + "/echo")
				if err != nil {
					return // listener closed: fine
				}
				res.Body.Close()
			}
		}(l.Addr().String())
	}
	// Close listeners one by one while the survivors keep accepting: each
	// closed listener's loop waits out its own adoptions while siblings add.
	for i := 0; i < 3; i++ {
		time.Sleep(20 * time.Millisecond)
		ls[i].Close()
	}
	time.Sleep(50 * time.Millisecond)
	s.Close()
	close(stop)
	wg.Wait()
	<-errc
}

// A-4 regression (adversarial finding): a listener parked inside BaseContext
// during reserve has no accept loop to publish acceptance-stop, so Shutdown
// must end it directly instead of blocking past its context.
func TestServeAllShutdownDuringBaseContext(t *testing.T) {
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	parked := make(chan struct{}, 1)
	s := &Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}),
		BaseContext: func(l net.Listener) context.Context {
			if l.Addr().String() == l2.Addr().String() {
				parked <- struct{}{}
				time.Sleep(3 * time.Second)
			}
			return context.Background()
		},
	}
	errc := make(chan error, 1)
	go func() { errc <- s.ServeAll(l1, l2) }()
	<-parked // l1 is serving; l2 is reserved but parked in BaseContext

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Shutdown(ctx) }()
	select {
	case <-done:
		// Shutdown returned while l2 was still parked: the fix works.
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown blocked past its context while a listener was parked in BaseContext")
	}
	select {
	case err := <-errc:
		if err != nil && !errors.Is(err, ErrServerClosed) {
			t.Fatalf("ServeAll = %v", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("ServeAll never returned after Shutdown")
	}
}
