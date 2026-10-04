//go:build linux || darwin

package bench

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe"
	"github.com/DevNewbie1826/hoe/internal/conn"
)

type pipelineArm struct {
	delay  time.Duration
	start  time.Time
	expire func()
}

type pipelinePush struct {
	expired  bool
	err      error
	held     bool
	probeErr error
}

type pipelineRig struct {
	server *hoe.Server
	client net.Conn
	reader *bufio.Reader
	raw    *conn.Conn
	arms   chan pipelineArm
	pushes chan pipelinePush
}

type pipelineConfig struct {
	delay      time.Duration
	controlled bool
	handler    http.Handler
	expiryGate func()
	expiryDone func()
}

type pipelineConnKey struct{}

func pipelineTransport(r *http.Request) *conn.Conn {
	return r.Context().Value(pipelineConnKey{}).(*conn.Conn)
}

func pipelineServer(t *testing.T, cfg pipelineConfig) *pipelineRig {
	t.Helper()
	rig := &pipelineRig{arms: make(chan pipelineArm, 64), pushes: make(chan pipelinePush, 64)}
	opened := make(chan *conn.Conn, 1)
	s := &hoe.Server{Handler: cfg.handler, PipelineFlushDelay: cfg.delay, ErrorLog: log.New(io.Discard, "", 0)}
	s.ConnContext = func(ctx context.Context, nc net.Conn) context.Context {
		c := nc.(*conn.Conn)
		c.SetPipelineHooks(conn.PipelineHooks{
			BeforeExpiry: cfg.expiryGate,
			AfterExpiry:  cfg.expiryDone,
			Timer: func(delay time.Duration, expire func()) bool {
				rig.arms <- pipelineArm{delay, time.Now(), expire}
				return cfg.controlled
			},
			Pushed: func(expired bool, err error) {
				held, probeErr := c.CorkEnabled()
				rig.pushes <- pipelinePush{expired, err, held, probeErr}
			},
		})
		opened <- c
		return context.WithValue(ctx, pipelineConnKey{}, c)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rig.server = s
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() {
		if rig.client != nil {
			rig.client.Close()
		}
		if err := s.Close(); err != nil {
			t.Error(err)
		}
		if err := pipelineEvent(t, done); !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("Serve: %v", err)
		}
		t.Log("cleanup: TCP client/server closed; Serve joined; test channels released")
	})
	rig.client, err = net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	rig.client.SetDeadline(time.Now().Add(3 * time.Second))
	rig.reader = bufio.NewReader(rig.client)
	rig.raw = pipelineEvent(t, opened)
	return rig
}

func pipelineReleased(t *testing.T, rig *pipelineRig) pipelinePush {
	t.Helper()
	event := pipelineEvent(t, rig.pushes)
	if event.err != nil || event.held || event.probeErr != nil {
		t.Fatalf("push did not clear kernel hold: %+v", event)
	}
	return event
}

func pipelineEvent[T any](t *testing.T, events <-chan T) T {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(time.Second):
		t.Fatal("pipeline event timeout")
		var zero T
		return zero
	}
}

func pipelineProbe(t *testing.T, c *conn.Conn, want bool) {
	t.Helper()
	got, err := c.CorkEnabled()
	if err != nil || got != want {
		t.Errorf("kernel hold=%v err=%v want=%v", got, err, want)
	}
}

func pipelineSend(t *testing.T, c net.Conn, paths ...string) {
	t.Helper()
	request := ""
	for _, path := range paths {
		request += "GET " + path + " HTTP/1.1\r\nHost: t\r\n\r\n"
	}
	t.Logf("TCP Write(%q)", request)
	if _, err := io.WriteString(c, request); err != nil {
		t.Fatal(err)
	}
}

func pipelineReceipt(t *testing.T, rig *pipelineRig, body string) {
	t.Helper()
	r, err := http.ReadResponse(rig.reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, err := io.ReadAll(r.Body)
	r.Body.Close()
	if err != nil || string(p) != body || r.StatusCode != 200 {
		t.Fatalf("HTTP receipt status=%d body=%q err=%v want=%q", r.StatusCode, p, err, body)
	}
	t.Logf("HTTP/1.1 %s; headers=%v; body=%q", r.Status, r.Header, p)
}
