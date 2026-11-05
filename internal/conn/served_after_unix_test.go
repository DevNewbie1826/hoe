//go:build linux || darwin

package conn

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"syscall"
	"testing"
	"time"
)

// servedAfterKey carries the listener's base-context marker.
type servedAfterKey struct{}

// servedAfterMarker is the base l1's listener installs. l2 installs a different
// one, so an assertion that sees this value proves the connection kept l1's
// base rather than picking up the surviving listener's.
const servedAfterMarker = "served-after-base-l1"

// servedAfterOther is l2's marker: the engine stays alive through it, and it
// must never appear on a connection that belongs to l1.
const servedAfterOther = "served-after-base-l2"

// TestServedAfterEndListenerKeepsBaseContext pins the served-after invariant
// inside the engine: once a listener has served, ending it must not cost a
// connection the listener's BaseContext. Two shapes run on one engine:
//
//   - a connection that already served a request keeps its base context for
//     the next keep-alive request after its listener ends, and
//   - a connection adopted from the retired listener after the end still
//     carries that listener's base context into its first request.
//
// Both observations come from the handler's request context and from
// ConnContext, so a base context replaced by context.Background() anywhere on
// the adopt -> OnOpen -> serve path fails the test.
func TestServedAfterEndListenerKeepsBaseContext(t *testing.T) {
	// Two listeners with distinct markers: the engine stays alive through l2,
	// so every value asserted below must come from l1's retired state.
	l1 := listenServedAfter(t)
	l2 := listenServedAfter(t)
	defer l2.Close()
	l2Addr := l2.Addr().String()

	var mu sync.Mutex
	connBases := make(map[net.Conn]string)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			v, _ := r.Context().Value(servedAfterKey{}).(string)
			fmt.Fprintf(w, "path=%s base=%s", r.URL.Path, v)
		}),
		BaseContext: func(l net.Listener) context.Context {
			marker := servedAfterMarker
			if l.Addr().String() == l2Addr {
				marker = servedAfterOther
			}
			return context.WithValue(context.Background(), servedAfterKey{}, marker)
		},
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			v, _ := ctx.Value(servedAfterKey{}).(string)
			mu.Lock()
			connBases[c] = v
			mu.Unlock()
			return ctx
		},
	}
	// One reactor for both connections: the guard must not depend on the host's
	// CPU count, and two connections on one reactor are the shape where a
	// reactor-local tag loss is visible.
	e, err := New(srv, Policy{KeepAlive: func() bool { return true }, Loops: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { e.Close() })

	// Reserve both listeners before either accept loop runs, then serve them:
	// these are ServeAll's own two steps (accept_unix.go:68-88), taken here so
	// the test knows both listeners are registered before l1 ends. Registration
	// order is the property under test, and reserving synchronously removes any
	// wait for an accept loop to publish state.
	st1, err := e.reserve(l1)
	if err != nil {
		t.Fatal(err)
	}
	st2, err := e.reserve(l2)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 2)
	go func() { served <- e.serveReserved(l1, st1) }()
	go func() { served <- e.serveReserved(l2, st2) }()

	// A connection that has already served one request on l1.
	c, err := net.Dial("tcp", l1.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	br := bufio.NewReader(c)
	writeServedAfterRequest(t, c, "/first")
	if got := readServedAfterBody(t, c, br); got != "path=/first base="+servedAfterMarker {
		t.Fatalf("first request: %q", got)
	}

	// End l1 while the engine lives on through l2, and wait for that listener's
	// accept loop to return so the retired state is the only source of its base.
	e.endListener(l1)
	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("l1 accept loop did not return after endListener")
	}

	// The same connection keeps its base context after its listener ended,
	// both for a second round and for two heads pipelined in one write.
	writeServedAfterRequest(t, c, "/after-end")
	if got := readServedAfterBody(t, c, br); got != "path=/after-end base="+servedAfterMarker {
		t.Fatalf("keep-alive request after listener end: %q", got)
	}
	// Both heads go out in a single write, so the second one is parsed from the
	// same read buffer the first came from: that is the path where a non-first
	// request could pick up a different parent context.
	if _, err := io.WriteString(c,
		"GET /pipeline-1 HTTP/1.1\r\nHost: t\r\n\r\n"+
			"GET /pipeline-2 HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/pipeline-1", "/pipeline-2"} {
		if got := readServedAfterBody(t, c, br); got != "path="+path+" base="+servedAfterMarker {
			t.Fatalf("pipelined request %s after listener end: %q", path, got)
		}
	}

	// One more request after the pipelined pair: a bind that resets the base at
	// the end of a round is only visible to the request that follows it.
	writeServedAfterRequest(t, c, "/after-pipeline")
	if got := readServedAfterBody(t, c, br); got != "path=/after-pipeline base="+servedAfterMarker {
		t.Fatalf("keep-alive request after the pipelined pair: %q", got)
	}

	// A connection adopted from the retired listener after its end keeps the
	// listener's base context.
	peer, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	live, err := net.DialTCP("tcp", nil, peer.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	srvConn, err := peer.AcceptTCP()
	if err != nil {
		live.Close()
		t.Fatal(err)
	}
	defer srvConn.Close()
	raw, err := live.SyscallConn()
	if err != nil {
		live.Close()
		t.Fatal(err)
	}
	fd := -1
	var dupErr error
	if err := raw.Control(func(s uintptr) { fd, dupErr = syscall.Dup(int(s)) }); err != nil {
		live.Close()
		t.Fatal(err)
	}
	live.Close()
	if dupErr != nil {
		t.Fatal(dupErr)
	}
	// adopt owns fd from here on: it closes the descriptor itself on success and
	// on an enqueue error, so the test must not close it again.
	if err := e.adopt(fd, l1); err != nil {
		t.Fatalf("adopt after listener end: %v", err)
	}
	writeServedAfterRequest(t, srvConn, "/adopted-after-end")
	if got := readServedAfterBody(t, srvConn, bufio.NewReader(srvConn)); got != "path=/adopted-after-end base="+servedAfterMarker {
		t.Fatalf("adopted-after-end request: %q", got)
	}

	mu.Lock()
	bases := make(map[net.Conn]string, len(connBases))
	for c, v := range connBases {
		bases[c] = v
	}
	mu.Unlock()
	// Two distinct connections, not two callbacks: a ConnContext that runs
	// twice for one connection while never seeing the adopted one must fail.
	if len(bases) != 2 {
		t.Fatalf("ConnContext observed %d distinct connections, want 2", len(bases))
	}
	for c, v := range bases {
		if v != servedAfterMarker {
			t.Fatalf("ConnContext observed base %q for %s", v, c.RemoteAddr())
		}
	}
}

func listenServedAfter(t *testing.T) *net.TCPListener {
	t.Helper()
	l, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func writeServedAfterRequest(t *testing.T, c net.Conn, path string) {
	t.Helper()
	if _, err := io.WriteString(c, "GET "+path+" HTTP/1.1\r\nHost: t\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
}

func readServedAfterBody(t *testing.T, c net.Conn, br *bufio.Reader) string {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
