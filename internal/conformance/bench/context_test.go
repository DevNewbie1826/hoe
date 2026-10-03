package bench

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/DevNewbie1826/hoe/internal/http1"
)

func TestRequestContextCancellationOrder(t *testing.T) {
	for _, order := range []string{"request_first", "parent_first", "expired_parent", "observed_before_cancel"} {
		t.Run(order, func(t *testing.T) {
			// Given: the same cancellable base for std and a request-block context.
			parent, parentCancel := context.WithCancelCause(context.Background())
			defer parentCancel(nil)
			var base context.Context = parent
			if order == "expired_parent" {
				var cancel context.CancelFunc
				base, cancel = context.WithDeadline(parent, time.Now().Add(-time.Second))
				defer cancel()
			}
			std, cancelStd := context.WithCancel(base)
			defer cancelStd()
			r, err := http1.ParseHead([]byte(get), nil)
			if err != nil {
				t.Fatal(err)
			}
			own := http1.BindContext(r, base)
			cause := errors.New("parent cause")
			if order == "observed_before_cancel" {
				_ = r.Context().Done()
				_ = r.Context().Value("irrelevant")
			}
			// When: cancellation occurs in either order, even before observation.
			if order == "parent_first" || order == "expired_parent" {
				parentCancel(cause)
			}
			cancelStd()
			own.Cancel()
			parentCancel(cause)
			// Then: retained contexts preserve Err, Cause, Deadline and callbacks.
			if r.Context().Err() != std.Err() || context.Cause(r.Context()) != context.Cause(std) {
				t.Fatalf("Err/Cause std=%v/%v hoe=%v/%v", std.Err(), context.Cause(std), r.Context().Err(), context.Cause(r.Context()))
			}
			ownDeadline, ownOK := r.Context().Deadline()
			stdDeadline, stdOK := std.Deadline()
			if ownOK != stdOK || !ownDeadline.Equal(stdDeadline) {
				t.Fatal("deadline differs from std")
			}
			callback := make(chan struct{})
			stop := context.AfterFunc(r.Context(), func() { close(callback) })
			defer stop()
			select {
			case <-callback:
			case <-time.After(5 * time.Second):
				t.Fatal("AfterFunc did not observe cancellation")
			}
			child, cancel := context.WithCancel(r.Context())
			defer cancel()
			select {
			case <-child.Done():
			default:
				t.Fatal("derived context missed cancellation")
			}
		})
	}
}

func TestRetainedRequestContext(t *testing.T) {
	// Given: the handler retains requests without observing their context.
	retained := make(chan *http.Request, 2)
	addr, _ := launch(t, true, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		retained <- r
		io.WriteString(w, hello)
	}))
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	in := bufio.NewReader(c)
	// When: both keep-alive requests have returned before context observation.
	for range 2 {
		if _, err := io.WriteString(c, get); err != nil {
			t.Fatal(err)
		}
		if err := readHello(in); err != nil {
			t.Fatal(err)
		}
	}
	first, second := <-retained, <-retained
	// Then: independent request contexts are cancelled and remain observable.
	if first == second || first.Context() == second.Context() {
		t.Fatal("handler-retainable request/context reused")
	}
	for _, r := range []*http.Request{first, second} {
		if r.Context().Err() != context.Canceled || context.Cause(r.Context()) != context.Canceled {
			t.Fatal("retained cancellation differs from std")
		}
		select {
		case <-r.Context().Done():
		default:
			t.Fatal("retained Done channel is open")
		}
	}
}
