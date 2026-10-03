//go:build linux || darwin

package compat

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	engineconn "github.com/DevNewbie1826/hoe/internal/conn"
)

func TestBodyContextBeforeReadOnPeerHalfClose(t *testing.T) {
	run := func(own bool) []byte {
		entered, readBody := make(chan struct{}), make(chan struct{})
		addr, _ := launch(t, own, &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(entered)
			<-readBody
			before := r.Context().Err()
			p, err := io.ReadAll(r.Body)
			fmt.Fprintf(w, "before=%v body=%s read=%v", before, p, err)
		})})
		c := dial(t, addr)
		io.WriteString(c, "POST / HTTP/1.1\r\nHost: t\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc")
		signal(t, entered)
		if err := c.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
			close(readBody)
			t.Fatal(err)
		}
		if own {
			// The internal hook proves FIN was observed while body data was
			// still unread; this would fail under eager reactor cancellation.
			timeout := time.NewTimer(5 * time.Second)
			defer timeout.Stop()
			for {
				event := engineconn.WatchResources()
				if engineconn.SnapshotResources().PeerEOF > 0 {
					break
				}
				select {
				case <-event:
				case <-timeout.C:
					close(readBody)
					t.Fatal("FIN event not observed")
				}
			}
		}
		close(readBody)
		p := readResponse(t, bufio.NewReader(c), "POST")
		if string(p) != "before=<nil> body=abc read=<nil>" {
			t.Fatalf("unread-body context=%q", p)
		}
		return p
	}
	same(t, run(false), run(true))
}
