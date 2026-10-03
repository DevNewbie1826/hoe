package bench

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

type peerKey struct{}

func TestPeerAddressSnapshotAfterCallbacks(t *testing.T) {
	for _, stage := range []string{"ConnContext", "StateNew", "after_first_handler"} {
		t.Run(stage, func(t *testing.T) {
			for _, own := range []bool{false, true} {
				// Given: callbacks change the address before std snapshots it.
				mutate := func(c net.Conn) {
					addr := c.RemoteAddr().(*net.TCPAddr)
					addr.IP, addr.Port = net.IPv4(127, 0, 0, 2), 54321
				}
				calls := 0
				cfg := &http.Server{
					ConnContext: func(ctx context.Context, c net.Conn) context.Context {
						if stage == "ConnContext" {
							mutate(c)
						}
						return context.WithValue(ctx, peerKey{}, c)
					},
					ConnState: func(c net.Conn, state http.ConnState) {
						if state == http.StateNew && stage != "ConnContext" {
							mutate(c)
						}
					},
					Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls++
						if calls == 1 && stage == "after_first_handler" {
							r.Context().Value(peerKey{}).(net.Conn).RemoteAddr().(*net.TCPAddr).Port++
						}
						io.WriteString(w, r.RemoteAddr)
					}),
				}
				addr, _ := launchConfig(t, own, cfg)
				c, err := net.DialTimeout("tcp", addr, 5*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				c.SetDeadline(time.Now().Add(5 * time.Second))
				in := bufio.NewReader(c)
				// When: two requests reuse the same real TCP lifetime.
				if _, err := io.WriteString(c, get+get); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					r, err := http.ReadResponse(in, nil)
					if err != nil {
						t.Fatal(err)
					}
					p, err := io.ReadAll(r.Body)
					r.Body.Close()
					// Then: the post-callback snapshot remains stable like std.
					if err != nil || string(p) != "127.0.0.2:54321" {
						t.Fatalf("hoe=%t stage=%s peer=%q error=%v", own, stage, p, err)
					}
				}
			}
		})
	}
}
