package compat

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func TestReviewMaxBytesPipeline(t *testing.T) {
	for _, mode := range []string{"cloned", "assigned", "local", "plain"} {
		for _, n := range []int{8, 9} {
			t.Run(fmt.Sprintf("%s/%d", mode, n), func(t *testing.T) {
				// Given: only the cloned middleware replaces a copied Request.
				h := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/next" {
						fmt.Fprint(w, "next")
						return
					}
					reader := r.Body
					switch mode {
					case "assigned":
						r.Body = http.MaxBytesReader(w, r.Body, 8)
						reader = r.Body
					case "local":
						reader = http.MaxBytesReader(w, r.Body, 8)
					case "cloned", "plain":
					default:
						t.Errorf("unexpected mode %q", mode)
						return
					}
					p, err := io.ReadAll(reader)
					fmt.Fprintf(w, "body=%q err=%v", p, err)
				}))
				if mode == "cloned" {
					h = http.MaxBytesHandler(h, 8)
				}
				stdAddr, _ := launch(t, false, &http.Server{Handler: h})
				ownAddr, _ := launch(t, true, &http.Server{Handler: h})

				// When: a boundary-sized POST precedes a pipelined GET.
				want := capture(t, stdAddr, maxRequest(n))
				got := capture(t, ownAddr, maxRequest(n))

				// Then: match framing, body bytes, close policy and reply count.
				type reply struct {
					body  string
					close bool
				}
				parse := func(raw []byte) []reply {
					in := bufio.NewReader(bytes.NewReader(raw))
					var replies []reply
					for {
						if _, err := in.Peek(1); err == io.EOF {
							return replies
						} else if err != nil {
							t.Fatal(err)
						}
						response, err := http.ReadResponse(in, nil)
						if err != nil {
							t.Fatal(err)
						}
						p, readErr := io.ReadAll(response.Body)
						closeErr := response.Body.Close()
						if readErr != nil || closeErr != nil {
							t.Fatalf("response body: read=%v close=%v", readErr, closeErr)
						}
						replies = append(replies, reply{string(p), response.Close})
					}
				}
				stdReplies, ownReplies := parse(want), parse(got)
				if n > 8 && (mode == "local" || mode == "cloned") {
					// Like TestMaxBytesLocalVariableException, the limiter is not
					// reachable from the engine-visible Request.Body. It cannot
					// invoke net/http's package-private requestTooLarge hook.
					if len(stdReplies) != 1 || len(ownReplies) != 2 {
						t.Fatalf("approved exception reply counts: std=%d hoe=%d", len(stdReplies), len(ownReplies))
					}
					if !stdReplies[0].close || ownReplies[0].close || !bytes.Contains(want, []byte("\r\nConnection: close\r\n")) {
						t.Fatalf("approved exception close policy: std=%q hoe=%q", want, got)
					}
					// Parse both responses to prove that unread body did not corrupt the next one.
					if stdReplies[0].body != `body="xxxxxxxx" err=http: request body too large` ||
						ownReplies[0].body != stdReplies[0].body || ownReplies[1].body != "next" {
						t.Fatalf("approved exception bodies: std=%+v hoe=%+v", stdReplies, ownReplies)
					}
					t.Logf("approved exception: std=%q hoe=%q", want, got)
					return
				}
				if !bytes.Equal(want, got) {
					t.Fatalf("DIFF\nstd %q\nhoe %q", want, got)
				}
				wantCount := 2
				if n > 8 && mode == "assigned" {
					wantCount = 1
				}
				if len(ownReplies) != wantCount {
					t.Fatalf("response count=%d want=%d", len(ownReplies), wantCount)
				}
				t.Logf("SAME %q", got)
			})
		}
	}
}
