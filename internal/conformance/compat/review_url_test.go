package compat

import (
	"bytes"
	"fmt"
	"net/http"
	"testing"
	"time"
)

func TestReviewDifferentialURLFields(t *testing.T) {
	for _, target := range []string{"/a[b]", `/a\b`, "/a%5Bb%5D", "/a%5Cb", "/a%2Fb?q=1", "/ordinary?", "/ordinary?q=[x]\\y"} {
		t.Run(target, func(t *testing.T) {
			run := func(own bool) ([]byte, []byte) {
				// Given: subscribe before sending a real raw TCP request.
				fields := make(chan []byte, 1)
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					snapshot := fieldSnapshot(r)
					fields <- snapshot
					fmt.Fprintf(w, "Path=%q RawPath=%q RawQuery=%q ForceQuery=%t URI=%q",
						r.URL.Path, r.URL.RawPath, r.URL.RawQuery, r.URL.ForceQuery, r.RequestURI)
				})
				addr, _ := launch(t, own, &http.Server{Handler: h})
				// When
				wire := exerciseWire(t, addr, wireCase{raw: request("GET", target, true), status: 200})
				// Then
				select {
				case snapshot := <-fields:
					return wire, snapshot
				case <-time.After(5 * time.Second):
					t.Fatal("handler snapshot timeout")
					return nil, nil
				}
			}
			wantWire, wantFields := run(false)
			gotWire, gotFields := run(true)
			if !bytes.Equal(wantFields, gotFields) {
				t.Errorf("handler fields DIFF\nstd %s\nhoe %s", wantFields, gotFields)
			}
			same(t, wantWire, gotWire)
		})
	}
}

func TestReviewDifferentialRetainedURL(t *testing.T) {
	for _, target := range []string{"/a[b]", `/a\b`, "/a%2Fb", "/ordinary?q=1"} {
		t.Run(target, func(t *testing.T) {
			run := func(own bool) []byte {
				// Given: retain the first request across a later pipeline request.
				var kept *http.Request
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if kept == nil {
						kept = r
						fmt.Fprint(w, "retained")
						return
					}
					fmt.Fprintf(w, "URL=%+v URI=%q Host=%q Header=%q",
						*kept.URL, kept.RequestURI, kept.Host, kept.Header.Get("X-Kept"))
				})
				addr, _ := launch(t, own, &http.Server{Handler: h})
				raw := fmt.Sprintf("GET %s HTTP/1.1\r\nHost: retained\r\nX-Kept: original\r\n\r\n", target)
				raw += request("GET", "/different", true)
				// When: sequential handlers on one persistent connection.
				return exerciseWire(t, addr, wireCase{raw: raw, status: 200})
			}
			// Then: both retained handler fields and both raw responses agree.
			same(t, run(false), run(true))
		})
	}
}
