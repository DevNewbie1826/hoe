package http1

import (
	"bufio"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestReviewURLFields(t *testing.T) {
	targets := []string{"/a[b]", `/a\b`, "/a%5Bb%5D", "/a%5Cb", "/a%2Fb?q=1", "/ordinary?", "/ordinary?q=[x]\\y"}
	// Cover the whole printable path boundary, not just two examples of it.
	for c := byte('!'); c < 127; c++ {
		if c != '%' && c != '#' && c != '?' {
			targets = append(targets, "/a"+string(c)+"b")
		}
	}
	for _, target := range targets {
		t.Run(target, func(t *testing.T) {
			// Given: the same raw request for both public parsers.
			raw := "GET " + target + " HTTP/1.1\r\nHost: t\r\n\r\n"
			want, err := http.ReadRequest(bufio.NewReader(strings.NewReader(raw)))
			if err != nil {
				t.Fatal(err)
			}
			// When
			got, err := ParseHead([]byte(raw), nil)
			if err != nil {
				t.Fatal(err)
			}
			// Then: every URL field and serialization agree.
			if !reflect.DeepEqual(got.URL, want.URL) || got.URL.String() != want.URL.String() {
				t.Fatalf("URL DIFF target=%q\nstd %+v\nhoe %+v", target, *want.URL, *got.URL)
			}
		})
	}
}

func TestReviewURLRetainsOwnedHead(t *testing.T) {
	for _, target := range []string{"/a[b]", `/a\b`, "/a%2Fb", "/ordinary?q=1"} {
		t.Run(target, func(t *testing.T) {
			// Given
			head := []byte(fmt.Sprintf("GET %s HTTP/1.1\r\nHost: retained\r\nX-Kept: original\r\n\r\n", target))
			want, err := http.ReadRequest(bufio.NewReader(strings.NewReader(string(head))))
			if err != nil {
				t.Fatal(err)
			}
			got, err := ParseHead(head, nil)
			if err != nil {
				t.Fatal(err)
			}
			// When: the caller reuses its transport storage.
			clear(head)
			// Then
			if !reflect.DeepEqual(got.URL, want.URL) || got.RequestURI != want.RequestURI ||
				got.Host != want.Host || !reflect.DeepEqual(got.Header, want.Header) {
				t.Fatalf("retained request DIFF: std=%+v hoe=%+v", want, got)
			}
		})
	}
}

func TestReviewOrdinaryTargetsStayFast(t *testing.T) {
	for _, target := range []string{"/", "/ordinary?", "/a/b;$&+,=:@-_.~?q=1", "/ordinary?q=[x]\\y", "*"} {
		t.Run(target, func(t *testing.T) {
			// Given
			raw := "GET " + target + " HTTP/1.1\r\nHost: t\r\n\r\n"
			// When
			got, err := ParseHead([]byte(raw), nil)
			if err != nil {
				t.Fatal(err)
			}
			// Then: the ordinary request still uses its immutable head block.
			ctx, ok := got.Context().(*requestContext)
			if !ok || ctx.head != raw {
				t.Fatalf("ordinary target left fast path: %q", target)
			}
		})
	}
}
