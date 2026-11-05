package compat

import (
	"bytes"
	"fmt"
	"testing"
)

const (
	imfDate    = "Sun, 06 Nov 1994 08:49:37 GMT"
	imfDateAlt = "Mon, 02 Jan 2006 15:04:05 GMT"
)

func httpResponse(headers, body string) string {
	return "HTTP/1.1 200 OK\r\n" + headers + "\r\n" + body
}

func withLength(body string) string {
	return fmt.Sprintf("Content-Length: %d\r\n", len(body))
}

func TestNormalizeKeepsMalformedDateHeader(t *testing.T) {
	in := []byte(httpResponse("Date: not-a-date\r\nContent-Length: 0\r\n", ""))
	if got := normalize(in); !bytes.Equal(got, in) {
		t.Fatalf("malformed Date header was rewritten\nwant %q\n got %q", in, got)
	}
}

func TestNormalizeRewritesValidDateHeader(t *testing.T) {
	in := []byte(httpResponse("Date: "+imfDate+"\r\nContent-Length: 0\r\n", ""))
	want := []byte(httpResponse("Date: DATE\r\nContent-Length: 0\r\n", ""))
	if got := normalize(in); !bytes.Equal(got, want) {
		t.Fatalf("valid Date header not normalized\nwant %q\n got %q", want, got)
	}
}

func TestNormalizeLeavesDateLineInBody(t *testing.T) {
	body := "Date: " + imfDateAlt + "\r\n"
	header := "Date: " + imfDate + "\r\n"

	t.Run("content_length", func(t *testing.T) {
		in := []byte(httpResponse(header+withLength(body), body))
		want := []byte(httpResponse("Date: DATE\r\n"+withLength(body), body))
		if got := normalize(in); !bytes.Equal(got, want) {
			t.Fatalf("body Date line altered\nwant %q\n got %q", want, got)
		}
	})

	t.Run("chunked", func(t *testing.T) {
		chunk := fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(body), body)
		pipeline := httpResponse("Date: "+imfDateAlt+"\r\nContent-Length: 0\r\n", "")
		in := []byte(httpResponse(header+"Transfer-Encoding: chunked\r\n", chunk) + pipeline)
		want := []byte(httpResponse("Date: DATE\r\nTransfer-Encoding: chunked\r\n", chunk) +
			httpResponse("Date: DATE\r\nContent-Length: 0\r\n", ""))
		if got := normalize(in); !bytes.Equal(got, want) {
			t.Fatalf("chunked body or following response mishandled\nwant %q\n got %q", want, got)
		}
	})

	t.Run("close_delimited_last", func(t *testing.T) {
		in := []byte(httpResponse(header, body))
		want := []byte(httpResponse("Date: DATE\r\n", body))
		if got := normalize(in); !bytes.Equal(got, want) {
			t.Fatalf("close-delimited body or header mishandled\nwant %q\n got %q", want, got)
		}
	})
}

func TestNormalizePipelinedDates(t *testing.T) {
	first := httpResponse("Date: "+imfDate+"\r\n"+withLength("one"), "one")
	second := httpResponse("Date: "+imfDateAlt+"\r\n"+withLength("two"), "two")
	third := httpResponse("Date: not-a-date\r\nContent-Length: 0\r\n", "")
	in := []byte(first + second + third)
	want := []byte(
		httpResponse("Date: DATE\r\n"+withLength("one"), "one") +
			httpResponse("Date: DATE\r\n"+withLength("two"), "two") +
			third)
	got := normalize(in)
	if !bytes.Equal(got, want) {
		t.Fatalf("pipeline Date normalization differs\nwant %q\n got %q", want, got)
	}
	if n := bytes.Count(got, []byte("Date: DATE")); n != 2 {
		t.Fatalf("placeholder count=%d want=2", n)
	}
}

func TestNormalizeCollapsesGenuineDateDifference(t *testing.T) {
	a := []byte(httpResponse("Date: "+imfDate+"\r\nContent-Length: 0\r\n", ""))
	b := []byte(httpResponse("Date: "+imfDateAlt+"\r\nContent-Length: 0\r\n", ""))
	na, nb := normalize(a), normalize(b)
	if !bytes.Equal(na, nb) {
		t.Fatalf("genuine date difference not normalized\na=%q\nb=%q", na, nb)
	}
	if !bytes.Contains(na, []byte("Date: DATE\r\n")) {
		t.Fatalf("placeholder missing from %q", na)
	}
}
