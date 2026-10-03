package compat

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

type requestFields struct {
	Method                 string
	URL                    url.URL
	Proto                  string
	ProtoMajor, ProtoMinor int
	Header                 http.Header
	Host, RequestURI       string
	ContentLength          int64
	TransferEncoding       []string
	Close                  bool
	Trailer                http.Header
	TLSNil                 bool
	RemoteShape            string
	Form, PostForm         url.Values
	MultipartValues        map[string][]string
	MultipartFiles         map[string][]string
	Body, ReadError        string
}

func fieldSnapshot(r *http.Request) []byte {
	p, err := io.ReadAll(r.Body)
	f := requestFields{Method: r.Method, URL: *r.URL, Proto: r.Proto, ProtoMajor: r.ProtoMajor, ProtoMinor: r.ProtoMinor,
		Header: r.Header, Host: r.Host, RequestURI: r.RequestURI, ContentLength: r.ContentLength,
		TransferEncoding: r.TransferEncoding, Close: r.Close, Trailer: r.Trailer, TLSNil: r.TLS == nil, Body: string(p)}
	if err != nil {
		f.ReadError = err.Error()
	}
	ip, port, addrErr := net.SplitHostPort(r.RemoteAddr)
	n, portErr := strconv.Atoi(port)
	f.RemoteShape = fmt.Sprintf("tcp=%t loopback=%t port=%t", addrErr == nil, net.ParseIP(ip).IsLoopback(), portErr == nil && n > 0 && n <= 65535)
	r.Body = io.NopCloser(bytes.NewReader(p))
	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/") {
		if err := r.ParseMultipartForm(1024); err != nil {
			f.ReadError += "|" + err.Error()
		}
		if r.MultipartForm != nil {
			defer r.MultipartForm.RemoveAll()
			f.MultipartValues = r.MultipartForm.Value
			f.MultipartFiles = make(map[string][]string)
			for k, files := range r.MultipartForm.File {
				for _, file := range files {
					rd, err := file.Open()
					if err != nil {
						f.MultipartFiles[k] = append(f.MultipartFiles[k], err.Error())
						continue
					}
					content, readErr := io.ReadAll(rd)
					rd.Close()
					f.MultipartFiles[k] = append(f.MultipartFiles[k], fmt.Sprintf("%s:%d:%s:%v", file.Filename, file.Size, content, readErr))
				}
			}
		}
	} else {
		r.ParseForm()
	}
	f.Form, f.PostForm = r.Form, r.PostForm
	b, err := json.Marshal(f)
	if err != nil {
		panic(err)
	}
	return b
}

func multipartWire(t *testing.T) string {
	t.Helper()
	var b bytes.Buffer
	w := multipart.NewWriter(&b)
	if err := w.SetBoundary("hoe-conformance-boundary"); err != nil {
		t.Fatal(err)
	}
	w.WriteField("note", "body")
	w.WriteField("note", "body2")
	part, err := w.CreateFormFile("upload", "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	io.WriteString(part, "hello")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("POST /upload?note=query HTTP/1.1\r\nHost: t\r\nConnection: close\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s", w.FormDataContentType(), b.Len(), b.String())
}

func TestDifferentialRequestFields(t *testing.T) {
	cases := append(corpus(), wireCase{name: "MultipartForm", raw: multipartWire(t)})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := func(own bool) ([]byte, [][]byte) {
				snapshots := make(chan []byte, 4)
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					snapshots <- fieldSnapshot(r)
					fmt.Fprint(w, "fields")
				})
				addr, _ := launch(t, own, &http.Server{Handler: h, MaxHeaderBytes: tc.maxHeader})
				input := tc
				input.status = 0
				wire := exerciseWire(t, addr, input)
				var fields [][]byte
				for {
					select {
					case b := <-snapshots:
						fields = append(fields, b)
					default:
						return wire, fields
					}
				}
			}
			want, wf := run(false)
			got, gf := run(true)
			if !bytes.Equal(want, got) {
				t.Fatalf("DIFF wire: std %q hoe %q", want, got)
			}
			wj, _ := json.Marshal(wf)
			gj, _ := json.Marshal(gf)
			if !bytes.Equal(wj, gj) {
				t.Fatalf("DIFF fields\nstd %s\nhoe %s", wf, gf)
			}
			t.Logf("SAME %d handler request snapshots", len(gf))
		})
	}
}
