package framework

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func formHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		if r.URL.Path == "/upload" {
			err = r.ParseMultipartForm(1024)
		} else {
			err = r.ParseForm()
		}
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		files := make(map[string][]string)
		var values map[string][]string
		if r.MultipartForm != nil {
			defer r.MultipartForm.RemoveAll()
			values = r.MultipartForm.Value
			for k, hs := range r.MultipartForm.File {
				for _, h := range hs {
					rd, err := h.Open()
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					p, err := io.ReadAll(rd)
					rd.Close()
					if err != nil {
						http.Error(w, err.Error(), 500)
						return
					}
					files[k] = append(files[k], fmt.Sprintf("%s:%s", h.Filename, p))
				}
			}
		}
		json.NewEncoder(w).Encode(struct {
			Form, PostForm map[string][]string
			Trace          string
			Values         map[string][]string
			Files          map[string][]string
		}{
			r.Form, r.PostForm, r.Header.Get("X-Trace"), values, files,
		})
	})
}

// Ported binder request-surface scenarios; this exercises live server requests
// rather than hon's implementation-specific GetRequest constructor.
func TestBinderSemantics_RequestSurfaceParity(t *testing.T) {
	for _, ct := range []string{"application/x-www-form-urlencoded", "application/x-www-form-urlencoded; charset=UTF-8"} {
		t.Run(ct, func(t *testing.T) {
			compare(t, formHandler(), "POST", "/submit?name=query&tag=qs", ct, "name=body&tag=body1&tag=b2", "")
		})
	}
	t.Run("multipart_surface", func(t *testing.T) {
		ct, b := multipartBody(t, [][2]string{{"note", "body"}, {"color", "blue"}, {"color", "green"}}, []upload{{"upload", "test.txt", "hello"}})
		compare(t, formHandler(), "POST", "/upload?note=query", ct, b, "")
	})
	t.Run("multipart_precedence", func(t *testing.T) {
		ct, b := multipartBody(t, [][2]string{{"note", "body"}, {"note", "body2"}, {"tag", "body1"}}, []upload{{"upload", "a.txt", "hello"}, {"upload", "b.txt", "world"}})
		compare(t, formHandler(), "POST", "/upload?note=query&tag=q1&tag=q2", ct, b, "")
	})
}

func TestMultipartEdgeSemantics(t *testing.T) {
	ct, b := multipartBody(t, [][2]string{{"note", "a"}, {"note", "b"}}, []upload{
		{"upload", "a.txt", "hello"}, {"upload", "b.txt", "world"}, {"optional", "empty.txt", ""},
	})
	compare(t, formHandler(), "POST", "/upload?tag=q1&tag=q2", ct, b, "")
}
