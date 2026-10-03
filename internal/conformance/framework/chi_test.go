package framework

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
)

// Ported from hon/pkg/adaptor/chi_integration_test.go, using a bound listener
// instead of reserving a port and polling for readiness.
func TestChiCompatibility_RouteParamsQueryHeadersAndMiddleware(t *testing.T) {
	type key struct{}
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), key{}, "seen")))
		})
	})
	r.Get("/articles/{articleID}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "articleID=%s query=%s trace=%s middleware=%v host=%s", chi.URLParam(r, "articleID"), r.URL.Query().Get("q"), r.Header.Get("X-Trace"), r.Context().Value(key{}), r.Host)
	})
	compare(t, r, "GET", "/articles/42?q=hon", "", "", "articleID=42 query=hon trace=abc123 middleware=seen host=api.local")
}

func TestChiCompatibility_HostAndPathDependentMiddleware(t *testing.T) {
	type key struct{}
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Host == "api.local" && r.URL.Path == "/tenant/acme" {
				r = r.WithContext(context.WithValue(r.Context(), key{}, "acme"))
			}
			next.ServeHTTP(w, r)
		})
	})
	r.Get("/tenant/{tenantID}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "tenantID=%s tenant=%v host=%s path=%s", chi.URLParam(r, "tenantID"), r.Context().Value(key{}), r.Host, r.URL.Path)
	})
	compare(t, r, "GET", "/tenant/acme", "", "", "tenantID=acme tenant=acme host=api.local path=/tenant/acme")
}

func TestChiCompatibility_EscapedPathParity(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/files/{name}", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "name=%s path=%s rawPath=%s", chi.URLParam(r, "name"), r.URL.Path, r.URL.RawPath)
	})
	compare(t, r, "GET", "/files/a%2Fb", "", "", "name=a%2Fb path=/files/a/b rawPath=/files/a%2Fb")
}
