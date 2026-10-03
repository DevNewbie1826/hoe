// Package hoe provides a net/http-compatible server API.
//
// Handlers and routers use the standard http.Handler and http.ResponseWriter
// interfaces. TLS and protocol configuration have the same semantics as
// net/http. This initial implementation delegates all serving to net/http.
package hoe
