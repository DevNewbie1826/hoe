// Package hoe provides a net/http-compatible server API.
//
// Handlers and routers use the standard http.Handler and http.ResponseWriter
// interfaces. TLS and protocol configuration have the same semantics as
// net/http. Plain HTTP/1.1 over TCP is served by native event loops; TLS,
// HTTP/2, non-TCP listeners, and unsupported platforms fall back to net/http.
package hoe
