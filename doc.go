// Package hoe provides a net/http-compatible server API.
//
// Handlers and routers use the standard http.Handler and http.ResponseWriter
// interfaces. TLS and protocol configuration have the same semantics as
// net/http. Plain HTTP/1.1 over TCP is served by native event loops; TLS,
// HTTP/2, non-TCP listeners, and unsupported platforms fall back to net/http.
//
// The native HTTP/1 engine coalesces pipelined output in the kernel per read
// round. PipelineFlushDelay bounds the delivery delay (default 1 ms), including
// when a later handler blocks; negative disables this timing difference.
// Every response is still written and its error checked before the next handler.
//
// ServeAll serves multiple listeners with one shared engine when they are
// eligible for the native HTTP/1 engine. This can reduce tail latency: in a
// 2-CPU echo test, 50 per-port engines measured 1.48 s p99; one shared engine
// measured 313 ms. BaseContext is called once per listener, and connections
// from each listener carry that listener's base context. A permanent accept
// error stops only that listener; ServeAll returns when all listeners stop.
package hoe
