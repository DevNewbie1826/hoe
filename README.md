<p align="center"><img src="assets/hoe-pickaxe.svg" width="160" alt="hoe"></p>

<h1 align="center">hoe</h1>

hoe is a drop-in, net/http-compatible HTTP/1.1 and WebSocket server for Go that runs on event loops. You keep your handlers, your router and your middleware. The API mirrors `net/http`, so switching is an import change. Anything hoe doesn't serve natively falls back to `net/http` with the same semantics.

## Usage

```sh
go get github.com/DevNewbie1826/hoe
```

A complete server:

```go
package main

import (
	"log"
	"net/http"

	"github.com/DevNewbie1826/hoe"
)

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Hello, hoe\n"))
	})
	log.Fatal(hoe.ListenAndServe(":8080", nil))
}
```

That's the whole migration. Coming from `net/http`, the diff is:

```diff
 import (
 	"log"
 	"net/http"
+
+	"github.com/DevNewbie1826/hoe"
 )

-	log.Fatal(http.ListenAndServe(":8080", nil))
+	log.Fatal(hoe.ListenAndServe(":8080", nil))
```

Any `http.Handler` works: `http.ServeMux`, chi, gin, echo, and libraries that use `Hijack`.

Need timeouts or other settings? `hoe.Server` has the same fields as `http.Server` (fragment):

```go
srv := &hoe.Server{
	Addr:         ":8080",
	Handler:      mux,
	ReadTimeout:  5 * time.Second,
	WriteTimeout: 10 * time.Second,
}
log.Fatal(srv.ListenAndServe())
```

`Shutdown`, `Close`, `RegisterOnShutdown`, `SetKeepAlivesEnabled`, `Serve` and `ListenAndServeTLS` behave as they do on `http.Server`. Already have an `*http.Server`? Convert it in one line (fragment):

```go
log.Fatal(hoe.FromStd(stdServer).ListenAndServe())
```

## Protocols and caveats

| Traffic | Served by |
| --- | --- |
| HTTP/1.1, plaintext TCP, on linux or darwin | native hoe event loop engine |
| HTTP/1.1 over TLS | `net/http` fallback, same semantics |
| HTTP/2 (TLS with ALPN) | `net/http` fallback, same semantics |
| h2c, enabled through `Protocols` | `net/http` fallback, same semantics |
| Non-TCP listeners (for example Unix sockets) | `net/http` fallback, same semantics |
| Other operating systems | `net/http` fallback, same semantics |

Caveats:

- **`PipelineFlushDelay`.** Pipelined HTTP/1 responses are coalesced in the kernel per read round. Zero means a 1 ms cap, a positive value sets your own cap, and a negative value turns coalescing off. Every response is still written, and its error checked, before the next handler runs. The only difference from `net/http` is delivery timing: while a later pipelined handler blocks, an earlier response may wait up to the cap.
- **`MaxBytesReader` exception.** If a `http.MaxBytesReader` isn't reachable from the served `r.Body` (kept only in a local variable, or installed on a copied request by `http.MaxBytesHandler`), an overflow produces the same response and error as `net/http`, but the connection stays reusable instead of closing when at most 256 KiB of body remains unread. Assign the wrapper to `r.Body` to keep the `net/http` close behavior.
- **Go 1.25 or newer** is required.

### WebSocket

The `hoe/websocket` package is a callback-based RFC 6455 server with optional permessage-deflate. Your handler can return right after `Upgrade`. On hoe plaintext servers idle websockets wait in the shared event loop, with no goroutine per connection. On any other server, each connection gets one reader goroutine.

An echo server:

```go
package main

import (
	"log"
	"net/http"

	"github.com/DevNewbie1826/hoe"
	"github.com/DevNewbie1826/hoe/websocket"
)

func main() {
	upgrader := websocket.Upgrader{EnableCompression: true}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		_, err := upgrader.Upgrade(w, r, websocket.Handler{
			OnMessage: func(c *websocket.Conn, kind int, p []byte) {
				if err := c.WriteMessage(kind, p); err != nil {
					c.Close()
				}
			},
			OnClose: func(c *websocket.Conn, err error) {
				log.Println("closed:", err)
			},
		})
		if err != nil {
			log.Println("upgrade:", err)
		}
	})
	log.Fatal(hoe.ListenAndServe(":8080", nil))
}
```

`Upgrader` also takes `CheckOrigin`, `Subprotocols` and `ReadLimit`, and `websocket.Upgrade(w, r, h)` uses the defaults. `Conn` has `WriteMessage`, `WriteControl`, `Close` and `SetReadLimit`. Hijack-based libraries such as gorilla/websocket work too.

## Benchmark

Conditions: the server ran on an Apple M1 Mac with `GOMAXPROCS=4`. Load came from wrk on a separate Mac (macmini) over Wi-Fi, with 1000 connections, 10 s per run and 3 rounds. Numbers are medians. The handler writes `Hello Benchmark`.

| Scenario | hoe | net/http |
| --- | --- | --- |
| Pipelined, 16 requests per connection write | 116.0k RPS | 50.4k RPS |
| Normal requests, server CPU time per request | 31.6 µs | 48.3 µs |
| Normal requests, allocations per request | 2.23 | 13.14 |

The CPU figures are server CPU time per request, not latency.

A note on the setup: the link was Wi-Fi, both servers saw occasional socket read errors, and net/http also had some timeouts. That's why medians are reported.
