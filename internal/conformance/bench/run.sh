#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../../.."
platform="${1:?Usage: run.sh linux|darwin}"
case "$platform" in
  linux|darwin) ;;
  *) printf 'Unknown platform: %s\n' "$platform" >&2; exit 1 ;;
esac
if [[ "$platform" == linux && "$(uname -s)" != Linux ]]; then
  # Wait on actual container completion, not a fixed delay or a timing guess.
  while :; do
    # Autobahn launches many short clients against one long-lived server.
    # Waiting for a client alone admits measurement between two batches.
    busy=$(docker ps --format '{{.ID}} {{.Image}} {{.Names}}' | \
      awk '/crossbario/ || ($2 ~ /^golang:/ && $3 ~ /^hoe-ws-.*-server$/) {print $1}')
    [[ -z "$busy" ]] && break
    printf 'Waiting for competing Autobahn containers: %s\n' "$busy"
    for id in $busy; do docker wait "$id"; done
  done
  exec docker run --rm --label hoe.qa=n5 \
    -v "$PWD:/src" -v "$(go env GOMODCACHE):/go/pkg/mod" -w /src \
    -e HOE_BENCH_DURATION="${HOE_BENCH_DURATION:-15s}" \
    -e HOE_BENCH_RUNS="${HOE_BENCH_RUNS:-3}" \
    golang:1.25 bash internal/conformance/bench/run.sh linux
fi
if [[ "$platform" == darwin && "$(uname -s)" != Darwin ]]; then
  printf 'Darwin measurement requires Darwin\n' >&2
  exit 1
fi
temp=$(mktemp -d "${TMPDIR:-/tmp}/hoe-n5.XXXXXXXX")
cleanup() {
  rm -rf "$temp"
  printf 'CLEANUP temporary benchmark binaries removed: %s\n' "$temp"
}
trap cleanup EXIT
export GOBIN="$temp"
cd internal/conformance
go build -o "$temp/benchserver" ./bench/benchserver
go build -o "$temp/driver" ./bench/driver
go build -o "$temp/wsclient" ./bench/wsclient
go install github.com/codesenberg/bombardier@v1.2.6
go version
"$temp/driver" -server "$temp/benchserver" -bombardier "$temp/bombardier" \
  -wsclient "$temp/wsclient" \
  -platform "$platform" -duration "${HOE_BENCH_DURATION:-15s}" \
  -runs "${HOE_BENCH_RUNS:-3}" -connections 125
