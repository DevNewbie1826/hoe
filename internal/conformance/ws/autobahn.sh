#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
suite="$root/internal/conformance/ws"
mkdir -p "$root/.omo/evidence/n4"
output="${AUTOBAHN_OUTPUT:-$(mktemp -d "$root/.omo/evidence/n4/autobahn.XXXXXXXX")}"
image="${AUTOBAHN_IMAGE:-crossbario/autobahn-testsuite:latest}"
mkdir -p "$output"
output="$(cd "$output" && pwd)"
if [[ -e "$output/reports" ]]; then
  printf 'Refusing to reuse reports: %s\n' "$output/reports" >&2
  exit 1
fi
tmp="$(mktemp -d -t hoe-autobahn.XXXXXXXX)"
name="hoe-ws-$(basename "$tmp" | tr '[:upper:]' '[:lower:]')"
network="$name-network"
server="$name-server"
client="$name-client"
cleanup() {
  docker rm -f "$client" "$server" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  rm -rf "$tmp"
  printf 'cleanup: removed containers %s %s; network %s; temp %s\n' \
    "$client" "$server" "$network" "$tmp" | tee "$output/cleanup.txt"
}
trap cleanup EXIT
printf 'WORKING: N4 - Build Autobahn targets\n'
(
  cd "$root/internal/conformance"
  CGO_ENABLED=0 GOOS=linux go build -o "$tmp/server" ./ws/autobahn-server
)
# Derive the exact selected case IDs from the image that will execute them.
docker run --rm --name "$client" --entrypoint python "$image" -c \
  'import json; from autobahntestsuite.case import Cases; ids=[".".join(x.__name__[4:].split("_")) for x in Cases]; print(json.dumps(sorted([x for x in ids if int(x.split(".")[0]) in list(range(1,11))+[12,13]])))' \
  > "$output/expected-cases.json"
docker network create "$network" >/dev/null
docker create --name "$server" --network "$network" --network-alias server \
  -e GOMEMLIMIT=256MiB -v "$tmp/server:/server:ro" --entrypoint /server golang:1.25 >/dev/null
docker start "$server" >/dev/null
mkdir "$output/reports"
printf 'WORKING: N4 - Run all Autobahn targets\n'
# Read the server's real readiness event. Keeping the log stream attached until
# the client finishes avoids polling, arbitrary delays and a leaked log process.
docker logs --follow "$server" 2>&1 | {
  IFS= read -r -t 60 readiness
  [[ "$readiness" == READY* ]] || { printf '%s\n' "$readiness" >&2; exit 1; }
  printf '%s\n' "$readiness"
  status=0
  (
    cd "$root/internal/conformance"
    go run ./ws/autobahn-report -prepare "$suite/autobahn.json" \
      -expected "$output/expected-cases.json" -batches "$output/batches"
  ) || status=$?
  if [[ "$status" == 0 ]]; then
    while IFS= read -r batch; do
      printf 'WORKING: N4 - Autobahn %s\n' "$(basename "$batch")"
      docker run --rm --name "$client" --network "$network" \
        -e PYTHONUNBUFFERED=1 -e PYPY_GC_MAX=512MB -e PYPY_GC_MAX_DELTA=64MB \
        -e PYPY_GC_MAJOR_COLLECT=1.1 \
        -v "$batch/spec.json:/config/fuzzingclient.json:ro" \
        -v "$batch/reports:/reports" "$image" \
        wstest --mode fuzzingclient --spec /config/fuzzingclient.json \
        > "$batch/client.log" 2>&1 || { status=$?; break; }
    done < "$output/batches/manifest.txt"
  fi
  docker stop "$server" >/dev/null
  exit "$status"
}
(
  cd "$root/internal/conformance"
  go run ./ws/autobahn-report -merge "$output/batches" -reports "$output/reports"
  go run ./ws/autobahn-report -reports "$output/reports" \
    -expected "$output/expected-cases.json" \
    -summary "$output/autobahn-summary.txt"
)
printf 'Evidence: %s\n' "$output"
