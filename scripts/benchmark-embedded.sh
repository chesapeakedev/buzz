#!/usr/bin/env bash
# Run repeatable embedded-relay write benchmarks and collect operator evidence.
#
# The default matrix is 100, 1,000, and 10,000 authenticated WebSocket
# connections. Override BUZZ_BENCH_LEVELS with comma-separated CONNS:QPS pairs
# when running on a smaller host (for example, 100:25,500:50). Set
# BUZZ_BENCH_SOAK_SECONDS to run the final level for a longer mixed-write soak.
#
# Outputs JSON summaries, raw latency samples, container memory snapshots, and
# a restart-time record under BUZZ_BENCH_OUTDIR (default: test-results/embedded).
# Connections are authenticated in bounded batches so the benchmark does not
# turn the relay's challenge path into an artificial connection stampede.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/deploy/embedded/compose.yml"
image="${BUZZ_EMBEDDED_IMAGE:-ghcr.io/chesapeakedev/buzz:main}"
levels="${BUZZ_BENCH_LEVELS:-100:50,1000:100,10000:250}"
duration="${BUZZ_BENCH_DURATION_SECONDS:-10}"
soak_seconds="${BUZZ_BENCH_SOAK_SECONDS:-0}"
connect_batch_size="${BUZZ_BENCH_CONNECT_BATCH_SIZE:-25}"
connect_batch_delay_ms="${BUZZ_BENCH_CONNECT_BATCH_DELAY_MS:-100}"
rate_limit="${BUZZ_BENCH_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC:-100000}"
outdir="${BUZZ_BENCH_OUTDIR:-$repo_root/test-results/embedded}"
project="buzz-embedded-bench-${RANDOM}-${RANDOM}"
port="${BUZZ_BENCH_PORT:-$((31000 + RANDOM % 1000))}"
container="${project}-relay-1"
private_key="${BENCH_PRIVATE_KEY:-1111111111111111111111111111111111111111111111111111111111111111}"

mkdir -p "$outdir"
cleanup() {
  docker compose --project-name "$project" -f "$compose_file" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

export BUZZ_IMAGE="$image"
export BUZZ_HTTP_PORT="$port"
export RELAY_URL="ws://localhost:$port"
export RELAY_ACCESS=open
export BUZZ_REQUIRE_AUTH_TOKEN=false
export BUZZ_GIT_ENABLED=false
export BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC="$rate_limit"
export BENCH_CONNECT_BATCH_SIZE="$connect_batch_size"
export BENCH_CONNECT_BATCH_DELAY_MS="$connect_batch_delay_ms"

docker compose --project-name "$project" -f "$compose_file" up -d --wait
relay_url="ws://localhost:$port"
http_url="http://localhost:$port"
curl --fail --silent "$http_url/_readiness" >"$outdir/readiness.json"

stats() {
  local label="$1"
  docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}} {{.PIDs}}' "$container" \
    | sed "s/^/${label} /" >>"$outdir/container-stats.txt"
  docker exec "$container" sh -c 'find /data -type f -printf "%s\\n" 2>/dev/null | awk "{sum += \$1} END {print sum + 0}"' \
    >"$outdir/sqlite-and-objects-${label}.bytes"
}

: >"$outdir/container-stats.txt"
: >"$outdir/benchmark-levels.jsonl"
stats idle

IFS=',' read -ra matrix <<<"$levels"
failed_levels=0
for entry in "${matrix[@]}"; do
  conns="${entry%%:*}"
  qps="${entry#*:}"
  if [[ -z "$conns" || -z "$qps" || "$conns" == "$entry" ]]; then
    echo "invalid benchmark level '$entry' (expected CONNS:QPS)" >&2
    exit 2
  fi
  sample="$outdir/latency-${conns}.ms"
  summary="$outdir/summary-${conns}.json"
  BENCH_PRIVATE_KEY="$private_key" BUZZ_RELAY_URL="$relay_url" \
    cargo run --quiet -p buzz-test-client --bin wamp_bench -- \
      auto "$qps" "$duration" "$conns" "$sample" >"$summary"
  jq --arg level "$entry" '. + {level: $level}' "$summary" >>"$outdir/benchmark-levels.jsonl"
  if jq -e '.publish_errors > 0' "$summary" >/dev/null; then
    failed_levels=$((failed_levels + 1))
    echo "benchmark level $entry recorded publish errors" >&2
  fi
  stats "${conns}c"
done

if ((soak_seconds > 0)); then
  last_entry="${matrix[${#matrix[@]}-1]}"
  conns="${last_entry%%:*}"
  qps="${last_entry#*:}"
  BENCH_PRIVATE_KEY="$private_key" BUZZ_RELAY_URL="$relay_url" \
    cargo run --quiet -p buzz-test-client --bin wamp_bench -- \
      auto "$qps" "$soak_seconds" "$conns" "$outdir/latency-soak.ms" \
      >"$outdir/summary-soak.json"
  stats soak
fi

start_ms=$(date +%s%3N)
docker compose --project-name "$project" -f "$compose_file" restart relay >/dev/null
until curl --fail --silent "$http_url/_readiness" >/dev/null; do sleep 1; done
end_ms=$(date +%s%3N)
printf '%s\n' "$((end_ms - start_ms))" >"$outdir/restart-time.ms"

echo "embedded benchmark complete: image=$image output=$outdir"
if ((failed_levels > 0)); then
  echo "embedded benchmark recorded publish errors in $failed_levels level(s)" >&2
  exit 3
fi
