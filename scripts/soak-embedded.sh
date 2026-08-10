#!/usr/bin/env bash
# Run repeated embedded write/search cycles with relay restarts between them.
# This is a bounded calibration runner; an overnight mixed workload should use
# the same artifact layout with the product-specific media/workflow/git lanes.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/deploy/embedded/compose.yml"
image="${BUZZ_EMBEDDED_IMAGE:-ghcr.io/chesapeakedev/buzz:main}"
cycles="${BUZZ_SOAK_CYCLES:-3}"
duration="${BUZZ_SOAK_CYCLE_SECONDS:-10}"
conns="${BUZZ_SOAK_CONNECTIONS:-20}"
qps="${BUZZ_SOAK_QPS:-5}"
search_iterations="${BUZZ_SOAK_SEARCH_ITERATIONS:-3}"
outdir="${BUZZ_SOAK_OUTDIR:-$repo_root/test-results/embedded-soak}"
project="buzz-embedded-soak-${RANDOM}-${RANDOM}"
port="${BUZZ_SOAK_PORT:-$((33000 + RANDOM % 1000))}"
container="${project}-relay-1"
private_key="${BENCH_PRIVATE_KEY:-1111111111111111111111111111111111111111111111111111111111111111}"

[[ "$cycles" =~ ^[1-9][0-9]*$ && "$duration" =~ ^[1-9][0-9]*$ ]] || {
  echo "BUZZ_SOAK_CYCLES and BUZZ_SOAK_CYCLE_SECONDS must be positive integers" >&2
  exit 2
}
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
export BUZZ_RATE_LIMIT_HUMAN_WS_EVENTS_PER_SEC=100000
export BUZZ_RATE_LIMIT_HUMAN_MESSAGES_PER_MIN=100000
export BENCH_CONNECT_BATCH_SIZE=25
export BENCH_CONNECT_BATCH_DELAY_MS=100

docker compose --project-name "$project" -f "$compose_file" up -d --wait
http_url="http://localhost:$port"
curl --fail --silent "$http_url/_readiness" >"$outdir/readiness.json"
: >"$outdir/cycles.jsonl"
failed=0

for cycle in $(seq 1 "$cycles"); do
  cycle_dir="$outdir/cycle-$cycle"
  mkdir -p "$cycle_dir"
  start_ms=$(date +%s%3N)
  set +e
  BENCH_PRIVATE_KEY="$private_key" BUZZ_RELAY_URL="$RELAY_URL" \
    BENCH_CHANNEL_FILE="$cycle_dir/channel.id" \
    cargo run --quiet -p buzz-test-client --bin wamp_bench -- \
      auto "$qps" "$duration" "$conns" "$cycle_dir/latency.ms" \
      >"$cycle_dir/write.json" 2>"$cycle_dir/write.stderr"
  write_status=$?
  set -e

  search_status=0
  if [[ -s "$cycle_dir/channel.id" ]]; then
    set +e
    BENCH_PRIVATE_KEY="$private_key" BUZZ_RELAY_URL="$RELAY_URL" \
      cargo run --quiet -p buzz-test-client --bin search_bench -- \
      "$(<"$cycle_dir/channel.id")" wamp-bench "$search_iterations" \
      "$cycle_dir/search.json" > /dev/null 2>"$cycle_dir/search.stderr"
    search_status=$?
    set -e
  else
    printf '%s\n' '{"benchmark_error":"write cycle did not create a channel"}' \
      >"$cycle_dir/search.json"
    search_status=1
  fi

  docker compose --project-name "$project" -f "$compose_file" restart relay >/dev/null
  until curl --fail --silent "$http_url/_readiness" >/dev/null; do sleep 1; done
  recovery_ms=$(( $(date +%s%3N) - start_ms ))
  jq -n \
    --argjson cycle "$cycle" \
    --argjson write_status "$write_status" \
    --argjson search_status "$search_status" \
    --argjson recovery_ms "$recovery_ms" \
    '{cycle: $cycle, write_status: $write_status, search_status: $search_status, restart_recovery_ms: $recovery_ms}' \
    >>"$outdir/cycles.jsonl"
  if ((write_status != 0 || search_status != 0)); then
    failed=$((failed + 1))
  fi
done

echo "embedded restart soak complete: image=$image cycles=$cycles output=$outdir"
if ((failed > 0)); then
  echo "embedded restart soak recorded $failed failed cycle(s)" >&2
  exit 3
fi
