#!/usr/bin/env bash
set -euo pipefail

# Run one backend-neutral relay workload against canonical upstream's
# distributed runtime and this fork's embedded runtime. This is intentionally
# local and pre-publication: GitHub Actions never performs upstream syncs.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
upstream_ref="${BUZZ_UPSTREAM_SMOKE_REF:-upstream/main}"
test_root="$(mktemp -d)"
upstream_tree="$test_root/upstream"
embedded_data="$test_root/embedded-data"
embedded_port="${BUZZ_FORK_SMOKE_PORT:-3001}"
upstream_pid_file=/tmp/buzz-relay.pid
upstream_log=/tmp/buzz-relay.log
embedded_pid=""

cleanup() {
  local status=$?
  if [[ -n "$embedded_pid" ]]; then
    kill "$embedded_pid" >/dev/null 2>&1 || true
    wait "$embedded_pid" >/dev/null 2>&1 || true
  fi
  if [[ -f "$upstream_pid_file" ]]; then
    kill "$(cat "$upstream_pid_file")" >/dev/null 2>&1 || true
    rm -f "$upstream_pid_file"
  fi
  if [[ -d "$upstream_tree" ]]; then
    cp "$upstream_log" "$test_root/upstream.log" >/dev/null 2>&1 || true
    docker compose -f "$upstream_tree/docker-compose.yml" down --remove-orphans >/dev/null 2>&1 || true
    git -C "$repo_root" worktree remove --force "$upstream_tree" >/dev/null 2>&1 || true
  fi
  if [[ $status -eq 0 ]]; then
    rm -rf "$test_root"
  else
    echo "upstream/fork smoke logs retained at $test_root" >&2
  fi
  return "$status"
}
trap cleanup EXIT

fail() {
  echo "upstream/fork smoke: $*" >&2
  exit 1
}

command -v docker >/dev/null 2>&1 || fail "docker is required"
for container in buzz-postgres buzz-redis buzz-minio buzz-minio-init; do
  if docker inspect "$container" >/dev/null 2>&1; then
    fail "container $container already exists; stop the development stack before this isolated smoke"
  fi
done
git -C "$repo_root" diff --quiet && git -C "$repo_root" diff --cached --quiet \
  || fail "start from a clean worktree"
git -C "$repo_root" rev-parse --verify "$upstream_ref^{commit}" >/dev/null \
  || fail "missing $upstream_ref; fetch upstream first"
git -C "$repo_root" merge-base --is-ancestor "$upstream_ref" HEAD \
  || fail "$upstream_ref is not an ancestor of fork HEAD; finish the rebase first"

tests=(
  test_connect_and_authenticate
  test_send_event_and_receive_via_subscription
  test_subscription_filters_by_kind
  test_stored_events_returned_before_eose
  test_ephemeral_event_not_stored
  test_nip11_relay_info
  test_pubkey_mismatch_rejected
  test_eose_sent_for_empty_subscription
)

run_protocol_smoke() {
  local label="$1" url="$2" test_name
  echo "Running shared protocol smoke against $label ($url)..."
  for test_name in "${tests[@]}"; do
    RELAY_URL="$url" cargo test -p buzz-test-client --test e2e_relay \
      "$test_name" -- --ignored --exact
  done
}

echo "Preparing canonical upstream distributed relay at $(git -C "$repo_root" rev-parse --short "$upstream_ref")..."
git -C "$repo_root" worktree add --detach "$upstream_tree" "$upstream_ref" >/dev/null
(
  cd "$upstream_tree"
  . ./bin/activate-hermit
  ./scripts/start-relay-for-tests.sh --profile dev
)
(
  cd "$repo_root"
  . ./bin/activate-hermit
  run_protocol_smoke "upstream distributed" "ws://localhost:3000"
)
kill "$(cat "$upstream_pid_file")"
wait "$(cat "$upstream_pid_file")" 2>/dev/null || true
rm -f "$upstream_pid_file"

echo "Preparing fork embedded relay at $(git -C "$repo_root" rev-parse --short HEAD)..."
(
  cd "$repo_root"
  . ./bin/activate-hermit
  cargo build -p buzz-relay -p buzz-test-client
)
mkdir -p "$embedded_data"
env \
  BUZZ_DEPLOYMENT_MODE=embedded \
  BUZZ_DATA_DIR="$embedded_data" \
  BUZZ_BIND_ADDR="127.0.0.1:$embedded_port" \
  BUZZ_HEALTH_PORT="$((embedded_port + 1))" \
  BUZZ_METRICS_PORT="$((embedded_port + 2))" \
  RELAY_URL="ws://localhost:$embedded_port" \
  RELAY_ACCESS=open \
  BUZZ_REQUIRE_AUTH_TOKEN=false \
  BUZZ_GIT_ENABLED=false \
  "$repo_root/target/debug/buzz-relay" >"$test_root/embedded.log" 2>&1 &
embedded_pid=$!
for _ in $(seq 1 60); do
  if ! kill -0 "$embedded_pid" 2>/dev/null; then
    cat "$test_root/embedded.log" >&2
    fail "fork embedded relay exited before readiness"
  fi
  if curl --fail --silent "http://127.0.0.1:$embedded_port/_readiness" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl --fail --silent "http://127.0.0.1:$embedded_port/_readiness" >/dev/null \
  || { cat "$test_root/embedded.log" >&2; fail "fork embedded relay did not become ready"; }
(
  cd "$repo_root"
  . ./bin/activate-hermit
  run_protocol_smoke "fork embedded" "ws://localhost:$embedded_port"
)

echo "upstream/fork compatibility smoke passed: upstream=$upstream_ref fork=HEAD"
