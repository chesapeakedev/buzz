#!/usr/bin/env bash
set -euo pipefail

# Run one backend-neutral relay workload against canonical upstream's
# distributed runtime and this fork's embedded runtime. This is intentionally
# local and pre-publication: GitHub Actions never performs upstream syncs.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
upstream_ref="${BUZZ_UPSTREAM_SMOKE_REF:-upstream/main}"
test_root="$(mktemp -d)"
upstream_tree="$test_root/upstream"
upstream_target="$repo_root/target/upstream-smoke"
embedded_data="$test_root/embedded-data"
embedded_port="${BUZZ_FORK_SMOKE_PORT:-3001}"
upstream_port="${BUZZ_UPSTREAM_SMOKE_PORT:-3100}"
postgres_port="${BUZZ_UPSTREAM_POSTGRES_PORT:-15432}"
redis_port="${BUZZ_UPSTREAM_REDIS_PORT:-16379}"
minio_port="${BUZZ_UPSTREAM_MINIO_PORT:-19000}"
smoke_id="buzz-upstream-smoke-${RANDOM}-${RANDOM}"
network="$smoke_id"
postgres="$smoke_id-postgres"
redis="$smoke_id-redis"
minio="$smoke_id-minio"
upstream_pid=""
embedded_pid=""

cleanup() {
  local status=$?
  if [[ -n "$embedded_pid" ]]; then
    kill "$embedded_pid" >/dev/null 2>&1 || true
    wait "$embedded_pid" >/dev/null 2>&1 || true
  fi
  if [[ -n "$upstream_pid" ]]; then
    kill "$upstream_pid" >/dev/null 2>&1 || true
    wait "$upstream_pid" >/dev/null 2>&1 || true
  fi
  docker rm -f "$postgres" "$redis" "$minio" >/dev/null 2>&1 || true
  docker network rm "$network" >/dev/null 2>&1 || true
  if [[ -d "$upstream_tree" ]]; then
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
docker network create "$network" >/dev/null
docker run -d --name "$postgres" --network "$network" -p "127.0.0.1:$postgres_port:5432" \
  -e POSTGRES_USER=buzz -e POSTGRES_PASSWORD=buzz_dev -e POSTGRES_DB=buzz \
  postgres:17-alpine >/dev/null
docker run -d --name "$redis" --network "$network" -p "127.0.0.1:$redis_port:6379" \
  redis:7-alpine >/dev/null
docker run -d --name "$minio" --network "$network" -p "127.0.0.1:$minio_port:9000" \
  -e MINIO_ROOT_USER=buzz_dev -e MINIO_ROOT_PASSWORD=buzz_dev_secret \
  minio/minio:latest server /data >/dev/null
for _ in $(seq 1 60); do
  docker exec "$postgres" pg_isready -U buzz >/dev/null 2>&1 \
    && docker exec "$redis" redis-cli ping >/dev/null 2>&1 \
    && curl --fail --silent "http://127.0.0.1:$minio_port/minio/health/live" >/dev/null 2>&1 \
    && break
  sleep 2
done
docker exec "$postgres" pg_isready -U buzz >/dev/null 2>&1 || fail "isolated PostgreSQL did not become ready"
docker exec "$redis" redis-cli ping >/dev/null 2>&1 || fail "isolated Redis did not become ready"
curl --fail --silent "http://127.0.0.1:$minio_port/minio/health/live" >/dev/null \
  || fail "isolated MinIO did not become ready"
docker run --rm --network "$network" --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set smoke http://$minio:9000 buzz_dev buzz_dev_secret && mc mb smoke/buzz-media"
(
  cd "$upstream_tree"
  . ./bin/activate-hermit
  PGHOST=127.0.0.1 PGPORT="$postgres_port" PGDATABASE=buzz PGUSER=buzz PGPASSWORD=buzz_dev \
    PGSCHEMA_PLAN_HOST=127.0.0.1 PGSCHEMA_PLAN_PORT="$postgres_port" \
    PGSCHEMA_PLAN_DB=buzz PGSCHEMA_PLAN_USER=buzz PGSCHEMA_PLAN_PASSWORD=buzz_dev \
    ./bin/pgschema apply --file schema/schema.sql --auto-approve
  docker exec -i -e PGPASSWORD=buzz_dev "$postgres" psql -U buzz -d buzz -v ON_ERROR_STOP=1 \
    < scripts/attach-schema-partitions.sql
  docker exec -i -e PGPASSWORD=buzz_dev "$postgres" psql -U buzz -d buzz -v ON_ERROR_STOP=1 <<SQL
INSERT INTO communities (id, host)
VALUES ('00000000-0000-4000-8000-00000000c0de', 'localhost:$upstream_port')
ON CONFLICT (lower(host)) DO NOTHING;
SQL
  CARGO_TARGET_DIR="$upstream_target" cargo build -p buzz-relay
)
env \
  DATABASE_URL="postgres://buzz:buzz_dev@127.0.0.1:$postgres_port/buzz" \
  REDIS_URL="redis://127.0.0.1:$redis_port" \
  BUZZ_S3_ENDPOINT="http://127.0.0.1:$minio_port" \
  BUZZ_S3_ACCESS_KEY=buzz_dev \
  BUZZ_S3_SECRET_KEY=buzz_dev_secret \
  BUZZ_S3_BUCKET=buzz-media \
  BUZZ_S3_REGION=us-east-1 \
  BUZZ_S3_ADDRESSING_STYLE=path \
  RELAY_URL="ws://localhost:$upstream_port" \
  BUZZ_BIND_ADDR="127.0.0.1:$upstream_port" \
  BUZZ_HEALTH_PORT="$((upstream_port + 1))" \
  BUZZ_METRICS_PORT="$((upstream_port + 2))" \
  BUZZ_REQUIRE_AUTH_TOKEN=false \
  "$upstream_target/debug/buzz-relay" >"$test_root/upstream.log" 2>&1 &
upstream_pid=$!
for _ in $(seq 1 60); do
  curl --fail --silent "http://127.0.0.1:$upstream_port/_readiness" >/dev/null 2>&1 && break
  kill -0 "$upstream_pid" 2>/dev/null || { cat "$test_root/upstream.log" >&2; fail "upstream relay exited"; }
  sleep 1
done
curl --fail --silent "http://127.0.0.1:$upstream_port/_readiness" >/dev/null \
  || { cat "$test_root/upstream.log" >&2; fail "upstream relay did not become ready"; }
(
  cd "$repo_root"
  . ./bin/activate-hermit
  run_protocol_smoke "upstream distributed" "ws://localhost:$upstream_port"
)
kill "$upstream_pid"
wait "$upstream_pid" 2>/dev/null || true
upstream_pid=""

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
