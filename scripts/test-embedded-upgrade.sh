#!/usr/bin/env bash
set -euo pipefail

# Exercise an in-place embedded upgrade with two immutable images and one
# durable /data volume. This intentionally refuses to use the same image twice:
# a restart smoke test is not upgrade evidence.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/deploy/embedded/compose.yml"
old_image="${BUZZ_EMBEDDED_OLD_IMAGE:-}"
new_image="${BUZZ_EMBEDDED_NEW_IMAGE:-}"
if [[ -z "$old_image" || -z "$new_image" ]]; then
  echo "set BUZZ_EMBEDDED_OLD_IMAGE and BUZZ_EMBEDDED_NEW_IMAGE to immutable images" >&2
  exit 2
fi
if [[ "$old_image" == "$new_image" ]]; then
  echo "old and new embedded images must differ" >&2
  exit 2
fi

test_root="$(mktemp -d)"
project="buzz-embedded-upgrade-${RANDOM}-${RANDOM}"
volume="${project}_buzz-data"
port="${BUZZ_EMBEDDED_UPGRADE_PORT:-$((32000 + RANDOM % 1000))}"
env_file="$test_root/.env"
cleanup() {
  docker compose -p "$project" -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$test_root"
}
trap cleanup EXIT

cat >"$env_file" <<EOF
BUZZ_IMAGE=$old_image
BUZZ_HTTP_PORT=$port
RELAY_URL=ws://127.0.0.1:$port
RELAY_ACCESS=open
BUZZ_REQUIRE_AUTH_TOKEN=false
BUZZ_GIT_ENABLED=false
EOF

compose() {
  docker compose -p "$project" --env-file "$env_file" -f "$compose_file" "$@"
}
readiness() {
  curl --fail --silent --show-error "http://127.0.0.1:$port/_readiness"
}

compose up -d --wait
old_readiness="$(readiness)"
grep -Fq '"status":"ready"' <<<"$old_readiness"
grep -Fq '"database":"sqlite"' <<<"$old_readiness"
volume_key_before="$(docker run --rm -v "$volume:/data:ro" busybox:1.36 sha256sum /data/secrets/relay.key)"
test -n "$volume_key_before"

compose stop relay
sed -i "s#^BUZZ_IMAGE=.*#BUZZ_IMAGE=$new_image#" "$env_file"
compose up -d --wait
new_readiness="$(readiness)"
grep -Fq '"status":"ready"' <<<"$new_readiness"
grep -Fq '"database":"sqlite"' <<<"$new_readiness"
volume_key_after="$(docker run --rm -v "$volume:/data:ro" busybox:1.36 sha256sum /data/secrets/relay.key)"
test "$volume_key_before" = "$volume_key_after"

echo "embedded upgrade smoke passed: old=$old_image new=$new_image"
