#!/usr/bin/env bash
set -euo pipefail

# Smoke-test the embedded community and pairing relays from an empty volume.
# The test is deliberately image-based: it catches missing runtime files,
# permissions, SQLite bootstrap failures, readiness regressions, restart key
# rotation, and backup/restore mistakes that Rust unit tests cannot observe.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
compose_file="$repo_root/deploy/embedded/compose.yml"
test_root="$(mktemp -d)"
project="buzz-embedded-smoke-${RANDOM}-${RANDOM}"
volume="${project}_buzz-data"
cleanup() {
  docker compose -p "$project" -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$test_root"
}
trap cleanup EXIT

image="${BUZZ_EMBEDDED_IMAGE:-ghcr.io/chesapeakedev/buzz:main}"
port="${BUZZ_EMBEDDED_SMOKE_PORT:-$((30000 + RANDOM % 1000))}"
pairing_port="${BUZZ_EMBEDDED_PAIRING_SMOKE_PORT:-$((31000 + RANDOM % 1000))}"
env_file="$test_root/.env"

# The embedded artifact must never inherit the desktop app's Block-hosted
# Builderlab integration. Check the exact runtime binary and bundled web assets,
# not just source paths, so a packaging regression cannot silently add it.
if docker run --rm --entrypoint sh "$image" -c \
  'grep -aiq builderlab /usr/local/bin/buzz-relay || grep -Raiq builderlab /srv/buzz'; then
  echo "embedded runtime unexpectedly contains a Builderlab reference" >&2
  exit 1
fi

cat >"$env_file" <<EOF
BUZZ_IMAGE=$image
BUZZ_HTTP_PORT=$port
BUZZ_PAIRING_PORT=$pairing_port
RELAY_URL=ws://127.0.0.1:$port
BUZZ_PAIRING_RELAY_URL=ws://127.0.0.1:$pairing_port
RELAY_ACCESS=open
BUZZ_REQUIRE_AUTH_TOKEN=false
BUZZ_GIT_ENABLED=false
EOF

compose() {
  docker compose -p "$project" --env-file "$env_file" -f "$compose_file" "$@"
}

compose up -d --wait
curl --fail --silent --show-error "http://127.0.0.1:$port/_liveness" >/dev/null
readiness="$(curl --fail --silent --show-error "http://127.0.0.1:$port/_readiness")"
grep -Fq '"status":"ready"' <<<"$readiness"
grep -Fq '"database":"sqlite"' <<<"$readiness"
grep -Fq '"coordination":"local"' <<<"$readiness"
grep -Fq '"objects":"filesystem"' <<<"$readiness"
bash -ec "exec 3<>/dev/tcp/127.0.0.1/$pairing_port"
nip11="$(curl --fail --silent --show-error \
  -H 'Accept: application/nostr+json' "http://127.0.0.1:$port/")"
grep -Fq "\"pairing_relay_url\":\"ws://127.0.0.1:$pairing_port\"" <<<"$nip11"

key_before="$(docker run --rm -v "$volume:/data:ro" busybox:1.36 sha256sum /data/secrets/relay.key)"
test -n "$key_before"

compose restart relay
compose up -d --wait
key_after="$(docker run --rm -v "$volume:/data:ro" busybox:1.36 sha256sum /data/secrets/relay.key)"
test "$key_before" = "$key_after"

compose stop relay
mkdir -p "$test_root/backup"
docker run --rm -v "$volume:/data:ro" -v "$test_root/backup:/backup" busybox:1.36 \
  tar -C /data -cf /backup/buzz-data.tar .
test -s "$test_root/backup/buzz-data.tar"
compose down -v --remove-orphans
docker run --rm -v "$volume:/data" -v "$test_root/backup:/backup" busybox:1.36 \
  tar -C /data -xf /backup/buzz-data.tar
compose up -d --wait
key_restored="$(docker run --rm -v "$volume:/data:ro" busybox:1.36 sha256sum /data/secrets/relay.key)"
test "$key_before" = "$key_restored"

echo "embedded compose smoke passed: $image"
