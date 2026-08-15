#!/usr/bin/env bash
set -euo pipefail

container="${1:-buzz-embedded-relay-1}"
database_dir="${2:-/data/db}"
snapshot_dir="${3:-}"

if [[ -z "$snapshot_dir" ]]; then
  snapshot_dir="$(mktemp -d /tmp/buzz-sqlite-snapshot.XXXXXX)"
else
  mkdir -p "$snapshot_dir"
fi

docker inspect "$container" >/dev/null
docker cp "$container:$database_dir/." "$snapshot_dir" >/dev/null

database="$snapshot_dir/buzz.sqlite3"
if [[ ! -f "$database" ]]; then
  echo "snapshot did not contain $database_dir/buzz.sqlite3" >&2
  exit 1
fi

sqlite3 -readonly "$database" 'PRAGMA integrity_check;' | grep -Fxq ok
printf '%s\n' "$database"
