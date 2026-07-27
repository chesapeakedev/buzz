#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
workflow="$repo_root/.github/workflows/docker.yml"

require_text() {
  local text="$1"
  if ! grep -Fq "$text" "$workflow"; then
    echo "Error: relay image workflow is missing: $text" >&2
    exit 1
  fi
}

require_text "IMAGE_NAME: ghcr.io/chesapeakedev/buzz"
require_text "if: github.event_name == 'pull_request' || github.repository == 'chesapeakedev/buzz'"
require_text "if: github.repository == 'chesapeakedev/buzz' && github.event_name != 'pull_request'"
require_text "name: relay-\${{ github.ref_type == 'tag' && 'release' || 'main' }}"
require_text "org.opencontainers.image.source=https://github.com/chesapeakedev/buzz"
require_text "org.opencontainers.image.revision=\${{ github.sha }}"
require_text "subject-name: \${{ env.IMAGE_NAME }}"
require_text "push-to-registry: true"
require_text "gh attestation verify oci://\${IMAGE_NAME}@\${MERGED_DIGEST} --owner chesapeakedev"
require_text "type=ref,event=branch"
require_text "type=sha,prefix=sha-"
require_text "type=semver,pattern={{version}}"
grep -Fq 'repository = "https://github.com/chesapeakedev/buzz"' "$repo_root/Cargo.toml"
grep -Fq 'org.opencontainers.image.source="https://github.com/chesapeakedev/buzz"' \
  "$repo_root/Dockerfile"
grep -Fq 'ghcr.io/chesapeakedev/buzz:main' "$repo_root/deploy/compose/compose.yml"

# Fork-owned relay publication must not be redirectable to an inherited or
# caller-selected namespace. Block-owned push-gateway content follows this
# marker and is intentionally outside this check.
relay_section="$(sed '/^  push-gateway-build:/,$d' "$workflow")"
if grep -Fq "ghcr.io/block/" <<<"$relay_section"; then
  echo "Error: fork-owned relay publication still references ghcr.io/block" >&2
  exit 1
fi
if grep -Eq 'vars\.(GHCR_IMAGE|GHCR_[A-Z_]*IMAGE)' <<<"$relay_section"; then
  echo "Error: relay publication target is caller-configurable" >&2
  exit 1
fi

# The relay job may publish only for fork main/tag events; pull requests stay
# build-only and inherited push-gateway publication remains Block-owned.
require_text 'push=${{ github.event_name != '"'"'pull_request'"'"' }}'
require_text "if: github.repository == 'block/buzz' && github.event_name != 'pull_request'"

echo "ChesapeakeDev relay release contract passed"
