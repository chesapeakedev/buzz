#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

require_text() {
  local file="$1"
  local text="$2"
  if ! grep -Fq "$text" "$file"; then
    echo "Error: $file must contain fork release guard: $text" >&2
    exit 1
  fi
}

# These upstream lanes can build or validate in pull requests, but publication
# must remain restricted to block/buzz until a ChesapeakeDev-owned lane replaces
# each one deliberately.
require_text .github/workflows/docker.yml \
  "if: github.repository == 'block/buzz' && github.event_name != 'pull_request'"
require_text .github/workflows/helm-chart.yml \
  "github.repository == 'block/buzz' &&"
require_text .github/workflows/push-gateway-helm-chart.yml \
  "if: github.repository == 'block/buzz' && github.event_name != 'pull_request'"
require_text .github/workflows/sprig.yml \
  "github.repository == 'block/buzz' &&"
require_text .github/workflows/promote-oss-desktop-release.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/sprig-image.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/release.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/mobile-release-candidate.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/auto-tag-on-release-pr-merge.yml \
  "(github.repository == 'block/buzz' || github.repository == 'chesapeakedev/buzz') &&"
require_text .github/workflows/linux-canary.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/signed-macos-canary.yml \
  "if: github.repository == 'block/buzz'"
require_text .github/workflows/windows-canary.yml \
  "if: github.repository == 'block/buzz'"

# Client CI stays Block-owned. ChesapeakeDev validates web locally and runs
# relay-focused Actions only (see UPSTREAM.md intentional omissions).
require_text .github/workflows/ci.yml \
  "github.repository == 'block/buzz' &&"
require_text .github/workflows/ci.yml \
  "name: Relay CI Artifacts"
require_text .github/workflows/sprig.yml \
  "if: github.repository == 'block/buzz'"

# Upstream synchronization is deliberately local-only. Reintroducing an
# Actions workflow would spend runner minutes and create an unnecessary
# repository-write surface.
if [[ -e .github/workflows/upstream-sync.yml ]]; then
  echo "Error: upstream synchronization must remain local-only" >&2
  exit 1
fi

# Adding another write-capable workflow is a deliberate security decision. This
# inventory makes new publication or repository-mutation surfaces fail CI until
# they are reviewed and added alongside an explicit ownership guard.
expected_write_workflows="$(
  cat <<'EOF'
.github/workflows/docker.yml
.github/workflows/embedded-release.yml
.github/workflows/helm-chart.yml
.github/workflows/promote-oss-desktop-release.yml
.github/workflows/push-gateway-helm-chart.yml
.github/workflows/release.yml
.github/workflows/signed-macos-canary.yml
.github/workflows/sprig-image.yml
.github/workflows/sprig.yml
EOF
)"
actual_write_workflows="$(
  grep -El '(^|[[:space:]])(contents|packages|id-token|attestations|pull-requests): write' \
    .github/workflows/*.yml | sort
)"
if [[ "$actual_write_workflows" != "$expected_write_workflows" ]]; then
  echo "Error: write-capable workflow inventory changed" >&2
  diff -u <(printf '%s\n' "$expected_write_workflows") \
    <(printf '%s\n' "$actual_write_workflows") >&2 || true
  exit 1
fi

echo "fork release safety checks passed"
