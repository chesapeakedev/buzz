#!/usr/bin/env bash
# Rebase the fork patch stack onto block/buzz and publish it directly to the
# fork's main branch. No upstream pull request is created by this workflow.
#
# Modes:
#   status         Fetch and report fork/upstream divergence.
#   sync           Rebase the fork stack onto upstream (no push); on conflict,
#                  write a sentinel and exit non-zero with a short stderr line.
#   resolve        LLM-resolve a rebase-conflict sentinel (codex) and finish.
#   release        Publish the prepared stack (if any), then cut a relay release
#                  and run the regression upgrade smoke against the last release.
#   publish-main   Publish the prepared stack only (legacy, no release).
set -euo pipefail

mode="${1:-}"
origin_remote="${BUZZ_ORIGIN_REMOTE:-origin}"
upstream_remote="${BUZZ_UPSTREAM_REMOTE:-upstream}"
upstream_url="${BUZZ_UPSTREAM_URL:-https://github.com/block/buzz.git}"
main_branch="${BUZZ_MAIN_BRANCH:-main}"
sync_branch="${BUZZ_SYNC_BRANCH:-upstream-sync}"
upstream_branch="${BUZZ_UPSTREAM_BRANCH:-main}"

fail() { echo "Error: $*" >&2; exit 1; }
usage() { echo "Usage: $0 status|sync|resolve|release|publish-main" >&2; exit 1; }

remote_identity() {
  local url="$1"
  case "$url" in
    *github.com:*) printf '%s\n' "${url##*github.com:}" | sed -E 's|^/+||; s|\.git/?$||' ;;
    *github.com/*) printf '%s\n' "${url##*github.com/}" | sed -E 's|^/+||; s|\.git/?$||' ;;
    *) printf '%s\n' "${url%/}" ;;
  esac
}

ensure_remotes() {
  git rev-parse --git-dir >/dev/null 2>&1 || fail "run inside a Git repository"
  git remote get-url "$origin_remote" >/dev/null 2>&1 ||
    fail "required fork remote '$origin_remote' is not configured"
  if ! git remote get-url "$upstream_remote" >/dev/null 2>&1; then
    git remote add "$upstream_remote" "$upstream_url"
    echo "Added $upstream_remote remote: $upstream_url"
  fi
  local actual expected
  actual="$(remote_identity "$(git remote get-url "$upstream_remote")")"
  expected="$(remote_identity "$upstream_url")"
  [[ "$actual" == "$expected" ]] ||
    fail "remote '$upstream_remote' does not point to '$upstream_url'"
}

fetch_branches() {
  git fetch "$origin_remote" "refs/heads/$main_branch:refs/remotes/$origin_remote/$main_branch" --no-tags
  git fetch "$upstream_remote" "refs/heads/$upstream_branch:refs/remotes/$upstream_remote/$upstream_branch" --no-tags
}

require_clean_tree() {
  [[ -z "$(git status --porcelain=v1)" ]] ||
    fail "working tree is not clean; commit or stash changes before syncing"
}

require_identity() {
  git config user.name >/dev/null 2>&1 || fail "git user.name is required"
  git config user.email >/dev/null 2>&1 || fail "git user.email is required"
}

show_status() {
  local fork_only upstream_only
  read -r fork_only upstream_only < <(
    git rev-list --left-right --count "$origin_remote/$main_branch...$upstream_remote/$upstream_branch"
  )
  echo "Fork-only commits: $fork_only"
  echo "Upstream-only commits: $upstream_only"
  echo "Fork main: $(git rev-parse "$origin_remote/$main_branch")"
  echo "Upstream main: $(git rev-parse "$upstream_remote/$upstream_branch")"
}

checkout_sync_branch() {
  local current
  current="$(git symbolic-ref --quiet --short HEAD)" || fail "detached HEAD is unsupported"
  [[ "$current" == "$main_branch" || "$current" == "$sync_branch" ]] ||
    fail "run from '$main_branch' or '$sync_branch' (currently '$current')"
  if [[ "$current" == "$main_branch" ]] &&
    [[ "$(git rev-parse HEAD)" != "$(git rev-parse "$origin_remote/$main_branch")" ]]; then
    fail "local '$main_branch' is not aligned with '$origin_remote/$main_branch'"
  fi
  git switch -C "$sync_branch" "$origin_remote/$main_branch"
}

validate_patch_stack() {
  local base="$1" commit subject
  while read -r commit; do
    [[ -n "$commit" ]] || continue
    subject="$(git show -s --format=%s "$commit")"
    [[ "$subject" =~ ^(feat|fix|docs|refactor|test|chore|ci)(\([a-z0-9._-]+\))?!?:[[:space:]].+ ]] ||
      fail "fork commit $commit does not use Conventional Commits: $subject"
    git show -s --format=%B "$commit" | grep -Eq '^Signed-off-by: .+ <.+>$' ||
      fail "fork commit $commit is missing a DCO Signed-off-by trailer"
  done < <(git rev-list --reverse "$base..HEAD")
}

sync_upstream() {
  require_clean_tree
  require_identity
  fetch_branches
  show_status
  local previous_base
  previous_base="$(git merge-base "$origin_remote/$main_branch" "$upstream_remote/$upstream_branch")"
  SENTINEL_UPSTREAM_TIP="$(git rev-parse "$upstream_remote/$upstream_branch")"
  SENTINEL_FORK_TIP_BEFORE="$(git rev-parse "$origin_remote/$main_branch")"
  SENTINEL_MERGE_BASE="$previous_base"
  if [[ "$(git rev-parse "$upstream_remote/$upstream_branch")" == "$previous_base" ]]; then
    echo "Fork main already uses the current upstream base; nothing to sync."
    return 0
  fi
  checkout_sync_branch
  local log rc conflicted_json files n
  log="$(git rev-parse --git-dir)/sync-upstream-attempt.log"
  SENTINEL_LOG_PATH="$log"
  set +e
  git rebase --onto "$upstream_remote/$upstream_branch" "$previous_base" "$sync_branch" >"$log" 2>&1
  rc=$?
  set -e
  if [[ "$rc" -ne 0 ]]; then
    conflicted_json="$(git diff --name-only --diff-filter=U -z | python3 -c "import json,sys; print(json.dumps([l for l in sys.stdin.buffer.read().decode('utf-8','surrogateescape').split(chr(0)) if l]))" 2>/dev/null || echo '[]')"
    write_sentinel "rebase-conflict" "$conflicted_json" ""
    n="$(printf '%s' "$conflicted_json" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null || echo "?")"
    files="$(git diff --name-only --diff-filter=U | tr '\n' ',' | sed 's/,$//')"
    fail "upstream rebase failed: ${n} conflict(s) in ${files:-<unknown>}. Tree left mid-rebase on '$sync_branch'. Sentinel at $(sentinel_path). Resolve with 'just sync-resolve' (LLM) or fix manually then 'just sync-release'. Full log: $log"
  fi
  echo "Prepared linear fork patch stack on '$sync_branch'."
}

lease_value() { git rev-parse --verify --quiet "refs/remotes/$origin_remote/$1" || true; }

prepared_sync_branch() {
  local current
  current="$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
  [[ "$current" == "$sync_branch" ]] || return 1
  git merge-base --is-ancestor "$upstream_remote/$upstream_branch" HEAD
}

publish_main() {
  require_clean_tree
  require_identity
  fetch_branches
  if prepared_sync_branch; then
    echo "Reusing prepared linear stack on '$sync_branch'."
  else
    sync_upstream
  fi
  [[ "$(git rev-list --count --merges "$upstream_remote/$upstream_branch..HEAD")" == "0" ]] ||
    fail "rebased fork patch stack contains merge commits"
  validate_patch_stack "$upstream_remote/$upstream_branch"
  local expected
  expected="$(lease_value "$main_branch")"
  git push "$origin_remote" HEAD:"refs/heads/$main_branch" \
    "--force-with-lease=refs/heads/$main_branch:$expected"
  git update-ref "refs/remotes/$origin_remote/$main_branch" HEAD
  git switch -C "$main_branch" HEAD
  echo "Published the linear fork patch stack to '$main_branch' with --force-with-lease."
}

sentinel_path() { printf '%s/upstream-sync-conflict.json' "$(git rev-parse --git-dir)"; }

# Write a state sentinel inside .git (never committed) describing the sync state.
# $1=state, $2=conflicted_files JSON array (default []), $3=detail string (default "").
# Reads upstream tip / fork tip / merge-base / log path from SENTINEL_* globals
# (set by sync_upstream before a conflict); empty when called from other paths.
write_sentinel() {
  local state="$1" conflicted_json="${2:-[]}" detail="${3:-}"
  local current_commit="" commits_remaining=""
  if git rev-parse -q --verify REBASE_HEAD >/dev/null 2>&1; then
    current_commit="$(git show -s --format='%H %s' REBASE_HEAD)"
    commits_remaining="$(git rev-list --count REBASE_HEAD..HEAD 2>/dev/null || echo 0)"
  fi
  STATE="$state" CONFLICTED="$conflicted_json" DETAIL="$detail" \
  CURRENT="$current_commit" REMAINING="$commits_remaining" \
  UP="${SENTINEL_UPSTREAM_TIP:-}" FORK="${SENTINEL_FORK_TIP_BEFORE:-}" MB="${SENTINEL_MERGE_BASE:-}" \
  LOG="${SENTINEL_LOG_PATH:-}" \
  python3 - >"$(sentinel_path)" <<'PY'
import json, os
print(json.dumps({
  "state": os.environ["STATE"],
  "branch": "upstream-sync",
  "upstream_tip": os.environ["UP"],
  "fork_tip_before": os.environ["FORK"],
  "merge_base": os.environ["MB"],
  "conflicted_files": json.loads(os.environ["CONFLICTED"]) if os.environ["CONFLICTED"] else [],
  "current_commit": os.environ["CURRENT"],
  "commits_remaining": int(os.environ["REMAINING"]) if os.environ["REMAINING"].isdigit() else 0,
  "detail": os.environ["DETAIL"],
  "log_path": os.environ["LOG"],
  "resolve_target": "just sync-resolve",
  "release_target": "just sync-release",
}, indent=2))
PY
}

# Resolve a rebase-conflict sentinel using codex (maintain-fork skill),
# then publish the rebased stack and cut a release.
resolve_conflicts() {
  local sp repo sentinel_contents last_msg prompt
  sp="$(sentinel_path)"
  [[ -f "$sp" ]] || fail "no conflict sentinel at $sp; run 'just sync' first"
  python3 - "$sp" <<'PY' || fail "sentinel at $1 is not in 'rebase-conflict' state"
import json, sys
d = json.load(open(sys.argv[1]))
sys.exit(0 if d.get("state") == "rebase-conflict" else 1)
PY
  git rev-parse -q --verify REBASE_HEAD >/dev/null 2>&1 \
    || fail "no rebase in progress; sentinel at $sp is stale. Remove it and run 'just sync'."
  command -v codex >/dev/null 2>&1 \
    || fail "codex CLI not found on PATH; install it or resolve conflicts manually then run 'just sync-release'. Sentinel: $sp"
  repo="$(git rev-parse --show-toplevel)"
  sentinel_contents="$(cat "$sp")"
  last_msg="$(git rev-parse --git-dir)/sync-upstream-resolve.txt"
  prompt="You are resuming an interrupted upstream rebase in the ChesapeakeDev buzz fork at $repo.

A 'git rebase --onto <upstream> <merge-base> upstream-sync' stopped on a conflict. The repo is mid-rebase on branch 'upstream-sync' (REBASE_HEAD exists).

Follow the conflict-resolution procedure in .codex/skills/maintain-fork/SKILL.md (the 'Build a Defended Linear History' / rebase sections): preserve upstream behavior by default, then reapply fork-specific changes behind narrow seams. Never resolve conflicts wholesale with an ours/theirs strategy. Keep every replayed commit Conventional-Commit compliant and DCO-signed.

Sentinel state (JSON):
$sentinel_contents

For each conflicted file (those in the sentinel plus any from 'git diff --name-only --diff-filter=U'):
1. Read the conflicted file and understand both sides.
2. Edit it to preserve upstream behavior and reapply the fork diff behind a narrow seam.
3. 'git add <file>'.
4. Continue non-interactively: GIT_EDITOR=true git rebase --continue --signoff
5. If the next commit also conflicts, repeat until the rebase completes (REBASE_HEAD is gone and you are on 'upstream-sync').

If you cannot confidently resolve a file, stop, leave the rebase in place, and report exactly: resolve-failed: <files>. Do not abort and do not force any resolution.
When the rebase is complete, report: rebase-complete."
  if ! codex exec -C "$repo" --dangerously-bypass-approvals-and-sandbox -o "$last_msg" "$prompt"; then
    fail "codex failed to complete the rebase; see $last_msg. Re-run 'just sync-resolve' or resolve manually then 'just sync-release'. Sentinel: $sp"
  fi
  if git rev-parse -q --verify REBASE_HEAD >/dev/null 2>&1; then
    local remaining files
    remaining="$(git diff --name-only --diff-filter=U -z | python3 -c "import json,sys; print(json.dumps([l for l in sys.stdin.buffer.read().decode('utf-8','surrogateescape').split(chr(0)) if l]))" 2>/dev/null || echo '[]')"
    write_sentinel "resolve-failed" "$remaining" "codex returned but the rebase is still in progress; see $last_msg"
    files="$(git diff --name-only --diff-filter=U | tr '\n' ',' | sed 's/,$//')"
    fail "rebase still in progress after codex; conflicts may remain in ${files:-<unknown>}. Sentinel updated at $sp. See $last_msg."
  fi
  echo "codex completed the rebase. Last message: $(cat "$last_msg" 2>/dev/null || true)"
  validate_patch_stack "$upstream_remote/$upstream_branch"
  echo "Prepared resolved fork patch stack on '$sync_branch'; proceeding to publish + release."
}

# Publish the prepared rebased stack (if any), then cut a relay release and run
# the regression upgrade smoke. Idempotent across re-runs after a smoke failure.
release_main() {
  require_clean_tree
  require_identity
  fetch_branches
  local current origin_tip last_tag version synced_tip shortsha
  current="$(git symbolic-ref --quiet --short HEAD 2>/dev/null || true)"
  if [[ "$current" == "$sync_branch" ]]; then
    prepared_sync_branch || fail "no prepared rebase on '$sync_branch'; run 'just sync' first"
    publish_main            # validates + force-with-lease push + switch to $main_branch
    current="$main_branch"
  fi
  if [[ "$current" != "$main_branch" ]]; then
    fail "run from '$main_branch' or '$sync_branch' (currently '${current:-detached}')"
  fi
  git merge-base --is-ancestor "$upstream_remote/$upstream_branch" HEAD \
    || fail "local '$main_branch' is not rebased onto upstream; run 'just sync' first"
  origin_tip="$(git rev-parse "$origin_remote/$main_branch")"

  last_tag="${BUZZ_LAST_RELEASE_TAG:-$(git tag --list 'relay-v[0-9]*' --sort=-v:refname | head -1)}"
  [[ -n "$last_tag" ]] || fail "no 'relay-v*' tag found for smoke baseline; set BUZZ_LAST_RELEASE_TAG"

  # Resume an in-progress release (local main is exactly one release-bump commit
  # ahead of origin/main) so a smoke failure or interruption is re-runnable.
  if [[ "$(git rev-parse HEAD)" != "$origin_tip" ]]; then
    if [[ "$(git rev-parse -q --verify HEAD^ 2>/dev/null)" == "$origin_tip" ]] \
       && git show -s --format=%s HEAD 2>/dev/null | grep -q '^chore(release): release relay '; then
      version="$(grep -m1 '^version = ' crates/buzz-relay/Cargo.toml | sed -E 's/^version = "(.*)"$/\1/')"
      [[ -n "$version" ]] || fail "in-progress release detected but crates/buzz-relay/Cargo.toml has no version"
      echo "Resuming in-progress release relay-v$version (tag may already be pushed)."
    else
      fail "local '$main_branch' diverges from '$origin_remote/$main_branch'; push or 'git reset --hard $origin_tip' before releasing"
    fi
  else
    synced_tip="$origin_tip"
    shortsha="$(git rev-parse --short "$synced_tip")"
    version="0.2.1-g$shortsha"
    if git rev-parse -q --verify "refs/tags/relay-v$version" >/dev/null 2>&1; then
      fail "release tag 'relay-v$version' already exists; not overwriting"
    fi
    perl -i -pe 's/^version = ".*"/version = "'"$version"'"/' crates/buzz-relay/Cargo.toml
    cargo update -p buzz-relay >/dev/null
    git add crates/buzz-relay/Cargo.toml Cargo.lock
    if ! git diff --cached --quiet; then
      git commit -q -s -m "chore(release): release relay $version"
    fi
    git tag "relay-v$version"
    git push "$origin_remote" "refs/tags/relay-v$version"
    echo "Committed release bump and pushed tag relay-v$version (main pushed only after smoke passes)."
  fi

  if [[ -n "${BUZZ_SKIP_SMOKE:-}" ]]; then
    [[ "$(git rev-parse HEAD)" != "$origin_tip" ]] && git push "$origin_remote" "refs/heads/$main_branch"
    echo "sync complete (smoke skipped): main published, relay-v$version tagged."
    return 0
  fi

  command -v docker >/dev/null 2>&1 \
    || fail "docker required for regression smoke; release tag 'relay-v$version' is pushed. Install docker or re-run with BUZZ_SKIP_SMOKE=1."
  local old_image="ghcr.io/chesapeakedev/buzz:${last_tag#relay-v}"
  local new_image="ghcr.io/chesapeakedev/buzz:$version"
  docker manifest inspect "$old_image" >/dev/null 2>&1 \
    || fail "smoke baseline image '$old_image' not pullable; release tag 'relay-v$version' is pushed (main is not). Set BUZZ_LAST_RELEASE_TAG to a pullable release tag or BUZZ_SKIP_SMOKE=1."
  echo "Waiting for release image $new_image to be published..."
  local i
  for i in $(seq 1 60); do
    docker manifest inspect "$new_image" >/dev/null 2>&1 && break
    sleep 20
  done
  docker manifest inspect "$new_image" >/dev/null 2>&1 \
    || fail "release image '$new_image' not published within 20 min; tag 'relay-v$version' is pushed (main is not). Re-run 'just sync-release' once the image is available, or set BUZZ_SKIP_SMOKE=1."
  echo "Running regression smoke (old=$old_image new=$new_image)..."
  if ! BUZZ_EMBEDDED_OLD_IMAGE="$old_image" BUZZ_EMBEDDED_NEW_IMAGE="$new_image" \
       scripts/test-embedded-upgrade.sh; then
    git push "$origin_remote" ":refs/tags/relay-v$version" 2>/dev/null || true
    git tag -d "relay-v$version" >/dev/null 2>&1 || true
    git reset --hard "$origin_tip" >/dev/null 2>&1 || true
    write_sentinel "smoke-failed" "[]" "regression smoke failed (old=$old_image new=$new_image); tag relay-v$version rolled back, main reset to origin."
    fail "regression smoke failed; release tag 'relay-v$version' rolled back and main reset. Inspect $(sentinel_path). Re-run 'just sync-release' after fixing the image or set BUZZ_SKIP_SMOKE=1."
  fi
  [[ "$(git rev-parse HEAD)" != "$origin_tip" ]] && git push "$origin_remote" "refs/heads/$main_branch"
  echo "sync complete: main published, relay-v$version tagged, regression smoke passed."
}

ensure_remotes
case "$mode" in
  status) fetch_branches; show_status ;;
  sync) sync_upstream ;;
  resolve) resolve_conflicts ;;
  release) release_main ;;
  publish-main) publish_main ;;
  *) usage ;;
esac
