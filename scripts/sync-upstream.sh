#!/usr/bin/env bash
# Rebase the fork patch stack onto block/buzz and publish it directly to the
# fork's main branch. No upstream pull request is created by this workflow.
set -euo pipefail

mode="${1:-}"
origin_remote="${BUZZ_ORIGIN_REMOTE:-origin}"
upstream_remote="${BUZZ_UPSTREAM_REMOTE:-upstream}"
upstream_url="${BUZZ_UPSTREAM_URL:-https://github.com/block/buzz.git}"
main_branch="${BUZZ_MAIN_BRANCH:-main}"
sync_branch="${BUZZ_SYNC_BRANCH:-upstream-sync}"
upstream_branch="${BUZZ_UPSTREAM_BRANCH:-main}"

fail() { echo "Error: $*" >&2; exit 1; }
usage() { echo "Usage: $0 status|sync|publish-main" >&2; exit 1; }

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
  if [[ "$(git rev-parse "$upstream_remote/$upstream_branch")" == "$previous_base" ]]; then
    echo "Fork main already uses the current upstream base; nothing to sync."
    return 0
  fi
  checkout_sync_branch
  git rebase --onto "$upstream_remote/$upstream_branch" "$previous_base" "$sync_branch"
  echo "Prepared linear fork patch stack on '$sync_branch'."
}

lease_value() { git rev-parse --verify --quiet "refs/remotes/$origin_remote/$1" || true; }

publish_main() {
  require_clean_tree
  require_identity
  fetch_branches
  sync_upstream
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

ensure_remotes
case "$mode" in
  status) fetch_branches; show_status ;;
  sync) sync_upstream ;;
  publish-main) publish_main ;;
  *) usage ;;
esac
