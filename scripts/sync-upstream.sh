#!/usr/bin/env bash
# Prepare and publish a reviewed linear replay of the fork onto block/buzz.
set -euo pipefail

mode="${1:-}"
origin_remote="${BUZZ_ORIGIN_REMOTE:-origin}"
upstream_remote="${BUZZ_UPSTREAM_REMOTE:-upstream}"
upstream_url="${BUZZ_UPSTREAM_URL:-https://github.com/block/buzz.git}"
main_branch="${BUZZ_MAIN_BRANCH:-main}"
sync_branch="${BUZZ_SYNC_BRANCH:-upstream-sync}"
base_branch="${BUZZ_UPSTREAM_BASE_BRANCH:-upstream-base}"
upstream_branch="${BUZZ_UPSTREAM_BRANCH:-main}"
pr_title="${BUZZ_SYNC_PR_TITLE:-chore(upstream): replay fork onto block/buzz main}"

fail() {
  echo "Error: $*" >&2
  exit 1
}

usage() {
  echo "Usage: $0 status|sync|pr|finalize" >&2
  exit 1
}

remote_identity() {
  local url="$1"
  case "$url" in
    *github.com:*)
      printf '%s\n' "${url##*github.com:}" | sed -E 's|^/+||; s|\.git/?$||'
      ;;
    *github.com/*)
      printf '%s\n' "${url##*github.com/}" | sed -E 's|^/+||; s|\.git/?$||'
      ;;
    *)
      printf '%s\n' "${url%/}"
      ;;
  esac
}

ensure_repository() {
  git rev-parse --git-dir >/dev/null 2>&1 ||
    fail "run this command from inside a Git repository"
}

ensure_remotes() {
  git remote get-url "$origin_remote" >/dev/null 2>&1 ||
    fail "required fork remote '$origin_remote' is not configured"

  if ! git remote get-url "$upstream_remote" >/dev/null 2>&1; then
    git remote add "$upstream_remote" "$upstream_url"
    echo "Added $upstream_remote remote: $upstream_url"
  fi

  local actual expected
  actual="$(git remote get-url "$upstream_remote")"
  expected="$(remote_identity "$upstream_url")"
  if [[ "$(remote_identity "$actual")" != "$expected" ]]; then
    fail "remote '$upstream_remote' points to '$actual'; expected '$upstream_url'"
  fi
}

fetch_branches() {
  git fetch "$origin_remote" \
    "refs/heads/$main_branch:refs/remotes/$origin_remote/$main_branch" --no-tags
  git fetch "$upstream_remote" \
    "refs/heads/$upstream_branch:refs/remotes/$upstream_remote/$upstream_branch" --no-tags

  for branch in "$sync_branch" "$base_branch"; do
    if git ls-remote --exit-code --heads "$origin_remote" "$branch" >/dev/null 2>&1; then
      git fetch "$origin_remote" \
        "refs/heads/$branch:refs/remotes/$origin_remote/$branch" --no-tags
    else
      git update-ref -d "refs/remotes/$origin_remote/$branch"
    fi
  done
}

require_clean_tree() {
  if [[ -n "$(git status --porcelain=v1)" ]]; then
    fail "working tree is not clean; commit or stash changes before syncing"
  fi
}

require_identity() {
  git config user.name >/dev/null 2>&1 ||
    fail "git user.name is required to replay signed-off commits"
  git config user.email >/dev/null 2>&1 ||
    fail "git user.email is required to replay signed-off commits"
}

checkout_sync_branch() {
  local current
  current="$(git symbolic-ref --quiet --short HEAD)" ||
    fail "detached HEAD is unsupported; switch to '$main_branch' first"

  if [[ "$current" != "$main_branch" && "$current" != "$sync_branch" ]]; then
    fail "run from '$main_branch' or an existing '$sync_branch' branch (currently '$current')"
  fi

  if [[ "$current" == "$main_branch" ]] &&
    [[ "$(git rev-parse HEAD)" != "$(git rev-parse "$origin_remote/$main_branch")" ]]; then
    fail "local '$main_branch' is not aligned with '$origin_remote/$main_branch'; update it first"
  fi

  git switch -C "$sync_branch" "$origin_remote/$main_branch"
}

show_status() {
  local fork_only upstream_only
  read -r fork_only upstream_only < <(
    git rev-list --left-right --count \
      "$origin_remote/$main_branch...$upstream_remote/$upstream_branch"
  )
  echo "Fork-only commits: $fork_only"
  echo "Upstream-only commits: $upstream_only"
  echo "Fork main: $(git rev-parse "$origin_remote/$main_branch")"
  echo "Upstream main: $(git rev-parse "$upstream_remote/$upstream_branch")"
}

sync_upstream() {
  require_clean_tree
  require_identity
  fetch_branches
  show_status

  if [[ "$(git rev-parse "$upstream_remote/$upstream_branch")" == \
    "$(git merge-base "$origin_remote/$main_branch" "$upstream_remote/$upstream_branch")" ]]; then
    echo "Fork main already uses the current upstream base; nothing to sync."
    return 0
  fi

  local previous_base
  previous_base="$(
    git merge-base "$origin_remote/$main_branch" "$upstream_remote/$upstream_branch"
  )"
  checkout_sync_branch
  git rebase --onto "$upstream_remote/$upstream_branch" "$previous_base" "$sync_branch"
  echo "Prepared linear fork patch stack on '$sync_branch' for review."
}

lease_value() {
  local branch="$1"
  git rev-parse --verify --quiet "refs/remotes/$origin_remote/$branch" || true
}

push_with_lease() {
  local source="$1"
  local destination="$2"
  local expected
  expected="$(lease_value "$destination")"
  git push "$origin_remote" "$source:refs/heads/$destination" \
    "--force-with-lease=refs/heads/$destination:$expected"
}

validate_patch_stack() {
  local base="$1"
  local commit subject
  while read -r commit; do
    [[ -n "$commit" ]] || continue
    subject="$(git show -s --format=%s "$commit")"
    [[ "$subject" =~ ^(feat|fix|docs|refactor|test|chore|ci)(\([a-z0-9._-]+\))?!?:[[:space:]].+ ]] ||
      fail "fork commit $commit does not use a Conventional Commits subject: $subject"
    git show -s --format=%B "$commit" | grep -Eq '^Signed-off-by: .+ <.+>$' ||
      fail "fork commit $commit is missing a DCO Signed-off-by trailer"
  done < <(git rev-list --reverse "$base..HEAD")
}

publish_pr() {
  require_clean_tree
  fetch_branches

  if [[ "$(git rev-parse "$upstream_remote/$upstream_branch")" == \
    "$(git merge-base "$origin_remote/$main_branch" "$upstream_remote/$upstream_branch")" ]]; then
    echo "Fork main already contains upstream main; no pull request is needed."
    return 0
  fi

  local current
  current="$(git symbolic-ref --quiet --short HEAD)" ||
    fail "detached HEAD is unsupported"
  [[ "$current" == "$sync_branch" ]] ||
    fail "switch to '$sync_branch' or run 'just sync-upstream' first"
  git merge-base --is-ancestor "$upstream_remote/$upstream_branch" HEAD ||
    fail "'$sync_branch' does not contain current upstream main"

  if [[ "$(git rev-list --count --merges "$upstream_remote/$upstream_branch..HEAD")" != "0" ]]; then
    fail "'$sync_branch' contains merge commits; the fork patch stack must be linear"
  fi
  validate_patch_stack "$upstream_remote/$upstream_branch"

  push_with_lease "$upstream_remote/$upstream_branch" "$base_branch"
  git update-ref "refs/remotes/$origin_remote/$base_branch" \
    "$upstream_remote/$upstream_branch"
  push_with_lease HEAD "$sync_branch"
  git update-ref "refs/remotes/$origin_remote/$sync_branch" HEAD

  command -v gh >/dev/null 2>&1 ||
    fail "GitHub CLI 'gh' is required to create or update the sync pull request"

  local repo pr body_file merge_base
  repo="${BUZZ_GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
  pr="$(gh pr list --repo "$repo" --state open --base "$base_branch" \
    --head "$sync_branch" --json number --jq '.[0].number // empty')"
  merge_base="$(git rev-parse "$upstream_remote/$upstream_branch")"
  body_file="$(mktemp)"
  trap "rm -f '$body_file'" EXIT
  {
    echo "## Upstream sync"
    echo
    echo "Replays the ChesapeakeDev fork patch stack onto"
    echo "\`block/buzz@$upstream_branch\` at \`$merge_base\`."
    echo
    echo "- Existing fork tip: \`$(git rev-parse "$origin_remote/$main_branch")\`"
    echo "- Reviewed linear tip: \`$(git rev-parse HEAD)\`"
    echo "- Strategy: linear rebase; finalize with a guarded lease update"
    echo
    echo "If CI or a future sync reports conflicts, use"
    echo "\`\$deliver-embedded-backends\` and run \`just sync-upstream\` locally."
  } >"$body_file"

  if [[ -n "$pr" ]]; then
    gh pr edit "$pr" --repo "$repo" --title "$pr_title" --body-file "$body_file"
    echo "Updated upstream sync PR #$pr."
  else
    gh pr create --repo "$repo" --base "$base_branch" --head "$sync_branch" \
      --title "$pr_title" --body-file "$body_file"
  fi
}

finalize_sync() {
  require_clean_tree
  fetch_branches

  for branch in "$sync_branch" "$base_branch"; do
    git show-ref --verify --quiet "refs/remotes/$origin_remote/$branch" ||
      fail "remote review branch '$branch' is missing; run 'just sync-upstream-pr' first"
  done

  [[ "$(git rev-parse "$origin_remote/$base_branch")" == \
    "$(git rev-parse "$upstream_remote/$upstream_branch")" ]] ||
    fail "upstream advanced after review preparation; rerun the sync"
  git merge-base --is-ancestor "$origin_remote/$base_branch" "$origin_remote/$sync_branch" ||
    fail "reviewed sync tip is not based on the reviewed upstream base"
  [[ "$(git rev-list --count --merges \
    "$origin_remote/$base_branch..$origin_remote/$sync_branch")" == "0" ]] ||
    fail "reviewed sync tip contains merge commits"

  command -v gh >/dev/null 2>&1 ||
    fail "GitHub CLI 'gh' is required to verify the upstream-sync review"
  local repo review_decision
  repo="${BUZZ_GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"
  review_decision="$(
    gh pr list --repo "$repo" --state open --base "$base_branch" \
      --head "$sync_branch" --json reviewDecision --jq '.[0].reviewDecision // empty'
  )"
  [[ "$review_decision" == "APPROVED" ]] ||
    fail "upstream-sync review is not approved"

  push_with_lease "$origin_remote/$sync_branch" "$main_branch"
  echo "Updated '$main_branch' to the approved linear upstream-sync tip."
}

ensure_repository
ensure_remotes

case "$mode" in
  status)
    fetch_branches
    show_status
    ;;
  sync)
    sync_upstream
    ;;
  pr)
    publish_pr
    ;;
  finalize)
    finalize_sync
    ;;
  *)
    usage
    ;;
esac
