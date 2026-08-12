#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
sync_script="$script_dir/sync-upstream.sh"
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT

git_env=(
  -c user.name="Sync Test"
  -c user.email="sync-test@example.com"
  -c commit.gpgSign=false
)

commit_file() {
  local repo="$1"
  local path="$2"
  local content="$3"
  local message="$4"
  printf '%s\n' "$content" >"$repo/$path"
  git -C "$repo" add "$path"
  git -C "$repo" "${git_env[@]}" commit -q -s -m "$message"
}

setup_fixture() {
  local name="$1"
  local fixture="$test_root/$name"
  mkdir -p "$fixture"
  git init -q --bare "$fixture/upstream.git"
  git init -q --bare "$fixture/origin.git"
  git init -q -b main "$fixture/source"
  commit_file "$fixture/source" README.md base base
  git -C "$fixture/source" remote add upstream "$fixture/upstream.git"
  git -C "$fixture/source" remote add origin "$fixture/origin.git"
  git -C "$fixture/source" push -q upstream main
  git -C "$fixture/source" push -q origin main
  git --git-dir="$fixture/upstream.git" symbolic-ref HEAD refs/heads/main
  git --git-dir="$fixture/origin.git" symbolic-ref HEAD refs/heads/main
  git clone -q "$fixture/origin.git" "$fixture/work"
  git -C "$fixture/work" config user.name "Sync Test"
  git -C "$fixture/work" config user.email "sync-test@example.com"
  printf '%s\n' "$fixture"
}

run_sync() {
  local fixture="$1"
  local mode="$2"
  BUZZ_UPSTREAM_URL="$fixture/upstream.git" \
    "$sync_script" "$mode"
}

fixture="$(setup_fixture no_changes)"
output="$(cd "$fixture/work" && run_sync "$fixture" sync)"
grep -Fq "nothing to sync" <<<"$output"
[[ "$(git -C "$fixture/work" branch --show-current)" == "main" ]]

fixture="$(setup_fixture clean_rebase)"
git -C "$fixture/source" switch -q main
commit_file "$fixture/source" upstream.txt upstream upstream-change
git -C "$fixture/source" push -q upstream main
(
  cd "$fixture/work"
  run_sync "$fixture" sync
)
[[ "$(git -C "$fixture/work" branch --show-current)" == "upstream-sync" ]]
git -C "$fixture/work" merge-base --is-ancestor \
  "$(git --git-dir="$fixture/upstream.git" rev-parse main)" HEAD
[[ "$(git -C "$fixture/work" rev-list --count --merges upstream/main..HEAD)" == "0" ]]
prepared_head="$(git -C "$fixture/work" rev-parse HEAD)"
(
  cd "$fixture/work"
  run_sync "$fixture" sync
)
[[ "$(git -C "$fixture/work" rev-parse HEAD)" == "$prepared_head" ]]

(
  cd "$fixture/work"
  run_sync "$fixture" publish-main
)
[[ "$(git --git-dir="$fixture/origin.git" rev-parse main)" == \
  "$(git -C "$fixture/work" rev-parse HEAD)" ]]

fixture="$(setup_fixture fork_and_upstream)"
commit_file "$fixture/source" fork.txt fork "feat(test): add fork change"
git -C "$fixture/source" push -q origin main
git -C "$fixture/source" reset -q --hard HEAD^
commit_file "$fixture/source" upstream.txt upstream upstream-change
git -C "$fixture/source" push -q upstream main
(
  cd "$fixture/work"
  git pull -q --ff-only origin main
  run_sync "$fixture" sync
)
git -C "$fixture/work" merge-base --is-ancestor upstream/main HEAD
[[ "$(git -C "$fixture/work" rev-list --count --merges upstream/main..HEAD)" == "0" ]]
[[ "$(git -C "$fixture/work" show HEAD:fork.txt)" == "fork" ]]
git -C "$fixture/work" show -s --format=%B HEAD |
  grep -Fq "Signed-off-by:"

fixture="$(setup_fixture conflict)"
commit_file "$fixture/source" shared.txt upstream upstream-change
git -C "$fixture/source" push -q upstream main
git -C "$fixture/source" reset -q --hard HEAD^
commit_file "$fixture/source" shared.txt fork "feat(test): add conflicting fork change"
git -C "$fixture/source" push -q origin main
conflict_err="$test_root/conflict.err"
set +e
(
  cd "$fixture/work"
  git pull -q --ff-only origin main
  run_sync "$fixture" sync
) >/dev/null 2>"$conflict_err"
status=$?
set -e
[[ "$status" -ne 0 ]]
grep -Fq "upstream rebase failed" "$conflict_err"
grep -Fq "Sentinel at" "$conflict_err"
git -C "$fixture/work" rev-parse -q --verify REBASE_HEAD >/dev/null
sentinel_file="$(git -C "$fixture/work" rev-parse --absolute-git-dir)/upstream-sync-conflict.json"
[[ -f "$sentinel_file" ]]
python3 -c "import json,sys; d=json.load(open(sys.argv[1])); sys.exit(0 if d['state']=='rebase-conflict' and d['conflicted_files']==['shared.txt'] else 1)" "$sentinel_file"
git -C "$fixture/work" rebase --abort

fixture="$(setup_fixture dirty)"
printf '%s\n' dirty >"$fixture/work/untracked"
set +e
output="$(cd "$fixture/work" && run_sync "$fixture" sync 2>&1)"
status=$?
set -e
[[ "$status" -ne 0 ]]
grep -Fq "working tree is not clean" <<<"$output"

fixture="$(setup_fixture wrong_remote)"
git -C "$fixture/work" remote add upstream "$fixture/origin.git"
set +e
output="$(cd "$fixture/work" && run_sync "$fixture" status 2>&1)"
status=$?
set -e
[[ "$status" -ne 0 ]]
grep -Fq "does not point to '$fixture/upstream.git'" <<<"$output"

# resolve mode errors clearly with no sentinel
fixture="$(setup_fixture resolve_no_sentinel)"
set +e
output="$(cd "$fixture/work" && run_sync "$fixture" resolve 2>&1)"
status=$?
set -e
[[ "$status" -ne 0 ]]
grep -Fq "no conflict sentinel" <<<"$output"

# release mode errors when main is not rebased onto upstream
fixture="$(setup_fixture release_unrebased)"
commit_file "$fixture/source" upstream.txt upstream upstream-change
git -C "$fixture/source" push -q upstream main
set +e
output="$(cd "$fixture/work" && run_sync "$fixture" release 2>&1)"
status=$?
set -e
[[ "$status" -ne 0 ]]
grep -Fq "run 'just sync' first" <<<"$output"

grep -Fq 'sync-upstream-status:' "$script_dir/../Justfile"
grep -Fq 'sync-upstream:' "$script_dir/../Justfile"
grep -Fq 'sync-upstream-publish-main:' "$script_dir/../Justfile"
grep -Eq '^sync:' "$script_dir/../Justfile"
grep -Eq '^sync-resolve:' "$script_dir/../Justfile"
grep -Eq '^sync-release:' "$script_dir/../Justfile"
grep -Fq 'status|sync|resolve|release|publish-main' "$sync_script"
grep -Fq 'codex exec' "$sync_script"
grep -Fq 'test-embedded-upgrade.sh' "$sync_script"
grep -Fq '0.2.1-g' "$sync_script"
grep -Fq 'BUZZ_LAST_RELEASE_TAG' "$sync_script"
grep -Fq 'BUZZ_SKIP_SMOKE' "$sync_script"
grep -Fq 'upstream-sync-conflict.json' "$sync_script"
workflow="$script_dir/../.github/workflows/upstream-sync.yml"
! grep -Fq 'cron: "17 9 * * *"' "$workflow"
grep -Fq 'workflow_dispatch:' "$workflow"
grep -Fq "github.repository == 'chesapeakedev/buzz'" "$workflow"
grep -Fq 'contents: write' "$workflow"
grep -Fq 'run: just sync-upstream' "$workflow"
grep -Fq 'run: just sync-upstream-publish-main' "$workflow"
! grep -Fq 'pull-requests:' "$workflow"
! grep -Fq 'gh pr' "$sync_script"
grep -Fq -- '--force-with-lease=' "$sync_script"
! grep -Eq 'git merge( |$)' "$sync_script"

echo "sync-upstream contract tests passed"
