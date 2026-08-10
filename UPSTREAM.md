# Upstream Synchronization

This repository is the public ChesapeakeDev fork of
[`block/buzz`](https://github.com/block/buzz). Canonical upstream changes remain
an ongoing input. Fork-only commits are replayed onto the current upstream base
in a temporary `upstream-sync` branch before the fork's `main` is updated with
an explicit force-with-lease. This fork does not open pull requests against
`block/buzz`.

## Current baseline

- Current upstream base commit: `ab55fee81896d2b03edf5d2ca5012b715be2b93d`
- Upstream branch: `block/buzz` `main`
- Fork branch: `chesapeakedev/buzz` `main`

Update the commit above in every upstream-sync publication record.

The latest attempted sync target is
`07a3c768d619db31fee3f0590f9433cdd1213e8f`. It has not replaced the fork
baseline because the semantic rebase is not yet complete.

## Rebase conflict audit

The rebase from `137185e056c469ff613efc16f88044bc036a9dc6` to
`ab55fee81896d2b03edf5d2ca5012b715be2b93d` required these manual
resolutions:

- `2e461439` (`ci(relay): publish ChesapeakeDev container images`) conflicted
  in `.github/workflows/docker.yml`. The resolution retained upstream's
  release/debug image matrix and Block-only push-gateway jobs while reapplying
  the ChesapeakeDev image name, repository guards, OCI source/revision labels,
  and fork-owned merge guard. It replayed as `1b7cbc72b`.
- `51cf0bfd6` (`refactor(db): introduce explicit backend dispatch`) conflicted
  in `crates/buzz-db/src/lib.rs`. The resolution retained upstream's
  `AdminReportDetail` return type and new relay-invite methods while routing
  their PostgreSQL calls through the explicit `Db::postgres()` adapter. It
  replayed as `42d85da35`.

All other fork commits replayed without manual conflict resolution.

The 2026-08-09 attempt from fork tip `f933e18b0` to upstream
`5bf78671f45178f8de02ba18d3d321cbbf19cd1f` first conflicted in `README.md`
while replaying `chore(fork): establish linear upstream maintenance`. The
working resolution preserved upstream's current client-package table and
added the fork relay URL note. The next conflict was
`crates/buzz-db/src/lib.rs` while replaying
`refactor(db): introduce explicit backend dispatch`: upstream's newer
replica-fence/session layout overlaps the fork's backend-dispatch facade. The
rebase was aborted without publishing; this conflict still requires a semantic
resolution and must not be handled with an ours/theirs strategy.

The 2026-08-10 attempt from fork tip `1bf6a8358` to upstream
`07a3c768d619db31fee3f0590f9433cdd1213e8f` reproduced the same sequence:
`README.md` was resolved by retaining upstream's package table and the fork
relay URL note, then `crates/buzz-db/src/lib.rs` conflicted while replaying
`refactor(db): introduce explicit backend dispatch`. Upstream has continued to
evolve replica-fence/session routing since the previous attempt; the rebase was
aborted cleanly and no fork `main` rewrite was published.

An isolated semantic-resolution experiment based on fork tip `b7777d557`
successfully replayed the dispatch seam, SQLite storage-runtime commit,
replica-fence precision test, and Git CAS abstraction. It then reached the
next overlap at `88b49bf3b` (`feat(db): add SQLite facade construction`), where
upstream's replica-routing methods and the fork's SQLite dispatch branches
modify the same `Db` methods. That experiment was aborted without publication;
it narrows the remaining work but is not a successful sync.

## Intentional omissions

- Block and Square internal release authority, signing credentials, GitHub
  Apps, ruleset identifiers, and private-repository handoffs are not available
  to this fork.
- Desktop auto-update publishing remains disabled until ChesapeakeDev owns a
  separate signing and updater channel.
- Helm, mobile, desktop, Sprig, and push-gateway publication are disabled until
  each lane has a ChesapeakeDev owner, secrets inventory, and explicit need.
  Relay container publication is the sole fork-owned release lane.
- Client CI (desktop, mobile, web, Windows Tauri/mesh builds, Sprig builds)
  remains restricted to `block/buzz`. The fork's supported client is web,
  exercised locally against the relay; Actions covers relay lint, unit tests,
  backend integration, relay E2E, security, cross-compile, and fork-release
  safety only.

## Known semantic differences

- No runtime semantic differences have been introduced yet.
- Relay images publish from fork `main` and `relay-v*` tags to
  `ghcr.io/chesapeakedev/buzz`; inherited publication targets remain disabled.
- The planned embedded profile is fresh-install-only for SQLite v1; the
  PostgreSQL/Redis/S3 distributed profile remains supported.

## Upstream compatibility

Backend-neutral database, coordination, search, audit, object-store seams, and
shared contract tests remain intentionally compatible with upstream. They are
maintained in this fork and are not submitted as upstream pull requests.

## Local synchronization

```bash
just sync-upstream-status
just sync-upstream
just sync-upstream-publish-main
```

`sync-upstream` prepares the rebased fork patch stack locally without pushing.
`sync-upstream-publish-main` validates the signed, merge-free stack and
force-with-lease updates the fork's `main`; it never creates an upstream pull
request. Conflicts must be resolved file by file, preserving upstream behavior
before reapplying fork-specific changes behind narrow seams, and recorded in
this ledger.
