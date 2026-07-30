# Upstream Synchronization

This repository is the public ChesapeakeDev fork of
[`block/buzz`](https://github.com/block/buzz). Canonical upstream changes remain
an ongoing input. Fork-only commits are replayed onto the current upstream base
in a fixed `upstream-sync` branch and reviewed as a linear patch stack before
the fork's `main` is updated with an explicit lease.

## Current baseline

- Current upstream base commit: `ab55fee81896d2b03edf5d2ca5012b715be2b93d`
- Upstream branch: `block/buzz` `main`
- Fork branch: `chesapeakedev/buzz` `main`

Update the commit above in every upstream-sync pull request.

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

## Intentional omissions

- Block and Square internal release authority, signing credentials, GitHub
  Apps, ruleset identifiers, and private-repository handoffs are not available
  to this fork.
- Desktop auto-update publishing remains disabled until ChesapeakeDev owns a
  separate signing and updater channel.
- Helm, mobile, desktop, Sprig, and push-gateway publication are disabled until
  each lane has a ChesapeakeDev owner, secrets inventory, and explicit need.
  Relay container publication is the sole fork-owned release lane.

## Known semantic differences

- No runtime semantic differences have been introduced yet.
- Relay images publish from fork `main` and `relay-v*` tags to
  `ghcr.io/chesapeakedev/buzz`; inherited publication targets remain disabled.
- The planned embedded profile is fresh-install-only for SQLite v1; the
  PostgreSQL/Redis/S3 distributed profile remains supported.

## Candidate upstream contributions

- Backend-neutral database, coordination, search, audit, and object-store seams
  that are independently useful without ChesapeakeDev policy or branding.
- Shared backend contract tests and backend-neutral readiness reporting.

## Local synchronization

```bash
just sync-upstream-status
just sync-upstream
just sync-upstream-pr
just sync-upstream-finalize
```

`sync-upstream` prepares the rebased fork patch stack locally without pushing.
`sync-upstream-pr` lease-updates the `upstream-base` and `upstream-sync` review
branches and creates or refreshes their pull request. After approval,
`sync-upstream-finalize` verifies the reviewed tips and lease-updates `main`.
Conflicts must be resolved file by file, preserving upstream behavior before
reapplying fork-specific changes behind narrow seams.
