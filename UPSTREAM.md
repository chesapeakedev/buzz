# Upstream Synchronization

This repository is the public ChesapeakeDev fork of
[`block/buzz`](https://github.com/block/buzz). Canonical upstream changes remain
an ongoing input. Fork-only commits are replayed onto the current upstream base
in a temporary `upstream-sync` branch before the fork's `main` is updated with
an explicit force-with-lease. Fork changes publish directly to `main`; this
repository does not use pull requests against either `chesapeakedev/buzz` or
`block/buzz` unless an operator explicitly requests one.

Synchronization is initiated manually from the maintained local development
checkout when useful. There is no scheduled GitHub workflow and no prescribed
daily or weekly cadence, and upstream synchronization is never run inside
GitHub Actions. The repository `just` targets are the authoritative preparation,
validation, and publication path.

## Current baseline

- Current upstream base commit: `caa64b5e8f584a740e331887a5dd1cda32bcb958`
- Upstream branch: `block/buzz` `main`
- Fork branch: `chesapeakedev/buzz` `main`

Update the commit above in every upstream-sync publication record.

The latest sync target is `caa64b5e8f584a740e331887a5dd1cda32bcb958`. The
complete fork stack was semantically rebased onto that base in the fixed
`upstream-sync` branch. It is merge-free, Conventional Commit compliant,
DCO-signed, passes the sync and fork-release safety contract tests, and was
published directly to fork `main`. Post-sync compatibility, smoke-harness, and
maintenance-policy fixes were then fast-forwarded normally; `origin/main` is
the authoritative current publication tip.

## Rebase conflict audit

The successful 2026-08-14 rebase from fork tip `4a494673e` to
`caa64b5e8f584a740e331887a5dd1cda32bcb958` preserved upstream datastore
instrumentation, community-deletion fencing, replica-fence routing, and pool
metrics while replaying the fork's database, coordination, SQLite search/audit,
filesystem media, Git CAS, embedded configuration, and backend-injection seams.
Conflicts were resolved in `buzz-db`, `buzz-relay`, `buzz-media`, `buzz-search`,
`buzz-audit`, and `Cargo.lock`; dependency conflicts retained both dependency
sets. A signed post-rebase fix removed duplicate PostgreSQL acquisitions and
reconciled facade/store ownership exposed by warnings-denied Clippy.

The same rebase also exposed three non-textual integration conflicts after Git
had finished replaying commits: upstream changed the Git store field from a
concrete reference shape to a trait object, raised the default hosted-community
limit, and added a PostgreSQL migration whose SQLite counterpart increased the
embedded migrator count. The fixes pass trait objects with `.as_ref()` and make
tests derive expectations from `MAX_COMMUNITIES_PER_OWNER` and the embedded
migrator itself. Future fork tests should prefer authoritative constants and
registries over copied counts; these compile/test conflicts are useful sync
evidence even though they do not appear in `git diff --diff-filter=U`.

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
it narrowed the remaining work but is not the successful sync recorded below.

The successful 2026-08-10 rebase from fork tip `f372e1000` to
`07a3c768d619db31fee3f0590f9433cdd1213e8f` recorded these file-level
resolutions. Each row names the fork commit being replayed; all other commits
replayed without conflicts:

| Fork commit(s) | Conflicted file(s) | Resolution |
| --- | --- | --- |
| `c444dc339` | `README.md` | Kept upstream package documentation and restored the fork relay note. |
| `cb406ad14`, `88b49bf3b`, `0a57a0385`, `322ed6cf6`, `f4f9fa681`, `071ee8e49`, `5a494f64d`, `7df93e632`, `a8844e4d8`, `9202e1c22`, `512fe6e03`, `1f8de8107`, `18378f856`, `b676ac5ef`, `9dc772337`, `00c193fba`, `a8c8a8bc3`, `b2707c186`, `033495c64`, `6c44af253`, `c34caff12`, `0ed55a4fe`, `59202cf74` | `crates/buzz-db/src/lib.rs` | Preserved upstream replica-fence/session routing and added SQLite dispatch branches with PostgreSQL fallbacks; transactional command paths use the PostgreSQL pool only after the SQLite early return. |
| `00114fba2` | `crates/buzz-db/Cargo.toml`, `crates/buzz-db/src/lib.rs`, migration/runtime files | Retained both upstream dependencies and the SQLite runtime/migration additions. |
| `dbe65fe4b` | `crates/buzz-db/src/replica_fence.rs` | Kept upstream precision assertions and restored the fork regression assertion against the recorded timestamp. |
| `d9d767f7c` | `crates/buzz-relay/src/api/git/store.rs`, `crates/buzz-relay/src/state.rs` | Kept upstream S3 addressing configuration while preserving the backend-neutral Git storage seam. |
| `db70b1cce` | `crates/buzz-relay/src/config.rs`, `crates/buzz-relay/src/state.rs` | Kept deployment-mode configuration and injected selected backend services without recreating the Git store. |
| `b0d7d00bc` | `crates/buzz-relay/src/main.rs` | Combined embedded SQLite startup with upstream read-replica boot diagnostics. |
| `7b4e5c942` | `crates/buzz-db/src/lib.rs` | Kept the upstream page-limit export and added SQLite blob/pointer exports. |
| `19361a8e0` | `crates/buzz-relay/src/config.rs` | Preserved read-pool sizing and used the persisted embedded public URL only as the relay URL fallback. |
| `8c77b18e6` | `Cargo.lock`, `crates/buzz-relay/Cargo.toml`, `crates/buzz-relay/CHANGELOG.md` | Kept upstream changelog history and added the fork's embedded `relay-v0.3.0` section/version. |
| `f8c252dbf` | `Cargo.lock` | Retained the upstream `metrics-util` dependency. |

Post-rebase compile fixes were committed as `22aa5fbae`, `7bd2921ea`, and
`96d10959d`; they close upstream signature/configuration gaps and guard the two
new upstream write-capable workflows (`promote-oss-desktop-release.yml` and
`sprig-image.yml`) on `block/buzz`.

## Conflict-minimizing design guidance

Some overlap is inherent because the fork replaces concrete distributed
backends beneath code that upstream continues to evolve. The following patterns
from the August 2026 rebases identify where code shape can reduce the size and
semantic risk of future conflicts.

### Keep backend dispatch at stable facade boundaries

Several conflicts in `buzz-db/src/lib.rs` occurred because fork commits copied
the body of a PostgreSQL method and then inserted an early SQLite return. Later
upstream changes to the PostgreSQL body therefore overlapped the dispatch patch.
Prefer a small stable facade method that matches on `DatabaseBackend` and calls
`PostgresStore::operation` or `SqliteStore::operation`; keep each backend body in
its adapter module. This turns upstream SQL edits and fork SQLite edits into
different-file changes.

Likewise, do not retain duplicate compatibility fields on both `Db` and
`PostgresStore`. During this rebase, replica-fence and pool-limit state existed
in both places, which produced unused fields and made it unclear which copy was
authoritative. Each value should have one owner, with facade accessors
dispatching to that owner.

### Move atomic operations whole, including cross-cutting guards

The DM, workflow-definition, and approval conflicts came from moving SQL
transactions out of relay handlers while upstream added deletion fencing around
the old transaction. When transaction ownership moves into `buzz-db`, move its
full invariant envelope too: event insertion, mutation, deletion/serving guard,
commit, and idempotency result. A relay handler should call one domain operation
instead of opening or decorating a backend transaction.

This keeps future upstream guards visible as a single adapter concern and avoids
the dangerous intermediate shape where SQLite dispatch is correct but the
PostgreSQL branch silently loses an upstream fence.

### Instrument adapters, not temporary orchestration helpers

Upstream datastore spans conflicted with fork code that removed PostgreSQL-only
helpers. For backend-neutral public methods, use a neutral span at the facade
and put `system = "postgresql"` or `system = "sqlite"` instrumentation on the
adapter method. Avoid attaching a PostgreSQL span to a helper that a later fork
commit intends to delete. The audit-service resolution followed this pattern:
neutral dispatch instrumentation remained on `log`, while the datastore span
moved to `log_postgres`.

### Pass backend traits consistently at call sites

Media and Git conflicts repeatedly differed only between `&state.media_storage`
and `state.media_storage.as_ref()`. Store selected services as trait objects at
the state boundary and pass `&dyn Trait` consistently from the first seam
commit. Avoid a transitional series where some handlers still know the concrete
S3 type; broad mechanical call-site conversion then collides with unrelated
upstream safety edits in those handlers.

### Compose readiness and metrics from independent checks

The readiness conflict combined upstream's deletion-catalog gate with the
fork's optional Redis and backend status reporting. Model each check as a named
backend capability (`database_ok`, `coordination_ok`, `deletion_catalog_ok`) and
compose the final result once. Distributed-only checks should explicitly return
`true` or `not_applicable` in embedded mode rather than being removed from the
handler. Apply the same pattern to metrics: guard only the Redis pool metrics,
not adjacent backend-independent deletion metrics.

### Isolate additive exports, dependencies, and tests

Many low-risk conflicts were adjacent-line additions in `Cargo.lock`,
`Cargo.toml`, `lib.rs` re-export lists, and the end of large inline test modules.
Keep re-exports one symbol per line, sort workspace dependencies consistently,
and put backend contract tests in dedicated test files or backend modules.
These choices let Git merge independent additions and keep lockfile conflicts
mechanical: regenerate from the already-resolved manifests rather than choosing
one side.

Schema evolution needs the same pairing discipline. Upstream added
`workflow_runs.error_code` to the PostgreSQL record while the fork's SQLite
adapter still accepted only a free-form error string. The conflict compiled
far enough to expose a missing facade argument and row field. When an upstream
domain record or method changes, search every backend adapter and add the
paired SQLite migration in the same semantic-resolution commit.

### Treat safety changes as additive constraints

When upstream adds a fence, validation, metric, or regression test, preserve it
by default and adapt the fork seam around it. Examples from this rebase include
retaining community-deletion leases around media upload and Git CAS, keeping the
exclusive PostgreSQL migration/destruction lock alongside the SQLite migrator,
and retaining upstream atomic-replacement tests next to fork DM command tests.
The preferred resolution question is “where does this invariant live for each
backend?” rather than “which side wins?”

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

- With no legacy distributed-backend variables, the relay selects the
  single-process embedded profile: SQLite, local coordination, and filesystem
  media storage. Explicit or compatibility-selected distributed mode retains
  PostgreSQL, Redis, and S3 behavior.
- Embedded Git is opt-in and intended only for bounded low-volume use; it is
  not part of the default embedded product or compatibility smoke gate.
- Relay images publish from fork `main` and `relay-v*` tags to
  `ghcr.io/chesapeakedev/buzz`; inherited publication targets remain disabled.
- The embedded profile is fresh-install-only for SQLite v1; the
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
