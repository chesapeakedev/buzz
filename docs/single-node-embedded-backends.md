# ChesapeakeDev Buzz Fork: IRC-like Single-Node Deployment

## Fork Objective

Create and maintain `chesapeakedev/buzz` as a public fork of `block/buzz`
focused on IRC-like self-hosting:

- one process or container;
- one durable data directory;
- one small, understandable configuration file;
- no mandatory PostgreSQL, Redis, MinIO, Kubernetes, or hosted control plane;
- an open protocol and clients that can connect without a fork-specific account
  service;
- a documented path from a laptop or inexpensive VPS to the existing
  distributed architecture.

Retain the Buzz name, protocol, desktop/mobile compatibility, and Apache-2.0
license during the first implementation. Do not begin with a product rename,
client fork, protocol fork, or PostgreSQL-to-SQLite migration utility. Those
would obscure the deployment goal and make upstream synchronization harder.

## Summary

Make the default self-hosting experience one relay process, one writable
directory, and one command:

```bash
docker run -p 127.0.0.1:3000:3000 -v buzz-data:/data \
  -e BUZZ_BIND=0.0.0.0:3000 -e RELAY_ACCESS=open \
  ghcr.io/chesapeakedev/buzz:main
```

The embedded profile uses SQLite for both relational data and blob metadata,
content-addressed filesystem object storage, Tokio local signaling, and Moka
for bounded TTL state. PostgreSQL/Redis/S3 remain the
distributed production profile. Existing deployments retain their current
behavior, and v1 supports fresh SQLite installations only.

Git hosting is deliberately not an embedded requirement. A local filesystem
Git store is reasonable for a single device that keeps a few private source or
configuration repositories, with infrequent pushes and a bounded disk quota.
It is not a substitute for object storage on a busy relay: concurrent clones,
large histories, and many repositories should use the distributed
PostgreSQL/Redis/S3 profile. Embedded Git is therefore disabled by default and
is enabled explicitly with `BUZZ_GIT_ENABLED=true` after the operator accepts
those limits.

The first ChesapeakeDev release publishes the relay container and source. It
does not initially reproduce Block's signed desktop/mobile release lanes.
Upstream clients remain usable by entering the fork relay's `ws://` or `wss://`
URL.

## Fork Establishment and Governance

### Create the GitHub fork

1. Create the public fork at `chesapeakedev/buzz` through GitHub's fork UI or:

   ```bash
   gh repo fork block/buzz --org chesapeakedev --clone=false
   ```

2. Clone the fork into a new sibling checkout and retain the canonical project
   as `upstream`:

   ```bash
   git clone git@github.com:chesapeakedev/buzz.git chesapeakedev-buzz
   cd chesapeakedev-buzz
   git remote add upstream https://github.com/block/buzz.git
   git fetch upstream --tags
   ```

3. Keep `main` as the default branch. Protect it with:
   - pull requests required for ordinary feature changes;
   - at least one approval;
   - required conversation resolution;
   - required ChesapeakeDev CI checks;
   - linear history required;
   - force updates restricted to the reviewed upstream-sync maintainer flow;
   - branch deletion disabled;
   - signed commits encouraged, with the existing DCO check retained.

4. Create labels for `fork-maintenance`, `embedded`, `sqlite`, `coordination`,
   `filesystem-storage`, `upstream-sync`, and `release`.

5. Record the fork relationship and goals in the README without removing the
   original copyright, license, or upstream attribution.

### Establish the upstream policy

- Treat `upstream/main` as the base for a maintained, linear fork patch stack.
- Run a daily scheduled workflow that fetches upstream and opens or refreshes
  an `upstream-sync` pull request.
- Rebase the fork-only commit series onto the current `upstream/main` in the
  fixed `upstream-sync` branch. Keep an `upstream-base` mirror branch so the
  review PR shows only the fork patch stack.
- Never auto-apply the rewritten stack to `main`. Require the normal CI matrix
  and review
  conflicts in database, pub/sub, storage, deployment, and release files.
- After approval, update `main` with `--force-with-lease` against the reviewed
  old tip. Rewriting the fork patch stack is intentional; unrestricted force
  pushes remain prohibited.
- Keep fork-specific changes concentrated behind backend interfaces,
  configuration modules, and deployment assets to minimize recurring conflicts.
- Add an `UPSTREAM.md` ledger containing:
  - current upstream base commit;
  - intentionally omitted upstream features;
  - known semantic differences;
  - any fork patches that should eventually be proposed upstream.
- Prefer contributing backend-neutral refactors upstream when they are useful
  independently of SQLite or the ChesapeakeDev deployment profile.

Local and scheduled synchronization use the same commands:

```bash
just sync-upstream-status
just sync-upstream
just sync-upstream-pr
just sync-upstream-finalize
```

`sync-upstream` rebases fork-only commits onto the current upstream base on the
fixed `upstream-sync` branch without pushing. `sync-upstream-pr` updates the
review branches with leases and creates or refreshes a PR against the
`upstream-base` mirror. After approval, `sync-upstream-finalize` verifies the
review and lease-updates `main` to the reviewed linear history. The daily
workflow uses the repository
`GITHUB_TOKEN`; repository administrators must enable the Actions setting that
allows workflows to create pull requests. CI runs created by that token require
maintainer approval. If a rebase conflicts, use the repository-scoped
`$deliver-embedded-backends` Codex skill to reproduce and resolve it locally.

### Remove Block-specific release authority

Before enabling Actions with write permissions:

- Disable or delete fork runs for signed macOS canaries, mobile release
  candidates, Block release-app tag creation, and other workflows guarded to
  `block/buzz`.
- Remove dependencies on the private `buzz-release-bot`, Block signing
  certificates, Block ruleset IDs, `squareup/*` repositories, and internal
  release handoffs.
- Change repository/package defaults to:
  - source: `https://github.com/chesapeakedev/buzz`;
  - relay image: `ghcr.io/chesapeakedev/buzz`;
  - optional charts: `oci://ghcr.io/chesapeakedev/buzz/charts`;
  - container attestation owner: `chesapeakedev`.
- Keep upstream URLs only where they are intentional attribution, protocol
  documentation, or comparison links.
- Add a CI guard that rejects new publishing references to `ghcr.io/block/*`
  and new release mutations targeting `block/buzz`.
- Leave desktop auto-update publishing disabled until ChesapeakeDev owns
  signing keys and intentionally creates a separate updater channel.

### ChesapeakeDev CI and releases

- Start with pull-request CI for Rust formatting, Clippy, unit tests, desktop
  lint/tests, and embedded backend tests.
- Publish multi-architecture relay images from `main` as `main` and
  `sha-<short>`.
- Publish stable relay images from `relay-vX.Y.Z` tags as `X.Y.Z` and `latest`;
  use GitHub environments for release approval.
- Enable GitHub artifact attestations and package retention policies.
- Keep PostgreSQL/Redis integration jobs in CI so the distributed backend
  cannot silently regress.
- Do not enable Helm, desktop, mobile, Sprig, or push-gateway publishing until
  each lane has a ChesapeakeDev owner, secrets inventory, and explicit need.

## Architecture and Backend Changes

- Add `embedded` and `distributed` deployment modes to the existing relay
  binary.
  - With no legacy backend variables, select `embedded`.
  - Existing `DATABASE_URL`, `REDIS_URL`, or S3 configuration continues to
    select `distributed`.
  - Explicit embedded mode rejects read replicas, Redis, mesh, or other
    multi-process settings.
  - Acquire an OS-level exclusive lock under `/data` so two embedded relay
    processes cannot use the same state directory.
  - Expose the selected mode in startup logs, status output, and metrics without
    leaking filesystem paths or credentials.

- Refactor `buzz-db` behind its existing `Db` facade.
  - Internally dispatch to `PostgresStore` or `SqliteStore`; do not use
    `AnyPool`, because the schemas, locking, search, arrays, and SQL syntax
    differ substantially.
  - Stop exposing `PgPool` and PostgreSQL transactions to relay code. Move
    compound operations such as command execution behind atomic `Db` methods.
  - Update search, audit, administration, workflows, and metrics to consume
    backend-neutral services.
  - Preserve PostgreSQL queries and migrations with minimal behavioral changes.
  - Use explicit backend dispatch rather than a broad ORM rewrite. Domain
    validation, event conversion, and access-control logic remain shared; SQL
    and transaction mechanics remain backend-specific.
  - Replace the PostgreSQL transaction returned by `Db::begin_transaction`
    with higher-level atomic domain operations so callers cannot depend on a
    concrete SQLx driver.

- Add a separate SQLite migration stream.
  - Store UUIDs as canonical text, binary identifiers as BLOBs, JSON as
    validated text, and timestamps as UTC integer values.
  - Use WAL mode, foreign keys, a busy timeout, `synchronous=NORMAL`, a small
    read pool, and a single async writer gate.
  - Replace PostgreSQL advisory locks and `SKIP LOCKED` with serialized
    `BEGIN IMMEDIATE` transactions; single-process schedulers and outbox workers
    need no leader election.
  - Use ordinary unpartitioned event tables. Disable partition management and
    replica-fence behavior for SQLite.
  - Maintain tenant isolation through mandatory `community_id` predicates and
    composite constraints.
  - Implement search with SQLite FTS5, preserving authorization, filters,
    pagination, excluded kinds, and phrase/word behavior. Ranking and
    tokenization may differ from PostgreSQL.
  - Maintain an independent `migrations/sqlite` history. Start with a flattened
    current-schema baseline because v1 only supports fresh installs; require
    paired PostgreSQL and SQLite migrations for later cross-backend schema
    changes.
  - Replace database-generated UUIDs and timestamps with application-generated
    values where doing so makes results consistent across backends.
  - Keep SQLite's durable security tables and primary domain tables in the same
    database so backup and restore have one relational consistency boundary.

- Replace Redis in embedded mode with purpose-specific local components.
  - Use existing Tokio broadcast channels for event fan-out, cache
    invalidation, and connection-control signals.
  - Use Moka's async cache for presence and other TTL state, bounded by a
    configurable default of 100,000 entries. Moka already exists in the
    workspace; enable its `future` feature for `moka::future::Cache`, which
    provides concurrent access and per-entry expiry;
    [`quick_cache`](https://docs.rs/quick_cache/latest/quick_cache/) lacks
    built-in expiration policy, while
    [`scc::HashCache`](https://docs.rs/scc/latest/scc/) is not primarily a TTL
    store. See the
    [Moka cache API](https://docs.rs/moka/latest/moka/future/struct.Cache.html).
  - Persist NIP-98 replay claims and every security-relevant fixed-window
    counter in SQLite using unique inserts/upserts and indexed expiry columns.
    Errors fail closed.
  - Run bounded cleanup for expired replay claims and rate windows.
  - Reject mesh startup in embedded mode; huddles remain local to the one relay
    process.
  - Keep the Redis implementation unchanged for distributed mode.
  - Do not expose a generic Redis-compatible API. Define coordination
    operations in terms of Buzz behavior: publish events, presence, replay
    claims, rate windows, cache invalidation, and connection control.

- Add the filesystem media backend and keep Git hosting optional.
  - Introduce backend-neutral blob and compare-and-swap interfaces, retaining
    the existing S3 implementations. The blob interface is required for media;
    the Git implementation is an opt-in compatibility adapter only.
  - Map media keys directly to filesystem subdirectories, keeping keys
    identical so media objects are portable between S3 and filesystem. When
    `BUZZ_GIT_ENABLED=true`, apply the same mapping to Git pack/manifest/index
    objects; Git paths are not part of the default embedded data contract:

    | S3 prefix | Filesystem path |
    |-----------|----------------|
    | `media/<key>` | `objects/media/<sha256[:2]>/<sha256[2:4]>/<sha256>` |
    | `packs/<sha256>` | `objects/git/packs/<sha256>` |
    | `manifests/<sha256>` | `objects/git/manifests/<sha256>` |
    | `idx/<pack_digest>` | `objects/git/idx/<pack_digest>` |
    | `repos/<community>/<owner>/<repo>/pointer` | `objects/git/pointers/<community>/<owner>/<repo>/pointer` |

  - Implement writes through same-directory temporary files, flush, and atomic
    rename.
  - Serialize git pointer CAS operations per repository; the deployment-wide
    exclusive lock makes process-local coordination sufficient.
  - Preserve streaming downloads and HTTP byte ranges without loading whole
    objects into memory.
  - Fsync parent directories after durable renames where the platform supports
    it, and remove abandoned temporary files during startup recovery.
  - Store blob metadata (MIME type, size, upload timestamps, community,
    uploader pubkey) in SQLite `media_objects` and `git_pointers` tables
    rather than sidecar JSON files. The SQLite metadata row is the atomic
    publication gate for a blob write, replacing the sidecar-JSON approach
    used in the filesystem-backed S3 implementation.
  - Keep object keys and manifests wire-compatible with S3 storage so a
    future migration tool can copy objects between filesystem and S3 by
    transferring key-content pairs without rewriting content.

### Object storage architecture rationale

For large blobs (media uploads, git packs), the filesystem is the correct
production-grade choice for single-node embedded deployments — not a fallback
from S3:

- **Byte-range reads**: HTTP 206 Partial Content requires streaming sub-ranges
  of large blobs. The filesystem provides this natively through `pread` and
  `sendfile`. No embedded KV database (RocksDB, Redb, Fjall, Sled) exposes a
  partial-read API — all require loading entire values into memory before
  slicing.
- **Immutability**: Content-addressed blobs are write-once. The filesystem's
  write-then-rename with fsync provides atomicity without compaction overhead.
  No LSM-tree or B-tree compaction is needed for objects that are never
  updated in place.
- **Zero-copy I/O**: `sendfile()` copies data directly from the page cache to
  the socket. KV databases deserialize through their own buffer and cache
  layers before returning bytes to the application.
- **No background threads**: Filesystem operations use Tokio's blocking pool.
  RocksDB spawns its own compaction and flush threads that are unaware of the
  async runtime and can interfere with cooperative scheduling.
- **No C++ build dependency**: An embedded KV database with comparable
  production maturity (RocksDB) would require the full Clang/LLVM toolchain
  and add minutes to every compilation. Pure-Rust alternatives (Redb, Fjall,
  Sled) are either not production-mature or lack key-value separation for
  large blobs.

The SQLite relational database, already planned for embedded mode, is the
correct complement: it stores blob metadata (MIME type, size, uploader,
timestamps, community) with ACID guarantees, while the filesystem stores the
immutable blob bytes. This hybrid mirrors the architecture used by PocketBase,
GitHub, and Discord — content-addressed CAS on the filesystem, metadata in a
relational database.

## Embedded Runtime Layout

Use a single root, `/data` in the container and a configurable local path for a
native binary:

```text
/data/
  buzz.toml
  instance.lock
  db/
    buzz.sqlite3
    buzz.sqlite3-wal
    buzz.sqlite3-shm
  secrets/
    relay.key
  objects/               # content-addressed blob store, metadata in SQLite
    media/               # media files keyed by SHA-256
    git/                 # used only when BUZZ_GIT_ENABLED=true
      packs/             # pack files (content-addressed by SHA-256)
      manifests/         # manifest JSON (content-addressed by SHA-256)
      idx/               # pack index sidecars (keyed by pack digest)
      pointers/          # mutable CAS pointer files per repo
  work/
    git/
    uploads/
```

- `db`, `secrets`, and `objects` are durable and included in backups. Blob
  metadata is stored in the SQLite database (not sidecar JSON files), keeping
  metadata and blobs within a single backup scope.
- `work` contains reconstructable temporary state and may be cleared only while
  the relay is stopped.
- Private files are created with owner-only permissions on Unix.
- Paths are resolved under the canonical data directory; configuration cannot
  escape it through `..`, symlinks, or absolute object keys.
- Startup creates missing directories idempotently, validates ownership and
  writability, acquires `instance.lock`, opens SQLite, runs migrations, performs
  temporary-file recovery, and only then reports readiness.

## Deployment and Configuration UX

- Add a small `/data/buzz.toml`, created automatically on first start, with
  environment variables taking precedence:

  ```toml
  [server]
  public_url = "ws://localhost:3000"
  bind = "127.0.0.1:3000"

  [community]
  access = "open"
  # owner_pubkey = "<npub-or-hex>"
  ```

- Persist the generated relay signing key in a separate
  permission-restricted file under `/data`; never write private keys into TOML
  or logs.
- A loopback-bound embedded relay may start open with no owner. Binding to a
  non-loopback address requires an owner and closed membership unless
  `access = "open"` is explicitly configured.
- Provide a minimal public example requiring only the advertised URL, owner
  npub, port, and mounted directory. TLS may remain an optional Caddy/reverse
  proxy concern.
- Native embedded mode defaults to `127.0.0.1:3000`. The container image
  defaults to `0.0.0.0:3000` inside its network namespace so Docker port
  publishing can reach it; local-only container examples restrict exposure
  with `-p 127.0.0.1:3000:3000`.
- Replace the current single-node Compose bundle with relay-only storage
  dependencies; retain a separate distributed Compose example with PostgreSQL,
  Redis, and MinIO.
- Ship `deploy/embedded/compose.yml` as the relay-only Compose example and keep
  its Caddy TLS override separate from `deploy/compose/`'s distributed stack.
- Make health output backend-neutral while reporting selected database,
  coordination, and object-store kinds. Remove PostgreSQL/Redis readiness
  requirements in embedded mode.
- Document stop-and-copy backup and restore of `/data`; consistent online
  backup tooling is deferred.

### Configuration resolution

Apply this precedence:

1. explicit environment variable;
2. `buzz.toml`;
3. detected compatibility mode;
4. built-in default.

Configuration behavior:

- `BUZZ_DEPLOYMENT_MODE=embedded|distributed` is the explicit selector.
- With no backend variables, use embedded mode.
- Presence of a PostgreSQL `DATABASE_URL`, `REDIS_URL`, or S3 credentials
  selects distributed compatibility mode unless deployment mode was explicit.
- Embedded mode accepts `BUZZ_DATA_DIR` and common server/community variables
  but rejects distributed-only configuration instead of silently ignoring it.
- `BUZZ_GIT_ENABLED` defaults to `false` in embedded mode and `true` in
  distributed mode; changing it does not change database, coordination, or
  media backend selection.
- Distributed mode retains the existing environment-variable contract and
  defaults for established deployments.
- Unknown TOML fields are startup errors, preventing misspelled security
  settings from being ignored.
- Secrets may be supplied through existing environment variables, but generated
  secrets are stored only under `/data/secrets`.

### First-run commands

Local native evaluation:

```bash
buzz-relay
```

Local container evaluation, explicitly bound to loopback:

```bash
docker run --name buzz \
  -p 127.0.0.1:3000:3000 \
  -v buzz-data:/data \
  -e BUZZ_BIND=0.0.0.0:3000 \
  -e RELAY_ACCESS=open \
  ghcr.io/chesapeakedev/buzz:main
```

Public deployment:

```bash
docker run --name buzz \
  -p 3000:3000 \
  -v buzz-data:/data \
  -e BUZZ_BIND=0.0.0.0:3000 \
  -e RELAY_ACCESS=open \
  -e RELAY_URL=wss://buzz.example.com \
  -e RELAY_OWNER_PUBKEY=<owner-pubkey-hex> \
  ghcr.io/chesapeakedev/buzz:main
```

The public example assumes TLS termination by Caddy, another reverse proxy, or
the host platform. The relay remains the only required application service and
storage dependency.

For a reproducible Compose deployment, see
[`deploy/embedded/README.md`](../deploy/embedded/README.md). It includes the
relay-only stack, the Caddy override, and the stop-and-copy backup/restore
procedure. Keep the distributed stack under `deploy/compose/` for scaled
PostgreSQL/Redis/S3 deployments.

### Access and bootstrap policy

- Embedded native startup bound only to loopback may default to open access.
- A non-loopback bind fails startup without an owner and closed membership,
  unless the operator explicitly sets `access = "open"`.
- Generate the relay signing key automatically; never generate a user's owner
  identity silently.
- Print actionable first-run instructions showing the relay URL, config path,
  public/closed status, and how to add the relay in a client.
- Do not phone home, contact Builderlab, or require a ChesapeakeDev service.
- Hosted-community UI may remain in upstream clients, but the fork's
  documentation always directs self-hosters to “Join an existing community.”

## Implementation Program

### Epic 0: Fork safety and reproducibility

Deliver:

- `chesapeakedev/buzz` fork, protected `main`, DCO, labels, and upstream remote
  policy;
- fork README and `UPSTREAM.md`;
- Block-only publishing disabled;
- ChesapeakeDev CI green without privileged secrets;
- relay image published to `ghcr.io/chesapeakedev/buzz`;
- a source and image provenance check.

Exit gate: a commit on fork `main` builds and publishes a relay image without
access to any Block or Square repository, App, package, or secret.

### Epic 1: Backend seams with no behavior change

Deliver:

- backend-neutral database, search, audit, coordination, and object-store
  interfaces;
- PostgreSQL/Redis/S3 adapters implementing those interfaces;
- removal of direct `PgPool`, Redis pool, and S3 client access from relay
  handlers;
- shared backend contract-test harness;
- backend-neutral readiness, status, and metrics.

Exit gate: the distributed deployment passes the existing CI and E2E suites,
and no embedded backend is selected in production yet.

### Epic 2: Embedded coordination

Deliver:

- Tokio local event/invalidation/control buses;
- Moka-backed presence and TTL state;
- a coordination backend selected by deployment mode;
- mesh rejection and local huddle behavior;
- metrics for entry counts, evictions, event-bus lag, and dropped receivers.

Exit gate: a single PostgreSQL-backed relay runs with no Redis process and
passes fan-out, presence, moderation disconnect, replay, and rate-limit tests.

### Epic 3: SQLite relational backend

Port in vertical slices, keeping each pull request usable:

1. communities, tenant resolution, relay membership, identities, and API tokens;
2. event insert/query, replacement rules, channels, DMs, threads, reactions,
   and counters;
3. FTS5 search, feeds, pagination, and deletion;
4. moderation, archived identities, audit chains, and administration;
5. workflows, approvals, schedules, reminders, durable push leases/outbox, and
   storage sweeps;
6. git registry, usage reports, security replay claims, and rate windows.

For each slice:

- add SQLite schema and implementation;
- run the same domain contract against PostgreSQL and SQLite;
- add concurrency and cross-community isolation cases;
- keep the PostgreSQL implementation unchanged except for interface adaptation.

Exit gate: the complete relay E2E suite passes against SQLite without
PostgreSQL or Redis available.

### Epic 4: Content-addressed filesystem with SQLite metadata (Git optional)

Deliver:

- content-addressed filesystem media, with an optional Git backend mapping S3 key prefixes
  to subdirectories (`packs/`, `manifests/`, `idx/`, `pointers/`);
- filesystem git pack/manifest/pointer backend as an opt-in compatibility capability;
- SQLite `media_objects` and `git_pointers` tables replacing sidecar JSON
  for ACID blob metadata;
- atomic writes, per-repository CAS, recovery, ranges, streaming, quotas, and
  traversal protection;
- S3/filesystem shared behavior tests and key-format compatibility.

Git is not on the embedded server critical path or its acceptance gate. It is
only justified for a single-device owner with a few private repositories,
infrequent pushes, and a bounded disk quota. A relay with significant
concurrent clone/push traffic, large histories, or many repositories must use
the distributed PostgreSQL/Redis/S3 profile; this plan does not treat a
filesystem Git store as a scalable object-storage substitute.

Exit gate: media E2E tests pass after relay restart with MinIO and external S3
unavailable. Git storage is tested separately when `BUZZ_GIT_ENABLED=true`.
Embedded mode must remain usable with Git disabled.

### Epic 5: IRC-like deployment product

Deliver:

- automatic embedded first run;
- `buzz.toml`, `/data` layout, durable secrets, and exclusive lock;
- relay-only container and minimal Compose/Caddy example;
- backup/restore, upgrade, troubleshooting, and security documentation;
- a release artifact and end-to-end “empty VPS to connected client” runbook.

Exit gate: a clean Linux host with Docker can start Buzz, create/join a
community, send/search messages, upload media, restart, and restore from backup
without installing or configuring a database, cache, or object store. Git is
not part of the default embedded smoke path: set `BUZZ_GIT_ENABLED=true` only
for a bounded, low-volume single-device repository use case.

### Epic 6: Release and upstream maintenance

Deliver:

- first `relay-vX.Y.Z` ChesapeakeDev release;
- SBOM, image attestation, migration notes, and known limitations;
- daily upstream-sync workflow and first successful sync PR;
- compatibility policy covering database schema, clients, and Nostr event
  behavior.

Exit gate: the release can be reproduced from its tag, upgraded in place from
the previous embedded prerelease, and synchronized with a newer upstream commit
without bypassing tests.

## Pull Request Sequence

1. Fork hygiene, upstream policy, and non-publishing CI.
2. ChesapeakeDev relay image publishing and release guardrails.
3. Database facade and transaction leakage removal.
4. Coordination trait plus unchanged Redis adapter.
5. Local event bus, cache invalidation, and connection control.
6. Moka presence/TTL adapter.
7. Durable security-window abstraction.
8. SQLite connection, migration runner, and test fixture.
9. SQLite community/auth slice.
10. SQLite event/channel/thread slice.
11. SQLite search/feed slice.
12. SQLite moderation/audit slice.
13. SQLite workflow/reminder/push slice.
14. SQLite git/usage/security slice.
15. Blob/CAS interfaces plus unchanged S3 adapters.
16. SQLite blob metadata tables (`media_objects`, `git_pointers`) replacing
    sidecar JSON.
17. Filesystem media backend.
18. Optional filesystem git backend (non-blocking compatibility lane).
19. Embedded config, data layout, locking, and recovery.
20. Relay-only container, Compose/Caddy example, and operational docs.
21. Embedded release candidate, resource benchmarks, soak test, and stable
    release.

Each stage must keep the PostgreSQL/Redis/S3 path green and deployable.

## Test and Acceptance Plan

- Run a shared database contract suite against PostgreSQL and SQLite for every
  public `Db` operation, including cross-community isolation and transactional
  race cases.
- Run the existing relay E2E suite against both distributed and embedded
  profiles. Search tests compare allowed result sets and pagination invariants,
  not backend-specific rank order.
- Verify NIP-98 replay rejection and rate counters survive relay restarts;
  verify presence and typing state intentionally do not.
- Stress concurrent event insertion, replaceable events, thread counters,
  workflow claims, audit-chain appends, and git CAS under SQLite.
- Test filesystem traversal rejection, atomic replacement, byte ranges,
  interrupted writes, and restart recovery.
- Add configuration tests for legacy distributed auto-detection, embedded
  defaults, environment-over-TOML precedence, unsafe public-open warnings, and
  invalid embedded/distributed combinations.
- Add a container smoke test that starts from an empty volume with no
  PostgreSQL, Redis, or MinIO network access and exercises channels, messages,
  search, media, workflows, restart, and backup/restore with Git disabled.
  Run Git hosting tests as a separate opt-in compatibility job with
  `BUZZ_GIT_ENABLED=true`; failures there must not make the embedded media
  release gate fail.
- Run `scripts/test-embedded-compose.sh` against the relay image for the
  startup/readiness, durable-key, restart, and stop-and-copy backup/restore
  portion of that gate; protocol workload coverage remains an explicit E2E
  follow-up before the stable release.
- Record an ADR and focused benchmark comparing Moka, `quick_cache`, and `scc`
  using Buzz presence/TTL workloads; Moka remains the selected implementation
  unless it fails the correctness or bounded-memory tests.

### Fork-specific gates

- Verify no enabled workflow requires Block GitHub Apps, signing credentials,
  internal repositories, or ruleset identifiers.
- Verify all fork-owned publication targets resolve under `chesapeakedev`.
- Verify pull requests from external forks cannot publish packages or access
  release secrets.
- Test the daily upstream-sync workflow against a temporary branch without
  mutating `main`.
- Build the relay image from a release tag and verify its OCI source/revision
  labels and GitHub attestation.

### Resource and reliability targets

- Capture an idle baseline and representative 100/1,000/10,000-client synthetic
  workloads for resident memory, SQLite size, write latency, search latency,
  and restart time.
- Require bounded Moka growth and report evictions; correctness must not depend
  on retaining a cache entry.
- Run an overnight mixed workload with messages, replacements, reactions,
  searches, workflows, media, and git while repeatedly restarting the relay.
- Inject process termination during SQLite transactions, object writes, git
  pointer swaps, and migrations; restart must either recover automatically or
  fail with an actionable non-destructive error.
- Run filesystem-full and read-only-volume tests. Writes must fail cleanly
  without corrupting existing data.

## Upgrade, Backup, and Recovery

- Apply SQLite migrations automatically under the exclusive instance lock.
- Make every migration transactional where SQLite permits it; record checksum
  and version before readiness.
- Before stable releases, test upgrades from every prior ChesapeakeDev embedded
  release still under support.
- Document the initial backup procedure as:
  1. stop the relay;
  2. copy or snapshot the entire data directory;
  3. restart the relay;
  4. verify readiness and record the backup's relay version.
- Restore only into an empty data directory using the same or a newer compatible
  relay version.
- Never reuse the relay key from one live instance in a second concurrently
  running instance.
- Defer live SQLite backup, PostgreSQL import/export, and object-store migration
  until after the embedded format is stable.

## Risks and Mitigations

- **Large database port:** `buzz-db` contains hundreds of PostgreSQL-oriented
  operations. Mitigate with vertical slices and a shared behavioral contract,
  not a big-bang SQL rewrite.
- **SQLite write contention:** serialize writes at the adapter boundary, keep
  transactions short, enable WAL/busy timeout, and benchmark high-write paths.
- **Search divergence:** promise behavioral rather than rank-identical parity
  and assert authorization independently of search ranking.
- **Security state reset:** store replay and security rate windows durably;
  limit Moka to state that is safe to lose.
- **Filesystem crash consistency:** use temporary files, fsync, rename, startup
  recovery, and fault-injection tests.
- **Fork drift:** isolate fork features behind narrow interfaces, sync upstream
  regularly, and maintain the divergence ledger.
- **Release supply chain:** begin with relay-only publishing, least-privilege
  Actions permissions, protected environments, attestations, and no inherited
  Block secrets.
- **Scope expansion:** keep rebranding, signed client distribution, live
  migration, clustering SQLite, and hosted account services outside v1.

## Assumptions

- SQLite is a fresh-install backend in v1; PostgreSQL-to-SQLite import/export is
  deferred.
- SQLite supports all single-process Buzz features, but not read replicas,
  multiple relay processes, Redis mesh fencing, or distributed leader election.
- PostgreSQL/Redis/S3 remain the default architecture for horizontally scaled
  deployments.
- External integrations such as APNs, webhooks, and TLS termination remain
  optional services, not embedded dependencies.
- No workspace-owned `unsafe` code is introduced; SQLx's SQLite driver
  statically links SQLite through its existing native binding.
- The GitHub repository remains named `buzz`, so the fork is
  `chesapeakedev/buzz`.
- ChesapeakeDev initially publishes the relay image only; upstream desktop and
  mobile clients connect by relay URL.
- The embedded profile is the recommended ChesapeakeDev self-hosting path,
  while distributed mode remains supported for upstream compatibility.
- The plan document lives at `docs/single-node-embedded-backends.md` in the
  ChesapeakeDev fork so contributors and repository-scoped agent skills share
  one versioned source of truth.
