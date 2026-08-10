# Embedded Backends — Remaining Work

Progress tracker for the ChesapeakeDev single-node embedded backends effort
(`docs/single-node-embedded-backends.md`). Each dependency-ordered delivery
slice must keep the PostgreSQL/Redis/S3 path green and deployable.

## Status snapshot

Completed in the linear fork patch stack (to be published on fork `main`):

- PR 1 — Fork hygiene, upstream policy, non-publishing CI
- PR 2 — ChesapeakeDev relay image publishing and release guardrails
- PR 3 — Database facade and transaction leakage removal
- PR 4 — Coordination trait plus unchanged Redis adapter
- PR 5 — Local event bus, cache invalidation, connection control
- PR 6 — Moka presence/TTL adapter (folded into PR 5 single-process backend)
- PR 7 — Durable security-window abstraction
- PR 8 — SQLite connection, migration runner, test fixture
- PR 9 — SQLite community/auth slice
- PR 10 — SQLite event/channel/thread slice
- PR 11 — SQLite search/feed slice
- PR 12 — SQLite moderation/audit slice
- PR 13 — SQLite workflow/reminder/push slice
- PR 14 — SQLite git/usage/security slice
- PR 15 — Blob/CAS interfaces plus unchanged S3 adapters
- PR 17 — Filesystem media backend
- PR 18 — Filesystem git backend
- Facade wiring — SQLite `Db` facade dispatch + selected-backend service injection
  (unnumbered integration step completing Epic 3's exit gate)
- PR 19 — Embedded config, data directory layout, durable relay identity,
  access bootstrap, recovery, and backend-neutral readiness
- PR 20 — Relay-only embedded Compose/Caddy bundle and operational runbook

Remaining:

PR19 and PR20 are complete on `upstream-sync`; the remaining release and
evidence work is tracked in PR21 below.

## PR 16 — SQLite blob metadata tables replacing sidecar JSON

The filesystem media/git backends (PR 17/18) currently persist blob metadata as
sidecar JSON via `BlobStorage::{get_sidecar, put_sidecar}`. PR 16 moves that
metadata into SQLite so a single metadata row is the atomic publication gate
for a blob write. The distributed S3 path keeps its existing sidecar JSON.

- [x] 16.1 — Add SQLite migration `0018_blob_metadata.sql` defining
      `media_objects` and `git_pointers` (community-scoped, `community_id`-led
      keys; MIME/size/upload timestamp/uploader pubkey + CAS fields). Done in
      `migrations/sqlite/0018_blob_metadata.sql`; updated the SQLite migration
      count and tenant-leading-PK lint in `crates/buzz-db/src/sqlite.rs`.
      Verified: `cargo test -p buzz-db --lib sqlite::`, `cargo test -p
      buzz-audit --lib`, `cargo test -p buzz-search --test backend_contract`,
      `cargo fmt -p buzz-db -- --check`, `cargo clippy -p buzz-db --lib`.
- [x] 16.2 — Add a backend-neutral `BlobMetadata` trait alongside the existing
      blob/CAS interfaces (`buzz-media` for media, `buzz-relay/src/api/git/store`
      for git), keeping S3 sidecar JSON as the distributed adapter. Done in
      `crates/buzz-media/src/storage.rs`: new `BlobMetadata` trait (community-
      scoped `get_metadata`/`put_metadata`/`read_mime`) with the S3
      `MediaStorage` adapter implementing it via the existing sidecar JSON path
      (404 → `None` on get). Additive — existing `BlobStorage` sidecar methods
      and callers are untouched (no behavior change). Git's pointer CAS seam is
      already backend-neutral via the existing `GitStorage` trait (delivered
      with PR 15/18); the `git_pointers` metadata adapter is built in 16.3 and
      wired into the filesystem `GitStorage` impl in 16.5. Verified: `cargo
      test -p buzz-media --lib` (104 passed), `cargo fmt -p buzz-media
      -- --check`, `cargo clippy -p buzz-media --lib`, `cargo check -p
      buzz-relay` (library compiles; relay test binaries need system
      `libssl-dev`, a pre-existing environment limitation).
- [x] 16.3 — Implement the SQLite blob metadata adapter: `media_objects`
      upsert/get/delete and `git_pointers` put/get/CAS-swap, with the metadata
      row as the atomic publication gate (write row before publishing the
      serve-gate, delete row with the blob).
- [x] 16.4 — Wire the filesystem media backend to use SQLite metadata instead
      of `_meta/{community}/{sha256}.json` sidecars; update the upload serve
      gate to the SQLite row. `FilesystemBlobStorage::open_with_metadata`
      delegates embedded metadata reads/writes to the SQLite adapter, including
      idempotency checks and MIME reads; the legacy constructor retains sidecars
      for storage-only tests and distributed behavior is unchanged. Verified:
      `cargo test -p buzz-media --test filesystem_storage`, `cargo fmt --all
      -- --check`, and `cargo check -p buzz-relay`.
- [x] 16.5 — Wire the filesystem git backend to use SQLite `git_pointers`
      metadata for pointer reads and the CAS swap instead of pointer-envelope
      files. Immutable packs, manifests, and indexes remain filesystem-backed;
      the S3 `GitStore` path is unchanged. Verified: `cargo check -p buzz-relay`
      and `cargo test -p buzz-relay --lib api::git::filesystem`.
- [x] 16.6 — Add S3/filesystem shared behavior tests and key-format
      compatibility tests; verify the distributed S3 path is unchanged. Added
      shared metadata publication, MIME, deletion, tenant-isolation, and media
      key-format coverage; the same contract runs against filesystem storage
      and the ignored live-MinIO S3 test.

## PR 19 — Embedded config, data layout, locking, and recovery (finish)

Data layout + `instance.lock` already exist; the rest of the Epic 5 runtime UX
remains.

- [x] 19.1 — `buzz.toml` reader with environment-over-TOML-over-default
      precedence and unknown-field rejection (`unknown TOML fields are startup
      errors`). Strict TOML parsing and precedence tests added.
- [x] 19.2 — Durable relay signing key generation/persistence under
      `/data/secrets/relay.key` (owner-only, never written to TOML or logs).
      The key is generated under the instance lock and reused across restarts.
- [x] 19.3 — Automatic first run + access bootstrap policy: loopback may start
      open with no owner; non-loopback requires owner + closed membership unless
      `access = "open"`; the embedded README supplies actionable first-run
      commands.
- [x] 19.4 — Startup recovery sequence: create missing dirs idempotently,
      validate ownership/writability, acquire lock, open SQLite, run migrations,
      recover abandoned temp files, then report readiness. Filesystem adapters
      perform temporary-file recovery during open.
- [x] 19.5 — Backend-neutral readiness/health output reporting selected
      database, coordination, and object-store kinds; drop Postgres/Redis
      readiness requirements in embedded mode.

## PR 20 — Relay-only container, Compose/Caddy example, operational docs

- [x] 20.1 — Relay-only container image + entrypoint defaulting to
      `0.0.0.0:3000` with `/data` volume.
- [x] 20.2 — Minimal relay-only Compose + Caddy TLS example in `deploy/embedded/`;
      the distributed PostgreSQL/Redis/MinIO Compose remains separate.
- [x] 20.3 — Backup/restore (stop-and-copy `/data`), upgrade, troubleshooting,
      and security documentation in the embedded deployment README and plan.
- [x] 20.4 — End-to-end "empty VPS to connected client" runbook in the
      embedded deployment README.

## PR 21 — Embedded release candidate, benchmarks, soak, stable release

- [x] Image-level startup/restart/backup smoke harness in
      `scripts/test-embedded-compose.sh`; Docker Compose configurations and a
      locally built relay image pass the smoke gate. Published-image execution
      remains a release-candidate follow-up.
- [x] Release notes and known embedded limitations documented in
      `deploy/embedded/README.md` (fresh-install SQLite, distributed profile
      boundary, optional low-volume Git, and backup-before-upgrade policy).
- [x] Embedded migration and release evidence notes added in
      `deploy/embedded/MIGRATIONS.md`, covering additive SQLite upgrades,
      backup/restore rollback, migration visibility, SBOM, and provenance
      verification.
- [x] Two-image embedded upgrade smoke harness added at
      `scripts/test-embedded-upgrade.sh`; it refuses same-image false positives
      and checks readiness plus durable relay identity across one `/data`
      volume. A real run remains open until an older immutable image is
      available.
- [x] Focused embedded protocol evidence: NIP-11, subscription-limit handling,
      SQLite FTS search, and filesystem media upload/download pass against the
      locally built relay image. The broader capacity matrix and overnight soak
      remain open below.
- [x] Paced benchmark harness calibration: two- and ten-client workloads pass
      with zero rejected writes (p50 1.85 ms / 1.56 ms respectively), and a
      20-client, 5-qps-per-client workload passes with zero rejected writes
      (p50 1.65 ms) and restart recovery. The harness now
      batches authentication and phases writes so connection admission is not
      mistaken for steady-state throughput.
- [x] A 50-client, 1-qps-per-client resource profile also passes with zero
      rejected writes (p50 1.91 ms, 22.72 MiB observed container memory) and
      restart recovery. A 100-client run is not claimed until its admission
      and write behavior is reproducible.
- [x] SQLite write-scaling investigation recorded in the plan and operator
      README: measure writer-gate/transaction/checkpoint/fan-out latency first,
      then prototype bounded backpressure and write-amplification reductions;
      do not weaken durability or introduce multi-process SQLite writers. The
      first signal, `buzz_sqlite_writer_wait_seconds`, is now emitted by the
      shared SQLite writer gate, and successful event transactions emit
      `buzz_sqlite_event_transaction_seconds`.
- [ ] Capacity follow-up remains open: corrected evidence has 20 clients at 5
      total qps with 0 publish errors, but 20 clients at both 100 and 200 total
      qps recorded 20 publish errors. The practical distributed-profile trigger
      is therefore below 100 durable writes/s on this host; rerun the matrix on
      target hardware. A 100-client/100-qps run recorded 40 publish errors,
      ≈20.67 MiB, ≈4.83 MiB `/data`, and ≈5.36 s restart; it is resource
      calibration only, not a reliable write target. A 1k-client/100-qps run
      hit one connection reset before writes, used ≈45.02 MiB and ≈5.38 s
      restart, and is likewise a host-admission ceiling. Complete the 10k
      resource level before claiming the full gate. A 10k-client/100-qps run
      recorded 8,075 `EMFILE`/resource-busy connection errors, no writes,
      ≈64.88 MiB, ≈1.47 MiB `/data`, and ≈5.41 s restart; it confirms the host
      admission ceiling rather than an embedded capacity target. Publish and
      connection errors are recorded per level while later resource evidence continues;
      host-terminated levels now emit an explicit `benchmark_error` artifact. A
      5-second, 20-client/5-qps soak completed with 0 publish errors and restart
      recovery. A pre-fix 30-second run recorded 20 quota-induced timeouts;
      after passing the synthetic human-message quota through Compose, the
      corrected 30-second run recorded 160 accepted writes, 0 publish/connection
      errors, p50 ≈1.68 ms, p95 ≈2.16 ms, and restart recovery. This remains
      calibration evidence rather than the required overnight gate.
- [x] 21.1 — First `relay-vX.Y.Z` ChesapeakeDev release from a tag. Published
      `relay-v0.3.0` from fork `main` with the embedded distribution archive.
- [x] 21.2 — SBOM, image attestation, migration notes, known limitations.
      The tag workflow published the multi-architecture image and the release
      archive/checksum; migration and limitation notes ship in the archive.
- [ ] 21.3 — Resource benchmarks (idle + 100/1k/10k clients: memory, SQLite
      size, write/search latency, restart time) and overnight soak with restarts.
      A fresh 2026-08-10 run against `buzz-embedded-new:local` recorded idle
      10.79 MiB / 4,919,094 bytes `/data`; 100 clients at 100 writes/s passed
      (22.81 MiB, p50 1.29 ms, p95 1.70 ms); 1,000 clients used 159.8 MiB but
      exited during admission/publish; and 10,000 clients used 192.9 MiB with
      three `EMFILE` connection errors. Restart was 5,543 ms. The raw ledger
      is in `docs/embedded-operations.md`; search-latency and overnight mixed
      restart evidence remain open.
- [x] 21.4 — Stable release + in-place upgrade from prior embedded prerelease.
      The stable `relay-v0.3.0` release and immutable prior
      `relay-v0.2.1-embedded.1` release are published. GitHub Actions run
      `31425422178` passed the two-image harness with GHCR-authenticated
      manifests, readiness, one shared `/data` volume, and relay-key
      continuity. Distinct local images also passed previously.
- [ ] 21.5 — Daily upstream-sync workflow and first successful direct
      fork-main sync (no upstream pull request). The workflow is active, but
      the 2026-08-10 target `07a3c768d` still conflicts in
      `crates/buzz-db/src/lib.rs` while replaying the backend-dispatch seam;
      the conflict and clean abort are recorded in `UPSTREAM.md`.
