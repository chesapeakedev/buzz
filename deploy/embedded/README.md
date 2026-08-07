# Embedded Buzz relay

This bundle runs only the relay. SQLite, local coordination, filesystem media,
and the durable data directory are provided by the image; PostgreSQL, Redis,
and MinIO are intentionally absent.

## Local-only start

```bash
docker compose -f compose.yml up -d --wait
curl -fsS http://127.0.0.1:8080/_readiness
```

The default `RELAY_ACCESS=open` is appropriate only when the published port is
bound to loopback. For a public deployment, copy `.env.example`, set a
64-character owner pubkey, use `RELAY_ACCESS=closed`, and run with the Caddy
override:

```bash
cp .env.example .env
docker compose --env-file .env -f compose.yml -f compose.caddy.yml up -d --wait
```

## Backup and restore

Stop the relay, copy the entire `buzz-data` volume, and record the image tag.
Restore only into an empty volume while the relay is stopped; then start the
same or a newer compatible image and verify `/_readiness`. Do not run two
instances with the same data volume or relay key.

The image-level regression check is reproducible from the repository root:

```bash
BUZZ_EMBEDDED_IMAGE=ghcr.io/chesapeakedev/buzz:main \
  ./scripts/test-embedded-compose.sh
```

The repeatable write benchmark records per-level latency, publish and
connection errors, and captures a `benchmark_error` when the host stops a
level before the load generator can emit a summary.
container memory,
`/data` growth, and restart recovery. Run the planned 100/1,000/10,000-client
matrix on a suitably sized host, or override it for a local smoke run:

```bash
BUZZ_EMBEDDED_IMAGE=ghcr.io/chesapeakedev/buzz:main \
  BUZZ_BENCH_LEVELS=100:50,1000:100,10000:250 \
  ./scripts/benchmark-embedded.sh
```

The `QPS` value is the total target across all connections (each connection
gets an equal share). The benchmark is evidence for capacity planning, not a claim that SQLite or
filesystem storage replaces PostgreSQL/Redis/S3 for high-concurrency relays. It
raises the per-identity WebSocket event quota for synthetic traffic only; a
normal Compose deployment retains the production default of 50 events/second.
The corrected generator's local 20-client, 5-qps-total, 2-second calibration
recorded 20 accepted writes with no publish errors (p50 about 1.73 ms,
19.53 MiB). At 100 and 200 total writes/s, the same host recorded 20 publish
errors per run, so keep sustained workloads near or above 100 writes/s on the
distributed profile.

### SQLite scaling boundary

SQLite is the simple, low-throughput single-relay profile. It is not a shared
coordination layer: needing a second relay node, cross-node fan-out, failover,
or shared durable storage is the trigger to deploy PostgreSQL/Redis/S3.

| Profile | Local calibration | Recommendation |
| --- | --- | --- |
| 20 clients × 5 total writes/s | 0 publish errors; p50 ≈1.73 ms; ≈19.53 MiB relay memory | Healthy corrected calibration |
| 50 clients × 1 total write/s | 10 publish errors; p50 ≈1.90 ms; ≈32.55 MiB relay memory | Above the reliable tested envelope |
| 20 clients × 100 total writes/s | 20 publish errors; p50 ≈1.26 ms | Move sustained workloads to distributed storage |
| 20 clients × 200 total writes/s | 20 publish errors; p50 ≈1.22 ms | Not an embedded target |
| 100 clients × 100 total writes/s | 40 publish errors; p50 ≈1.28 ms; ≈20.67 MiB | Resource calibration only; not a reliable write target |
| 1,000 clients × 100 total writes/s | 1 connection reset; no writes; ≈45.02 MiB | Host admission ceiling; not an embedded target |

These are measurements, not universal SLAs. Move to the distributed profile
before sustained demand approaches 200 durable writes/s, active connections
require a second relay process, or high availability and cross-node presence
become requirements.

### Write-scaling investigation

The embedded adapter uses WAL, bounded busy timeouts, pooled reads, and one
process-local writer gate. Durable events still use `BEGIN IMMEDIATE` for the
event row and related index/mention work, so serialized commit time is the
first suspected limit. Before raising the boundary, measure writer-gate wait,
transaction/commit time, busy errors, WAL checkpoints, and post-commit fan-out
independently.
The `buzz_sqlite_writer_wait_seconds` and
`buzz_sqlite_event_transaction_seconds` histograms are built-in signals; use
them with benchmark results before changing the deployment boundary.

The safe optimization order is: inspect query plans and write amplification;
prototype bounded writer-queue backpressure; reduce redundant lookups or move
safe independent work after commit; then benchmark WAL checkpoint and
durability settings on the target disk. Do not add multiple SQLite writers,
share one SQLite file across relay processes, or weaken durability silently.
Those requirements belong on PostgreSQL/Redis/S3.

## Release notes and known limits

The embedded profile is a fresh-install deployment for this release line.
SQLite is not an in-place import target for an existing PostgreSQL database;
keep the distributed profile when migrating an established installation.
PostgreSQL/Redis/S3 remain the supported profile for high-concurrency relays,
large media collections, and multi-replica operation.

Git hosting is disabled by default. Enable `BUZZ_GIT_ENABLED=true` only for a
single-device installation with a few private repositories, infrequent pushes,
and a bounded disk quota. Git packs and pointer metadata are not a scalable
replacement for S3-backed object storage.

Before upgrading, stop the relay and take a complete `/data` backup. Restore
only into an empty volume, then verify `/_readiness` and the relay identity
before reconnecting clients. The first stable release will publish its exact
image tag and migration notes alongside the signed `relay-vX.Y.Z` release.
See [MIGRATIONS.md](MIGRATIONS.md) for the SQLite upgrade contract, rollback
boundary, and release evidence checklist.

When two immutable images are available, run the upgrade smoke explicitly:

```bash
BUZZ_EMBEDDED_OLD_IMAGE=ghcr.io/chesapeakedev/buzz:<old> \
BUZZ_EMBEDDED_NEW_IMAGE=ghcr.io/chesapeakedev/buzz:<new> \
  ./scripts/test-embedded-upgrade.sh
```

The harness rejects identical image references, reuses one `/data` volume, and
checks readiness plus relay-key continuity across the image change. It cannot
prove an upgrade from a rolling `:main` image or when the prior immutable image
is unavailable.
