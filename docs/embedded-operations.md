# Embedded Buzz relay operations

This is the canonical operator runbook and empirical capacity record for the
SQLite/filesystem embedded relay. It covers a single relay process with no
PostgreSQL, Redis, or MinIO. The distributed PostgreSQL/Redis/S3 deployment
remains the supported profile for high throughput, shared storage, failover,
and multiple relay nodes.

## 1. Start and connect a client

For a loopback-only development relay:

```bash
cd deploy/embedded
docker compose -f compose.yml up -d --wait
curl -fsS http://127.0.0.1:8080/_readiness
```

The default `RELAY_ACCESS=open` is safe only when port 3000 is bound to
loopback. For a public relay, configure an owner key and closed access, then
enable the Caddy/TLS override:

```bash
cp .env.example .env
# Set RELAY_URL=wss://..., RELAY_OWNER_PUBKEY, and RELAY_ACCESS=closed.
docker compose --env-file .env -f compose.yml -f compose.caddy.yml up -d --wait
```

The client relay URL is `ws://127.0.0.1:3000` locally or the configured
`wss://` URL publicly. A CLI smoke test is:

```bash
cargo build -p buzz-cli
export BUZZ_RELAY_URL=ws://127.0.0.1:3000
export BUZZ_PRIVATE_KEY=nsec1_replace_with_a_test_key
./target/debug/buzz channels list | jq .
```

An empty JSON array is valid on a new relay. Enter the same URL in the desktop
or mobile client's community settings. Never reuse a development private key
for a real community.

## 2. Data, backup, restore, and upgrade

The named `buzz-data` volume contains SQLite state, filesystem objects, and the
durable relay key. To back up safely:

1. Stop the relay.
2. Copy or snapshot the complete `/data` volume and record the image tag.
3. Restart and verify `/_readiness` plus relay-key continuity.

Restore only into an empty volume while the relay is stopped. Never mount one
volume or relay key into two running instances.

The published embedded release is
[`relay-v0.3.0`](https://github.com/chesapeakedev/buzz/releases/tag/relay-v0.3.0),
with an archive and SHA-256 checksum. Verify the checksum before installation.
SQLite is fresh-install-only for this release line; migrating an existing
PostgreSQL installation requires the distributed profile.

For an immutable two-image upgrade test:

```bash
BUZZ_EMBEDDED_OLD_IMAGE=ghcr.io/chesapeakedev/buzz:<old> \
BUZZ_EMBEDDED_NEW_IMAGE=ghcr.io/chesapeakedev/buzz:0.3.0 \
  ./scripts/test-embedded-upgrade.sh
```

The harness rejects identical or rolling image references, reuses one `/data`
volume, checks readiness, and verifies relay-key continuity. A local gate run
passed with distinct images built from the pre-release embedded commit
`fc8abc3f0` and `relay-v0.3.0`. The published immutable pair was verified by
GitHub Actions run
[`31425422178`](https://github.com/chesapeakedev/buzz/actions/runs/31425422178)
using `relay-v0.2.1-embedded.1` → `relay-v0.3.0`.

## 3. Troubleshooting

| Symptom | Checks and action |
| --- | --- |
| Readiness never becomes `ready` | Run `docker compose logs relay`; check `/data` ownership, writable disk, and that no other process holds `instance.lock`. |
| Clients cannot connect | Confirm port 3000 or Caddy’s TLS port, use `ws://` locally / `wss://` publicly, and check `RELAY_URL` matches the client URL. |
| Public relay accepts unauthenticated traffic | Set `RELAY_ACCESS=closed`, configure `RELAY_OWNER_PUBKEY`, and use the Caddy override; do not expose the default open mode. |
| Data disappears after restart | Confirm the named `buzz-data` volume is mounted and never run `docker compose down -v` on the production volume. |
| Migration or write errors | Stop the relay, preserve `/data`, check disk space/read-only mounts, and restore only into an empty directory. Do not delete SQLite files to force startup. |
| Publish timeouts under load | Inspect the benchmark’s `stderr-*.log`/`stderr-soak.log` and JSON ledger. Distinguish rate-limit admission from SQLite contention before changing limits. |
| Need a second node or shared presence | Switch to PostgreSQL/Redis/S3; SQLite is process-local and cannot coordinate multiple relay nodes. |
| Git clone/push load grows materially | Keep `BUZZ_GIT_ENABLED=false` or use it only for a few private repositories with bounded disk; use S3-backed distributed storage for significant Git/object traffic. |

Useful checks:

```bash
docker compose -f deploy/embedded/compose.yml ps
curl -fsS http://127.0.0.1:8080/_liveness
curl -fsS http://127.0.0.1:8080/_readiness | jq .
docker compose -f deploy/embedded/compose.yml logs --tail=200 relay
```

## 4. Capacity boundary and empirical evidence

The benchmark `QPS` target is aggregate across all connections. It raises both
the synthetic identity's WebSocket-event and human-message quotas; normal
Compose defaults remain 50 WebSocket events/second and 60 messages/minute.
Every result below is calibration evidence from a local Docker host, not an
SLO. Publish or connection errors mean the level is not a passing capacity
target.

| Profile | Observed result | Interpretation |
| --- | --- | --- |
| 2 clients, 5 total writes/s, 1 second | 6 accepted, 0 errors, p50 ≈2.01 ms | Generator sanity check |
| 20 clients, 5 total writes/s, 2 seconds | 20 accepted, 0 errors, p50 ≈1.73 ms, ≈19.53 MiB, restart ≈5.39 s | Healthy corrected calibration |
| 50 clients, 1 total write/s, 2 seconds | 40 accepted, 10 publish errors, p50 ≈1.90 ms, ≈32.55 MiB | Above reliable tested envelope |
| 20 clients, 100 total writes/s, 2 seconds | 60 accepted, 20 publish errors, p50 ≈1.26 ms, restart ≈5.38 s | Use distributed storage for sustained demand |
| 20 clients, 200 total writes/s, 2 seconds | 60 accepted, 20 publish errors, p50 ≈1.22 ms, restart ≈5.38 s | Not an embedded target |
| 100 clients, 100 total writes/s, 1 second | 60 accepted, 40 publish errors, p50 ≈1.28 ms, ≈20.67 MiB, `/data` ≈4.83 MiB, restart ≈5.36 s | Resource calibration only |
| 1,000 clients, 100 total writes/s, 1 second | 1 connection reset, no writes, ≈45.02 MiB, `/data` ≈1.47 MiB, restart ≈5.38 s | Host admission ceiling |
| 10,000 clients, 100 total writes/s, 1 second | 8,075 `EMFILE`/resource-busy errors, no writes, ≈64.88 MiB, `/data` ≈1.47 MiB, restart ≈5.41 s | Host admission ceiling |
| 20 clients, 5 total writes/s, 5-second soak | 40 accepted, 0 publish/connection errors, restart recovery | Short calibration only |
| 20 clients, 5 total writes/s, 30-second soak (pre-fix) | 40 accepted, 20 timeouts at each connection's third message | Synthetic human-message quota was not passed through Compose; not storage evidence |
| 20 clients, 5 total writes/s, 30-second soak (corrected) | 160 accepted, 0 publish/connection errors, p50 ≈1.68 ms, p95 ≈2.16 ms; restart recovery | Sustained calibration pass; overnight gate remains open |

The search probe also supports a channel-scoped NIP-50 calibration. A local
three-query run after six synthetic writes completed with p50 0.89 ms, p95
1.54 ms, and zero errors. This is a smoke calibration only; repeat it beside
each target-host resource level before treating search latency as a capacity
claim.

Latest resource calibration (local Docker host, `buzz-embedded-new:local`,
2026-08-10; one-second levels) recorded 10.79 MiB idle relay memory and
4,919,094 bytes of `/data` before workload. The 100-client level accepted all
100 writes with p50 1.29 ms and p95 1.70 ms, used 22.81 MiB, and left
4,919,094 bytes in `/data`. The 1,000-client level used 159.8 MiB and ended
with a `wamp_bench` admission/publish failure before producing writes. The
10,000-client level used 192.9 MiB, left 4,919,094 bytes in `/data`, and
recorded three connection errors; stderr identified `Too many open files`
(`EMFILE`). Restart recovery was 5,543 ms. These are host-admission and
resource observations, not throughput promises; preserve the raw artifacts
(`benchmark-levels.jsonl`, `container-stats.txt`, `stderr-*.log`, and
`restart-time.ms`) with each target-host run.

The pre-fix 30-second failure was traced to the benchmark identity exhausting
the separate 60-messages/minute quota because Compose only passed through the
WebSocket-event override. The harness and Compose bundle now raise both
synthetic quotas and capture soak stderr. The corrected run passed; retain the
pre-fix result as an audit trail rather than attributing it to SQLite.

Operational boundary: keep embedded workloads near the validated low-throughput
envelope. Move to PostgreSQL/Redis/S3 before sustained demand approaches 100
durable writes/second, before a second relay process/node is needed, or when
shared storage, cross-node presence, failover, or high-concurrency media/Git is
required.

## 5. Scaling investigation and benchmark runbook

Run the repeatable matrix on a target host:

```bash
BUZZ_EMBEDDED_IMAGE=ghcr.io/chesapeakedev/buzz:0.3.0 \
BUZZ_BENCH_LEVELS=100:50,1000:100,10000:250 \
./scripts/benchmark-embedded.sh
```

Add authenticated NIP-50 samples for the last benchmark channel with
`BUZZ_BENCH_SEARCH_ITERATIONS=20`. The resulting `summary-search.json` and
`stderr-search.log` are appended to `benchmark-levels.jsonl` and fail the
benchmark when the search probe cannot complete.

For a controlled soak, set `BUZZ_BENCH_SOAK_SECONDS`; inspect
`benchmark-levels.jsonl`, `summary-soak.json`, `stderr-soak.log`, container
memory, `/data` growth, and `restart-time.ms`. A host-terminated level must
produce a structured `benchmark_error` rather than an empty result.

The adapter uses WAL, bounded busy timeouts, pooled reads, and one
process-local writer gate. Durable events use `BEGIN IMMEDIATE`, so measure
writer-gate wait, transaction duration, busy errors, WAL checkpoints, and
post-commit fan-out separately. Built-in metrics are
`buzz_sqlite_writer_wait_seconds` and
`buzz_sqlite_event_transaction_seconds`.

Safe investigation order:

1. Capture query plans and write amplification.
2. Prototype bounded writer-queue backpressure with explicit overload errors.
3. Reduce redundant lookups or move safe independent work after commit.
4. Benchmark WAL checkpoint and durability settings on target disks.

Do not add multiple SQLite writers, share a SQLite file across relay processes,
or silently weaken durability. Those requirements select the distributed
PostgreSQL/Redis/S3 profile.

## 6. Evidence and next milestone

The next milestone should extend this document rather than scatter new limits
through deployment notes. Remaining evidence gates are search-latency coverage
within the 100/1k/10k matrix and an overnight mixed workload with restarts.
Upstream sync is also pending semantic reconciliation of the fork's
backend-dispatch facade with upstream's newer replica-fence/session
implementation; see [`UPSTREAM.md`](../UPSTREAM.md).
