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

The repeatable write benchmark records per-level latency, container memory,
`/data` growth, and restart recovery. Run the planned 100/1,000/10,000-client
matrix on a suitably sized host, or override it for a local smoke run:

```bash
BUZZ_EMBEDDED_IMAGE=ghcr.io/chesapeakedev/buzz:main \
  BUZZ_BENCH_LEVELS=100:50,1000:100,10000:250 \
  ./scripts/benchmark-embedded.sh
```

The benchmark is evidence for capacity planning, not a claim that SQLite or
filesystem storage replaces PostgreSQL/Redis/S3 for high-concurrency relays. It
raises the per-identity WebSocket event quota for synthetic traffic only; a
normal Compose deployment retains the production default of 50 events/second.
On the current local image, 20 clients at 5 qps each completed with zero
rejected writes (p50 about 1.65 ms); 20 clients at 10 qps each timed out on
later writes. Treat that as an operator-facing SQLite capacity boundary until
another host produces stronger evidence, and keep high-concurrency workloads
on the distributed profile.

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
