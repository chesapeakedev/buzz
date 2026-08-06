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
