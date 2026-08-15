# Embedded Buzz relay

This bundle runs only the relay. SQLite, local coordination, filesystem media,
and the durable data directory are provided by the image; PostgreSQL, Redis,
and MinIO are intentionally absent.

## Local-only start

```bash
docker compose -f compose.yml up -d --wait
curl -fsS http://127.0.0.1:8080/_readiness
```

To publish a relay from a device behind NAT without opening inbound ports, use
the Cloudflare Tunnel guide for [`buzz.chesapeake.dev`](cloudflare-tunnel.md).

The default `RELAY_ACCESS=open` is appropriate only when the published port is
bound to loopback. For a public deployment, copy `.env.example`, set the
owner public key as either the `npub` shown by Buzz Desktop or 64-character
hex, use `RELAY_ACCESS=closed`, and run with the Caddy override:

```bash
cp .env.example .env
docker compose --env-file .env -f compose.yml -f compose.caddy.yml up -d --wait
```

## Quick start: connect a client

The relay speaks Nostr over WebSocket. After the readiness check succeeds, set
the relay URL for a client to `ws://127.0.0.1:3000` (or the public `wss://`
URL configured in `.env`). For a quick CLI smoke test, build the repository's
agent-first client and provide a test Nostr private key:

```bash
cargo build -p buzz-cli
export BUZZ_RELAY_URL=ws://127.0.0.1:3000
export BUZZ_PRIVATE_KEY=nsec1_replace_with_a_test_key
./target/debug/buzz channels list | jq .
```

The command should return JSON (an empty array is valid on a fresh relay). The
same `BUZZ_RELAY_URL` is the value to enter in the desktop or mobile client's
community/relay settings. In Buzz Desktop, use **Join a community** and enter
the relay URL. **Create a community** and **I own the community** are entry
points for Block-hosted relay management and open Builderlab; they are not part
of self-hosted relay setup. Ownership is granted locally when the authenticated
client key matches `RELAY_OWNER_PUBKEY`.

The embedded image contains the relay and repository web bundle only. It has no
Builderlab login, API, or runtime dependency. Keep `RELAY_ACCESS=open` only for
a loopback-bound development relay; for a public relay connect with `wss://`.
Do not reuse a development private key for a real community.

## Operations, limits, and troubleshooting

The canonical operator runbook and empirical scaling record is
[`docs/embedded-operations.md`](../../docs/embedded-operations.md). It includes
backup/restore, upgrade checks, client connection guidance, troubleshooting,
the complete measured capacity table, benchmark commands, and the remaining
next-milestone evidence gates.

The focused image smoke test remains reproducible from the repository root:

```bash
BUZZ_EMBEDDED_IMAGE=ghcr.io/chesapeakedev/buzz:0.3.0 \
  ./scripts/test-embedded-compose.sh
```
