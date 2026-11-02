# Run embedded Buzz at `buzz.chesapeake.dev` with Cloudflare Tunnel

This guide runs an embedded Buzz community relay and ephemeral pairing relay on
this device using Docker Compose.
SQLite state, media objects, and the relay identity live in the `buzz-data`
Docker volume. A `cloudflared` sidecar makes an outbound connection to
Cloudflare, so the router does not need port forwarding and the host does not
need inbound ports 80 or 443 open.

The public client URLs are `wss://buzz.chesapeake.dev` and
`wss://pairing.buzz.chesapeake.dev`. Cloudflare terminates TLS and forwards
ordinary HTTP/WebSocket traffic through the tunnel to the corresponding
container on the private Compose network.

## 1. Configure this device

Docker Engine with Compose v2 is required. From the repository root:

```bash
cp deploy/embedded/.env.example deploy/embedded/.env
```

Edit `deploy/embedded/.env` and set:

```dotenv
BUZZ_IMAGE=buzz-embedded:<YYYY-MM-DD>-<short-commit>
BUZZ_DOMAIN=buzz.chesapeake.dev
RELAY_URL=wss://buzz.chesapeake.dev
BUZZ_PAIRING_DOMAIN=pairing.buzz.chesapeake.dev
BUZZ_PAIRING_RELAY_URL=wss://pairing.buzz.chesapeake.dev
RELAY_ACCESS=closed
RELAY_OWNER_PUBKEY=<owner-npub-or-64-character-hex-public-key>
BUZZ_REQUIRE_AUTH_TOKEN=true
BUZZ_GIT_ENABLED=false
BUZZ_SERVE_GIT_WEB_GUI=true
CLOUDFLARE_TUNNEL_TOKEN=<token-copied-in-step-2>
```

Use the public key, not an `nsec` private key. Buzz accepts either the `npub`
shown by Desktop or its 64-character hex representation. The `.env` file is
ignored by Git, but it is plaintext: restrict it to your user after editing it:

```bash
chmod 600 deploy/embedded/.env
```

The Compose override pins `cloudflared` by image digest. Upgrade that digest
deliberately after checking Cloudflare's release notes; `docker compose pull`
will not silently replace it with a different `latest` image.

The ChesapeakeDev package may require registry authentication. This guide
therefore prepares the device from the checked-out source. Record the commit,
choose a date-and-commit tag (for example, `2026-08-14-05cdcbf`), build that
exact checkout, and put the same tag in `BUZZ_IMAGE`:

```bash
git rev-parse HEAD
docker build -t buzz-embedded:2026-08-14-05cdcbf .
```

Record the full commit and image ID with each backup. Treat the local tag as an
operator convention, not a registry-enforced immutable reference: never move
an existing deployment tag to different contents. If a public, tested
ChesapeakeDev image is available later, pin `BUZZ_IMAGE` to its immutable
version or digest; do not use a moving `main` or `local` tag for an unattended
relay.

### Choose the desktop owner identity

Complete Buzz Desktop's identity step and make a tested backup before starting
the public relay. On **Join a community**, copy the displayed public ID and put
that `npub` directly in `RELAY_OWNER_PUBKEY`. The relay normalizes it to hex and
bootstraps that identity as the sole owner on startup.

After the relay is healthy, stay on **Join a community** and enter
`wss://buzz.chesapeake.dev`. Do not select **Create a community** or **I own the
community**: those buttons manage Block-hosted relays and intentionally open
Builderlab. They are unrelated to the embedded relay, whose runtime contains no
Builderlab client or login dependency.

If `RELAY_OWNER_PUBKEY` is changed later, recreating the relay container
promotes the new key and demotes the previous owner to admin. After verifying
the new owner can connect and manage invites, remove the retired identity from
the member list if it should no longer retain administrative access.

## 2. Push these buttons in Cloudflare

The `chesapeake.dev` zone must already be active in the same Cloudflare account.
These labels reflect Cloudflare's current main dashboard:

1. Sign in to the Cloudflare dashboard.
2. In the left sidebar, select **Networking**, then **Tunnels**.
3. Select **Create a tunnel** (or **Create Tunnel**).
4. Name it `chesapeake-buzz`, then select **Create Tunnel**.
5. On **Setup Environment**, choose **Docker**. Do not run the displayed
   `docker run` command. Copy only the long value following `--token` and put it
   in `CLOUDFLARE_TUNNEL_TOKEN` in `deploy/embedded/.env`.
6. Start the Compose stack as described in step 3 below.
7. Return to the tunnel page. Wait for the connector to show **Connected** or
   the tunnel to show **Healthy**, then select **Continue** if it is offered.
8. Select the tunnel, open **Routes**, select **Add route**, and choose
   **Published application**.
9. For **Hostname**, enter subdomain `buzz` and select domain
   `chesapeake.dev`. Leave the path empty.
10. For **Service URL**, enter exactly `http://relay:3000`.
11. Select **Add route** or **Save**. Cloudflare creates the proxied DNS record
    for `buzz.chesapeake.dev` automatically.
12. Add a second **Published application** route. Use hostname
    `pairing.buzz.chesapeake.dev`, leave the path empty, and set **Service URL**
    to exactly `http://pairing-relay:5000`.

Do not create a Cloudflare Access application in front of this hostname unless
all Buzz clients are known to support that additional authentication flow.
Buzz WebSocket and HTTP requests must reach the relay; relay membership and
event authentication are handled by Buzz itself.

Cloudflare also exposes complete Tunnel management under **Zero Trust** >
**Networks** > **Connectors** in accounts that use the Zero Trust dashboard.
Prefer **Networking** > **Tunnels** for this public application.

## 3. Start and verify the stack

From the repository root:

```bash
docker compose \
  --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml \
  config --quiet

docker compose \
  --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml \
  up -d --wait
```

The override binds both relays only to loopback; neither is exposed on the LAN.
Verify the local origins and tunnel container:

```bash
curl -fsS http://127.0.0.1:3000/_readiness
bash -ec 'exec 3<>/dev/tcp/127.0.0.1/5000'
docker compose \
  --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml \
  ps
```

After the Cloudflare route is saved, verify public HTTPS and the relay's NIP-11
document:

```bash
curl -fsS https://buzz.chesapeake.dev/_readiness
curl -fsS -H 'Accept: application/nostr+json' https://buzz.chesapeake.dev/
```

The NIP-11 response should contain
`"pairing_relay_url":"wss://pairing.buzz.chesapeake.dev"`. A WebSocket client
can then verify that `wss://pairing.buzz.chesapeake.dev` accepts a connection.

Clients should use `wss://buzz.chesapeake.dev`, not an `https://` URL.
With `BUZZ_SERVE_GIT_WEB_GUI=true`, opening
`https://buzz.chesapeake.dev/` in a browser serves the compiled repository web
client. Requests with `Accept: application/nostr+json` still receive the NIP-11
relay document, and WebSocket upgrades still reach the Nostr relay on the same
path.

## 4. Routine operation

Use the same Compose file set for every command:

```bash
# Status and recent logs
docker compose --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml ps
docker compose --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml logs --tail=200 relay pairing-relay cloudflared

# Restart without deleting data
docker compose --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml restart

# Stop while preserving the named data volume
docker compose --env-file deploy/embedded/.env \
  -f deploy/embedded/compose.yml \
  -f deploy/embedded/compose.cloudflare.yml down
```

Never add `-v` to `docker compose down` for this deployment: that would remove
the volume containing the relay's database, objects, and identity. Follow
[`docs/embedded-operations.md`](../../docs/embedded-operations.md) for backups,
restores, upgrades, capacity limits, and troubleshooting.

If the tunnel is **Down**, inspect `cloudflared` logs and confirm outbound DNS,
HTTPS, and Cloudflare Tunnel traffic (port 7844) are allowed. If Cloudflare
shows error 1016, confirm the tunnel is running and the published application
is attached to the correct tunnel. If Cloudflare reports an origin error, make
sure its Service URL is `http://relay:3000`, not `localhost`: inside the
`cloudflared` container, `localhost` refers to `cloudflared` itself.

## 5. Security and persistence checklist

- Keep `RELAY_ACCESS=closed` and set the intended owner public key before first
  public use.
- Keep the relay port loopback-only; the tunnel needs no router port forward.
- Protect `deploy/embedded/.env` and rotate the tunnel token in Cloudflare if it
  is exposed.
- Back up the complete stopped `buzz-data` volume and record the exact image
  tag. Test restore into an empty volume.
- Configure this device not to suspend while it is expected to host Buzz.
- Docker's `restart: unless-stopped` brings all three containers back after a
  Docker daemon or device restart, unless an operator explicitly stopped them.

## Cloudflare references

- [Create a remotely managed tunnel](https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/get-started/create-remote-tunnel/)
- [Route a published application](https://developers.cloudflare.com/tunnel/routing/)
- [Cloudflare Tunnel setup](https://developers.cloudflare.com/tunnel/setup/)
