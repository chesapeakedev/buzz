---
name: trace-sqlite-incidents
description: Correlate reported Buzz behavior with relay or client logs, embedded SQLite state, and the responsible code path. Use for incident questions such as "what happened," "trace this failure," "did this reach SQLite," "correlate this timestamp or pubkey," or repeated debugging of the local embedded Compose deployment.
---

# Trace SQLite Incidents

## Workflow

1. Establish the incident window, affected entity, user-visible message, and deployment. Use exact timestamps, event IDs, pubkeys, channel IDs, or request routes when available.
2. Inspect current service identity and health before interpreting evidence. Record container creation time, image, effective non-secret configuration, and volume name. A recreated container cannot supply logs from its predecessor.
3. Preserve read-only database evidence with `scripts/snapshot-embedded-sqlite.sh`. Query the snapshot, never the live WAL database or Docker volume directly.
4. Inspect schema before writing queries. Buzz stores timestamps in microseconds and uses both binary pubkeys (`users`, `events`) and lowercase text pubkeys (`relay_members`). Normalize comparisons deliberately.
5. Build a timeline across:
   - application/client records and logs;
   - relay structured logs and HTTP/WebSocket status;
   - event rows and side-effect tables;
   - configuration and the source path that gates each write.
6. Distinguish evidence from inference. Absence from SQLite proves only that the captured database has no matching row; use admission order and logs to determine whether rejection preceded storage.
7. Report the earliest failing boundary, supporting identifiers/times, unavailable evidence, confidence, and the smallest safe remediation. Diagnose only unless the user also asks for a change.

## Useful queries

Inspect `.schema` first, then adapt these patterns:

```sql
-- Event timeline; timestamps are microseconds.
SELECT kind, lower(hex(id)), lower(hex(pubkey)),
       datetime(received_at / 1000000, 'unixepoch', 'localtime')
FROM events
WHERE lower(hex(pubkey)) = lower(:pubkey)
ORDER BY received_at;

-- Cross-check the mixed pubkey representations.
SELECT EXISTS(SELECT 1 FROM relay_members WHERE pubkey = lower(:pubkey)),
       EXISTS(SELECT 1 FROM users WHERE lower(hex(pubkey)) = lower(:pubkey));
```

Prefer bounded log windows and structured filters. Never print secrets, auth tags, private keys, tunnel tokens, or full environment dumps. Select only named non-secret variables.

## Embedded deployment cautions

- Activate Hermit before repository Git or checks.
- Do not stop services, recreate containers, mutate SQLite, or change configuration during diagnosis unless the user requests it.
- Do not delete `buzz-embedded_buzz-data`.
- Explain when deployment recreation has destroyed the relevant container logs.
- Remove or retain snapshots according to the user's needs; tell the user where any retained snapshot lives.
