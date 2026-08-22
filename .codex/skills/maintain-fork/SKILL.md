---
name: maintain-fork
description: Maintain the ChesapeakeDev Buzz embedded-backend fork against block/buzz upstream. Use when fetching or rebasing upstream/main, resolving upstream-sync conflicts, validating distributed-versus-embedded compatibility, recording conflict guidance, or publishing the validated fork stack directly to origin/main.
---

# Maintain the Embedded Backend Fork

Keep the fork's linear embedded-backend patch stack current with
`upstream/main`, with all checks passing. This is a maintenance workflow, not a
feature-delivery goal: do not create or manage a persistent Codex goal. Ordinary
fork feature work follows `AGENTS.md` and does not invoke this skill.

## Respect the Fork Workflow

- Work manually from the local development machine. Do not create a scheduled
  sync or run an upstream sync in GitHub Actions.
- Publish directly to `origin/main`; do not create or review a pull request for
  the fork.
- Rebase `upstream/main` beneath the fork-owned commits. Do not merge upstream
  into the patch stack.
- Activate Hermit before Git or repository commands and preserve user-owned
  work. Require a clean tree before starting synchronization.

## Rebase and Resolve

1. Read `AGENTS.md`, `UPSTREAM.md`, and the relevant portions of
   `docs/single-node-embedded-backends.md`.
2. Run `just sync-upstream-status`, confirm `main` matches the expected
   `origin/main`, then run `just sync-upstream`.
3. Resolve every conflict semantically. Preserve upstream safety fixes,
   instrumentation, tests, and behavior, then reapply fork behavior through the
   narrow database, coordination, object-store, configuration, deployment, and
   release seams. Never accept all of ours or theirs across a conflicted file.
4. Treat changed upstream interfaces as integration feedback. Prefer adapters,
   shared contracts, `.as_ref()` at trait-object boundaries, additive exports,
   and expectations derived from authoritative constants or migrator contents
   over duplicated literals.
5. Keep PostgreSQL/Redis/S3 green. Preserve tenant isolation, atomicity,
   idempotency, durable security state, restart recovery, and embedded-mode
   rejection of distributed-only settings.
6. Record representative conflicts and their conflict-reducing lesson in
   `UPSTREAM.md`. Include the upstream base and any intentional semantic
   divergence. Do not turn the log into a file-by-file transcript.

## Run the Compatibility Gate

After the rebase is complete and before publishing, run
`just smoke-upstream-fork`. The suite must build and exercise these exact tips:

- `upstream/main` with its canonical PostgreSQL/Redis/S3 runtime;
- the rebased fork `HEAD` with SQLite/local coordination/filesystem storage.

Drive the same backend-neutral protocol workload against both relays. At
minimum verify readiness and NIP-11, authenticated WebSocket connection,
publish-plus-live-delivery, stored query replay and EOSE, kind filtering,
ephemeral non-persistence, and rejection of a pubkey/signature mismatch.
Compare semantic outcomes rather than backend-specific IDs, ordering between
ties, timings, readiness detail fields, or storage representations. Use
isolated ports and state, retain useful logs on failure, and always clean up
processes, worktrees, and containers. A failure on either tip blocks
publication; do not waive it as an upstream problem without recording concrete
evidence and asking the user how to proceed.

Also run:

```bash
just test-sync-upstream
just fork-ci
git diff --check
```

Run focused checks for every conflicted subsystem first. Do not run desktop,
Tauri, web, or mobile gates: this fork does not change client code and upstream
owns those checks. Audit every fork-owned
commit in the rebased range for a valid DCO trailer, expected identity,
Conventional Commit subject, and absence of merge commits.

## Publish and Verify

Run `just sync-upstream-publish-main` only after every gate passes. It must use
the repository's guarded `--force-with-lease` path and the previously observed
`origin/main` tip; never replace it with an unguarded force push. Then verify
that `origin/main` equals local `main`, contains `upstream/main`, remains linear,
and has a successful deterministic push CI run.

Report the old and new upstream bases, the rewritten fork range, conflicts and
lessons recorded, exact checks run, publication result, and any remaining
divergence risk.
