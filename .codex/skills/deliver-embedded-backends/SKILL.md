---
name: deliver-embedded-backends
description: Deliver the ChesapeakeDev Buzz single-node embedded-backends program as a persistent, dependency-ordered goal. Use when Codex is asked to continue, implement, review, resolve an upstream-sync conflict for, or report progress on the embedded SQLite, local coordination, filesystem storage, deployment, fork-safety, or upstream-maintenance work described in docs/single-node-embedded-backends.md.
---

# Deliver Embedded Backends

Treat `docs/single-node-embedded-backends.md` as the versioned source of truth
for a long-running delivery goal. Advance it in reviewable slices while keeping
the distributed PostgreSQL/Redis/S3 deployment green and the fork's final
`main` branch publishable. This fork does not open pull requests against
`block/buzz`.

## Establish the Goal

1. Read the complete plan, the root `AGENTS.md`, and any nearer instructions for
   files in scope.
2. Inspect the repository, current branch, worktree, remotes, recent history,
   and relevant implementation before choosing work.
3. Query the current Codex goal.
   - If no unfinished goal exists, create one whose objective is to complete
     every epic and exit gate in the embedded-backends plan.
   - If the active goal is this program, resume it.
   - If an unrelated goal is active, do not replace it. Explain the conflict
     and request direction.
4. Do not mark the goal complete until every epic exit gate is supported by
   repository and test evidence.

## Choose and Deliver a Slice

1. Derive progress from the plan, merged code, tests, and repository history;
   do not assume a numbered item is complete because scaffolding exists.
2. Select the earliest incomplete dependency-safe item from the delivery
   sequence. Keep the slice small enough for one focused PR and leave the
   repository deployable.
3. State the slice, acceptance evidence, and relevant checks in the working
   plan, then implement it unless the user requested planning or status only.
4. Preserve existing behavior before adding an embedded implementation:
   introduce backend-neutral seams, keep current adapters intact, and use
   shared contract tests.
5. Separate generally useful refactors from ChesapeakeDev branding, release
   authority, or embedded-default policy whenever practical; all slices remain
   in the fork's linear publication stack.
6. Run focused tests during iteration and the quality gates required by
   `AGENTS.md` for the affected subsystem. Record concrete results.
7. Finish each verified delivery slice with well-defended local commits unless the
   user explicitly requests no commits. A skill invocation for implementation
   authorizes these local commits. A push is authorized only when the user
   explicitly requests fork publication.

## Build a Defended Linear History

Make the fork history read as a linear, reviewable hill climb toward the plan:

1. Split work into dependency-ordered commits that each leave the tree
   coherent. Do not mix fork governance, backend-neutral refactors, embedded
   policy, or unrelated user changes in one commit.
2. Before every commit, inspect the complete staged diff, stage explicit paths,
   and run the narrow tests that defend the behavior changed by that commit.
   Run the applicable `AGENTS.md` gates before closing the PR slice.
3. Use Conventional Commits subjects (`type(scope): imperative summary`), with
   a specific scope when useful. Prefer `refactor`, `feat`, `fix`, `test`,
   `docs`, `ci`, or `chore`; do not use vague subjects such as “updates” or
   “work in progress.”
4. Defend non-trivial commits in the message body: explain why the change is
   needed, identify the compatibility or security invariants it preserves, and
   record the exact tests run. The diff and message together must defend the
   patch without relying on unstated fork-only context.
5. Follow `CONTRIBUTING.md`'s “Sign Your Commits” rule for every fork commit:
   create it with DCO sign-off (`git commit -s`) so its message contains a
   `Signed-off-by: Name <email>` trailer matching the contributor identity.
   The repository uses “sign” here to mean DCO sign-off (`-s`), not
   cryptographic Git signature (`-S`). Pass `-s` explicitly even when the
   commit hook would add the trailer. Never bypass hooks, and activate Hermit
   before Git operations as required by `AGENTS.md`.
6. Keep feature history linear: add commits on top of the current slice, avoid
   feature merge commits, and do not disturb user-owned commits.
7. Rebase the fork-owned patch stack onto new upstream bases when synchronizing
   the fork. Rewritten fork commit IDs are expected; release tags remain stable.
   Publish the resulting linear stack directly to fork `main` only with
   explicit `--force-with-lease` protection after confirming the expected old
   tip.
8. Before handoff or publication, audit every commit in the proposed range,
   not only `HEAD`. Fail the slice if any commit lacks a valid sign-off or uses
   an unexpected identity. Repair a private unsigned series with
   `git rebase --signoff <base>` and re-run the audit; never claim DCO
   compliance from the hook configuration alone.

Keep backend-neutral changes compatible with upstream so future rebases stay
small, but do not create upstream topic branches or pull requests. The
publication target is the complete linear `chesapeakedev/buzz:main` stack; the
eventual embedded release is a GitHub release from that fork.

## Match Repository Norms

Before designing a slice, read the applicable parts of `CONTRIBUTING.md`,
`ARCHITECTURE.md`, `TESTING.md`, the root and nearest `AGENTS.md`, and
subsystem-local documentation. Inspect recent analogous code and commits as
implementation evidence; do not rely on the embedded plan alone. Before
committing, audit the diff against those sources and record any intentional
deviation in the commit body.

Apply these established patterns unless the slice explicitly and defensibly
introduces a new seam:

- Keep PRs focused and avoid drive-by cleanup, cosmetic renames, broad
  dependency swaps, or style-only churn.
- Keep `buzz-core` free of I/O. Put persistence in `buzz-db`; keep
  `buzz-relay` as the orchestration layer rather than moving business or SQL
  logic into handlers.
- Preserve the signed Nostr event pipeline and existing kind registry. Prefer
  events and post-storage side effects over new endpoint-specific HTTP APIs.
- Use `sqlx::query()` runtime queries, explicit backend adapters, and the
  existing `Db` facade. Do not introduce compile-time SQL macros, an offline
  query cache, a broad ORM, or `AnyPool`.
- Keep every tenant-visible operation explicitly scoped by `CommunityId`.
  Preserve membership authorization, fail-closed lookup behavior, and
  transaction-bound check-then-modify sequences.
- Keep durable mutations atomic and idempotent. Run notifications, cache
  invalidation, fan-out, search, audit, and other best-effort effects only at
  the same post-commit boundary used by the existing event pipeline.
- Use `thiserror` for library errors, `anyhow` for application propagation,
  `?` or explicit handling in production, and no new production `unwrap()` or
  `expect()`. Add no `unsafe` code.
- Use structured `tracing` fields and never expose secrets, credentials, or
  sensitive filesystem paths in logs, status, metrics, tests, or errors.
- Document new public APIs and operator-visible configuration. Update
  architecture or user documentation when behavior or boundaries change.
- Add tests at the narrowest owning layer, a regression test for fixes, shared
  backend contracts for backend seams, cross-community and concurrency cases
  for persistence, and relay E2E coverage for protocol-visible behavior.
- Use default `rustfmt`, Clippy with warnings denied, Hermit-pinned tools, and
  the repository `just` targets. Run `just ci` before presenting a PR slice as
  ready; if an environment limitation prevents it, report the exact unrun gate
  rather than weakening or rewriting it.

## Preserve Program Invariants

- Keep Nostr protocol behavior and upstream desktop/mobile compatibility.
- Keep PostgreSQL/Redis/S3 supported and green throughout the program.
- Treat SQLite as fresh-install-only for v1; do not invent an import utility.
- Use backend-specific SQL adapters rather than `AnyPool` or a broad ORM
  rewrite.
- Keep security replay claims and security-relevant counters durable and
  fail-closed; use Moka only for state that is safe to lose.
- Keep embedded mode single-process and reject distributed-only settings.
- Maintain tenant predicates, thread counters, atomic object writes, traversal
  protection, and restart recovery as specified by the plan.
- Do not expand v1 into rebranding, signed client distribution, live backup,
  clustering SQLite, or hosted account services.

## Resolve Upstream Sync Conflicts

The fork maintains its changes as a linear patch stack rebased onto
`upstream/main`; the final publication is always the fork's `main` branch.

1. Start from a clean `main` aligned with `origin/main`.
2. Run `just sync-upstream`. If it reports conflicts, remain on
   `upstream-sync` and inspect every conflicted file.
3. Preserve upstream behavior by default, then reapply fork differences behind
   the narrow backend, configuration, deployment, and release seams described
   in the plan. Never resolve conflicts wholesale with an ours/theirs strategy.
4. Stage each resolution, continue the rebase, and confirm every replayed
   commit retains its DCO sign-off and defensible Conventional Commit message.
   If a replayed or amended commit loses its trailer, add it with
   `git rebase --signoff` or `git commit --amend -s`; do not add a blanket
   trailer without verifying that the committer has the right to certify it.
5. Run checks for every conflicted subsystem plus the sync contract test.
6. Run `just sync-upstream-publish-main`; it must validate the complete
   rebased, signed, merge-free stack and use `--force-with-lease` to update
   fork `main`. It must never call the GitHub pull-request API.
7. Update `UPSTREAM.md` when it exists with the upstream base commit,
   intentional omissions, semantic differences, and conflict resolutions.

## Report Progress

Report the completed slice, evidence, remaining earliest dependency, and any
new divergence risk. Distinguish the overall program goal from the current
slice: completing one delivery slice never completes the full goal.

## Finish Every Response

End every final response with two short handoff inventories:

- **Verify:** list the exact commands the user can run to verify the work
  completed in that response. Include focused tests first and broader required
  gates only when relevant. State any command that could not be run and why.
- **Code pointers:** link the files containing the major changes, with a line
  number when it helps locate the new seam, implementation, test, workflow, or
  documentation.

Keep both inventories concise and specific to the response's completed work.
Do not list unrelated program-wide checks or unchanged files.
