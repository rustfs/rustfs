# RustFS Agent Instructions

This file contains repository-wide rules. Use the nearest subdirectory
`AGENTS.md` for path-specific invariants.

## Precedence

1. System/developer instructions.
2. The current user request.
3. The nearest `AGENTS.md`.
4. This file.

## Operating Model

- Inquiry, diagnosis, review, and planning tasks are read-only unless the user
  explicitly requests changes.
- For implementation, read the relevant code, tests, and local guidance, then
  make the smallest change that satisfies the request.
- State assumptions only when they affect behavior or verification. Ask only
  when a wrong assumption would materially change the result.
- Do not load every skill or inspect unrelated modules preemptively. Select a
  skill only when its description directly matches the request or changed
  surface.
- Avoid repeated reads and equivalent verification commands once enough
  evidence exists.

## Worktree and Disk Hygiene

- Start implementation from the latest `origin/main` and confirm the requested
  change is not already present.
- An existing clean, isolated task worktree is sufficient. Create another
  worktree only when the current checkout is shared, dirty with unrelated work,
  or belongs to another task.
- Never commit from a shared checkout. Use an `overtrue/` feature branch unless
  the user requests another name.
- Check free space before artifact-heavy builds, tests, coverage, or downloads.
  Re-check before a broad gate when space is tight.
- Remove only task-owned temporary/build artifacts. Never delete another task's
  worktree or uncommitted data.
- At handoff, mention disk or cleanup details only when they affected execution
  or artifacts/worktrees remain intentionally.

## Change Style

- Preserve existing control flow unless changing it is required for correctness.
- Prefer a direct local edit over new files, wrappers, managers, or speculative
  abstractions.
- Add a helper only when it removes current duplication, names a real domain
  boundary, or isolates a non-trivial invariant.
- Remove an in-scope path superseded by the change. If compatibility requires it,
  adapt at the boundary to one canonical core and use the repository's
  `RUSTFS_COMPAT_TODO` policy.
- Comments explain non-obvious invariants or reasons. Do not narrate code or
  record change history.
- Mention unrelated problems when useful; do not fix them in a narrow task.

## Reuse and Boundary Rules

- Before adding helpers, constants, fixtures, or wrappers, search the touched
  crate, the domain-owning crate, `crates/utils`, `crates/common`, and relevant
  direct dependencies.
- Reuse requires matching semantics: normalization, error types, deadlines,
  durability, and compatibility must fit the call site. A narrowly named local
  helper is better than forced reuse with different semantics.
- Validate untrusted input at its trust boundary, then trust the validated type.
  Values crossing disk, RPC, persistence, or version boundaries remain
  untrusted at every consumer.
- Re-check boundary values immediately before destructive actions such as
  delete, overwrite, or quorum decisions.
- Every new branch needs a concrete triggering input/state. For decoded or peer
  data, corruption and mixed-version input are valid triggers.
- Required values must return a typed error when absent or corrupt; do not use a
  default that converts corruption into a plausible result.
- Attach error context once where it is actionable. Do not erase typed errors
  below aggregation or quorum layers.

## Sources of Truth

- Workspace membership: `Cargo.toml`.
- Local gates: `Makefile` and `.config/make/`.
- CI gates: `.github/workflows/ci.yml`.
- PR format: `.github/pull_request_template.md`.
- Architecture routing: `ARCHITECTURE.md` and `docs/architecture/README.md`.
- Agent skills: `.agents/skills/*/SKILL.md`.

Do not commit one-shot plans, trackers, migration ledgers, benchmark snapshots,
or agent scratch notes. Durable architecture belongs under `docs/architecture/`,
operations under `docs/operations/`, and testing references under
`docs/testing/`. `scripts/check_no_planning_docs.sh` enforces this boundary.

## Verification

Select checks from the final task-owned diff. Scoped `AGENTS.md` files may add a
concrete path-specific check, but must not replace this tiering with a generic
full-workspace gate.

### Documentation and Instructions

For prose, comments, agent instructions, and skill metadata that cannot affect
runtime/build output:

- Run `git diff --check`.
- Run the relevant documentation guard or skill validator when applicable.
- Skip Cargo formatting, compilation, Clippy, tests, `make pre-commit`, and
  `make pre-pr`.

### Non-Behavioral Source Changes

- Run the formatter/validator for the changed language.
- Add compilation or doctests only when syntax or executable examples changed.

### Localized Behavior Changes

- Run `cargo fmt --all --check` for Rust changes.
- Run the narrowest test that exercises the changed behavior.
- Add package-scoped `cargo check` or Clippy only for targets, features, public
  APIs, error handling, or control flow not compiled by the focused test.
- Use `make pre-commit` only when its repository-wide fast checks add confidence
  beyond the focused checks.

### Broad or High-Risk Changes

After the required adversarial review, run `make pre-pr` when targeted coverage
cannot bound the impact, including dependency/toolchain/build-matrix changes,
unbounded cross-crate APIs, or locking, durability, erasure coding, replication,
RPC, IAM/KMS/auth, cryptography, on-disk/on-wire, and S3-visible behavior.

`make pre-pr` includes `make pre-commit`; never run both for the same unchanged
diff. Do not repeat a check already covered by a successful umbrella gate.
Rerun only checks affected by later edits.

Never weaken a gate to get green: do not add baselines/allowances, suppress
lints, ignore tests, or relax assertions unless changing that policy is itself
the reviewed task. Follow `docs/testing/README.md` for flaky tests.

## Adversarial Validation

Adversarial validation applies to final implementation diffs, explicitly
requested adversarial/design reviews, and agent-instruction changes that alter
execution. Ordinary questions, diagnoses, status reports, non-adversarial code
reviews, and low-risk planning do not trigger it.

Risk and review shape:

- **Exempt:** documentation, comments, formatting, or typos with no runtime,
  build, test, or agent-execution effect.
- **Mechanical:** renames, moves, test/tooling-only changes, and agent-rule
  changes. Run correctness and simplicity lenses.
- **Standard:** localized behavior changes. Run one integrated final-diff pass
  covering correctness, simplicity, and test coverage; add only domain lenses
  matched by the diff.
- **High risk / substantial PR review:** high risk includes locking,
  erasure/quorum/heal, replication, multipart, RPC, lifecycle/tiering,
  persistence/fsync, IAM/KMS/auth, cryptography, on-disk/on-wire formats, and
  S3-visible semantics. Cover all applicable lenses using exactly two
  independent reviewers when delegation is explicitly authorized. Split the
  lenses between them. Otherwise perform two fresh sequential passes.

Available domain lenses are security, concurrency/durability, compatibility,
and performance. Select `.agents/skills/adversarial-validation/SKILL.md` for an
explicit adversarial request, a high-risk change, or a substantial PR review;
then read only its matching role references. A routine standard pass does not
load the playbook unless the reviewer needs a RustFS-specific probe.

A finding must name a concrete input/state/interleaving and wrong outcome, or a
specific missing regression check, with `file:line`. Resolve it by fixing the
diff or rebutting it with code-path/test/invariant evidence. After a non-trivial
fix, rerun only affected lenses.

For high-risk PRs, record one concise verdict per covered lens in the PR body.

## Pull Request Lifecycle

- Creating or updating a PR includes one immediate snapshot of checks,
  mergeability, reviews, and unresolved threads.
- Unless the user explicitly requests monitoring, a release workflow requires
  it, or an automation already owns it, hand off after the PR is open with the
  current state and next event to watch. Do not delay ordinary handoff with
  fixed quiet-period sleeps.
- For requested monitoring, use event-driven or bounded waits. Report only state
  changes, actionable failures, or a meaningful prolonged delay.
- Investigate failures/comments before changing code. Fix task-attributable
  issues, rerun affected verification, push, reply or resolve the thread, then
  resume the requested monitor.
- Never merge without required reviewer approval or explicit authority.
- After an observed merge, verify the commit reached the base, then clean the
  task worktree/branch when safe. Preserve unmerged work for closed PRs unless
  deletion was explicitly authorized.

## Git and PR Baseline

- Follow Conventional Commits; keep the subject at most 72 characters.
- Source comments, commits, PR titles, and PR bodies are in English.
- Keep every heading from `.github/pull_request_template.md`; use `N/A` where
  needed and include commands actually run.
- Use `--body-file` for multiline `gh pr create`/`gh pr edit` content.
- PR/issue/discussion content must not contain the literal sequence `\n` or
  hard-wrapped prose paragraphs.
- Do not include local absolute paths or tool-specific labels/prefixes in GitHub
  content.
- Resolve review threads after the underlying issue is fixed. If declining a
  suggestion, reply with a short evidence-based reason.

## Security Baseline

- Never commit secrets, credentials, or key material.
- Use environment variables or vault tooling for sensitive configuration.
- For localhost-sensitive tests, bypass proxies explicitly.
- Untrusted S3 XML/JSON, lifecycle, policy, replication, and RPC structures use
  strict deserialization where compatibility permits. Security-critical
  defaults require explicit validation.

## Logging

For every added or edited `tracing` call:

- Reuse the module's `EVENT_*`, `LOG_COMPONENT_*`, and `LOG_SUBSYSTEM_*`
  constants and field shape.
- Put fields first and a short label last.
- Use `error` for behavior/security failure, `warn` for degradation/fallback,
  `info` for low-frequency lifecycle, `debug` for diagnostics, and `trace` for
  repetitive request/object success paths.
- Never log secrets, credential payloads, or merged configs.

Use `.agents/skills/rustfs-logging-governance/SKILL.md` for logging changes.

## Cross-Cutting Storage Invariants

- Write internal object metadata under both `x-rustfs-internal-<suffix>` and
  `x-minio-internal-<suffix>` using
  `crates/utils/src/http/metadata_compat.rs` helpers.
- Read binary UUID metadata with
  `.and_then(|v| Uuid::from_slice(&v).ok()).filter(|u| !u.is_nil())`; absent,
  empty, and nil all mean no value.
- Remote-tier version `None` or `""` means an unversioned bucket; send no
  `versionId` on tier GET/DELETE.
- `DataUsageCacheInfo` and `DataUsageEntry` keep their hand-written map
  serialization and new fields remain `#[serde(default)]` for older readers.

## Naming

Use Rust API naming: `SCREAMING_SNAKE_CASE` constants/statics, `snake_case`
functions/variables, and `PascalCase` types. Do not rename unrelated existing
violations.

## Scoped Guidance

Before editing, locate the nearest instructions with:

```bash
git ls-files '*AGENTS.md'
```

The nearest file wins for domain invariants. Keep generic workflow and
validation policy in this root file.
