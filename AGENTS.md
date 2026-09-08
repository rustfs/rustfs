# RustFS Agent Instructions

This file contains repository-wide rules. Use the nearest subdirectory
`AGENTS.md` for path-specific invariants.

## Precedence

1. System/developer instructions.
2. The current user request.
3. Applicable `AGENTS.md` files, with the nearest file winning conflicts.
4. Selected skills and reference documents.

Nested instructions add to ancestor rules; they do not discard non-conflicting
rules. A skill cannot expand the user's requested scope or grant authorization.

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
- Resolve repository workflow skills under `.agents/skills/` when a global
  skill has the same name, unless the user explicitly selects another path.
- Avoid repeated reads and equivalent verification commands once enough
  evidence exists.
- Search for relevant symbols/headings before reading long files; return only
  matching ranges. If output is truncated, narrow the query instead of repeating
  a full read. Keep reusable raw logs in task artifacts and report the evidence.
- Reuse authorization already given in the conversation. Resolve routine choices
  within that scope and continue independent work while a material question is
  pending. Before requesting missing approval, prepare the concrete result that
  is already authorized; retain explicit merge and release gates.

## Task-Specific Guidance

Read only the reference needed for the current task, once per unchanged context:

- Before code changes or artifact-heavy work, read [implementation rules](.agents/references/implementation.md).
  For a read-only code review, use its change-style and boundary sections as needed.
- Before commits, pushes, PR creation/updates, or posting to PRs/issues/discussions,
  read [Git and PR rules](.agents/references/pull-requests.md).
  Reuse existing authorization; a reference does not authorize posting, merging, or publishing.
- Preserve unrelated work. Never commit from a shared checkout or delete another task's artifacts.
- Source comments, commits, PR titles, and PR bodies are in English.

## Sources of Truth

- Workspace membership: `Cargo.toml`.
- Local gates: `Makefile` and `.config/make/`.
- CI gates: `.github/workflows/ci.yml`.
- PR format: `.github/pull_request_template.md`.
- Architecture routing: `ARCHITECTURE.md` and `docs/architecture/README.md`.
- Knowledge-base index and documentation rules: `docs/architecture/README.md`.
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

### Broad Cross-Module Changes

Do not run `make pre-pr` by default before opening a PR. Consider it only when
the final diff is broad, spans multiple modules, and targeted checks cannot
bound the impact. Decide dynamically from the affected boundaries and risks;
otherwise use the scoped formatting, linting, compilation, and test checks
above.

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

For applicable work and substantial PR reviews, read the [risk tiers and review shape](.agents/references/adversarial-validation.md).
Load only the matching domain probes; ordinary reviews do not become adversarial
merely because this reference exists.

A review has no finding quota; `No findings` is a complete outcome. A request to
find problems is not evidence that a defect exists. Before reporting a candidate,
check callers, invariants, and existing tests for evidence that disproves it.
Findings need `file:line` and a concrete failure or violation of an explicit
requirement. Missing required tests/checks are verification gaps, not proof of a
runtime bug; name the unprotected behavior or unmet gate. Keep optional style or
refactoring preferences out of defect findings unless that review was requested.

Fix or rebut supported findings within the authorized scope. Once the required
passes are complete, stop. Reopen only for changed code, new evidence, an
unresolved finding, or an explicit re-review request; an unchanged diff does not
need another pass at every conversation turn or workflow handoff.

For high-risk PRs, record one concise verdict per covered lens in the PR body.

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

## Scoped Guidance

Before editing, locate the nearest instructions with:

```bash
git ls-files '*AGENTS.md'
```

The nearest file wins for domain invariants. Keep generic workflow and
validation policy in this root file and its task-specific references.
