# RustFS Agent Instructions (Global)

This root file keeps repository-wide rules only.
Use the nearest subdirectory `AGENTS.md` for path-specific guidance.

## Rule Precedence

1. System/developer instructions.
2. Current user/task instructions.
3. The nearest `AGENTS.md` in the current path.
4. This file (global defaults).

If repo-level instructions conflict, follow the nearest file and keep behavior aligned with CI.

## Execution Discipline

- Read the relevant existing code, tests, and local guidance before changing behavior. For new helpers or test setup, that read includes `crates/utils`, `crates/common`, and the touched crate's own `test_util`/fixtures (see Reuse Before You Write).
- State assumptions when they affect the implementation or verification path.
- If a task has multiple plausible interpretations, list the options briefly and choose the narrowest reasonable path; ask when the ambiguity would make the change risky.
- For multi-step work, keep the plan minimal and tied to verifiable outcomes.
- Avoid redundant file reads, repeated commands, and unnecessary exploratory work once enough context is available.
- A good result is a minimal diff with clear assumptions, no over-engineering, and independent verification that survives Adversarial Validation (below).

## Communication and Language

- Respond in the same language used by the requester.
- Keep source code, comments, commit messages, and PR title/body in English.
- Be concise. Avoid sycophantic openers, closing fluff, and verbose status reporting.

## Change Style for Existing Logic

- Prefer direct, local code over extracting one-off helpers.
- Extract a helper only when logic is reused or the extraction materially clarifies a non-trivial flow.
- Use Rust's default module file layout (`mod foo;` with `foo.rs` or `foo/mod.rs`/`foo/*.rs`).
  Avoid `#[path = "..."]` for module inclusion; move files into the canonical module tree instead.
  If an unavoidable generated-code, FFI, or test-fixture exception remains, keep it local and document why the canonical layout cannot work.
- Solve only the requested problem; do not add speculative features, configurability, or adjacent improvements.
- Prefer editing existing code over rewriting files or reshaping unrelated logic.
- Modify only what is required and remove only artifacts introduced by your own changes.
- Preserve the existing control-flow and logic shape when fixing bugs or addressing review comments, especially in init, distributed coordination, locking, metadata, and concurrency paths.
- Do not refactor existing code only to make it easier to unit test.
- Keep fixes narrowly aligned with the requested behavior; avoid semantic-adjacent rewrites while touching sensitive paths.
- Keep code elegant, concise, and direct. Prefer minimal, readable implementations over over-engineering and excessive abstraction. Use comments to clarify non-obvious intent and invariants, not to compensate for unclear code.
- Do not write comments that narrate what the next line does, restate a signature, or describe the change you just made — that commentary belongs in the PR description, not the code. Required invariant comments — lock ordering, `SAFETY`, unwrap justification, `#[allow(dead_code)]` rationale, `RUSTFS_COMPAT_TODO` — are never narration.
- Mention unrelated issues when useful, but do not fix them as part of a narrow task.

## Reuse Before You Write

Search for an existing implementation before writing a new one; extend what exists instead of duplicating it:

- **Helpers and utilities** (path/string handling, hashing, retry, env parsing, IO wrappers): check `ls crates/utils/src` first — file names map to operations (`retry.rs`, `envs.rs`, `hash.rs`, `path.rs`, `string.rs`, `io.rs`) — plus `crates/common` (shared structures/globals), then `rg -i 'fn \w*<term>' crates/utils/src crates/common/src <touched-crate>/src` for signatures. Helpers are snake_case: a full-text single-word grep over a large crate drowns you and a multi-word phrase returns nothing. Reimplementing an existing workspace helper — or hand-rolling what `std`, `tokio`, or an existing workspace dependency already provides — is a review finding, not a style preference.
- **Reuse requires matching semantics, not a matching name**: before adopting a helper, check its normalization (`clean` resolves `.`/`..` — never apply it to raw S3 object keys), error type, backoff/deadline behavior, and durability gating against the call site. When semantics differ, a new narrowly-named helper with a comment naming the rejected lookalike is the correct outcome. The inverse also holds: workspace wrappers exist because raw `std`/`tokio` semantics were insufficient (durability gates, retries) — prefer the wrapper over the raw call.
- **Constants and fixed tokens** (protocol labels, error identifiers, header keys, event names, metric names, command tags): search for existing constants/enums that already represent the same semantic value and reuse them. If a value is truly new, define one local constant near related logic; never scatter the literal across sites. When changing existing behavior, align naming and format with the established constants.
- **Test scaffolding**: reuse existing test utilities and fixtures (the touched crate's own `test_util` module and `tests/fixtures`, or `crates/test-utils`) instead of writing new setup code — run `rg -l '<fn-under-test>' <crate>/src <crate>/tests` before writing a test. A new test must pin a failure mode no existing test covers. Near-duplicate means same code path AND same poison-value class: this repo's boundary companions (n==max vs max+1, absent vs empty vs nil UUID bytes, MetaObject vs MetaDeleteMarker) are distinct by definition and must all be written.

## Necessary Code Only

Net-new code — files, types, branches, comments — is cost to justify, not progress:

- Validate at the trust boundary — untrusted client input, bytes read from disk, RPC payloads, config (see Serde Safety and Cross-Cutting Domain Invariants) — then trust the type: do not re-check what the type system or a validated upstream layer already guarantees, and cite the establishing check (`file:line`) when the guarantee is not obvious.
- The exception is load-bearing: a value that crossed a persistence, RPC, or version boundary is never guaranteed by the code on the other side — a peer may be older or buggy, disk bytes may be corrupt — so the Cross-Cutting Domain Invariant patterns apply at every consumer, and re-checks immediately before a destructive action (delete, overwrite, quorum decision) stay. Deleting an existing guard is a behavior change requiring adversarial review, not cleanup.
- Every new branch needs a nameable trigger: a concrete input, state, or failure that reaches it — for boundary-crossing values, corrupt or stale persisted/peer data is always nameable. If you cannot name one, do not write the branch. If the case is truly unreachable, encode the invariant in the type; where that is impossible, return a typed internal error (fail closed). `debug_assert!` is acceptable only for pure internal arithmetic on values that never crossed a disk/RPC/config boundary — never as the sole guard on decoded or peer-supplied data.
- Never substitute a default where the value is required (e.g. `unwrap_or_default()` on metadata that must exist) — that converts corruption into a wrong answer. Return the typed error instead: explicit failure over implicit success.
- Attach error context once, at the layer where it is actionable: re-wrapping equivalent context at every hop is noise, and expanding a fallible chain into nested `match` blocks where `?` or a combinator suffices is a finding. Never add context by converting a typed error into a generic variant below an error-aggregation or quorum layer (`reduce_errs` classifies by variant equality) — context there belongs in a `tracing` event, not the error value.

## Sources of Truth

- Workspace layout and crate membership: `Cargo.toml` (`[workspace].members`)
- Local quality commands: `Makefile` and `.config/make/`
- CI quality gates: `.github/workflows/ci.yml`
- PR template: `.github/pull_request_template.md`
- High-level architecture and crate map: `ARCHITECTURE.md`
- Migration guardrails, readiness contracts, support matrices:
  `docs/architecture/README.md` (routes by audience)
- Shared agent skills (all tools): `.agents/skills/` — each `SKILL.md` carries
  a frontmatter `description` stating when it applies. Scan the descriptions
  before starting a task and follow any skill that matches, even if your tool
  does not auto-load skills:
  `grep -m1 '^description:' .agents/skills/*/SKILL.md`
  Claude Code reads them through the `.claude/skills` symlink; add new skills
  to `.agents/skills/` only, never as separate copies per tool

Avoid duplicating long crate lists or command matrices in instruction files.
Reference the source files above instead.

Do not commit planning-type documents — one-shot implementation/optimization
plans, task trackers, migration-progress ledgers, phase/PR templates,
issue-scoped benchmark-result snapshots or optimization conclusions, or
agent-generated working notes (e.g. anything a `superpowers`/scratch workflow
produces). Keep that work in the issue tracker or your local worktree, not in
the repository. Only durable reference — the architecture set under
`docs/architecture/`, repeatable operational runbooks under `docs/operations/`,
and the test-suite references under `docs/testing/` — belongs in version
control; `.gitignore` ignores everything else under `docs/` by default, so a new
plan file will not be tracked unless someone force-adds it — don't.
`scripts/check_no_planning_docs.sh` (wired into `make pre-commit`/`pre-pr` and
CI) fails the build if anything is committed under `docs/superpowers/`, even via
`git add -f`.

## Verification Before PR

Convert changes into independently verifiable outcomes. Prefer focused tests for behavior changes and run the relevant checks before declaring completion.
Non-exempt changes must also pass Adversarial Validation (next section) before the checks below count as completion.

For code changes, run and pass the following before opening a PR:

```bash
make pre-pr
```

Before committing code changes, prefer focused verification for the touched
surface and use the faster local gate when a broad smoke check is needed:

```bash
make pre-commit
```

For migration batches, do not run the full `make pre-pr` gate before every
intermediate commit. Use focused tests and `make pre-commit` during
development, then reserve `make pre-pr` for the final PR-ready branch.

Before pushing code changes, make sure formatting is clean:

- Run `cargo fmt --all`.
- Run `cargo fmt --all --check` and ensure no files are modified unexpectedly.

If `make` is unavailable, run the equivalent checks defined under `.config/make/`.
Documentation-only or instruction-only changes are exempt from the verification commands above (including the `.config/make/` equivalents), though any locally installed git pre-commit hooks may still run on commit unless explicitly skipped.
After build-based verification completes, clean generated build artifacts before wrapping up to avoid unnecessary disk usage.
Do not open a PR with code changes when the required checks fail.
Make a failing check pass by fixing the cause, never by weakening the gate:
do not loosen or skip a guard script, add entries to a baseline or allowance
list, suppress a lint with `#[allow]`, mark a failing test `#[ignore]`, or
delete or relax a failing assertion to get green. If a check itself is wrong,
change it deliberately and state the rationale in the PR.

For flaky tests, do not paper over them with retries. Follow the flake policy
in [docs/testing/README.md](docs/testing/README.md) (open an issue within 24h,
quarantine with an issue link, fix or delete within 30 days); the local
`default` nextest profile never retries.

## Adversarial Validation (Default On)

Every non-exempt output (see Risk tiers) — code change, bug fix, or
design/solution proposal — passes multi-role adversarial review before it
counts as done.
Author confidence is not evidence: each role's job is to refute the change,
not to bless it.

### Risk tiers

Pick the tier from the riskiest file touched; when in doubt, pick the higher.

- **Exempt:** docs/comments/instruction-only changes, formatting, typos with
  no runtime surface. Skip this section.
- **Mechanical:** pure renames, file moves, test-only or tooling changes —
  correctness and simplicity adversaries only.
- **Standard (the default):** any change that affects behavior.
- **High risk:** touches locking, erasure coding, quorum/heal, replication,
  multipart, RPC, lifecycle/tiering, metadata formats (`xl.meta`),
  persistence/fsync, IAM/KMS/auth, on-disk or on-wire formats, or
  S3 API-visible behavior.

### Roles

Run each applicable role as an independent pass over the final diff (or
proposal text) — parallel reviewer agents where the tooling supports them,
otherwise sequential passes that each start fresh from the diff and the
nearest scoped `AGENTS.md`, discarding the writing session's assumptions.
Each role either produces findings or reports "attacked X, Y, Z — no break
found"; a bare pass is not a result. Repo-specific attack probes for every
role live in `.agents/skills/adversarial-validation/` — run them, they
encode this repo's shipped bugs.

- **Correctness adversary** — construct a concrete input/state/interleaving
  that yields wrong output, data loss, or a crash. Probe error paths and edge
  values (empty, nil UUID, zero-length, quorum−1, missing version).
- **Simplicity adversary** — same behavior, less code. Hunt the materially smaller or more idiomatic diff (see Change Style for Existing Logic, Reuse Before You Write, and Necessary Code Only): reimplemented workspace helpers, one-caller extractions, rewrites where an in-place edit suffices, defensive branches with no nameable trigger, redundant error wrapping, near-duplicate tests, narration comments. A smaller diff achieving identical behavior is a finding, reported with the concrete replacement; forced reuse of a helper with mismatched semantics is equally a finding.
- **Security reviewer** — authn/authz bypass, injection, secret leakage,
  untrusted deserialization (see Serde Safety), path traversal, timing leaks.
- **Concurrency/durability reviewer** — lock ordering, races, cancellation,
  partial failure, retry/idempotency, crash and power-loss ordering.
- **Compatibility reviewer** — S3 API surface, MinIO interop, on-disk and
  on-wire formats, mixed-version upgrade/downgrade paths.
- **Performance reviewer** — allocation and cloning on hot paths, lock hold
  time across IO, sync or CPU-heavy work on async runtime threads, added
  fsync/flush outside the durability gate, hot-path logging noise. A
  measurable regression on a per-request or per-object path is a finding.
- **Test-coverage skeptic** — for each claimed behavior, name the test that
  fails if the change is reverted; then name a changed line that could be
  wrong while all tests stay green — if one exists, coverage is insufficient.
  A missing test is a finding, not a note.

Standard tier: correctness adversary + simplicity adversary + test-coverage
skeptic, plus every role whose domain the diff touches (async or
shared-state code → concurrency; parsing of untrusted input → security;
public crate API shape → compatibility; per-request or per-object hot paths
→ performance).
High risk: all seven roles.

### Protocol

1. A finding states a concrete failure scenario (input/state → wrong
   outcome) or names a missing test, with severity and file:line. "Looks
   risky" is not a finding.
2. Resolve every finding: fix it, or rebut it with evidence — a test, a
   traced code path, or a cited invariant. Restated intent and "unlikely"
   are not rebuttals.
3. After non-trivial fixes, re-run the roles whose domain the fix touched.
4. For proposals with no diff, roles attack assumptions, failure modes,
   migration/rollback, and testability instead — including the simplest
   rejected alternative and the blast radius when the design fails.

### Exit criteria

- Every applicable role has run; every finding is fixed or rebutted with
  evidence.
- Every behavior change has a test that fails without it.
- The Verification Before PR gates pass — adversarial review supplements
  those gates, never replaces them.
- High risk only: record a one-line verdict per role in the PR description.

## Git and PR Baseline

- Use feature branches based on the latest `main`.
- Assume other agent sessions work this repository concurrently. Never commit
  in a shared checkout; do all work on a dedicated feature branch, preferably
  in a dedicated worktree.
- Immediately before branching, fetch `origin/main` and branch from it;
  confirm the target issue is not already fixed there before writing code.
- Follow Conventional Commits, with subject length <= 72 characters.
- Keep PR title and description in English.
- Use `.github/pull_request_template.md` and keep all section headings.
- Use `N/A` for non-applicable template sections.
- Include verification commands in the PR description.
- When using `gh pr create`/`gh pr edit`, write the markdown body to a file
  and pass `--body-file`; multiline inline `--body` is unsafe — backticks and
  shell expansion can corrupt content or trigger unintended commands.
  Pattern: `cat > /tmp/pr_body.md <<'EOF' ... EOF`, then
  `--body-file /tmp/pr_body.md` (keep the file outside the checkout).
- Do not include the literal sequence `\n` in any GitHub issue, pull request, or discussion comment.
- Do not hard-wrap prose in PR/issue/discussion bodies; write each paragraph as a
  single line and let it reflow. GitHub renders single newlines inside a paragraph
  as line breaks, so mid-sentence wrapping shows up as ugly breaks. Only break lines
  for list items, code blocks, and deliberate separators.
- After fixing code review comments or CI findings, always mark corresponding review
  comments/threads as resolved before returning to the user.
- In handling review comments, confirm the underlying issue before changing code.
  If a suggested change is not appropriate for behavior or risk, reply with a
  concise rationale instead of blindly applying it.

## Security Baseline

- Never commit secrets, credentials, or key material.
- Use environment variables or vault tooling for sensitive configuration.
- For localhost-sensitive tests, verify proxy settings to avoid traffic leakage.

## Tools

### xl.meta decode tool Quick Use

```
cargo run -p rustfs-filemeta --example dump_fileinfo -- "/path/to/file/xl.meta"
```

## Serde Safety

- Add `#[serde(deny_unknown_fields)]` to structs deserialized from untrusted input (S3 API XML/JSON, lifecycle rules, bucket policies, replication configs).
- When `deny_unknown_fields` is impractical (backward compatibility), at minimum log unknown fields at `warn` level.
- Never use `#[serde(default)]` on security-critical fields without explicit validation of the resulting value.

## Cross-Cutting Domain Invariants

- Write internal object metadata under **both** `x-rustfs-internal-<suffix>`
  and `x-minio-internal-<suffix>` keys (MinIO interop). Use the helpers in
  `crates/utils/src/http/metadata_compat.rs` (`get_bytes` prefers the RustFS
  key); never write only one of the two.
- Read binary UUID metadata defensively:
  `.and_then(|v| Uuid::from_slice(&v).ok()).filter(|u| !u.is_nil())` —
  absent, empty, and nil all mean "no value", never `Uuid::nil()`.
- A remote-tier version of `None`/`""` means the tier bucket is unversioned:
  send **no** `versionId` on tier GET/DELETE.

## Naming Conventions

- Follow Rust API Guidelines for naming: `SCREAMING_SNAKE_CASE` for statics and constants, `snake_case` for functions and variables, `PascalCase` for types.
- Do not use camelCase or Hungarian notation (e.g., `globalDeploymentIDPtr` → `GLOBAL_DEPLOYMENT_ID`).
- If existing code violates naming conventions, do not widen the violation in new code. Do not rename existing symbols as part of an unrelated task; mention the violation instead (see Change Style for Existing Logic).

## Scoped Guidance in This Repository

Many crates and modules carry their own `AGENTS.md` with path-specific rules
(security boundaries, lock ordering, domain invariants). Before editing a
path, check for the nearest one:

```bash
git ls-files '*AGENTS.md'
```

The nearest file wins. Do not maintain a hand-written index of these files
here — it goes stale.
