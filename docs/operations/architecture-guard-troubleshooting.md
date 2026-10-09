
# Architecture Guard Troubleshooting

Read only the section for the failing guard. Use `.config/make/` and the current
workflow to verify its wiring; not every guard is part of every gate. Fix the
cause and rerun the failed guard; never weaken a check to get green.

## `check_layer_dependencies.sh` — layer DAG in `rustfs/src`

Enforces `composition (server, startup/init) → interface (admin,
storage/ecfs, storage/s3_api) → app → infra`; no upward imports. Server source
files are composition roots, while imports of their exported HTTP contracts
are classified as interface dependencies. Known legacy violations live in
`scripts/layer-dependency-baseline.txt`.

Dedicated `*_test.rs` and `tests/` modules are outside this production guard.
Inline `#[cfg(test)]` imports remain checked under their source file's layer;
move architecture-crossing test scaffolding into a dedicated test module.

- **New violation**: restructure your change so the dependency points
  downward (move the shared type/function to the lower layer).
- **You legitimately removed a baseline entry**: run
  `./scripts/check_layer_dependencies.sh --update-baseline` and commit the
  shrunken baseline. Never add new entries to the baseline to make a new
  violation pass.

## `check_architecture_migration_rules.sh` — required doc sections

Asserts that the core docs under `docs/architecture/` (overview,
crate-boundaries, runtime-lifecycle, readiness-matrix,
storage-control-data-plane, global-state-crate-split-plan,
ecstore-module-split-plan, …) still contain specific headings and exact
source lines. If it fails after a doc edit, you reworded or removed a
guarded line — restore the wording or update the script deliberately in the
same PR, with rationale.

## OIDC architecture boundaries

`check_architecture_migration_rules.sh` also runs `scripts/check_oidc_architecture_boundaries.sh`. Its rule identifier and `file:line` point to the source boundary that changed. Keep OIDC provider configuration, discovery, transport, state, and runtime under `crates/iam/src/oidc/`; keep verified identity mapping under `crates/iam/src/federation/mapper.rs`; construct the runtime in IAM startup and `rustfs/src/startup_auth.rs`; publish the federation service and OIDC query together through `AppContext`. The Keycloak workflow must include the OIDC module path for pull requests and pushes.

The Admin OIDC handler delegates persisted configuration reads, updates, and provider validation to `admin/service/oidc_config.rs`. Site replication uses `OidcConfigQuery::site_replication_snapshot()` and receives only the fields required for its response, including a hashed client secret. The Keycloak workflow also tracks the Admin configuration service path for both events.

Run `bash scripts/check_oidc_architecture_boundaries.sh --self-test` after editing a rule. Its negative and accepted examples under `scripts/fixtures/architecture_migration_rules/oidc/` must produce the exact expected rule identifiers. Then run the full architecture guard.

## `check_unsafe_code_allowances.sh`

Every `#[allow(unsafe_code)]` needs a `SAFETY:` comment within a few lines.
Write the actual safety argument; don't add a placeholder.

## `check_logging_guardrails.sh`

A fixed list of security-sensitive files (auth, IAM, KMS, admin handlers…)
is scanned for logging violations. If you created a new sensitive file,
consider adding it to the script's `checked_files` list.

## `check_doc_paths.sh`

Instruction docs (`AGENTS.md`, `CLAUDE.md`, `ARCHITECTURE.md`) and every
Markdown file under `docs/` (architecture, operations, testing, index) must not
reference repo file paths that no longer exist. If your refactor moved code,
update the docs that point at it — the error message lists `doc -> stale-path`
pairs. In durable docs, cite paths plus symbol names rather than line numbers
(see `docs/architecture/README.md`). Review findings still need `file:line`.

## `check_no_planning_docs.sh`

Planning-type documents must not be committed (see AGENTS.md "Sources of
Truth"). The guard fails if anything is tracked under `docs/superpowers/` —
`.gitignore` already ignores it, but `git add -f` bypasses that, so this closes
the hole. Fix by removing the listed file(s) with `git rm`; keep the plan or
spec in the issue tracker or a local worktree instead.
