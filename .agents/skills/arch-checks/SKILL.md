---
name: arch-checks
description: Diagnose failures from check_layer_dependencies.sh, check_architecture_migration_rules.sh, check_unsafe_code_allowances.sh, check_logging_guardrails.sh, check_doc_paths.sh, or check_no_planning_docs.sh. Use when one of these guards fails, not for every architecture question or documentation edit.
---

# Architecture Guard Checks

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
