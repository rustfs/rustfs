---
name: arch-checks
description: Resolve failures from the repository's architecture guard scripts — check_layer_dependencies.sh, check_architecture_migration_rules.sh, check_unsafe_code_allowances.sh, check_logging_guardrails.sh, check_doc_paths.sh. Use when make pre-commit / pre-pr or CI fails on one of these checks.
---

# Architecture Guard Checks

All five run in `make pre-commit` / `make pre-pr` and in CI. Fix the cause;
never weaken a check to get green.

## `check_layer_dependencies.sh` — layer DAG in `rustfs/src`

Enforces `interface (admin, storage/ecfs, storage/s3_api) → app → infra`; no
upward imports. Known legacy violations live in
`scripts/layer-dependency-baseline.txt`.

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

Instruction/architecture docs (`AGENTS.md`, `CLAUDE.md`, `ARCHITECTURE.md`,
`docs/architecture/*.md`) must not reference repo file paths that no longer
exist. If your refactor moved code, update the docs that point at it — the
error message lists `doc -> stale-path` pairs. Historical plans under
`docs/superpowers/plans/` are exempt.
