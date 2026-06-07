# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-migration-ci-gates`
- Baseline: `upstream/main` at `f00898d0703ac94a4a215a54b6666c747ebe6622`
- PR type for this branch: `ci-gate`
- Runtime behavior changes: none
- Rust code changes: none
- Script changes: stabilize `scripts/check_layer_dependencies.sh` against the
  current accepted baseline.

## Phase 0 Tasks

- [x] `G-001` Refresh `main` and record baseline.
  - Acceptance: baseline commit, title, and branch are recorded.
  - Verification: `git fetch upstream main --prune`; `git rev-parse upstream/main`.
- [x] `G-002` Create migration tracking checklist.
  - Acceptance: this file records task state, context, verification, and handoff.
- [x] `G-003` Classify PR types.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) lists exactly one
    allowed PR type per PR.
- [x] `G-004` Define re-export and wrapper policy.
  - Acceptance: temporary compatibility code must use `RUSTFS_COMPAT_TODO`.
- [x] `G-005` Add dependency direction guard.
  - Acceptance: `./scripts/check_layer_dependencies.sh` passes on current
    `upstream/main` while still rejecting new unaccepted layer dependencies.
- [~] `G-006` Create migration loss-prevention checks.
  - Current branch: keeps the existing layer guard stable.
  - Next PR: add checks for public re-export, route matrix, and storage trait
    coverage before pure moves.
- [x] `G-009` Enforce pre-push three-expert review.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) requires
    quality/architecture, migration-preservation, and testing/verification review
    before push.
- [~] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Current branch: not in scope.
  - Next PR: add a mechanical check that all migration docs use the same PR type
    vocabulary.

## Next PRs

1. `docs-only` or `test-only`: capture startup timeline and admin route-action
   snapshot.
2. `docs-only`: inventory `ecstore::config::{Config, KV, KVS}` consumers.
3. `ci-gate`: add focused checks for PR type vocabulary and temporary
   compatibility marker/register consistency.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Final review confirmed the guard scope, baseline logic, and storage target classification; the segment-level `storage::ecfs` / `storage::s3_api` follow-up passed |
| Migration preservation | pass | Final review confirmed no Rust, runtime, storage hot-path, or global-state logic changes and no missing `RUSTFS_COMPAT_TODO` marker |
| Testing/verification | pass | Final review accepted the full gate, focused guard checks, stale-baseline check, and negative baseline-removal check as sufficient for this `ci-gate` PR |

## Verification Notes

Passed:

- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `bash -n scripts/check_layer_dependencies.sh`
- `make pre-commit`
- temporary negative check that adds an unaccepted reverse dependency to a copied
  fixture and confirms the guard fails
- temporary negative check that adds single-line root grouped
  `use crate::{admin::bad};` and confirms the guard fails
- temporary negative check that adds multiline root grouped
  `use crate::{ ... }` and confirms the guard fails
- temporary negative check that adds root grouped alias
  `use crate::{admin as admin_alias};` and confirms the guard fails
- temporary negative check that adds grouped `self` imports and confirms the
  guard fails
- temporary negative check that adds a stale baseline row and confirms the guard
  fails
- temporary negative check that removes the accepted
  `crate::storage::ecfs::FS` baseline row for `rustfs/src/protocols/client.rs`
  and confirms the guard fails
- `git diff --check`
- focused shell review of changed scripts

## Handoff Notes

- Keep Phase 0 PRs small. Do not start Config, Storage API, Runtime, or ECStore
  movement inside this `ci-gate` branch.
- Keep CI checks in a separate `ci-gate` PR so the PR type rule remains enforceable.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
