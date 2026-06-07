# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-startup-timeline`
- Baseline: `upstream/main` at `ae9d25879d72bc8977f08e61062c022e2142483b`
- PR type for this branch: `docs-only`
- Runtime behavior changes: none
- Rust code changes: none
- Docs changes: add the binary startup timeline baseline for later
  runtime/lifecycle migration work.

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
  - Current branch: not in scope.
  - Next PR: add checks for public re-export, route matrix, and storage trait
    coverage before pure moves.
- [x] `G-007` Create startup timeline table.
  - Acceptance: [`startup-timeline.md`](startup-timeline.md) records current
    binary startup order, side effects, fatal boundaries, and readiness stages.
- [x] `G-009` Enforce pre-push three-expert review.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) requires
    quality/architecture, migration-preservation, and testing/verification review
    before push.
- [~] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Current branch: not in scope.
  - Next PR: add a mechanical check that all migration docs use the same PR type
    vocabulary.

## Next PRs

1. `docs-only` or `test-only`: capture admin route-action snapshot.
2. `docs-only`: inventory `ecstore::config::{Config, KV, KVS}` consumers.
3. `ci-gate`: add focused checks for PR type vocabulary and temporary
   compatibility marker/register consistency.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Final review confirmed the startup, deferred IAM, readiness, and shutdown tables match current source behavior after blocker fixes |
| Migration preservation | pass | Final review confirmed this branch is docs-only and does not touch runtime logic, storage hot paths, global-state migration, or compatibility code |
| Testing/verification | pass | Final review accepted docs-only verification with layer guard, metrics reference guard, diff checks, and staged whitespace check |

## Verification Notes

Passed:

- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `git diff --check`
- `git diff --cached --check`
- focused source review of `rustfs/src/main.rs`, `rustfs/src/startup_iam.rs`,
  and `rustfs/src/server/readiness.rs`

## Handoff Notes

- Keep Phase 0 PRs small. Do not start Config, Storage API, Runtime, or ECStore
  movement inside this `docs-only` branch.
- Keep CI checks in a separate `ci-gate` PR so the PR type rule remains enforceable.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
