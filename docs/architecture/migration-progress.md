# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-ecstore-config-inventory`
- Baseline: `upstream/main` at `241e45d3b040684e5d217d2dd95f84c72f1de316`
- PR type for this branch: `docs-only`
- Runtime behavior changes: none
- Rust code changes: none
- Docs changes: add the ECStore config consumer inventory for later config-model
  contract, pure-move, and global-state migration work.

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
- [x] `G-008` Capture admin route-action snapshot.
  - Acceptance: [`admin-route-action-snapshot.md`](admin-route-action-snapshot.md)
    records current route families, handler ownership, authorization actions,
    public exceptions, table-catalog routes, and `/minio/admin` compatibility
    alias behavior.
- [x] `G-009` Enforce pre-push three-expert review.
  - Acceptance: [`crate-boundaries.md`](crate-boundaries.md) requires
    quality/architecture, migration-preservation, and testing/verification review
    before push.
- [x] `G-010` Inventory `ecstore::config::{Config, KV, KVS}` consumers.
  - Acceptance:
    [`ecstore-config-consumer-inventory.md`](ecstore-config-consumer-inventory.md)
    records the current model definitions, global accessors, persistence helpers,
    consumer groups, migration risks, and do-not-change contract.
- [~] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Current branch: not in scope.
  - Next PR: add a mechanical check that all migration docs use the same PR type
    vocabulary.

## Next PRs

1. `ci-gate`: add focused checks for PR type vocabulary and temporary
   compatibility marker/register consistency.
2. `test-only`: add a mechanical admin route matrix guard from the current
   snapshot and `route_registration_test.rs`.
3. `contract`: define the config-model contract surface while preserving the
   existing `Config`, `KV`, and `KVS` behavior.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Re-review confirmed dependency/call direction arrows, scanner global readers, adjacent scanner/module-switch/IAM persistence helpers, notify config-manager persistence, narrowed test wording, and `Config model contract` wording are source-backed |
| Migration preservation | pass | Confirmed this branch is docs-only, aligned with `rustfs/backlog#660`, and does not touch runtime logic, storage hot paths, global state implementation, compatibility code, scripts, or crate boundaries |
| Testing/verification | pass | Confirmed docs-only verification is sufficient after wording was narrowed and the final staged diff check covers all docs |

## Verification Notes

Passed locally (docs-only):

- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `git diff --check`

Final pre-push after staging all docs:

- `git diff --cached --check`
- focused source review of `crates/ecstore/src/config/mod.rs`,
  `crates/ecstore/src/config/com.rs`, `rustfs/src/app/context.rs`,
  `rustfs/src/server/{event,audit}.rs`, `rustfs/src/admin/**/*.rs`,
  `crates/{notify,audit,targets,iam,scanner}/**/*.rs`
- three-expert review: quality/architecture, migration preservation, and
  testing/verification

## Handoff Notes

- Keep Phase 0 PRs small. Do not move Config, Storage API, Runtime, or ECStore
  code inside this `docs-only` branch.
- Keep CI checks in a separate `ci-gate` PR so the PR type rule remains enforceable.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- The next config-model PR must preserve the current tuple-struct shapes and
  persistence behavior before introducing narrower provider contracts.
