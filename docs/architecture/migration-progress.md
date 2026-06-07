# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-admin-route-matrix-guard`
- Baseline: `upstream/main` at `dee550a831cbfc24aa5cecd60f6f3a4cfe1a2e30`
- PR type for this branch: `test-only`
- Runtime behavior changes: none
- Rust code changes: add test-only admin route matrix coverage, test-only
  registered route tracking, and a private shared admin route registration helper
  so tests exercise the production registration sequence.
- CI/script changes: none
- Docs changes: record the route matrix guard handoff.

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
  - Current branch: add a mechanical admin route matrix guard from
    [`admin-route-action-snapshot.md`](admin-route-action-snapshot.md) and
    `rustfs/src/admin/route_registration_test.rs`.
  - Remaining follow-up: add checks for public re-export and storage trait
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
- [x] `TEST-PRTYPE-001` Check PR type enum consistency.
  - Acceptance: `./scripts/check_architecture_migration_rules.sh` parses the
    allowed PR types from [`crate-boundaries.md`](crate-boundaries.md) and fails
    when `ARCHITECTURE.md` or architecture docs reference an unknown PR type.
- [x] `COMPAT-REG-001` Check temporary compatibility cleanup consistency.
  - Acceptance: `./scripts/check_architecture_migration_rules.sh` fails when a
    source `RUSTFS_COMPAT_TODO(<task-id>)` marker lacks a cleanup-register entry,
    when a register entry lacks a source marker, or when a source marker omits a
    removal condition.

## Next PRs

1. `contract`: define the config-model contract surface while preserving the
   existing `Config`, `KV`, and `KVS` behavior.
2. `ci-gate`: add focused checks for public re-export and storage trait coverage
   before pure moves.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Confirmed the matrix helpers keep the snapshot explicit, `S3Router` instrumentation is fully `cfg(test)`, the production registration order is shared through one private helper, and the non-test insertion path keeps existing route construction/insert behavior |
| Migration preservation | pass | Confirmed this remains `test-only` in effect: no route handler/auth/alias semantics changed, no storage hot-path/global-state/crate-split changes, and MinIO admin alias coverage is expanded across all admin-prefix matrix entries |
| Testing/verification | pass | Confirmed focused route tests pin `ENV_HEALTH_ENDPOINT_ENABLE=true`, use the production registration helper, and pass with formatting, non-test cargo check, migration guard scripts, diff check, full `make pre-commit`, and temporary unaccounted-route negative coverage |

## Verification Notes

Passed:
- `cargo fmt --all --check`
- `cargo check -p rustfs --lib`
- `cargo test -p rustfs admin::route_registration_test -- --nocapture`
- `./scripts/check_architecture_migration_rules.sh`
- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `git diff --check`
- temporary negative check for an unaccounted admin route registration
- `make pre-commit`

## Handoff Notes

- Keep Phase 0 PRs small. Do not move Config, Storage API, Runtime, or ECStore
  code inside this `test-only` branch.
- Route matrix instrumentation must remain test-only and must not alter dispatch
  or auth behavior.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- The next config-model PR must preserve the current tuple-struct shapes and
  persistence behavior before introducing narrower provider contracts.
- Do not move `config::com` until a dedicated helper-user inventory covers
  ECStore-internal persistence paths as well as adjacent non-model users.
