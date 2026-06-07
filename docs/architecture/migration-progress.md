# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-security-governance-types`
- Baseline: `upstream/main` at `c03c0ebc363209a4820e6d6f2267e0342c6a7782`
- PR type for this branch: `contract`
- Runtime behavior changes: none
- Rust code changes: add the pure `rustfs-security-governance` contract crate
  with admin route matrix metadata types, typed validation errors, and unit tests.
- CI/script changes: none
- Docs changes: record the Phase 1 security-governance contract handoff.

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

## Phase 1 Security Governance Tasks

- [x] `S-001` Add `crates/security-governance`.
  - Acceptance: the crate is a workspace member and has no dependency on
    `rustfs`, `ecstore`, admin handlers, Axum, or runtime state.
  - Verification: `cargo check -p rustfs-security-governance`.
- [x] `S-002` Add admin route matrix core types.
  - Acceptance: `AdminRouteSpec`, `AdminRouteAccess`, `AdminActionRef`,
    `PublicRouteKind`, `RouteRiskLevel`, and validation errors model route
    governance metadata without registering routes or enforcing auth.
  - Verification: `cargo test -p rustfs-security-governance`.
- [ ] `S-003` Add redaction contract types.
- [ ] `S-004` Add serde policy marker types.
- [ ] `S-005` Add supply-chain policy contract types.
- [ ] `S-006` Add `rustfs/src/admin/route_policy.rs` backed by these contract
  types, without changing route registration or auth behavior.

## Next PRs

1. `contract`: add redaction, serde policy, or supply-chain governance contract
   modules inside rustfs-security-governance.
2. `contract`: add an admin route policy table that consumes the new
   admin_matrix types while preserving route registration and auth behavior.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Confirmed this is a pure `contract` PR: the new crate has a narrow admin_matrix module, typed errors via thiserror, no dependency on implementation crates, and no route registration or auth integration |
| Migration preservation | pass | Confirmed no runtime code paths changed: no admin handler/router edits, no startup/global-state/storage hot-path movement, no compatibility wrappers, and no behavior drift |
| Testing/verification | pass | Confirmed focused unit tests cover valid admin/public specs, duplicate method/path rejection, empty path rejection, and empty action rejection; focused checks, migration guard scripts, dependency tree review, diff check, and full `make pre-commit` passed |

## Verification Notes

Passed:
- `cargo check -p rustfs-security-governance`
- `cargo test -p rustfs-security-governance`
- `cargo fmt --all --check`
- `cargo tree -p rustfs-security-governance --edges normal`
- `./scripts/check_architecture_migration_rules.sh`
- `./scripts/check_layer_dependencies.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `git diff --check`
- `make pre-commit`

## Handoff Notes

- Keep this Phase 1 branch as a pure `contract` PR. Do not add
  `rustfs/src/admin` integration, route registration changes, auth enforcement,
  Config moves, Storage API moves, Runtime moves, or ECStore moves.
- The new crate is allowed to depend on generic Rust libraries such as
  `thiserror`, but must stay independent from implementation crates and runtime
  state.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- The next admin route policy PR may consume the contract types, but must
  preserve current route registration, alias canonicalization, public
  exceptions, and handler-level authorization behavior.
