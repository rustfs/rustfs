# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-admin-route-policy`
- Baseline: `origin/main` at `3d0e6ce0da93de0f4618beb194ae2241df71f344`
- PR type for this branch: `contract`
- Runtime behavior changes: none
- Rust code changes: add `rustfs/src/admin/route_policy.rs` as a pure
  admin-route policy inventory backed by `rustfs-security-governance` route
  contract types, with explicit deferred entries for routes that need
  contextual, S3-action, multi-action, credential-only, or not-implemented
  contract support.
- CI/script changes: none
- Docs changes: record the S-006 route policy handoff.

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
- [x] `S-003` Add redaction contract types.
  - Acceptance: `RedactionRule`, `RedactionLevel`, and validation errors model
    sensitive field handling without logging, masking, or runtime integration.
  - Verification: `cargo test -p rustfs-security-governance`.
- [x] `S-004` Add serde policy marker types.
  - Acceptance: `SerdePolicy`, `SerdePolicyKind`, `UnknownFieldPolicy`, and
    validation errors model strict ingress and compatibility serde contracts
    without changing deserialization behavior.
  - Verification: `cargo test -p rustfs-security-governance`.
- [x] `S-005` Add supply-chain policy contract types.
  - Acceptance: `ArtifactIntegrityPolicy`, `ArtifactSourceKind`, and validation
    errors model digest, signature, and provenance requirements without changing
    release or CI behavior.
  - Verification: `cargo test -p rustfs-security-governance`.
- [x] `S-006` Add `rustfs/src/admin/route_policy.rs` backed by these contract
  types, without changing route registration or auth behavior.
  - Acceptance: direct `AdminRouteSpec` entries cover routes with a single
    stable admin policy action, deferred inventory records routes that need
    richer contract support, and tests prove the combined inventory covers every
    registered admin route.

## Next PRs

1. `contract`: add initial policy inventory tables for redaction, serde, or
   supply-chain governance only after the contract shape remains stable.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Pure contract inventory module; no runtime route registration, alias canonicalization, or handler auth mutation. Names and deferred reasons are explicit and avoid false policy precision. |
| Migration preservation | pass | Aligns with backlog #660: directory and contract boundary first, no global-state migration, no crate split, and no storage hot-path behavior drift. |
| Testing/verification | pass | Route-policy tests cover validation, public exceptions, table-catalog scope, deferred contextual routes, and every registered admin route. Focused checks, guard scripts, and full pre-commit pass. |

## Verification Notes

Passed:
- `cargo test -p rustfs admin::route_policy`
- `cargo fmt --all`
- `cargo fmt --all --check`
- `cargo check -p rustfs`
- `./scripts/check_architecture_migration_rules.sh`
- `./scripts/check_metrics_migration_refs.sh`
- `./scripts/check_layer_dependencies.sh`
- `git diff --check`
- Rust quality scans for production `unwrap`/`expect`, narrowing casts,
  string errors, boxed dynamic errors, debug printing, and relaxed atomics in
  `rustfs/src/admin/route_policy.rs`
- `make pre-commit`

Notes:
- `cargo test -p rustfs admin::route_policy` passed 7 route-policy tests.
- `make pre-commit` passed all checks, including 5526 nextest tests and
  workspace doctests.

## Handoff Notes

- Keep this S-006 branch as a pure `contract` PR. Do not change
  admin route registration, `/minio/admin` alias canonicalization, handler auth
  enforcement, Config moves, Storage API moves, Runtime moves, or ECStore moves.
- `rustfs` may depend on `rustfs-security-governance` for contract metadata;
  the security-governance crate must stay independent from implementation
  crates and runtime state.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- Deferred route policy entries must remain explicit until the contract crate
  gains richer support for contextual owner checks, S3 actions, multiple
  accepted actions, credential-only gates, and not-implemented routes.
