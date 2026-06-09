# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-kms-handler-actions`
- Baseline: `upstream/main` at `4fec606dc4f92b19e085f1609a188e82a72720ff`
- PR type for this branch: `security-change`
- Runtime behavior changes: high-risk KMS admin handlers now authorize through
  dedicated `kms:*` actions instead of broad `ServerInfoAdminAction`.
- Rust code changes: migrate KMS handler action lists and route policy inventory
  to dedicated KMS actions, while keeping temporary legacy create/status admin
  action compatibility with `RUSTFS_COMPAT_TODO(S-012)`.
- CI/script changes: none
- Docs changes: record S-012 action migration status and temporary compatibility cleanup.

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
- [x] `S-011` Add KMS action taxonomy.
  - Acceptance: `KmsAction` can parse and serialize dedicated configure,
    service-control, clear-cache, generate-data-key, delete, rotate, list, and
    describe actions; wildcard matching still works.
  - Verification: `cargo test -p rustfs-policy action --no-fail-fast`.
- [x] `S-012` Migrate KMS handlers to dedicated actions.
  - Acceptance: KMS data-key, delete/cancel-delete, cache, configure,
    service-control, list, and describe handlers use dedicated `kms:*` actions.
  - Compatibility: legacy KMS create/status admin actions are retained only as
    temporary compatibility paths and registered in
    [`compat-cleanup-register.md`](compat-cleanup-register.md).
  - Verification: focused handler and route policy tests, migration rules,
    formatting, and `make pre-commit`.

## Next PRs

1. `contract`: add initial policy inventory tables for redaction, serde, or
   supply-chain governance only after the contract shape remains stable.
2. `security-change`: apply KMS response/config redaction after action
   migration settles.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Single `security-change` PR; KMS auth action lists are local helper functions, names match `KmsAction`, and no storage/startup/global-state logic is touched. |
| Migration preservation | pass | Legacy create/status admin actions are retained only behind `RUSTFS_COMPAT_TODO(S-012)` and registered for cleanup; broad `ServerInfoAdminAction` is intentionally not retained for high-risk KMS operations. |
| Testing/verification | pass | Focused handler/route-policy tests cover dedicated actions and broad-action negative cases; migration rules, formatting, full `make pre-commit`, nextest, and doctests pass. |

## Verification Notes

Passed:
- Baseline `cargo test -p rustfs admin::handlers::kms --no-fail-fast`
- Baseline `cargo test -p rustfs admin::route_policy --no-fail-fast`
- `cargo fmt --all --check`
- `cargo test -p rustfs admin::handlers::kms --no-fail-fast`
- `cargo test -p rustfs admin::route_policy --no-fail-fast`
- `./scripts/check_architecture_migration_rules.sh`
- `git diff --check`
- `make pre-commit`

Notes:
- This branch changes only KMS admin authorization action selection and route
  policy inventory. It does not change KMS runtime defaults, redaction, startup
  order, global state, storage paths, or crate boundaries.
- `make pre-commit` passed all checks, including 5682 nextest tests and
  workspace doctests.

## Handoff Notes

- Keep this S-012 branch as a focused `security-change` PR. Do not change KMS
  defaults, redaction, admin route registration shape, Config moves, Storage API
  moves, Runtime moves, or ECStore moves.
- `rustfs` may depend on `rustfs-security-governance` for contract metadata;
  the security-governance crate must stay independent from implementation
  crates and runtime state.
- Do not add temporary compatibility code without a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry.
- The next KMS security PR should handle redaction or production default
  hardening separately; do not bundle those with this action migration.
