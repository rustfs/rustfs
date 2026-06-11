# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-storage-admin-remaining-readers`
- Baseline: `origin/main` at `f325b9f71ce4807488829e6f558383242bbcb6a2`
- PR type for this branch: `consumer-migration`
- Runtime behavior changes: none.
- Rust code changes: route maintenance/background read-side storage inventory
  consumers through the inventory-facing `StorageAdminApi` contract while
  preserving the old `StorageAPI` compatibility surface.
- CI/script changes: none.
- Docs changes: record API-007 maintenance/background inventory-reader context,
  verification evidence, and expert review outcomes.

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

## Phase 1a Config Model Tasks

- [x] `CFG-001` Inventory `ecstore::config::{Config, KV, KVS}` consumers.
  - Acceptance:
    [`ecstore-config-consumer-inventory.md`](ecstore-config-consumer-inventory.md)
    records the current definitions, persistence helpers, global accessors,
    consumer groups, migration risks, and do-not-change contract.
- [x] `CFG-002` Decide model boundary.
  - Acceptance:
    [`config-model-boundary-adr.md`](config-model-boundary-adr.md) records
    `rustfs-config` as the target package, `server_config` as the future model
    module, allowed dependencies, forbidden dependencies, preserved shape, and
    extraction verification gates.
- [ ] `CFG-003` Move pure model definitions.
  - Next boundary: move only `Config`, `KV`, `KVS`, and default-registration
    surface into `rustfs-config`; keep persistence helpers and global
    server-config state in `ecstore`.
- [ ] `CFG-004` Keep old `ecstore::config::*` compatibility path.
  - Required compatibility: source must contain `RUSTFS_COMPAT_TODO(CFG-004)`
    and a matching cleanup-register entry.

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
- [x] `S-013` Apply KMS redaction.
  - Acceptance: KMS Debug output and admin status response summaries contain no
    Vault token, AppRole secret ID, or local master key values.
  - Must preserve: internal KMS config values remain available to runtime code
    and persisted config serialization still writes the original secret values.
  - Verification: focused KMS redaction/status tests, full KMS tests, migration
    guards, Rust quality scan, clippy, and `make pre-commit` passed.
- [x] `KMSD-001` Inventory KMS development defaults.
  - Acceptance:
    [`kms-development-defaults-inventory.md`](kms-development-defaults-inventory.md)
    records Local and Vault defaults for missing master keys, temp key dirs,
    HTTP Vault addresses, default dev-token credentials, and skip-TLS behavior.
  - Must preserve: no KMS runtime behavior, config serialization,
    authorization, startup order, storage path, or crate boundary changes.
  - Verification: docs diff review, migration guards, metrics reference guard,
    and `git diff --check`.

## Phase 2 Storage API Tasks

- [x] `API-001` Add `crates/storage-api`.
  - Acceptance: `rustfs-storage-api` is a workspace member and remains a
    dependency-free contract crate.
  - Verification: `cargo check -p rustfs-storage-api`.
- [x] `API-002` Move public storage error/result contracts.
  - Current PR: `rustfs/rustfs#3313` merged.
  - Completed slice: add public `StorageErrorCode` and `StorageResult`
    contracts in `rustfs-storage-api`, then make ECStore
    `StorageError::to_u32/from_u32` consume the shared code table.
  - Deferred: keep the full ECStore `StorageError` enum and ECStore-specific
    conversions in `rustfs-ecstore` until the `DiskError`, filemeta, lock, and
    `std::io::Error` downcast boundary is proven safe.
  - Acceptance: storage-api contract tests pass, ECStore compatibility tests
    prove numeric codes match the new contract, and
    `cargo check -p rustfs-storage-api -p rustfs-ecstore` passes.
  - Must preserve: storage error display, conversions, object error mapping,
    quorum classification, and reserved code gaps `0x2B/0x2C`.
  - Risk defense: no storage hot-path enum move in this PR; only numeric code
    mapping uses the new contract.
- [x] `API-003` Move DTOs.
  - Current PR: `rustfs/rustfs#3314` merged.
  - Completed slice: move the pure bucket/options DTO subset:
    `MakeBucketOptions`, `SRBucketDeleteOp`, `DeleteBucketOptions`,
    `BucketOptions`, and `BucketInfo`.
  - Acceptance: `rustfs-storage-api` exports these DTOs, ECStore re-exports
    them from the old `ecstore::store_api` path, and compatibility cleanup is
    registered with `RUSTFS_COMPAT_TODO(API-003)`.
  - Must preserve: no `ObjectOptions`, `ObjectInfo`, reader, compression,
    encryption, filemeta conversion, multipart conversion, route, storage, or
    runtime behavior changes in this PR.
- [x] `API-006` Add disk inventory/admin trait.
  - Current PR: `rustfs/rustfs#3330` merged.
  - Completed slice: add `StorageAdminApi` and `DiskSetSelector` to
    `rustfs-storage-api`.
  - Acceptance: `StorageAdminApi` exposes backend info, global storage info,
    local storage info, disk-set inventory, and drive-count surfaces without
    depending on ECStore implementation types.
  - Must preserve: no `StorageAPI::get_disks` removal, no ECStore implementation
    change, no admin/readiness/capacity behavior change.
  - Risk defense: use associated types for backend/storage/disk DTOs so this
    contract slice does not pull `rustfs-madmin` or `rustfs-ecstore` into
    `rustfs-storage-api`.
  - Verification: focused storage-api tests, dependency tree, migration guards,
    formatting, and diff hygiene.
- [~] `API-007` Dual-route `get_disks` consumers.
  - Completed first slice: `rustfs/rustfs#3331` bound `ECStore` to
    `StorageAdminApi` while keeping all consumers unchanged.
  - Completed second slice: `rustfs/rustfs#3332` migrated the admin
    storage-class config drive-count consumer to
    `StorageAdminApi::set_drive_counts`.
  - Completed third slice: `rustfs/rustfs#3333` migrated
    `DefaultAdminUsecase` storage-info reads to
    `StorageAdminApi::storage_info`.
  - Completed fourth slice: `rustfs/rustfs#3334` migrated account-info
    `backend_info`, rebalance status `storage_info`, and runtime readiness
    `storage_info`.
  - Completed fifth slice: `rustfs/rustfs#3335` migrated grouped observability,
    RPC health, server-info, realtime metrics, and notification read-side
    consumers.
  - Completed sixth slice: `rustfs/rustfs#3336` migrated ECStore internal
    decommission space, local-storage-info, backend-info, drive-count, and
    disk-inventory admin handlers away from old `StorageAPI` method calls.
  - Current branch slice: migrate maintenance/background read-side storage
    inventory consumers in rebalance metadata initialization, heal resume disk
    lookup, and scanner local disk scan lookup.
  - Acceptance: these maintenance/background consumers no longer use old
    `StorageAPI` calls for storage-info or disk-set inventory when the
    inventory-facing `StorageAdminApi` contract already represents the same
    read-only operation.
  - Must preserve: old `StorageAPI` trait shape, `StorageAPI::get_disks`
    behavior, rebalance metadata serialization/save/load, heal resume disk
    selection, scanner local disk selection, object/rebalance object selection
    paths, scanner data-cache persistence, heal object repair, object paths,
    replication/config/tier persistence, and storage hot paths.
  - Risk defense: change only trait call entry points to existing ECStore
    `StorageAdminApi` handlers; do not migrate object APIs, config or
    replication persistence, scanner cache writes, heal object repair, or
    storage implementation hot paths in this PR.
  - Verification:
    - `cargo fmt --all && cargo fmt --all --check`.
    - `cargo check -p rustfs-ecstore -p rustfs-heal -p rustfs-scanner`.
    - `cargo test -p rustfs-ecstore rebalance --lib`.
    - `cargo test -p rustfs-heal storage --lib`.
    - `cargo test -p rustfs-scanner scanner_io --lib`.
    - `./scripts/check_architecture_migration_rules.sh`.
    - `./scripts/check_layer_dependencies.sh`.
    - `./scripts/check_metrics_migration_refs.sh`.
    - `./scripts/check_unsafe_code_allowances.sh`.
    - `git diff --check`.
  - Pre-push review: pending required quality/architecture,
    migration-preservation, and testing/verification review.

## Phase 8 Background Controller Tasks

- [x] `BGC-001` Inventory background services.
  - Acceptance:
    [`background-services-inventory.md`](background-services-inventory.md)
    records scanner, heal, lifecycle, replication, config reload, metrics,
    shutdown, cancellation, and side-effect surfaces before controller work.
  - Must preserve: no code behavior change and no new controller contract in
    this PR.
  - Verification: docs-only architecture checks and diff hygiene.
- [x] `BGC-002` Define minimal controller contract.
  - Acceptance:
    [`background-controller-contract.md`](background-controller-contract.md)
    defines desired/current/status/reconcile vocabulary, status state
    semantics, service boundaries, and side-effect rules without starting
    workers or changing scheduling.
  - Must preserve: no Rust trait, scheduler, service registry, worker
    start/stop path, storage write, readiness change, peer signal, or runtime
    behavior change.
  - Verification: docs-only architecture checks and diff hygiene.

## Next PRs

1. `consumer-migration`: migrate the remaining readiness/admin/capacity
   consumers to the inventory-facing admin contract one group at a time.
2. `dependency-migration`: remove duplicate old-path admin surfaces only after
   consumer migration proves equivalent behavior.
3. `api-extraction`: move only the pure server-config model into
   rustfs-config as CFG-003.
4. `api-extraction`: keep the old rustfs_ecstore::config::* path with
   RUSTFS_COMPAT_TODO(CFG-004) and cleanup-register coverage.
5. `consumer-migration`: migrate external consumers one group at a time only
   after the model path and compatibility shim are stable.
6. `security-change`: make Local KMS unsafe defaults explicit development
   opt-ins or production failures in KMSD-002.
7. `security-change`: make Vault unsafe defaults explicit development opt-ins
   or production failures in KMSD-003.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Confirmed the diff is limited to maintenance/background inventory-reader entry-point migration plus accurate progress notes; dependency direction, naming, and scope are clean. |
| Migration preservation | pass | Confirmed old `StorageAPI` shape remains, ECStore old/new trait paths still delegate to the same storage-info and disk-inventory handlers, and rebalance/heal/scanner call sites only change the read-side entry point. |
| Testing/verification | pass | Confirmed the focused ECStore/heal/scanner checks, migration guards, diff hygiene, and added-line Rust quality scan are sufficient for this equivalent read-side call-path migration while skipping full pre-commit under the current instruction. |

## Verification Notes

Passed:
- `cargo fmt --all && cargo fmt --all --check`.
- `cargo check -p rustfs-ecstore -p rustfs-heal -p rustfs-scanner`.
- `cargo test -p rustfs-ecstore rebalance --lib`; 198 passed.
- `cargo test -p rustfs-heal storage --lib`; 3 passed.
- `cargo test -p rustfs-scanner scanner_io --lib`; 18 passed.
- `./scripts/check_architecture_migration_rules.sh`.
- `./scripts/check_layer_dependencies.sh`.
- `./scripts/check_metrics_migration_refs.sh`.
- `./scripts/check_unsafe_code_allowances.sh`.
- `git diff --check`.
- Rust code-quality scan on changed `.rs` files, plus added-line scan for
  unwrap/expect, numeric casts, `Result<_, String>`, `Box<dyn Error>`,
  println/eprintln, and `Ordering::Relaxed`.

Notes:
- Full pre-commit was intentionally skipped because the focused tests and guards
  above passed, per the current migration instruction to increase PR granularity.
- The broad changed-file quality scan reports pre-existing test unwrap/expect
  plus pre-existing casts and relaxed atomics in touched ECStore files; the
  added-line scan found no new risky code patterns.
- Old `StorageAPI` trait shape and implementations remain in place; ECStore old
  and new trait paths delegate to the same storage-info and disk-inventory
  handlers.
- Object/rebalance object selection paths, scanner cache persistence, heal
  object repair, object APIs, replication/config/tier persistence paths, and
  storage hot paths are unchanged.
- No temporary compatibility shim was added.

## Handoff Notes

- Keep this API-007 slice as a maintenance/background inventory-reader
  `consumer-migration` PR.
- The only scanner/heal scope in this PR is read-side disk lookup for scanner
  local disk scan and heal resume; do not migrate scanner cache writes, heal
  object repair, object APIs, replication, config/tier persistence, or storage
  hot-path consumers in this PR.
- Do not remove `StorageAPI::get_disks` or route object/hot-path consumers
  around it in this PR.
- Do not make the old `StorageAPI` trait inherit `StorageAdminApi` in this PR.
- Do not add temporary compatibility code unless a matching
  `RUSTFS_COMPAT_TODO(<task-id>)` marker and cleanup-register entry are added.
