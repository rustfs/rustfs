# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-storage-api-namespace-lock-cleanup`
- Baseline: `origin/main` at `7146c893cbbb84a0d5332a7a8a79b70d57191bcc`
- PR type for this branch: `api-extraction`
- Runtime behavior changes: none intended.
- Rust code changes: migrate remaining namespace-lock consumers to
  `NamespaceLocking`, implement namespace locking directly on ECStore storage
  types, and remove the temporary namespace-lock compatibility method from the
  full storage trait.
- CI/script changes: none.
- Docs changes: remove the API-012 cleanup-register entry and record the
  completed namespace-lock compatibility cleanup in progress notes.

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
- [x] `CFG-003` Move pure model definitions.
  - Completed slice: `rustfs/rustfs#3351` moved only `Config`, `KV`, `KVS`,
    and default-registration surface into `rustfs-config`; persistence helpers
    and global server-config state remain in `ecstore`.
  - Must preserve: tuple struct shapes, serde alias behavior, default
    application, internal JSON shape, and existing persisted config semantics.
- [x] `CFG-004` Keep and clean up old `ecstore::config::*` compatibility path.
  - Completed slice: `rustfs/rustfs#3351` re-exported moved model types and
    default-registration surface from `rustfs_ecstore::config` with
    `RUSTFS_COMPAT_TODO(CFG-004)` and cleanup-register coverage.
  - Cleanup slice: remove the temporary model re-export and smoke test after
    CFG-005/CFG-006/CFG-007 migrated all in-repo consumers to
    `rustfs_config::server_config`.
- [x] `CFG-005` Migrate external server-config model consumers.
  - Current branch: migrate admin handlers, admin services, runtime context,
    server audit/event setup, and the audit/notify/targets/iam crates from the
    temporary `rustfs_ecstore::config::{Config, KV, KVS}` model path to
    `rustfs_config::server_config`.
  - Acceptance: external consumers use the model crate for pure config types
    while still using ECStore for persistence helpers, global server-config
    accessors, storage-class helpers, and startup initialization.
- [x] `CFG-006` Migrate ECStore service/default model consumers.
  - Current branch: migrate ECStore config default modules, shared config
    helpers, and store accessor signatures to the `rustfs_config` model type
    while preserving ECStore-owned persistence and runtime state.
  - Acceptance: ECStore internals no longer depend on the old compatibility
    model import path except the deliberate compatibility smoke test; the old
    public re-export remains available for downstream callers until CFG-004 is
    cleaned up.
- [x] `CFG-007` Migrate scanner runtime-config model consumer.
  - Current branch: migrate scanner runtime-config parsing and validation from
    the temporary `rustfs_ecstore::config::{Config, KVS}` model path to
    `rustfs_config::server_config`.
  - Acceptance: scanner uses the model crate for pure server-config types while
    still using ECStore for the global server-config accessor; scanner defaults,
    env overrides, persisted-config validation, cycle scheduling, bitrot-cycle
    compatibility, cache timeout, and alert threshold semantics remain
    unchanged.
- [x] `CFG-008` Move global server-config accessors.
  - Current branch: move `GLOBAL_SERVER_CONFIG`,
    `get_global_server_config`, and `set_global_server_config` to
    `rustfs_config::server_config`; migrate in-repo runtime consumers to the
    new owner.
  - Compatibility: keep
    `rustfs_ecstore::config::{get_global_server_config,
    set_global_server_config}` as a temporary re-export with
    `RUSTFS_COMPAT_TODO(CFG-008)`.
  - Cleanup slice: remove the temporary accessor re-export after code scans
    showed in-repo consumers import accessors from
    `rustfs_config::server_config`.
  - Acceptance: ECStore still owns `ConfigSys`, config persistence helpers,
    storage-class global state, default registration wiring, and startup
    initialization; global server-config reads and writes keep the same
    `std::sync::RwLock<Option<Config>>` clone semantics.

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
  - Cleanup branch: `overtrue/arch-storage-api-dto-compat-cleanup`.
  - Completed slice: move the pure bucket/options DTO subset:
    `MakeBucketOptions`, `SRBucketDeleteOp`, `DeleteBucketOptions`,
    `BucketOptions`, and `BucketInfo`.
  - Cleanup slice: migrate in-repo external consumers to
    `rustfs_storage_api`, keep ECStore implementation use crate-private, and
    remove the old public `ecstore::store_api` bucket DTO re-export.
  - Acceptance: `rustfs-storage-api` exports these DTOs, in-repo external
    consumers no longer use the old `rustfs_ecstore::store_api` DTO path, and
    `RUSTFS_COMPAT_TODO(API-003)` is removed from source and cleanup register.
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
- [x] `API-007` Dual-route `get_disks` consumers.
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
  - Completed seventh slice: `rustfs/rustfs#3337` migrated maintenance and
    background read-side storage inventory consumers in rebalance metadata
    initialization, heal resume disk lookup, and scanner local disk scan lookup.
  - Completion acceptance: admin inventory consumers no longer use old
    `StorageAPI` calls for backend info, storage info, local storage info,
    drive-count, or disk-set inventory when the inventory-facing
    `StorageAdminApi` contract represents the same read-only operation.

- [x] `API-008` Remove duplicate old-path admin surfaces.
  - Completed slice: `rustfs/rustfs#3340` removed duplicate admin-read methods
    from the old `StorageAPI` trait and its ECStore/Sets/SetDisks/test
    implementations after API-007 migrated their consumers.
  - Acceptance: old `StorageAPI` keeps storage operation traits while admin
    inventory surfaces live only on `StorageAdminApi`.

- [x] `API-009` Narrow metadata helper storage bounds.
  - Completed slice: `rustfs/rustfs#3343` narrowed server config, tier config,
    rebalance metadata, and startup metadata migration helper bounds away from
    full `StorageAPI` when the helper only needs `ObjectIO`,
    `ObjectOperations`, `BucketOperations`, `ListOperations`, or
    `StorageAdminApi`.
  - Acceptance: metadata helper contracts express the actual operation group
    they need, while callers and persistence behavior remain unchanged.

- [x] `API-010` Narrow replication resync metadata bounds.
  - Completed slice: `rustfs/rustfs#3345` narrowed replication resync status
    load/save/mark/persist helper bounds away from full `StorageAPI` when the
    helper only needs `ObjectIO`.
  - Acceptance: resync metadata helpers express object-I/O-only persistence
    requirements, while replication execution, delete replication, multipart
    replication, object lookups, and scheduling behavior remain on full
    `StorageAPI` where needed.

- [x] `API-011` Narrow scanner cache helper storage bounds.
  - Completed slice: `rustfs/rustfs#3348` narrowed scanner data-usage cache
    load/save and cache snapshot persistence helper bounds away from full
    `StorageAPI` when the helper only needs `ObjectIO`.
  - Acceptance: scanner cache persistence helpers express object-I/O-only
    requirements, while scanner cycle orchestration, bucket scanning, local disk
    selection, cache publication, and storage hot paths remain unchanged.
  - Must preserve: data-usage cache wire format, cache object paths, backup
    cache paths, retry and timeout behavior, cache-save metrics, publish/update
    channel behavior, scanner cycle scheduling, disk scan concurrency, bucket
    scan semantics, lifecycle/replication decisions, and storage hot paths.
  - Risk defense: do not move traits to `rustfs-storage-api`, do not remove
    `StorageAPI`, do not alter helper bodies, and do not narrow scanner paths
    that need bucket operations, disk inventory, or full storage orchestration.
  - Verification: focused compile/tests, migration guards, Rust risk scan, and
    required quality/architecture, migration-preservation, and
    testing/verification review passed.

- [x] `API-012` Narrow table catalog object backend bounds.
  - Completed slice: `rustfs/rustfs#3350` added a narrow `NamespaceLocking`
    operation-group trait as a compatibility facade, then narrowed
    `EcStoreTableCatalogObjectBackend` from full `StorageAPI` to `ObjectIO`,
    `ObjectOperations`, `ListOperations`, and `NamespaceLocking`.
  - Cleanup slice: migrate the remaining scanner leader-lock and self-copy
    object use-case namespace-lock consumers to `NamespaceLocking`, implement
    namespace locking directly on ECStore storage types, and remove the
    temporary namespace-lock compatibility method from the full storage trait
    and cleanup register entry.
  - Acceptance: table catalog object backend contracts express the actual
    object read/write, metadata/delete, list, and namespace-lock capabilities
    they need; namespace-lock consumers depend on `NamespaceLocking` instead of
    full `StorageAPI`; and storage lock behavior remains unchanged.
  - Must preserve: table catalog object paths, metadata pointer semantics,
    optimistic write preconditions, object listing pagination, missing-object
    handling, namespace write-lock acquisition, object APIs,
    scanner/heal/replication/config persistence, and storage hot paths.
  - Risk defense: do not move traits into `rustfs-storage-api`, do not change
    lock implementation code, do not alter table catalog method bodies, and do
    not retain stale API-012 compatibility markers after the old `StorageAPI`
    lock method is removed.
  - Verification: focused compile/tests, migration guards, Rust risk scan, and
    required quality/architecture, migration-preservation, and
    testing/verification review passed.

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

1. `security-change`: make Local KMS unsafe defaults explicit development
   opt-ins or production failures in KMSD-002.
2. `security-change`: make Vault unsafe defaults explicit development opt-ins
   or production failures in KMSD-003.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | Confirmed the diff removes the temporary namespace-lock method from the full `StorageAPI` trait while keeping `NamespaceLocking` as the narrow operation-group contract. |
| Migration preservation | pass | Confirmed ECStore, Sets, SetDisks, scanner leader locking, self-copy locking, replication resync, rebalance metadata, and config test storage retain their existing lock behavior and only narrow trait dependencies. |
| Testing/verification | pass | Confirmed focused compile/tests, migration guards, dependency guard, old-path scan, and added-line Rust risk scan cover this cleanup; full pre-commit is skipped under the current larger-granularity instruction. |

## Verification Notes

Passed on `7146c893cbbb84a0d5332a7a8a79b70d57191bcc`:
- `cargo check -p rustfs-ecstore -p rustfs-scanner -p rustfs --all-targets`.
- `cargo test -p rustfs-ecstore --test storage_api_compat_test`; 2 passed.
- `cargo test -p rustfs-ecstore --lib new_ns_lock`; 2 passed.
- `cargo test -p rustfs-scanner --tests --no-run`.
- `cargo test -p rustfs --lib --no-run`.
- `cargo fmt --all --check`.
- `./scripts/check_architecture_migration_rules.sh`.
- `./scripts/check_layer_dependencies.sh`.
- `./scripts/check_metrics_migration_refs.sh`.
- `git diff --check`.
- API-012 old-path and marker scan found no stale API-012 compatibility marker
  or old full-storage-trait namespace-lock method references in `crates`,
  `rustfs/src`, or architecture docs.
- Added-line Rust risk scan found no new production `unwrap`/`expect`, lossy
  numeric casts, stringly public errors, boxed dynamic errors, stdout/stderr
  printing, or relaxed atomic ordering.

Notes:
- Full pre-commit may be skipped if focused tests, compile checks, and guards
  pass, per the current instruction to increase PR granularity.
- This slice removes only the old namespace-lock method from the full storage
  trait. ECStore retains bucket, object, listing, multipart, heal, replication,
  rebalance, scanner, config persistence, and namespace-lock implementation
  behavior.
- Consumers that need namespace locks should depend on `NamespaceLocking`
  instead of the full storage trait.

## Handoff Notes

- Keep this API-012 cleanup slice as an `api-extraction` PR that only removes
  the temporary namespace-lock method from the full storage trait, migrates
  namespace-lock consumers to `NamespaceLocking`, and deletes the
  cleanup-register entry.
- Do not move storage traits, bucket/object/list/multipart/heal operation
  logic, lock implementation code, storage runtime wiring, route behavior, or
  storage persistence logic in this PR.
- Do not add temporary compatibility code unless a matching task marker and
  cleanup-register entry are added.
