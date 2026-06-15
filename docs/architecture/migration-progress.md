# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-storage-namespace-lock-large-cleanup`
- Baseline: `origin/main` at `49c0f131205035d125271bb5b87db5b0f5bc2a6d`
- PR type for this branch: `consumer-migration`
- Runtime behavior changes: no external behavior change expected.
- Rust code changes: narrow replication pool, resync, delete replication, and
  object replication storage bounds away from full `StorageAPI` to their actual
  object I/O, object operation, list, and namespace-lock capabilities.
- CI/script changes: none.
- Docs changes: record the current `API-012` consumer cleanup slice and its
  verification state.

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
- [x] `G-006` Create migration loss-prevention checks.
  - Completed slices: add a mechanical admin route matrix guard from
    [`admin-route-action-snapshot.md`](admin-route-action-snapshot.md) and
    `rustfs/src/admin/route_registration_test.rs`; add migration rules for
    public storage-api re-export coverage, StorageAPI operation-group coverage,
    NamespaceLocking separation, and ECStore compatibility-test coverage.
  - Acceptance: architecture migration rules fail if the public storage-api
    contract re-export surface drifts, if `StorageAPI` stops covering the
    documented storage operation groups, if `NamespaceLocking` is folded back
    into the full storage facade, or if ECStore compile-time compatibility tests
    for these contracts are removed.
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

## Phase 1b Context Foundation Tasks

- [x] `CTX-001` Split AppContext files.
  - Current branch: split `rustfs/src/app/context.rs` into `interfaces`,
    `handles`, `global`, and `compat` submodules.
  - Acceptance: old `crate::app::context::*` imports continue to compile via
    re-exports; context-first and global fallback resolver bodies are moved
    without semantic changes.
  - Must preserve: AppContext construction, default adapters, global singleton
    initialization, resolver fallback order, and all consumer import paths.
  - Verification: formatting, compile checks, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-002` Add resolver compatibility tests.
  - Do: test context-first and global fallback for KMS runtime, bucket
    metadata, object store, endpoints, tier config, server config, and buffer
    config.
  - Acceptance: context wins when present and global fallback works when absent.
  - Verification: focused resolver compatibility test, formatting, compile
    checks, migration guards, diff hygiene, Rust risk scan, and full
    `make pre-commit`.
- [x] `CTX-003` Add IAM deferred recovery readiness test.
  - Do: verify IAM degraded recovery can still publish `IamReady` and
    `FullReady`.
  - Acceptance: boot/lifecycle changes cannot lose deferred readiness
    publication.
  - Verification: focused IAM recovery test, formatting, compile checks,
    migration guards, diff hygiene, Rust risk scan, and full
    `make pre-commit`.
- [x] `CTX-004` Migrate app usecase object-store consumers.
  - Do: migrate admin, bucket, multipart, and object usecases to resolve the
    object store from AppContext first.
  - Acceptance: usecase object-store lookups use AppContext when present and
    preserve the existing global object-layer fallback when absent.
  - Verification: formatting, compile check, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-005` Migrate admin object-store consumers.
  - Do: migrate admin handlers, admin services, and admin router helpers to the
    shared object-store resolver.
  - Acceptance: admin object-store lookups use AppContext when present and
    preserve the existing global object-layer fallback when absent.
  - Verification: focused resolver test, formatting, compile check, migration
    guards, diff hygiene, Rust risk scan, and full `make pre-commit`.
- [x] `CTX-006` Migrate ECFS object-store consumers.
  - Do: migrate S3 ECFS object operations to the shared object-store resolver.
  - Acceptance: ECFS object-store lookups use AppContext when present and
    preserve the existing global object-layer fallback when absent.
  - Must preserve: S3 object/bucket API behavior, object-lock/tagging/metadata
    semantics, and existing storage error paths.
  - Verification: formatting, compile check, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-007` Migrate admin ZIP object-store consumers.
  - Do: migrate admin object ZIP download object-store lookups to the shared
    object-store resolver.
  - Acceptance: admin ZIP object-store lookups use AppContext when present and
    preserve the existing global object-layer fallback when absent.
  - Must preserve: admin download authorization/preflight behavior, ZIP listing
    and streaming behavior, and existing storage error paths.
  - Verification: formatting, compile check, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-008` Migrate standalone crate object-store consumers.
  - Do: add an ECStore-owned resolver hook for AppContext-first object-store
    lookup and migrate Swift, S3 Select, scanner, notify, and observability
    object-store consumers to that resolver.
  - Acceptance: standalone crates can prefer the AppContext-owned object store
    without depending on the `rustfs` application crate and preserve the
    existing global object-layer fallback.
  - Must preserve: Swift protocol behavior, S3 Select object reads, scanner
    cache/scan behavior, notification config persistence, observability stats
    collection, and existing storage error paths.
  - Verification: formatting, compile checks, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-009` Migrate server/storage infra object-store consumers.
  - Do: migrate server readiness/module-switch and storage access, ecfs
    extension, and node RPC object-store lookups to the ECStore-owned resolver.
  - Acceptance: server/storage infra consumers prefer the AppContext-owned
    object store after context initialization and preserve the existing global
    object-layer fallback.
  - Must preserve: readiness reporting, module-switch config persistence,
    storage access authorization checks, ecfs extension validation, node RPC
    metadata/storage-info/rebalance/tier reload behavior, and existing storage
    error paths.
  - Verification: formatting, compile checks, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-010` Migrate ECStore internal object-store consumers.
  - Do: migrate ECStore internal/background object-store lookups to the
    ECStore-owned resolver.
  - Acceptance: ECStore metrics realtime, notification, tier config save,
    decommission, admin server info, bucket metadata, replication decision,
    lifecycle compensation/expiry, and data-usage cache consumers prefer the
    AppContext-owned object store after context initialization and preserve the
    existing global object-layer fallback.
  - Must preserve: metrics collection, notification rebalance stop behavior,
    tier config persistence, decommission startup, admin server info reporting,
    bucket metadata persistence, replication decisions, lifecycle queueing, data
    usage cache persistence, and existing storage error paths.
  - Verification: formatting, compile checks, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.
- [x] `CTX-011` Consolidate app usecase object-store fallback.
  - Do: migrate app admin, bucket, multipart, and object usecases away from
    direct `new_object_layer_fn` calls and through an explicit-context resolver
    helper.
  - Acceptance: usecase lookups keep their injected AppContext precedence,
    preserve `without_context()` legacy global object-layer fallback semantics,
    and avoid consulting the global AppContext when a usecase intentionally has
    no context.
  - Must preserve: admin storage/data-usage reads, bucket create/delete/list
    behavior, multipart object writes, object API reads/writes, lifecycle
    transition tests, and existing "Not init" error paths.
  - Verification: formatting, compile checks, migration guards, diff hygiene,
    Rust risk scan, and full `make pre-commit`.

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
- [x] `KMSD-002` Make Local KMS unsafe defaults explicit dev opt-in.
  - Acceptance: Local KMS now rejects missing master keys and process-temp key
    directories unless `allow_insecure_dev_defaults` is explicitly set.
  - Compatibility: server CLI/config now accepts `RUSTFS_KMS_LOCAL_MASTER_KEY`
    for production local encryption and
    `RUSTFS_KMS_ALLOW_INSECURE_DEV_DEFAULTS=true` for development-only local
    setups.
- [x] `KMSD-003` Make Vault unsafe defaults explicit dev opt-in.
  - Acceptance: Vault KV2 and Vault Transit now reject HTTP addresses,
    `dev-token`, and `skip_tls_verify` unless explicit development opt-in is set.
  - Compatibility: the KMS env loader and admin configure requests support the
    same explicit development opt-in.
- [x] `KMSD-004` Add production KMS default tests.
  - Acceptance: focused tests cover Local and Vault production rejection plus
    explicit development opt-in paths across config, env loading, admin request
    conversion, and service-manager validation.
- [x] `KMSD-005` Write KMS compatibility notes.
  - Acceptance:
    [`kms-development-defaults-inventory.md`](kms-development-defaults-inventory.md)
    now records the production-safe alternatives and explicit development opt-in
    behavior for deployments that relied on old defaults.

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
  - Completed cleanup slice: `rustfs/rustfs#3477` narrowed remaining table
    catalog backend and rebalance metadata helper consumers away from full
    `StorageAPI` where they only need object I/O, object operations, list
    operations, and namespace locking.
  - Current cleanup slice: narrow replication pool, resync leader-lock, delete
    replication, object replication, and multipart replication helpers away
    from full `StorageAPI` where they only need object I/O, object operations,
    list operations, and namespace locking.
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
- [x] `BGC-003` Add read-only status snapshot.
  - Acceptance: memory observability exposes a typed status snapshot that reports
    service state, metrics enablement, configured interval, cancellation source,
    and shutdown handle shape.
  - Must preserve: no controller framework, admin route, worker lifecycle
    change, storage write, readiness change, peer signal, or metrics emission
    behavior change.
  - Verification: focused memory observability tests, compile checks, migration
    guards, formatting, and pre-commit quality gate.
- [x] `BGC-004` Pilot one controller.
  - Acceptance: memory observability exposes a typed controller snapshot and
    reconcile plan that compare desired state with current status.
  - Must preserve: no admin route, scheduler, service registry, worker
    lifecycle mutation, storage write, readiness signal, peer signal, or metrics
    emission behavior change.
  - Verification: focused controller tests prove repeated reconcile is
    idempotent, cancellation state is preserved, and worker mutation remains
    none.
- [x] `TEST-BGC-001` Add controller harness coverage.
  - Acceptance: controller tests cover cancellation state, repeated reconcile,
    paused-time stability, and no worker mutation for the low-risk controller
    surfaces.
  - Must preserve: no worker spawn, start, stop, resize, wakeup, storage write,
    readiness signal, peer signal, or metrics emission behavior change.
  - Verification: focused memory observability and allocator reclaim controller
    tests.
- [x] `BGC-005` Add allocator reclaim controller/status surface.
  - Acceptance: allocator reclaim exposes typed desired/status/controller
    snapshots and a typed reconcile plan that reports backend, effective force,
    idle interval, runtime cancellation, shutdown handle shape, and no-op worker
    mutation.
  - Must preserve: existing allocator reclaim enablement, backend-specific force
    handling, idle-streak logic, metrics emission, runtime-token cancellation,
    and startup call shape.
  - Verification: focused allocator reclaim tests, compile checks, formatting,
    migration guards, Rust risk scan, and pre-commit quality gate.
- [x] `BGC-006` Add metrics runtime controller/status surface.
  - Acceptance: metrics runtime exposes typed desired/status/controller
    snapshots and a typed reconcile plan that reports observability enablement,
    collector task count, configured intervals, runtime cancellation, shutdown
    handle shape, and no-op worker mutation.
  - Must preserve: existing metrics collector grouping, interval parsing,
    replication bandwidth tombstone cycles, metrics emission, runtime-token
    cancellation, and startup call shape.
  - Verification: focused metrics runtime tests, compile checks, formatting,
    migration guards, Rust risk scan, and pre-commit quality gate.
- [x] `TEST-BGC-002` Preserve config reload and shutdown assumptions.
  - Acceptance: dynamic server-config reload reports no worker mutation for
    scanner/heal runtime config, bucket lifecycle/replication config files are
    not dynamic server-config reload targets, and background shutdown keeps
    scanner before AHM while preserving the scanner-implies-AHM dependency.
  - Must preserve: no scanner, heal, lifecycle, replication, audit, storage
    class, peer-signal, readiness, or worker lifecycle behavior change.
  - Verification: focused config reload and shutdown tests, compile checks,
    formatting, diff hygiene, and Rust risk scan.

## Phase 9 Startup Bootstrap Tasks

- [x] `R-009` Centralize startup IAM readiness publication bootstrap.
  - Do: move the ReadyInline/Deferred readiness publication decision behind
    `startup_iam::publish_ready_for_iam_bootstrap` and use it from binary and
    embedded startup.
  - Acceptance: inline IAM bootstrap still waits for runtime readiness and
    updates service state, deferred IAM bootstrap does not publish readiness
    from main or embedded startup, and embedded runtime readiness failures still
    trigger embedded shutdown error mapping.
  - Must preserve: startup ordering, IAM degraded recovery ownership,
    `IamReady`/`FullReady` publication semantics, and embedded shutdown
    behavior.
  - Verification: focused startup IAM tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, and pre-commit quality gate.

- [x] `R-010` Centralize startup optional service bootstrap.
  - Do: move event notifier, audit startup, and notification system startup
    behind `startup_services` helpers with caller-owned logging/error policy.
  - Acceptance: binary still initializes the event notifier before audit, logs
    audit start/failure through the same startup target, and treats notification
    init failure as fatal; embedded still treats audit and notification failures
    as non-fatal warnings.
  - Must preserve: startup order, audit non-fatal behavior, notification fatal
    boundary in binary, embedded warn-and-continue behavior, and event notifier
    initialization.
  - Verification: focused startup service tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, and pre-commit quality gate.

- [x] `R-011` Centralize startup protocol sidecar bootstrap.
  - Do: move FTP, FTPS, WebDAV, and SFTP startup orchestration behind
    `startup_protocols::init_protocol_shutdown_senders`.
  - Acceptance: feature-gated protocols still return `None` when not compiled
    or enabled, started/disabled/failure logging preserves protocol and state
    fields, and startup failures still abort binary startup with the same
    `Error::other` mapping.
  - Must preserve: protocol feature gates, env-driven enable/disable behavior,
    startup log event/state/protocol values, shutdown handle ownership, and
    existing shutdown ordering.
  - Verification: focused startup protocol tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, and pre-commit quality gate.

- [x] `R-012` Centralize startup runtime foundation bootstrap.
  - Do: move dial9 runtime status logging, runtime license status logging,
    startup logo logging, profiling setup, trusted-proxy setup, rustls provider
    setup, and outbound TLS material publication behind
    `startup_runtime::init_startup_runtime_foundation`.
  - Acceptance: BOOT-006 order is unchanged, configured TLS material load
    remains fatal with the same `Error::other(err.to_string())` mapping, TLS
    generation remains saturating, TLS metrics still initialize only when
    metrics are enabled and TLS is configured, and profiling/proxy/provider
    setup remains non-fatal.
  - Must preserve: dial9/license log event names and fields, startup logo
    logging, profiling init timing, trusted-proxy init timing, crypto provider
    already-installed handling, outbound TLS publication, generation metric
    consumer, TLS metric init condition, and fatal boundaries.
  - Verification: focused startup runtime tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-013` Centralize startup server preflight bootstrap.
  - Do: move external-prefix compatibility reporting, config snapshot
    initialization, runtime license initialization, observability guard
    initialization/storage, and startup runtime foundation bootstrap behind
    `startup_preflight::init_startup_server_preflight`.
  - Acceptance: env compatibility is applied before command parsing and reported
    after observability starts, config snapshot and license init happen before
    runtime foundation, observability init failure still emits the dedicated
    fatal stderr and sentinel, guard storage failure still returns the original
    error, and runtime foundation ordering/fatal boundaries stay unchanged.
  - Must preserve: env compat conflict/applied events, observability guard
    set/failure events, startup order, fatal stderr suppression sentinel, and
    existing command/subcommand behavior.
  - Verification: focused startup preflight tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-014` Centralize startup listen and HTTP server bootstrap.
  - Do: move server config logging, readiness creation, region/address setup,
    default credential warning, global action credentials, global port/address
    publication, capacity management, service state manager setup, and
    S3/console HTTP server startup behind `startup_server` helpers.
  - Acceptance: endpoint/storage initialization still happens after listen
    context setup and before HTTP server startup; S3 still disables console
    mode; console server still starts only when enabled with a non-empty console
    address; global action credential and address error mappings remain
    unchanged.
  - Must preserve: sanitized config/start/default credential/action credential
    log events, region validation, server address/port derivation, global
    port/address publication, capacity init timing, service `Starting` update,
    S3/console server config shape, and shutdown handle ownership.
  - Verification: focused startup server tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-015` Centralize startup storage foundation bootstrap.
  - Do: move endpoint parsing, unsupported filesystem policy enforcement, global
    endpoint publication, erasure type update, local disk initialization, local
    disk ID map prewarm, lock client initialization, and storage pool logging
    behind a `startup_storage` helper.
  - Acceptance: storage foundation still runs after listen context setup and
    before HTTP server startup; endpoint parse errors and local disk init errors
    keep the same logging and `Error::other` mappings; global endpoints and
    erasure type are published before local disk and lock client setup.
  - Must preserve: endpoint parse start/failure events, unsupported filesystem
    policy enforcement, global endpoint clone shape, erasure type update timing,
    local disk init/prewarm order, lock client setup, storage pool
    formatting/host-risk/debug logs, and endpoint pool ownership for later
    ECStore startup.
  - Verification: focused startup storage tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-016` Centralize startup storage runtime bootstrap.
  - Do: move runtime cancellation token creation, ECStore initialization,
    ECStore config initialization, server-config migration attempt, global
    config retry loop, `StorageReady` stage publication, and background
    replication startup behind the `startup_storage` boundary.
  - Acceptance: storage runtime still starts after HTTP server startup and
    before KMS startup; ECStore init failure keeps the same structured error log
    and propagated error; global config init still logs every failed attempt,
    sleeps between attempts, and becomes fatal after the 16th failed attempt;
    `StorageReady` is still marked after global config init succeeds and before
    background replication startup.
  - Must preserve: cancellation token ownership for later shutdown, endpoint
    pool clone ownership for ECStore startup, ECStore config init/migration
    order, retry count/log fields, fatal error string, readiness stage timing,
    and non-fatal background replication startup behavior.
  - Verification: focused startup storage tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-017` Centralize startup runtime service bootstrap.
  - Do: move KMS startup, optional protocol shutdown collection, buffer
    profiling, event notifier/audit startup, deadlock detector startup, bucket
    metadata migration, replication resync, IAM bootstrap, Keystone/OIDC auth
    integration startup, notification runtime setup, AHM/heal setup, server info,
    update check, allocator reclaim, metrics runtime, memory observability, and
    auto-tuner startup behind the `startup_services` boundary.
  - Acceptance: startup service initialization still runs after storage runtime
    initialization and before the server-ready log; `main.rs` keeps ownership of
    shutdown handling, server-ready publication, global init time, and scanner
    start; `startup_services` returns protocol shutdown handles, IAM bootstrap
    disposition, and scanner enablement.
  - Must preserve: KMS fatal behavior, protocol fatal/disabled behavior, audit
    non-fatal behavior, deadlock detector logging, bucket list and replication
    resync fatal behavior, bucket/IAM metadata migration non-fatal behavior, IAM
    deferred recovery semantics, Keystone parse fatal and runtime non-fatal
    behavior, OIDC non-fatal behavior, notification init fatal behavior,
    scanner-implies-heal behavior, metric-enabled guard, and shutdown token
    ownership.
  - Verification: focused startup services tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-018` Centralize startup ready, scanner, and shutdown lifecycle.
  - Do: move server-ready logging, IAM readiness publication, global init time,
    scanner start, shutdown signal wait, background shutdown ordering, protocol
    shutdown, notifier/audit/profiling shutdown, HTTP shutdown, and final stopped
    state logging behind the `startup_services` boundary.
  - Acceptance: `main.rs` still initializes listen/storage/runtime services in
    the same order, then delegates lifecycle completion; `startup_services`
    owns the shutdown handles, runtime token, readiness handle, store, and
    service runtime needed for ready/scanner/shutdown orchestration.
  - Must preserve: server-ready log fields, inline/deferred IAM readiness
    behavior, global init time timing, scanner start timing, shutdown signal log,
    runtime token cancellation before service-specific shutdown, scanner before
    AHM shutdown order, protocol shutdown order, notifier/audit/profiling
    shutdown order, HTTP shutdown order, stopped service state, and final stopped
    logs.
  - Verification: focused startup services tests, binary/lib compile checks,
    formatting, migration guards, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-019` Centralize startup command and bootstrap entrypoint.
  - Do: move Tokio runtime result handling, command parsing/dispatch, server
    preflight error mapping, startup run orchestration, and pre-observability
    fatal stderr formatting behind `startup_entrypoint::run_process`.
  - Acceptance: `main.rs` only owns the global allocator declarations and calls
    the startup entrypoint; `startup_entrypoint` preserves the existing
    command, preflight, listen, storage, runtime-service, ready, and shutdown
    order.
  - Must preserve: Tokio runtime build fatal `expect`, command parse fatal
    stderr context and exit code, info/TLS subcommand behavior, observability
    fatal sentinel suppression, server runtime failure log fields, startup stage
    ordering, readiness publication, and shutdown ownership.
  - Verification: focused startup entrypoint and observability guardrail tests,
    binary/lib compile checks, formatting, migration guards, Rust risk scan,
    branch freshness check, and pre-commit quality gate.

## Next PRs

1. `pure-move`/`consumer-migration`: continue larger cleanup slices with the
   loss-prevention guards active for public re-exports and storage trait
   coverage.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | passed | Replication consumers now depend on a local replication storage capability boundary plus object-I/O-only helper bounds, without changing replication method bodies. |
| Migration preservation | passed | Resync leader locks, per-object replication locks, delete replication, object reader flow, multipart upload flow, MRF recovery, and replication scheduling semantics are unchanged. |
| Testing/verification | passed | Focused replication tests, storage API compatibility test, compile checks, migration/layer guards, formatting, diff hygiene, Rust risk scan, and full `make pre-commit` passed. |

## Verification Notes

Passed on `49c0f131205035d125271bb5b87db5b0f5bc2a6d`:

- `cargo check -p rustfs-ecstore`: passed.
- `cargo test -p rustfs-ecstore bucket::replication --no-fail-fast`: passed.
- `cargo test -p rustfs-ecstore --test storage_api_compat_test --no-fail-fast`:
  passed.
- `cargo check -p rustfs -p rustfs-ecstore`: passed.
- `./scripts/check_architecture_migration_rules.sh`: passed.
- `./scripts/check_layer_dependencies.sh`: passed.
- `cargo fmt --all --check`: passed.
- `git diff --check`: passed.
- Rust risk scan: no new production `unwrap`/`expect`, lossy casts, string
  errors, public boxed errors, production `println`/`eprintln`, or relaxed
  atomics in added Rust lines.
- `make pre-commit`: passed.

Notes:

- This slice keeps the existing replication method bodies unchanged while
  narrowing the generic storage capabilities they require.
- Replication storage still depends on object I/O, object metadata/delete
  operations, bucket walking, and namespace locking; multipart replication
  helpers only need object reader access to stream source ranges.
- The slice does not remove the full storage facade or move traits across crate
  boundaries.

## Handoff Notes

- API-012 replication storage cleanup is locally verified and current with
  `origin/main`.
- Remaining namespace-lock cleanup can continue by migrating other consumers
  that no longer need the full storage facade.
