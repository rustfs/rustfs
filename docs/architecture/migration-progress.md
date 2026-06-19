# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-scanner-replication-admission-snapshots`
- Baseline: `origin/main` after `rustfs/rustfs#3605`
  (`00ca3b7c1c4ffbe98c0dd8530cb26b63e94c586a`).
- PR type for this branch: `consumer-migration`
- Runtime behavior changes: none.
- Rust code changes: extend read-only workload admission snapshots from heal
  repair counters to replication runtime worker and queue counters.
- CI/script changes: extend migration guard coverage for RustFS workload
  admission provider implementations.
- Docs changes: add RustFS replication provider notes to
  [`workload-admission-contracts.md`](workload-admission-contracts.md) and
  record the API-058/R-018 provider slice.

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
    public storage-api re-export coverage, ECStore compatibility-test coverage,
    and a production-source guard against reintroducing the removed
    `StorageAPI` aggregate facade identifier; add a source guard that rejects
    direct `rustfs_ecstore` imports outside compatibility boundary modules; add
    a guard that rejects production compatibility boundaries hiding unused
    ECStore re-exports.
  - Acceptance: architecture migration rules fail if the public storage-api
    contract re-export surface drifts or if ECStore compile-time compatibility
    tests for the remaining storage-admin and namespace-lock contracts are
    removed.
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
- [x] `G-011` Inventory scheduler baseline.
  - Acceptance:
    [`scheduler-baseline.md`](scheduler-baseline.md) records current owners for
    request admission, reusable scheduler/backpressure facades, workers, scanner
    budget, heal admission, and the Tokio runtime builder.
  - Must preserve: no Rust source changes, no scheduler/controller contract
    changes, and no runtime behavior changes.
- [x] `G-012` Inventory placement and repair invariants.
  - Acceptance:
    [`placement-repair-invariants.md`](placement-repair-invariants.md) records
    object-to-set hashing, pool/set/disk assignment boundaries, set-aware
    readiness and lock quorum, scanner budget, and heal admission preservation
    gates.
  - Must preserve: no placement, repair, scanner, heal, readiness, lock, or
    storage metadata behavior changes.
- [x] `G-013` Inventory profiling and NUMA capabilities.
  - Acceptance:
    [`profiling-numa-capability-inventory.md`](profiling-numa-capability-inventory.md)
    records current CPU/memory profiling, cgroup memory sampling, allocator
    backend, eBPF, and NUMA capability support plus no-op fallback invariants.
  - Must preserve: no startup, profiling, allocator, runtime, or platform-gate
    behavior changes.

## Issue #660 Capability Contract Tasks

- [x] `PR-08/API-013` Add observability snapshot contract.
  - Completed slice: add `CapabilityState`, `CapabilityStatus`,
    `CapabilitySnapshotError`, `ObservabilitySnapshot`,
    `UserspaceProfilingCapability`, `MemorySamplingState`,
    `PlatformSupport`, and `ObservabilitySnapshotProvider` to
    `rustfs-storage-api`.
  - Acceptance: runtime telemetry, userspace profiling, memory sampling, and
    platform support states are representable without runtime, ECStore, admin,
    profiling, exporter, sidecar, eBPF, or OTEL implementation dependencies.
  - Must preserve: no profiling, startup, admin route, exporter, sidecar, eBPF,
    OTEL, or runtime behavior changes.
  - Verification: storage-api contract tests for unknown, unsupported,
    disabled, and supported capability states; focused storage-api check;
    migration guard; formatting; diff hygiene; and three-expert review.

- [x] `PR-09/API-014` Add topology capability contract.
  - Completed slice: add `TopologySnapshot`, `TopologyCapabilities`,
    `TopologyPool`, `TopologySet`, `TopologyDisk`, `TopologyLabels`,
    `DiskCapabilities`, and `TopologySnapshotProvider` to
    `rustfs-storage-api`.
  - Acceptance: pool, set, and disk identity fields plus optional zone, rack,
    node, media, NUMA, and additional labels are representable without
    `rustfs-ecstore`.
  - Must preserve: no ECStore endpoint/set implementation, placement,
    membership, NUMA pinning, or runtime behavior changes.
  - Verification: storage-api contract tests for missing and additional labels
    plus supported, unsupported, unknown, and disabled capability states;
    focused storage-api check; migration guard; formatting; diff hygiene; and
    three-expert review.

- [x] `PR-05/TEST-SCH-001` Add scheduler preservation tests.
  - Completed slice: pin worker over-release clamping, reusable scheduler
    default thresholds and priority boundaries, backpressure pipe metadata
    reads, and get-object queue snapshot saturation/zero-total semantics.
  - Acceptance: current reusable scheduling and admission-facing behavior is
    covered before later read-only snapshot extraction.
  - Must preserve: scheduler algorithm, queue capacity, threshold defaults,
    Tokio runtime settings, request admission, scanner admission, heal
    admission, replication admission, and background task admission behavior.
  - Verification: focused concurrency tests, focused concurrency check,
    migration guard, formatting, diff hygiene, and three-expert review.

- [x] `PR-07/R-015` Add runtime workload class contract.
  - Completed slice: add `WorkloadClass`, `AdmissionState`,
    `WorkloadAdmissionSnapshot`, `WorkloadAdmissionRegistrySnapshot`, and
    `WorkloadAdmissionSnapshotProvider` to `rustfs-concurrency`.
  - Acceptance: foreground read, foreground write, metadata, scanner, repair,
    and replication workload classes are representable through read-only
    admission registry snapshots without ECStore dependency.
  - Must preserve: no SchedulerManager decision logic, Tokio worker defaults,
    scanner/heal admission behavior, replication admission behavior, cluster
    scheduling, placement, membership, or business call-site migration.
  - Verification: workload contract unit tests, focused concurrency check,
    migration guard, formatting, diff hygiene, and three-expert review.

- [x] `API-055/SCH-001` Expose set-local scheduler admission snapshot.
  - Completed slice: implement `WorkloadAdmissionSnapshotProvider` for the
    RustFS storage `ConcurrencyManager` and expose foreground-read disk-read
    permit usage through a local read-only workload registry snapshot.
  - Acceptance: local foreground read admission reports active permit usage,
    configured limit, and open/saturated/disabled state without ECStore,
    admin-route, cluster, or scheduler mutation dependencies.
  - Must preserve: disk-read semaphore acquisition, priority assignment,
    buffer sizing, storage media detection, request guards, and queue behavior.
  - Verification: storage concurrency tests, focused RustFS library check,
    migration guard, formatting, diff hygiene, and three-expert review.

- [x] `API-056/R-016` Wire runtime capability snapshot providers.
  - Completed slice: implement `ObservabilitySnapshotProvider` for RustFS
    runtime capability state and `TopologySnapshotProvider` for
    `EndpointServerPools` topology snapshots.
  - Acceptance: observability and endpoint topology snapshots are available
    through the storage-api contracts without admin routes, sidecars, ECStore
    placement mutation, profiling startup changes, or endpoint behavior changes.
  - Must preserve: profiling opt-in behavior, memory and cgroup sampling
    behavior, endpoint pool/set/disk assignment, placement, readiness, locks,
    and local path privacy.
  - Verification: focused runtime capability tests, focused RustFS library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

- [x] `API-057/R-017` Expose heal repair admission snapshot.
  - Completed slice: implement a RustFS workload admission snapshot provider
    that maps existing heal active-task and queue-length counters to the
    `Repair` workload class.
  - Acceptance: repair admission state is observable through the
    `rustfs-concurrency` workload snapshot contract without changing heal
    queueing, scheduling, retry, priority merge/drop, or repair behavior.
  - Must preserve: heal request admission, queue capacity, scheduler wakeups,
    task retry handling, active-task accounting, and repair execution.
  - Verification: focused workload admission tests, focused RustFS library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

- [x] `API-058/R-018` Expose replication admission snapshot.
  - Completed slice: extend the RustFS workload admission provider to map
    existing replication worker and site queue counters to the `Replication`
    workload class.
  - Acceptance: replication admission pressure is observable through the
    `rustfs-concurrency` workload snapshot contract without changing
    replication queueing, channel capacity, worker resize, MRF, target dispatch,
    or resync behavior.
  - Must preserve: replication admission, queue channel capacity, worker resize
    policy, MRF handling, target dispatch, resync behavior, and queue stats
    accounting.
  - Verification: focused workload admission tests, focused RustFS library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

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
- [x] `S-014` Remove legacy KMS admin action fallbacks.
  - Acceptance: KMS create, describe, and list-key handlers authorize only the
    dedicated `kms:*` actions and no longer retain legacy admin grant fallbacks.
  - Must preserve: legacy KMS endpoint URLs, query aliases, request bodies, and
    response contracts remain unchanged.
  - Verification: focused KMS auth and route-policy tests, migration guards,
    formatting, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed before push.
- [x] `S-015` Remove legacy KMS admin policy action taxonomy.
  - Acceptance: `admin:KMSCreateKey` and `admin:KMSKeyStatus` no longer parse as
    valid policy actions; KMS key handlers keep using dedicated `kms:*` actions.
  - Must preserve: legacy KMS endpoint URLs, query aliases, request bodies, and
    response contracts remain unchanged.
  - Verification: focused policy and KMS auth tests, route-policy tests,
    migration guards, formatting, diff hygiene, risk scan, full pre-commit, and
    required three-expert review passed before push.
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
  - Completed follow-up slice: remove the remaining ECStore-internal bucket DTO
    aliases from `store_api` and guard against restoring that compatibility
    path.
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
  - Final cleanup slice: remove the old `StorageAPI` facade after all real
    consumers moved to concrete operation groups.
  - Loss-prevention cleanup slice: rename the remaining ECStore contract
    compatibility test away from the old storage-api facade name and guard
    production ECStore/RustFS source against reintroducing the removed
    aggregate facade identifier.
  - Acceptance: storage operation traits remain available directly while admin
    inventory surfaces live only on `StorageAdminApi`.

- [x] `API-009` Narrow metadata helper storage bounds.
  - Completed slice: `rustfs/rustfs#3343` narrowed server config, tier config,
    rebalance metadata, and startup metadata migration helper bounds away from
    full `StorageAPI` when the helper only needs `ObjectIO`,
    `ObjectOperations`, `BucketOperations`, `ListOperations`, or
    `StorageAdminApi`.
  - Cleanup slice: remove stale full `StorageAPI` dependencies from config
    persistence test support after the server-config persistence helpers moved
    to their actual object I/O and storage-admin bounds.
  - Completed cleanup slice: `rustfs/rustfs#3489` removed the stale full
    facade dependency from config persistence test support.
  - Acceptance: metadata helper contracts express the actual operation group
    they need, while callers and persistence behavior remain unchanged.

- [x] `API-010` Narrow replication resync metadata bounds.
  - Completed slice: `rustfs/rustfs#3345` narrowed replication resync status
    load/save/mark/persist helper bounds away from full `StorageAPI` when the
    helper only needs `ObjectIO`.
  - Acceptance: resync metadata helpers express object-I/O-only persistence
    requirements, while replication execution, delete replication, multipart
    replication, object lookups, and scheduling behavior remain on the concrete
    operation groups they need.

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
  - Risk defense: do not move traits to `rustfs-storage-api`, do not alter
    helper bodies, and do not narrow scanner paths that need bucket operations,
    disk inventory, or full storage orchestration.
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
  - Completed follow-up slice: `rustfs/rustfs#3485` narrowed replication pool,
    resync leader-lock, delete replication, object replication, and multipart
    replication helpers away from full `StorageAPI` where they only need object
    I/O, object operations, list operations, and namespace locking.
  - Final cleanup slice: remove the unused old `StorageAPI` facade, its
    implementation blocks, public re-export, and stale guard coverage.
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
    not leave stale full-facade compatibility coverage after consumers move to
    concrete operation groups.
  - Verification: focused compile/tests, migration guards, Rust risk scan, and
    required quality/architecture, migration-preservation, and
    testing/verification review passed.

- [x] `API-013` Move multipart list/result DTO contracts.
  - Completed slice: move `MultipartUploadResult`, `PartInfo`,
    `MultipartInfo`, `ListMultipartsInfo`, and `ListPartsInfo` from ECStore
    `store_api` into `rustfs-storage-api`; update ECStore traits and RustFS S3
    multipart response builders to import these shared contracts directly.
  - Acceptance: `rustfs-storage-api` exports the multipart DTO contracts,
    in-repo consumers no longer use the old `rustfs_ecstore::store_api` path
    for these DTOs, and migration guards reject restoring the old ECStore-owned
    definitions or re-exports.
  - Must preserve: multipart upload creation, part listing, multipart upload
    listing, part metadata, checksum fields, S3 response mapping, and storage
    operation trait behavior.
  - Risk defense: keep `CompletePart`, `ObjectInfo`, `ObjectOptions`, readers,
    filemeta conversions, replication state, encryption, compression, and range
    semantics in ECStore for this slice.
  - Verification: focused storage-api/ECStore/RustFS compile checks, multipart
    response tests, migration/layer guards, formatting, diff hygiene, Rust risk
    scan, and required three-expert review passed.

- [x] `API-014` Move bucket operation contract.
  - Completed slice: move `BucketOperations` from ECStore `store_api` into
    `rustfs-storage-api`, keep ECStore/Sets/SetDisks implementations in
    ECStore, and migrate in-repo consumers to import the shared contract path.
  - Acceptance: `rustfs-storage-api` exports the bucket operation contract,
    in-repo consumers no longer use the old `rustfs_ecstore::store_api` path
    for `BucketOperations`, and migration guards reject restoring the old
    ECStore-owned definition or re-export.
  - Must preserve: bucket create/delete/list/info behavior, object store
    initialization, bucket metadata migration, Swift/admin/storage consumers,
    and all storage hot paths.
  - Risk defense: only the trait contract crosses into `rustfs-storage-api`;
    ECStore errors, object contracts, list contracts, readers, lock handling,
    and implementation bodies stay in ECStore.
  - Verification: focused storage-api/ECStore/RustFS/downstream compile checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, and
    required three-expert review passed.

- [x] `API-015` Move object option helper contracts.
  - Completed slice: move `CompletePart`, `HTTPPreconditions`, and
    `ObjectLockRetentionOptions` from ECStore `store_api` into
    `rustfs-storage-api`; keep `ObjectOptions`, object/list DTOs, readers,
    filemeta conversions, and storage implementations in ECStore.
  - Acceptance: `rustfs-storage-api` exports the moved helper contracts,
    in-repo consumers no longer use the old `rustfs_ecstore::store_api` path
    for these helpers, and migration guards reject restoring the old ECStore
    definitions or public re-exports.
  - Must preserve: multipart completion mapping, HTTP precondition semantics,
    object-lock retention fields, object lookup/drop-precondition behavior,
    storage hot paths, and ECStore-owned implementation-heavy object contracts.
  - Risk defense: only pure helper DTOs cross into `rustfs-storage-api`;
    ECStore keeps `ObjectOptions`, `ObjectInfo`, list contracts, readers,
    lifecycle/replication/rio/filemeta coupling, errors, and implementation
    bodies.
  - Verification: focused storage-api/ECStore/RustFS/downstream compile checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, and
    required three-expert review passed.

- [x] `API-016` Move HTTP range helper contracts.
  - Completed slice: move `HTTPRangeSpec` and `HTTPRangeError` from ECStore
    `store_api/readers.rs` into `rustfs-storage-api`; keep `ObjectInfo` part
    adaptation in ECStore and migrate RustFS, ECStore, Swift, scanner, and
    S3-select consumers to import the shared range contract directly.
  - Acceptance: `rustfs-storage-api` exports the range helper contracts,
    in-repo consumers no longer use the old `rustfs_ecstore::store_api` path
    for `HTTPRangeSpec`, and migration guards reject restoring old ECStore
    definitions or public re-exports.
  - Must preserve: S3 range semantics, suffix ranges, multipart part-range
    boundaries, SSE/rio/compressed range planning, Swift/S3-select reads, and
    ECStore-owned object-info/filemeta adaptation.
  - Risk defense: only pure range contract behavior crosses into
    `rustfs-storage-api`; ECStore keeps readers, `ObjectInfo`, part plaintext
    size selection, encryption/compression planning, lifecycle/replication/rio
    coupling, and storage implementation bodies.
  - Verification: focused storage-api/ECStore/RustFS/downstream compile checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, and
    required three-expert review passed.

- [x] `API-017` Move object list helper contracts.
  - Completed slice: move `VersionMarker` and `WalkVersionsSortOrder` from
    ECStore `store_api/types.rs` into `rustfs-storage-api`; keep
    `versions_after_marker`, `WalkOptions`, `ObjectInfo`, list result DTOs,
    readers, and storage list/walk implementations in ECStore.
  - Acceptance: `rustfs-storage-api` exports the list helper contracts,
    in-repo production code no longer imports them from
    `rustfs_ecstore::store_api`, and migration guards reject restoring old
    ECStore definitions or public re-exports.
  - Must preserve: list-object-versions marker parsing, null version markers,
    version marker application only to the first matching entry, walk sort
    default, and ECStore-owned filemeta/list implementation behavior.
  - Risk defense: only pure marker/sort contracts cross into
    `rustfs-storage-api`; ECStore keeps filemeta conversion, list result DTOs,
    walk options with filemeta filters, readers, lifecycle/replication coupling,
    and storage implementation bodies.
  - Verification: focused storage-api/ECStore/RustFS/downstream compile checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, and
    required three-expert review passed.

- [x] `API-018` Move object precondition helper contracts.
  - Completed slice: add `ObjectPreconditionState`,
    `ObjectPreconditionPart`, and `ObjectPreconditionError` to
    `rustfs-storage-api`; make ECStore `ObjectOptions::precondition_check`
    adapt `ObjectInfo` into the shared pure contract and map the contract
    result back to the existing ECStore errors.
  - Acceptance: `rustfs-storage-api` exports the precondition helper contracts,
    ECStore keeps `ObjectOptions` and `ObjectInfo`, and migration guards reject
    dropping the public precondition contract re-export.
  - Must preserve: requested-part validation, empty condition handling,
    `If-None-Match`/`If-Modified-Since` `NotModified` behavior,
    `If-Match`/`If-Unmodified-Since` `PreconditionFailed` behavior, wildcard
    ETag matching, and ECStore error mapping.
  - Risk defense: only pure precondition decision state and result contracts
    cross into `rustfs-storage-api`; ECStore keeps object metadata adaptation,
    storage error types, `ObjectOptions`, `ObjectInfo`, readers,
    lifecycle/replication coupling, and storage implementation bodies.
  - Verification: focused storage-api tests, ECStore/RustFS/downstream compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    and required three-expert review passed.

- [x] `API-019` Move object list response contracts.
  - Completed slice: move `ListObjectsInfo`, `ListObjectsV2Info`,
    `ListObjectVersionsInfo`, and `ObjectInfoOrErr` from ECStore
    `store_api/types.rs` into `rustfs-storage-api` as generic public
    contracts, then keep ECStore's old public names as type aliases bound to
    `ObjectInfo` and `Error`.
  - Acceptance: `rustfs-storage-api` exports the generic list response
    contracts, ECStore no longer defines local response structs for these
    contracts, existing ECStore consumers keep their old import path, and
    migration guards reject dropping the public storage-api re-export or
    reintroducing local ECStore definitions.
  - Must preserve: list v1/v2 truncation and marker fields, list-object-version
    marker fields, object/prefix vectors, walk item/error channel shape, and
    ECStore list/walk runtime behavior.
  - Risk defense: only generic response containers cross into
    `rustfs-storage-api`; ECStore keeps `ObjectInfo`, `ObjectOptions`,
    `WalkOptions`, filemeta filters, object metadata adaptation, storage errors,
    readers, lifecycle/replication coupling, and list/walk implementation
    bodies.
  - Verification: focused storage-api tests, ECStore/RustFS/downstream compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.

- [x] `API-020` Move walk options contract.
  - Completed slice: move `WalkOptions` from ECStore `store_api/types.rs` into
    `rustfs-storage-api` as a generic public contract over the filter type,
    then keep ECStore's old public `WalkOptions` name as a type alias bound to
    the existing `fn(&FileInfo) -> bool` filter shape.
  - Acceptance: `rustfs-storage-api` exports `WalkOptions`, ECStore no longer
    defines a local `WalkOptions` struct, existing ECStore consumers keep their
    old import path, and migration guards reject dropping the public
    storage-api re-export or reintroducing a local ECStore definition.
  - Must preserve: walk filter optionality, marker, latest-only flag, ask-disks
    string, version sort default, limit semantics, include-free-versions flag,
    and ECStore list/walk runtime behavior.
  - Risk defense: only the generic options container crosses into
    `rustfs-storage-api`; ECStore keeps the concrete `FileInfo` filter binding,
    list/walk implementations, metadata conversion, readers, storage errors,
    lifecycle/replication coupling, and operation traits.
  - Verification: focused storage-api tests, ECStore/RustFS/downstream compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.

- [x] `API-021` Move list operations contract.
  - Completed slice: move `ListOperations` from ECStore `store_api/traits.rs`
    into `rustfs-storage-api` as a generic public operation contract over list
    response, walk option, cancellation, sender, and error associated types;
    keep ECStore's old public `ListOperations` name as a fixed associated-type
    compatibility subtrait.
  - Acceptance: `rustfs-storage-api` exports `ListOperations`, ECStore no
    longer defines local list operation method signatures, existing ECStore
    generic bounds keep the old import path, and migration guards reject
    dropping the public storage-api re-export or reintroducing local ECStore
    list method definitions.
  - Must preserve: list v2 pagination, list-object-versions pagination, walk
    channel shape, cancellation token usage, ECStore public compatibility
    bounds, and all ECStore list/walk runtime behavior.
  - Risk defense: only the trait contract crosses into `rustfs-storage-api`;
    ECStore keeps the concrete associated type bindings, response aliases,
    walk option alias, object metadata conversion, storage errors, lifecycle
    and replication coupling, and implementation bodies.
  - Verification: focused storage-api tests, ECStore/RustFS/downstream compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.

- [x] `API-022` Move object and multipart operation contracts.
  - Completed slice: move `ObjectIO`, `ObjectOperations`, and
    `MultipartOperations` from ECStore `store_api/traits.rs` into
    `rustfs-storage-api` as generic public operation contracts over ECStore
    reader, option, metadata, multipart DTO, file-info, delete, header, range,
    and error associated types; keep ECStore's old public trait names as fixed
    associated-type compatibility subtraits.
  - Acceptance: `rustfs-storage-api` exports the object and multipart
    operation contracts, ECStore no longer defines local object/multipart method
    signatures, existing ECStore generic bounds keep the old import path, and
    migration guards reject dropping the public storage-api re-export or
    reintroducing local ECStore object/multipart method definitions.
  - Must preserve: object reader/writer behavior, object metadata/tag/delete
    behavior, multipart create/copy/part/list/complete/abort behavior, ECStore
    public compatibility bounds, and all ECStore object/multipart runtime
    behavior.
  - Risk defense: only the trait contracts cross into `rustfs-storage-api`;
    ECStore keeps the concrete associated type bindings, readers,
    `ObjectInfo`, `ObjectOptions`, `PutObjReader`, filemeta adaptation, storage
    errors, lifecycle/replication/rio/compression/encryption coupling, and
    implementation bodies.
  - Verification: focused storage-api tests, ECStore/RustFS/downstream compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.
- [x] `API-023` Move heal and namespace-lock operation contracts.
  - Completed slice: move `HealOperations` and `NamespaceLocking` from ECStore
    `store_api/traits.rs` into `rustfs-storage-api` as generic public
    operation contracts over ECStore heal result/options, namespace-lock
    wrapper, and error associated types; keep ECStore's old public trait names
    as fixed associated-type compatibility subtraits.
  - Acceptance: `rustfs-storage-api` exports the heal and namespace-lock
    operation contracts, ECStore no longer defines local heal/namespace-lock
    method signatures, focused consumers use the shared trait for method
    resolution, and migration guards reject dropping the public storage-api
    re-export or reintroducing local ECStore method definitions.
  - Must preserve: heal format/bucket/object behavior, abandoned-part checks,
    pool/set lookup behavior, namespace-lock acquisition behavior, ECStore
    public compatibility bounds, and all runtime lock/heal implementation
    bodies.
  - Risk defense: only the trait contracts cross into `rustfs-storage-api`;
    ECStore keeps concrete associated type bindings, `HealOpts`,
    `HealResultItem`, `NamespaceLockWrapper`, lock implementation, peer heal
    behavior, set/pool dispatch, and storage error mapping.
  - Verification: focused storage-api/ECStore/RustFS/heal/scanner compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.

- [x] `API-024` Clean shared list operation consumer bounds.
  - Completed slice: migrate RustFS S3/bucket usecase list response builders from
    ECStore `ListObjectVersionsInfo`/`ListObjectsV2Info` aliases to
    `rustfs-storage-api` generic list response contracts bound to ECStore
    `ObjectInfo`; migrate IAM walk channel typing from ECStore
    `ObjectInfoOrErr` alias to the shared generic item contract.
  - Acceptance: outer RustFS/IAM consumers use storage-api list response
    contracts directly, ECStore keeps concrete aliases for internal
    implementation and compatibility, and migration guards reject restoring the
    old outer-consumer imports.
  - Must preserve: S3 list v2/version output mapping, IAM config walk channel
    item/error handling, ECStore concrete object metadata shape, walk options
    inference, and storage error conversion behavior.
  - Risk defense: this slice moves only low-coupling generic response/channel
    typing; ECStore still owns `ObjectInfo`, `ObjectOptions`, readers,
    filemeta-bound walk filter type, delete DTOs, and list/walk implementation
    bodies.
  - Verification: focused RustFS/IAM compile and tests, migration/layer guards,
    formatting, diff hygiene, Rust risk scan, full pre-commit, and required
    three-expert review passed.

- [x] `API-025` Clean external operation consumer bounds.
  - Completed slice: migrate scanner data-usage cache storage bounds, RustFS
    object-usecase namespace-lock helper bounds, and table catalog object
    backend storage bounds from ECStore compatibility operation traits to
    `rustfs-storage-api` operation traits with explicit ECStore concrete
    associated-type bindings.
  - Acceptance: outer RustFS/scanner consumers no longer import ECStore
    operation traits, ECStore keeps compatibility traits for internal
    implementation and downstream compatibility, and migration guards reject
    restoring old outer-consumer operation trait imports.
  - Must preserve: scanner cache load/save behavior, scanner backend timeout
    and retry behavior, object self-copy namespace-lock quorum/error mapping,
    table catalog object read/write/list/lock behavior, ECStore object metadata
    shape, reader shape, walk filter shape, and storage error conversion.
  - Risk defense: this slice changes only generic bounds/import ownership;
    ECStore still owns concrete object DTOs, readers, delete DTOs, lock wrappers,
    walk filters, and implementation bodies.
  - Verification: focused RustFS/scanner compile and tests, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, full pre-commit, and
    required three-expert review passed.

- [x] `API-026` Clean external DTO consumer boundaries.
  - Current branch: `overtrue/arch-storage-dto-consumer-boundaries`.
  - Completed slice: introduce crate-local semantic aliases for ECStore-owned
    object metadata/options/readers/delete DTOs in scanner, heal, notify, Swift,
    S3 Select, and RustFS storage/app consumers; update production and affected
    test call sites to use those local aliases instead of raw
    `rustfs_ecstore::store_api` DTO imports.
  - Acceptance: non-ECStore direct `rustfs_ecstore::store_api` references are
    limited to boundary alias definitions, ECStore remains the owner of
    `ObjectInfo`, `ObjectOptions`, object readers, delete DTOs, walk filters,
    lock wrappers, and implementation behavior, and external consumers express
    their local semantic dependency through crate-owned names.
  - Must preserve: object metadata shape, object option defaults, reader/writer
    behavior, delete replication DTO handling, scanner cache semantics, heal
    storage metadata semantics, Swift and S3 Select object reads, notification
    event payloads, S3 response DTO mapping, and storage/app test behavior.
  - Risk defense: this slice uses type aliases and import-boundary cleanup only;
    it does not move DTO definitions, alter serialization, change object-store
    implementations, or adjust runtime control flow.
  - Verification: focused compile/tests, migration/layer guards, formatting,
    diff hygiene, direct import scan, Rust risk scan, full pre-commit, and
    required three-expert review passed.

- [x] `API-027` Clean remaining external storage DTO imports.
  - Current branch: `overtrue/arch-storage-compat-contract-cleanup`.
  - Completed slice: move table catalog, IAM object-store, admin zip-download,
    capacity dirty-scope tests, heal integration tests, scanner, Swift, S3
    Select, and notify event payloads from raw ECStore `store_api` DTO imports
    to crate-local compatibility aliases/modules.
  - Acceptance: non-ECStore direct `rustfs_ecstore::store_api` references are
    limited to explicit boundary alias points in RustFS storage plus scanner,
    heal, IAM, notify, Swift, and S3 Select compatibility modules; table
    catalog, affected tests, and protocol/scanner/notification consumers
    consume those boundary names instead of raw ECStore DTO paths.
  - Must preserve: table catalog storage trait bindings, IAM metadata/lazy
    rewrite behavior, object zip preflight/read semantics, capacity dirty-disk
    assertions, heal integration object read/write behavior, scanner cache
    load/save semantics, Swift object read/write/copy/delete behavior, S3
    Select object-store reads, notify event payload shape, and ECStore-owned DTO
    concrete shapes.
  - Risk defense: this slice changes import ownership and type aliases only; it
    does not move DTO definitions, alter serialization, change object-store
    implementation bodies, or adjust runtime control flow.
  - Verification: focused compile/tests, migration/layer guards, formatting,
    diff hygiene, direct import scan, Rust risk scan, full pre-commit, and
    required three-expert review passed.

- [x] `API-028` Clean Swift ECStore runtime boundary imports.
  - Current branch: `overtrue/arch-swift-ecstore-boundaries`.
  - Completed slice: move Swift account, container, object, and versioning
    access to ECStore object-store resolver and bucket metadata get/set calls
    behind the Swift-local `storage_compat` module.
  - Acceptance: direct Swift module references to `rustfs_ecstore` for object
    store resolution, bucket metadata reads, bucket metadata writes, and object
    DTO aliases are limited to `swift::storage_compat`; Swift business modules
    consume Swift-owned compatibility names.
  - Must preserve: Swift account metadata tags, container metadata tags,
    versioning location tags, ACL tag storage, object CRUD/copy/range behavior,
    storage-not-initialized error mapping, and bucket metadata load/save error
    mapping.
  - Risk defense: this slice changes import ownership and thin wrapper
    boundaries only; it does not move ECStore definitions, alter metadata
    serialization, change Swift bucket naming, or adjust runtime control flow.
  - Verification: focused Swift compile/tests, migration/layer guards,
    formatting, diff hygiene, direct Swift import scan, Rust risk scan, full
    pre-commit, and required three-expert review passed.

- [x] `API-029` Clean scanner and heal ECStore runtime boundaries.
  - Current branch: `overtrue/arch-scanner-heal-runtime-boundaries`.
  - Completed slice: move scanner and heal direct ECStore runtime, disk,
    metadata, lifecycle, replication, config, and error imports behind their
    crate-local compatibility modules.
  - Acceptance: direct `rustfs_ecstore` references in `crates/scanner/src` and
    `crates/heal/src` are limited to scanner/heal compatibility boundary
    modules; scanner/heal business modules consume local compatibility names.
  - Must preserve: scanner cache load/save behavior, lifecycle and replication
    scan behavior, disk bucket scan inventory lookup, heal object/bucket/format
    behavior, resume state storage, heal channel test contracts, and existing
    ECStore-owned concrete types.
  - Risk defense: this slice changes import ownership and thin compatibility
    boundaries only; it does not alter scanner scheduling, heal scheduling,
    object I/O logic, disk operations, metadata serialization, or error
    mapping.
  - Verification: focused scanner/heal compile/tests, direct import scans,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, full
    pre-commit, and required three-expert review passed.

- [x] `API-030` Clean app, storage, and admin ECStore runtime boundaries.
  - Current branch: `overtrue/arch-app-storage-admin-runtime-boundaries`.
  - Completed slice: add crate-local app, storage, and admin compatibility
    boundary modules for ECStore-owned runtime contracts, then migrate direct
    `rustfs_ecstore` imports in `rustfs/src/app`, `rustfs/src/storage`, and
    `rustfs/src/admin` through those boundary modules.
  - Acceptance: direct `rustfs_ecstore` references in app/storage/admin source
    are limited to the local compatibility boundary modules; app, storage, and
    admin business/test modules consume local compatibility names.
  - Must preserve: app object/bucket/multipart/admin usecase behavior, storage
    ECFS/access/SSE/RPC behavior, admin route/handler/service behavior,
    metadata serialization, encryption handling, authorization, and existing
    ECStore-owned concrete type ownership.
  - Risk defense: this slice changes import ownership only; it does not move
    ECStore definitions, alter runtime control flow, adjust route registration,
    change storage I/O, mutate metadata formats, or alter admin authorization.
  - Verification: direct app/storage/admin import scan, RustFS test compile
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    full pre-commit, and required three-expert review passed.

- [x] `API-031` Clean runtime, observability, S3 Select, notify, and IAM
  ECStore runtime boundaries.
  - Current branch: `overtrue/arch-runtime-observability-select-boundaries`.
  - Completed slice: add RustFS root, obs, and IAM compatibility boundary
    modules; extend notify and S3 Select compatibility modules; migrate direct
    `rustfs_ecstore` imports in RustFS startup/server/runtime/table-catalog
    code plus obs, notify, S3 Select, and IAM through those local boundaries.
  - Acceptance: direct `rustfs_ecstore` references in those source areas are
    limited to local compatibility boundary modules; runtime and crate business
    modules consume local compatibility names.
  - Must preserve: startup ordering, readiness/RPC behavior, capacity metrics,
    table catalog object I/O behavior, notification config persistence, S3
    Select object-store reads, IAM storage/error mapping, and observability
    metrics collection behavior.
  - Risk defense: this slice changes import ownership only; it does not move
    ECStore definitions, alter runtime control flow, adjust readiness checks,
    mutate table catalog metadata, change IAM policy behavior, or alter notify,
    S3 Select, or obs runtime semantics.
  - Verification: focused compile, direct import scan, migration/layer guards,
    formatting, diff hygiene, Rust risk scan, full pre-commit, and required
    three-expert review passed.

- [x] `API-032` Clean test harness and fuzz ECStore compatibility boundaries.
  - Current branch: `overtrue/arch-test-harness-fuzz-storage-boundaries`.
  - Completed slice: add scanner/heal integration test, e2e test, and fuzz
    target compatibility boundary modules; migrate direct `rustfs_ecstore`
    imports in those test/fuzz harnesses through local boundaries.
  - Acceptance: direct `rustfs_ecstore` references in scanner/heal integration
    tests, e2e test helpers, and fuzz targets are limited to local
    compatibility boundary modules; test and fuzz modules consume local
    compatibility names.
  - Must preserve: scanner lifecycle integration behavior, heal integration and
    bug-fix test behavior, e2e node/grpc/replication helpers, fuzz target input
    shape, and existing ECStore-owned concrete type ownership.
  - Risk defense: this slice changes import ownership only; it does not move
    ECStore definitions, alter test setup semantics, change fuzz inputs, adjust
    runtime control flow, or mutate metadata formats.
  - Verification: focused scanner/heal/e2e compile, fuzz target compile,
    migration/layer guards, formatting check, diff hygiene, direct import scan,
    risk scan, full pre-commit, and required three-expert review passed.

- [x] `API-033` Narrow ECStore compatibility export surfaces.
  - Current branch: `overtrue/arch-narrow-storage-compat-exports`.
  - Completed slice: replace local whole-crate ECStore compatibility aliases
    with explicit re-export modules for RustFS runtime/app/admin/storage, obs,
    notify, S3 Select, IAM, scanner/heal integration tests, e2e helpers, and
    fuzz targets.
  - Acceptance: local ECStore compatibility boundaries expose only the ECStore
    modules/functions required by their consumers; direct `rustfs_ecstore`
    references remain limited to compatibility boundary modules.
  - Must preserve: all runtime, admin, storage, observability, notification, S3
    Select, IAM, scanner/heal test, e2e helper, and fuzz behavior from
    API-031/API-032.
  - Risk defense: this slice changes compatibility re-export ownership only; it
    does not move ECStore definitions, alter runtime control flow, mutate
    metadata formats, change test setup semantics, or adjust fuzz inputs.
  - Verification: focused compile, fuzz target compile, migration/layer guards,
    formatting check, diff hygiene, direct import scan, risk scan, full
    pre-commit, and required three-expert review passed.

- [x] `API-034` Narrow remaining ECStore compatibility export surfaces.
  - Current branch: `overtrue/arch-remaining-storage-compat-exports`.
  - Completed slice: narrow the remaining scanner, heal, Swift, and IAM store
    ECStore compatibility boundary modules from direct ECStore imports to
    explicit local `ecstore` re-export surfaces while keeping existing local
    semantic aliases unchanged; add a migration guard that rejects future direct
    `rustfs_ecstore` imports outside compatibility boundary modules.
  - Acceptance: direct `rustfs_ecstore` references in non-ECStore source are
    limited to local compatibility boundary modules; business modules continue
    to consume crate-local compatibility names, and migration rules reject
    bypassing those boundaries.
  - Must preserve: scanner cache/lifecycle/replication behavior, heal storage
    and disk behavior, Swift object/bucket metadata behavior, IAM object-store
    metadata behavior, and all ECStore-owned concrete type ownership.
  - Risk defense: this slice changes compatibility import ownership only; it
    does not move ECStore definitions, alter runtime control flow, mutate
    metadata formats, change Swift/IAM semantics, or adjust scanner/heal
    scheduling.
  - Verification: focused scanner/heal/IAM compile, Swift feature compile,
    migration/layer guards, formatting check, diff hygiene, direct import scan,
    risk scan, full pre-commit, and required three-expert review passed.

- [x] `API-035` Prune compatibility re-export allowances.
  - Current branch: `overtrue/arch-compat-reexport-prune`.
  - Current slice: remove unused-import allowances from production and fuzz
    ECStore compatibility boundary modules, keep target-specific test harness
    exceptions explicit, gate test-only RustFS storage compatibility re-exports
    with `cfg(test)`, and add a migration rule preventing production
    compatibility boundaries from hiding unused ECStore re-exports.
  - Acceptance: production and fuzz `storage_compat.rs` modules compile without
    unused-import allows, test-only compatibility exceptions remain scoped to
    harnesses with target-specific compile needs, and migration rules reject
    reintroducing broad unused-import allowances in production compatibility
    boundaries.
  - Must preserve: all ECStore-owned concrete types and runtime behavior,
    startup/storage/admin/app/Swift/scanner/heal/IAM/notify/obs/S3 Select
    import paths, test harness behavior, and fuzz target behavior.
  - Risk defense: this slice changes only compatibility boundary re-export
    hygiene and migration guard coverage; it does not move definitions, alter
    runtime control flow, mutate metadata formats, or change storage behavior.
  - Verification: focused compile checks, fuzz manifest compile, migration and
    layer guards, formatting check, diff hygiene, risk scan, full pre-commit,
    and required three-expert review passed.

- [x] `API-036` Move delete-object DTO contracts.
  - Current branch: `overtrue/arch-delete-object-contracts`.
  - Current slice: move `ObjectToDelete` and `DeletedObject` from ECStore
    `store_api` into `rustfs-storage-api`, keep old ECStore paths as type
    aliases for compatibility, migrate RustFS/scanner aliases to the
    storage-api contracts, and guard against reintroducing ECStore-owned delete
    DTO definitions.
  - Acceptance: storage-api exports delete-object DTO contracts, ECStore keeps
    compatibility type aliases without owning the definitions, external
    RustFS/scanner aliases consume storage-api directly, and migration rules
    reject restoring ECStore definitions or public re-exports.
  - Must preserve: delete-object field names and types, replication-state helper
    semantics, ECStore object/delete operation associated types, scanner delete
    selection behavior, RustFS object delete behavior, and old ECStore import
    compatibility.
  - Risk defense: this is a pure DTO ownership move; it does not change
    deletion control flow, replication decisions, lifecycle expiry behavior, or
    object metadata persistence.
  - Verification: focused compile checks, storage-api tests, migration and layer
    guards, formatting check, diff hygiene, risk scan, full pre-commit, and
    required three-expert review passed.

- [x] `API-037` Clean delete-object DTO consumers.
  - Current branch: `overtrue/arch-delete-object-contracts`.
  - Current slice: migrate ECStore internal delete-object DTO consumers from
    old `crate::store_api` imports to `rustfs-storage-api` contracts while
    keeping public ECStore type aliases for downstream compatibility.
  - Acceptance: ECStore object, set, lifecycle, and replication internals use
    storage-api delete DTO contracts directly; public old-path type aliases
    remain available; migration rules reject reintroducing ECStore internal
    old-path delete DTO consumers.
  - Must preserve: object delete result shape, batch delete error alignment,
    lifecycle replication scheduling, MRF delete replay, replication retry
    decisions, and old ECStore public import compatibility.
  - Risk defense: this is a consumer import cleanup over identical type
    definitions; it does not change delete control flow, replication decisions,
    lifecycle expiry behavior, or object metadata persistence.
  - Verification: focused ECStore/RustFS/scanner compile checks, migration and
    layer guards, formatting check, diff hygiene, risk scan, full pre-commit,
    and required three-expert review passed.

- [x] `API-038` Narrow remaining `store_api` compatibility re-export surfaces.
  - Current branch: `overtrue/arch-delete-object-contracts`.
  - Current slice: replace whole-module `rustfs_ecstore::store_api`
    compatibility re-exports in RustFS storage, scanner, heal, Swift,
    S3 Select, IAM, and notify boundaries with explicit contract type
    re-exports, and add a migration rule rejecting broad `store_api`
    compatibility re-exports.
  - Acceptance: storage compatibility boundaries expose only the concrete
    `store_api` contracts their consumers use; downstream local aliases keep
    the same names; migration rules reject reintroducing broad `store_api`
    passthroughs in production compatibility boundaries.
  - Must preserve: object info/options reader aliases, storage/list/multipart
    operation trait bindings, scanner/heal/Swift/S3 Select/IAM/notify behavior,
    and all ECStore-owned concrete type ownership.
  - Risk defense: this is compatibility import surface cleanup only; it does
    not move definitions, alter storage/runtime control flow, change object
    metadata conversion, or mutate reader behavior.
  - Verification: focused multi-crate compile, migration guard, formatting
    check, diff hygiene, risk scan, full pre-commit, and required three-expert
    review passed.

- [x] `API-039` Collapse nested `store_api` compatibility modules.
  - Current branch: `overtrue/arch-compat-boundary-prune`.
  - Current slice: replace nested `store_api` compatibility modules in RustFS
    storage, scanner, heal, Swift, S3 Select, IAM, and notify boundaries with
    direct local type aliases, and add a migration rule rejecting nested
    `store_api` modules in storage compatibility files.
  - Acceptance: storage compatibility boundaries no longer recreate
    `store_api` module shapes; downstream aliases keep the same concrete
    contract types; migration rules reject restoring nested `store_api`
    compatibility modules outside ECStore and test-only boundaries.
  - Must preserve: object info/options reader aliases, scanner/heal/Swift/S3
    Select/IAM/notify compile-time contracts, storage API compatibility names,
    and ECStore-owned concrete type ownership.
  - Risk defense: this is a local alias-shape cleanup only; it does not move
    definitions, alter storage/runtime control flow, change object metadata
    conversion, or mutate reader behavior.
  - Verification: focused multi-crate compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed.

- [x] `API-040` Lock remaining `store_api` compatibility aliases.
  - Current branch: `overtrue/arch-compat-boundary-prune`.
  - Current slice: add a migration rule that allows the remaining
    `rustfs_ecstore::store_api::*` references in production storage
    compatibility files only when they are explicit local type aliases to the
    four ECStore-owned contracts still intentionally kept in ECStore.
  - Acceptance: production compatibility boundaries can keep only explicit
    aliases to `GetObjectReader`, `ObjectInfo`, `ObjectOptions`, and
    `PutObjReader`; any broader import, module recreation, or new raw
    `store_api` compatibility dependency fails the architecture guard.
  - Must preserve: existing local alias names and concrete ECStore-owned reader,
    object info, and object option contract ownership.
  - Risk defense: this is a guardrail-only slice; it does not change runtime
    code, storage behavior, object metadata shape, or reader behavior.
  - Verification: migration and layer guards, formatting check, diff hygiene,
    risk scan, full pre-commit, and required three-expert review passed.

- [x] `API-041` Lock ECStore compatibility passthrough allowlists.
  - Current branch: `overtrue/arch-compat-passthrough-guards`.
  - Current slice: add a migration rule that snapshots every
    `rustfs_ecstore` module/function passthrough exposed from local
    `storage_compat.rs` boundaries across RustFS, scanner, heal, Swift,
    S3 Select, IAM, notify, observability, e2e, and fuzz harnesses.
  - Acceptance: compatibility boundaries cannot silently add or remove ECStore
    passthrough items; future cleanup PRs must update the explicit allowlist
    when they intentionally shrink or reshape a boundary.
  - Must preserve: all existing local compatibility paths, ECStore concrete
    type ownership, storage behavior, startup behavior, scanner/heal behavior,
    Swift/S3 Select/IAM/notify behavior, observability reads, and test/fuzz
    harness behavior.
  - Risk defense: this is a loss-prevention guard only; it does not change
    runtime code, storage APIs, object metadata shape, reader behavior, or
    worker lifecycle.
  - Verification: migration guard, formatting check, diff hygiene, risk scan,
    focused script check, and full pre-commit required before push.

- [x] `API-042` Split notify event object contract from ECStore ObjectInfo.
  - Current branch: `overtrue/arch-compat-passthrough-contracts`.
  - Current slice: give `rustfs-notify` its own lightweight
    `NotifyObjectInfo` event DTO, keep ECStore-to-notify conversion private to
    the notify compatibility boundary, and update RustFS event handoff sites to
    use the conversion explicitly.
  - Acceptance: notify no longer publicly re-exports ECStore `ObjectInfo` as
    its event object type; existing RustFS event generation, restore-completed
    event data, version IDs, object metadata filtering, and ECStore bridge
    behavior are preserved.
  - Must preserve: S3 event JSON shape, remove-event metadata suppression,
    restore-completed glacier data formatting, object key URL encoding,
    request/response headers, replication request filtering, and existing
    EventArgsBuilder call sites.
  - Risk defense: this is a consumer contract split only; ECStore remains the
    producer of storage metadata, while notify owns the event-facing DTO.
  - Verification: focused notify/RustFS compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed.

- [x] `API-043` Remove notify ECStore config passthroughs.
  - Current branch: `overtrue/arch-compat-passthrough-contracts`.
  - Current slice: replace notify's public compatibility passthroughs for
    ECStore config/global modules with a crate-local config update boundary,
    then shrink the passthrough guard snapshot.
  - Acceptance: notify config mutation code no longer reaches through
    ECStore config/global modules directly; the storage compatibility boundary
    owns ECStore handle resolution, read, save, and error classification.
  - Must preserve: target config read-modify-save behavior, unchanged-config
    no-op handling, storage-not-initialized error wording, read/save error
    mapping, target reload ordering, and runtime lifecycle logging.
  - Risk defense: this keeps persistence semantics unchanged while reducing
    the compatibility surface visible to notify business logic.
  - Verification: focused notify/RustFS compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review required before push.

- [x] `API-044` Remove S3 Select ECStore module passthroughs.
  - Current branch: `overtrue/arch-compat-passthrough-contracts`.
  - Current slice: replace S3 Select's public compatibility passthroughs for
    ECStore error, store, set-disk, and resolver modules with crate-local
    aliases/functions, then shrink the passthrough guard snapshot.
  - Acceptance: S3 Select object-store code no longer reaches through ECStore
    modules directly; storage errors, store handle resolution, ECStore store
    type ownership, and default read-buffer sizing remain behind the local
    storage compatibility boundary.
  - Must preserve: S3 Select object-store initialization, not-found error
    mapping, scan-range defaults, stream buffer sizing, JSON document handling,
    CSV conversion streams, and ECStore object reader/info calls.
  - Risk defense: this changes import ownership only; S3 Select still uses the
    same ECStore runtime APIs through narrower local compatibility names.
  - Verification: focused S3 Select/notify/RustFS compile, migration and layer
    guards, formatting check, diff hygiene, risk scan, full pre-commit, and
    required three-expert review required before push.

- [x] `API-045` Remove observability ECStore module passthroughs.
  - Current branch: `overtrue/arch-compat-passthrough-contracts`.
  - Current slice: replace OBS metrics passthroughs for ECStore bucket,
    data-usage, global, pools, and object-store resolver modules with
    crate-local storage compatibility functions and snapshots, then shrink the
    passthrough guard snapshot.
  - Acceptance: OBS metrics collection no longer reaches through ECStore
    modules directly; object-store resolution, data-usage loading, capacity
    calculation, quota reads, replication state, bucket bandwidth monitor
    access, and ILM runtime counters remain behind the OBS compatibility
    boundary.
  - Must preserve: cluster/health metrics, bucket usage metrics, replication
    and bandwidth metrics, scheduler tombstone behavior, disk/drive metrics,
    erasure-set metrics, ILM metrics, existing warning paths, and no-data
    fallback behavior.
  - Risk defense: this changes compatibility ownership only; OBS still reads
    the same ECStore runtime state through narrower local compatibility names.
  - Verification: focused OBS/notify/S3 Select/RustFS compile, migration and
    layer guards, formatting check, diff hygiene, risk scan, full pre-commit,
    and required three-expert review required before push.

- [x] `API-046` Remove IAM and Swift ECStore module passthroughs.
  - Current branch: `overtrue/arch-compat-iam-swift-boundaries`.
  - Current slice: replace IAM's ECStore config/error/global/notification/store
    module passthroughs and Swift's ECStore bucket/error/store resolver
    passthroughs with local compatibility aliases and wrapper functions, then
    shrink the passthrough guard snapshot.
  - Acceptance: IAM store, IAM notification fanout, IAM error conversion, IAM
    first-node checks, and Swift bucket metadata/object-store access no longer
    reach through ECStore modules directly from consumer code.
  - Must preserve: IAM config prefix layout, IAM config read/write/delete
    semantics, lazy rewrite precondition behavior, config-not-found mapping,
    peer notification fanout error logging, first-node initial load behavior,
    Swift object-store resolution, and Swift bucket metadata get/set behavior.
  - Risk defense: this is an import ownership and compatibility-boundary
    cleanup only; ECStore remains the owner of concrete storage/runtime state
    while IAM and Swift expose narrower local names to their consumers.
  - Verification: focused IAM/Swift compile, IAM unit tests, migration and
    layer guards, formatting check, diff hygiene, risk scan, full pre-commit,
    and required three-expert review required before push.

- [x] `API-047` Remove heal and scanner production ECStore module passthroughs.
  - Current branch: `overtrue/arch-heal-scanner-compat-boundaries`.
  - Current slice: replace heal and scanner production compatibility
    passthrough modules with explicit local aliases and wrapper functions,
    while leaving test-only ECStore compatibility harnesses for later cleanup.
  - Acceptance: heal and scanner production code no longer exposes broad
    ECStore module passthroughs for bucket/config/data-usage/disk/error/global,
    pools, set-disk, store, or store-utils through `storage_compat.rs`.
  - Must preserve: heal disk/resume/task behavior, scanner config persistence,
    scanner lifecycle/replication actions, bucket cache scanning, object-store
    resolution, erasure-mode checks, storage-class accounting, and data-usage
    memory updates.
  - Risk defense: this narrows import ownership only; ECStore remains the owner
    of concrete storage/runtime state and scanner/heal keep the same local
    compatibility names for existing call sites.
  - Verification: focused heal/scanner compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review required before push.

- [x] `API-048` Remove RustFS runtime ECStore module passthroughs.
  - Current branch: `overtrue/arch-rustfs-runtime-compat-boundaries`.
  - Current slice: replace the RustFS app, admin, storage, and root runtime
    compatibility passthrough modules with explicit local aliases and nested
    compatibility exports, while preserving existing consumer paths.
  - Acceptance: RustFS runtime compatibility files no longer expose broad
    ECStore top-level module passthroughs for app/admin/storage/root runtime
    consumers, and the passthrough guard snapshot keeps only test/fuzz
    harness allowances.
  - Must preserve: startup config/bootstrap behavior, server readiness checks,
    admin replication/rebalance/tier/config handlers, app object/bucket/
    multipart usecases, storage RPC/SSE/access paths, table catalog storage
    access, and existing test-only harness imports.
  - Risk defense: this is an import ownership and compatibility-boundary
    cleanup only; ECStore remains the owner of concrete storage/runtime state
    while RustFS runtime modules retain stable local compatibility paths.
  - Verification: focused RustFS test compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed before push.

- [x] `API-049` Remove test and fuzz ECStore module passthroughs.
  - Current branch: `overtrue/arch-test-fuzz-compat-boundaries`.
  - Current slice: replace the remaining e2e, heal-test, scanner-test, and
    fuzz-target ECStore module passthroughs with explicit local compatibility
    aliases, split fuzz storage compatibility by target, and empty the
    passthrough guard snapshot.
  - Acceptance: no `storage_compat.rs` file may expose broad
    `rustfs_ecstore` module passthroughs; the migration guard now rejects any
    new passthrough unless a later slice deliberately adds a reviewed
    allowlist entry.
  - Must preserve: e2e bucket target and RPC helper imports, heal test disk and
    store setup imports, scanner test lifecycle/tier/disk/storage imports,
    fuzz bucket validation behavior, and fuzz path containment behavior.
  - Risk defense: this is test-harness and fuzz-harness import ownership
    cleanup only; ECStore remains the owner of the same concrete APIs and no
    production runtime path is changed.
  - Verification: focused test/fuzz compiles, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed before push.

- [x] `API-050` Move lifecycle helper DTO contracts.
  - Current branch: `overtrue/arch-storage-api-lifecycle-contracts`.
  - Current slice: move `ExpirationOptions` and `TransitionedObject` into
    rustfs-storage-api, update ECStore internal consumers plus notify test
    coverage to import them directly, and keep ECStore old-path re-exports for
    downstream compatibility callers.
  - Acceptance: rustfs-storage-api exports both lifecycle helper DTOs, ECStore
    no longer owns their concrete struct definitions, ECStore internal
    consumers and notify coverage use the storage-api contracts directly, old
    ECStore lifecycle paths remain available as re-exports, and migration rules
    reject restoring the ECStore definitions or old internal imports.
  - Must preserve: lifecycle expiration flags, transitioned object journal
    metadata, object info construction, notify event conversion, and all old
    ECStore import paths used by existing callers.
  - Risk defense: this is a pure DTO move; no lifecycle scheduling, object I/O,
    transition journal, replication, or reader behavior is changed.
  - Verification: storage-api lifecycle helper unit test, ECStore transitioned
    lifecycle tests, notify event conversion test, focused compile checks,
    migration and layer guards, formatting check, diff hygiene, risk scan, full
    pre-commit, and required three-expert review passed before push.

- [x] `API-051` Flatten test harness storage compatibility aliases.
  - Current branch: `overtrue/arch-test-harness-compat-aliases`.
  - Current slice: flatten e2e, heal, scanner, and fuzz storage compatibility
    harnesses from nested `storage_compat::ecstore` modules into direct
    crate-local aliases, constants, and function imports.
  - Acceptance: no e2e, heal-test, scanner-test, or fuzz-target harness file
    may expose or consume nested `storage_compat::ecstore` paths, and migration
    rules reject reintroducing nested test/fuzz ECStore compatibility modules.
  - Must preserve: e2e bucket target/RPC/disk helper imports, heal ECStore disk
    and endpoint setup, scanner lifecycle/tier/disk/storage setup, fuzz bucket
    validation behavior, and fuzz path-containment validation behavior.
  - Risk defense: this is test-harness and fuzz-harness import cleanup only; no
    production runtime behavior, ECStore ownership, storage metadata format, or
    scanner/heal lifecycle logic is changed.
  - Verification: focused e2e/heal/scanner test compile, harness tests,
    migration and layer guards, formatting check, diff hygiene, risk scan, full
    pre-commit, and required three-expert review passed before push.

- [x] `API-052` Flatten RustFS runtime storage compatibility aliases.
  - Current branch: `overtrue/arch-rustfs-storage-compat-aliases`.
  - Current slice: flatten RustFS root, app, admin, and storage runtime
    compatibility facades from nested `storage_compat::ecstore` modules into
    direct crate-local aliases, constants, and function imports.
  - Acceptance: no RustFS runtime source file may expose or consume nested
    `storage_compat::ecstore` paths, and migration rules reject reintroducing
    nested RustFS runtime ECStore compatibility modules.
  - Must preserve: startup/config/bootstrap behavior, server readiness checks,
    admin replication/rebalance/tier/config handlers, app object/bucket/
    multipart usecases, storage RPC/SSE/access paths, table catalog storage
    access, and existing local compatibility ownership.
  - Risk defense: this is RustFS runtime import cleanup only; no production
    runtime behavior, ECStore ownership, storage metadata format, object I/O,
    admin authorization, or readiness semantics are changed.
  - Verification: focused RustFS compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed before push.

- [x] `API-053` Flatten RustFS runtime scalar storage compatibility aliases.
  - Current branch: `overtrue/arch-runtime-compat-surface-prune`.
  - Current slice: flatten RustFS root, app, admin, and storage runtime scalar
    compatibility facades such as store, error, global, endpoints, RPC,
    metrics, notification, set-disk, and data-usage paths into direct
    crate-local aliases and functions.
  - Acceptance: RustFS runtime source no longer consumes those scalar
    compatibility surfaces through secondary modules, while higher-coupling
    bucket/config/rio compatibility modules remain unchanged; migration rules
    reject restoring the flattened scalar paths.
  - Must preserve: startup config/bootstrap behavior, server readiness checks,
    admin replication/rebalance/tier/config handlers, app object/bucket/
    multipart usecases, storage RPC/SSE/access paths, table catalog storage
    access, and existing ECStore concrete type ownership.
  - Risk defense: this is import ownership and facade-shape cleanup only; no
    production runtime behavior, ECStore ownership, storage metadata format,
    object I/O, admin authorization, or readiness semantics are changed.
  - Verification: focused RustFS compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, full pre-commit, and required
    three-expert review passed before push.

- [x] `API-054` Flatten RustFS runtime secondary storage compatibility aliases.
  - Current branch: `overtrue/arch-runtime-secondary-compat-flatten`.
  - Current slice: flatten RustFS root, app, admin, and storage runtime
    secondary compatibility modules such as bucket, config, rio, client, tier,
    compress, disk, and rebalance into direct crate-local aliases, modules, and
    functions.
  - Acceptance: RustFS runtime source no longer consumes those compatibility
    surfaces through broad secondary modules, the runtime compatibility files no
    longer define those wrapper modules, and migration rules reject restoring
    the flattened secondary paths.
  - Must preserve: startup config/bootstrap behavior, server module-switch
    config reads, embedded startup storage initialization, admin bucket/meta/
    tier/rebalance/config handlers, app object/bucket/multipart usecases,
    storage RPC/SSE/access paths, table catalog storage access, and ECStore
    concrete type ownership.
  - Risk defense: this is import ownership and facade-shape cleanup only; no
    production runtime behavior, ECStore ownership, storage metadata format,
    object I/O, admin authorization, tier behavior, or readiness semantics are
    changed.
  - Verification: focused RustFS compile, migration and layer guards,
    formatting check, diff hygiene, risk scan, and required three-expert review
    passed before push.

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

1. `consumer-migration`: connect additional scanner, repair, replication, and
   metadata admission owners to the workload registry only after each owner has
   dedicated preservation coverage.
2. `pure-move`/`consumer-migration`: continue larger cleanup slices with the
   loss-prevention guards active for remaining ECStore compatibility contracts
   now that broad compatibility passthroughs are fully closed.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | passed | S-015 removes obsolete KMS admin policy action variants after the handler fallback cleanup; API-042/API-043/API-044/API-045/API-046/API-047/API-048/API-049/API-050/API-051/API-052/API-053/API-054 narrow notify, S3 Select, OBS, IAM, Swift, heal, scanner, RustFS runtime, test, fuzz, lifecycle helper, harness, and RustFS runtime compatibility contracts without moving ECStore storage metadata ownership; G-011/G-012/G-013 add docs-only baselines for scheduler, placement/repair, and profiling/NUMA work; Issue #660 PR-08/PR-09 add read-only observability and topology contracts in storage-api only; PR-05/PR-07 add scheduler preservation tests and workload contracts; API-055/SCH-001 adds a local storage concurrency provider; current API-056/R-016 wires runtime observability and endpoint topology providers without moving implementation ownership. |
| Migration preservation | passed | KMS endpoint URLs, query aliases, request bodies, response contracts, and dedicated `kms:*` authorization behavior are preserved; event builder call sites, ECStore event bridge conversion, restore event data, version IDs, metadata filtering, config read/save semantics, S3 Select store/error/buffer semantics, OBS metrics state reads, IAM config/notification/error semantics, Swift bucket metadata access, heal disk/resume/task behavior, scanner lifecycle/replication/data-usage behavior, RustFS startup/admin/app/storage runtime access, e2e/test/fuzz import behavior, lifecycle expiration/transition helper DTO field contracts, flattened harness and RustFS runtime scalar/secondary alias behavior, unchanged no-op handling, remove-event behavior, scheduler/readiness/placement/profiling runtime behavior, platform gates, missing/unknown capability states, placement/topology labels, scheduler thresholds, queue snapshot semantics, disk-read semaphore behavior, and admission behavior are preserved. |
| Testing/verification | passed | Focused compiles/tests, fuzz target compile, guards, formatting, diff hygiene, risk scan, and full `make pre-commit` passed for prior code slices; current Issue #660 API-056/R-016 slice uses runtime capability provider tests, focused RustFS library check, migration and layer guards, formatting, diff hygiene, and three-expert review. |

## Verification Notes

Passed before push:

- Issue #660 API-056/R-016 current slice:
  - `cargo test -p rustfs --lib runtime_capabilities -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 API-055/SCH-001 current slice:
  - `cargo test -p rustfs --lib storage::concurrency::manager::integration_tests -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 PR-05/PR-07 current slice:
  - `cargo test -p rustfs-concurrency --no-fail-fast`: passed.
  - `cargo check -p rustfs-concurrency`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 PR-08/PR-09 current slice:
  - `cargo test -p rustfs-storage-api`: passed.
  - `cargo check -p rustfs-storage-api`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- G-011/G-012/G-013 current slice:
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `git diff --check`: passed.
  - Three-expert review: passed.
  - Full `make pre-commit`: not run because this slice is documentation-only.

- API-054 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; only existing import and path rewrites were
    reviewed, with no new unwrap/expect, panic/todo/unsafe, risky casts,
    ad-hoc error construction, or sensitive-token handling semantics.

- API-053 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; only existing import and path rewrites were
    reviewed, with no new unwrap/expect, panic/todo/unsafe, risky casts,
    ad-hoc error construction, or sensitive-token handling semantics.
  - `make pre-commit`: passed.

- API-052 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; only existing-semantic path replacement hits were
    reviewed, with no new unwrap/expect, panic/todo/unsafe, risky casts,
    ad-hoc error construction, or sensitive-token handling semantics.
  - `make pre-commit`: passed, including 6250 nextest tests and doctests.

- API-050 current slice:
  - `cargo test -p rustfs-storage-api lifecycle_helper_defaults_preserve_existing_contracts --no-fail-fast`:
    passed.
  - `cargo check --tests -p rustfs-storage-api -p rustfs-ecstore -p rustfs-notify`:
    passed.
  - `cargo test -p rustfs-ecstore transitioned --no-fail-fast`: passed.
  - `cargo test -p rustfs-notify ecstore_object_info_conversion_preserves_notify_event_fields --no-fail-fast`:
    passed.
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, panic/todo/unsafe, risky
    casts, ad-hoc error construction, or sensitive-token handling in added
    lines.
  - `make pre-commit`: passed.

- API-051 current slice:
  - `cargo check --tests -p e2e_test -p rustfs-heal -p rustfs-scanner`:
    passed.
  - `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: passed.
  - `cargo test -p rustfs-heal --test endpoint_index_test test_endpoint_index_settings --no-fail-fast`:
    passed.
  - `cargo test -p rustfs-scanner --test lifecycle_integration_test --no-run`:
    passed.
  - `cargo test -p e2e_test --no-run`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; only existing test `unwrap` calls were touched by
    import path rewrites, with no new unwrap/expect, panic/todo/unsafe, risky
    casts, ad-hoc error construction, or sensitive-token handling semantics.
  - `make pre-commit`: passed.

- S-015 current slice:
  - `cargo test -p rustfs-policy test_legacy_kms_admin_actions_are_rejected --no-fail-fast`:
    passed.
  - `cargo test -p rustfs kms_key_auth_actions_use_dedicated_kms_actions --no-fail-fast`:
    passed.
  - `cargo test -p rustfs route_policy_records_dedicated_kms_actions --no-fail-fast`:
    passed.
  - `cargo test -p rustfs route_policy_rejects_server_info_for_sensitive_kms_actions --no-fail-fast`:
    passed.
  - `cargo check --tests -p rustfs-policy -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed.

- S-014 previous slice:
  - `cargo test -p rustfs kms_key_auth_actions_use_dedicated_kms_actions --no-fail-fast`:
    passed.
  - `cargo test -p rustfs route_policy_records_dedicated_kms_actions --no-fail-fast`:
    passed.
  - `cargo test -p rustfs route_policy_rejects_server_info_for_sensitive_kms_actions --no-fail-fast`:
    passed.
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Source marker scan: passed; no non-doc `RUSTFS_COMPAT_TODO` markers remain.
  - Rust risk scan: passed; no new unwrap/expect, panic/todo/unsafe, risky
    casts, ad-hoc error construction, or sensitive-token handling in added
    lines.
  - `make pre-commit`: passed.

- API-049 current slice:
  - `cargo check --tests -p rustfs-heal -p rustfs-scanner -p e2e_test`:
    passed.
  - `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, panic/todo/unsafe, risky
    casts, ad-hoc error construction, or sensitive-token handling in added
    lines.
  - `make pre-commit`: passed.

- API-048 current slice:
  - `cargo check --tests -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Rust risk scan: passed; no new unwrap/expect, panic/todo/unsafe, risky
    casts, ad-hoc error construction, or sensitive-token handling in added
    lines.
  - `make pre-commit`: passed.

- API-047 current slice:
  - `cargo check --tests -p rustfs-heal -p rustfs-scanner`: passed.
  - `cargo test -p rustfs-heal -p rustfs-scanner`: passed, 290 tests passed
    and 14 ignored.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; the only match was a test-only scanner config init
    re-export.

- API-046 current slice:
  - `cargo check --tests -p rustfs-iam -p rustfs-protos`: passed.
  - `cargo test -p rustfs-iam`: passed, 150 tests.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: reviewed added lines; only existing error-mapping behavior
    was renamed to IAM-local compatibility aliases.
  - `make pre-commit`: passed.

- API-042 current slice:
  - `cargo check --tests -p rustfs-notify -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

- API-043 current slice:
  - `cargo test -p rustfs-notify
    storage_compat::tests::ecstore_object_info_conversion_preserves_notify_event_fields`:
    passed.
  - `cargo check --tests -p rustfs-notify -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed, including 6245 nextest tests passed and 111
    skipped.

- API-044 current slice:
  - `cargo check --tests -p rustfs-s3select-api -p rustfs-notify -p
    rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed, including 6245 nextest tests passed and 111
    skipped.

- API-045 current slice:
  - `cargo check --tests -p rustfs-obs -p rustfs-s3select-api -p
    rustfs-notify -p rustfs`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed, including 6245 nextest tests passed and 111
    skipped.

- API-041 current slice:
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no Rust code changed.
  - `make pre-commit`: passed.

- API-040 current slice:
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

- API-039 current slice:
  - `cargo check --tests -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-protocols -p rustfs-s3select-api -p rustfs-iam -p rustfs-notify`:
    passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

- API-038 current slice:
  - `cargo check --tests -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-protocols -p rustfs-s3select-api -p rustfs-iam -p rustfs-notify`:
    passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

- API-037 current slice:
  - `cargo check --tests -p rustfs-ecstore -p rustfs -p rustfs-scanner`:
    passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

- API-036 current slice:
  - `cargo test -p rustfs-storage-api`: passed.
  - `cargo check --tests -p rustfs-storage-api -p rustfs-ecstore -p rustfs-scanner -p rustfs`:
    passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
    public APIs, boxed public errors, production println/eprintln, or relaxed
    ordering introduced in changed Rust files.
  - `make pre-commit`: passed.

API-035 prior slice:

- `cargo check --tests -p rustfs-scanner -p rustfs-heal -p rustfs-iam`:
  passed.
- `cargo check --tests -p rustfs-protocols --features swift`: passed.
- `cargo check --tests -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-iam -p rustfs-notify -p rustfs-obs -p rustfs-s3select-api -p e2e_test`:
  passed.
- `cargo check --manifest-path fuzz/Cargo.toml --bins`: passed.
- `rg -n 'rustfs_ecstore' crates/scanner/src crates/heal/src crates/protocols/src/swift crates/iam/src/store --glob '*.rs'`:
  remaining matches are deliberate compatibility boundary definitions.
- `./scripts/check_architecture_migration_rules.sh`: passed.
- `./scripts/check_layer_dependencies.sh`: passed.
- `cargo fmt --all --check`: passed.
- `git diff --check`: passed.
- Rust risk scan: passed; no new unwrap/expect, numeric casts, string error
  public APIs, boxed public errors, production println/eprintln, or relaxed
  ordering introduced in changed Rust files.
- `make pre-commit`: passed.

Earlier API-033 verification retained in prior branch/PR:

- `cargo check --tests -p rustfs -p rustfs-obs -p rustfs-notify -p rustfs-s3select-api -p rustfs-iam`:
  passed.
- `cargo check --manifest-path fuzz/Cargo.toml --bins`: passed.
- `rg -n 'rustfs_ecstore' rustfs/src crates/obs/src crates/notify/src crates/s3select-api/src crates/iam/src --glob '*.rs'`:
  remaining matches are deliberate compatibility boundary definitions.
- Direct import scan for target scanner/heal/e2e/fuzz paths: passed; remaining
  matches are deliberate compatibility boundary definitions.
- `./scripts/check_architecture_migration_rules.sh`: passed.
- `./scripts/check_layer_dependencies.sh`: passed.
- `cargo fmt --all --check`: passed.
- `git diff --check`: passed.
- Rust risk scan: reviewed added `.unwrap()` matches as preserved test setup
  unwraps caused by path rewrite formatting; no new risky behavior added.
- `make pre-commit`: passed.

Notes:

- This larger slice is based on `origin/main` after `rustfs/rustfs#3572`
  merged.
- Direct ECStore imports in the target runtime/obs/notify/S3 Select/IAM and
  scanner/heal/e2e/fuzz areas now remain only in local compatibility boundary
  modules.
- The slice does not alter startup behavior, readiness behavior, table catalog
  object I/O, notification persistence, S3 Select reads, IAM error mapping,
  observability metrics, test/fuzz semantics, or ECStore definitions.

## Handoff Notes

- Continue with larger consumer-migration batches outside the cleaned
  app/storage/admin/scanner/heal/Swift/runtime/obs/notify/S3 Select/IAM/test
  and fuzz boundaries; keep ECStore-owned behavior in ECStore until concrete
  behavior is isolated enough for a pure-move slice.
