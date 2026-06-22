# Architecture Migration Progress

Status values: `[ ]` not started, `[~]` in progress, `[x]` complete, `[!]` blocked.

## Current Context

- Issue: [`rustfs/backlog#660`](https://github.com/rustfs/backlog/issues/660)
- Branch: `overtrue/arch-rustfs-runtime-owner-symbols`
- Baseline: completed `C-011/C-012/C-013/API-055/API-059/API-079/API-080/API-081/API-082/API-083/API-084/API-085/API-086/API-087/API-088/API-089/API-090/API-091/API-092/API-093/API-094/API-095/API-096/API-097/API-098/API-099/API-100/API-101/API-102/API-103/API-104/API-105/API-106/API-107/API-108/API-109/API-110/API-111/API-112/API-113/API-114/API-115/API-116/API-117/API-118/API-119/API-120/API-121/API-122/API-123/API-124/API-125/API-126/API-127/API-128/API-129/API-130/API-131/API-132/API-133/API-134/API-135/API-136`.
- Based on: API-136 stacked slice.
- PR type for this branch: `pure-move`
- Runtime behavior changes: none.
- Rust code changes: replace RustFS app/admin/storage owner-root ECStore facade
  aliases with owner-local curated symbol modules.
- CI/script changes: lock completed owner and test/fuzz boundaries against
  bare or glob ECStore facade module imports.
- Docs changes: record the API-136/API-137 RustFS owner facade cleanup.

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
- [x] `C-011-POLICY` Bridge storage concurrency policies.
  - Completed slice: add explicit projections from storage object backpressure
    and request hang/deadlock policies into the shared `rustfs-concurrency`
    facade and reusable `rustfs-io-core` configs.
  - Acceptance: storage keeps existing env/default ownership and runtime
    behavior, while later controller/read-only status work can consume the
    shared facade policy shape instead of duplicating field mapping.
  - Must preserve: no worker start/stop, no object pipe state-machine change, no
    deadlock detector lifecycle change, no metrics label change, and no S3 I/O
    behavior change.
  - Verification: storage backpressure/deadlock policy tests, compile coverage,
    formatting, diff hygiene, risk scan, architecture guard, pre-commit quality
    gate, and three-expert review.
- [x] `C-012-POLICY` Consume storage concurrency policy bridges.
  - Completed slice: route object backpressure threshold derivation and request
    hang/deadlock runtime policy reads through the shared `rustfs-concurrency`
    facade policies.
  - Acceptance: storage keeps env/default ownership and local state machines,
    while threshold and hang-policy consumption is anchored on the shared
    concurrency policy shapes.
  - Must preserve: no worker start/stop, no object pipe state-machine change, no
    deadlock detector lifecycle change, no metrics label change, and no S3 I/O
    behavior change.
  - Verification: storage backpressure/deadlock consumer tests, compile
    coverage, formatting, diff hygiene, risk scan, architecture guard,
    pre-commit quality gate, and three-expert review.
- [x] `C-013-ADMISSION` Compose workload admission providers.
  - Completed slice: add workload admission registry overlay support, compose
    the RustFS workload admission provider from the storage concurrency
    provider plus RustFS runtime owner snapshots, and guard the composition
    boundary.
  - Acceptance: foreground-read admission remains owned by the storage
    concurrency provider, RustFS runtime owner snapshots overlay metadata,
    scanner, repair, replication, and foreground-write status, and later
    controller/status work can consume one provider-composed registry.
  - Must preserve: disk-read semaphore acquisition, scanner activity counter,
    heal task/queue counters, replication worker/queue stats, metadata runtime
    initialization checks, object write paths, and queue behavior.
  - Verification: workload contract tests, RustFS workload admission tests,
    compile coverage, formatting, diff hygiene, risk scan, architecture guard,
    pre-commit quality gate, and three-expert review.
- [x] `API-079` Prune root runtime bucket compatibility modules.
  - Completed slice: collapse RustFS root `storage_compat.rs` bucket
    metadata/quota module passthroughs into explicit notification-config,
    table-catalog metadata, and quota-error aliases, and guard the boundary
    against broad module restores.
  - Acceptance: root runtime consumers use direct compatibility aliases for
    bucket notification loading, table-catalog metadata checks, and quota error
    mapping, while app/admin/storage owner-local compatibility modules keep
    their narrower module paths until their own cleanup slices.
  - Must preserve: bucket notification loading, notifier event registration,
    table-bucket mutation guards, quota error to S3 error mapping, ECStore
    bucket metadata ownership, and all app/admin/storage compatibility paths.
  - Verification: RustFS compile coverage, formatting, diff hygiene, risk
    scan, architecture guard, pre-commit quality gate, and three-expert review.
- [x] `API-080` Prune root runtime config and disk compatibility aliases.
  - Completed slice: replace root config `com` passthroughs with explicit
    config read/write aliases for module switches, expose ECStore config
    initialization as `init_ecstore_config`, split disk endpoint access into an
    explicit `Endpoint` alias, and guard these root aliases against broad
    module restores.
  - Acceptance: startup storage initializes ECStore config through a direct
    compatibility alias, module switch persistence uses direct config IO
    aliases, root runtime disk endpoint consumers keep the same endpoint type,
    and app/admin/storage local compatibility modules remain unchanged for
    their own cleanup slices.
  - Must preserve: startup storage initialization order, global config
    migration/retry behavior, module switch persistence semantics, endpoint
    parsing/layout behavior, local disk and lock-client initialization,
    readiness marking, and all app/admin/storage compatibility paths.
  - Verification: RustFS compile coverage, formatting, diff hygiene, risk
    scan, architecture guard, pre-commit quality gate, and three-expert review.
- [x] `API-081` Prune admin config compatibility aliases.
  - Completed slice: replace admin config `com` and `init` passthroughs with
    explicit aliases for config object read/write/delete, server-config
    read/write, storage-class subsystem access, and config-default
    initialization.
  - Acceptance: admin config handlers, dynamic KMS/OIDC/audit handlers, site
    replication state, router notification reads, and dynamic config reload
    paths use direct admin compatibility aliases while preserving their
    existing storage keys and config defaults.
  - Must preserve: admin auth/authorization behavior, config history object
    names, KMS/OIDC/audit runtime persistence, site-replication state
    persistence, storage-class subsystem semantics, and admin route contracts.
  - Verification: RustFS compile coverage, admin focused compile coverage,
    formatting, diff hygiene, risk scan, architecture guard, pre-commit quality
    gate, and three-expert review.
- [x] `API-082` Prune storage bucket compatibility aliases.
  - Completed slice: replace storage `metadata`, `metadata_sys`,
    `object_lock`, `policy_sys`, `replication`, `tagging`, `utils`,
    `versioning`, `versioning_sys`, `object_api_utils`, and test-only `com`
    passthroughs with explicit storage compatibility aliases.
  - Acceptance: storage S3 handlers, access checks, SSE resolution, RPC
    metadata loading, CORS/object-lock helpers, list-output ETag helpers, and
    storage tests use direct compatibility aliases while preserving the same
    bucket metadata keys and ECStore facade APIs.
  - Must preserve: bucket metadata update/delete/read semantics, object-lock
    retention/default behavior, bucket policy/public-access checks, SSE bucket
    defaults, replication stats lookups, object tag encoding/decoding, S3 list
    ETag formatting, and test-only storage-class signal constants.
  - Verification: RustFS compile coverage, storage compatibility residual
    scan, formatting, diff hygiene, risk scan, architecture guard, pre-commit
    quality gate, and three-expert review.
- [x] `API-083` Prune admin/app bucket compatibility aliases.
  - Completed slice: replace admin and app broad bucket/client/storage-class
    compatibility passthroughs with explicit local compatibility modules and
    symbol whitelists.
  - Acceptance: admin replication, bucket metadata, quota, tier, site
    replication, router, and app bucket/object/multipart/lifecycle consumers
    keep their existing call paths while `storage_compat.rs` no longer exposes
    broad upstream modules.
  - Must preserve: admin bucket target updates, replication status and resync
    DTOs, site-replication metadata serialization, quota checks, lifecycle
    transition hooks, bucket metadata IO, object ETag conversion, object-lock
    checks, and app storage-class behavior.
  - Verification: RustFS compile coverage, admin/app compatibility residual
    scans, formatting, diff hygiene, risk scan, architecture guard, pre-commit
    quality gate, and three-expert review.
- [x] `API-084` Prune edge compatibility passthrough aliases.
  - Completed slice: replace scanner grouped bucket compatibility exports,
    notify broad config/global imports, observability data-usage passthroughs,
    and e2e grouped RPC passthroughs with explicit edge-local aliases,
    wrappers, and DTO projections.
  - Acceptance: scanner bucket contracts stay explicitly named, notify config
    persistence routes through local wrappers, observability metrics consume a
    local data-usage DTO, and e2e RPC helper access stays narrow.
  - Must preserve: scanner lifecycle and replication behavior, notification
    server-config update semantics, observability cluster/bucket usage metrics,
    and e2e RPC client/interceptor call sites.
  - Verification: focused edge crate compile coverage, edge compatibility
    residual scans, formatting, diff hygiene, risk scan, architecture guard,
    pre-commit quality gate, and three-expert review.
- [x] `API-085` Prune test and fuzz compatibility passthrough aliases.
  - Completed slice: replace heal/scanner test and fuzz grouped ECStore
    compatibility passthroughs with direct type aliases and local wrapper
    functions.
  - Acceptance: test harnesses keep their existing ECStore setup and lifecycle
    helper call sites while exposing only narrow compatibility symbols, and
    fuzz targets exercise bucket utility contracts through local wrappers.
  - Must preserve: heal test ECStore setup, scanner lifecycle integration setup,
    local disk initialization, bucket metadata updates, transition enqueue
    behavior, and fuzz validation semantics.
  - Verification: focused heal/scanner compile coverage, test/fuzz
    compatibility residual scans, formatting, diff hygiene, architecture guard,
    pre-commit quality gate, and three-expert review.
- [x] `API-086` Prune root runtime compatibility re-exports.
  - Completed slice: replace root RustFS runtime `storage_compat.rs` ECStore API
    re-exports with local constants, type aliases, a minimal disk trait, and
    wrapper functions.
  - Acceptance: root runtime startup, metadata, replication admission,
    topology, notification, RPC, capacity, table-catalog, and shutdown call
    sites keep their existing local compatibility names while the root boundary
    no longer re-exports ECStore API symbols directly.
  - Must preserve: startup storage initialization order, bucket metadata
    migration/init, replication runtime startup and admission counts,
    notification init, RPC signature checks, capacity disk references,
    topology snapshots, table-catalog metadata access, and shutdown behavior.
  - Verification: RustFS compile coverage, root compatibility re-export
    residual scan, formatting, diff hygiene, architecture guard, pre-commit
    quality gate, and three-expert review.
- [x] `API-087` Prune storage owner compatibility re-exports.
  - Completed slice: replace RustFS storage-owner `storage_compat.rs` ECStore
    API re-exports for metadata, object-lock, replication stats, tags, XML
    helpers, RPC globals, metrics, global accessors, tier reloads, and local
    disk helpers with local aliases and wrappers; keep only temporary trait
    imports required for method resolution.
  - Acceptance: storage S3 handlers, ECFS replication metrics, RPC node service,
    and storage tests keep their existing compatibility names while the storage
    owner boundary no longer exposes direct ECStore API symbol re-exports for
    functions, constants, globals, or DTO aliases.
  - Must preserve: bucket metadata read/write/delete semantics, object-lock
    retention checks, replication proxy metrics, object tag encoding/decoding,
    XML serialization behavior, RPC signature checks, transition-tier reloads,
    global object-store/lock/region access, and local disk lookup behavior.
  - Verification: RustFS compile coverage, storage-owner re-export residual
    scan, migration guard, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-088` Prune admin and app compatibility re-exports.
  - Completed slice: replace RustFS admin and app `storage_compat.rs` ECStore
    API re-exports with local constants, type aliases, proxy statics, and
    wrapper functions; keep only temporary trait imports required for method
    resolution.
  - Acceptance: admin handlers and app object/runtime paths keep their existing
    compatibility names while the admin and app boundaries no longer expose
    direct ECStore API symbol re-exports for functions, constants, globals, or
    DTO aliases.
  - Must preserve: admin config reads/writes, bucket metadata access, lifecycle
    enqueue/restore behavior, replication admission and scheduling, object-lock
    checks, RIO reader wrapping, data usage accounting, global object-store
    access, and local disk initialization behavior.
  - Verification: RustFS compile coverage, admin/app re-export residual scan,
    migration guard, formatting, diff hygiene, Rust risk scan, pre-commit
    quality gate, and three-expert review.
- [x] `API-089` Prune trait import compatibility re-exports.
  - Completed slice: remove the remaining direct ECStore API `pub use`
    compatibility exports from RustFS admin/app/storage and scanner/heal/e2e
    boundaries, replacing non-trait access with local wrappers and moving
    method-resolution trait imports into the files that call those methods.
  - Acceptance: compatibility boundary files no longer expose
    `pub(crate) use rustfs_ecstore::api` symbols, while scanner, heal, e2e,
    admin, app, and storage call sites keep their existing behavior through
    direct trait imports or local wrappers.
  - Must preserve: scanner lifecycle and replication evaluation, heal local
    disk scanning, e2e RPC signature setup, app restore/lifecycle/object-lock
    checks, admin site-replication behavior, storage RPC disk access, and S3
    versioning/replication behavior.
  - Verification: RustFS and edge crate compile coverage, compatibility
    re-export residual scan, migration guard, formatting, diff hygiene, Rust
    risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-090` Prune outer compat object/error facade aliases.
  - Completed slice: replace app/admin/storage raw ECStore object/error facade
    aliases with storage-api associated object aliases and local `StorageError`
    aliases.
  - Acceptance: app/admin/storage compatibility boundaries no longer refer to
    `rustfs_ecstore::api::object::{ObjectInfo,ObjectOptions}` or
    `rustfs_ecstore::api::error::{Error,Result}` while behavior stays
    unchanged.
  - Must preserve: lifecycle restore/options, object-lock deletion checks,
    replication scheduling decisions, admin/storage config error matching, and
    storage S3 error mapping.
  - Verification: RustFS compile coverage, residual scan, migration guard,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-091` Prune outer compat signature facade paths.
  - Completed slice: replace app/admin/storage raw ECStore metadata,
    object-lock, lifecycle journal, monitor, and notification facade paths in
    compatibility function signatures with local aliases.
  - Acceptance: app/admin/storage compatibility function signatures no longer
    expose raw ECStore facade paths for `BucketMetadataSys`,
    `ObjectLockBlockReason`, lifecycle `Jentry`, bandwidth `Monitor`, or
    `NotificationSys`.
  - Must preserve: test metadata-system access, object-lock retention checks,
    lifecycle tier-delete journal persistence, admin bandwidth monitor access,
    and notification-system access.
  - Verification: RustFS compile coverage, signature residual scan, migration
    guard, formatting, diff hygiene, Rust risk scan, pre-commit quality gate,
    and three-expert review.
- [x] `API-092` Prune storage-owner raw facade paths.
  - Completed slice: replace scattered raw `rustfs_ecstore::api::...` paths in
    the RustFS storage-owner compatibility boundary with local `ecstore_*`
    module aliases.
  - Acceptance: `rustfs/src/storage/storage_compat.rs` no longer contains raw
    `rustfs_ecstore::api::...` facade paths outside the centralized module alias
    import.
  - Must preserve: storage metadata, object-lock, replication stats, tagging,
    RPC signature, metrics, tier reload, local disk lookup, and object I/O
    associated type compatibility.
  - Verification: RustFS compile coverage, storage-owner raw facade path
    residual scan, migration guard, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-093` Prune app/admin raw facade paths.
  - Completed slice: replace scattered raw `rustfs_ecstore::api::...` paths in
    the RustFS app/admin storage compatibility boundaries with local `ecstore_*`
    module aliases.
  - Acceptance: `rustfs/src/app/storage_compat.rs` and
    `rustfs/src/admin/storage_compat.rs` no longer contain raw
    `rustfs_ecstore::api::...` facade paths outside their centralized local
    `ecstore_*` aliases.
  - Must preserve: app lifecycle, metadata, object-lock, replication, data
    usage, notification, tier, layout, compression, admin rebalance, metrics,
    bucket target, quota, storage class, and server configuration compatibility.
  - Verification: RustFS compile coverage, app/admin raw facade path residual
    scan, migration guard, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-094` Prune consumer raw facade paths.
  - Completed slice: replace scattered raw `rustfs_ecstore::api::...` paths in
    peripheral consumer storage compatibility boundaries with local
    `ecstore_*` module aliases.
  - Acceptance: IAM, heal, scanner, notify, observability, Swift, S3 Select,
    test, and fuzz storage compatibility modules no longer contain raw
    `rustfs_ecstore::api::...` facade paths outside centralized local alias
    imports.
  - Must preserve: IAM config and notifications, heal disk lookup, scanner
    lifecycle and tier helpers, notify server config IO, observability runtime
    metrics, Swift metadata wrappers, S3 Select error checks, and test/fuzz
    harness wrappers.
  - Verification: RustFS compile coverage, consumer raw facade path residual
    scan, migration guard, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-095` Prune root/e2e raw facade paths.
  - Completed slice: replace scattered raw `rustfs_ecstore::api::...` paths in
    the RustFS root runtime and e2e storage compatibility boundaries with local
    `ecstore_*` module aliases.
  - Acceptance: `rustfs/src/storage_compat.rs` and
    `crates/e2e_test/src/storage_compat.rs` no longer contain raw
    `rustfs_ecstore::api::...` facade paths outside centralized local alias
    imports.
  - Must preserve: root runtime metadata/config/global/storage/RPC wrappers and
    e2e RPC harness aliases.
  - Verification: RustFS compile coverage, root/e2e raw facade path residual
    scan, migration guard, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-096` Prune bucket trait method imports.
  - Completed slice: move outer bucket lifecycle, replication, versioning,
    object-lock, and restore-request method access behind local compatibility
    traits and wrapper functions in app, admin, storage, and scanner
    boundaries.
  - Acceptance: non-compat RustFS, scanner, and heal sources no longer import
    ECStore bucket API traits directly; the migration guard only keeps the
    remaining disk/RPC/warm-backend method-resolution exceptions.
  - Must preserve: app replication scheduling and restore validation, admin site
    replication checks, storage object/versioning behavior, scanner lifecycle
    and replication scans, and existing disk/RPC method-resolution behavior.
  - Verification: RustFS/scanner/heal compile coverage, direct bucket trait
    import residual scan, migration guard, formatting, diff hygiene, Rust risk
    scan, pre-commit quality gate, and three-expert review.
- [x] `API-097` Prune disk/RPC/warm-backend method imports.
  - Completed slice: move disk RPC, peer S3 RPC, heal/scanner disk, and
    warm-backend test method access behind local compatibility traits or
    aliases in the owning boundaries.
  - Acceptance: non-compat RustFS, scanner, heal, and test sources no longer
    import ECStore `DiskAPI`, `PeerS3Client`, or `WarmBackend` traits directly;
    the migration guard no longer allowlists those direct imports.
  - Must preserve: disk RPC request/response behavior, internode HTTP file and
    walk streams, heal resume and auto-scan disk handling, scanner disk scan
    behavior, and transition warm-backend test harness behavior.
  - Verification: RustFS/scanner/heal/e2e compile coverage, direct
    disk/RPC/warm-backend trait import residual scan, migration guard,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-098` Prune root runtime capacity/server compat consumers.
  - Completed slice: move capacity disk access, HTTP RPC signature verification,
    event dispatch bridging, module-switch config persistence, and readiness
    storage/lock quorum lookups into local `capacity` and `server`
    compatibility boundaries.
  - Acceptance: root runtime `storage_compat.rs` no longer owns
    capacity/server-only ECStore wrapper functions, trait shims, or constants;
    migration rules reject restoring those wrappers to the root facade.
  - Must preserve: capacity background refresh disk discovery, internode RPC
    signature verification, live event dispatch, module-switch persistence,
    storage readiness, and distributed lock quorum behavior.
  - Verification: RustFS test-target compile coverage, capacity/server residual
    scan, migration and layer guards, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-099` Prune root runtime startup compat consumers.
  - Completed slice: move startup storage bootstrap, bucket metadata migration,
    notification initialization, global region/port setup, background shutdown,
    and startup service ECStore aliases into a dedicated startup compatibility
    boundary.
  - Acceptance: startup and init modules no longer consume root
    `storage_compat.rs`; root runtime `storage_compat.rs` no longer owns
    startup-only ECStore wrapper functions or aliases; migration rules reject
    restoring those wrappers to the root facade.
  - Must preserve: endpoint parsing, unsupported filesystem policy,
    local-disk and lock-client initialization, global config migration,
    bucket metadata migration, IAM migration, notification registration,
    default-region fallback, background replication, and shutdown behavior.
  - Verification: RustFS test-target compile coverage, startup residual scan,
    migration and layer guards, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.
- [x] `API-100` Retire root runtime storage compatibility consumers.
  - Completed slice: move table catalog metadata constants and bucket metadata
    reads, runtime topology capability mapping, workload admission runtime
    state probes, S3 error mapping aliases, and config test disk-layout aliases
    into local compatibility boundaries, then remove the root
    `storage_compat.rs` module.
  - Acceptance: no RustFS source consumes `crate::storage_compat`; root
    runtime compatibility file is removed; migration rules still reject direct
    ECStore imports outside `*storage_compat.rs` boundaries.
  - Must preserve: table catalog internal metadata paths, lock timeout lookup,
    runtime topology snapshots, workload admission status reporting, quota and
    storage error mapping, and config disk-layout parsing tests.
  - Verification: RustFS test-target compile coverage, direct root compatibility
    consumer residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-101` Localize owner compatibility consumers.
  - Completed slice: route admin handler/service/router, app usecase/context,
    and storage RPC/S3 API compatibility consumers through local owner
    boundary modules instead of their root owner `storage_compat.rs` facades.
  - Acceptance: selected admin, app, and storage owner consumers no longer
    import `crate::admin::storage_compat`, `crate::app::storage_compat`, or
    `crate::storage::storage_compat` directly outside local compatibility
    boundary modules; migration rules reject regressions.
  - Must preserve: admin config and bucket metadata behavior, replication and
    heal status mapping, app runtime context wiring, RPC verification and disk
    lookup behavior, and S3 API ETag conversion.
  - Verification: RustFS test-target compile coverage, owner compatibility
    consumer residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-102` Localize storage core compatibility consumers.
  - Completed slice: route storage access, ECFS, ECFS extension, head-prefix,
    options, SSE, storage module aliases, and storage tests through
    `core_storage_compat` instead of the storage owner `storage_compat.rs`
    facade.
  - Acceptance: no non-compat RustFS storage source imports
    `crate::storage::storage_compat` directly; migration rules reject
    regressions across `rustfs/src/storage`.
  - Must preserve: bucket access validation, ECFS object operations, SSE
    encryption/decryption setup, storage option mapping, storage object aliases,
    and storage compatibility tests.
  - Verification: RustFS test-target compile coverage, storage compatibility
    consumer residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-103` Narrow selected local compatibility re-exports.
  - Completed slice: replace glob re-exports in admin router/service, app
    context, storage core, and storage RPC local compatibility boundaries with
    explicit re-export lists.
  - Acceptance: narrowed local compatibility boundaries expose only the symbols
    consumed by their owners; migration rules reject restoring glob re-exports
    in those files.
  - Must preserve: admin route behavior, dynamic config reload behavior, app
    context startup handles, storage core option/SSE/access behavior, and
    storage RPC request handling.
  - Verification: RustFS test-target compile coverage, narrowed local
    compatibility glob-export scan, migration and layer guards, formatting,
    diff hygiene, Rust risk scan, pre-commit quality gate, and three-expert
    review.
- [x] `API-104` Narrow remaining local compatibility re-exports.
  - Completed slice: replace the remaining admin handler and app usecase local
    compatibility glob re-exports with explicit re-export lists.
  - Acceptance: no narrowed RustFS local compatibility boundary restores a glob
    re-export from its owner `storage_compat.rs` facade; migration rules reject
    regressions across all narrowed files.
  - Must preserve: admin handler config, bucket metadata, site replication,
    tier, rebalance, metrics, heal, quota, and object-zip behavior; app bucket,
    object, multipart, admin, lifecycle transition, quota, object-lock, and
    replication usecase behavior.
  - Verification: RustFS test-target compile coverage, narrowed local
    compatibility glob-export scan, migration and layer guards, formatting,
    diff hygiene, Rust risk scan, pre-commit quality gate, and three-expert
    review.
- [x] `API-105` Guard root compatibility facade aliases.
  - Completed slice: route the S3 API storage compatibility ETag helper through
    a local ECStore client module alias and add a repository-wide storage
    compatibility guard against scattered raw ECStore facade paths.
  - Acceptance: storage compatibility boundaries may import ECStore facade
    modules as local `ecstore_*` aliases, but no compatibility wrapper body or
    signature may reintroduce a scattered raw `rustfs_ecstore::api::...` path.
  - Must preserve: S3 API ETag conversion behavior and all existing
    compatibility module import boundaries.
  - Verification: RustFS test-target compile coverage, full storage
    compatibility raw-facade residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-106` Split compatibility facade imports.
  - Completed slice: replace grouped `rustfs_ecstore::api::{...}` imports
    across storage compatibility boundaries with explicit per-module
    `ecstore_*` aliases and extend migration guards to reject grouped facade
    imports.
  - Acceptance: storage compatibility boundaries keep every ECStore facade
    module dependency visible as its own local alias, and wrapper bodies or
    signatures still cannot reintroduce scattered raw
    `rustfs_ecstore::api::...` paths.
  - Must preserve: all compatibility wrapper bodies, public alias names,
    storage/admin/app/runtime/edge/test/fuzz behavior, and API surface.
  - Verification: RustFS test-target compile coverage, grouped-import and
    raw-facade residual scans, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-107` Collapse compatibility facade self references.
  - Completed slice: replace crate-qualified app/admin
    `storage_compat::ecstore_*` self references with local `ecstore_*` aliases
    at the root boundary and `super::ecstore_*` paths inside nested
    compatibility modules.
  - Acceptance: RustFS app/admin compatibility boundaries no longer route
    wrapper bodies and aliases through their own crate-qualified
    `storage_compat::ecstore_*` paths; migration rules reject regressions.
  - Must preserve: app bucket/lifecycle/object-lock/replication helper
    behavior, admin bucket metadata/target/replication/tier/config helper
    behavior, public local compatibility names, and ECStore facade ownership.
  - Verification: RustFS test-target compile coverage, local facade
    self-reference residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-108` Collapse local compatibility bridge self paths.
  - Completed slice: replace crate-qualified app/admin/storage
    `storage_compat` references in local compatibility bridge modules with
    relative `super::storage_compat` paths.
  - Acceptance: RustFS local compatibility bridge modules no longer point back
    to their owner `storage_compat` facades through crate-qualified paths;
    migration rules reject regressions.
  - Must preserve: all app usecase/context, admin router/handler/service, and
    storage core/RPC compatibility re-export names and owner facade behavior.
  - Verification: RustFS test-target compile coverage, local bridge owner
    self-path residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-109` Collapse root compatibility consumer paths.
  - Completed slice: replace crate-qualified root compatibility consumers in
    startup/runtime/table/error/workload modules plus selected storage owner
    consumers with relative `super::` or `self::` paths.
  - Acceptance: selected root and storage owner modules no longer point back to
    local compatibility facades through crate-qualified paths; migration rules
    reject regressions.
  - Must preserve: startup notification/storage/background/service behavior,
    runtime capability snapshots, workload admission wiring, table catalog
    helpers, root error aliases, storage SSE/access/ECFS helper behavior, and
    public storage module aliases.
  - Verification: RustFS test-target compile coverage, root/storage owner
    compatibility consumer residual scans, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-110` Collapse RustFS local compatibility consumer paths.
  - Completed slice: replace crate-qualified app usecase, admin router, and
    storage ECFS test compatibility consumers with relative owner paths.
  - Acceptance: selected RustFS app/admin/storage consumers no longer point back
    to local compatibility facades through crate-qualified paths; migration
    rules reject regressions.
  - Must preserve: app object/bucket/multipart/admin usecase behavior,
    lifecycle transition and capacity tests, admin replication/router helpers,
    and storage ECFS test coverage.
  - Verification: RustFS test-target compile coverage, local compatibility
    consumer residual scan, migration and layer guards, formatting, diff
    hygiene, Rust risk scan, pre-commit quality gate, and three-expert review.
- [x] `API-111` Collapse storage RPC and S3 API local compatibility consumers.
  - Completed slice: replace crate-qualified storage RPC and S3 API local
    compatibility consumers with relative owner paths.
  - Acceptance: selected storage RPC and S3 API modules no longer point back to
    local compatibility facades through crate-qualified paths; migration rules
    reject regressions.
  - Must preserve: internode RPC request handling, node service helper tests,
    S3 list bucket output mapping, multipart listing output mapping, and ETag
    helper behavior.
  - Verification: RustFS test-target compile coverage, storage RPC/S3 API
    local compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-112` Collapse admin local compatibility consumers.
  - Completed slice: replace crate-qualified admin handlers/service local
    compatibility consumers with relative owner paths.
  - Acceptance: selected admin handlers and service modules no longer point back
    to local compatibility facades through crate-qualified paths; migration
    rules reject regressions.
  - Must preserve: admin route contracts, replication/config/rebalance/heal
    handler behavior, service config reload behavior, and admin test coverage.
  - Verification: RustFS test-target compile coverage, admin local
    compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-113` Collapse app context and server local compatibility consumers.
  - Completed slice: replace crate-qualified app context and server readiness
    local compatibility consumers with relative owner paths.
  - Acceptance: selected app context and server readiness modules no longer
    point back to local compatibility facades through crate-qualified paths;
    migration rules reject regressions.
  - Must preserve: app context dependency resolution, startup bootstrap,
    default interface handles, readiness storage quorum behavior, and readiness
    test coverage.
  - Verification: RustFS test-target compile coverage, app context/server local
    compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-114` Collapse config, heal, and scanner test compatibility consumers.
  - Completed slice: replace crate-qualified config test, heal crate, and
    heal/scanner integration test local compatibility consumers with relative
    owner paths.
  - Acceptance: selected config, heal, and scanner test harnesses no longer
    point back to local compatibility facades through crate-qualified paths;
    migration rules reject regressions.
  - Must preserve: config layout parsing tests, heal channel/storage test
    coverage, endpoint index tests, and scanner lifecycle integration coverage.
  - Verification: RustFS test-target compile coverage, config/heal/scanner
    local compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-115` Collapse standalone crate local compatibility consumers.
  - Completed slice: replace crate-qualified scanner, IAM, observability,
    S3 Select, and e2e local compatibility consumers with relative owner paths.
  - Acceptance: selected standalone crate modules no longer point back to their
    local compatibility facades through crate-qualified paths; migration rules
    reject regressions.
  - Must preserve: scanner data usage and object IO behavior, IAM storage
    adapter contracts, observability metric collection, S3 Select object-store
    reads, and e2e RPC helper coverage.
  - Verification: standalone crate compile coverage, standalone local
    compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, pre-commit quality gate, and
    three-expert review.
- [x] `API-116` Collapse fuzz-target local compatibility consumers.
  - Completed slice: replace crate-qualified bucket-validation and
    path-containment fuzz-target local compatibility consumers with relative
    owner paths.
  - Acceptance: selected fuzz targets no longer point back to their local
    compatibility facades through crate-qualified paths; migration rules reject
    regressions.
  - Must preserve: fuzz harness entrypoints, corpus behavior, bucket/object
    validation coverage, and path-containment assertions.
  - Verification: fuzz package compile coverage, fuzz-target local
    compatibility consumer residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, and three-expert review.
- [x] `API-117` Remove app/admin secondary compatibility bridges.
  - Completed slice: replace app use-case and admin router consumers of
    `usecase_storage_compat` and `router_storage_compat` with direct owner
    `storage_compat` paths, then delete the secondary bridge modules.
  - Acceptance: app use-cases, app tests, and the admin router no longer route
    through a second local compatibility bridge; migration rules reject
    reintroduced bridge names.
  - Must preserve: app object/bucket/multipart use-case behavior, lifecycle
    transition test setup, admin route replication/bucket-target contracts, and
    existing owner `storage_compat` aliases.
  - Verification: RustFS compile coverage, app/admin secondary bridge residual
    scan, migration and layer guards, formatting, diff hygiene, Rust risk scan,
    and three-expert review.
- [x] `API-118` Remove storage core secondary compatibility bridge.
  - Completed slice: replace storage owner consumers of `core_storage_compat`
    with direct `storage_compat` paths, then delete the secondary bridge module.
  - Acceptance: storage owner modules and tests no longer route through a
    second local compatibility bridge; migration rules reject reintroduced
    `core_storage_compat` references.
  - Must preserve: ECFS object operations, storage access checks, SSE
    encryption helpers, storage option resolution, and existing owner
    `storage_compat` aliases.
  - Verification: RustFS compile coverage, storage secondary bridge residual
    scan, migration and layer guards, formatting, diff hygiene, path-only risk
    review, and three-expert review.
- [x] `API-119` Remove nested secondary compatibility bridges.
  - Completed slice: replace admin service, app context, and storage RPC
    consumers of nested `storage_compat` bridge modules with direct owner
    `storage_compat` paths, then delete the nested bridge modules.
  - Acceptance: nested service, context, and RPC modules no longer route
    through a second local compatibility bridge; migration rules reject
    reintroduced bridge files or module declarations.
  - Must preserve: admin dynamic config and site-replication behavior, app
    context handle wiring, storage RPC signature and disk lookup behavior, and
    existing owner `storage_compat` aliases.
  - Verification: RustFS compile coverage, nested secondary bridge residual
    scan, migration and layer guards, formatting, diff hygiene, path-only risk
    review, and three-expert review.
- [x] `API-120` Remove admin handlers secondary compatibility bridge.
  - Completed slice: replace admin handler consumers of
    `handlers::storage_compat` with direct admin owner `storage_compat` paths,
    then delete the handler bridge module.
  - Acceptance: admin handler modules no longer route through a second local
    compatibility bridge; migration rules reject the bridge file, module
    declaration, or direct handler-level `super::storage_compat` consumers.
  - Must preserve: admin handler config, replication, rebalance, quota, tier,
    table catalog, metrics, trace, and heal behavior plus existing admin owner
    `storage_compat` aliases.
  - Verification: RustFS admin handler compile coverage, handler secondary
    bridge residual scan, migration and layer guards, formatting, diff hygiene,
    path-only risk review, and three-expert review.
- [x] `API-121` Remove runtime local compatibility bridges.
  - Completed slice: replace capacity, server, and S3 API local compatibility
    bridge consumers with direct owner APIs, then delete the bridge modules.
  - Acceptance: capacity, server, and S3 API modules no longer route through
    local `storage_compat` bridges; migration rules reject bridge files, module
    declarations, or bridge consumers.
  - Must preserve: capacity disk discovery, HTTP RPC signature verification,
    event dispatch hook wiring, module-switch persistence, readiness quorum
    checks, and S3 ETag conversion behavior.
  - Verification: RustFS compile coverage, runtime local bridge residual scan,
    migration and layer guards, formatting, diff hygiene, path-only risk
    review, and three-expert review.
- [x] `API-122` Remove root one-off compatibility bridges.
  - Completed slice: replace config test, error mapping, runtime capability,
    table catalog, and workload admission consumers with direct ECStore API
    imports, then delete the root one-off bridge modules.
  - Acceptance: the deleted bridge files and module declarations are gone;
    migration rules reject reintroduced files, declarations, or bridge
    references.
  - Must preserve: config disk-layout tests, API error mapping, runtime
    topology snapshots, table-catalog paths and lock behavior, and workload
    admission snapshots.
  - Verification: RustFS compile coverage, root one-off bridge residual scan,
    migration and layer guards, formatting, diff hygiene, Rust risk scan, and
    three-expert review.
- [x] `API-123` Remove startup storage compatibility bridge.
  - Completed slice: replace startup storage, notification, bucket metadata,
    service, shutdown, server, lifecycle, IAM, background, fs guard, and init
    consumers with direct ECStore API owner imports, then delete
    `startup_storage_compat.rs`.
  - Acceptance: startup/init consumers no longer route through the startup
    compatibility bridge; migration rules reject the deleted file, module
    declaration, or bridge references.
  - Must preserve: endpoint parsing, unsupported filesystem policy, ECStore
    initialization, global endpoint/erasure registration, local disk and lock
    client initialization, config migration/retry behavior, metadata/IAM
    migration, notification startup, background replication, scanner/heal
    startup and shutdown, and readiness marking.
  - Verification: RustFS compile coverage, startup bridge residual scan,
    migration and layer guards, formatting, diff hygiene, Rust risk scan, and
    three-expert review.
- [x] `API-124` Remove test and fuzz storage compatibility bridges.
  - Completed slice: replace heal tests, scanner lifecycle tests, and bucket/path
    fuzz targets with direct ECStore API owner imports, then delete their local
    `storage_compat.rs` modules.
  - Acceptance: migrated test/fuzz targets no longer route through local
    storage compatibility bridges; migration rules reject deleted files, module
    declarations, or bridge references.
  - Must preserve: heal endpoint indexing, heal mock storage signatures,
    lifecycle metadata updates, scanner warm-tier mocks, fuzz target validation
    invariants, and direct compile coverage for affected crates/targets.
  - Verification: heal/scanner test compile coverage, fuzz target compile
    coverage, test/fuzz bridge residual scan, migration and layer guards,
    formatting, diff hygiene, Rust risk scan, and three-expert review.
- [x] `API-125` Remove standalone thin compatibility bridges.
  - Completed slice: replace e2e tests, IAM store object access, and notify
    config persistence consumers with direct owner APIs, then delete their local
    `storage_compat.rs` bridge modules.
  - Acceptance: e2e, IAM store, and notify no longer route through local thin
    storage compatibility bridges; migration rules reject deleted files, module
    declarations, or bridge consumers.
  - Must preserve: e2e RPC client behavior, site-replication target contracts,
    IAM object associated types, notify server-config read/modify/save behavior,
    and reload-if-changed semantics.
  - Verification: affected crate compile coverage, standalone thin bridge
    residual scan, migration and layer guards, formatting, diff hygiene, Rust
    risk scan, and three-expert review.
- [x] `API-126` Remove remaining standalone owner compatibility bridges.
  - Completed slice: replace OBS metrics, Swift object/container/account, and
    S3 Select object-store consumers with direct owner APIs, then delete their
    local `storage_compat.rs` bridge modules.
  - Acceptance: OBS, Swift, and S3 Select no longer route through local thin
    storage compatibility bridges; migration rules reject deleted files, module
    declarations, or bridge consumers.
  - Must preserve: OBS capacity, bucket usage, replication, and ILM metrics;
    Swift bucket metadata and object IO contracts; S3 Select object reader,
    error mapping, and default read-buffer behavior.
  - Verification: affected crate compile coverage, remaining standalone bridge
    residual scan, migration and layer guards, formatting, diff hygiene, Rust
    risk scan, and three-expert review.
- [x] `API-127` Remove external owner compatibility bridges.
  - Completed slice: move IAM root, heal, and scanner bridge contracts into
    their owner modules, delete their local `storage_compat.rs` bridge modules,
    and update consumers to import owner APIs directly.
  - Acceptance: IAM root, heal, and scanner no longer route through local
    storage compatibility bridges; migration rules reject deleted files, module
    declarations, or bridge consumers.
  - Must preserve: IAM config object IO and notification wrappers, heal disk
    extension behavior and object aliases, scanner lifecycle/replication/disk
    wrappers, data-usage persistence, and scanner object IO contracts.
  - Verification: focused IAM/heal/scanner compile coverage, external owner
    bridge residual scan, migration and layer guards, formatting, diff hygiene,
    Rust risk scan, and three-expert review.
- [x] `API-128` Remove RustFS owner compatibility bridges.
  - Completed slice: move app, admin, and storage bridge contracts into their
    owner modules, delete their local `storage_compat.rs` bridge modules, and
    update consumers to import owner APIs directly.
  - Acceptance: RustFS app, admin, and storage no longer route through local
    storage compatibility bridges; migration rules reject deleted files, module
    declarations, or bridge consumers.
  - Must preserve: app object/multipart/bucket behavior, admin route and
    config contracts, storage access/SSE/RPC behavior, object DTO aliases,
    bucket metadata helpers, and ECStore facade ownership.
  - Verification: focused RustFS compile coverage, RustFS owner bridge
    residual scan, migration and layer guards, formatting, diff hygiene, Rust
    risk scan, and three-expert review.
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

- [x] `API-059/R-019` Expose RustFS runtime owner admission snapshots.
  - Completed slice: extend the RustFS workload admission provider to map
    foreground-read disk permit state, scanner active work units, and bucket
    metadata runtime initialization into the workload registry.
  - Acceptance: RustFS-level workload admission snapshots expose existing
    foreground-read, scanner, and metadata owner state without changing
    admission, queueing, scanner scheduling, metadata loading, metadata locks,
    or object write behavior.
  - Must preserve: disk-read semaphore acquisition, scanner cycle scheduling,
    bucket metadata initialization and loading, object write paths, request
    guards, and queue behavior.
  - Verification: focused workload admission tests, focused RustFS library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

- [x] `API-060` Remove heal and namespace-lock operation compatibility facades.
  - Completed slice: remove the old ECStore `store_api::HealOperations` and
    `store_api::NamespaceLocking` compatibility subtraits after ECStore storage
    types already implemented the shared `rustfs_storage_api` contracts
    directly.
  - Acceptance: internal ECStore bounds and compile-time coverage use the
    shared storage-api heal and namespace-lock contracts directly, while the
    remaining object/list/multipart compatibility bindings stay unchanged for
    their active internal consumers.
  - Must preserve: heal operation behavior, namespace-lock acquisition,
    replication resync locking, rebalance metadata locking, object I/O,
    multipart, list, and storage hot paths.
  - Verification: focused ECStore contract tests, focused ECStore library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

- [x] `API-061` Remove public ECStore object operation compatibility facades.
  - Completed slice: remove the old public ECStore `store_api` object, list,
    and multipart operation compatibility subtraits, and keep internal generic
    bounds on crate-private storage-api contract constraints instead of public
    downstream compatibility traits.
  - Acceptance: `store_api` no longer exports public operation compatibility
    traits, ECStore direct storage-api compile-time coverage includes object,
    object-operation, list, multipart, namespace-lock, heal, and admin
    contracts, and remaining public `store_api` exports are DTO/reader
    compatibility paths only.
  - Must preserve: object I/O, list/walk behavior, multipart behavior, config
    persistence, tier config migration, rebalance metadata locking, lifecycle
    journal handling, replication MRF/resync persistence, and downstream DTO
    import compatibility.
  - Verification: focused ECStore contract tests, focused ECStore library
    check, migration and layer guards, formatting, diff hygiene, risk scan, and
    three-expert review.

- [x] `API-062` Establish explicit ECStore object API boundary.
  - Completed slice: add `rustfs_ecstore::object_api` as the explicit public
    path for ECStore-owned object DTO and reader contracts, then migrate
    RustFS, scanner, heal, IAM, Swift, S3 Select, notify, and ECStore
    integration-test compatibility aliases away from the legacy public
    `store_api` path.
  - Acceptance: external compatibility boundary modules no longer reference
    `rustfs_ecstore::store_api` for ECStore-owned object DTO and reader
    aliases, while `store_api` remains available only as the old internal
    implementation module pending final compatibility removal.
  - Must preserve: object metadata shape, option defaults, reader/writer
    behavior, Swift/scanner/heal/IAM/S3 Select/notify boundary semantics, and
    all storage hot paths.
  - Verification: focused ECStore/RustFS/downstream compile checks, migration
    guard, formatting, diff hygiene, risk scan, and three-expert review.

- [x] `API-063` Make legacy ECStore store API module private.
  - Completed slice: remove `rustfs_ecstore::store_api` from the public crate
    module surface after external compatibility boundaries moved to
    `rustfs_ecstore::object_api`.
  - Acceptance: ECStore object DTO and reader compatibility remains available
    through `object_api`, integration contract tests consume the new public
    path, and migration rules reject restoring `pub mod store_api`.
  - Must preserve: internal ECStore object DTO definitions, reader/writer
    behavior, storage-api trait bindings, and downstream object/list/multipart
    compile-time contracts.
  - Verification: focused ECStore contract tests, migration guard, formatting,
    diff hygiene, risk scan, and three-expert review.

- [x] `API-064` Retire the ECStore store API module name.
  - Completed slice: move ECStore object DTO, reader, and option definitions
    from the private `store_api` module into the public `object_api` module,
    then migrate ECStore internal imports to `crate::object_api`.
  - Acceptance: no ECStore `store_api` module file or directory remains, public
    consumers keep using `rustfs_ecstore::object_api`, and migration rules
    reject restoring the retired module path.
  - Must preserve: object metadata shape, reader/writer behavior, storage-api
    contract bindings, object/list/multipart behavior, and downstream public
    object API compatibility.
  - Verification: focused ECStore compile checks, migration guard, formatting,
    diff hygiene, risk scan, and three-expert review.

- [x] `API-065` Use storage-api list contracts inside ECStore.
  - Completed slice: migrate ECStore internal list response, walk options, and
    walk result bindings to local aliases over the generic `rustfs-storage-api`
    contracts, including replication worker trait bounds, while retaining
    public `rustfs_ecstore::object_api` aliases for downstream compatibility.
  - Acceptance: ECStore implementation modules no longer import list/walk
    compatibility aliases from `crate::object_api`, and migration rules reject
    reintroducing those internal imports.
  - Must preserve: list response shape, walk result item shape, object metadata
    shape, storage-api trait bindings, and downstream public object API
    compatibility.
  - Verification: focused ECStore compile checks, migration guard, formatting,
    diff hygiene, risk scan, and three-expert review.

- [x] `API-066` Prune ECStore object API storage aliases.
  - Completed slice: remove unused public storage-api passthrough aliases from
    ECStore `object_api` for list responses, walk options, walk result items,
    and delete-object DTOs, then bind the ECStore contract test directly to the
    generic `rustfs-storage-api` contracts.
  - Acceptance: ECStore `object_api` no longer exposes storage-api passthrough
    aliases, the storage contract test still proves ECStore implements the
    storage-api traits with the same associated concrete types, and migration
    rules reject restoring the object_api passthrough aliases.
  - Must preserve: ECStore-owned object metadata, object options, reader/writer
    types, storage-api trait associated type bindings, list/delete/walk response
    shapes, and runtime behavior.
  - Verification: focused ECStore compile checks, storage contract test,
    downstream compile checks, migration and layer guards, formatting, diff
    hygiene, risk scan, full pre-commit, and three-expert review.

- [x] `API-067` Guard remaining external ECStore object API aliases.
  - Completed slice: add a migration guard that snapshots the exact external
    `storage_compat.rs` aliases still allowed to reference
    `rustfs_ecstore::object_api::{GetObjectReader,ObjectInfo,ObjectOptions,PutObjReader}`
    and rejects new object-api names in compatibility boundaries.
  - Acceptance: all remaining external `object_api` references are deliberate
    compatibility aliases in `storage_compat.rs` modules, future additions fail
    the migration guard, and the API-066 passthrough alias cleanup stays
    protected.
  - Must preserve: no runtime code changes, all existing compatibility aliases,
    object metadata shape, options, and reader/writer ownership.
  - Verification: bash syntax check, migration and layer guards, formatting,
    diff hygiene, full pre-commit, and three-expert review.

- [x] `API-068` Prune notify ECStore object-info compatibility alias.
  - Completed slice: remove notify's private `EcstoreObjectInfo` alias and
    ECStore-object conversion implementation, then map ECStore event objects to
    `NotifyObjectInfo` inside the RustFS event and operation notification
    bridges.
  - Acceptance: `crates/notify` no longer references
    `rustfs_ecstore::object_api::ObjectInfo`, the remaining object-api alias
    allowlist shrinks accordingly, and notify event payload fields keep the
    same serialized values.
  - Must preserve: live event dispatch behavior, event names, bucket/object
    fields, version IDs, metadata, restore-completed timestamps, storage class,
    transitioned tier, host/port parsing, and replication request filtering.
  - Verification: focused RustFS event conversion test, focused notify/RustFS
    compile checks, migration and layer guards, formatting, diff hygiene, full
    pre-commit, and three-expert review.

- [x] `API-069` Prune IAM direct ECStore object metadata/options aliases.
  - Completed slice: replace IAM config and store `ObjectInfo`/`ObjectOptions`
    compatibility aliases with `IamStore` `ObjectOperations` associated types.
  - Acceptance: IAM no longer names
    `rustfs_ecstore::object_api::{ObjectInfo,ObjectOptions}` directly, the
    remaining object-api alias allowlist shrinks by four entries, and IAM config
    read/write metadata and lazy-rewrite precondition behavior are unchanged.
  - Must preserve: IAM config encryption/decryption, lazy rewrite ETag matching,
    list walk item/error typing, metadata return shape, storage preconditions,
    system-path failure classification, and notification peer behavior.
  - Verification: focused IAM compile/tests, migration and layer guards,
    formatting, diff hygiene, full pre-commit, and three-expert review.

- [x] `API-070` Prune consumer direct ECStore object aliases.
  - Completed slice: replace scanner, s3select, and Swift
    `GetObjectReader`/`ObjectInfo`/`ObjectOptions`/`PutObjReader`
    compatibility aliases with concrete store `rustfs_storage_api` associated
    types.
  - Acceptance: scanner, s3select, and Swift no longer name
    `rustfs_ecstore::object_api::{GetObjectReader,ObjectInfo,ObjectOptions,PutObjReader}`
    directly, and the remaining object-api alias allowlist shrinks by eleven
    entries.
  - Must preserve: scanner lifecycle/replication IO bounds and config helpers,
    s3select read buffer/object error handling, Swift bucket metadata helpers,
    and object reader/writer concrete types exposed through each local
    compatibility boundary.
  - Verification: focused consumer compile/tests, migration and layer guards,
    formatting, diff hygiene, full pre-commit, and three-expert review.

- [x] `API-071` Prune final direct ECStore object aliases.
  - Completed slice: replace heal and RustFS storage
    `GetObjectReader`/`ObjectInfo`/`ObjectOptions`/`PutObjReader`
    compatibility aliases with concrete store `rustfs_storage_api` associated
    types.
  - Acceptance: no external `storage_compat.rs` module names
    `rustfs_ecstore::object_api::{GetObjectReader,ObjectInfo,ObjectOptions,PutObjReader}`
    directly, and the external object-api alias allowlist is empty.
  - Must preserve: heal object metadata and rewrite reader construction,
    RustFS storage object read/write paths, S3 response metadata semantics,
    SSE/encryption handling, and storage object option behavior.
  - Verification: focused heal/storage compile/tests, migration and layer
    guards, formatting, diff hygiene, full pre-commit, and three-expert review.

- [x] `API-072` Establish ECStore public facade for outer compatibility.
  - Completed slice: add `rustfs_ecstore::api` facade groups for layout,
    storage, admin, metrics, notification, and capacity helper surfaces, then
    migrate RustFS, scanner, observability, IAM, heal, Swift, S3 Select,
    heal-test, and scanner-test compatibility boundaries away from direct
    ECStore module paths for those surfaces.
  - Acceptance: selected outer `storage_compat.rs` boundaries no longer import
    `rustfs_ecstore::{admin_server_info,endpoints,disks_layout,metrics_realtime,notification_sys,pools,store_utils,store}`
    directly, and the migration guard rejects restoring those direct public
    surface paths.
  - Must preserve: endpoint and disks-layout types, ECStore owner type and
    init helpers, admin server-info helpers, local metrics collection,
    notification peer behavior, capacity helpers, bucket-name helpers, and all
    runtime storage behavior.
  - Verification: affected package test-target compile, migration and layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `API-073` Expand ECStore public facade coverage.
  - Completed slice: add `rustfs_ecstore::api` facade groups for bucket,
    config, disk, error, event, global, RPC, set-disk, reader, client, tier,
    data-usage, cache, compression, and rebalance compatibility surfaces, then
    migrate all outer `storage_compat.rs` boundaries to those facade paths.
  - Acceptance: RustFS, app/admin/storage runtime, scanner, heal, IAM, notify,
    observability, Swift, S3 Select, e2e, test, and fuzz compatibility
    boundaries no longer import those ECStore public surfaces through direct
    pre-facade module paths, and the migration guard rejects restoring them.
  - Must preserve: storage owner types, config IO, bucket metadata/lifecycle
    helpers, disk/RPC/error contracts, global state accessors, reader wrappers,
    tier helpers, rebalance status DTOs, test/fuzz harness behavior, and all
    existing runtime behavior.
  - Verification: affected package test-target compile, migration and layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `API-074` Enforce ECStore API facade for compatibility boundaries.
  - Completed slice: extend the architecture migration guard so every
    non-ECStore `storage_compat.rs` import from `rustfs_ecstore` must route
    through `rustfs_ecstore::api`, not only the previously enumerated public
    ECStore module paths.
  - Acceptance: RustFS, app/admin/storage runtime, scanner, heal, IAM, notify,
    observability, Swift, S3 Select, e2e, test, and fuzz compatibility
    boundaries cannot reintroduce direct pre-facade ECStore paths through new
    modules or grouped imports.
  - Must preserve: no runtime behavior, type ownership, compatibility alias, or
    ECStore public facade behavior changes.
  - Verification: migration guard, direct old-path scan, formatting, diff
    hygiene, branch freshness check, pre-commit quality gate, and three-expert
    review.

- [x] `API-075` Prune ECStore legacy layout root modules.
  - Completed slice: make the legacy ECStore root `endpoints` and
    `disks_layout` compatibility modules crate-private now that outer
    compatibility boundaries use `rustfs_ecstore::api::layout`.
  - Acceptance: `rustfs_ecstore::api::layout` remains the public facade for
    endpoint pools and disk layout helpers, while migration rules reject
    restoring the old root layout compatibility modules as public modules.
  - Must preserve: endpoint layout types, disk layout helper behavior, ECStore
    internal call sites, and all outer compatibility facade paths.
  - Verification: ECStore and affected outer package compile, migration and
    layer guards, formatting, diff hygiene, Rust risk scan, branch freshness
    check, pre-commit quality gate, and three-expert review.

- [x] `API-076` Prune facade-covered ECStore root modules.
  - Completed slice: make facade-covered legacy ECStore root modules
    crate-private after all in-repo outer compatibility boundaries route
    through `rustfs_ecstore::api`.
  - Acceptance: `rustfs_ecstore::api::*` remains the public facade for storage,
    admin, config, metrics, notification, RPC, disk, error, tier, rebalance,
    and layout helper surfaces, while migration rules reject restoring those
    legacy root modules as public modules.
  - Must preserve: ECStore internal module access, public `api` facade paths,
    object API paths, bitrot and erasure coding test/bench paths, and storage
    contract compatibility tests.
  - Verification: ECStore and affected outer package compile, migration and
    layer guards, formatting, diff hygiene, Rust risk scan, branch freshness
    check, pre-commit quality gate, and three-expert review.

- [x] `API-077` Prune remaining ECStore root compatibility modules.
  - Completed slice: add explicit `rustfs_ecstore::api` facade groups for
    bitrot, erasure coding, object DTO/reader, event name, and store-list
    helper surfaces, then migrate ECStore tests and benches away from the
    legacy root module paths.
  - Acceptance: `batch_processor`, `bitrot`, `erasure_coding`, `event`,
    `object_api`, and `store_list_objects` are no longer public ECStore root
    modules, and the migration guard rejects restoring them as public modules.
  - Must preserve: ECStore internal module access, public facade access for
    compatibility tests/benches, bitrot reader/writer behavior, erasure coding
    constructors/helpers, object reader/DTO wire shape, and list option
    semantics.
  - Verification: migration guard, ECStore compatibility tests/benches compile
    coverage, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `API-078` Prune ECStore root global re-exports.
  - Completed slice: remove the remaining `pub use global::*` compatibility
    exports from the ECStore crate root and route internal ECStore users to
    `crate::global` directly.
  - Acceptance: outer access to ECStore global helpers remains available only
    through `rustfs_ecstore::api::global`, internal ECStore modules use the
    real owner path, and the migration guard rejects restoring root global
    re-exports.
  - Must preserve: object-store resolver behavior, endpoint/global lock client
    publication, erasure-type updates, tier/notification/data-usage metadata
    loading, and existing `api::global` facade names.
  - Verification: migration guard, ECStore and RustFS compile coverage,
    formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

## Phase 5 Cluster Control Plane Tasks

- [x] `C-001` Add topology model.
  - Completed slice: move endpoint-pool topology mapping behind ECStore's
    crate-private `cluster` owner module and expose it through
    `rustfs_ecstore::api::cluster`.
  - Acceptance: pool, set, and disk topology snapshots are built from existing
    endpoint assignments without exposing local disk paths or changing
    placement, readiness, or endpoint construction.
  - Must preserve: endpoint pool/set/disk indexes, local path privacy,
    storage-api topology contract shape, runtime capability reasons, and
    existing RustFS topology provider behavior.
  - Verification: ECStore topology tests, RustFS runtime topology tests,
    migration guard, compile coverage, formatting, diff hygiene, risk scan,
    pre-commit quality gate, and three-expert review.

- [x] `C-002` Add membership model.
  - Completed slice: add a static membership snapshot that groups endpoint
    drives by node identity and records drive placement without dynamic
    membership, health checks, control RPC, or hot-path changes.
  - Acceptance: URL endpoints group by host:port, path endpoints group under a
    local node identity, and drive membership carries pool/set/disk placement
    plus endpoint type/local flags.
  - Must preserve: no Raft, no Kubernetes watcher, no peer-health behavior, no
    dynamic membership, and no object I/O or lock-quorum behavior changes.
  - Verification: ECStore membership tests, compile coverage, migration guard,
    formatting, diff hygiene, risk scan, pre-commit quality gate, and
    three-expert review.

- [x] `C-003` Add read-only control plane facade.
  - Completed slice: add `ClusterControlPlane` as a read-only facade that
    combines topology and membership snapshots from existing endpoint pools.
  - Acceptance: outer crates use `rustfs_ecstore::api::cluster` for the facade,
    while ECStore root `cluster` remains crate-private and migration rules
    reject restoring it as a public root module.
  - Must preserve: no worker start/stop, health impact, lock registry mutation,
    pool state mutation, endpoint publication, or readiness behavior changes.
  - Verification: control-plane read-snapshot test, migration guard, compile
    coverage, formatting, diff hygiene, risk scan, pre-commit quality gate, and
    three-expert review.

- [x] `C-004` Add pool state snapshot.
  - Completed slice: add a static pool-state snapshot derived from existing
    endpoint pools and expose it through `rustfs_ecstore::api::cluster`.
  - Acceptance: pool state records pool index, set count, drives per set,
    endpoint counts, local/remote drive counts, legacy flag, and endpoint type
    coverage without reading disks or changing pool ownership.
  - Must preserve: no placement change, no pool mutation, no command-line path
    exposure, and no endpoint publication changes.
  - Verification: ECStore pool-state tests, compile coverage, formatting, diff
    hygiene, risk scan, pre-commit quality gate, and three-expert review.

- [x] `C-005` Add local-node storage snapshot.
  - Completed slice: add a read-only local-node storage projection from static
    endpoint membership.
  - Acceptance: local nodes include only local membership entries and report
    aggregate path/url drive counts and pool coverage without exposing local
    disk paths.
  - Must preserve: no storage readiness, disk health, lock quorum, or object I/O
    behavior changes.
  - Verification: ECStore local-node storage tests, compile coverage,
    formatting, diff hygiene, risk scan, pre-commit quality gate, and
    three-expert review.

- [x] `C-006` Add peer health snapshot.
  - Completed slice: add a static peer-health read model that reports peer
    identities from membership with unknown health status until real peer health
    wiring lands.
  - Acceptance: peer health is explicitly unknown and read-only; no background
    health checks, RPC calls, timers, or failure-state mutation are introduced.
  - Must preserve: no dynamic membership, no peer health loop, no control RPC,
    no readiness impact, and no lock/object behavior changes.
  - Verification: ECStore peer-health tests, compile coverage, formatting, diff
    hygiene, risk scan, pre-commit quality gate, and three-expert review.

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

- [x] `R-020` Isolate profiling lifecycle hooks.
  - Do: route BOOT-006 profiling initialization and STOP-004 profiling shutdown
    through `startup_profiling` hook functions while keeping `profiling.rs` as
    the CPU/memory profiling implementation and admin dump API owner.
  - Acceptance: startup still initializes profiling before trusted proxies and
    outbound TLS material; shutdown still stops profiling after notifier/audit
    shutdown and before HTTP shutdown; unsupported targets and disabled
    profiling keep their existing no-op behavior.
  - Must preserve: profiling env flags, CPU/memory mode handling, target gates,
    cancellation-token ownership, admin pprof routes, non-fatal startup
    behavior, and shutdown ordering.
  - Verification: focused startup profiling hook tests, binary/lib compile
    checks, formatting, migration guards, Rust risk scan, branch freshness
    check, and pre-commit quality gate.

- [x] `X-012` Define ops profiler extension schema contract.
  - Do: add `ops.profiler.v1` capability DTOs for profiler backend status,
    capability-description mode, profile export redaction requirements, and
    provenance in the extension schema contract crate.
  - Acceptance: disabled, unsupported, enabled, and unknown backend states are
    representable; execution requests are rejected; profile export declarations
    require local path redaction; provenance records source, collection
    boundary, and trust level without credentials.
  - Must preserve: no plugin execution, no sidecar startup, no profile route or
    admin API behavior changes, no exporter/storage/object-path/telemetry
    behavior changes, and no dependency edge from `extension-schema` to
    implementation crates.
  - Verification: extension schema check/tests, formatting, migration/layer
    guards, diff hygiene, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `X-013` Add ops profiler capability snapshot contract.
  - Do: add `OpsProfilerCapabilitySnapshot` and `OpsProfilerRuntimeSnapshot`
    DTOs plus validation for the `ops.profiler.v1` capability, disabled
    external runtimes, and non-fatal profiler startup behavior.
  - Acceptance: disabled, unsupported, and enabled profiler backend states
    round-trip through the snapshot contract; sidecar/Wasm profiler runtimes
    remain disabled by default; profiler snapshots cannot declare a startup
    fatal boundary.
  - Must preserve: no plugin execution, no sidecar startup, no profile route,
    no admin API behavior changes, no runtime startup/shutdown behavior
    changes, and no dependency edge from `extension-schema` to runtime or
    storage implementation crates.
  - Verification: extension schema check/tests, formatting, migration/layer
    guards, diff hygiene, Rust risk scan, branch freshness check, and
    pre-commit quality gate.

- [x] `R-021` Extract optional runtime shutdown boundary.
  - Do: add `startup_optional_runtimes` and move optional protocol shutdown
    ownership/logging out of `startup_services`.
  - Acceptance: optional protocol shutdown plan order stays FTP, FTPS, WebDAV,
    SFTP; stopping logs remain before event notifier/audit/profiling shutdown;
    signal/wait remains after S3/console HTTP shutdown; later optional
    sidecars have an explicit owner without startup behavior changes.
  - Must preserve: protocol initialization, protocol shutdown signaling and
    waiting, shutdown order, profiling/audit/event notifier shutdown, HTTP
    shutdown, readiness state, and fatal boundaries.
  - Verification: focused startup optional runtime/service tests, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-022` Extract optional runtime startup boundary.
  - Do: add `init_optional_runtime_services` so optional protocol startup is
    owned by `startup_optional_runtimes`, while `startup_protocols` remains the
    protocol implementation adapter.
  - Acceptance: optional protocol startup order stays FTP, FTPS, WebDAV, SFTP;
    KMS initialization still happens before optional protocol startup; buffer
    profiling, audit, deadlock detection, metadata, IAM, notification, scanner,
    heal, and observability startup remain after optional protocol startup.
  - Must preserve: protocol feature gates, disabled protocol behavior,
    protocol startup error mapping, fatal boundary on protocol startup errors,
    startup order, shutdown order, readiness state, and runtime behavior.
  - Verification: focused optional runtime/protocol/startup service tests,
    RustFS lib check, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `R-023` Extract startup shutdown lifecycle boundary.
  - Do: add `startup_shutdown` and move runtime token cancellation, service
    state transitions, background shutdown, notifier/audit/profiling shutdown,
    HTTP shutdown, and optional runtime wait sequencing out of
    `startup_services`.
  - Acceptance: shutdown order stays runtime token cancellation, `Stopping`
    state, scanner/AHM shutdown, optional runtime shutdown planning,
    notifier/audit/profiling shutdown, S3 and console HTTP shutdown, optional
    runtime waits, then `Stopped` state.
  - Must preserve: service state transitions, readiness state behavior,
    scanner/heal enable flag handling, notifier/audit/profiling shutdown logs,
    HTTP shutdown ordering, optional protocol shutdown ordering, and fatal
    boundaries.
  - Verification: focused shutdown/service/optional runtime tests, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-024` Extract startup ready lifecycle boundary.
  - Do: add `startup_lifecycle` and move ready publication, global init time,
    scanner startup, shutdown-signal wait, shutdown delegation, and final
    stopped-state logging out of `startup_services`.
  - Acceptance: lifecycle order stays server-ready log, IAM readiness
    publication, global init time, optional scanner startup, shutdown wait,
    shutdown sequence delegation, and final stopped log.
  - Must preserve: inline/deferred IAM readiness behavior, scanner start timing,
    global init-time timing, shutdown signal wait semantics, shutdown ordering,
    service state reporting, and fatal boundary on readiness publication.
  - Verification: focused lifecycle/service/shutdown tests, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-025` Extract startup service component boundary.
  - Do: add `startup_service_components` and move audit/deadlock, bucket
    metadata, IAM bootstrap, auth integration, notification, background service,
    and observability component helpers out of `startup_services`.
  - Acceptance: `startup_services` keeps the same runtime service orchestration
    order while component helpers own the individual service startup side
    effects.
  - Must preserve: KMS before optional runtime startup, buffer profiling before
    audit, event notifier before audit, bucket metadata before IAM, IAM before
    auth and notification, notification before background services, and
    observability startup after background service setup.
  - Verification: focused startup service component/service/lifecycle tests,
    RustFS lib check, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `R-026` Extract optional runtime sidecar boundary.
  - Do: add `startup_optional_runtime_sidecars` and move optional runtime
    sidecar ownership, shutdown planning, shutdown execution, and protocol
    shutdown order tests out of `startup_optional_runtimes`.
  - Acceptance: optional protocol startup still happens after KMS and before
    buffer profiling, while shutdown planning still records FTP, FTPS, WebDAV,
    then SFTP handles before later shutdown signaling.
  - Must preserve: feature-gated protocol startup behavior, disabled-protocol
    handling, protocol shutdown ordering, HTTP shutdown before optional protocol
    shutdown signaling, and the compatibility `startup_optional_runtimes` API.
  - Verification: focused optional runtime sidecar/runtime/shutdown tests,
    RustFS lib check, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `R-027` Extract startup runtime hook boundary.
  - Do: add `startup_runtime_hooks` and move startup runtime diagnostics,
    profiling hook dispatch, shutdown profiling dispatch, and default crypto
    provider installation out of `startup_runtime` and `startup_profiling`.
  - Acceptance: BOOT-006 keeps diagnostics, profiling init, trusted proxy init,
    provider install, and outbound TLS material load in the same order, while
    STOP-004 still stops profiling through the existing compatibility path.
  - Must preserve: startup logo and telemetry/license log behavior, profiling
    hook dispatch behavior, rustls provider install behavior, trusted proxy init
    order, outbound TLS fatal boundary, and profiling shutdown call path.
  - Verification: focused runtime hook/profiling/runtime/shutdown tests, RustFS
    lib check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-028` Extract startup TLS material boundary.
  - Do: add `startup_tls_material` and move outbound TLS material loading,
    global TLS publication, generation recording, TLS metrics initialization,
    and existing TLS path/generation tests out of `startup_runtime`.
  - Acceptance: BOOT-006 keeps diagnostics, profiling init, trusted proxy init,
    provider install, and outbound TLS material load in the same order.
  - Must preserve: configured TLS material fatal behavior, TLS path trimming,
    saturating TLS generation behavior, outbound TLS global state publication,
    generation metric recording, and metrics initialization when observability
    metrics are enabled.
  - Verification: focused TLS material/runtime tests, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-029` Reuse startup phase boundaries in embedded mode.
  - Do: move embedded listen setup, endpoint/local disk setup, ECStore/global
    config setup, storage readiness publication, and replication startup behind
    startup server/storage helpers.
  - Acceptance: embedded startup keeps its stable-port requirement, global
    startup guard placement, S3-only HTTP startup, readiness publication, and
    storage initialization order while sharing the same startup phase owners.
  - Must preserve: embedded port 0 rejection, credential/region publication,
    endpoint and unsupported filesystem validation, local disk and lock client
    initialization, ECStore fatal shutdown behavior, global config retry limit,
    and embedded-specific non-fatal KMS/audit/notification behavior.
  - Verification: focused embedded/startup storage checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-030` Reuse runtime service boundaries in embedded mode.
  - Do: move embedded KMS/buffer/audit setup, bucket metadata migration, IAM
    bootstrap, notification setup, and event/audit shutdown cleanup behind
    startup service/shutdown helpers.
  - Acceptance: embedded startup keeps KMS/audit/notification failures
    non-fatal, preserves bucket metadata and IAM initialization order, and
    keeps shutdown cleanup behavior unchanged.
  - Must preserve: KMS warning-only behavior, buffer profile initialization,
    audit warning-only behavior, bucket listing failure shutdown, bucket
    metadata migration before IAM migration, IAM bootstrap fatal behavior,
    notification warning-only behavior, readiness publication, event notifier
    shutdown, audit stop warning behavior, and temp directory cleanup.
  - Verification: focused embedded/service/shutdown checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-031` Reuse lifecycle publication boundaries in embedded mode.
  - Do: move embedded IAM readiness publication, global init-time publication,
    and ready-state logging behind startup lifecycle helpers.
  - Acceptance: embedded startup still publishes readiness after runtime
    service setup, preserves the `runtime readiness` error prefix on failure,
    records global init time after successful readiness publication, and logs
    the same ready endpoint message after the server handle is built.
  - Must preserve: deferred IAM bootstrap readiness behavior, ready-inline
    runtime readiness publication, startup failure shutdown signaling, global
    init-time publication ordering, and endpoint-address normalization used by
    the ready log.
  - Verification: focused embedded/lifecycle checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-032` Publish ops profiler runtime contract boundaries.
  - Do: add the builtin ops profiler extension schema/contract to the targets
    catalog, expose it through the admin extension catalog, and add a read-only
    registry for profiler backend capability descriptions.
  - Acceptance: the catalog advertises `builtin:ops-profiler` with
    `ops.profiler.v1`, backend capability descriptions validate through the
    extension-schema contract, and registry access is admin/capability limited
    without executing profiler collection.
  - Must preserve: existing `/debug/pprof/*` admin behavior, profiling startup
    and shutdown hooks, disabled external profiler runtime defaults, local path
    redaction requirements, and no plugin execution or sidecar startup.
  - Verification: focused targets/admin extension checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-033` Expose extension runtime capability snapshots.
  - Do: add read-only diagnostics/profiler runtime capability snapshots to the
    admin extension catalog response using existing schema and contract DTOs.
  - Acceptance: `/v4/extensions/catalog` reports builtin diagnostics and
    profiler capability contracts with their runtime boundaries, disabled
    defaults, and non-fatal startup flags while preserving schema validation.
  - Must preserve: existing extension catalog route/auth, plugin instance
    listing, profiler/diagnostics execution paths, and external plugin flow
    status semantics.
  - Verification: focused admin catalog and targets runtime checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-034` Extract embedded runtime hook boundary.
  - Do: move embedded observability guard setup, default crypto provider
    installation, and trusted proxy initialization behind startup runtime hooks.
  - Acceptance: embedded startup keeps observability initialization before the
    global startup guard/listen/storage phases while sharing the runtime hook
    owner used by normal startup.
  - Must preserve: `init_obs` and `set_global_guard` error prefixes, embedded
    crypto provider already-installed debug fields, trusted proxy init timing,
    and no added embedded server runtime behavior.
  - Verification: focused embedded/runtime hook checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-035` Extract embedded shutdown glue boundary.
  - Do: move embedded async shutdown logging, cancellation, event/audit cleanup,
    HTTP shutdown, and temporary directory cleanup behind startup shutdown
    helpers.
  - Acceptance: embedded server shutdown preserves the same stopping/stopped
    logs, cancellation timing, best-effort audit cleanup, HTTP shutdown, and
    temp-dir cleanup behavior while leaving `Drop` as a synchronous best-effort
    fallback.
  - Must preserve: event notifier shutdown before audit stop, audit stop
    warning-only behavior, HTTP shutdown after background cancellation, temp
    directory cleanup warning fields, and final stopped log.
  - Verification: focused embedded/shutdown checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-036` Extract embedded startup config preparation boundary.
  - Do: move embedded temporary volume allocation, custom volume directory
    creation, and embedded `Config` construction behind startup server helpers.
  - Acceptance: embedded builder still creates a temporary volume when none is
    provided, creates missing custom volume directories, disables console for
    embedded S3 startup, and keeps the temp-dir guard alive until success.
  - Must preserve: temp-dir cleanup-on-failure behavior, configured address,
    access key, secret key, region, volume ordering, directory creation error
    text, and no new normal startup behavior.
  - Verification: focused startup server and embedded checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-037` Extract embedded S3-only HTTP startup boundary.
  - Do: move embedded S3-only HTTP server startup behind a startup server
    helper that returns the bound address and shutdown handle.
  - Acceptance: embedded startup keeps console disabled for the HTTP server,
    keeps using the same readiness object, and preserves the shutdown handle
    and bound address used by `RustFSServer`.
  - Must preserve: S3-only embedded HTTP config, readiness sharing, startup
    error propagation, shutdown signaling, bound endpoint reporting, and no
    public embedded API behavior changes.
  - Verification: focused startup server and embedded checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-038` Extract embedded process-global startup guard boundary.
  - Do: move the embedded process-global once guard behind a startup lifecycle
    helper.
  - Acceptance: embedded startup still allows retry before irreversible global
    initialization, treats repeated marks inside the same startup as idempotent,
    and rejects a second process-local embedded server after the first
    irreversible mark.
  - Must preserve: startup guard timing after runtime hooks and listen context,
    `AlreadyStarted` error mapping, no reset-after-stop behavior, and no normal
    startup behavior changes.
  - Verification: focused startup lifecycle and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-039` Extract embedded startup failure shutdown signal boundary.
  - Do: move the post-HTTP embedded startup failure shutdown signal behind a
    startup shutdown helper.
  - Acceptance: embedded startup still signals the HTTP shutdown handle and
    cancels the background token before returning initialization errors from
    storage runtime, service runtime, or readiness publication failures.
  - Must preserve: no shutdown signal before HTTP startup exists, signal-then-
    cancel ordering, `Init` error mapping, and no public embedded API behavior
    changes.
  - Verification: focused startup shutdown and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-040` Extract embedded build orchestration owner.
  - Do: move the embedded build sequence behind a crate-only startup embedded
    helper.
  - Acceptance: embedded startup still runs config preparation, runtime hooks,
    listen context, process-global guard, storage foundation, HTTP startup,
    storage runtime, runtime services, and readiness publication in the same
    order.
  - Must preserve: retry-before-global-init behavior, temp-dir guard lifetime,
    post-HTTP startup failure shutdown signaling, readiness publication error
    text, and no public embedded API behavior changes.
  - Verification: focused embedded checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-041` Keep embedded public API as handle assembly.
  - Do: keep `embedded.rs` focused on public builder inputs, `RustFSServer`
    handle construction, endpoint reporting, shutdown, and drop cleanup.
  - Acceptance: builder defaults and fluent setters still feed the same startup
    fields, server accessors still return the configured credentials and
    region, endpoint normalization stays in the public handle, and shutdown/drop
    cleanup remains unchanged.
  - Must preserve: `ServerError` variants and messages, `Io` versus `Init`
    error mapping, endpoint URL shape, shutdown handle ownership, cancellation
    token ownership, and temp-dir cleanup path.
  - Verification: focused embedded checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-042` Extract embedded endpoint normalization.
  - Do: move unspecified-address endpoint normalization into a crate-only
    startup lifecycle helper.
  - Acceptance: embedded endpoint reporting still rewrites unspecified IPv4 and
    IPv6 bind addresses to localhost while preserving concrete bound hosts.
  - Must preserve: public endpoint URL shape, `address()` returning the bound
    socket address, ready-log endpoint text, and no public embedded API
    signature changes.
  - Verification: focused startup lifecycle and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-043` Extract embedded drop cleanup boundary.
  - Do: move synchronous embedded server drop cleanup into a crate-only startup
    shutdown helper.
  - Acceptance: dropping a server still cancels the token, signals the shutdown
    handle, and best-effort removes the temporary directory.
  - Must preserve: explicit async shutdown behavior, shutdown handle ownership,
    temp-dir cleanup behavior, ignored drop cleanup errors, and no public
    embedded API signature changes.
  - Verification: focused startup shutdown and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-044` Keep embedded builder state in startup args.
  - Do: replace duplicated public builder private fields with crate-only
    embedded startup arguments while preserving the fluent builder API.
  - Acceptance: builder defaults, fluent setters, server credential accessors,
    region accessors, and startup arguments remain behaviorally unchanged.
  - Must preserve: public builder signatures, default address and credentials,
    volume replacement semantics, region publication, and error mapping.
  - Verification: focused embedded/startup-embedded checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-045` Move embedded port probing behind startup server.
  - Do: delegate public embedded available-port probing to a crate-only startup
    server helper.
  - Acceptance: `find_available_port` still returns a bindable localhost TCP
    port and preserves the same public result type.
  - Must preserve: public helper signature, localhost bind target, ephemeral
    port behavior, and no embedded startup side effects.
  - Verification: focused startup-server and embedded checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-046` Encapsulate embedded startup argument mutation.
  - Do: hide embedded startup argument fields behind crate-only setter methods
    used by the public builder.
  - Acceptance: public builder fluent methods still apply the same address,
    credential, region, and volume values in the same order.
  - Must preserve: builder method signatures, default values, `volume` append
    semantics, `volumes` replacement semantics, and startup input ownership.
  - Verification: focused startup-embedded and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-047` Return embedded server identity from startup result.
  - Do: let the crate-only startup result carry the access key, secret key, and
    region used by the public server handle.
  - Acceptance: public server accessors still expose the configured values
    without the public builder duplicating identity assembly.
  - Must preserve: startup error mapping, readiness logging order, endpoint
    address handling, shutdown handle ownership, and no public API signature
    changes.
  - Verification: focused startup-embedded and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-048` Consume embedded builder startup arguments directly.
  - Do: make public embedded build consume the builder state and pass startup
    arguments directly into the crate-only startup owner.
  - Acceptance: fluent builder behavior, defaults, configured credentials,
    region, volume ordering, and public build signature remain unchanged.
  - Must preserve: startup argument ownership, public builder method chaining,
    startup error mapping, and no public API signature changes.
  - Verification: focused startup-embedded and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-049` Keep embedded ready logging with startup completion.
  - Do: move embedded ready logging to the startup owner once readiness has
    been published and before the startup result is returned.
  - Acceptance: ready log endpoint text and endpoint normalization remain the
    same while the public builder only converts the startup result to a handle.
  - Must preserve: readiness publication order, endpoint address normalization,
    shutdown handle ownership, and no public API signature changes.
  - Verification: focused startup-embedded and embedded checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-050` Keep embedded identity with prepared startup config.
  - Do: return the embedded access key, secret key, and region alongside the
    prepared startup config so startup result assembly uses one prepared owner.
  - Acceptance: public server identity accessors still return configured
    credentials and region for default, explicit, and generated-volume builds.
  - Must preserve: credential initialization inputs, region initialization,
    startup config ownership, startup error mapping, and no public API
    signature changes.
  - Verification: focused startup-server/startup-embedded/embedded checks,
    RustFS lib check, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `R-051` Remove residual embedded startup argument clone contract.
  - Do: drop the `Clone` derivation from embedded startup arguments now that
    the public builder consumes startup state directly.
  - Acceptance: builder chaining, configured volumes, retry-before-global-init
    behavior, and startup ownership remain unchanged.
  - Must preserve: public builder method chaining, prepared config contents,
    temporary directory cleanup ownership, and no public API signature changes.
  - Verification: focused startup-server/startup-embedded/embedded checks,
    RustFS lib check, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `R-052` Make IAM AppContext bootstrap outcome explicit.
  - Do: replace boolean-or-global probing in IAM startup with a crate-private
    AppContext bootstrap disposition that reports already-available versus
    initialized context.
  - Acceptance: successful inline IAM bootstrap still initializes or reuses
    AppContext before publishing IAM readiness, while failure still returns an
    I/O error.
  - Must preserve: IAM initialization order, global AppContext singleton
    behavior, KMS/IAM handle construction, degraded-mode fallback, and
    readiness stage updates.
  - Verification: focused startup IAM checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-053` Reuse explicit AppContext bootstrap in IAM recovery.
  - Do: route degraded IAM recovery finalization through the same AppContext
    bootstrap result helper as inline startup.
  - Acceptance: recovered IAM still marks `IamReady` and publishes runtime
    readiness only after AppContext is available.
  - Must preserve: recovery retry/backoff behavior, shutdown-token handling,
    readiness publication retry behavior, and log semantics.
  - Verification: focused startup IAM checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-054` Move startup AppContext bootstrap owner into app context.
  - Do: move the post-IAM AppContext bootstrap helper out of IAM startup and
    into the app context owner while keeping IAM startup on the existing
    context boundary.
  - Acceptance: inline startup and deferred IAM recovery still initialize or
    reuse the global AppContext through one owner.
  - Must preserve: global AppContext singleton behavior, IAM handle lookup,
    KMS handle wiring, startup error mapping, and readiness ordering.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-055` Retire stale startup IAM layer baseline entries.
  - Do: remove the old direct `get_global_app_context` and
    `init_global_app_context` startup IAM baseline entries after the app
    context owner absorbs those calls.
  - Acceptance: layer dependency guard reports no new reverse dependencies and
    no stale baseline entries.
  - Must preserve: the existing accepted startup-to-AppContext boundary,
    AppContext initialization semantics, and no new layer cycles.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-056` Move startup KMS runtime handle owner into app context.
  - Do: route startup IAM KMS handle resolution through the app context startup
    boundary while keeping startup service orchestration on the startup IAM
    API.
  - Acceptance: inline and deferred IAM bootstrap use the same KMS manager
    reuse-or-init path without adding new startup service to app reverse
    dependencies.
  - Must preserve: KMS global singleton behavior, IAM bootstrap call order,
    degraded recovery KMS handle reuse, readiness publication, and layer guard
    boundaries.
  - Verification: focused startup KMS checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-057` Move startup IAM runtime facade into startup IAM.
  - Do: move the main and embedded IAM runtime facade helpers out of startup
    service components and into the startup IAM module.
  - Acceptance: startup services still call one IAM-facing API for embedded and
    main startup, while service components no longer own IAM facade wiring.
  - Must preserve: embedded versus main state-manager wiring, shutdown token
    propagation, IAM bootstrap disposition handling, KMS startup handle
    resolution, and degraded recovery behavior.
  - Verification: focused startup IAM/KMS checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-058` Move startup bucket metadata runtime owner.
  - Do: move embedded and main bucket metadata runtime helpers out of startup
    service components and into a bucket metadata startup module.
  - Acceptance: startup services still receive the same bucket list and bucket
    metadata, replication resync, bucket metadata system, and IAM config
    migration order stay unchanged.
  - Must preserve: embedded list-bucket error text, main list-bucket error
    mapping, replication resync placement, metadata migration order, and bucket
    list cloning semantics.
  - Verification: focused startup service checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-059` Move startup notification runtime owner.
  - Do: move embedded and main notification runtime helpers out of startup
    service components and into a notification startup module.
  - Acceptance: startup services still configure bucket notification state
    before notification system initialization and keep embedded notification
    failures non-fatal while main startup failures remain fatal.
  - Must preserve: notification config ordering, embedded skipped-service log
    fields, main failure log fields, error mapping, and notification init source
    error behavior.
  - Verification: focused startup notification/service checks, RustFS lib
    check, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-060` Move startup auth integration owner.
  - Do: move Keystone and OIDC startup integration wiring out of startup service
    components and into an auth startup module.
  - Acceptance: startup services still initialize auth integrations after IAM
    bootstrap and before notification setup, with Keystone failures remaining
    non-fatal and OIDC failures still logged as warnings.
  - Must preserve: Keystone env parsing error mapping, Keystone success/failure
    log fields, OIDC warning fields, and startup ordering.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-061` Move startup background service owner.
  - Do: move scanner/heal background runtime setup out of startup service
    components and into a background startup module.
  - Acceptance: startup services still receive the same scanner-enabled flag,
    while AHM cancellation-token creation, scanner/heal env parsing, heal
    manager initialization, and disabled-state logging stay unchanged.
  - Must preserve: env alias behavior, heal/scanner default enablement, disabled
    debug log fields, and heal storage ownership.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-062` Move startup observability runtime owner.
  - Do: move server-info, update-check, allocator reclaim, metrics, memory
    observability, and auto-tuner startup wiring out of startup service
    components and into an observability startup module.
  - Acceptance: observability side effects still run after background services,
    metrics-gated components keep the same guard, and cancellation-token clone
    behavior stays unchanged.
  - Must preserve: server-info/update-check ordering, allocator reclaim
    initialization, metrics enablement, memory observability setup, and
    auto-tuner startup.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-063` Move startup audit runtime owner.
  - Do: move audit/event-notifier startup wiring and its ordering tests out of
    startup service components and into an audit startup module.
  - Acceptance: startup services still start audit after buffer profiling, and
    embedded optional startup still shares the same event-notifier-before-audit
    helper.
  - Must preserve: audit started/failed log fields, event notifier ordering,
    audit source error propagation, and embedded audit skipped-service behavior.
  - Verification: focused startup audit checks, RustFS lib check,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-064` Move startup deadlock detector owner.
  - Do: move deadlock detector startup wiring out of startup service components
    and into a deadlock startup module.
  - Acceptance: startup services still initialize the detector after audit and
    before bucket metadata setup, with enabled/disabled states unchanged.
  - Must preserve: detector singleton lookup, enabled start behavior, disabled
    no-op behavior, and log fields.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-065` Retire startup service components aggregate.
  - Do: move embedded optional service startup wiring into an embedded optional
    startup module and remove the now-empty startup service components module.
  - Acceptance: startup services import focused owners directly and embedded
    optional startup keeps KMS, buffer profile, and audit skipped-service
    handling unchanged.
  - Must preserve: embedded KMS skipped-service log fields, buffer profile
    placement, audit skipped-service log fields, and no public runtime API
    changes.
  - Verification: focused startup checks, RustFS lib check, migration/layer
    guards, formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `R-066` Narrow internal startup owner module visibility.
  - Do: make focused startup owner modules crate-private after their public
    aggregate was retired.
  - Acceptance: the binary entrypoint and embedded public API still compile
    through the intended startup entrypoints, while audit/auth/background/bucket
    metadata/deadlock/embedded optional/notification/observability owner
    modules are no longer part of the public library surface.
  - Must preserve: all startup call order, log fields, readiness behavior,
    embedded startup behavior, optional runtime behavior, and public embedded
    builder API.
  - Verification: RustFS lib and bin check, focused startup checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-067` Narrow startup orchestration module visibility.
  - Do: make internal startup orchestration modules crate-private while keeping
    the binary entrypoint and existing compatibility/test-facing startup paths
    public.
  - Acceptance: `startup_entrypoint`, `startup_iam`, `startup_profiling`, and
    `startup_optional_runtimes` keep their public paths, while fs-guard,
    lifecycle, optional-runtime sidecars, preflight, protocols, runtime,
    runtime hooks, server, services, shutdown, storage, and TLS material modules
    are no longer public library modules.
  - Must preserve: binary startup entrypoint access, embedded public API,
    startup ordering, readiness behavior, optional runtime compatibility,
    profiling compatibility, IAM test/debug hooks, and all log fields.
  - Verification: RustFS lib and bin check, focused startup checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-068` Narrow remaining startup compatibility shim visibility.
  - Do: make the IAM bootstrap startup shim crate-private, remove the unused
    optional-runtime and profiling forwarding shims, and keep the binary
    entrypoint public.
  - Acceptance: `startup_entrypoint` remains public for `rustfs/src/main.rs`,
    while `startup_iam`, `startup_optional_runtimes`, and `startup_profiling`
    no longer appear as public library modules; migration rules reject
    restoring those public shim paths.
  - Must preserve: binary startup entrypoint access, IAM readiness bootstrap
    flow, embedded readiness publication, optional runtime shutdown wiring,
    profiling shutdown behavior, and test-only IAM retry hook behavior.
  - Verification: RustFS lib and bin check, focused startup checks,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `R-069` Narrow startup owner item visibility.
  - Do: make internal items in crate-private startup modules use crate
    visibility, and extend the migration guard so only `startup_entrypoint`
    can remain a public startup module.
  - Acceptance: startup owner modules expose no bare public items outside the
    public binary entrypoint module, and migration rules reject restoring public
    startup modules or public items inside crate-private startup files.
  - Must preserve: binary startup entrypoint access, embedded public API,
    startup ordering, IAM readiness bootstrap, optional runtime shutdown,
    profiling hooks, TLS material initialization, and all log fields.
  - Verification: RustFS lib and bin check, focused startup tests,
    migration/layer/unsafe guards, formatting, diff hygiene, Rust risk scan,
    pre-commit quality gate, and three-expert review.

- [x] `E-001/E-SET-001` Add ECStore layout skeleton and set-layout boundary.
  - Do: create the ECStore internal layout ownership buckets and pin static set
    layout versus runtime `Sets`/`SetDisks` orchestration boundaries before any
    file moves.
  - Acceptance: the skeleton documents future ownership buckets, static format
    set distribution is preserved, and runtime flat disk plus per-set lock-host
    mapping is described by focused tests.
  - Must preserve: format distribution, object-to-set hashing owner, local disk
    replacement, lock client mapping, existing public module paths, and runtime
    `Sets`/`SetDisks` behavior.
  - Verification: focused ECStore set layout tests, ECStore/RustFS compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-002/E-LAYOUT-001` Move ECStore format and disk-layout owners.
  - Do: pure-move persisted format ownership and disk-layout expansion into
    the ECStore layout bucket while keeping compatibility stubs at the old
    public paths.
  - Acceptance: `crate::disk::format::*` and `crate::disks_layout::*` remain
    usable, `layout::format` owns `FormatV3`, and `layout::disks_layout` owns
    CLI volume expansion.
  - Must preserve: format JSON wire shape, disk UUID lookup, distribution
    algorithm, `RUSTFS_ERASURE_SET_DRIVE_COUNT` handling, endpoint expansion,
    and old public module paths.
  - Verification: focused ECStore format and disks-layout tests,
    ECStore/RustFS/Heal compile checks, migration/layer guards, formatting,
    diff hygiene, Rust risk scan, branch freshness check, pre-commit quality
    gate, and three-expert review.

- [x] `E-003/E-LAYOUT-002` Move ECStore endpoint layout owners.
  - Do: pure-move endpoint parsing and endpoint grouping into the ECStore
    layout bucket while keeping compatibility stubs at the old public paths.
  - Acceptance: `crate::disk::endpoint::*` and `crate::endpoints::*` remain
    usable, `layout::endpoint` owns `Endpoint`, and `layout::endpoints` owns
    `EndpointServerPools` and endpoint grouping.
  - Must preserve: endpoint string parsing, URL/path validation, local-host
    detection, pool/set/disk indexes, endpoint grouping, disk independence
    checks, setup type classification, and old public module paths.
  - Verification: focused ECStore endpoint tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-004/E-LAYOUT-003` Move ECStore set-format heal helpers.
  - Do: move runtime-neutral set-format heal helper logic into the ECStore
    layout bucket while keeping disk initialization and `Sets` orchestration in
    `sets.rs`.
  - Acceptance: `layout::set_heal` owns drive-info mapping and unformatted
    format regeneration helpers, `Sets` keeps the same heal orchestration, and
    focused tests cover the extracted helper behavior.
  - Must preserve: disk format heal state mapping, unformatted disk format
    regeneration, current disk-info preservation, dry-run behavior, save-format
    behavior, and all `Sets` runtime control flow.
  - Verification: focused ECStore set-heal tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-005/E-LAYOUT-004` Move ECStore pool-space selection helpers.
  - Do: move runtime-neutral pool-space selection helper structs into the
    ECStore layout bucket while keeping the old `store` export path available.
  - Acceptance: `layout::pool_space` owns `PoolAvailableSpace` and
    `ServerPoolsAvailableSpace`, rebalance pool selection keeps the same tuple
    storage access inside the crate, and external `store` imports remain
    source-compatible through re-export.
  - Must preserve: pool index ordering, available-space summation,
    max-used-percent filtering semantics, excluded-pool zeroing, object
    placement pool selection, and rebalance pool-space behavior.
  - Verification: focused ECStore pool-space tests, ECStore/RustFS/Heal
    compile checks, migration/layer guards, formatting, diff hygiene, Rust risk
    scan, branch freshness check, pre-commit quality gate, and three-expert
    review.

- [x] `E-006/E-REBALANCE-001` Move ECStore rebalance support helpers.
  - Do: move rebalance-only helper DTOs, pool lookup error classification, and
    delete/latest-object result reducers into `store::rebalance::support`
    while keeping async store orchestration in the existing modules.
  - Acceptance: rebalance callers keep the same `PoolObjInfo`/`PoolErr`
    access inside `store`, delete aggregation and latest-object selection keep
    the same behavior, and the moved helpers remain private to the rebalance
    boundary.
  - Must preserve: latest-object tie-breaks, delete result aggregation, pool
    lookup not-found/version-not-found classification, rebalance disk-set lookup
    error context, object delete flows, and existing rebalance control flow.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-007/E-LAYOUT-005` Move ECStore pool-space builder helpers.
  - Do: move `has_space_for` and server-pool available-space construction into
    the ECStore layout pool-space owner while keeping `store::has_space_for`
    source-compatible through re-export.
  - Acceptance: `layout::pool_space` owns capacity checks, pool availability
    construction, filter helpers, and focused tests; rebalance only gathers
    runtime disk snapshots and calls the layout owner.
  - Must preserve: unknown-size handling, erasure fill-fraction math,
    inode/free-space guard behavior, meta-bucket capacity bypass, pool index
    ordering, available-space summation, and rebalance pool selection.
  - Verification: focused ECStore pool-space and rebalance tests,
    ECStore/RustFS/Heal compile checks, migration/layer guards, formatting,
    diff hygiene, Rust risk scan, branch freshness check, pre-commit quality
    gate, and three-expert review.

- [x] `E-008/E-REBALANCE-002` Move ECStore rebalance metadata helpers.
  - Do: move rebalance metadata status, bucket queue, terminal event,
    participant, cleanup-warning, metadata merge, and stop-state helpers into
    `rebalance::meta` while keeping wire structs and ECStore orchestration in
    `rebalance.rs`.
  - Acceptance: `rebalance::meta` owns the helper functions, `rebalance.rs`
    keeps save/load and object-flow orchestration, and focused rebalance tests
    keep covering the moved behavior.
  - Must preserve: metadata wire shape, stopped/completed/failed precedence,
    bucket queue ordering, cleanup-warning merge semantics, participant
    resolution, data-usage cache filtering, start/stop validation, and
    percent-free goal math.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-009/E-REBALANCE-003` Move ECStore rebalance worker helpers.
  - Do: move rebalance worker task/result handling, transient retry
    classification, retry timing, bucket config loading, source cleanup
    decisions, and listing retry wrappers into `rebalance::worker` while keeping
    high-level rebalance orchestration in `rebalance.rs`.
  - Acceptance: `rebalance::worker` owns worker helper functions,
    `rebalance.rs` keeps orchestration and wire structs, and focused rebalance
    tests keep covering the moved behavior.
  - Must preserve: worker join error context, transient/terminal error
    classification, retry backoff, missing bucket config handling, delete-marker
    skip and cleanup decisions, listing retry cancellation behavior, and
    migration result accounting.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-010/E-REBALANCE-004` Move ECStore rebalance migration helpers.
  - Do: move migration backend abstraction, migration version result,
    delete-marker/remote-tier option builders, and version migration retry flow
    into `rebalance::migration` while keeping high-level rebalance orchestration
    in `rebalance.rs`.
  - Acceptance: `rebalance::migration` owns migration helper functions and
    result types, `rebalance.rs` keeps orchestration and wire structs, and
    focused rebalance tests keep covering moved behavior.
  - Must preserve: remote-tier object movement, delete-marker replication
    state, data-usage cache skip behavior, source read/write retry semantics,
    transient/non-transient classification, retry backoff, not-found handling,
    migration stage labels, and cleanup accounting.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-011/E-REBALANCE-005` Move ECStore rebalance state impls.
  - Do: move `RebalanceStats` update helpers, `RebalStatus` conversions, and
    `RebalanceMeta` load/save impls into `rebalance::meta` while leaving public
    wire structs in `rebalance.rs`.
  - Acceptance: `rebalance::meta` owns metadata/state behavior, `rebalance.rs`
    keeps data contracts and ECStore orchestration, and focused rebalance tests
    keep covering moved behavior.
  - Must preserve: serialized rebalance metadata header format/version,
    empty/short/unknown metadata handling, last refresh timestamps, save-skip
    behavior for empty pool stats, object/version/byte accounting, batch update
    behavior, status display labels, and legacy status byte mapping.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-012/E-REBALANCE-006` Move ECStore rebalance control impls.
  - Do: move ECStore rebalance metadata save/load/update/init/status/stop
    control methods into `rebalance::control` while leaving the worker loop and
    entry migration orchestration in `rebalance.rs`.
  - Acceptance: `rebalance::control` owns metadata/control methods,
    `rebalance.rs` keeps public data contracts and worker orchestration, and
    focused rebalance tests keep covering moved behavior.
  - Must preserve: metadata merge locking, load/save error wrapping, pool stats
    refresh and extension, init free-space goal, pool stat update behavior,
    bucket queue done/defer behavior, cleanup warning recording, start/stop
    status checks, decommission conflict checks, and stop snapshot persistence.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-013/E-REBALANCE-007` Move ECStore rebalance runtime loop.
  - Do: move `start_rebalance`, the pool rebalance worker loop, completion
    check, and periodic stats save loop into `rebalance::runtime` while leaving
    entry/object/bucket migration orchestration in `rebalance.rs`.
  - Acceptance: `rebalance::runtime` owns start and pool runtime orchestration,
    `rebalance.rs` keeps public data contracts and entry/object/bucket
    migration flow, and focused rebalance tests keep covering moved behavior.
  - Must preserve: decommission/start validation, duplicate-start skipping,
    pool-at-goal and empty-queue completion persistence, participant/local
    endpoint filtering, cancellation handling, deferred-bucket repeated failure
    guard, bucket done/defer behavior, terminal event application, save-task
    error precedence, goal completion math, and save option persistence.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-014/E-REBALANCE-008` Move ECStore rebalance entry flow.
  - Do: move the remaining entry, object-transfer, deferred-error, and bucket
    entry-scan migration flow into `rebalance::entry` while leaving public data
    contracts in `rebalance.rs`.
  - Acceptance: `rebalance::entry` owns bucket/entry migration flow,
    `rebalance::runtime` keeps pool-level orchestration, and focused rebalance
    tests keep covering moved behavior.
  - Must preserve: directory and completed-pool skips, lifecycle-expired
    filtering, delete-marker skip semantics, data-movement retry flow, deferred
    transient failure recording, batch stats updates, source cleanup warning
    recording, entry worker semaphore limits, cancellation handling, listing
    retry flow, and bucket outcome precedence.
  - Verification: focused ECStore rebalance tests, ECStore/RustFS/Heal compile
    checks, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `E-015/E-REBALANCE-009` Split ECStore rebalance unit tests.
  - Do: move the large inline `rebalance_unit_tests` module out of
    `rebalance.rs` into `rebalance/rebalance_unit_tests.rs` while preserving
    the module name and test filter path.
  - Acceptance: `rebalance.rs` is reduced to public rebalance data contracts
    plus submodule wiring, rebalance unit tests remain under
    `rebalance::rebalance_unit_tests`, and focused rebalance tests keep covering
    moved behavior.
  - Must preserve: test coverage, helper visibility, legacy metadata
    serialization coverage, migration backend spies, panic-context tests, and
    every existing rebalance unit-test filter path.
  - Verification: focused ECStore rebalance tests, migration/layer guards,
    formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `E-016/E-REBALANCE-010` Move ECStore rebalance type contracts.
  - Do: move rebalance stats, status, info, metadata DTOs, and internal
    bucket/entry outcomes into `rebalance::types` while preserving root
    re-exports.
  - Acceptance: public `crate::rebalance::*` paths remain stable, internal
    submodules keep `super::...` access, and `rebalance.rs` only wires shared
    constants, modules, and re-exports.
  - Must preserve: serde field names/defaults, rebalance metadata wire shape,
    status/save-option defaults, cancellation/refresh metadata fields, and
    internal bucket/entry outcome semantics.
  - Verification: focused ECStore rebalance tests, migration/layer guards,
    formatting, diff hygiene, Rust risk scan, branch freshness check,
    pre-commit quality gate, and three-expert review.

- [x] `API-129` Route RustFS internal ECStore consumers through owner boundary.
  - Do: expose crate-local ECStore facade module aliases from
    `rustfs/src/storage/mod.rs` and migrate RustFS startup, server, capacity,
    config, table-catalog, workload admission, and S3 API helper consumers to
    import those aliases from `crate::storage`.
  - Acceptance: non-owner RustFS files no longer import `rustfs_ecstore::api`
    directly, while `app`, `admin`, and `storage` owner modules remain the only
    RustFS crate direct ECStore facade import points.
  - Must preserve: startup sequencing, global endpoint/config side effects,
    readiness checks, RPC signature verification, notification event dispatch,
    capacity refresh behavior, table-catalog constants, workload admission
    snapshots, and S3 ETag conversion behavior.
  - Verification: focused RustFS compile, direct import residual scan,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `API-130` Centralize external ECStore facade alias imports.
  - Do: replace grouped and raw-subpath `rustfs_ecstore::api` imports in IAM,
    notify, observability, Swift, S3 Select, e2e helpers, heal/scanner tests,
    and fuzz targets with per-module `ecstore_*` aliases plus local type
    aliases or module-qualified calls.
  - Acceptance: non-ECStore source no longer uses grouped
    `rustfs_ecstore::api::{...}` imports or raw
    `rustfs_ecstore::api::<module>::...` subpaths, while owner alias imports
    remain explicit.
  - Must preserve: IAM config IO, notify config persistence, observability
    metrics collection, Swift metadata access, S3 Select object-store access,
    e2e RPC helpers, heal/scanner ECStore test setup, and fuzz validation
    semantics.
  - Verification: focused external crate compile, grouped/raw facade residual
    scans, migration/layer guards, formatting, diff hygiene, Rust risk scan,
    branch freshness check, pre-commit quality gate, and three-expert review.

- [x] `API-131` Route nested external production ECStore imports through owner roots.
  - Do: expose notify, observability metrics, and S3 Select ECStore facade
    aliases from their crate or module owner roots, and migrate nested
    production files to import those local aliases instead of importing
    `rustfs_ecstore::api` directly.
  - Acceptance: nested production files under notify, observability, and S3
    Select no longer import ECStore facade modules directly, while IAM,
    scanner, heal, Swift, and owner root files remain the only approved
    external production direct facade import points.
  - Must preserve: notify config persistence, observability metrics collection
    and scheduler bucket-monitor checks, S3 Select object-store error and
    storage access behavior, and all public crate APIs.
  - Verification: focused notify/obs/S3 Select compile, nested direct-import
    residual scan, migration/layer guards, formatting, diff hygiene, Rust risk
    scan, branch freshness check, pre-commit quality gate, and three-expert
    review.

- [x] `API-132` Replace completed external owner module aliases with symbols.
  - Do: replace notify, Swift, and S3 Select owner-root `ecstore_*` module
    aliases with explicit local ECStore symbols, type aliases, constants, and
    wrapper functions.
  - Acceptance: completed external owner roots no longer expose broad
    `ecstore_*` module aliases, while nested modules keep using owner-local
    symbols and the remaining larger observability, IAM, scanner, and heal
    owner roots stay unchanged for later slices.
  - Must preserve: notify config persistence, Swift bucket metadata access, S3
    Select object-store error mapping, object reads, scan buffering, and all
    public crate APIs.
  - Verification: focused notify/Swift/S3 Select compile, completed-owner
    alias residual scan, migration/layer guards, formatting, diff hygiene, Rust
    risk scan, branch freshness check, pre-commit quality gate, and
    three-expert review.

- [x] `API-133` Replace scanner owner module aliases with symbols.
  - Do: replace scanner owner-root `ecstore_*` module aliases with explicit
    local ECStore symbols, type aliases, constants, and wrapper functions.
  - Acceptance: scanner no longer exposes broad `ecstore_*` module aliases,
    nested scanner modules continue to consume scanner-local symbols, and the
    migration guard prevents reintroducing scanner owner-root module aliases.
  - Must preserve: scanner lifecycle config reads, versioning/replication
    helper traits, disk metadata access, tier listing, erasure checks,
    replication-heal queueing, config persistence, raw list traversal, and
    bucket usage replacement behavior.
  - Verification: focused scanner compile, completed-owner alias residual scan,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `API-134` Replace remaining external owner module aliases with symbols.
  - Do: replace heal, IAM, and observability owner-root `ecstore_*` module
    aliases with explicit local ECStore symbols, type aliases, constants, and
    wrapper functions.
  - Acceptance: heal, IAM, and observability no longer expose broad `ecstore_*`
    module aliases, nested modules continue to consume owner-local symbols, and
    the migration guard prevents reintroducing these owner-root module aliases.
  - Must preserve: heal disk metadata and local disk lookup, IAM config
    persistence and notification fanout, observability storage/data-usage,
    quota, lifecycle, replication, capacity, and bucket monitor collection.
  - Verification: focused heal/IAM/observability compile, completed-owner alias
    residual scan, migration/layer guards, formatting, diff hygiene, Rust risk
    scan, branch freshness check, pre-commit quality gate, and three-expert
    review.

- [x] `API-135` Replace test and fuzz owner module aliases with symbols.
  - Do: replace e2e, heal/scanner integration-test, and fuzz-target
    `ecstore_*` module aliases with explicit ECStore symbols.
  - Acceptance: the completed test/fuzz files no longer import broad
    `ecstore_*` owner modules, direct symbols preserve the same ECStore facade
    contracts, and the migration guard prevents reintroducing module aliases in
    these files.
  - Must preserve: e2e RPC client construction, replication target tests, heal
    ECStore setup, scanner lifecycle/tier/transition behavior, and bucket/path
    fuzz validation semantics.
  - Verification: focused e2e/heal/scanner compile, fuzz manifest compile,
    completed test/fuzz alias residual scan, migration/layer guards, formatting,
    diff hygiene, Rust risk scan, branch freshness check, pre-commit quality
    gate, and three-expert review.

- [x] `API-136` Replace RustFS runtime owner module aliases with symbols.
  - Do: replace RustFS app/admin/storage owner-root `ecstore_*` facade aliases
    with owner-local curated symbol modules that expose only the ECStore
    submodules, functions, types, and constants consumed by those runtime
    boundaries.
  - Acceptance: `rustfs/src/app/mod.rs`, `rustfs/src/admin/mod.rs`, and
    `rustfs/src/storage/mod.rs` no longer import broad ECStore facade modules
    as `ecstore_*`; migration guards reject reintroducing those broad aliases.
  - Must preserve: app object/lifecycle/replication helpers, admin config,
    metrics, tiering, rebalance helpers, storage S3/RPC metadata helpers, and
    startup/server consumers of the storage owner boundary.
  - Verification: focused RustFS compile, runtime owner alias residual scan,
    migration/layer guards, formatting, diff hygiene, Rust risk scan, branch
    freshness check, pre-commit quality gate, and three-expert review.

- [x] `API-137` Guard completed owner facade import shapes.
  - Do: extend migration rules so completed owner and test/fuzz boundaries
    cannot reintroduce bare `rustfs_ecstore::api::<module>` imports or glob
    facade imports.
  - Acceptance: completed owner roots and completed test/fuzz boundaries keep
    explicit symbol imports, type aliases, constants, or wrappers; migration
    guards reject bare module and glob facade imports.
  - Must preserve: all API-136 RustFS owner symbol boundaries, API-135 test/fuzz
    direct symbol imports, and external owner root symbol imports.
  - Verification: architecture migration guard, shell syntax check, formatting,
    diff hygiene, branch freshness check, and three-expert review.

## Next PRs

1. `pure-move`: continue pruning remaining facade compatibility and owner boundaries.

## Pre-Push Review Log

| Expert | Status | Notes |
|---|---|---|
| Quality/architecture | pass | API-137 tightens completed owner and test/fuzz facade import-shape rules without changing runtime code. |
| Migration preservation | pass | API-136 RustFS owner symbol boundaries and API-135 test/fuzz explicit symbol imports remain unchanged. |
| Testing/verification | pass | Shell syntax, migration guard, formatting, diff hygiene, and stacked-base freshness checks passed for API-137. |

## Verification Notes

Passed before push:

- Issue #660 API-137 current slice:
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Stacked-base freshness check against
    `origin/overtrue/arch-test-fuzz-owner-symbols`: passed.

- Issue #660 API-136 current slice:
  - `cargo check --tests -p rustfs`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed.
  - Completed runtime-owner module-alias residual scan: passed.
  - Rust risk scan: no new production unwrap/expect, panic/todo/unsafe, or
    risky behavior added; existing `Result<Vec<String>>` storage trait
    signatures remain unchanged compatibility surfaces.

- Issue #660 API-135 current slice:
  - `cargo check --tests -p e2e_test -p rustfs-heal -p rustfs-scanner`: passed.
  - `cargo check --manifest-path fuzz/Cargo.toml --bins`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed.
  - Completed test/fuzz module-alias residual scan: passed.
  - Rust risk scan: diff-only scan found import and call-target rewrites only;
    no new production unwrap/expect, panic/todo/unsafe, or risky behavior added.

- Issue #660 API-134 current slice:
  - `cargo check --tests -p rustfs-heal -p rustfs-iam -p rustfs-obs`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed.
  - Heal/IAM/observability completed-owner module-alias residual scan: passed.
  - Rust risk scan: diff-only scan found explicit symbol imports and wrapper
    calls only; no new unwrap/expect, panic/todo/unsafe, or risky behavior
    added.

- Issue #660 API-133 current slice:
  - `cargo check --tests -p rustfs-scanner`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed.
  - Scanner completed-owner module-alias residual scan: passed.
  - Rust risk scan: diff-only scan found explicit symbol imports and wrapper
    calls only; no new unwrap/expect, panic/todo/unsafe, or risky behavior
    added.

- Issue #660 API-132 current slice:
  - `cargo check --tests -p rustfs-notify -p rustfs-s3select-api -p rustfs-protocols --features rustfs-protocols/swift`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed, including clippy, script tests, nextest
    `6518 passed, 111 skipped`, and doc-tests.
  - Completed external owner module-alias residual scan: passed.
  - Rust risk scan: diff-only scan found explicit `as` symbol imports only; no
    new unwrap/expect, panic/todo/unsafe, or risky behavior added.

- Issue #660 API-131 current slice:
  - `cargo check --tests -p rustfs-notify -p rustfs-obs -p rustfs-s3select-api`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed, including clippy, script tests, nextest
    `6518 passed, 111 skipped`, and doc-tests.
  - Nested external production ECStore facade residual scan: passed.
  - Rust risk scan: diff-only scan found new `as ecstore_*` import aliases
    only; no new risky behavior added.

- Issue #660 API-130 current slice:
  - `cargo check --tests -p rustfs-notify -p rustfs-obs -p rustfs-protocols -p rustfs-s3select-api -p e2e_test -p rustfs-heal -p rustfs-scanner -p rustfs-iam`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed, including clippy, script tests, nextest
    `6518 passed, 111 skipped`, and doc-tests.
  - Grouped/raw ECStore facade residual scan outside ECStore: passed.
  - Rust risk scan: diff-only scan found path-rewritten existing test
    unwraps/expects only; no new risky behavior added.

- Issue #660 API-129 current slice:
  - `cargo check --tests -p rustfs`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `make pre-commit`: passed, including clippy, script tests, nextest
    `6509 passed, 111 skipped`, and doc-tests.
  - RustFS direct ECStore facade residual scan outside owner modules: passed.
  - Rust risk scan: diff-only scan found no new unwrap/expect, panic/todo,
    debug prints, relaxed ordering, or integer casts.

- Issue #660 API-128 current slice:
  - `cargo check --tests -p rustfs`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - RustFS owner compatibility bridge residual scan: passed.
  - Rust risk scan: diff-only scan found path-rewritten existing test unwraps,
    test expects, and part-number casts only; no new risky behavior added.
  - `make pre-commit`: passed, including 6509 nextest tests passed and 111
    skipped.

- Issue #660 API-127 current slice:
  - `cargo check --tests -p rustfs-iam -p rustfs-heal -p rustfs-scanner`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - External owner compatibility bridge residual scan: passed.
  - Rust risk scan: diff-only scan found no new unwrap/expect, numeric casts,
    string-error public APIs, boxed public errors, println/eprintln, or relaxed
    ordering.

- Issue #660 API-126 current slice:
  - `cargo check --tests -p e2e_test -p rustfs-iam -p rustfs-notify -p rustfs-obs -p rustfs-protocols -p rustfs-s3select-api`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Remaining standalone compatibility bridge residual scan: passed.
  - Rust risk scan: diff-only scan found no new unwrap/expect, numeric casts,
    string-error public APIs, boxed public errors, println/eprintln, or relaxed
    ordering.

- Issue #660 API-125 current slice:
  - `cargo check --tests -p e2e_test -p rustfs-iam -p rustfs-notify`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Standalone thin compatibility bridge residual scan: passed.
  - Rust risk scan: diff-only scan found no new unwrap/expect, numeric casts,
    string-error public APIs, boxed public errors, println/eprintln, or relaxed
    ordering.

- Issue #660 API-124 current slice:
  - `cargo check --tests -p rustfs-heal -p rustfs-scanner`: passed.
  - `cargo check --manifest-path fuzz/Cargo.toml --bins`: passed; transient `fuzz/Cargo.lock` refresh was restored to avoid dependency churn.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Test/fuzz compatibility bridge residual scan: passed.
  - Rust risk scan: reviewed pre-existing test-only unwrap/expect/panic/unsafe usage; no new production risk.

- Issue #660 API-123 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Startup compatibility bridge residual scan: passed.
  - Rust risk scan: passed.

- Issue #660 API-122 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Root one-off compatibility bridge residual scan: passed.
  - Rust risk scan: passed.

- Issue #660 API-121 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Runtime local compatibility bridge residual scan: passed.
  - Rust risk review on path-only replacements and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-120 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Admin handlers secondary compatibility bridge residual scan: passed.
  - Rust risk review on path-only replacements and guard script: passed.

- Issue #660 API-119 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Nested secondary compatibility bridge residual scan: passed.
  - Rust risk review on path-only replacements and guard script: passed.

- Issue #660 API-118 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Storage secondary compatibility bridge residual scan: passed.
  - Rust risk review on path-only replacements and guard script: passed.

- Issue #660 API-117 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - App/admin secondary compatibility bridge residual scan: passed.
  - Rust risk review on path-only replacements and guard script: passed.

- Issue #660 API-116 current slice:
  - `cargo check --manifest-path fuzz/Cargo.toml --bins`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Fuzz-target local compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.

- Issue #660 API-115 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo check -p rustfs-scanner --tests`: passed.
  - `cargo check -p rustfs-iam --tests`: passed.
  - `cargo check -p rustfs-obs --tests`: passed.
  - `cargo check -p rustfs-s3select-api --tests`: passed.
  - `cargo check -p e2e_test --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Standalone crate local compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-111 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Storage RPC/S3 API local compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-110 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - RustFS local compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-109 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Root compatibility consumer residual scan: passed.
  - Storage owner compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-108 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Full RustFS local bridge owner self-path residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-107 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Full storage compatibility self-reference residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-106 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Full storage compatibility grouped-import residual scan: passed.
  - Full storage compatibility raw-facade residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-105 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Full storage compatibility raw-facade residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-104 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Narrowed local compatibility glob-export scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-103 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Narrowed local compatibility glob-export scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-102 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Storage compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-101 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Owner compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-098 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Direct root capacity/server compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-099 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Startup/init root compatibility consumer residual scan: passed.
  - Root startup consumer wrapper residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-100 current slice:
  - `cargo check -p rustfs --tests`: passed.
  - `cargo fmt --all`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Direct root compatibility consumer residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-097 current slice:
  - `cargo check -p rustfs -p rustfs-scanner -p rustfs-heal -p e2e_test --tests`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Direct non-compat disk/RPC/warm-backend trait import residual scan: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-096 current slice:
  - `cargo check -p rustfs -p rustfs-scanner -p rustfs-heal`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Direct non-compat bucket trait import residual scan: passed.
  - Added-line Rust risk scan: passed.
  - `make pre-commit`: passed; nextest run
    `a18de942-8181-48fa-adf0-e01c2a5d37c3`, 6354 passed, 111 skipped;
    doctests passed.

- Issue #660 API-095 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - RustFS root/e2e raw facade path residual scan: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed; nextest run
    `a1771057-5015-4861-9a38-b856c8abb6f6`, 6354 passed, 111 skipped; doctests
    passed.

- Issue #660 API-094 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Consumer raw facade path residual scan: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 API-093 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - RustFS app/admin raw facade path residual scan: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 API-092 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - RustFS storage-owner raw facade path residual scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-091 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Outer app/admin/storage raw signature facade path residual scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-090 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Outer app/admin/storage object/error facade alias residual scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-089 current slice:
  - `cargo check -p rustfs -p rustfs-scanner -p rustfs-heal -p e2e_test`:
    passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - ECStore API re-export residual scan for compatibility boundaries: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-087 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Storage-owner ECStore API re-export residual scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-086 current slice:
  - `cargo check -p rustfs`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Root runtime ECStore API re-export residual scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-085 current slice:
  - `cargo check --tests -p rustfs-heal -p rustfs-scanner`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Test/fuzz grouped compatibility passthrough residual scans: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-084 current slice:
  - `cargo check --tests -p rustfs-scanner -p rustfs-notify -p rustfs-obs -p e2e_test`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Scanner/notify/obs/e2e broad compatibility residual scans: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-083 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Admin/app broad compatibility export scans: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-082 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Storage compatibility residual scan excluding `storage_compat.rs`: passed.
  - Broad storage compatibility export scan: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-081 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Admin compatibility residual scan for broad `com`, bare `init`, and old
    config IO call paths: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-080 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 C-013 current slice:
  - `cargo test -p rustfs-concurrency workload::tests:: -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib workload_admission::tests:: -- --nocapture`:
    passed.
  - `cargo check -p rustfs-concurrency`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 API-079 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 C-012 current slice:
  - `cargo test -p rustfs --lib storage::backpressure::tests:: -- --nocapture`: passed.
  - `cargo test -p rustfs --lib storage::deadlock_detector::tests:: -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed storage Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 C-011 current slice:
  - `cargo test -p rustfs --lib storage::deadlock_detector::tests::test_request_hang_policy_projects_to_concurrency_and_core_config -- --nocapture`: passed.
  - `cargo test -p rustfs --lib storage::backpressure::tests::test_backpressure_policy_projects_to_concurrency_and_core_config -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed storage Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 C-004/C-005/C-006 current slice:
  - `cargo test -p rustfs-ecstore cluster -- --nocapture`: passed, 7 tests.
  - `cargo check -p rustfs-ecstore --all-targets`: passed.
  - `cargo check -p rustfs --lib --bins`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.

- Issue #660 C-001/C-002/C-003 current slice:
  - `cargo check -p rustfs-ecstore --all-targets`: passed.
  - `cargo check -p rustfs --lib --bins`: passed.
  - `cargo test -p rustfs-ecstore cluster -- --nocapture`: passed; 4 tests.
  - `cargo test -p rustfs --lib runtime_capabilities -- --nocapture`: passed; 3 tests.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-078 current slice:
  - `cargo check -p rustfs-ecstore --all-targets`: passed.
  - `cargo check -p rustfs --lib --bins`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 R-069 current slice:
  - `cargo check -p rustfs --lib`: passed.
  - `cargo check -p rustfs --bins`: passed.
  - `cargo test -p rustfs --lib startup_ -- --nocapture`: passed; 53 tests.
  - `cargo fmt --all`: applied formatting.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Startup public owner scan: passed; only `startup_entrypoint::run_process`
    remains public.
  - Rust added-line risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed.

- Issue #660 API-077 current slice:
  - `cargo check -p rustfs-ecstore --all-targets`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust added-line risk scan on changed Rust files: passed.
  - `make pre-commit`: passed; nextest ran 6340 tests with 6340 passed, 111 skipped, and doctests passed.

- Issue #660 API-076 current slice:
  - `cargo check --tests -p rustfs-ecstore -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-iam -p rustfs-notify -p rustfs-obs -p rustfs-protocols -p rustfs-s3select-api -p e2e_test`: passed.
  - `cargo check --benches -p rustfs-ecstore`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111 skipped, and doctests passed.

- Issue #660 API-075 current slice:
  - `cargo check --tests -p rustfs-ecstore -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-iam -p rustfs-notify -p rustfs-obs -p rustfs-protocols -p rustfs-s3select-api -p e2e_test`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111 skipped, and doctests passed.

- Issue #660 API-074 current slice:
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - Direct old ECStore path scan in non-ECStore `storage_compat.rs` boundaries: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111 skipped, and doctests passed.

- Issue #660 API-073 current slice:
  - `cargo check --tests -p rustfs-ecstore -p rustfs -p rustfs-scanner -p rustfs-heal -p rustfs-iam -p rustfs-notify -p rustfs-obs -p rustfs-protocols -p rustfs-s3select-api -p e2e_test`: passed.
  - `cargo check --manifest-path fuzz/Cargo.toml --all-targets`: passed; Cargo refreshed the fuzz lockfile during verification and the generated lockfile change was not retained.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Direct old ECStore facade path scan in outer storage compatibility boundaries: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111 skipped, and doctests passed.

- Issue #660 R-068 current slice:
  - `cargo check -p rustfs --lib --bins`: passed.
  - `cargo test -p rustfs --lib startup_ -- --nocapture`: passed; 53 tests.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files and guard script: passed; only existing test-only `expect` calls were present.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111 skipped, and doctests passed.

- Issue #660 API-072 current slice:
  - `cargo check --tests -p rustfs-ecstore`: passed.
  - `cargo check --tests -p rustfs -p rustfs-scanner -p rustfs-obs -p rustfs-iam -p rustfs-heal -p rustfs-protocols -p rustfs-s3select-api`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `bash -n scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files and guard script: passed.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed,
    111 skipped, and doctests passed.

- Issue #660 R-056/R-067 current slice:
  - `cargo test -p rustfs --lib startup_kms -- --nocapture`: passed; 2
    tests.
  - `cargo test -p rustfs --lib startup_iam -- --nocapture`: passed; 8
    tests.
  - `cargo test -p rustfs --lib startup_ -- --nocapture`: passed; 53
    tests.
  - `cargo test -p rustfs --lib startup_audit -- --nocapture`:
    passed; 2 tests.
  - `cargo test -p rustfs --lib startup_notification -- --nocapture`:
    passed; 1 test.
  - `cargo check -p rustfs --lib --bins`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only `expect`
    calls were present.
  - `make pre-commit`: passed; nextest ran 6341 tests with 6341 passed, 111
    skipped, and doctests passed.

- Issue #660 R-054/R-055 current slice:
  - `cargo test -p rustfs --lib startup_ -- --nocapture`: passed; 51 tests.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; only a test-only `expect`
    call was present.
  - `make pre-commit`: passed; nextest ran 6339 tests with 6339 passed, 111
    skipped, and doctests passed.

- Issue #660 R-052/R-053 current slice:
  - `cargo test -p rustfs --lib startup_iam -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; only a test-only `expect`
    call was present.
  - `make pre-commit`: passed; nextest ran 6336 tests with 6336 passed and
    111 skipped, and doctests passed.

- Issue #660 R-050/R-051 current slice:
  - `cargo test -p rustfs --lib startup_server -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_embedded -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only `expect`
    calls were present.
  - `make pre-commit`: passed; nextest ran 6329 tests with 6329 passed and
    111 skipped, and doctests passed.

- Issue #660 R-048/R-049 current slice:
  - `cargo test -p rustfs --lib startup_embedded -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; no risky-token matches were
    present in changed Rust files.
  - `make pre-commit`: passed; nextest ran 6329 tests with 6329 passed and
    111 skipped, and doctests passed.

- Issue #660 R-046/R-047 current slice:
  - `cargo test -p rustfs --lib startup_embedded -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - Rust risk scan on changed Rust files: passed; matches were limited to
    existing embedded doc examples.
  - `make pre-commit`: passed; nextest ran 6324 tests with 6324 passed and
    111 skipped, and doctests passed.

- Issue #660 R-044/R-045 current slice:
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_embedded -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_server -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - Rust risk scan on changed Rust files: passed; matches were limited to
    existing doc examples and test-only `expect` calls.
  - `./scripts/check_unsafe_code_allowances.sh`: passed after avoiding a local
    `pipefail` false positive when `rg -q` finds nearby `SAFETY:` comments.
  - `make pre-commit`: passed.

- Issue #660 R-042/R-043 current slice:
  - `cargo test -p rustfs --lib startup_lifecycle -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; the only production risky
    token was the intended move of embedded drop `remove_dir_all` cleanup from
    the public embedded handle into `startup_shutdown`.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - `make pre-commit`: passed.

- Issue #660 R-040/R-041 current slice:
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; newly added risky-token
    matches were empty, and the changed-file scan only matched the existing
    embedded `Drop` cleanup path.
  - `make pre-commit`: passed.

- Issue #660 R-038/R-039 current slice:
  - `cargo test -p rustfs --lib startup_lifecycle -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: reviewed; newly added risky-token
    matches were limited to test-only `expect` calls, and broader changed-file
    matches were pre-existing lifecycle/doc examples plus cleanup paths.
  - `make pre-commit`: passed.

- Issue #660 R-036/R-037 current slice:
  - `cargo test -p rustfs --lib startup_server -- --nocapture`: passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only
    `expect` calls and the existing embedded temp-dir cleanup path were
    present.
  - `make pre-commit`: passed.

- Issue #660 R-034/R-035 current slice:
  - `cargo test -p rustfs --lib startup_runtime_hooks -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed; no
    matching unit tests currently exist.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only existing default
    credential fields and moved temp-dir cleanup paths were present.
  - `make pre-commit`: passed.

- Issue #660 R-031 current slice:
  - `cargo test -p rustfs --lib startup_lifecycle -- --nocapture`: passed;
    no matching unit tests currently exist.
  - `cargo test -p rustfs --lib embedded -- --nocapture`: passed; no
    matching unit tests currently exist.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only existing embedded doc
    examples use `Box<dyn Error>` / `println!`.
  - `make pre-commit`: passed.

- Issue #660 R-032 current slice:
  - `cargo test -p rustfs-targets ops_profiler -- --nocapture`: passed.
  - `cargo test -p rustfs-targets builtin_ops_profiler -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib extension_catalog -- --nocapture`: passed.
  - `cargo check -p rustfs-targets`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only
    expectations/assertion paths were present.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-033 current slice:
  - `cargo test -p rustfs --lib extension_catalog -- --nocapture`: passed.
  - `cargo test -p rustfs-targets ops_diagnostics -- --nocapture`: passed.
  - `cargo test -p rustfs-targets ops_profiler -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only
    expectations/assertion paths were present.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-001/E-SET-001 current slice:
  - `cargo test -p rustfs-ecstore test_eset -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only
    expectation paths were present.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-002/E-LAYOUT-001 current slice:
  - `cargo test -p rustfs-ecstore format::test -- --nocapture`: passed.
  - `cargo test -p rustfs-ecstore disks_layout -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only existing test-only
    unwrap/println/panic/expect paths were present.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-003/E-LAYOUT-002 current slice:
  - `cargo test -p rustfs-ecstore layout::endpoint -- --nocapture`: passed.
  - `cargo test -p rustfs-ecstore layout::endpoints -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only existing endpoint
    production/test unwrap and expectation paths were moved.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-004/E-LAYOUT-003 current slice:
  - `cargo test -p rustfs-ecstore layout::set_heal -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only test-only unwrap
    expectations were added around deterministic helper construction.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-005/E-LAYOUT-004 current slice:
  - `cargo test -p rustfs-ecstore layout::pool_space -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; only existing `store.rs`
    test-only `expect` calls and an existing `Result<String>` method signature
    were present outside the moved helper body.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-006/E-REBALANCE-001 current slice:
  - `cargo test -p rustfs-ecstore store::rebalance -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; no risky added lines were
    introduced.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-007/E-LAYOUT-005 current slice:
  - `cargo test -p rustfs-ecstore layout::pool_space -- --nocapture`: passed.
  - `cargo test -p rustfs-ecstore store::rebalance -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; added cast lines are moved
    capacity math from the existing implementation.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-008/E-REBALANCE-002 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-009/E-REBALANCE-003 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-012/E-REBALANCE-006 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; added casts are moved
    pool-index accounting from the existing implementation and remain guarded.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-013/E-REBALANCE-007 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; moved casts are existing
    pool completion math and remain guarded.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-014/E-REBALANCE-008 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; moved casts and unwraps are
    existing test or migration-flow code and remain guarded.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-015/E-REBALANCE-009 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; the runtime diff is a test
    module move plus a SAFETY-comment proximity fix required by the guard.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 E-016/E-REBALANCE-010 current slice:
  - `cargo test -p rustfs-ecstore rebalance::rebalance_unit_tests -- --nocapture`: passed.
  - `cargo check -p rustfs-ecstore -p rustfs -p rustfs-heal`: passed.
  - `./scripts/check_unsafe_code_allowances.sh`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed; production changes are a
    type-contract move and existing Windows FFI casts remain unchanged.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 X-012 current slice:
  - `cargo test -p rustfs-extension-schema`: passed.
  - `cargo check -p rustfs-extension-schema`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 X-013 current slice:
  - `cargo test -p rustfs-extension-schema`: passed.
  - `cargo check -p rustfs-extension-schema`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-021 current slice:
  - `cargo test -p rustfs --lib startup_optional_runtimes -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_services -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-022 current slice:
  - `cargo test -p rustfs --lib startup_optional_runtimes -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_protocols -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_services -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-023 current slice:
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_services -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_optional_runtimes -- --nocapture`:
    passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-024 current slice:
  - `cargo test -p rustfs --lib startup_lifecycle -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_services -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-025 current slice:
  - `cargo test -p rustfs --lib startup_service_components -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_services -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_lifecycle -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-026 current slice:
  - `cargo test -p rustfs --lib startup_optional_runtime_sidecars -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_optional_runtimes -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-027 current slice:
  - `cargo test -p rustfs --lib startup_runtime_hooks -- --nocapture`:
    passed.
  - `cargo test -p rustfs --lib startup_profiling -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_runtime -- --nocapture`: passed.
  - `cargo test -p rustfs --lib startup_shutdown -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

- Issue #660 R-020 current slice:
  - `cargo test -p rustfs --lib startup_profiling -- --nocapture`: passed.
  - `cargo check -p rustfs --lib`: passed.
  - `./scripts/check_architecture_migration_rules.sh`: passed.
  - `./scripts/check_layer_dependencies.sh`: passed.
  - `cargo fmt --all --check`: passed.
  - `git diff --check`: passed.
  - Rust risk scan on changed Rust files: passed.
  - `make pre-commit`: passed.
  - Three-expert review: passed.

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
