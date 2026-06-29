# Global State Inventory

This inventory records the issue #730 baseline for global runtime state after
the AppContext foundation and owner-local runtime-source boundaries were added.
It is intentionally documentation-only: it classifies migration targets without
changing startup, readiness, object IO, lifecycle, replication, or notification
behavior.

## Counting Baseline

The audit uses the current workspace Rust sources and keeps broad static
caches separate from runtime migration targets.

| Scope | Count | Command |
|---|---:|---|
| Rust source files | 1,252 | `rg --files -g '*.rs'` |
| `OnceLock` references | 221 lines | `rg -n --glob '*.rs' 'OnceLock'` |
| `GLOBAL_*` references | 273 lines | `rg -n --glob '*.rs' '\bGLOBAL_[A-Za-z0-9_]*\b'` |
| `static NAME:` definitions | 621 lines | `rg -n --glob '*.rs' '^\s*(pub(\([^)]*\))?\s+)?static(\s+mut)?\s+[A-Za-z_][A-Za-z0-9_]*\s*:'` |
| `lazy_static!` `static ref` definitions | 58 lines | `rg -n --glob '*.rs' '^\s*(pub\s+)?static\s+ref\s+[A-Za-z_][A-Za-z0-9_]*\s*:'` |
| `static mut` definitions | 0 lines | `rg -n --glob '*.rs' '^\s*(pub(\([^)]*\))?\s+)?static\s+mut\s+'` |

## Global State Classification

| Category | Rule | Representative owners |
|---|---|---|
| Process-global | Process identity, metrics registries, lock manager, audit guard, TLS material, or other state that is intentionally one per process. | `crates/credentials`, `crates/common`, `crates/io-metrics`, `crates/lock`, `crates/obs`, `crates/tls-runtime` |
| Runtime migration target | Mutable runtime state that describes the active object store, endpoints, local disks, lifecycle, replication, notification, config, or background controllers. | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, `rustfs/src/app/context/*` |
| Owner-local compatibility | Existing compatibility adapters that are allowed to read globals while callers migrate to AppContext-first or owner-local runtime-source APIs. | `rustfs/src/*/runtime_sources.rs`, `rustfs/src/*/storage_api.rs`, `crates/*/storage_api.rs` |
| Test or fixture state | Static setup used by tests to amortize expensive ECStore setup or isolate compatibility harness state. | `rustfs/src/app/*_test.rs`, `crates/scanner/tests/*`, `crates/ecstore/src/**/tests` |
| Cache or constant | Regexes, metrics descriptors, defaults, KVS registrations, headers, path constants, and small process caches that are not runtime ownership handles. | `crates/config`, `crates/obs/src/metrics`, `crates/utils`, `rustfs/src/server/readiness.rs` |
| Legacy naming or review-needed | Old MinIO-port naming, stale comments, or names that need owner confirmation before code movement. | `GLOBAL_OBJECT_API` |

## Runtime Migration Inventory

These are the issue #730 targets that should remain visible until an owner
migration PR removes or replaces each item.

| State | Current boundary | Category | Migration stance |
|---|---|---|---|
| `APP_CONTEXT_SINGLETON` | `rustfs/src/app/context/global.rs` | Owner-local compatibility | Keep as the context-first facade while no-context startup and embedded callers still exist. |
| `GLOBAL_OBJECT_API`, `GLOBAL_OBJECT_STORE_RESOLVER` | `crates/ecstore/src/runtime/global.rs` | Runtime migration target | Do not migrate first; it is tied to storage startup, IAM-after-storage AppContext publication, and data-plane resolver compatibility. |
| `GLOBAL_ENDPOINTS`, `GLOBAL_IS_ERASURE`, `GLOBAL_IS_DIST_ERASURE`, `GLOBAL_IS_ERASURE_SD`, `GLOBAL_ROOT_DISK_THRESHOLD` | `crates/ecstore/src/runtime/global.rs` and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Endpoint, setup-type, and root-disk-threshold access now stays behind ECStore runtime helpers; move endpoint ownership only after readiness and quorum behavior have explicit coverage. |
| `GLOBAL_LOCAL_DISK_MAP`, `GLOBAL_LOCAL_DISK_ID_MAP`, `GLOBAL_LOCAL_DISK_SET_DRIVES` | `crates/ecstore/src/runtime/global.rs` and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Local disk map, disk-id cache, and set-drive access now stay behind ECStore runtime-source helpers instead of direct global access; preserve disk lookup, remote/local classification, and test reset hooks in later ownership changes. |
| `GLOBAL_EXPIRY_STATE`, `GLOBAL_TRANSITION_STATE`, `GLOBAL_LIFECYCLE_SYS` | `crates/ecstore/src/bucket/lifecycle/*`, `crates/ecstore/src/runtime/global.rs`, and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Lifecycle state globals now stay behind ECStore lifecycle owner helpers and ECStore runtime-source helpers; RustFS AppContext has expiry/transition state interfaces and resolver coverage, and daily tier stats derive from the transition-state handle instead of a separate context boundary; scanner expiry-state access still uses the ECStore runtime `expiry_state_handle` boundary until scanner gets an injected provider. |
| `GLOBAL_REPLICATION_POOL`, `GLOBAL_REPLICATION_STATS`, `GLOBAL_BUCKET_MONITOR` | `crates/ecstore/src/bucket/replication/*`, `crates/ecstore/src/runtime/global.rs` | Runtime migration target | Replication pool/stat access now stays behind replication owner and ECStore runtime-source helpers; bucket-monitor direct access now stays behind ECStore runtime helpers while AppContext/runtime-source resolvers remain the caller boundary. |
| `GLOBAL_TIER_CONFIG_MGR`, `GLOBAL_STORAGE_CLASS`, `GLOBAL_CONFIG_SYS`, `GLOBAL_SERVER_CONFIG` | `crates/ecstore/src/config`, `crates/config`, `rustfs/src/app/context/runtime_sources.rs` | Runtime migration target | Tier config manager reads and reloads now use the ECStore runtime-source helper; move remaining config state through config/runtime-source owners only, without combining storage-class behavior or persistence changes. |
| `GLOBAL_EVENT_NOTIFIER`, `GLOBAL_NOTIFICATION_SYS` | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, and `crates/ecstore/src/services/*` | Runtime migration target | `GLOBAL_EVENT_NOTIFIER` and `GLOBAL_NOTIFICATION_SYS` access now stay behind ECStore runtime-source and notification owner helpers; move remaining notification ownership only through notify/runtime-source boundaries. |
| `EVENT_DISPATCH_HOOK` | `crates/ecstore/src/services/event_notification.rs`, RustFS server event bridge, and storage compatibility APIs | Runtime migration target / owner helper | Direct hook storage stays inside the ECStore event-notification owner; RustFS registers the bridge through the storage compatibility facade until event dispatch ownership moves behind an injected notification sink. |
| `GLOBAL_BUCKET_METADATA_SYS` | `crates/ecstore/src/metadata/bucket_sys.rs`, `crates/ecstore/src/runtime/sources.rs`, and RustFS storage compatibility APIs | Runtime migration target | Bucket metadata system direct access now stays inside the ECStore metadata owner; callers use metadata owner helpers or storage/runtime-source compatibility functions until metadata ownership moves behind an injected runtime context. |
| `GLOBAL_BOOT_TIME`, `GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN`, `GLOBAL_DEPLOYMENT_ID`, `GLOBAL_REGION`, `GLOBAL_RUSTFS_PORT`, `GLOBAL_LOCAL_NODE_NAME_FALLBACK`, `GLOBAL_LOCAL_NODE_NAME_HEX_FALLBACK` | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Boot time, background service cancellation token reads, and ECStore local-node-name fallback reads now stay behind the ECStore runtime-source API; deployment ID, region, and port direct access now stays behind owner helpers until the remaining scalar handles migrate. |
| `WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER` | `crates/ecstore/src/runtime/sources.rs`, RustFS startup background setup, and storage compatibility APIs | Runtime migration target / owner helper | Startup publishes the workload provider through the storage compatibility facade, and ECStore data movement reads it only through the runtime-source helper until workload admission ownership moves into an explicit runtime context. |
| `GLOBAL_LOCAL_LOCK_CLIENT`, `GLOBAL_LOCK_CLIENTS`, `GLOBAL_LOCK_MANAGER` | `crates/ecstore/src/runtime/global.rs`, `crates/lock` | Runtime migration target / process-global split | ECStore lock client direct access now stays behind ECStore runtime helpers; preserve lock quorum and lock client selection while keeping the process-level lock manager separate from endpoint-specific clients. |
| `GLOBAL_CONN_MAP`, `GLOBAL_LOCAL_NODE_NAME`, `GLOBAL_RUSTFS_HOST`, `GLOBAL_RUSTFS_ADDR`, `GLOBAL_ROOT_CERT`, `GLOBAL_MTLS_IDENTITY`, `GLOBAL_OUTBOUND_TLS_GENERATION` | `crates/common`, `crates/tls-runtime`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / process-global split | Internode connection cache, common local node name, RustFS host/address reads, and outbound TLS material reads are now owned behind `rustfs_common` helpers; migrate the remaining transport and TLS state only after internode transport and outbound TLS ownership are explicit, without changing cached channel reuse or TLS reload semantics. |
| `GLOBAL_RUSTFS_RPC_SECRET` | `crates/credentials`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / process-global split | RPC auth token writes now stay behind the `rustfs_credentials` helper boundary; migrate only if runtime secret ownership changes, preserving lazy environment and credential-derived token semantics. |
| `GLOBAL_HEAL_MANAGER`, `GLOBAL_HEAL_CHANNEL_PROCESSOR`, `GLOBAL_AHM_SERVICES_CANCEL_TOKEN` | `crates/heal/src/lib.rs` | Runtime migration target / process-global split | Direct access now stays inside the heal owner; callers use heal helper functions until heal runtime ownership moves behind explicit owner handles. |
| `AUDIT_SYSTEM` | `crates/audit/src/global.rs` | Runtime migration target / process-global split | Direct global access now stays inside the audit owner; callers use audit helper functions until audit lifecycle ownership moves behind AppContext or a runtime-source boundary. |
| `GLOBAL_PROCESSORS` | `crates/ecstore/src/services/batch_processor.rs`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / owner helper | Direct static access now stays inside the ECStore batch processor owner; callers use `get_global_processors` or the ECStore runtime-source helper until processor ownership moves into an injected runtime context. |
| `INTERNODE_DATA_TRANSPORT` | `crates/ecstore/src/cluster/rpc/internode_data_transport.rs` | Runtime migration target / owner helper | Direct static access now stays inside the ECStore internode transport owner; callers use `build_internode_data_transport_from_env` until backend selection moves into an injected runtime context. |
| `GLOBAL_KMS_SERVICE_MANAGER` | `crates/kms/src/service_manager.rs`, RustFS KMS runtime sources | Runtime migration target / owner helper | Direct static access now stays inside the `rustfs_kms` service manager owner; RustFS callers use KMS helpers or AppContext/runtime-source handles until KMS ownership fully moves into runtime context. |
| `GLOBAL_CAPACITY_MANAGER` | `crates/object-capacity/src/capacity_manager.rs`, RustFS capacity service | Runtime migration target / owner helper | Direct static access now stays inside the object-capacity owner; callers use `get_capacity_manager` or isolated manager factories until capacity ownership moves into an injected runtime context. |
| `GLOBAL_BUCKET_TARGET_SYS` | `crates/ecstore/src/bucket/bucket_target_sys.rs`, admin/app/scanner/replication target paths | Runtime migration target / owner helper | Direct static access now stays inside the ECStore bucket target owner; callers still use `BucketTargetSys::get()` until bucket target ownership moves behind a runtime-source or replication target boundary. |
| `USAGE_MEMORY_CACHE`, `USAGE_CACHE_UPDATING` | `crates/ecstore/src/data_usage/mod.rs` | Runtime migration target / owner-local cache | Data-usage memory overlay and singleflight state stay private to the ECStore data-usage owner; callers use data-usage functions until scanner/data-usage ownership moves behind an injected runtime context. |

## Owner-Local Cache Inventory

These owner-local caches and static guards are part of the broad issue #730
`OnceLock` audit, but they are not runtime ownership handles. They stay private
to the defining owner module; callers must use the existing owner APIs instead
of reaching across module boundaries.

| State | Owner boundary | Category | Migration stance |
|---|---|---|---|
| `READ_REPAIR_HEAL_CACHE` | `crates/ecstore/src/set_disk/read.rs` | Cache or constant / owner-local cache | Read-repair heal suppression stays local to set-disk read handling. |
| `DISK_COMPRESSION_CONFIG` | `crates/ecstore/src/io_support/compress.rs` | Cache or constant / owner-local cache | Disk compression environment parsing stays local to IO support compression helpers. |
| `CACHED_MAX_INFLIGHT_BYTES`, `CACHED_BATCH_BLOCKS`, `CACHED_BYTESMUT_INGEST` | `crates/ecstore/src/erasure/coding/encode.rs` | Cache or constant / owner-local cache | Erasure encode tuning caches stay local to the coding owner. |
| `CACHED_PUT_LARGE_BATCH_MIN_SIZE_BYTES`, `CACHED_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES`, `OBJECT_LOCK_DIAG_ENABLED` | `crates/ecstore/src/set_disk/mod.rs` | Cache or constant / owner-local cache | Set-disk batching and diagnostics caches stay local to the set-disk owner. |
| `DRIVE_TIMEOUT_PROFILE_CACHE`, `DRIVE_TIMEOUT_HEALTH_POLICY_CACHE` | `crates/ecstore/src/disk/disk_store.rs` | Cache or constant / owner-local cache | Drive timeout environment caches stay local to the disk-store owner. |
| `TIER_FREE_VERSION_RECOVERY_STARTED`, `TIER_DELETE_JOURNAL_RECOVERY_STARTED` | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` | Cache or constant / owner-local static guard | Lifecycle recovery single-run guards stay local to lifecycle operations. |
| `REMOTE_DELETE_INFLIGHT`, `REMOTE_DELETE_LIMITER`, `REMOTE_DELETE_BREAKER`, `REMOTE_TIER_DELETE_TEST_HOOK` | `crates/ecstore/src/bucket/lifecycle/tier_sweeper.rs` | Cache or constant / owner-local static guard | Remote tier delete concurrency, breaker, and test hook state stay local to the tier sweeper owner. |

## First Code-Bearing Candidate

`GLOBAL_EXPIRY_STATE` is the safest first runtime migration candidate:

- AppContext already exposes `ExpiryStateInterface` and resolver coverage in
  `rustfs/src/app/context.rs`.
- ECStore access is already concentrated in
  `crates/ecstore/src/runtime/sources.rs`.
- The main external readers can be moved through storage/observability facades
  before changing lifecycle queue ownership.

Do not migrate `GLOBAL_OBJECT_API` first. It is coupled to storage startup,
object-store resolver publication, IAM-after-storage AppContext initialization,
and broad data-plane compatibility.

## Verification

Inventory and guardrail PRs should run:

- `bash -n scripts/check_architecture_migration_rules.sh`
- `./scripts/check_architecture_migration_rules.sh`
- `cargo fmt --all --check`
- `git diff --check`

Code-bearing migration PRs must add focused tests for the owner being moved
before running broader gates.
