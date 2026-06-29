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
| Rust source files | 1,237 | `rg --files -g '*.rs'` |
| `OnceLock` references | 221 lines | `rg -n --glob '*.rs' 'OnceLock'` |
| `GLOBAL_*` references | 302 lines | `rg -n --glob '*.rs' '\bGLOBAL_[A-Za-z0-9_]*\b'` |
| `static NAME:` definitions | 621 lines | `rg -n --glob '*.rs' '^\s*(pub(\([^)]*\))?\s+)?static(\s+mut)?\s+[A-Za-z_][A-Za-z0-9_]*\s*:'` |
| `lazy_static!` `static ref` definitions | 59 lines | `rg -n --glob '*.rs' '^\s*(pub\s+)?static\s+ref\s+[A-Za-z_][A-Za-z0-9_]*\s*:'` |
| `static mut` definitions | 0 lines | `rg -n --glob '*.rs' '^\s*(pub(\([^)]*\))?\s+)?static\s+mut\s+'` |

## Global State Classification

| Category | Rule | Representative owners |
|---|---|---|
| Process-global | Process identity, metrics registries, lock manager, audit guard, TLS material, or other state that is intentionally one per process. | `crates/credentials`, `crates/common`, `crates/io-metrics`, `crates/lock`, `crates/obs`, `crates/tls-runtime` |
| Runtime migration target | Mutable runtime state that describes the active object store, endpoints, local disks, lifecycle, replication, notification, config, or background controllers. | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, `rustfs/src/app/context/*` |
| Owner-local compatibility | Existing compatibility adapters that are allowed to read globals while callers migrate to AppContext-first or owner-local runtime-source APIs. | `rustfs/src/*/runtime_sources.rs`, `rustfs/src/*/storage_api.rs`, `crates/*/storage_api.rs` |
| Test or fixture state | Static setup used by tests to amortize expensive ECStore setup or isolate compatibility harness state. | `rustfs/src/app/*_test.rs`, `crates/scanner/tests/*`, `crates/ecstore/src/**/tests` |
| Cache or constant | Regexes, metrics descriptors, defaults, KVS registrations, headers, path constants, and small process caches that are not runtime ownership handles. | `crates/config`, `crates/obs/src/metrics`, `crates/utils`, `rustfs/src/server/readiness.rs` |
| Legacy naming or review-needed | Mixed-case `GLOBAL_*`, old MinIO-port naming, stale comments, or names that need owner confirmation before code movement. | `GLOBAL_IsErasure`, `GLOBAL_Endpoints`, `GLOBAL_LocalNodeName`, `globalDeploymentIDPtr` |

## Runtime Migration Inventory

These are the issue #730 targets that should remain visible until an owner
migration PR removes or replaces each item.

| State | Current boundary | Category | Migration stance |
|---|---|---|---|
| `APP_CONTEXT_SINGLETON` | `rustfs/src/app/context/global.rs` | Owner-local compatibility | Keep as the context-first facade while no-context startup and embedded callers still exist. |
| `GLOBAL_OBJECT_API`, `GLOBAL_OBJECT_STORE_RESOLVER` | `crates/ecstore/src/runtime/global.rs` | Runtime migration target | Do not migrate first; it is tied to storage startup, IAM-after-storage AppContext publication, and data-plane resolver compatibility. |
| `GLOBAL_Endpoints`, `GLOBAL_IsErasure`, `GLOBAL_IsDistErasure`, `GLOBAL_IsErasureSD`, `GLOBAL_RootDiskThreshold` | `crates/ecstore/src/runtime/global.rs` and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Endpoint, setup-type, and root-disk-threshold access now stays behind ECStore runtime helpers; move endpoint ownership only after readiness and quorum behavior have explicit coverage. |
| `GLOBAL_LOCAL_DISK_MAP`, `GLOBAL_LOCAL_DISK_ID_MAP`, `GLOBAL_LOCAL_DISK_SET_DRIVES` | `crates/ecstore/src/runtime/global.rs` and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Local disk map, disk-id cache, and set-drive access now stay behind ECStore runtime-source helpers instead of direct global access; preserve disk lookup, remote/local classification, and test reset hooks in later ownership changes. |
| `GLOBAL_ExpiryState`, `GLOBAL_TransitionState`, `GLOBAL_LifecycleSys` | `crates/ecstore/src/bucket/lifecycle/*`, `crates/ecstore/src/runtime/global.rs`, and `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | `GLOBAL_LifecycleSys` access now stays behind ECStore runtime-source helpers; scanner expiry-state access now uses the ECStore runtime `expiry_state_handle` boundary; `GLOBAL_ExpiryState` remains the first code-bearing ownership candidate because AppContext already has an `ExpiryStateInterface`, resolver, and tests. |
| `GLOBAL_REPLICATION_POOL`, `GLOBAL_REPLICATION_STATS`, `GLOBAL_BUCKET_MONITOR` | `crates/ecstore/src/bucket/replication/*`, `crates/ecstore/src/runtime/global.rs` | Runtime migration target | Replication pool/stat access now stays behind replication owner and ECStore runtime-source helpers; bucket-monitor direct access now stays behind ECStore runtime helpers while AppContext/runtime-source resolvers remain the caller boundary. |
| `GLOBAL_TierConfigMgr`, `GLOBAL_STORAGE_CLASS`, `GLOBAL_CONFIG_SYS`, `GLOBAL_SERVER_CONFIG` | `crates/ecstore/src/config`, `crates/config`, `rustfs/src/app/context/runtime_sources.rs` | Runtime migration target | Tier config manager reads and reloads now use the ECStore runtime-source helper; move remaining config state through config/runtime-source owners only, without combining storage-class behavior or persistence changes. |
| `GLOBAL_EventNotifier`, `GLOBAL_NotificationSys` | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, and `crates/ecstore/src/services/*` | Runtime migration target | `GLOBAL_EventNotifier` access now stays behind ECStore runtime-source helpers; move remaining notification ownership only through notify/runtime-source boundaries. |
| `GLOBAL_BOOT_TIME`, `GLOBAL_BACKGROUND_SERVICES_CANCEL_TOKEN`, `globalDeploymentIDPtr`, `GLOBAL_REGION`, `GLOBAL_RUSTFS_PORT`, `GLOBAL_LocalNodeName`, `GLOBAL_LocalNodeNameHex` | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target | Boot time, background service cancellation token reads, and ECStore local-node-name fallback reads now stay behind the ECStore runtime-source API; deployment ID, region, and port direct access now stays behind owner helpers until the remaining scalar handles migrate. |
| `GLOBAL_LOCAL_LOCK_CLIENT`, `GLOBAL_LOCK_CLIENTS`, `GLOBAL_LOCK_MANAGER` | `crates/ecstore/src/runtime/global.rs`, `crates/lock` | Runtime migration target / process-global split | ECStore lock client direct access now stays behind ECStore runtime helpers; preserve lock quorum and lock client selection while keeping the process-level lock manager separate from endpoint-specific clients. |
| `GLOBAL_CONN_MAP`, `GLOBAL_LOCAL_NODE_NAME`, `GLOBAL_RUSTFS_HOST`, `GLOBAL_RUSTFS_ADDR`, `GLOBAL_ROOT_CERT`, `GLOBAL_MTLS_IDENTITY`, `GLOBAL_OUTBOUND_TLS_GENERATION` | `crates/common`, `crates/tls-runtime`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / process-global split | Internode connection cache, common local node name, RustFS host/address reads, and outbound TLS material reads are now owned behind `rustfs_common` helpers; migrate the remaining transport and TLS state only after internode transport and outbound TLS ownership are explicit, without changing cached channel reuse or TLS reload semantics. |
| `GLOBAL_RUSTFS_RPC_SECRET` | `crates/credentials`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / process-global split | RPC auth token writes now stay behind the `rustfs_credentials` helper boundary; migrate only if runtime secret ownership changes, preserving lazy environment and credential-derived token semantics. |
| `GLOBAL_HEAL_MANAGER`, `GLOBAL_HEAL_CHANNEL_PROCESSOR`, `GLOBAL_AHM_SERVICES_CANCEL_TOKEN` | `crates/heal` and `crates/common` | Runtime migration target | Needs heal runtime owner handles and queue tests; do not combine with ECStore disk-state movement. |
| `AUDIT_SYSTEM`, `GLOBAL_CAPACITY_MANAGER`, `GLOBAL_BUCKET_TARGET_SYS`, `INTERNODE_DATA_TRANSPORT`, `GLOBAL_KMS_SERVICE_MANAGER` | Owner crates | Runtime migration target / process-global split | Track as owner-specific follow-ups; each owner must decide whether AppContext, a runtime-source facade, or process-global state is the right final shape. |
| `GLOBAL_PROCESSORS` | `crates/ecstore/src/services/batch_processor.rs`, `crates/ecstore/src/runtime/sources.rs` | Runtime migration target / owner helper | Direct static access now stays inside the ECStore batch processor owner; callers use `get_global_processors` or the ECStore runtime-source helper until processor ownership moves into an injected runtime context. |

## First Code-Bearing Candidate

`GLOBAL_ExpiryState` is the safest first runtime migration candidate:

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
