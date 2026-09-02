# Global State Inventory

**Use this when:** you meet a `GLOBAL_*` static or an `OnceLock` and need to know whether it is a runtime ownership handle (reach it through a boundary), an owner-local static (leave it inside its module), or process-global by design.
**Source of truth:** `crates/ecstore/src/api/mod.rs` (the `pub mod runtime` and `pub mod global` re-export lists), `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, and the statics themselves. Boundary rules are in [global-state-crate-split-plan.md](global-state-crate-split-plan.md).

## Global State Classification

| Category | Rule | Representative owners |
|---|---|---|
| Process-global | Process identity, metrics registries, lock manager, audit guard, TLS material, or other state intentionally one per process. | `GLOBAL_LOCK_MANAGER` (`crates/lock`), `GLOBAL_CONN_MAP` (`crates/common`), `GLOBAL_RUSTFS_RPC_SECRET` (`crates/credentials`), `AUDIT_SYSTEM` (`crates/audit`), `crates/io-metrics`, `crates/obs`, `crates/tls-runtime` |
| Runtime migration target | Mutable runtime state describing the active object store, endpoints, local disks, lifecycle, replication, notification, config, or background controllers. | `crates/ecstore/src/runtime/global.rs`, `crates/ecstore/src/runtime/sources.rs`, `rustfs/src/app/context/` |
| Owner-local compatibility | Adapters allowed to read globals while callers migrate to AppContext-first or owner-local runtime-source APIs. | `rustfs/src/*/runtime_sources.rs`, `rustfs/src/*/storage_api.rs`, `crates/*/storage_api.rs` |
| Owner-local static | A static private to one module and reached only through that module's functions: caches, single-run guards, admission locks, module toggles. | The RustFS inventory below |
| Test or fixture state | Static setup that amortizes expensive ECStore setup or isolates harness state. | `rustfs/src/app/*_test.rs`, `crates/scanner/tests/`, `crates/test-utils/src/ecstore_test_compat.rs` |
| Cache or constant | Regexes, metrics descriptors, defaults, KVS registrations, headers, path constants. | `crates/config`, `crates/obs/src/metrics`, `crates/utils` |

## Runtime Migration Inventory

Runtime ownership handles that exist today. Reads go through `rustfs_ecstore::api::runtime`, bootstrap writes go through `rustfs_ecstore::api::global`, and RustFS code reaches both only from `rustfs/src/storage/storage_api.rs` and the AppContext resolvers.

| Handle (`rustfs_ecstore::api::runtime`) | Backing state | Stance |
|---|---|---|
| `object_store_handle` | `GLOBAL_OBJECT_API`, `GLOBAL_OBJECT_STORE_RESOLVER` (`crates/ecstore/src/runtime/global.rs`); the resolver is published from the AppContext owner path | Do not migrate first: tied to storage startup, IAM-after-storage AppContext publication, and data-plane resolver compatibility. |
| `endpoint_pools`, `setup_is_erasure`, `setup_is_dist_erasure`, `setup_is_erasure_sd`, `first_cluster_node_is_local` | `GLOBAL_ENDPOINTS` and setup-type state (`crates/ecstore/src/runtime/global.rs`) | Move endpoint ownership only after readiness and quorum behavior have explicit coverage. |
| `local_disk_map_read` | Local disk map and set-drive state (`crates/ecstore/src/runtime/sources.rs`) | Preserve disk lookup, remote/local classification, and test reset hooks. |
| `expiry_state_handle`, `transition_state_handle` | Lifecycle expiry and transition state, `GLOBAL_LIFECYCLE_SYS` (`crates/ecstore/src/runtime/global.rs`) | Lifecycle owner helpers and the AppContext `ExpiryStateInterface` (`rustfs/src/app/context/interfaces.rs`) are the caller boundary; the scanner still reads `expiry_state_handle` until it gets an injected provider. |
| `global_tier_config_mgr` | Tier config manager | Reads and reloads stay behind this helper. |
| `bucket_monitor` | Replication bandwidth monitor | Replication pool/stat handles are projected into RustFS wrapper types at the storage boundary. |
| `global_lock_client`, `global_lock_clients` | `GLOBAL_LOCAL_LOCK_CLIENT`, `GLOBAL_LOCK_CLIENTS` (`crates/ecstore/src/runtime/global.rs`) | Preserve lock quorum and client selection; the process-level `GLOBAL_LOCK_MANAGER` stays separate. |
| `boot_time`, `deployment_id`, `region`, `rustfs_port` | `GLOBAL_BOOT_TIME`, deployment id, region, and port state (`crates/ecstore/src/runtime/global.rs`) | Scalar writes remain behind the `api::global` setters (`set_global_endpoints`, `set_global_region`, `set_global_rustfs_port`, `set_object_store_resolver`, `shutdown_background_services`, `update_erasure_type`). |

Owner-helper handles outside the runtime-source list stay inside their owner and are reached through owner functions: `GLOBAL_EVENT_NOTIFIER` (`crates/ecstore/src/runtime/global.rs`); `GLOBAL_NOTIFICATION_SYS`, `EVENT_DISPATCH_HOOK`, `GLOBAL_PROCESSORS`, `INTERNODE_DATA_TRANSPORT`, `GLOBAL_BUCKET_TARGET_SYS`, `GLOBAL_CONFIG_SYS`, `GLOBAL_STORAGE_CLASS`, `WORKLOAD_ADMISSION_SNAPSHOT_PROVIDER` (ECStore owner modules); `GLOBAL_SERVER_CONFIG` (`crates/config/src/server_config.rs`); `GLOBAL_HEAL_RUNTIME`, `GLOBAL_AHM_SERVICES_CANCEL_TOKEN` (`crates/heal/src/lib.rs`); `GLOBAL_KMS_SERVICE_MANAGER` (`crates/kms/src/service_manager.rs`); `GLOBAL_CAPACITY_MANAGER` (`crates/object-capacity/src/capacity_manager.rs`); `APP_CONTEXT_SINGLETON` (`rustfs/src/app/context/global.rs`).

Regenerate:

```bash
rg -n -A4 'pub use crate::runtime::(sources|global)::' crates/ecstore/src/api/mod.rs
rg -n --glob '*.rs' 'static (ref )?GLOBAL_[A-Z_]+' crates rustfs/src
```

## RustFS Owner-Local Static Inventory

RustFS-side statics that matter architecturally because other modules are tempted to reach them. They stay private to their owner module; callers use the owner's functions.

| Static | Owner | Stance |
|---|---|---|
| `KEYSTONE_AUTH`, `KEYSTONE_MAPPER`, `KEYSTONE_CONFIG` | `rustfs/src/auth_keystone.rs` | Keystone provider, mapper, and config stay private to the Keystone owner. |
| `DEADLOCK_DETECTOR` | `rustfs/src/storage/deadlock_detector.rs` | Detector lifecycle stays private to the storage deadlock detector. |
| `CONCURRENCY_MANAGER` | `rustfs/src/storage/concurrency/manager.rs` | Storage concurrency scheduler state stays inside the concurrency owner. |
| `GLOBAL_KMS_DEK_PROVIDER`, `GLOBAL_SSE_DEK_PROVIDER` | `rustfs/src/storage/sse.rs` | DEK provider caches stay private to the SSE owner. |
| `ECSTORE_EVENT_DISPATCH_HOOK` | `rustfs/src/server/event.rs` | Event bridge registration goes through the storage facade. |
| `AUDIT_MODULE_ENABLED`, `NOTIFY_MODULE_ENABLED` | `rustfs/src/module_switches.rs` | Module toggles are read through module-switch helpers; `MODULE_SWITCH_RMW_LOCK` (`rustfs/src/server/module_switch.rs`) serializes persisted updates. |
| `RUNTIME_CONFIG_RELOAD_MUTEX` | `rustfs/src/admin/service/config.rs` | Serializes dynamic config reload fanout. |
| `EMBEDDED_RUNTIME_OWNERS` | `rustfs/src/startup_shutdown.rs` | Embedded runtime owner handles used for shutdown ordering. |
| `SERVICE_FROZEN` | `rustfs/src/admin/handlers/system.rs` | Service freeze flag stays behind the system admin handler. |
| `RECONCILER` | `rustfs/src/site_replication_reconcile.rs` | Site-replication reconciler singleton. |
| `CONSOLE_CONFIG` | `rustfs/src/admin/console.rs` | Console bootstrap config. |
| `LICENSE_STATE`, `LICENSE_VERIFIER` | `rustfs/src/license.rs` | License state and verifier stay behind license helpers. |

Regenerate the full list (long, mostly caches and test hooks):

```bash
rg -n '^\s*(pub(\(crate\))? )?static [A-Z_]+' rustfs/src
```
