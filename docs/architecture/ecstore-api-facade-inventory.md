# ECStore API Facade Inventory

This inventory records the current `rustfs_ecstore::api` compatibility surface
before any ECStore split PR removes or narrows re-exports. It is a planning and
guardrail document only. It must not be used as approval to move lifecycle,
replication, or `SetDisks` runtime behavior.

## Facade Group Inventory

| Facade group | Current role | Shrink posture |
|---|---|---|
| `storage`, `layout`, `error`, `runtime`, `cluster`, `rpc` | Compatibility spine for storage, topology, runtime handles, cluster control, and internode calls. | Keep until replacement contracts compile in downstream boundary files. |
| `bucket` | Domain facade consumed through owner-local `storage_api` boundaries. The public API keeps compatibility paths but exposes explicit submodules and symbol lists instead of whole bucket owner modules. | Keep explicit lists aligned with owner-boundary consumers; do not restore whole-module passthroughs. |
| `client`, `config`, `disk`, `tier` | Compatibility paths consumed through owner-local `storage_api` boundaries. The public API keeps existing path names but exposes explicit nested submodules and symbol lists instead of whole owner modules. | Keep explicit lists aligned with owner-boundary consumers; do not restore whole-module passthroughs. |
| `data_usage`, `capacity`, `notification`, `metrics`, `rebalance` | Domain and service facades still consumed through owner-local `storage_api` boundaries. | Narrow one group at a time after explicit aliases or wrappers exist. |
| `set_disk`, `object`, `rio`, `bitrot`, `erasure`, `compression`, `cache`, `store_list` | Low-level object IO, reader, erasure, cache, and migration helper compatibility. | Keep stable while `SetDisks` remains the shared state carrier. |
| `admin`, `event`, `global` | Admin, event hook, and legacy global compatibility. | Keep `global` limited to bootstrap writes and lifecycle controls; read-only runtime access must use runtime-source contracts. |

## External Consumer Boundaries

External `rustfs_ecstore::api` imports must stay in these local boundary files:

| Boundary file | Current facade families |
|---|---|
| `rustfs/src/storage/storage_api.rs` | Broad RustFS storage owner bridge for admin, explicit bucket facade submodules, capacity, client, compression, cluster, config, data usage, disk, error, event, global bootstrap controls, runtime-source getters, layout, metrics, notification, rebalance, rio, rpc, set disk, storage, and tier. Replication pool/stat handles are projected into RustFS-local wrapper types here. |
| `crates/scanner/src/storage_api.rs` | Scanner bridge for bucket lifecycle, replication, metadata, capacity, config, data usage, disk, error, runtime, set disk, storage, and tier. Replication queue config, admission, and heal object DTOs are projected into scanner-local types here. |
| `crates/obs/src/metrics/storage_api.rs` | Metrics bridge for bucket bandwidth, lifecycle, replication, quota, capacity, data usage, error, runtime, and storage. |
| `crates/iam/src/storage_api.rs` | IAM bridge for config, error, notification, runtime, and storage. |
| `crates/heal/src/heal/storage_api.rs` | Heal bridge for data usage, disk, error, runtime, and storage. |
| `crates/notify/src/storage_api.rs` | Notification bridge for config, runtime, and storage. |
| `crates/protocols/src/swift/storage_api.rs` | Swift bridge for bucket metadata, bucket metadata system, error, runtime, and storage. |
| `crates/s3select-api/src/storage_api.rs` | S3 Select bridge for error, runtime, set disk, and storage. |
| `crates/e2e_test/src/storage_api.rs` | E2E harness bridge for bucket targets, disk walking, and RPC helpers. |
| `crates/heal/tests/*/storage_api.rs`, `crates/scanner/tests/storage_api/mod.rs`, `fuzz/fuzz_targets/*_storage_api.rs` | Test and fuzz bridges for the same compatibility seams under test. |

New production imports outside these boundary files are migration drift. Add a
local boundary or storage-api contract first, then route consumers through it.

## Split Dependency Inventory

| Candidate | ECStore dependencies that block a crate split | Required owner contracts before movement |
|---|---|---|
| Lifecycle | Object API, `ECStore`, `SetDisks`, runtime sources/globals, bucket metadata/versioning/object lock/replication, disk, config, notification, audit, and tier services. | `LifecycleObjectStore`, `LifecycleMetadataStore`, `LifecycleRuntime`, `LifecycleReplicationSink`, and `LifecycleAuditSink`. |
| Replication | Bucket target and metadata systems, bucket target client config, disk, object API, runtime sources, notification, and SetDisks lock timing. | `ReplicationStorage`, `ReplicationMetadataStore`, `ReplicationRuntime`, `ReplicationEventSink`, and `ReplicationLifecycleBridge`. |
| SetDisks | Shared disks, endpoints, format state, namespace locks, cache, and implementations for object IO, namespace locking, bucket, object, list, multipart, and heal operations. | Pure shard source, disk error, bitrot IO, namespace lock, metrics label, and file metadata contracts before any operation family moves. |

`crates/ecstore/tests/ecstore_contract_compat_test.rs` keeps compile-time
coverage for `ECStore` and `SetDisks` storage-api trait compatibility before
any facade shrink or operation-family movement.

## Shrink Rules

1. Do not remove a facade item until its downstream boundary has compile-time
   coverage or a documented replacement.
2. Do not add direct `rustfs_ecstore::api` imports outside the boundary files
   listed above.
3. Do not split lifecycle or replication into crates while they depend on
   ECStore runtime state, queues, notification, audit, scanner, or SetDisks
   internals.
4. Do not replace `SetDisks` with multiple runtime structs in one PR. Move one
   operation family only after contracts and focused tests exist.
5. Remove or narrow one facade group per PR so rollback preserves object IO,
   quorum, lifecycle/replication queues, scanner repair, notification/audit
   events, and metadata compatibility.
6. Keep `api::bucket`, `api::client`, `api::config`, `api::disk`, and
   `api::tier` on explicit submodules and symbol lists; do not restore
   `pub use crate::<owner>::{...}` whole-module passthroughs for those groups.

## First PR Checklist

- inventory the facade group and all external boundary consumers;
- add explicit aliases or wrappers before deleting any broad passthrough;
- run `./scripts/check_architecture_migration_rules.sh`;
- run focused compile or tests for the touched owner boundary;
- keep runtime behavior unchanged unless the PR is explicitly a code-bearing
  follow-up with its own rollback plan.
