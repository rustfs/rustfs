# ECStore Replication Split Inventory

This directory still owns the ECStore replication workers. The resync status
contracts and wire format now live in `rustfs-replication`, while worker runtime
code still depends on ECStore object IO, bucket target clients, bucket metadata
systems, runtime state, notification events, and lifecycle/heal scheduling
paths.

## Current Modules

| Module | Current role | Split blocker |
|---|---|---|
| `config.rs` | Replication config helpers, rule matching, and tag filtering. | Uses replication-local filemeta/tagging boundaries and S3 DTOs directly. |
| `datatypes.rs` | ECStore compatibility re-export for resync status enums. | Re-exports `rustfs-replication` contracts while downstream facade consumers migrate. |
| `replication_pool.rs` | Replication queue, worker pool, MRF persistence, bucket stats, and delete/object scheduling. | Depends on bucket target sys, bucket metadata sys, metadata paths, and file metadata replication contracts through local boundaries, config storage, storage contracts through the replication storage boundary, runtime sources, and notification state. |
| `replication_resyncer.rs` | Object replication, delete replication, resync execution, MRF encode/decode, target calls, and multipart target upload paths. | Depends on target calls and target config types through the replication target boundary, metadata paths and metadata systems through the replication metadata boundary, file metadata replication contracts through the filemeta boundary, error contracts through the error boundary, versioning systems, storage contracts through the replication storage boundary, config-derived storage class labels through the config store, runtime sources, notification events and local event host selection through the event sink, bandwidth reader wrapping, and SetDisks lock timing. |
| `replication_state.rs` | Replication queue/stat state and worker accounting. | Reads runtime sources, file metadata replication contracts, error contracts, and bucket monitor handles through local boundaries, and owns shared replication pool/stat state. |
| `replication_lifecycle_bridge.rs` | Lifecycle-originated delete replication admission and version-purge state construction. | Depends on replication config/rule matching, delete-replication decisions, and replication delete scheduling through a local contract type. |
| `replication_migration_bridge.rs` | Bucket migration access to persisted replication resync codec helpers. | Keeps migration normalization behind a bridge instead of re-exporting resyncer codec helpers. |
| `replication_object_bridge.rs` | App and SetDisks object replication decisions plus object/delete scheduling. | Keeps object write/delete replication call sites behind a bridge instead of exporting low-level resyncer and pool helpers. |
| `replication_scanner_bridge.rs` | Scanner-originated replication heal admission. | Keeps scanner-facing heal queueing behind a local contract type instead of exporting the internal queue function directly. |
| `replication_target_config_bridge.rs` | Bucket target removal checks against replication target rules. | Keeps bucket target sys from importing replication config helper types directly. |
| `rule.rs` | Rule evaluation helpers for object replication options. | Depends on ECStore replication object option types. |
| `mod.rs` | Explicit compatibility re-export facade for the current ECStore owner. | Wildcard re-exports are guarded so internal helpers do not leak back into the public facade. |

## Required Contracts

| Contract | Responsibility | Current dependency to remove |
|---|---|---|
| `ReplicationObjectIO` | Object read/write primitives used by config, MRF, resync status, and multipart replication paths. | ECStore object API reader/writer types and storage-api object IO contracts are concentrated in `replication_storage_boundary.rs`. |
| `ReplicationStorage` | Object read/write/delete, object walk, metadata update, and target object IO. | ECStore object API, storage-api contracts, and read option types are concentrated in `replication_storage_boundary.rs`. |
| `ReplicationMetadataStore` | Replication config, MRF/resync state, target reset headers, and status persistence. | Metadata sys access and replication metadata path constants are exposed through the contract type in `replication_metadata_boundary.rs`; versioning sys and config storage imports remain separate contracts. |
| `ReplicationResyncContracts` | Resync options, target status, bucket status, status enum, and persisted resync status wire format. | Owned by `crates/replication`; ECStore maps its error type at the resyncer boundary. |
| `ReplicationConfigStore` | Replication config persistence and config-derived labels used by target options. | Config read/save helpers and storage class labels are exposed through the contract type in `replication_config_store.rs`. |
| `ReplicationFileMeta` | Replication status, decisions, MRF entries, resync decisions, and target reset helpers. | `rustfs_filemeta` replication contracts are concentrated in `replication_filemeta_boundary.rs`; `FileInfo` remains in the storage boundary for storage trait bindings and walk options. |
| `ReplicationErrorBoundary` | ECStore error/result contracts and replication-specific error classifiers. | `crate::error` imports are concentrated in `replication_error_boundary.rs`. |
| `ReplicationTargetStore` | Bucket target listing, target client lookup, target offline checks, target config types, and target operation option types. | Bucket target sys access, `BucketTargets`, and target operation types are exposed through the contract type in `replication_target_boundary.rs`. |
| `ReplicationRuntime` | Worker pool, queue sizing, stats, bucket monitor, local node identity, cancellation, and admission state. | Direct runtime source/global access and shared replication pool/stat state; ECStore object store and bucket monitor implementation types stay behind local storage/bandwidth boundaries. |
| `ReplicationBandwidthLimiter` | Target reader wrapping for replication bandwidth accounting and throttling. | Direct bucket bandwidth reader imports from resyncer paths. |
| `ReplicationEventSink` | Notification and audit events for skipped, failed, pending, and completed replication operations. | Event notification service calls and local event host selection are concentrated in `replication_event_sink.rs`. |
| `ReplicationVersioningStore` | Versioning state checks for object and delete replication decisions. | Bucket versioning sys access is exposed through the contract type in `replication_versioning_boundary.rs`. |
| `ReplicationLockTiming` | Namespace lock timing for replication resync, object replication, and delete replication locks. | SetDisks lock timeout access is exposed through the contract type in `replication_lock_boundary.rs`. |
| `ReplicationMsgpCodec` | MessagePack time encode/decode and unknown value skipping for persisted resync/MRF state. | Bucket MessagePack helpers are exposed through the contract type in `replication_msgp_boundary.rs`. |
| `ReplicationTagFilter` | Decode object tag strings for rule and metadata replication decisions. | Bucket tagging helper access is exposed through the contract type in `replication_tagging_boundary.rs`. |
| `ReplicationLifecycleBridge` | Lifecycle-originated delete and version-purge scheduling. | Lifecycle delete paths call the bridge contract in `replication_lifecycle_bridge.rs` instead of constructing replication delete work directly. |
| `ReplicationMigrationBridge` | Persisted resync status decode/encode access for bucket metadata migration. | Bucket migration calls the bridge contract in `replication_migration_bridge.rs` instead of importing internal resyncer codec helpers. |
| `ReplicationObjectBridge` | Object write/delete replication decision and scheduling entry point for app storage and SetDisks paths. | App and SetDisks object paths call the bridge contract in `replication_object_bridge.rs` instead of importing internal resyncer/pool helpers. |
| `ReplicationScannerBridge` | Scanner-originated replication heal scheduling. | Scanner heal paths call the bridge contract in `replication_scanner_bridge.rs` instead of importing the internal queue function directly. |
| `ReplicationTargetConfigBridge` | Bucket target removal checks against replication target rules. | Bucket target sys calls the bridge contract in `replication_target_config_bridge.rs` instead of importing replication config helper types directly. |

## Migration Rules

1. Do not move `bucket/replication` into a new crate while workers import
   bucket target sys, metadata sys, runtime sources, bandwidth reader,
   notification services, or SetDisks lock timing directly.
2. Keep existing queue behavior, MRF persistence, resync state, target client
   semantics, notification/audit events, and scanner/heal classifications
   unchanged during inventory and contract PRs.
3. Keep the current `ReplicationStorage` trait as the starting point. Split it
   only after call sites prove a narrower object read/write/delete/walk shape.
4. Preserve `rustfs_ecstore::api::bucket::replication` compatibility until
   lifecycle, scanner, OBS, heal, and tests compile through replacement paths.
5. Keep imports between modules in this directory relative to the local
   replication module, not `crate::bucket::replication::*` self paths.
6. Keep runtime source access from importing ECStore object store or bucket
   monitor implementation types directly; use local boundary-owned aliases.
7. Move at most one owner boundary per code-bearing PR and verify it with
   focused replication tests before broad gates. Non-behavioral contract-shape
   cleanup may batch already-established boundary wrappers when the owner and
   call semantics do not change.
8. Keep the compatibility facade in `mod.rs` as an explicit symbol list. Do not
   reintroduce wildcard re-exports for replication implementation modules.
9. Keep object write/delete replication helpers behind `ReplicationObjectBridge`;
   do not export internal resyncer or pool scheduling helpers through the
   compatibility facade.
10. Keep ECStore owner modules outside `bucket/replication` behind bridge
    contracts when they need replication codec or config helper behavior.

## First Code-Bearing Step

Start with `ReplicationRuntime` or `ReplicationEventSink`. Both can be added as
narrow internal contracts while keeping current queue, MRF, resync, and target
behavior unchanged. Do not start with a crate move.

Current compatibility guard: `crates/ecstore/tests/replication_facade_compat_test.rs`
keeps the ECStore replication facade types covered while architecture rules
keep direct imports behind local `storage_api` boundaries.
