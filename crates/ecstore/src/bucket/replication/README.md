# ECStore Replication Split Inventory

This directory still belongs to ECStore. It is not ready to become a standalone
crate because replication workers currently depend on ECStore object IO, bucket
target clients, bucket metadata systems, runtime state, notification events,
and lifecycle/heal scheduling paths.

## Current Modules

| Module | Current role | Split blocker |
|---|---|---|
| `config.rs` | Replication config helpers, rule matching, and tag filtering. | Uses replication-local filemeta/tagging boundaries and S3 DTOs directly. |
| `datatypes.rs` | Replication status and operation DTOs. | Publicly re-exported through the ECStore replication facade. |
| `replication_pool.rs` | Replication queue, worker pool, MRF persistence, bucket stats, and delete/object scheduling. | Depends on bucket target sys, bucket metadata sys, metadata paths, and file metadata replication contracts through local boundaries, config storage, storage contracts through the replication storage boundary, runtime sources, and notification state. |
| `replication_resyncer.rs` | Object replication, delete replication, resync, MRF encode/decode, target calls, and multipart target upload paths. | Depends on target calls and target config types through the replication target boundary, metadata paths and metadata systems through the replication metadata boundary, file metadata replication contracts through the filemeta boundary, error contracts through the error boundary, versioning systems, storage contracts through the replication storage boundary, config-derived storage class labels through the config store, runtime sources, notification events and local event host selection through the event sink, bandwidth reader wrapping, and SetDisks lock timing. |
| `replication_state.rs` | Replication queue/stat state and worker accounting. | Reads runtime sources, file metadata replication contracts, error contracts, and bucket monitor handles through local boundaries, and owns shared replication pool/stat state. |
| `rule.rs` | Rule evaluation helpers for object replication options. | Depends on ECStore replication object option types. |
| `mod.rs` | Compatibility re-export facade for the current ECStore owner. | Must stay stable until downstream scanner, lifecycle, heal, and metrics paths compile through replacement contracts. |

## Required Contracts

| Contract | Responsibility | Current dependency to remove |
|---|---|---|
| `ReplicationObjectIO` | Object read/write primitives used by config, MRF, resync status, and multipart replication paths. | ECStore object API reader/writer types and storage-api object IO contracts are concentrated in `replication_storage_boundary.rs`. |
| `ReplicationStorage` | Object read/write/delete, object walk, metadata update, and target object IO. | ECStore object API, storage-api contracts, and read option types are concentrated in `replication_storage_boundary.rs`. |
| `ReplicationMetadataStore` | Replication config, MRF/resync state, target reset headers, and status persistence. | Metadata sys access and replication metadata path constants are concentrated in `replication_metadata_boundary.rs`; versioning sys and config storage imports remain. |
| `ReplicationConfigStore` | Replication config persistence and config-derived labels used by target options. | Config read/save helpers and storage class labels are concentrated in `replication_config_store.rs`. |
| `ReplicationFileMeta` | Replication status, decisions, MRF entries, resync decisions, and target reset helpers. | `rustfs_filemeta` replication contracts are concentrated in `replication_filemeta_boundary.rs`; `FileInfo` remains in the storage boundary for storage trait bindings and walk options. |
| `ReplicationErrorBoundary` | ECStore error/result contracts and replication-specific error classifiers. | `crate::error` imports are concentrated in `replication_error_boundary.rs`. |
| `ReplicationTargetStore` | Bucket target listing, target client lookup, target offline checks, target config types, and target operation option types. | Bucket target sys access, `BucketTargets`, and target operation types are concentrated in `replication_target_boundary.rs`. |
| `ReplicationRuntime` | Worker pool, queue sizing, stats, bucket monitor, local node identity, cancellation, and admission state. | Direct runtime source/global access and shared replication pool/stat state; ECStore object store and bucket monitor implementation types stay behind local storage/bandwidth boundaries. |
| `ReplicationBandwidthLimiter` | Target reader wrapping for replication bandwidth accounting and throttling. | Direct bucket bandwidth reader imports from resyncer paths. |
| `ReplicationEventSink` | Notification and audit events for skipped, failed, pending, and completed replication operations. | Event notification service calls and local event host selection are concentrated in `replication_event_sink.rs`. |
| `ReplicationTagFilter` | Decode object tag strings for rule and metadata replication decisions. | Direct bucket tagging helper imports from replication workers. |
| `ReplicationLifecycleBridge` | Lifecycle-originated delete and version-purge scheduling. | Direct coupling between lifecycle worker paths and replication delete scheduling. |

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
7. Move at most one contract boundary per code-bearing PR and verify it with
   focused replication tests before broad gates.

## First Code-Bearing Step

Start with `ReplicationRuntime` or `ReplicationEventSink`. Both can be added as
narrow internal contracts while keeping current queue, MRF, resync, and target
behavior unchanged. Do not start with a crate move.

Current compatibility guard: `crates/ecstore/tests/replication_facade_compat_test.rs`
keeps the ECStore replication facade types covered while architecture rules
keep direct imports behind local `storage_api` boundaries.
