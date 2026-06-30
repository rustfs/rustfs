# ECStore Replication Split Inventory

This directory still belongs to ECStore. It is not ready to become a standalone
crate because replication workers currently depend on ECStore object IO, bucket
target clients, bucket metadata systems, runtime state, notification events,
and lifecycle/heal scheduling paths.

## Current Modules

| Module | Current role | Split blocker |
|---|---|---|
| `config.rs` | Replication config helpers, rule matching, and tag filtering. | Uses replication-local tagging boundary and S3 DTOs directly. |
| `datatypes.rs` | Replication status and operation DTOs. | Publicly re-exported through the ECStore replication facade. |
| `replication_pool.rs` | Replication queue, worker pool, MRF persistence, bucket stats, and delete/object scheduling. | Depends on bucket target sys, bucket metadata sys, config storage, object API, runtime sources, notification state, and storage-api object IO contracts. |
| `replication_resyncer.rs` | Object replication, delete replication, resync, MRF encode/decode, target calls, and multipart target upload paths. | Depends on bucket target clients, metadata/versioning systems, ECStore object readers/writers, disk paths, runtime sources, notification events, and SetDisks lock timing. |
| `replication_state.rs` | Replication queue/stat state and worker accounting. | Reads runtime sources and owns shared replication pool/stat state. |
| `rule.rs` | Rule evaluation helpers for object replication options. | Depends on ECStore replication object option types. |
| `mod.rs` | Compatibility re-export facade for the current ECStore owner. | Must stay stable until downstream scanner, lifecycle, heal, and metrics paths compile through replacement contracts. |

## Required Contracts

| Contract | Responsibility | Current dependency to remove |
|---|---|---|
| `ReplicationStorage` | Object read/write/delete, object walk, metadata update, and target object IO. | Direct ECStore object API and storage-api object IO coupling in worker paths. |
| `ReplicationMetadataStore` | Replication config, bucket targets, MRF/resync state, target reset headers, and status persistence. | Direct bucket target sys, metadata sys, versioning sys, config storage, and file metadata imports. |
| `ReplicationRuntime` | Worker pool, queue sizing, stats, bucket monitor, local node identity, cancellation, and admission state. | Direct runtime source/global access and shared replication pool/stat state. |
| `ReplicationEventSink` | Notification and audit events for skipped, failed, pending, and completed replication operations. | Direct event notification service calls from worker code. |
| `ReplicationTagFilter` | Decode object tag strings for rule and metadata replication decisions. | Direct bucket tagging helper imports from replication workers. |
| `ReplicationLifecycleBridge` | Lifecycle-originated delete and version-purge scheduling. | Direct coupling between lifecycle worker paths and replication delete scheduling. |

## Migration Rules

1. Do not move `bucket/replication` into a new crate while workers import
   bucket target sys, metadata sys, runtime sources, notification services, or
   SetDisks lock timing directly.
2. Keep existing queue behavior, MRF persistence, resync state, target client
   semantics, notification/audit events, and scanner/heal classifications
   unchanged during inventory and contract PRs.
3. Keep the current `ReplicationStorage` trait as the starting point. Split it
   only after call sites prove a narrower object read/write/delete/walk shape.
4. Preserve `rustfs_ecstore::api::bucket::replication` compatibility until
   lifecycle, scanner, OBS, heal, and tests compile through replacement paths.
5. Move at most one contract boundary per code-bearing PR and verify it with
   focused replication tests before broad gates.

## First Code-Bearing Step

Start with `ReplicationRuntime` or `ReplicationEventSink`. Both can be added as
narrow internal contracts while keeping current queue, MRF, resync, and target
behavior unchanged. Do not start with a crate move.

Current compatibility guard: `crates/ecstore/tests/replication_facade_compat_test.rs`
keeps the ECStore replication facade types covered while architecture rules
keep direct imports behind local `storage_api` boundaries.
