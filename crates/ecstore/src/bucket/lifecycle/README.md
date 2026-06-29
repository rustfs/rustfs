# ECStore Lifecycle Split Inventory

This directory still belongs to ECStore. It is not ready to become a standalone
crate because lifecycle workers currently depend on ECStore runtime state,
object IO, bucket metadata, replication scheduling, notification/audit sinks,
and tier services.

## Current Modules

| Module | Current role | Split blocker |
|---|---|---|
| `core.rs` | Lifecycle rule model, action evaluation, object options, and transition/expiry decisions. | Uses ECStore object metadata types and compatibility DTO re-exports. |
| `bucket_lifecycle_ops.rs` | Worker orchestration, expiry, transition, stale multipart cleanup, audit, replication delete scheduling, and queue state. | Depends on `ECStore`, `SetDisks`, runtime globals, bucket metadata/versioning/replication, object-lock checks, disk internals, event notification, and tier services. |
| `evaluator.rs` | Bucket lifecycle evaluation wrapper. | Uses lifecycle-local object-lock boundary and still reads replication config from ECStore bucket modules. |
| `rule.rs` | Lifecycle rule filter helpers. | Uses lifecycle-local tagging boundary. |
| `tier_delete_journal.rs` | Remote tier delete journal persistence and recovery. | Uses lifecycle-local config persistence boundary, object IO contracts, metadata bucket paths, and `ECStore`. |
| `tier_free_version_recovery.rs` | Free-version recovery queue and object restoration path. | Depends on `ECStore`, object metadata, storage-api contracts, and lifecycle queue callbacks. |
| `tier_last_day_stats.rs` | Tier statistics helpers. | Pure data/stat logic, but still part of lifecycle worker reporting. |
| `tier_sweeper.rs` | Remote tier deletion worker and transition cleanup. | Depends on runtime sources, `ECStore`, tier journal persistence, signer-error handling, and lifecycle object options. |
| `bucket_lifecycle_audit.rs` | Lifecycle audit event source labels. | Must remain wired to lifecycle audit and notification sinks. |

## Required Contracts

| Contract | Responsibility | Current dependency to remove |
|---|---|---|
| `LifecycleObjectStore` | Object stat, delete, transition, restore, multipart cleanup, and version-aware metadata operations. | Direct `ECStore`, `SetDisks`, disk, and object API access in worker paths. |
| `LifecycleMetadataStore` | Lifecycle, object-lock, replication, bucket versioning, and stale multipart metadata reads. | Direct bucket metadata, object-lock, versioning, and replication module imports. |
| `LifecycleRuntime` | Expiry state, transition state, tier config, deployment ID, local node name, queue metrics, cancellation, and worker sizing. | Direct runtime source/global access and process environment reads inside worker code. |
| `LifecycleConfigStore` | Persist, read, and remove lifecycle-owned journal/config objects. | Direct ECStore config persistence helper imports from worker paths. |
| `LifecycleTagFilter` | Decode object tag strings for lifecycle rule matching. | Direct bucket tagging helper imports from lifecycle rule paths. |
| `LifecycleObjectLockStore` | Object-lock retention and deletion checks used by lifecycle evaluation and worker deletion paths. | Direct object-lock module imports from lifecycle code. |
| `LifecycleReplicationSink` | Lifecycle-originated delete and version-purge replication scheduling. | Direct imports from bucket replication modules. |
| `LifecycleAuditSink` | Lifecycle audit and notification event emission. | Direct event notification service calls and audit-side effects from worker code. |

## Migration Rules

1. Do not move `bucket/lifecycle` into a new crate while any worker imports
   `crate::store::ECStore`, `crate::set_disk::SetDisks`, runtime globals, or
   bucket replication internals directly.
2. Extract pure contracts before runtime movement. The first code-bearing PR
   should introduce one contract boundary and keep the old ECStore call path.
3. Preserve lifecycle queue behavior, transition/expiry state, replication
   delete scheduling, notification/audit events, scanner-visible metrics, and
   stale multipart cleanup semantics.
4. Keep `rustfs_ecstore::api::bucket::lifecycle` compatibility until scanner,
   OBS, and test boundary files compile through replacement paths.
5. Verify each code-bearing step with focused lifecycle tests before attempting
   broad gates.

## First Code-Bearing Step

Start with `LifecycleRuntime` or `LifecycleAuditSink`. Both can be introduced
as narrow internal contracts while keeping the current ECStore worker behavior
unchanged. Do not start with a crate move.

Current first boundary: `runtime_boundary.rs` centralizes lifecycle access to
runtime state while preserving the existing ECStore-backed implementations.
`config_boundary.rs` centralizes lifecycle-owned config object persistence for
tier delete journal recovery while preserving the existing ECStore config store.
`tagging_boundary.rs` centralizes lifecycle tag decoding while preserving the
existing ECStore bucket tagging implementation.
`object_lock_boundary.rs` centralizes lifecycle object-lock checks while
preserving the existing ECStore object-lock implementation.
