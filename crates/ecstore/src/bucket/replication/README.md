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
| `replication_object_decision_boundary.rs` | Object replication option DTOs, resync target projection, delete replication decisions, and multipart planning helpers. | Keeps ECStore runtime modules from importing object decision contracts directly from `rustfs-replication`. |
| `replication_pool.rs` | Replication queue, worker pool, MRF persistence, bucket stats, and delete/object scheduling. | Depends on bucket target sys, bucket metadata sys, metadata paths, queue contracts through the queue boundary, file metadata replication contracts through local boundaries, config storage, storage contracts through the replication storage boundary, runtime sources, and notification state. |
| `replication_proxy.rs` | Proxy-target selection for GET/HEAD/Tagging reads of objects not yet replicated locally (MinIO `getProxyTargets` parity: anti-loop, version-suspended, and no-config empty branches). | Uses replication config lookup, rule matching, and target clients through local boundaries. |
| `replication_queue_boundary.rs` | Queue/admission DTOs, heal queue DTOs, worker sizing, and backpressure helpers. | Keeps ECStore runtime modules from importing queue/backpressure contracts directly from `rustfs-replication`. |
| `replication_resync_boundary.rs` | Resync DTOs, status classifiers, persisted resync/MRF codec wrappers, and ECStore error mapping. | Keeps ECStore runtime modules from importing resync contract helpers directly from `rustfs-replication`. |
| `replication_resyncer.rs` | Object replication, delete replication, resync execution, target calls, and multipart target upload paths. | Depends on target calls and target config types through the replication target boundary, metadata paths and metadata systems through the replication metadata boundary, file metadata replication contracts through the filemeta boundary, object decisions and multipart planning through the object decision boundary, resync contracts through the resync boundary, queue DTOs through the queue boundary, error contracts through the error boundary, versioning systems, storage contracts through the replication storage boundary, config-derived storage class labels through the config store, runtime sources, notification events and local event host selection through the event sink, bandwidth reader wrapping, and SetDisks lock timing. |
| `replication_state.rs` | Replication queue/stat state and worker accounting. | Reads stats DTOs through the stats boundary, runtime sources, file metadata replication contracts, error contracts, and bucket monitor handles through local boundaries, and owns shared replication pool/stat state. |
| `replication_stats_boundary.rs` | Bucket replication stats DTOs, queue/proxy metric caches, and worker metric snapshots. | Keeps ECStore runtime modules from importing stats contracts directly from `rustfs-replication`. |
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
| `EcstoreReplicationBoundaryImports` | ECStore-side imports from `rustfs-replication`. | Direct `rustfs-replication` imports under `crates/ecstore/src/bucket/replication` stay in `*_boundary.rs` modules, including config and resync facade re-exports. |
| `RuntimeReplicationFacadeConsumers` | Runtime owner consumers of replication DTOs and status types. | Scanner, admin, and storage owner facades import replication DTOs/status types through `rustfs-ecstore`; app storage keeps the remaining direct object/delete helper calls behind its local storage API boundary. |
| `ReplicationResyncContracts` | Resync options, target status, bucket status, status classifiers, and persisted resync/MRF status wire format. | Owned by `crates/replication`; ECStore imports them through `replication_resync_boundary.rs`, which maps crate errors to ECStore errors. |
| `ReplicationCrateFileMetaIndependence` | Replication status, decision, MRF, resync, and target-reset wire contracts owned by `rustfs-replication`. | `crates/replication/src/filemeta.rs` owns these contracts; `rustfs-replication` must not import or depend on `rustfs-filemeta`. |
| `ReplicationConfigStore` | Replication config persistence and config-derived labels used by target options. | Config read/save helpers and storage class labels are exposed through the contract type in `replication_config_store.rs`. |
| `ReplicationFileMeta` | ECStore compatibility conversions for filemeta replication state/status. | `rustfs_filemeta` to `rustfs_replication` conversions are concentrated in `replication_filemeta_boundary.rs`; `FileInfo` remains in the storage boundary for storage trait bindings and walk options. |
| `StorageApiReplicationContracts` | Storage-api delete DTO replication state/status helpers. | Storage-api owner DTOs keep their local replication boundary; ECStore converts them in `replication_storage_boundary.rs` before queueing replication work. |
| `ReplicationCrateStorageApiIndependence` | Delete work DTOs consumed by `rustfs-replication`. | `crates/replication/src/storage_api.rs` owns these DTOs; `rustfs-replication` must not import or depend on `rustfs-storage-api`. |
| `ReplicationObjectDecisionContracts` | Object replication options, delete replication decisions, resync target projection, multipart planning, and delete-marker retry classifiers. | Owned by `crates/replication`; ECStore imports them through `replication_object_decision_boundary.rs`. |
| `ReplicationQueueContracts` | Queue admission, heal queue results/actions, worker operations, worker sizing, and backpressure decisions. | Owned by `crates/replication`; ECStore imports them through `replication_queue_boundary.rs`. |
| `ReplicationStatsContracts` | Bucket stats, replication target stats, queue/proxy metrics, and worker metric snapshots. | Owned by `crates/replication`; ECStore imports them through `replication_stats_boundary.rs`. |
| `ReplicationErrorBoundary` | ECStore error/result contracts and replication-specific error classifiers. | `crate::error` imports are concentrated in `replication_error_boundary.rs`. |
| `ReplicationTargetStore` | Bucket target listing, target client lookup, target offline checks, target config types, target operation option types, and target HeadObject comparison adapters. | Bucket target sys access, `BucketTargets`, target operation types, and HeadObject-to-replication DTO adapters are exposed through the contract type in `replication_target_boundary.rs`. |
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
11. Keep storage-api replication status/state helpers behind
    `crates/storage-api/src/replication.rs`; ECStore converts owner DTOs at the
    replication storage boundary.
12. Keep `rustfs-replication` independent from `rustfs-filemeta`; ECStore
    compatibility conversions live in `replication_filemeta_boundary.rs`.
13. Keep `rustfs-replication` independent from `rustfs-storage-api`; ECStore
    compatibility conversions live in `replication_storage_boundary.rs`.
14. Keep direct `rustfs-replication` imports inside ECStore replication
    concentrated in `*_boundary.rs` modules.
15. Keep scanner, admin, and storage-owner replication status/DTO consumers
    behind the ECStore replication facade; only `rustfs/src/app/storage_api.rs`
    may retain direct object/delete replication helper calls.

## Completion Criteria

The split is complete when the "Current dependency to remove" column in the
Required Contracts table above is empty: every row is either deleted because
the dependency is gone, or reduced to "none". No other signal — file count,
boundary count, line count — measures completion.

Target end state:

- `replication_pool.rs`, `replication_resyncer.rs`, and `replication_state.rs`
  move into `crates/replication` behind the contracts above;
- the `*_boundary.rs` and `*_bridge.rs` micro-files dissolve naturally as the
  code they fence moves across the crate boundary. They are the mechanical
  seams of the migration ratchet — the architecture guard scripts anchor on
  their file names — so batch-merging them beforehand is explicitly rejected:
  it forces synchronized guard-script/mod/import churn with zero functional
  gain;
- `datatypes.rs` retired early (its sanctioned exception): it was a pure
  relay (`boundary -> datatypes -> mod.rs`), so the facade now re-exports
  `ResyncStatusType` from the resync boundary directly and the relay file is
  deleted. Note the original retirement wording ("consumers import through
  `rustfs-replication` directly") conflicted with Migration Rule #15 —
  consumers stay behind the ECStore facade; only the relay hop dissolves.

## Milestones

| Milestone | Scope | Status |
|---|---|---|
| M0 | Record the completion criteria and end state (this section). | Done |
| M1 | Contract extraction: resync/queue/stats/object-decision/filemeta/storage wire contracts owned by `crates/replication`; ECStore imports concentrated in `*_boundary.rs`; event sink and runtime access behind local contracts. | Done — see Required Contracts |
| M2 | Move resyncer pure decision logic (no IO) into `crates/replication`. | Done — moved the pure decision helpers with their unit tests: `resync_status_duration` (resync), `resync_existing_delete_replication_info` / `replicate_delete_outcome` / `target_delete_version_id` / `delete_marker_purge_version_id` / `delete_marker_purge_mrf_entry` (delete), `version_identity_drifted` / `is_replication_target_offline_error` / the SSE-C passthrough gate family incl. `SsecPassthroughCapability` (object; `ssec_passthrough_evidence_present` was param-demoted to the echoed customer-algorithm string, ECStore keeps the `HeadObjectOutput` adapter). ECStore imports them through the resync/object-decision/target boundaries; `bucket_target_sys` keeps only the verdict cache + TTL and re-exports the capability enum. Not moved (signatures carry ECStore or aws-sdk types): `verify_resync_head_result`, `resync_target_error_detail`, the `SdkError` classifiers (`has_raw_status`, `is_version_id_format_mismatch`), the `replicate_all_*` option/info builders, and `bounded_resync_max_jobs` (itself a pure clamp, but it forms one local configuration unit with the env-reading `configured_resync_max_jobs` and its ECStore-local constants — moving the clamp alone has negative value). |
| M3 | Move the worker runtime (`replication_pool.rs`, the IO paths of `replication_resyncer.rs`, `replication_state.rs`) once the contract traits are stable. Highest-risk step of the whole plan; do it last. | Pending |
| M4 | Retire the boundary modules together with their guard-script entries. | Pending (`datatypes.rs` already retired early alongside M2) |

The original first code-bearing step (narrow `ReplicationEventSink` /
`ReplicationRuntime` contracts) has landed — `replication_event_sink.rs`
exists and runtime access goes through local boundary aliases — so new work
starts from M2.

Current compatibility guard: `crates/ecstore/tests/replication_facade_compat_test.rs`
keeps the ECStore replication facade types covered while architecture rules
keep direct imports behind local `storage_api` boundaries.
