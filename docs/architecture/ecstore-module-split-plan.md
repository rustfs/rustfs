# ECStore Module Split Plan

This plan records the remaining ECStore split work after the final audit
remediation pass. It is intentionally design-only: no crate movement should
start until each candidate boundary has explicit contracts, compatibility
coverage, dependency evidence, and rollback steps.

## Current Shape

| Area | Current owner | Size | Split status |
|---|---|---:|---|
| Bucket lifecycle | `crates/ecstore/src/bucket/lifecycle/` | 8,657 lines | Proposal only |
| Bucket replication | `crates/ecstore/src/bucket/replication/` | 8,730 lines | Proposal only |
| Set disks | `crates/ecstore/src/set_disk/` | state carrier plus operation modules | Keep in ECStore |
| Public ECStore facade | `crates/ecstore/src/api/mod.rs` | broad compatibility surface | Shrink only through guarded PRs |

The file split inside `set_disk/` is already operation-oriented: read, write,
list, multipart, lock, heal, and replication code live in separate modules.
The remaining large surface is the shared `SetDisks` state and cross-cutting
contracts, not only file layout.

## Non-Negotiable Rules

- Do not split crates in the same PR that moves runtime state or changes
  startup behavior.
- Do not change object placement, quorum, reader semantics, lifecycle queues,
  replication queues, notification dispatch, audit events, or scanner repair
  behavior during inventory and contract PRs.
- Do not expose new direct ECStore internals to outer crates; use the existing
  storage-api and owner-local facade boundaries.
- Keep `rustfs_ecstore::api` compatibility visible until each consumer path has
  compile coverage and an explicit replacement.

## SetDisks Split Direction

Do not replace `SetDisks` with several runtime structs in one change. The safe
path is:

1. Keep `SetDisks` as the shared state carrier while operation modules continue
   to own read/write/list/multipart/lock/heal/replication behavior.
2. Extract pure contracts first: shard source, disk error, bitrot IO, namespace
   lock, metrics labels, and file metadata access.
3. Move one operation family only after its contracts are covered by focused
   tests and the facade compatibility path is explicit.
4. Preserve the old `rustfs_ecstore::api::set_disk` surface until downstream
   compatibility tests prove no caller depends on removed names.

The first executable SetDisks follow-up should be an inventory or guardrail PR,
not a runtime split PR.

## Lifecycle Candidate

`bucket/lifecycle` is not ready for a standalone crate yet.

Current coupling:

- lifecycle workers and transition state read ECStore runtime sources for
  object-store handles, expiry state, transition state, tier config, deployment
  IDs, and local node names;
- stale multipart cleanup depends on `SetDisks` internals and bucket metadata
  through the lifecycle metadata boundary;
- lifecycle expiry schedules bucket replication delete work through the
  replication lifecycle bridge contract;
- lifecycle evaluation shares S3 DTOs, object metadata, object lock, replication
  config reads through lifecycle-local boundaries, scanner metrics,
  notification/audit side effects, and tier services.

Required contracts before crate movement:

- `LifecycleObjectStore`: object stat, delete, transition, restore, multipart
  cleanup, and version-aware metadata operations needed by lifecycle workers.
- `LifecycleMetadataStore`: lifecycle, object-lock, replication, bucket
  versioning, and stale multipart metadata lookups without importing ECStore
  implementation modules. Current lifecycle config reads are concentrated in
  `crates/ecstore/src/bucket/lifecycle/metadata_boundary.rs`.
- `LifecycleRuntime`: expiry state, transition state, tier config, deployment
  ID, local node name, queue metrics, cancellation, and worker sizing.
- `LifecycleReplicationSink`: schedule lifecycle-originated replication deletes
  without depending on the replication implementation module.
- `LifecycleAuditSink`: lifecycle audit and notification emission boundary.

First safe PR:

- add a lifecycle extraction inventory section or module-level README;
- list current ECStore/runtime dependencies and the target contract owner for
  each dependency;
- add no code movement and no behavior changes.

The module-level inventory lives in
`crates/ecstore/src/bucket/lifecycle/README.md`.

Focused verification for the first code-bearing lifecycle PR:

- `cargo test -p rustfs-ecstore lifecycle --lib`
- `cargo check -p rustfs-ecstore --tests`
- `./scripts/check_architecture_migration_rules.sh`
- `git diff --check`

## Replication Candidate

`bucket/replication` is not ready for a standalone crate yet.

Current coupling:

- replication workers depend on `ReplicationStorage`, ECStore object APIs and
  storage-api contracts through the replication storage boundary, bucket target
  clients, bucket metadata, file metadata replication state through the
  filemeta boundary, config-derived storage class labels through the config
  store, scanner repair
  classification, runtime replication pool/stat handles, bucket monitor and
  bandwidth reader access through local boundaries, local node names, and
  notification events;
- resync and delete replication paths call metadata paths through the metadata
  boundary, while bucket target system access, target config types, and target
  operation types are concentrated behind the replication target boundary;
- lifecycle delete paths schedule replication work through
  `ReplicationLifecycleBridge`, while scanner heal paths schedule replication
  work through `ReplicationScannerBridge`, and app/SetDisks object write/delete
  paths use `ReplicationObjectBridge`;
- bucket metadata migration and bucket target removal checks use local
  replication bridges instead of importing resyncer codec or config helper
  internals;
- admin replication extension target filtering and resync request construction
  stay behind the admin storage boundary instead of exposing replication work
  DTO construction to handlers;
- app object and multipart writes call object-replication boundary helpers
  instead of constructing replication work DTOs or choosing object replication
  operation types at the use-case layer;
- global replication pool/stat initialization still lives with ECStore runtime
  compatibility state;
- modules inside `bucket/replication` use local relative paths rather than the
  ECStore owner path for replication self-imports;
- replication runtime source access uses storage/bandwidth boundary aliases for
  ECStore object store and bucket monitor implementation types;
- the ECStore replication facade in `mod.rs` uses explicit compatibility
  exports instead of wildcard re-exports from implementation modules.

Required contracts before crate movement:

- `ReplicationObjectIO`: object read/write primitives for config, MRF, resync
  status, and multipart replication paths. ECStore object API reader/writer
  types and storage-api object IO contracts are concentrated in
  `crates/ecstore/src/bucket/replication/replication_storage_boundary.rs`.
- `ReplicationStorage`: keep the existing trait as the starting point, then
  split object read/write/delete, walk, and metadata update responsibilities
  only when call sites prove a narrower shape. ECStore object API,
  storage-api contracts, and read option types are concentrated in
  `crates/ecstore/src/bucket/replication/replication_storage_boundary.rs`.
- `ReplicationMetadataStore`: replication config, target reset headers,
  MRF/resync state, and status persistence. Metadata sys access and replication
  metadata path constants are exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_metadata_boundary.rs`.
- `ReplicationConfigStore`: replication config persistence and config-derived
  labels used by target options. Config read/save helpers and storage class
  labels are exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_config_store.rs`.
- `ReplicationFileMeta`: replication status, decisions, MRF entries, resync
  decisions, and target reset helpers. `rustfs_filemeta` replication contracts
  are concentrated in
  `crates/ecstore/src/bucket/replication/replication_filemeta_boundary.rs`,
  while `FileInfo` remains in the storage boundary for storage trait bindings
  and walk options.
- `ReplicationErrorBoundary`: ECStore error/result contracts and
  replication-specific error classifiers. `crate::error` imports are
  concentrated in
  `crates/ecstore/src/bucket/replication/replication_error_boundary.rs`.
- `ReplicationTargetStore`: bucket target listing, target client lookup,
  target offline checks, target config types, and target operation option
  types. Bucket target sys access, `BucketTargets`, and target operation types
  are exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_target_boundary.rs`.
- `ReplicationRuntime`: pool, stats, worker admission, bucket monitor, local
  node identity, cancellation, and queue sizing. Concrete ECStore object store
  and bucket monitor types stay behind local storage/bandwidth boundaries.
- `ReplicationBandwidthLimiter`: target reader wrapping for replication
  bandwidth accounting and throttling.
- `ReplicationVersioningStore`, `ReplicationLockTiming`, `ReplicationMsgpCodec`,
  and `ReplicationTagFilter`: smaller state/codec/filter contracts that keep
  bucket versioning, SetDisks lock timing, MessagePack helpers, and bucket
  tagging helper access behind local replication boundary types.
- `ReplicationEventSink`: notification/audit events for skipped, failed, and
  completed replication operations, including local event host selection.
- `ReplicationLifecycleBridge`: lifecycle-originated delete and version-purge
  scheduling is exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_lifecycle_bridge.rs`.
- `ReplicationMigrationBridge`: persisted resync status decode/encode access
  for bucket metadata migration is exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_migration_bridge.rs`.
- `ReplicationObjectBridge`: app and SetDisks object write/delete replication
  decisions and scheduling are exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_object_bridge.rs`.
- `ObsReplicationStatsSnapshot`: observability reads replication bucket/site
  metrics through obs-local snapshot DTOs in
  `crates/obs/src/metrics/storage_api.rs` instead of carrying the ECStore
  replication stats handle through collectors.
- `ReplicationScannerBridge`: scanner-originated replication heal scheduling is
  exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_scanner_bridge.rs`.
  Scanner consumers receive scanner-local replication config/admission/heal
  object DTOs from `crates/scanner/src/storage_api.rs` instead of constructing
  or inspecting replication queue DTOs directly.
- `ReplicationTargetConfigBridge`: bucket target removal checks against
  replication target rules are exposed through the contract type in
  `crates/ecstore/src/bucket/replication/replication_target_config_bridge.rs`.
- `ReplicationFacade`: the current `rustfs_ecstore::api::bucket::replication`
  compatibility surface is an explicit symbol list guarded against wildcard
  re-exports while downstream owners migrate to narrower contracts.

First safe PR:

- add a replication extraction inventory section or module-level README;
- list current ECStore/runtime dependencies and the target contract owner for
  each dependency;
- keep global pool/stat initialization and queue behavior unchanged.

The module-level inventory lives in
`crates/ecstore/src/bucket/replication/README.md`.

Focused verification for the first code-bearing replication PR:

- `cargo test -p rustfs-ecstore replication --lib`
- `cargo check -p rustfs-ecstore --tests`
- `./scripts/check_architecture_migration_rules.sh`
- `git diff --check`

## Facade Shrink Plan

The broad `rustfs_ecstore::api` facade remains a compatibility boundary, not a
new architecture target. The current facade groups and external consumers are
recorded in
[`ecstore-api-facade-inventory.md`](ecstore-api-facade-inventory.md).
Shrinking it must be monotonic:

1. Inventory every public facade group and consumer.
2. Add compile-time coverage before removing or narrowing a facade item.
3. Move outer consumers to storage-api or owner-local compatibility boundaries.
4. Remove one facade group per PR only after downstream compatibility tests pass.

Do not delete facade groups only because the underlying module moved. Keep the
facade stable until the replacement path is visible and tested.

## Ready-To-Split Checklist

A candidate split is ready for code movement only when all items below are true:

- dependency graph shows no cycle with ECStore, storage-api, runtime sources, or
  owner-local compatibility modules;
- contract traits compile without importing ECStore implementation modules;
- old facade names have compatibility tests or explicit deprecation coverage;
- focused tests cover the changed owner path before any full gate is attempted;
- rollback preserves object IO, quorum, lifecycle/replication queues, scanner
  repair, notification/audit events, and metadata compatibility.
