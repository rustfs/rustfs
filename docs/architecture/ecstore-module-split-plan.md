# ECStore Module Split Plan

**Use this when:** you add lifecycle or replication logic and need to know which crate it belongs in, you plan to move an operation family out of `SetDisks`, or the guard fails on one of the split rules named below.
**Source of truth:** `scripts/check_architecture_migration_rules.sh` (the rules), `crates/ecstore/src/bucket/lifecycle/README.md` and `crates/ecstore/src/bucket/replication/README.md` (module-level contract inventories, completion criteria, milestones), and [ecstore-api-facade-inventory.md](ecstore-api-facade-inventory.md) (facade groups and boundary files).

## Current Shape

| Area | Owner | Split status |
|---|---|---|
| Bucket lifecycle | `crates/lifecycle/` (`rustfs-lifecycle`, pure contracts) + `crates/ecstore/src/bucket/lifecycle/` (runtime) | Core contracts extracted; runtime stays in ECStore |
| Bucket replication | `crates/replication/` (`rustfs-replication`, contracts and wire formats) + `crates/ecstore/src/bucket/replication/` (worker runtime) | Contracts extracted; runtime move pending |
| Set disks | `crates/ecstore/src/set_disk/` | Shared state carrier plus operation modules; stays in ECStore |
| Public facade | `crates/ecstore/src/api/mod.rs` | Shrinks only through guarded changes |
| S3 client | `crates/s3-client/` (`rustfs-s3-client`) | Extracted |

Measure size instead of trusting numbers in a document:

```bash
find crates/ecstore/src -name '*.rs' | xargs wc -l | sort -rn | head
find crates/ecstore/src/bucket/replication -name '*.rs' | xargs wc -l | tail -1
```

Rule for new code: in a domain that already has a contract crate, new logic that does not need ECStore runtime state lands in that crate (`rustfs-lifecycle`, `rustfs-replication`), not under `crates/ecstore/src/bucket/`.

The S3 client extraction is complete: the former `client/` directory moved to `crates/s3-client`, its two server-side modules moved to `crates/ecstore/src/object_api/object_api_utils.rs` and `crates/ecstore/src/bucket/lifecycle/object_handlers_common.rs`, and the remaining serving-side `s3s` references in ECStore are ratcheted shrink-only by `S3S_ECSTORE_FILES_BASELINE` in `scripts/check_s3s_footprint.sh`.

## Non-Negotiable Rules

- Do not split crates in the same change that moves runtime state or changes startup behavior.
- Do not change object placement, quorum, reader semantics, lifecycle queues, replication queues, notification dispatch, audit events, or scanner repair behavior during inventory and contract work.
- Do not expose new direct ECStore internals to outer crates; use storage-api and owner-local facade boundaries.
- Keep `rustfs_ecstore::api` compatibility visible until each consumer path has compile coverage and an explicit replacement.

## Guarded Split Rules

Each rule is enforced by `scripts/check_architecture_migration_rules.sh`; the name is the vocabulary used in reviews and guard failures.

| Rule | What the guard checks |
|---|---|
| `LifecycleCrateCoreIndependence` | `crates/lifecycle` (rule validation, filtering, event evaluation, transition/expiration options, tag decoding, object-lock metadata checks, expiry-time rounding) imports no ECStore internals, `rustfs-filemeta`, or `rustfs-utils`; ECStore owns the `ObjectInfo` adapter in `crates/ecstore/src/bucket/lifecycle/core.rs`. |
| `ReplicationCrateFileMetaIndependence` | Replication status, decision, MRF, resync, and target-reset wire contracts live in `crates/replication/src/filemeta.rs`; `rustfs-replication` neither imports nor depends on `rustfs-filemeta`. |
| `ReplicationCrateStorageApiIndependence` | Delete work DTOs live in `crates/replication/src/storage_api.rs`; ECStore converts storage-api delete DTOs at its replication storage boundary; `rustfs-replication` does not depend on `rustfs-storage-api`. |
| `ReplicationCrateUtilsIndependence` | HTTP metadata keys, S3 header labels, ETag trimming, and prefix matching used by replication wire contracts live in `crates/replication/src/http.rs`; `rustfs-replication` does not depend on `rustfs-utils`. |
| `EcstoreReplicationBoundaryImports` | ECStore-side `rustfs_replication` imports are confined to the `*_boundary.rs` modules under `crates/ecstore/src/bucket/replication/`; grouped queue, stats, resync, and object-decision symbols each have one owning boundary file. |
| `RuntimeReplicationFacadeConsumers` | Scanner, admin, storage-owner, and app code consume replication status/DTO/helper contracts through the `rustfs_ecstore` facade; the `rustfs` and `rustfs-scanner` crates do not depend on `rustfs-replication` directly. |
| `StorageApiReplicationContracts` | Owner-facing storage-api delete DTO replication state/status helpers stay in `crates/storage-api/src/replication.rs`; replication worker DTOs stay in `rustfs-replication`. |

## Lifecycle

`rustfs-lifecycle` owns the pure rule, event, evaluator, tag-filter, object-lock metadata check, and expiry-time contracts. ECStore keeps the object-store runtime, queues, tiering, audit/notification, metadata access (`crates/ecstore/src/bucket/lifecycle/metadata_boundary.rs`), and replication-delete scheduling adapters.

Coupling that still blocks a runtime move: lifecycle workers read ECStore runtime sources (object store, expiry and transition state, tier config, deployment id, local node name); stale multipart cleanup depends on `SetDisks` internals and bucket metadata; expiry schedules replication deletes through the replication lifecycle bridge; the lifecycle runtime coordinates scanner metrics and notification/audit side effects. The contract list and the next step live in `crates/ecstore/src/bucket/lifecycle/README.md`.

## Replication

`rustfs-replication` owns resync status contracts, the persisted resync status wire format, filemeta-derived wire contracts, delete work DTOs, and HTTP helper contracts. ECStore keeps the worker runtime, error mapping, MRF persistence, and global pool/stat initialization.

Boundary layout inside `crates/ecstore/src/bucket/replication/`: `*_boundary.rs` modules concentrate imports from `rustfs-replication`, storage-api, filemeta, config, target, error, lock, msgp, versioning, tagging, bandwidth, queue, stats, resync, and object-decision surfaces; `replication_*_bridge.rs` modules (lifecycle, scanner, object, migration, target-config) expose replication scheduling to other owners without leaking DTO construction; `replication_config_store.rs` exposes config persistence and storage-class labels. Modules inside the directory use relative self-imports, and the facade in `mod.rs` uses explicit symbol lists, never wildcard re-exports.

Consumers outside ECStore: RustFS runtime code receives pool/stat handles through storage-owner wrapper types in `rustfs/src/storage/storage_api.rs`; scanner code receives scanner-local config/admission/heal DTOs from `crates/scanner/src/storage_api.rs`; observability reads replication metrics through obs-local snapshot DTOs in `crates/obs/src/metrics/storage_api.rs`; app object and multipart writes call object-replication bridge helpers instead of constructing replication work DTOs.

Completion criteria, the milestone order, and the per-dependency contract inventory live in `crates/ecstore/src/bucket/replication/README.md` (sections "Completion Criteria" and "Milestones"). Remaining work starts from moving resyncer pure decision logic.

## SetDisks

Do not replace `SetDisks` with several runtime structs in one change:

1. Keep `SetDisks` as the shared state carrier while operation modules own read/write/list/multipart/lock/heal/replication behavior.
2. Extract pure contracts first: shard source, disk error, bitrot IO, namespace lock, metrics labels, and file metadata access.
3. Move one operation family only after its contracts are covered by focused tests and the facade compatibility path is explicit.
4. Preserve the `rustfs_ecstore::api::set_disk` surface until downstream compatibility tests prove no caller depends on removed names.

## Facade Shrink

Facade groups, boundary files, and shrink rules are in [ecstore-api-facade-inventory.md](ecstore-api-facade-inventory.md). Shrinking is monotonic: inventory, add compile-time coverage, move consumers to storage-api or owner-local boundaries, then remove one group per change. Do not delete facade groups only because the underlying module moved.

## Ready-To-Split Checklist

A candidate is ready for code movement only when all of these hold:

- the dependency graph shows no cycle with ECStore, storage-api, runtime sources, or owner-local compatibility modules;
- contract traits compile without importing ECStore implementation modules;
- old facade names have compatibility tests or explicit deprecation coverage;
- focused tests cover the changed owner path before any full gate is attempted;
- rollback preserves object IO, quorum, lifecycle/replication queues, scanner repair, notification/audit events, and metadata compatibility.
