# Global State And Crate Split Plan

**Use this when:** business logic needs runtime state (object store, endpoints, lock clients, lifecycle state, config) and you must pick the right boundary, or you are evaluating a new crate split out of ECStore.
**Source of truth:** `crates/ecstore/src/runtime/global.rs` and `crates/ecstore/src/runtime/sources.rs` (ECStore-owned state and its adapter), `rustfs/src/app/context.rs` and the `runtime_sources.rs` owner modules under `rustfs/src` (RustFS resolvers), and the `rustfs_ecstore::api::global` boundary list in `scripts/check_architecture_migration_rules.sh`. The static inventory is [global-state-inventory.md](global-state-inventory.md).

Broad resolver-fallback removal is complete: runtime resolver fallbacks live in explicit owner-local boundaries, not in the root facade. What remains is ECStore-owned bootstrap state and crate-split decisions.

## Remaining Global Owners

| Owner | Role | Stance |
|---|---|---|
| `rustfs/src/app/context.rs` | AppContext-first resolver facade. | Resolver helpers stay context-first and do not construct concrete no-AppContext defaults. |
| `rustfs/src/app/context/runtime_sources.rs` | Default adapters for KMS, IAM, object store, endpoints, config, metrics, and notification state used by AppContext construction. | Allowed adapter boundary, not a business-logic owner. |
| `rustfs/src/runtime_sources.rs`, `rustfs/src/admin/runtime_sources.rs`, `rustfs/src/app/runtime_sources.rs`, `rustfs/src/server/runtime_sources.rs`, `rustfs/src/storage/runtime_sources.rs` | Owner-local runtime-source boundaries. | Business modules use these instead of global state; owner facades decide when to apply no-AppContext compatibility defaults. |
| `rustfs/src/storage_api.rs`, `rustfs/src/admin/storage_api.rs`, `rustfs/src/app/storage_api.rs`, `rustfs/src/storage/storage_api.rs` | Owner-local storage contract/facade boundaries. | Storage helper and ECStore facade access stays visible at local owner boundaries. |
| `crates/*/storage_api.rs` | External crate-local storage facade boundaries (IAM, scanner, heal, notify, observability, Swift, S3 Select). | External runtime crates read ECStore runtime state through `rustfs_ecstore::api::runtime`, never the global facade. |
| `crates/ecstore/src/runtime/global.rs` | ECStore bootstrap/runtime state owner. | Internal until ECStore has explicit owner handles for all remaining bootstrap state. |
| `crates/ecstore/src/runtime/sources.rs` | ECStore runtime-source adapter over global state. | Preferred ECStore-internal access path while direct `runtime::global` reads shrink. |

## Runtime Source Boundaries

Runtime-source modules are the allowed compatibility layer between migrated consumers and process-global state. They keep these properties:

- context-first lookup when an `AppContext` handle exists;
- explicit fallback to the existing global only where compatibility still requires it, decided by the owner facade;
- no hidden service construction in business logic;
- the root `rustfs/src/runtime_sources.rs` is an entrypoint only: it composes no concrete fallback defaults (`unwrap_or`, `unwrap_or_else`, direct `init_global` or `new_global` calls);
- production callers outside runtime-source and `storage_api.rs` boundary modules do not import ECStore global state directly.

### Guarded Boundary List

The guard pins the production files allowed to reference `rustfs_ecstore::api::global` directly:

- `rustfs/src/storage/storage_api.rs`

That boundary keeps only bootstrap writes and lifecycle controls (`set_global_endpoints`, `set_global_region`, `set_global_rustfs_port`, `set_object_store_resolver`, `shutdown_background_services`, `update_erasure_type`). Read-only runtime getters are exported through `rustfs_ecstore::api::runtime` and consumed through the local storage facade. A new direct use either moves behind an existing owner-local boundary or updates this plan and the guard in the same reviewed change.

## Fallback Removal Plan

1. AppContext-first lookup is the stable resolver contract.
2. Concrete no-AppContext compatibility defaults exist only at the owner-local runtime-source facades that consume them.
3. Business logic does not call `AppContext` or ECStore globals directly when an owner-local runtime-source boundary exists.
4. Embedded startup and tests keep working before any remaining owner fallback is deleted.
5. ECStore bootstrap globals stay until ownership handles exist for local disks, endpoint pools, lock clients, notification state, tier config, lifecycle state, and object-store publication.

## Crate Split Evaluation

`ecstore-erasure` and `storage-cluster` are proposal-only; neither is ready for code movement. Lifecycle and replication split status is tracked in [ecstore-module-split-plan.md](ecstore-module-split-plan.md).

### `ecstore-erasure`

Coupling: erasure decoding depends on disk errors, disk read timeouts, and set-disk shard sources; set-disk read/write/heal paths construct codecs in hot object I/O paths; bitrot readers/writers live in ECStore IO support and serve both erasure and set-disk code; `rustfs_ecstore::api::erasure` is still a public compatibility surface.

Decision: do not split. The boundary becomes a candidate only after shard-source, disk-error, bitrot, and metrics contracts are explicit enough to avoid a dependency cycle back into ECStore, backed by encode/decode/reconstruction benchmarks and a rollback plan that keeps read/write quorum and old-version decode unchanged.

### `storage-cluster`

Coupling: cluster RPC remote-disk code depends on disk stores, disk health tracking, set-disk buffer sizing, local disk scan guards, internode metrics, and runtime credential/signature sources; peer S3 and peer REST clients share bucket metadata, disk quorum reduction, endpoint layout, local disk initialization, and store helpers; control-plane snapshots are separate from data-plane RPC, but remote disk and peer clients still own data-movement side effects inside ECStore.

Decision: do not split. The boundary becomes a candidate only after remote disk, peer health, lock/quorum, runtime metrics, and endpoint layout contracts can stand below ECStore without cycles, with compatibility plans for `rustfs_ecstore::api::cluster` and `api::rpc` and focused tests for remote disk error classification, peer health recovery, per-pool quorum reduction, lock behavior, and data-stream request paths.

## Preservation Rules

- Do not reintroduce AppContext resolver fallback families in broad cleanups.
- Do not introduce direct global reads in admin, app, server, storage, scanner, heal, IAM, notify, observability, Swift, or S3 Select business logic.
- Do not split crates in the same change that moves runtime state.
- Do not change startup order, readiness, KMS fatal boundaries, IAM recovery, lock quorum, object placement, reader behavior, or notification/audit lifecycle while shrinking global state.
