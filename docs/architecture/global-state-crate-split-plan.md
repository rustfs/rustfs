# Global State And Crate Split Plan

This document records the late global-state cleanup plan after the AppContext
foundation, storage API contracts, ECStore layout, runtime lifecycle, and cluster
control-plane boundaries are stable.

## Remaining Global Owners

| Owner | Current role | Migration stance |
|---|---|---|
| `rustfs/src/app/context.rs` | AppContext-first resolver facade with legacy fallback adapters. | Keep resolver entry points stable until every caller has an explicit context or owner-local runtime source. |
| `rustfs/src/app/context/runtime_sources.rs` | Default AppContext fallback adapters for KMS, IAM, object store, endpoints, config, metrics, and notification state. | This is an allowed fallback boundary, not a business logic owner. |
| `rustfs/src/*/runtime_sources.rs` | Root, admin, app, server, startup, and storage owner-local runtime-source boundaries. | Business modules use these boundaries instead of calling global state directly. |
| `rustfs/src/*/storage_api.rs` | Root, admin, app, and storage owner-local storage contract/facade boundaries. | Storage helper and ECStore facade access remains visible at local owner boundaries. |
| `crates/*/storage_api.rs` | External crate-local storage facade boundaries for IAM, scanner, heal, notify, observability, Swift, and S3 Select. | External runtime crates may consume ECStore facade globals only through their local storage API boundary. |
| `crates/ecstore/src/runtime/global.rs` | ECStore bootstrap/runtime state owner. | Keep internal until ECStore has explicit owner handles for all remaining bootstrap state. |
| `crates/ecstore/src/runtime/sources.rs` | ECStore runtime-source adapter over global state. | Preferred ECStore-internal access path while shrinking direct `runtime::global` reads. |

## Runtime Source Boundaries

Runtime-source modules are the allowed compatibility layer between migrated
consumers and process-global state. They must keep these properties:

- context-first lookup when an `AppContext` handle exists;
- explicit fallback to the existing global only where compatibility still
  requires it;
- no hidden service construction in business logic;
- no startup, readiness, IAM, KMS, lock, notification, or storage behavior
  change in inventory or guardrail PRs.

## Guarded Boundary List

The architecture guard snapshots the files currently allowed to reference
`rustfs_ecstore::api::global` directly:

- `crates/heal/src/heal/storage_api.rs`
- `crates/iam/src/storage_api.rs`
- `crates/notify/src/storage_api.rs`
- `crates/obs/src/metrics/storage_api.rs`
- `crates/protocols/src/swift/storage_api.rs`
- `crates/s3select-api/src/storage_api.rs`
- `crates/scanner/src/storage_api.rs`
- `crates/scanner/tests/storage_api/mod.rs`
- `rustfs/src/storage/storage_api.rs`

New direct uses must either move behind an existing owner-local boundary or
update this plan and the guard in the same reviewed migration PR.

## Fallback Removal Plan

1. Keep AppContext-first plus global fallback while compatibility tests still
   cover both paths.
2. Move one remaining consumer group at a time to explicit context or
   owner-local runtime-source handles.
3. Remove one fallback family per PR only after scans prove no production caller
   depends on it.
4. Keep embedded startup and tests working before deleting any fallback.
5. Do not remove ECStore bootstrap globals until ownership handles exist for
   local disks, endpoint pools, lock clients, notification state, tier config,
   lifecycle state, and object-store publication.

## Crate Split Evaluation

`ecstore-erasure` and `storage-cluster` remain proposal-only until dependency
cycles and hot-path risks are proven safe.

Required evidence before a split:

- dependency graph showing no cycle with ECStore, storage API, runtime, or
  cluster control-plane owners;
- benchmark or profiling evidence for erasure and bitrot hot paths;
- compatibility plan for public ECStore facade paths and test harnesses;
- rollback plan that preserves quorum, remote disk, lock, and data movement
  behavior.

## Preservation Rules

- Do not remove AppContext fallback in a broad cleanup PR.
- Do not introduce direct global reads in admin, app, server, storage, scanner,
  heal, IAM, notify, observability, Swift, or S3 Select business logic.
- Do not split crates in the same PR that moves runtime state.
- Do not change startup order, readiness, KMS fatal boundaries, IAM recovery,
  lock quorum, object placement, reader behavior, or notification/audit
  lifecycle while shrinking global state.
