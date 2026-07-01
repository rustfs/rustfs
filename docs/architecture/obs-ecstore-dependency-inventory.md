# Observability ECStore Dependency Inventory

This inventory closes the first `rustfs/backlog#735` step: make every
observability dependency on ECStore visible before introducing traits or moving
dependency direction.

No behavior or crate movement is planned in inventory PRs. The current boundary
is `crates/obs/src/metrics/storage_api.rs`; all direct `rustfs_ecstore` and
`rustfs_storage_api` source references in `rustfs-obs` must stay in that file
until the contracts below are extracted.

## Dependency Inventory

| Current symbol in `crates/obs/src/metrics/storage_api.rs` | Consumed by | Classification | Purpose |
|---|---|---|---|
| `rustfs_ecstore::api::storage::ECStore` as `ObsStore` | `stats_collector.rs` | Type dependency | Concrete object-store handle used to call storage admin methods and data-usage loaders. |
| `rustfs_storage_api::{BucketOperations, BucketOptions, StorageAdminApi}` | `stats_collector.rs` | Type and trait dependency | Method-resolution and associated type contracts for bucket listing, backend info, and storage info. |
| `rustfs_ecstore::api::runtime::object_store_handle` | `stats_collector.rs` | Runtime dependency | Resolves the currently published object-store handle for metric collection. |
| `rustfs_ecstore::api::data_usage::load_data_usage_from_backend` | `stats_collector.rs` | Behavior dependency | Loads bucket/object usage and is projected into obs-local DTOs before collectors consume it. |
| `rustfs_ecstore::api::capacity::{get_total_usable_capacity, get_total_usable_capacity_free}` | `stats_collector.rs` | Behavior dependency | Computes usable and free capacity from ECStore storage info. |
| `rustfs_ecstore::api::bucket::metadata_sys::get_quota_config` | `stats_collector.rs` | Behavior dependency | Reads per-bucket quota limits used in bucket usage metrics. |
| `rustfs_ecstore::api::bucket::bandwidth::monitor::Monitor` | `runtime_sources.rs`, `stats_collector.rs` | Type and runtime dependency | Reads replication bandwidth reports from the global bucket monitor handle. |
| `rustfs_ecstore::api::runtime::bucket_monitor` | `runtime_sources.rs` | Runtime dependency | Resolves the global bucket bandwidth monitor for metric collection. |
| `rustfs_ecstore::api::bucket::replication::get_global_replication_stats` | `storage_api.rs` snapshot helpers | Runtime dependency | Reads replication status, transfer, failure, and site-replication stats, then projects them into obs-local snapshot DTOs. |
| `rustfs_ecstore::api::bucket::lifecycle::bucket_lifecycle_ops::{GLOBAL_ExpiryState, GLOBAL_TransitionState}` | `runtime_sources.rs`, `stats_collector.rs` | Runtime dependency | Reads lifecycle expiry and transition queue counters. |
| `rustfs_ecstore::api::error::Result` as `ObsEcstoreResult` | `stats_collector.rs` | Type dependency | Preserves ECStore error propagation while data-usage behavior remains ECStore-owned. |

## Classification

The remaining coupling is not just a dependency declaration problem:

- type coupling: `ObsStore`, `ObsEcstoreResult`, `ObsBucketBandwidthMonitor`,
  `StorageAdminApi`, `BucketOperations`, and `BucketOptions`;
- runtime handle coupling: object-store handle, bucket monitor, replication
  stats inside snapshot helpers, expiry state, and transition state;
- behavior coupling: data-usage loading, quota lookup, and capacity math.

Removing `rustfs-ecstore` from `crates/obs/Cargo.toml` is unsafe until those
three categories have replacement contracts and compile coverage.

## Extraction Plan

1. Keep all direct ECStore and storage-api imports centralized in
   `crates/obs/src/metrics/storage_api.rs`.
2. Keep projecting ECStore data-usage and replication stats output into
   obs-local DTOs before collectors consume it.
3. Introduce obs-owned provider traits for storage info, bucket info, quota,
   data usage, replication, bandwidth, and lifecycle queue snapshots.
4. Implement those traits in ECStore or an ECStore-owned adapter crate after the
   trait shapes are covered by focused tests.
5. Remove the `rustfs-ecstore` dependency from `rustfs-obs` only after metrics
   behavior is unchanged through the provider traits.

## Guardrails

The architecture guard enforces this inventory boundary:

- `crates/obs/src/metrics/storage_api.rs` is the only `rustfs-obs` source file
  allowed to reference `rustfs_ecstore` or `rustfs_storage_api`;
- raw replication stats handles and ECStore replication stat methods must stay
  behind the snapshot helpers in `crates/obs/src/metrics/storage_api.rs`;
- `rustfs-obs` must not add `storage_compat.rs` or `ecstore_compat.rs`
  passthrough bridges;
- future extraction PRs must update this inventory and the guard in the same
  reviewed change when a dependency category is removed.
