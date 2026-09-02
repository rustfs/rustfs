# Observability ECStore Dependency Inventory

**Use this when:** adding, removing, or moving any `rustfs_ecstore` or `rustfs_storage_api` reference inside `crates/obs`.
**Source of truth:** the `use` block at the top of `crates/obs/src/metrics/storage_api.rs`; the guard in `scripts/check_architecture_migration_rules.sh`.

`rustfs-obs` still depends on `rustfs-ecstore` (`crates/obs/Cargo.toml`). Every direct reference is confined to one boundary file so the dependency can later be replaced by provider traits without touching collectors.

## Dependency Inventory

The authoritative list is the `pub(crate) use rustfs_ecstore::api::...` block in `crates/obs/src/metrics/storage_api.rs`; it is not copied here. Each import belongs to one of three coupling categories:

| Category | Covers | Examples (aliases defined in the boundary file) |
|---|---|---|
| Type coupling | Concrete ECStore types and storage-api traits used for method resolution | `ObsStore`, `ObsEcstoreResult`, `ObsBucketBandwidthMonitor`, the `rustfs_storage_api` trait imports |
| Runtime handle coupling | Resolving process-wide handles for metric collection | object-store handle, bucket monitor, expiry and transition state handles (`rustfs_ecstore::api::runtime::*`), replication stats read inside the snapshot helpers |
| Behavior coupling | ECStore-owned computations whose output is projected into obs-local DTOs | data-usage loading, compression totals, quota lookup, usable-capacity math |

Collectors consume only the aliases and the obs-local DTOs. Removing `rustfs-ecstore` from `crates/obs/Cargo.toml` is unsafe until all three categories have replacement contracts and compile coverage.

## Extraction Plan

1. Keep all direct ECStore and storage-api imports centralized in `crates/obs/src/metrics/storage_api.rs`.
2. Keep projecting ECStore data-usage and replication stats into obs-local DTOs before collectors consume them.
3. Introduce obs-owned provider traits for storage info, bucket info, quota, data usage, replication, bandwidth, and lifecycle queue snapshots.
4. Implement those traits in ECStore or an ECStore-owned adapter crate once the trait shapes are covered by focused tests.
5. Remove the `rustfs-ecstore` dependency from `rustfs-obs` only after metrics behavior is unchanged through the provider traits.

## Guardrails

Enforced by `scripts/check_architecture_migration_rules.sh`:

- `crates/obs/src/metrics/storage_api.rs` is the only `rustfs-obs` source file allowed to reference `rustfs_ecstore` or `rustfs_storage_api`.
- Raw replication stats handles and ECStore replication stat methods stay behind the snapshot helpers in that file.
- `rustfs-obs` must not add passthrough bridge modules (a second `storage_api.rs`, an `ecstore_compat.rs`, or similar) that re-export ECStore items to other crates.
- An extraction PR that removes a dependency category updates this inventory and the guard in the same change.
