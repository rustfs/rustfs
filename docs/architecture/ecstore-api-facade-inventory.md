# ECStore API Facade Inventory

**Use this when:** you need something from `rustfs_ecstore` in another crate, you are narrowing a `rustfs_ecstore::api` facade group, or the architecture guard reports a facade bypass.
**Source of truth:** `crates/ecstore/src/api/mod.rs` (facade groups), the boundary files listed below, and the facade rules in `scripts/check_architecture_migration_rules.sh`.

The broad `rustfs_ecstore::api` facade is a compatibility boundary, not an architecture target. It shrinks monotonically and only through guarded changes; it is never approval to move lifecycle, replication, or `SetDisks` runtime behavior.

## Facade Group Inventory

| Facade group | Role | Shrink posture |
|---|---|---|
| `storage`, `layout`, `error`, `runtime`, `cluster`, `rpc` | Compatibility spine for storage, topology, runtime handles, cluster control, and internode calls. | Keep until replacement contracts compile in downstream boundary files. |
| `bucket` | Domain facade consumed through owner-local `storage_api` boundaries; explicit submodules and symbol lists, never whole bucket owner modules. | Keep lists aligned with boundary consumers; never restore whole-module passthroughs. |
| `config`, `disk`, `tier` | Compatibility paths with explicit nested submodules and symbol lists. | Same as `bucket`. |
| `data_usage`, `capacity`, `notification`, `metrics`, `rebalance` | Domain and service facades consumed through owner-local boundaries. | Narrow one group at a time after explicit aliases or wrappers exist. |
| `set_disk`, `object`, `object_api_utils`, `rio`, `bitrot`, `erasure`, `compression`, `cache`, `store_list` | Low-level object IO, reader, erasure, cache, and migration helper compatibility. | Keep stable while `SetDisks` remains the shared state carrier. |
| `admin`, `event`, `global` | Admin, event hook, and bootstrap-global compatibility. | `global` is limited to bootstrap writes and lifecycle controls; read-only runtime access goes through `runtime`. |

The S3 client is no longer a facade group: it lives in `crates/s3-client` (`rustfs_s3_client`). Regenerate the group list with:

```bash
rg -n '^pub mod ' crates/ecstore/src/api/mod.rs
```

## External Consumer Boundaries

External `rustfs_ecstore::api` imports stay in these local boundary files:

| Boundary file | Facade families consumed |
|---|---|
| `rustfs/src/storage/storage_api.rs` | Broad storage-owner bridge: admin, bucket submodules, capacity, compression, cluster, config, data usage, disk, error, event, global bootstrap controls, runtime getters, layout, metrics, notification, rebalance, rio, rpc, set disk, storage, tier. Replication pool/stat handles are projected into RustFS-local wrapper types here. |
| `rustfs/src/storage_api.rs`, `rustfs/src/admin/storage_api.rs`, `rustfs/src/app/storage_api.rs` | Root, admin, and app owner boundaries: explicit aliases only, no `metadata`, `metadata_sys`, `quota`, `com`, or bare `init` module passthroughs; object and error aliases anchor on storage-api associated types and a local `StorageError`. |
| `crates/scanner/src/storage_api.rs` | Bucket lifecycle, replication, metadata, capacity, config, data usage, disk, error, runtime, set disk, storage, tier. Replication queue config, admission, and heal object DTOs are projected into scanner-local types. |
| `crates/obs/src/metrics/storage_api.rs` | Bucket bandwidth, lifecycle, replication, quota, capacity, data usage, error, runtime, storage; data usage is consumed as a local DTO projection. |
| `crates/iam/src/storage_api.rs` | Config, error, notification, runtime, storage. |
| `crates/heal/src/heal/storage_api.rs` | Data usage, disk, error, runtime, storage. |
| `crates/notify/src/storage_api.rs` | Config, runtime, storage; no broad `config` or `global` module imports. |
| `crates/protocols/src/swift/storage_api.rs` | Bucket metadata, bucket metadata system, error, runtime, storage. |
| `crates/s3select-api/src/storage_api.rs` | Error, runtime, set disk, storage. |
| `crates/e2e_test/src/storage_api.rs` | E2E harness bridge for bucket targets, disk walking, and RPC helpers; no grouped RPC passthroughs. |
| `crates/ecstore/tests/storage_api.rs`, `crates/heal/tests/storage_api.rs`, `crates/scanner/tests/storage_api/mod.rs`, `fuzz/fuzz_targets/*_storage_api.rs` | Test and fuzz bridges: direct aliases or local wrappers; fuzz harnesses wrap bucket utility entrypoints instead of grouped passthroughs. |
| `crates/test-utils/src/ecstore_test_compat.rs`, `crates/iam/tests/ecstore_test_compat/mod.rs`, `crates/protocols/tests/ecstore_test_compat/mod.rs` | Test-only compatibility harnesses that import the facade directly for fixture setup. |

`crates/replication/src/storage_api.rs` shares the file name but is not an ECStore boundary: it owns the delete work DTOs of `rustfs-replication`, which imports neither `rustfs_ecstore` nor `rustfs-storage-api`.

Regenerate the boundary list with:

```bash
rg -l 'rustfs_ecstore::api' crates rustfs/src fuzz -g '*.rs' -g '!crates/ecstore/src/**'
```

New production imports outside these files are migration drift. Do not add direct `rustfs_ecstore::api` imports outside the boundary files; add a local boundary or a storage-api contract first, then route consumers through it.

## Split Dependency Inventory

Lifecycle, replication, and `SetDisks` split blockers, extracted contracts, and guard rule names are tracked in [ecstore-module-split-plan.md](ecstore-module-split-plan.md) and the module inventories `crates/ecstore/src/bucket/lifecycle/README.md` and `crates/ecstore/src/bucket/replication/README.md`. `crates/ecstore/tests/ecstore_contract_compat_test.rs` keeps compile-time coverage for `ECStore` and `SetDisks` storage-api trait compatibility before any facade shrink or operation-family movement.

## Shrink Rules

1. Do not remove a facade item until its downstream boundary has compile-time coverage or a documented replacement.
2. Do not add direct `rustfs_ecstore::api` imports outside the boundary files listed above.
3. Do not split lifecycle or replication into crates while they depend on ECStore runtime state, queues, notification, audit, scanner, or `SetDisks` internals.
4. Do not replace `SetDisks` with multiple runtime structs in one change; move one operation family only after contracts and focused tests exist.
5. Remove or narrow one facade group per change so rollback preserves object IO, quorum, lifecycle/replication queues, scanner repair, notification/audit events, and metadata compatibility.
6. Keep `api::bucket`, `api::config`, `api::disk`, and `api::tier` on explicit submodules and symbol lists; do not restore `pub use crate::<owner>::{...}` whole-module passthroughs for those groups.
