# ECStore Layout Boundary

**Use this when:** you touch endpoint expansion, `FormatV3`, pool/set layout, or move files between ECStore's internal directories.
**Source of truth:** `crates/ecstore/src/layout/` (static layout), `crates/ecstore/src/core/sets.rs` (`Sets`) and `crates/ecstore/src/set_disk/mod.rs` (`SetDisks`) for runtime orchestration, `crates/ecstore/src/api/mod.rs` (`pub mod layout`) for the public surface.

## Directory Ownership

Ownership buckets under `crates/ecstore/src` (a subset; list the rest with `ls crates/ecstore/src`):

| Directory | Owns |
|---|---|
| `api` | Facade and compatibility re-exports |
| `core` | Store facade, pools, sets, and object/bucket/list/multipart/heal orchestration |
| `layout` | Static endpoint, disk, pool, and set layout (`disks_layout`, `endpoint`, `endpoints`, `format`, `pool_space`, `set_heal`, `set_layout`) |
| `disk` | Local disk, format compatibility, health, disk errors |
| `erasure` | Erasure coding and bitrot |
| `metadata` | Bucket metadata, config object store, data usage |
| `cluster` | Remote disk, peer, lock, membership, health control plane |
| `services` | Lifecycle, replication, tier, notification, rebalance, metrics services |
| `set_disk`, `store`, `data_movement`, `data_usage`, `object_api`, `runtime` | Set-level operations, store init, pool data movement, usage accounting, object API helpers, runtime state owners |

## Static Set Layout

Static layout is derived from persisted `FormatV3` data (`crates/ecstore/src/layout/format.rs`) and endpoint expansion (`crates/ecstore/src/layout/disks_layout.rs`). It may describe the deployment id, set count and drives per set, disk UUID positions inside `format.erasure.sets`, the distribution algorithm, and endpoint grouping produced before runtime disk initialization. It must not own disk handles, lock clients, reconnect loops, repair state, or shutdown signaling.

## Visibility

`layout::*` modules are `pub(crate)`; public access goes through `rustfs_ecstore::api::layout` (`DisksLayout`, `EndpointServerPools`, `Endpoints`, `PoolEndpoints`, `SetupType`). `disk::format` re-exports `layout::format` for crate-internal callers. Outer crates must not reach the root `endpoints` or `disks_layout` modules.

## Runtime Set Orchestration

`Sets` and `SetDisks` own the flat disk index to `(set_index, disk_index)` mapping, per-set local disk replacement after distributed setup detection, per-set lock-client host deduplication, endpoint reconnect monitoring and runtime shutdown signaling, and read/write/heal/list orchestration over initialized disks.

## Preservation Rules

- Object-to-set hashing and distribution algorithm selection must not change.
- Format `sets` ordering and disk UUID position lookup must not change.
- Local disk replacement and lock-client mapping stay runtime-only.
- File moves keep old public paths or add explicit compatibility coverage before deleting them.
