# Cluster and erasure-coding lifecycle operations

**Use this when:** planning, starting, expanding, rebalancing, decommissioning, healing, replacing drives in, or restarting an erasure-coded RustFS deployment, or deciding whether an `EC:<parity>` setting (including `EC:0`) is safe for a workload.
**Source of truth:** `crates/ecstore/src/layout/disks_layout.rs` (`DisksLayout::from_volumes`, `SET_SIZES`, `RUSTFS_ERASURE_SET_DRIVE_COUNT`), `crates/ecstore/src/config/storageclass.rs` (`default_parity_count`, `validate_parity_inner`, `Config::should_inline`, `Config::effective_inline_block`), `crates/ecstore/src/set_disk/mod.rs` (`resolve_write_layout`, `WriteLayout::from_parity`), `crates/ecstore/src/set_disk/metadata.rs` (`object_quorum_from_meta`, `common_parity`), `crates/ecstore/src/set_disk/ops/heal.rs` (no-parity `cannot_heal` classification), `crates/ecstore/src/store/init_format.rs` (`check_format_erasure_value_for_topology`), `rustfs/src/admin/handlers/pools.rs`, `rustfs/src/admin/handlers/rebalance.rs`, `rustfs/src/admin/handlers/heal.rs`, and `rustfs/src/admin/route_policy.rs` (admin surface).

This runbook is the operator entry point. It restates only what an operator needs to act and links the normative contracts for everything else: [erasure-coding.md](../architecture/erasure-coding.md) (algorithm, quorum, on-disk format), [ecstore-layout-boundary.md](../architecture/ecstore-layout-boundary.md) (immutable layout), [decommission-compatibility.md](../architecture/decommission-compatibility.md) (decommission and rebalance contract), [heal-concurrency-model.md](../architecture/heal-concurrency-model.md) (heal versus write locking), [rolling-restart.md](rolling-restart.md) (restarts), [replacement-generation-recovery.md](replacement-generation-recovery.md) (drive replacement recovery), and [scanner-runtime-controls.md](scanner-runtime-controls.md) (heal and scanner knobs). Admin routes below require an authenticated request holding the matching `AdminAction`; the route to action mapping is owned by `rustfs/src/admin/route_policy.rs` (see [admin-route-action-snapshot.md](../architecture/admin-route-action-snapshot.md)).

## 1. Immutable layout boundaries

A pool's endpoint count, set count, drives per set, position in the pool list, and the disk UUID positions in `FormatV3` are persisted layout. On startup `check_format_erasure_value_for_topology` (`crates/ecstore/src/store/init_format.rs`) compares the stored format with the configured drive count and set width and fails permanently with `PoolTopologyMismatch` (or `UnsupportedSnsdExpansion` for a single-drive deployment) instead of migrating. Restore the original endpoints and set width; do not delete `format.json` to get past the error. Details and the regression matrix are in [pool-layout-compatibility.md](../testing/pool-layout-compatibility.md).

Never do these:

- overwrite an existing pool with more or fewer endpoints, or change `RUSTFS_ERASURE_SET_DRIVE_COUNT` for an initialized pool;
- reorder volume arguments or move a drive directory to another slot;
- delete, copy, or rename object shards, `xl.meta`, or anything under `.rustfs.sys` (`RUSTFS_META_BUCKET` in `crates/ecstore/src/disk/mod.rs`) by hand;
- treat a manual file copy as data movement. Rebalance and decommission move objects under object locks with version re-checks and source cleanup; nothing else does.

Runtime roles:

- **Pool**: one endpoint list with persisted layout. Multi-drive sets contain 2 to 16 drives (`SET_SIZES`); a single local path is the only single-drive layout (`is_single_drive_layout`).
- **Pool leader**: the first endpoint of a pool. Decommission start, cancel, and clear may be sent to any node; when the target pool's first endpoint is remote the handler forwards over authenticated internode RPC (`decommission_peer_target` in `rustfs/src/admin/handlers/pools.rs`), and the leader still enforces `ensure_local_decommission_pool_leader` before mutating state.
- **Cluster-wide state**: rebalance and root heal propagate to peers and recover from persisted metadata on restart (§9).

## 2. Planning and first start

1. Plan endpoints by node, rack, power, and network failure domain. Topology admission is not a high-availability guarantee: a single-node pool loses every shard with its host, and at startup `log_storage_pool_layout` (`rustfs/src/startup_storage.rs`) warns `host_failure_data_unavailable` for every multi-drive pool.
2. Pick each pool's set width. Pools may differ in width, but each pool must split evenly into sets of one width from `SET_SIZES`; `RUSTFS_ERASURE_SET_DRIVE_COUNT` may pin a width that is already a symmetric divisor, nothing else.
3. Pick STANDARD and RRS parity per §4, then validate against the narrowest pool, because an explicit parity must fit every pool.
4. Record the full launch command, endpoint order, pool order, set width, and storage-class variables. They must be identical on every node and on every restart.
5. Give RustFS exclusive ownership of every endpoint path.

Volumes come from positional arguments or `RUSTFS_VOLUMES`; `rustfs` with no arguments behaves as `rustfs server` with volumes from the environment (`rustfs/src/config/cli.rs`). Ellipsis expansion follows MinIO:

```bash
# one pool, four nodes, four drives each: sixteen endpoints
rustfs server http://node{1...4}:9000/data{1...4}

# two pools; every argument must carry an ellipsis once there is more than one
rustfs server http://node{1...4}:9000/data{1...4} http://node{5...8}:9000/data{1...4}
```

Rules enforced by `DisksLayout::from_volumes`: with more than one argument every argument needs an ellipsis; each pool must expand to at least two distinct drive endpoints; a single-drive pool cannot join a multi-pool deployment; a singleton range such as `{3...3}` does not bypass the minimum.

Startup order (`init_startup_storage_foundation` in `rustfs/src/startup_storage.rs`): parse endpoints, validate storage classes against every pool (`ECStore::validate_startup_storage_class`), enforce the unsupported-filesystem policy, initialize local disks, initialize lock clients, then load or create the format quorum and build the store. A storage-class error stops startup before any disk is touched.

## 3. Startup configuration

| Variable | Effect | Boundary |
|---|---|---|
| `RUSTFS_ERASURE_SET_DRIVE_COUNT` | Pin the set width | Must be a valid symmetric divisor of the pool, at most `MAX_ERASURE_SET_DRIVE_COUNT` (16); cannot change an initialized pool |
| `RUSTFS_STORAGE_CLASS_STANDARD` | STANDARD parity as `EC:<n>` | Validated per pool; affects new writes only |
| `RUSTFS_STORAGE_CLASS_RRS` | RRS parity as `EC:<n>` | Validated per pool; an explicit value fails on a single drive, the persisted default `EC:1` resolves to `0` there |
| `RUSTFS_STORAGE_CLASS_OPTIMIZE` | Accepted for MinIO compatibility | Stored on `Config`, but `Config::capacity_optimized` has no caller: it changes nothing at runtime today |
| `RUSTFS_STORAGE_CLASS_INLINE_BLOCK` | Fixed per-shard inline limit (bytesize syntax such as `128KiB`) | Replaces the scaled default in §4.3; values above 128 KiB log `storage_class_inline_block_large` |

An empty `RUSTFS_STORAGE_CLASS_STANDARD` or `RUSTFS_STORAGE_CLASS_RRS` restores the automatic policy and overrides a persisted value (`standard_policy` / `rrs_policy`). `MINIO_`-prefixed spellings of these keys, of `ERASURE_SET_DRIVE_COUNT`, and of `VOLUMES` are mapped onto the `RUSTFS_` names when the `RUSTFS_` name is unset (`apply_external_env_compat` in `crates/utils/src/envs.rs`); when both are set the `RUSTFS_` value wins and the conflict is reported at startup.

Heal knobs (`RUSTFS_HEAL_*`) and their defaults are documented once, in [scanner-runtime-controls.md](scanner-runtime-controls.md#heal-runtime-controls); the deep bitrot cycle is the admin config key `heal.bitrot_cycle` (environment `RUSTFS_SCANNER_BITROT_CYCLE_SECS`), not a `RUSTFS_HEAL_*` variable. Data-movement knobs: `RUSTFS_REBALANCE_MAX_ATTEMPTS` (default `DEFAULT_REBALANCE_MAX_ATTEMPTS` = 3, `crates/ecstore/src/services/rebalance/mod.rs`), `RUSTFS_DECOMMISSION_BUCKET_CONCURRENCY` (default cap 4) and `RUSTFS_DECOMMISSION_ENTRY_CONCURRENCY` (default cap 8, hard cap 64) in `crates/ecstore/src/core/pools.rs`, and `RUSTFS_LIST_OBJECTS_QUORUM` (default `optimal`; `disk`, `reduced`, `optimal`, `auto`, anything else `strict`, per `normalize_list_quorum` in `crates/ecstore/src/store/list_objects.rs`). None of them relaxes object quorum, object locks, or version checks.

## 4. Storage classes, parity, capacity, quorum

### 4.1 Geometry

Each object is Reed-Solomon coded across the `N` drives of one set into `K` data and `M` parity shards, `N = K + M`; any `K` shards rebuild it. The geometry is stored per object in `xl.meta`, so changing the default later rewrites nothing. Objects hash to a set (`get_hashed_set_index`, [placement-repair-invariants.md](../architecture/placement-repair-invariants.md)) and shards rotate within it; sets never share a failure budget.

Default STANDARD parity (`default_parity_count`): 0 for one drive, 1 for 2 to 3, 2 for 4 to 5, 3 for 6 to 7, 4 for 8 to 16. Default RRS parity is 1 (0 on a single drive).

Validation (`validate_parity_inner`): each parity is at most `N/2` for every pool, and STANDARD parity is at least RRS parity **only when both are non-zero**. There is no minimum, so `EC:0` passes on any width (§5). Only `STANDARD` and `REDUCED_REDUNDANCY` are accepted on PUT, CopyObject, and CreateMultipartUpload; AWS labels return `InvalidStorageClass`.

### 4.2 Quorum and capacity

```text
usable capacity  ≈ raw × K / N
read quorum      = K            (metadata vote and shard decode)
write quorum     = K, or K + 1 when K == M
delete markers   = N/2 + 1      (majority, both write and vote)
```

Sixteen drives at `EC:4`: `K = 12`, about 75 percent usable, four failures tolerated, twelve valid shards needed to read. Two drives at `EC:1`: `K = M = 1`, so one drive down still reads but no longer writes. Internal metadata under `.rustfs.sys` is written with `max_parity`, meaning parity `N/2`, regardless of the configured class. Contract: [erasure-coding.md §2.2 and §10](../architecture/erasure-coding.md#22-parity-selection).

### 4.3 Inline objects

An object is stored inline in `xl.meta` when every shard is at most the per-shard budget (`Config::should_inline`). The default budget is `DEFAULT_INLINE_OBJECT_BUDGET / K` capped at `DEFAULT_INLINE_BLOCK` (256 KiB / K, at most 128 KiB per shard), so a wider set does not raise the maximum inline object size; on a versioned bucket the budget is divided by 8. `RUSTFS_STORAGE_CLASS_INLINE_BLOCK` replaces the scaled default with a fixed per-shard limit. Each drive keeps only its own shard inline, so inline data has the same loss semantics as external shards. Compressed or encrypted streams inline only when the stored size is known and within budget.

### 4.4 Write and read paths

Writes resolve `WriteLayout` per pool (`resolve_write_layout`), encode with `rs-vandermonde` in 1 MiB blocks (`BLOCK_SIZE_V2`), write bitrot-protected shards, and commit only after write quorum. A write below quorum is never reported as success; rollback is best effort and residue is reconciled by heal or the scanner ([erasure-coding.md §7](../architecture/erasure-coding.md#7-write-path-and-write-quorum)). Reads pick the authoritative metadata at read quorum, verify bitrot, decode from at least `K` shards, and fail closed below that; a read that succeeds with missing shards enqueues read-repair ([§8](../architecture/erasure-coding.md#8-read-path-and-read-quorum)).

Multipart parts use the object's geometry and write quorum; `CompleteMultipartUpload` re-checks metadata and commit quorum, so an uploaded part is not a committed object.

## 5. `EC:0`: zero-parity mode

`EC:0` is accepted for STANDARD, for RRS, or for both, on any set width. It gives raw capacity and no redundancy. Every statement here is what the code does, not a guideline.

**Configuration.**

- Explicit `EC:0` produces no startup warning. The `storage_class_zero_redundancy` warning (`publish_storage_class_config` in `crates/ecstore/src/config/mod.rs`) fires only for automatic zero parity, meaning a single-drive pool.
- `STANDARD=EC:2` with `RRS=EC:0` is valid because the ordering rule is skipped when either side is zero. Only objects written with `x-amz-storage-class: REDUCED_REDUNDANCY` then carry the semantics below; STANDARD objects keep parity 2.
- The set's default parity (`SetDisks.default_parity_count`) stays the topology default from `ec_drives_no_config`, not 0. Quorum arithmetic for a multi-drive set therefore takes the parity-present branch of `object_quorum_from_meta`; the `default_parity_count == 0` branch is the single-drive case.

**Writes.** `WriteLayout::from_parity(N, 0)` yields `K = N`, `M = 0`, write quorum `N`. If any drive of the set is unwritable, `put_object` returns `ErasureWriteQuorum` before or after encoding; nothing is committed. Because a zero-parity shard cannot be rebuilt later, the write path verifies every shard it just wrote against its bitrot hash before commit (`verify_written_bitrot_shards` in `put_object` and `put_object_part`, gated on `parity_blocks == 0`) and refuses to commit a shard that already fails.

**Reads.** `common_parity` classifies a parity value of 0 like a delete marker for vote selection (majority of replies), but the resulting read quorum is `data_blocks = N`. `find_file_info_in_quorum` (GET and HEAD) and `pick_valid_fileinfo` (listing cache) then need `N` matching metadata copies, and decode needs `N` shards. With any drive of the set offline, HEAD and GET of every zero-parity object in that set fail with `ErasureReadQuorum`. There is no degraded read.

**Deletes.** Delete markers and version deletes use majority quorum (`disks.len() / 2 + 1`), so a delete can succeed on a set whose reads and writes are failing. A successful delete is not evidence that the set is healthy.

**Heal.** `heal_object_with_explicit_version_regen` computes `cannot_heal` when the metadata copies to heal exceed `parity_blocks` (0) or any part has more missing or corrupt shards than `parity_blocks` (0). The result is `no-parity object is unrecoverable` with `FileCorrupt` for a bitrot failure and `ErasureReadQuorum` for a missing shard. Scanner and MRF heal requests repeat that outcome; they never repair. A replaced or wiped drive therefore loses every zero-parity object that had a shard on it, permanently, unless a copy exists outside the set. Recovery boundary and evidence handling: [no-parity-bitrot-recovery.md](no-parity-bitrot-recovery.md).

**What still works with one drive down.** Bucket operations and `.rustfs.sys` metadata (written at parity `N/2`), delete markers, and listings that satisfy the list quorum. Everything else in the set is unavailable, including rebalance and decommission of its objects, which must read all `N` shards.

**Operational consequences.**

- Any maintenance that takes a drive or its node offline is a read and write outage for ordinary objects in every set that spans it, for the whole window. Rolling restarts are not transparent.
- Drive replacement is data loss for that set, not a repair.
- Raising parity later rewrites nothing; re-upload or server-side copy each object after the change to re-protect it.
- Use `EC:0` only where an external layer (RAID, replication, backup, or a re-creatable source) owns durability and availability, and only with an explicit sign-off that one drive loss equals set loss. Production deployments should use `EC:1` or higher; `EC:2` or more per set is the usual floor once a set spans more than one node.

## 6. Post-start verification

Read-only checks:

```text
GET /rustfs/admin/v3/pools/list
GET /rustfs/admin/v3/pools/status?pool=<pool>[&by-id=true]
GET /rustfs/admin/v3/decommission/status[?pool=<pool>&by-id=true]
GET /rustfs/admin/v4/cluster/snapshot
GET /rustfs/admin/v4/runtime/capabilities
```

`pool` is a pool command line, or a zero-based index with `by-id=true`. Status queries ignore unknown parameters; mutation queries reject unknown or duplicate parameters and a `by-id` value other than `true` or `false` (`parse_pool_query`). Per-pool status fields, including `decommissionInfo`, are listed in [decommission-compatibility.md](../architecture/decommission-compatibility.md#status-response-shape); the `storage_classes` payload of runtime capabilities is owned by [runtime-capability-contracts.md](../architecture/runtime-capability-contracts.md); `pool_meta_write_gate` in the cluster snapshot is explained in [s3-write-failure-diagnostics.md](s3-write-failure-diagnostics.md).

Confirm: pool count, endpoints per pool, set count and width per pool, local versus remote endpoints, no legacy flag on a deployment that will ever decommission, peer readiness, and no topology mismatch.

Data-plane checks: write one STANDARD and one RRS object; GET, HEAD, range GET, CopyObject, and a multipart upload; take one drive or node offline within parity and confirm reads continue and writes behave per §4.2; bring it back and confirm the heal queue drains and bitrot errors stop; confirm every node reports the same pool metadata and cluster snapshot.

## 7. Expansion

An existing pool cannot grow in place. Add a pool:

1. Plan its nodes, drives, and failure domain; each new pool needs at least two drive endpoints and its own valid set layout.
2. Prepare empty, RustFS-exclusive paths on every node.
3. Append the pool to the launch arguments on **every** node, keeping existing pools, their order, endpoints, and set width unchanged. A single-node single-drive deployment cannot expand; migrate through S3 instead.
4. Restart per [rolling-restart.md](rolling-restart.md) and verify per §6.
5. New writes are placed across available pools; old objects stay where they are. To move them, start a rebalance (§8). A restart never rebalances.

Adding a pool does not add parity to existing objects and does not copy anything. Cross-pool movement is rebalance or decommission only.

## 8. Rebalance

Rebalance redistributes objects across pools by usage. Preconditions checked by `RebalanceStart` (`rustfs/src/admin/handlers/rebalance.rs`): more than one pool (a single pool returns `NotImplemented`), no decommission running or conflicting (`InvalidRequest`), and no query string on `start` or `stop` (`InvalidArgument`).

```text
POST /rustfs/admin/v3/rebalance/start     → {"id": "<uuid>"}
GET  /rustfs/admin/v3/rebalance/status
POST /rustfs/admin/v3/rebalance/stop
```

Status (`RebalanceAdminStatus`): `id`; `pools[]` with `id`, `status`, `stopping`, `used`, `lastError`, `cleanupWarnings`, and `progress` (`objects`, `versions`, `bytes`, `remainingBuckets`, `bucket`, `object`, `elapsed`, `eta`); `stoppedAt`; and `stopPropagation` (`lastAttemptAt`, `failedPeers`, `terminalReloadAttemptAt`, `terminalReloadFailedPeers`, `pendingTerminalReload`).

Start propagates an admission fence and worker state to peers; a propagation failure rolls back to a terminal state and reports any peer that did not roll back. Stop closes admission, stops local and remote workers, and persists the stopped state. Treat a rebalance as finished only when no pool is active or stopping, `lastError` is empty or explained, `cleanupWarnings` are understood, `pendingTerminalReload` is false, and migrated objects read from their new pool. Per-pool state lives in `rebalance.bin` (`REBAL_META_NAME`); never delete it to unblock a run. If the deployment ran rebalance on a release listed in [rebalance-stored-representation-impact.md](rebalance-stored-representation-impact.md), run that assessment first.

## 9. Decommission

Contract: [decommission-compatibility.md](../architecture/decommission-compatibility.md). Operator summary:

- Supported only on multi-pool, non-legacy (ellipsis) deployments; anything else returns `NotImplemented`. A rebalance in progress blocks start.
- Targets are validated as a batch before anything is persisted: unknown, duplicate, active, queued, or completed targets are rejected; failed or canceled targets must be cleared first unless they hold unresolved listing entries, in which case a new start is the retry path (`DecommissionStartPoolState::Retryable` versus `Blocked` in `crates/ecstore/src/core/pools.rs`).
- Requests go to any node and are forwarded to the pool leader (§1).

```text
POST /rustfs/admin/v3/pools/decommission?pool=<a>[,<b>]      # by command line
POST /rustfs/admin/v3/pools/decommission?pool=1&by-id=true   # by index
GET  /rustfs/admin/v3/decommission/status[?pool=1&by-id=true]
POST /rustfs/admin/v3/pools/cancel?pool=1&by-id=true
POST /rustfs/admin/v3/pools/clear?pool=1&by-id=true
```

Entry states persisted in `pool.bin`: `queued` → `active` → `completed`, `failed`, or `canceled`. One entry owns a worker at a time; startup resumes non-terminal entries and skips terminal predecessors. A source pool in any decommission state, terminal or not, rejects new ordinary PUTs and new multipart uploads (a staged PUT gets `SlowDown`); existing multipart uploads may drain while the source is non-terminal. `clear` removes failed or canceled metadata only and never moves data back. `queuedBuckets` is an inventory, not proof of a live worker.

Remove a pool from the launch arguments only after its entry is `completed`, free versions and tier ownership have converged, and every node shows the same status. Never detach source drives before that, and never substitute a directory delete for the configuration change.

## 10. Heal and drive replacement

### 10.1 Admin heal

```text
POST /rustfs/admin/v3/heal/                    # root: cluster, or one erasure set with pool+set
POST /rustfs/admin/v3/heal/<bucket>
POST /rustfs/admin/v3/heal/<bucket>/<prefix>
POST /rustfs/admin/v3/background-heal/status
GET  /rustfs/admin/v4/heal/replacement-recovery
```

The JSON body carries `recursive`, `dryRun`, `remove`, `recreate`, `scanMode`, `updateParity`, `nolock`, `readRepair`, `pool`, and `set`. Handler rules (`rustfs/src/admin/handlers/heal.rs`):

- a root heal must set `recursive=true` or name both `pool` and `set`; a bucket heal without a prefix is always recursive, and a prefix heal honors `recursive`;
- `readRepair=true` is rejected with `InvalidArgument`;
- `nolock` is accepted on the wire but forced to `false` (`build_heal_channel_request`); admin heals always take the object namespace lock, so there is no lock bypass to misuse;
- `clientToken` correlates start, status, and stop; `forceStart` and `forceStop` are validated against it and against each other, and unknown or duplicate query keys are rejected;
- overlapping starts follow `RUSTFS_HEAL_OVERLAP_POLICY`.

Heal selects the authoritative version from quorum metadata, refuses when more shards are missing than parity can rebuild or when the stored geometry does not match the set (`heal-concurrency-model.md`, [erasure-coding.md §9](../architecture/erasure-coding.md#9-healing)), and shares the `(bucket, object)` lock with PUT, delete, multipart complete, and data movement. Retained outcomes are described in [heal-terminal-reports.md](../architecture/heal-terminal-reports.md).

### 10.2 Drive replacement

1. Confirm the affected set still meets read quorum for its objects (impossible at `EC:0`, see §5) and record pool index, set index, slot, and the failed disk's UUID from the pool status.
2. Replace the drive at the **same** endpoint and mount path with an empty filesystem owned by RustFS. Do not copy the old drive's directories; a copied `format.json` claims an identity the set already tracks.
3. Bring the node back. The empty drive is not a format-quorum member, so `retain_format_quorum_members` keeps its slot offline; the endpoint monitor (`monitor_and_connect_endpoints_task` in `crates/ecstore/src/core/sets.rs`) retries the connection every 15 seconds. The erasure-set heal task (`crates/heal/src/heal/task/heal_erasure_set.rs`) writes the format into the original slot, through `heal_replacement_format` when an automatic replacement intent exists (schema 7 records under `.rustfs.sys/buckets/ahm-replacement/`, created when startup or the disk scanner observes a changed mount identity) or through `heal_format` for an admin erasure-set heal (`POST /rustfs/admin/v3/heal/` with `pool` and `set`), and then rebuilds the shards. Legacy schema 5/6 intents follow [replacement-generation-recovery.md](replacement-generation-recovery.md). If the drive held the node's pool metadata, follow [pool-metadata-recovery.md](pool-metadata-recovery.md#disk-replacement-and-metadata-erasure) first.
4. Watch `GET /rustfs/admin/v4/heal/replacement-recovery`, background heal status, disk health, and bitrot counters. Completion criteria are in [scanner-runtime-controls.md](scanner-runtime-controls.md#replacement-recovery-completion): an `idle` queue or a readable object is not proof.
5. The replacement is done when recovery reports `completed` for that instance and sampled objects have `xl.meta` and parts on the new drive.

If a set has lost more drives than its parity, heal cannot rebuild; recover from replication, a remote tier, or backup.

## 11. Restart and recovery

- **Ordinary restart.** No topology change in flight, identical arguments on every node, one node at a time per [rolling-restart.md](rolling-restart.md). Startup reloads pool metadata, `rebalance.bin`, the decommission queue, and heal recovery records, and resumes workers by leader and owner rules.
- **Rebalance.** Read `/rebalance/status` first. Handle active or stopping pools, `lastError`, cleanup warnings, and stop propagation before deciding to stop or restart. Do not delete `rebalance.bin`.
- **Decommission.** Non-terminal entries resume; failed and canceled entries survive restarts until cleared or retried through the allowed path (§9).
- **Root heal.** A graceful shutdown persists an unfinished cluster heal as `root-heal-<task-id>.json` in `.rustfs.sys` (`RootHealRecovery` in `crates/heal/src/heal/manager/root_recovery.rs`, 64 KiB bound) and replays it with the same task id. Terminal records use `terminal-root-heal-`; invalid, oversized, or unsupported records move to `quarantined-root-heal-intent-` or `quarantined-root-heal-terminal-` and are skipped with a log line until an operator repairs them. Do not delete quarantine markers or reuse a task id.
- **Unclean shutdown.** When the previous run left its marker behind, the heal manager's `process_unclean_shutdown` (`crates/heal/src/heal/manager/unclean_shutdown.rs`) enqueues a full erasure-set heal for every local set; graceful shutdown clears the marker. See [durability-modes.md](durability-modes.md) for which sync tier can leave what behind.

## 12. Mutual exclusion and prohibited actions

| Operation | With rebalance | With decommission | Notes |
|---|---|---|---|
| PUT, DELETE, multipart complete | Object lock | Source pool rejects new publication | Never bypass the pool fence |
| Decommission | Rejected while rebalance runs | Serial queue | Leader-only mutation |
| Rebalance | Multi-pool only | Rejected while decommission runs or conflicts | No query parameters |
| Admin heal | Object lock | Repairs data, never changes decommission state | `nolock` is ignored |
| Drive replacement | Cannot change `FormatV3` layout | Avoid on a source pool mid-decommission | Same slot, empty drive |

Do not: change launch endpoints while rebalance or decommission runs; delete `.rustfs.sys`, `rebalance.bin`, `pool.bin`, or root-heal records; run manual heal, file copies, and drive replacement on one pool at the same time; treat an HTTP 200 on a start call as completion.

## 13. Checklists

After start or expansion: identical endpoints and pool order on every node; topology equals `FormatV3`; peers ready and the pool metadata write gate open; new endpoints unique and RustFS-exclusive; GET, HEAD, range GET, and multipart verified.

After rebalance: no pool active or stopping; `lastError` empty or explained; `cleanupWarnings` handled; `pendingTerminalReload` false; migrated objects readable.

After decommission: status `completed`; other entries handled by policy; source pool rejects new writes; free versions, tier ownership, and listing entries converged; launch arguments updated on every node before drives are detached.

After heal or replacement: replacement recovery `completed`; slot and UUID correct; heal queue drained; no growing bitrot count; sampled current and historical versions readable; no leftover temporary directories.

## 14. `rc` CLI mapping

`rc` ships from the separate `rustfs/cli` repository; its admin reference is https://github.com/rustfs/cli/blob/main/docs/reference/rc/admin.md, and its `expand` group is an alias of `rebalance` (there is no server-side expand API). Flags below are the ones that reference documents; anything else, check `rc admin <group> --help` for the installed version.

```bash
rc alias set local http://node1:9000 <ACCESS_KEY> <SECRET_KEY>
rc ping local
rc ready local --timeout 2
rc admin info cluster local
rc admin info disk local --offline
rc admin pool list local
rc admin pool status local 0 --by-id
rc admin rebalance start local
rc admin rebalance status local
rc admin rebalance stop local
rc admin decommission start local '/data/pool1/disk{1...4}'
rc admin decommission status local 1 --by-id
rc admin decommission cancel local 1 --by-id
rc admin decommission clear local 1 --by-id
rc admin heal start local --scan-mode deep
rc admin heal start local --bucket logs --prefix 2026/ --scan-mode deep
rc admin heal status local --client-token <TOKEN_FROM_START>
rc admin heal stop local --client-token <TOKEN_FROM_START>
```

| `rc` command | Server route |
|---|---|
| `rc ping` / `rc ready` | `GET /health` / `GET /health/ready` |
| `rc admin pool list` / `status` | `GET /rustfs/admin/v3/pools/list` / `GET /rustfs/admin/v3/pools/status` |
| `rc admin rebalance …` (alias `expand`) | `/rustfs/admin/v3/rebalance/*` |
| `rc admin decommission start` / `cancel` / `clear` | `POST /rustfs/admin/v3/pools/decommission` / `cancel` / `clear` |
| `rc admin decommission status` | `GET /rustfs/admin/v3/decommission/status` |
| `rc admin heal start` / `status` / `stop` | `/rustfs/admin/v3/heal/*`, `POST /rustfs/admin/v3/background-heal/status` |

`rc admin heal start` exposes `--bucket`, `--prefix`, `--scan-mode`, `--remove`, `--recreate`, and `--dry-run`; it does not expose `nolock`, `forceStart`, `forceStop`, `pool`, or `set`. There is no `rc` command for replacement recovery; call `GET /rustfs/admin/v4/heal/replacement-recovery` directly.

## 15. Source and contract index

- [erasure-coding.md](../architecture/erasure-coding.md): algorithm, quorum, on-disk format, change procedure.
- [ecstore-layout-boundary.md](../architecture/ecstore-layout-boundary.md): `FormatV3` ordering and disk UUID positions.
- [placement-repair-invariants.md](../architecture/placement-repair-invariants.md): object to set placement, scanner and heal admission.
- [decommission-compatibility.md](../architecture/decommission-compatibility.md): decommission and rebalance contract, `PoolMeta`, status shape.
- [heal-concurrency-model.md](../architecture/heal-concurrency-model.md) and [heal-terminal-reports.md](../architecture/heal-terminal-reports.md): heal locking and retained reports.
- [pool-layout-compatibility.md](../testing/pool-layout-compatibility.md): expansion admission rules and regression matrix.
- [no-parity-bitrot-recovery.md](no-parity-bitrot-recovery.md), [durability-modes.md](durability-modes.md), [rolling-restart.md](rolling-restart.md), [pool-metadata-recovery.md](pool-metadata-recovery.md), [replacement-generation-recovery.md](replacement-generation-recovery.md), [scanner-runtime-controls.md](scanner-runtime-controls.md).
- `rustfs/src/startup_storage.rs`, `crates/ecstore/src/layout/disks_layout.rs`, `crates/ecstore/src/config/storageclass.rs`, `crates/ecstore/src/set_disk/mod.rs`, `crates/ecstore/src/set_disk/metadata.rs`, `crates/ecstore/src/set_disk/ops/heal.rs`, `crates/ecstore/src/core/pools.rs`, `crates/ecstore/src/services/rebalance/mod.rs`, `rustfs/src/admin/handlers/pools.rs`, `rustfs/src/admin/handlers/rebalance.rs`, `rustfs/src/admin/handlers/heal.rs`, `crates/heal/src/heal/manager/root_recovery.rs`, `crates/config/src/constants/heal.rs`.
