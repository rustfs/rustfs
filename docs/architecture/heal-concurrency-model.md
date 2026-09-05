# Heal concurrency model

**Use this when:** changing heal, PUT/multipart commit, delete, lifecycle expiry, or data-movement code that touches the same `(bucket, object)` commit surface; or evaluating whether RustFS needs a persistent per-object healing marker like MinIO's `x-minio-healing`.
**Source of truth:** `crates/ecstore/src/set_disk/ops/heal.rs` (`heal_object_with_explicit_version_regen`, `HealObjectLockKind`, `HEAL_RENAME_INCOMPLETE`), `crates/ecstore/src/set_disk/ops/object.rs` (PUT/DELETE lock sections, `reconcile_old_data_cleanup_receipts`), `crates/ecstore/src/set_disk/core/io_primitives.rs` (`commit_rename_data_dir`, `report_old_data_dir_cleanup`, `reclaim_orphan_data_dirs`), `crates/filemeta/src/fileinfo.rs` (`FileInfo::set_healing`), `crates/heal/src/heal/manager/queue.rs` (dedup keys).

For crate ownership, read [crate-boundaries.md](crate-boundaries.md): ECStore owns erasure-set repair primitives that share this lock and commit model, while `crates/heal` owns repair orchestration.

## Model

Heal and every foreground or background write path serialize on the same object-level namespace write lock (a quorum lock RPC in distributed mode, the in-process lock manager on a single node; granularity is the object, the version component is always `None`), and heal holds its guard across the whole rename commit. This describes the intended lock scope while the guard remains valid; it does not prove rejection of an already-dispatched disk syscall after distributed lease loss. The authority, delayed-mutation, and recovery boundary is specified in [unified-object-generation.md](unified-object-generation.md). MinIO's `x-minio-healing` marker is an out-of-lock defence against version-cleanup logic inside `RenameData` interleaving with a heal commit; RustFS's commit model has no such interleaving, so no persistent marker exists (`x-minio-healing` does not occur in `crates/` or `rustfs/`) and none is needed. Three layers replace it:

| Layer | Mechanism | Owner |
| --- | --- | --- |
| In-lock mutual exclusion | Heal and all write-path commit points take the `(bucket, object)` namespace write lock. | `acquire_heal_object_lock` in `crates/ecstore/src/set_disk/ops/heal.rs`; lock sections in `ops/object.rs` and `ops/multipart.rs` |
| Commit-model isolation | `rename_data` contains no version cleanup that could interleave with heal. Physical deletion of a replaced old `data_dir` runs after the object lock is released (the commit tail) and only for unshared directories already superseded by the new commit. | `commit_rename_data_dir` in `crates/ecstore/src/set_disk/core/io_primitives.rs` |
| Transient healing flag | `FileInfo::set_healing` sets the internal `SUFFIX_HEALING` key on the in-memory `FileInfo` of a heal commit; `rename_data` reads it through `is_healing` to clear a stale non-empty target `data_dir` before the rename (in-place repair reuses the `data_dir`, and `rename(2)` cannot replace a non-empty directory). The key is never persisted (`is_skip_meta_key` in `crates/filemeta/src/filemeta.rs`). A non-heal commit that meets a non-empty target fails explicitly; tests lock both directions. | `crates/filemeta/src/fileinfo.rs`, `crates/ecstore/src/disk/local.rs` |

## Heal lock scope

`heal_object` delegates to `heal_object_with_explicit_version_regen`, which takes the namespace write lock at entry unless `opts.no_lock` is set and binds the guard to the function scope. The guard covers the quorum metadata read, EC reconstruction, per-disk rename commit, tmp cleanup, the `HEAL_RENAME_INCOMPLETE` partial-commit return, and orphan `data_dir` reclamation (`reclaim_orphan_data_dirs`).

Read-repair heals (`opts.read_repair`) hold a shared lock (`HealObjectLockKind::Read`) during reconstruction so readers keep flowing, then `acquire_revalidated_read_repair_commit_lock` takes the write lock and re-reads a commit fingerprint; a changed fingerprint aborts the commit (`read_repair_commit_stale`).

## Lock-intersection matrix

| # | Concurrent path | Lock held by that path | Outcome | Where |
| --- | --- | --- | --- | --- |
| 1 | PUT commit | object write lock; `rename_data` inside it | serialized | `ops/object.rs` put commit |
| 2 | PUT old `data_dir` tail cleanup | none (runs after the lock is dropped) | unlocked, semantically safe ([commit tail](#commit-tail-cleanup)) | `commit_rename_data_dir` in `core/io_primitives.rs` |
| 3 | DELETE object or version | object write lock; `delete_version` inside it | serialized | `ops/object.rs` `delete_object` |
| 4 | Batch DELETE | per-object write locks (batch lock RPC in distributed mode) | serialized | `ops/object.rs` `delete_objects` |
| 5 | CompleteMultipartUpload | object write lock plus upload-path lock; rename inside | serialized | `ops/multipart.rs` |
| 6 | CompleteMultipart tail cleanup | none (after lock drop) | unlocked, semantically safe ([commit tail](#commit-tail-cleanup)) | `ops/multipart.rs` |
| 7 | AbortMultipartUpload | upload-path lock in the multipart bucket only | disjoint resources: abort never touches the object `data_dir` or `xl.meta` | `ops/multipart.rs` |
| 8 | ILM expiry including DeleteAllVersions | `delete_prefix_object=true` keeps the object lock; `FreeVersionTask` locks explicitly; noncurrent batches use batch locks | serialized | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` |
| 9 | Pure prefix delete | `delete_prefix` without `delete_prefix_object` takes no child-object lock | unlocked; no production caller ([prefix delete](#pure-prefix-delete)) | `ops/object.rs` lock condition in `delete_object` |
| 10 | Orphan `data_dir` reclamation | none inside the function; its only production caller runs inside the heal lock | serialized within heal | `reclaim_orphan_data_dirs` in `core/io_primitives.rs` |
| 11 | Old-cleanup receipt reconciliation | none inside the function; caller runs inside the heal lock and an epoch fence rejects stale receipts | serialized | `reconcile_old_data_cleanup_receipts` in `ops/object.rs` |
| 12 | Replication | data plane writes to the remote over HTTP; local metadata write-back takes the object lock | serialized or disjoint | `crates/ecstore/src/bucket/replication/replication_resyncer.rs` |
| 13 | Data movement, rebalance, decommission source cleanup | explicit object lock plus version-unchanged recheck; `no_lock` only reuses an already-held guard | serialized | `crates/ecstore/src/data_movement/mod.rs` |
| 14 | CopyObject | destination object lock through the PUT chain | serialized | `ops/object.rs` `copy_object` |
| 15 | Another heal task (different `HealType`, or `force_start`) | dedup keys are per `HealType` and `force_start` skips dedup, so tasks may coexist | serialized on the namespace write lock | `make_dedup_key_for_type` in `crates/heal/src/heal/manager/queue.rs` |
| 16 | Admin heal with `nolock=true` | caller bypasses the lock | unlocked by operator choice ([no_lock](#no_lock-and-force_start)) | `rustfs/src/admin/handlers/heal.rs` |
| 17 | Stale multipart cleanup | upload-path lock in the multipart bucket | disjoint resources | `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs` |

## Residual windows

### Commit tail cleanup

Rows 2 and 6. After a write path commits and releases the object lock, it best-effort deletes the replaced old `data_dir`; the code deliberately does not block the next operation on this. The deletion can race a concurrent heal reading or rebuilding that same old `data_dir`, but the race is semantically safe:

- The target is an unshared `data_dir` already replaced by the new commit. Heal's canonical metadata comes from quorum arbitration (ETag, mod time), and quorum already points at the new version, so heal cannot resurrect the replaced version as canonical.
- The worst outcome is one transient failure or no-op for the heal round on the old version; the next round converges. Cleanup residue is reported and re-queued for heal via `report_old_data_dir_cleanup`.
- Long heals such as drive replacement request explicit versions and read quorum metadata inside the lock, so the tail does not affect them.

### Pure prefix delete

Row 9. `delete_prefix && !delete_prefix_object` takes no child-object locks (an object namespace lock cannot protect a recursive prefix delete), so a heal running during the prefix delete could theoretically rebuild a version from stale quorum metadata. Every production `delete_prefix: true` call site also sets `delete_prefix_object: true` (and therefore takes the object lock); the remaining `delete_prefix`-only call sites are in test modules. A future caller that needs a pure prefix delete must prove isolation from heal and scanner at the call site (for example a bucket-level scan fence).

### `no_lock` and `force_start`

Row 16. Admin heal requests pass the client's `nolock` parameter through (`rustfs/src/admin/handlers/heal.rs`), matching the MinIO madmin option. Setting it is an explicit operator choice that accepts races with concurrent writes; it is documented, not restricted.

Heal-side invariants that hold regardless of the caller:

- Dedup keys are disjoint across `HealType` (object, metadata, MRF, EC decode, prefix), and admin `force_start` skips dedup. Several heal tasks for one object can therefore exist at once, but every production entry calls `heal_object` with `no_lock=false`, so their execution bodies serialize on the namespace write lock.
- Read-repair's local TTL reservation dedups only its own source and does not block heals from other sources; the namespace lock is the backstop.
- The healing flag is never persisted, so there is no reverse risk of a leftover marker making a later commit yield incorrectly.

## Regression tests

Both live in the test module of `crates/ecstore/src/set_disk/ops/heal.rs`:

| Test | Invariant |
| --- | --- |
| `heal_racing_version_delete_never_resurrects_the_deleted_version` | With a doomed version's shards corrupted, a versioned DELETE and a deep heal contend on the same lock; the deleted version is not resurrected and the surviving version is intact. |
| `heal_racing_unversioned_overwrites_preserves_the_last_commit` | Unversioned overwrite commits (exercising the commit-tail old `data_dir` deletion) race a deep-heal loop; the final current version is exactly the last commit (ETag-level equality). |

Related: the atomic-commit and best-effort-rollback invariants for the write path are in [erasure-coding.md](erasure-coding.md).
