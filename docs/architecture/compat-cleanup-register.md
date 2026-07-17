# Compatibility Cleanup Register

Use this file to track temporary compatibility code introduced by architecture
migration PRs. Entries are required only for compatibility paths that are planned
for later deletion.

## Required Source Marker

```rust
// RUSTFS_COMPAT_TODO(<task-id>): <why this compatibility path exists>. Remove after <specific condition>.
```

## Open Items
- #4648 walk-dir stream completion capability: old clients can append fallback output to an already-used metacache writer after a terminal body error, so servers emit terminal walk errors only to clients that sign the walk_dir_stream_completion=error-v1 query capability and its request-body digest. Remove the legacy clean-EOF path after the minimum supported RustFS peer version always advertises this capability.

- `RUSTFS_COMPAT_TODO(RPC-4300)`
  - Task: `RPC-4300`
  - Files: `crates/ecstore/src/cluster/rpc/remote_disk.rs`, `crates/ecstore/src/disk/mod.rs`
  - Why: delete rollback requests use a versioned envelope and fail closed before mutation on peers that cannot decode it. The unsafe 5-field transactional retry was removed; an exact decode-boundary rejection only caches the peer as unsupported, while ordinary non-transactional requests remain dual-encoded.
  - Removal condition: remove the exact rejection matcher and unsupported-peer capability cache after the minimum supported rolling-upgrade predecessor handles delete rollback envelope version 1 for one full release window.
  - Status: planned cleanup.

- `RUSTFS_COMPAT_TODO(RPC-4300-RENAME)`
  - Task: `RPC-4300-RENAME`
  - Files: `crates/ecstore/src/cluster/rpc/remote_disk.rs`, `crates/ecstore/src/disk/mod.rs`, `rustfs/src/storage/rpc/node_service/disk.rs`
  - Why: new senders detect predecessors that reject the versioned rename transaction envelope, cache that peer as unsupported, and fail closed. The unsafe legacy pre-stage and blind rollback fallback was removed.
  - Removal condition: remove the exact decode-error matcher and unsupported-peer capability cache after the minimum supported rolling-upgrade predecessor accepts rename transaction envelope version 1 for one full release window.
  - Status: planned cleanup.

- `RUSTFS_COMPAT_TODO(RPC-4300-LOCK-REFRESH)`
  - Task: `RPC-4300-LOCK-REFRESH`
  - Files: `crates/ecstore/src/cluster/rpc/remote_locker.rs`, `crates/protos/src/node.proto`, `rustfs/src/storage/rpc/node_service/lock.rs`
  - Why: long 1000-key deletes require one batched lease refresh per locker; rolling upgrades fall back to unary refresh when a predecessor does not implement the batch method.
  - Removal condition: remove the unary fallback after every supported rolling-upgrade predecessor implements the batch refresh method. Negative capability evidence is intentionally not cached so an in-place peer upgrade can recover the batch path.
  - Status: planned cleanup.

## Review Checklist

Before completing a PR that adds wrappers, re-exports, fallbacks, legacy action
mappings, or old endpoint compatibility layers:

- [ ] The source has a `RUSTFS_COMPAT_TODO(<task-id>)` marker.
- [ ] This register has a matching entry.
- [ ] The entry states why compatibility is needed.
- [ ] The entry states the exact removal condition.
- [ ] The cleanup is not bundled with new migration logic.
