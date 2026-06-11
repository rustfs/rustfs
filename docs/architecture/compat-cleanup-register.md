# Compatibility Cleanup Register

Use this file to track temporary compatibility code introduced by architecture
migration PRs. Entries are required only for compatibility paths that are planned
for later deletion.

## Required Source Marker

```rust
// RUSTFS_COMPAT_TODO(<task-id>): <why this compatibility path exists>. Remove after <specific condition>.
```

## Open Items

- `RUSTFS_COMPAT_TODO(S-012)`
  - Task: `S-012`
  - File: `rustfs/src/admin/handlers/kms_keys.rs`
  - Why: legacy KMS create-key and key-status admin grants must keep working during the dedicated KMS policy migration.
  - Removal condition: remove after KMS admin clients and built-in policies use `kms:Configure`, `kms:DescribeKey`, and `kms:ListKeys`.
  - Status: planned cleanup.
- `RUSTFS_COMPAT_TODO(API-003)`
  - Task: `API-003`
  - File: `crates/ecstore/src/store_api/types.rs`
  - Why: old `ecstore::store_api` bucket DTO import paths must keep compiling while storage API consumers migrate.
  - Removal condition: remove after all consumers import bucket DTOs from the storage API crate.
  - Status: planned cleanup.
- `RUSTFS_COMPAT_TODO(API-012)`
  - Task: `API-012`
  - File: `crates/ecstore/src/store_api/traits.rs`
  - Why: old `StorageAPI::new_ns_lock` callers must keep compiling while namespace-lock-only consumers migrate to NamespaceLocking.
  - Removal condition: remove after all namespace-lock-only consumers depend on NamespaceLocking and StorageAPI no longer owns namespace lock capability.
  - Status: planned cleanup.

## Review Checklist

Before completing a PR that adds wrappers, re-exports, fallbacks, legacy action
mappings, or old endpoint compatibility layers:

- [ ] The source has a `RUSTFS_COMPAT_TODO(<task-id>)` marker.
- [ ] This register has a matching entry.
- [ ] The entry states why compatibility is needed.
- [ ] The entry states the exact removal condition.
- [ ] The cleanup is not bundled with new migration logic.
