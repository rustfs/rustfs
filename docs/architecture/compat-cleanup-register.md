# Compatibility Cleanup Register

Use this file to track temporary compatibility code introduced by architecture
migration PRs. Entries are required only for compatibility paths that are planned
for later deletion.

## Required Source Marker

```rust
// RUSTFS_COMPAT_TODO(<task-id>): <why this compatibility path exists>. Remove after <specific condition>.
```

## Open Items

- `#4648` walk-dir stream completion capability: old clients can append fallback output to an already-used metacache writer after a terminal body error, so servers emit terminal walk errors only to clients that sign the `walk_dir_stream_completion=error-v1` query capability and its request-body digest. Remove the legacy clean-EOF path after the minimum supported RustFS peer version always advertises this capability.
- `heal-rpc-auth-v2` internode gRPC authentication: servers temporarily accept legacy prefix signatures so old peers remain available during rolling upgrades. Remove the legacy fallback after the minimum supported RustFS peer version sends v2 authentication on every internode gRPC request.
- `heal-status-rpc-v1` node heal status capability: new peers treat an unimplemented BackgroundHealStatus RPC as an explicitly incomplete rolling-upgrade response. Remove the fallback after the minimum supported RustFS peer version implements BackgroundHealStatus.

## Review Checklist

Before completing a PR that adds wrappers, re-exports, fallbacks, legacy action
mappings, or old endpoint compatibility layers:

- [ ] The source has a `RUSTFS_COMPAT_TODO(<task-id>)` marker.
- [ ] This register has a matching entry.
- [ ] The entry states why compatibility is needed.
- [ ] The entry states the exact removal condition.
- [ ] The cleanup is not bundled with new migration logic.
