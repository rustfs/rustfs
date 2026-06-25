# Compatibility Cleanup Register

Use this file to track temporary compatibility code introduced by architecture
migration PRs. Entries are required only for compatibility paths that are planned
for later deletion.

## Required Source Marker

```rust
// RUSTFS_COMPAT_TODO(<task-id>): <why this compatibility path exists>. Remove after <specific condition>.
```

## Open Items

- `CTX-002`
  - Why: admin OIDC and STS consumers now resolve through `resolve_oidc_handle()`, but that resolver still falls back to legacy global OIDC state while the AppContext-owned OIDC wiring is being completed.
  - Remove after: OIDC ownership is initialized and consumed through AppContext end-to-end, and `resolve_oidc_handle()` no longer needs the legacy global fallback path.

## Review Checklist

Before completing a PR that adds wrappers, re-exports, fallbacks, legacy action
mappings, or old endpoint compatibility layers:

- [ ] The source has a `RUSTFS_COMPAT_TODO(<task-id>)` marker.
- [ ] This register has a matching entry.
- [ ] The entry states why compatibility is needed.
- [ ] The entry states the exact removal condition.
- [ ] The cleanup is not bundled with new migration logic.
