# Compatibility Cleanup Register

Use this file to track temporary compatibility code introduced by architecture
migration PRs. Entries are required only for compatibility paths that are planned
for later deletion.

## Required Source Marker

```rust
// RUSTFS_COMPAT_TODO(<task-id>): <why this compatibility path exists>. Remove after <specific condition>.
```

## Open Items

- `scanner-usage-v2` persisted scanner usage migration: pre-v2 scanners write `.usage.json`, so upgraded clusters read that primary/backup pair only while the authoritative `.usage.v2.json` pair is absent and continue removing deleted buckets from legacy copies that still exist. Remove the fallback and legacy cleanup after every supported direct-upgrade source version writes `.usage.v2.json`.
- `ns-scanner-rpc-v3` namespace scanner capability and activity handshake: old peers and legacy internode transports lack the authenticated startup-epoch handshake and send scanner activity requests without a challenge. The activity codec accepts their field-empty protocol-0 response for wire compatibility, but the distributed scanner leaves the cycle incomplete and publishes no usage until every peer can prove its storage topology and data-movement state with authenticated protocol v4. After that cluster-wide fence is established, scanner selection treats HTTP 404/405/426 and the legacy MethodNotAllowed default as an explicit lack of remote scanner v3 support and assigns those disks to coordinator-driven workers; transient capability failures remain incomplete and do not activate the fallback. Remove the coordinator fallback after the minimum supported RustFS peer version implements namespace scanner protocol v3, and remove the activity shim after every supported peer implements authenticated scanner activity protocol v4; a future protocol revision must keep a dual-version server/codec window before changing the advertised version.
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
