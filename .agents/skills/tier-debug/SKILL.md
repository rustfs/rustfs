---
name: tier-debug
description: Debug ILM tiering / lifecycle transition issues — NoSuchVersion on tier GET, restore failures, xl.meta inspection, remote-tier versionId tracing. Use when investigating tiered/transitioned objects, warm backends, or transition metadata.
---

# Tier / ILM Debugging

Full playbook: [docs/operations/tier-ilm-debugging.md](../../../docs/operations/tier-ilm-debugging.md)
— read it before changing tier code.

Quick moves:

```bash
# Inspect transition metadata on disk (one xl.meta per erasure shard disk)
cargo run -p rustfs-filemeta --example dump_fileinfo -- "/path/to/{bucket}/{object}/xl.meta"

# Trace what versionId is sent to the remote tier
RUST_LOG=rustfs_ecstore::bucket::lifecycle=debug ./target/debug/rustfs …
```

Interpretation:

- `transition_ver_id: <none>` → correct for an unversioned tier bucket; no
  `versionId` must be sent on tier GET/DELETE.
- `transition_ver_id: 00000000-…` (nil) → corrupt legacy write-back; readers
  must filter it out, never send it.
- Empty-string `transitioned-versionID` metadata under both
  `x-rustfs-internal-*` and `x-minio-internal-*` keys → object went to an
  unversioned tier bucket.

Code entry points: `crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs`
(ILM actions), `crates/ecstore/src/services/tier/` (warm backends),
`crates/filemeta/src/filemeta/version.rs` (metadata read/write + regression
tests).
