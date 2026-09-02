# No-Parity Bitrot Recovery Guide

**Use this when:** a GET or deep heal reports `FileCorrupt` / `bitrot hash mismatch` on an object whose `xl.meta` shows `EcN=0` (data shards, no parity), and the raw `part.N` file is still readable from the filesystem.
**Source of truth:** `crates/ecstore/src/set_disk/ops/heal.rs` (heal returns `FileCorrupt` for confirmed no-parity bitrot, `ErasureReadQuorum` otherwise); `crates/filemeta/examples/dump_fileinfo.rs` (offline `xl.meta` decoder).

This guide covers historical objects written with erasure data shards but no parity shards (for example `EcM=1`, `EcN=0`). The write path now self-verifies no-parity writes and refuses to commit an object whose shard fails bitrot verification, so new objects of this class cannot be created, but already committed ones may still exist on disk.

## Symptom

- The raw shard file (`part.1`) is visible and readable from the local filesystem.
- An S3 GET or deep heal reports an integrity failure: `FileCorrupt`, `bitrot hash mismatch`, an unrecoverable heal result, or a truncated streaming response.
- No parity shards exist to reconstruct the corrupted data shard.

The filesystem-readable `part.N` is evidence, not trusted object data: the stored hash no longer matches the bytes on disk, and RustFS must not bypass bitrot validation to serve it.

## What to capture

Before deleting or moving anything, capture:

1. Bucket name, object key, and version ID if versioning is enabled.
2. The RustFS version, and whether the deployment ran with no parity (`EcN=0`) when the object was written.
3. The heal or GET error text.
4. `xl.meta` and the raw `part.N` file from every shard disk that still has the object.

Decode metadata locally and record the erasure geometry (`EcM`, `EcN`), object size, part number, part logical size, data directory, and checksum algorithm:

```bash
cargo run -p rustfs-filemeta --example dump_fileinfo -- /path/to/disk/bucket/object/xl.meta
```

## Size accounting

Erasure shard files include bitrot hash data in addition to object bytes: with the default `HighwayHash256S` checksum each protected block adds 32 bytes, so a raw `part.1` larger than the logical object size is normal (for example 8,250,370 logical bytes in 8 blocks → 8,250,626 raw bytes). The size relationship only shows the layout is plausible; the bitrot reader is the authority for integrity.

## Recovery boundary

If `EcN=0` and a data shard fails bitrot verification, RustFS cannot reconstruct the object from the erasure set. The valid options are:

- Restore the object from an external backup, replica, upstream source, or a known-good copy outside the affected erasure set.
- Preserve the affected `xl.meta` and `part.N` files as incident evidence, then delete the object through the normal S3/admin path when retention policy allows.
- Quarantine by copying evidence out of the live data path first; remove or isolate the live object path only after the incident owner confirms the evidence is no longer needed.

Do not edit `xl.meta`, rewrite `part.N`, or serve raw shard bytes to clients as the object. Those actions hide evidence and convert a detected integrity failure into silent data corruption.

If `EcN>0`, this guide is not the primary recovery path: run normal heal first, since parity may allow RustFS to reconstruct the missing or corrupt shard.

## Expected diagnostics

Deep heal reports no-parity corruption as an unrecoverable integrity failure rather than a generic read-quorum problem: the heal error is `FileCorrupt`, and the heal result `detail` states that the no-parity object is unrecoverable. The diagnostic context includes bucket, object, and version ID; data and parity shard counts; part number; whether the failing part had a bitrot failure; and the number of missing or corrupt shards.
