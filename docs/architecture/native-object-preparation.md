# Native Object Preparation

**Use this when:** changing private native object preparation, durable shard receipts, retry placement, or recovery before capture publication.

## Authority boundary

`ECStore::prepare_native_object` in `crates/ecstore/src/store/native_prepare.rs`
consumes actual reader bytes and returns `NativeQuorumPreparation` only after a
write quorum of distinct local disks has durably prepared matching object facts.
Its private receipt collection prevents callers from constructing a quorum out
of unchecked values. A preparation proves neither capture consensus nor public
object visibility. Its version identifier is private metadata identity, not an
assertion that an S3 version is readable. Preparation identifiers and capture
decision identifiers remain separate.

```mermaid
flowchart LR
    R[Actual object reader] --> P[Probe private placement]
    P --> E[Existing erasure and bitrot encoder]
    E --> D[Local disk intent and shard barriers]
    D --> Q[Distinct matching write-quorum receipts]
    Q --> O[Private preparation result]
```

## Encoding and placement

Small local candidates retain the bounded inline preparation path. Larger or
unknown-length candidates use the ordinary streaming erasure/bitrot encoder to
write private part files, with a fixed-size SHA-256 read buffer rather than
buffering the object. Unsupported owners fail explicitly; preparation never
falls back to a visible public PUT.
`SetDisks::prepare_native_bytes` in
`crates/ecstore/src/set_disk/ops/native_prepare.rs` uses the existing write layout
and encoder. Verification uses the codec indicated by the decoded metadata,
including its shard width and checksum compatibility mode.

New candidates use the existing pool/set selection. Recovery searches actual
sets for the preparation UUID, then verifies the retained physical disk set and
distribution rather than recomputing placement from the current key hash.
Existing private intents fix placement for retries. Absence is established only against a healthy disk identity;
unreadable state in another pool prevents assigning a new pool. Conflicting
identities are errors, even if another subset could form a quorum.

## Persistence and cancellation

`LocalDisk::prepare_native` in
`crates/ecstore/src/disk/local/native_prepare.rs` owns the metadata, object, and
private preparation lock chain through the blocking I/O and durability barriers.
Detached disk writes retain the parent object owner after early quorum return or
caller cancellation. An unobserved response does not authorize rollback.

Version 2 intents retain the original raw metadata so an intent-only interruption
can be repaired without reserializing metadata maps. Version 3 streaming intents
also retain the ordered physical disk UUID set and data directory UUID. The exact
FileInfo supplies the part manifest; each file's bitrot data and the aggregate
shard digest/length are verified before the record and prepared marker barriers.
Part files and their directory are synced before a disk can attest preparation. Existing version 1 intents
keep their original canonical representation and marker. A missing shard with an
existing prepared marker is corruption, not a successful preparation.

## Recovery

Recovery acquires fresh lifecycle and namespace owners, verifies persisted disk
facts, decodes the actual shards, and checks the complete object length and SHA-256.
It requires the write quorum and consistent identity, codec, modification time,
part manifest and block geometry. It decodes each retained part into a streaming
digest sink, so recovery does not allocate a buffer proportional to object size. Retry preserves original metadata bytes and rejects changed
content. Fewer receipts or unknown I/O outcomes leave durable facts available for
recovery without granting publication authority.

The actual filesystem tests are in
`crates/ecstore/src/store/native_prepare_tests.rs` and
`crates/ecstore/src/store/native_prepare_multi_pool_tests.rs`; local barrier and
format compatibility tests remain beside the LocalDisk implementation.
