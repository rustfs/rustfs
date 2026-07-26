# No-Parity Bitrot Recovery Guide

This guide covers historical objects written with erasure data shards but no
parity shards, for example an object whose `xl.meta` reports `EcM=1` and
`EcN=0`. PR #5179 prevents RustFS from committing new objects after it detects
this class of no-parity bitrot failure, but operators may still find already
committed objects on disk.

## Symptom

An affected object can look surprising during incident response:

- the raw shard file, such as `part.1`, is visible and readable from the local
  filesystem;
- an S3 GET or deep heal reports an integrity failure, commonly through
  `FileCorrupt`, `bitrot hash mismatch`, or an unrecoverable heal result;
- there are no parity shards available to reconstruct the corrupted data shard.

The filesystem-readable `part.N` file is therefore evidence, not trusted object
data. RustFS must not bypass bitrot validation to serve it through S3, because
the stored hash no longer matches the bytes on disk.

## What To Capture

Before deleting or moving anything, capture:

- bucket name, object key, and version ID if versioning is enabled;
- the RustFS version and whether the deployment was running with no parity
  (`EcN=0`) at the time the object was written;
- the heal or GET error, including any `FileCorrupt`, `bitrot hash mismatch`,
  `ErasureReadQuorum`, or truncated streaming response message;
- `xl.meta` from every shard disk that still has the object;
- the raw `part.N` file from every shard disk that still has the object.

For local inspection, decode metadata with:

```bash
cargo run -p rustfs-filemeta --example dump_fileinfo -- /path/to/disk/bucket/object/xl.meta
```

Record the erasure geometry (`EcM`, `EcN`), object size, part number, part
logical size, data directory, and checksum algorithm from the decoded metadata.

## Size Accounting

RustFS erasure shard files include bitrot hash data in addition to object bytes.
For the default HighwayHash256S checksum, each protected block adds 32 bytes of
hash data to the shard file. A raw `part.1` size can therefore be larger than
the object logical size and still be normal.

Example:

```text
logical object bytes: 8,250,370
protected blocks: 8
hash overhead: 8 * 32 = 256 bytes
raw part.1 bytes: 8,250,626
```

This size relationship only proves that the file layout is plausible. It does
not prove the bytes are valid. The bitrot reader is the authority for integrity.

## Recovery Boundary

If `EcN=0` and a data shard fails bitrot verification, RustFS cannot reconstruct
the object from the erasure set. The valid recovery options are:

- restore the object from an external backup, replica, upstream source, or a
  known-good copy outside the affected erasure set;
- preserve the affected `xl.meta` and `part.N` files as incident evidence, then
  delete the object through the normal S3/admin path when retention policy
  allows it;
- quarantine by copying evidence out of the live data path first, then remove
  or isolate the live object path only after the incident owner confirms the
  evidence is no longer needed.

Do not edit `xl.meta`, rewrite `part.N`, or serve raw shard bytes to clients as
the object. Those actions hide evidence and can convert a detected integrity
failure into silent data corruption.

If `EcN>0`, this guide is not the primary recovery path. Use normal heal first;
parity may allow RustFS to reconstruct the missing or corrupt shard.

## Expected Diagnostics

Deep heal should report no-parity corruption as an unrecoverable integrity
failure rather than only a generic read-quorum problem. The diagnostic context
should include:

- bucket, object, and version ID;
- erasure data shard count and parity shard count;
- part number;
- whether the failing part had a bitrot failure;
- the number of missing or corrupt shards.

When the failure is confirmed bitrot on a no-parity object, the heal error is
reported as `FileCorrupt`, and the heal result `detail` states that the
no-parity object is unrecoverable.
