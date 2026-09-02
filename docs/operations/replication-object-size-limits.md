# Replication object size and shape limits (generic S3 targets)

**Use this when:** an object fails to replicate to an S3-compatible target with `EntityTooLarge`/`EntityTooSmall`, or you need to know whether a large or oddly-chunked object is replicable before relying on it.
**Source of truth:** `crates/ecstore/src/bucket/replication/` (transport selection and part replay), `crates/replication/` (target client), `crates/config/src/constants/` (`RUSTFS_OBS_LOGGER_LEVEL`).

What RustFS can and cannot replicate to a generic S3 target (AWS S3, Wasabi,
MinIO, or any other S3-compatible endpoint configured as a bucket replication
target), and how a rejected object shows up in the log.

## The route is chosen by the source object's shape, not its size

RustFS mirrors how the object was written on the source:

| Source object was written as | Replication transport |
| --- | --- |
| a single `PutObject` | a single `PutObject` on the target |
| a multipart upload | a multipart upload replaying **the source's own part layout** |

RustFS does not re-chunk on the replication side. A single-`PutObject` object is
never converted into a multipart upload for the target, and a multipart object's
parts are never merged or re-split. The target's part layout is the source's,
because heal and delete convergence address the replica by that identity.

This is why object size alone does not tell you whether an object is
replicable — how it was uploaded does.

The shape is read from the object's own ETag: a multipart ETag carries a
`-<part count>` suffix. Nothing else selects the transport — in particular the
checksum algorithm and checksum type (`COMPOSITE` or `FULL_OBJECT`) an object
was uploaded with have no bearing on it.

## Limits

### Single-`PutObject` objects: 5 GiB

S3 caps `PutObject` at **5 GiB**. This is an S3 API limit that every target
enforces, not a RustFS tunable.

An object larger than 5 GiB that was written to the source with a single
`PutObject` therefore **cannot be replicated to a generic S3 target**. RustFS
detects this before streaming the body and fails the object immediately, rather
than uploading gigabytes only to collect an `EntityTooLarge` from the remote.

**Remedy:** re-upload the object using multipart. Most S3 clients do this
automatically above a threshold (the AWS CLI defaults to 8 MiB); a client
configured with a very high multipart threshold, or one that streams a single
`PutObject`, is the usual way an object ends up on the wrong side of this limit.

### Multipart objects: the target's multipart limits, applied to the source's layout

Because the source's part layout is replayed verbatim, the target's own
multipart constraints apply to that layout:

| Constraint | Target rejects with |
| --- | --- |
| every part except the last must be ≥ 5 MiB | `EntityTooSmall` |
| no part may exceed 5 GiB | `EntityTooLarge` |
| at most 10,000 parts | failure at `CompleteMultipartUpload` |

A source object whose parts satisfy these is replicable up to the S3 multipart
maximum of 5 TiB.

## Reliability characteristics for large objects

Worth knowing before replicating multi-gigabyte objects:

- Parts are transferred **sequentially**.
- There is **no part-level retry**. A failure on any single part fails the whole
  object; the target-side multipart upload is then aborted so no incomplete
  upload is left behind.
- Retry happens at the object level (MRF replay / heal scanner), so a failure
  late in a large transfer re-sends the object from the beginning.

For a 6 GiB object this means one long all-or-nothing transfer window. Part-level
retry and resumable transfer are tracked as a separate improvement.

## What a failed object looks like in the log

A replication attempt that ends in a terminal `FAILED` state emits one `error`
line per failed target. It is at `error` deliberately: the default log level
(`RUSTFS_OBS_LOGGER_LEVEL`, default `error`) must not hide an object that never
reached its target.

```
ERROR ... event=replication_object_failed bucket=photos object=backups/vm-image.qcow2
      version_id=... arn=arn:replication::wasabi endpoint=s3.wasabisys.com
      op_type=OBJECT size=6442450944 replication_status=FAILED
      error="object of 6442450944 bytes was not written as multipart on the source and
             exceeds the 5368709120 byte single-PutObject limit of an S3 target;
             re-upload it with multipart to make it replicable"
      Replication failed for object
```

The `error` field carries the target's own error code and message where the
target produced one, so a remote rejection is diagnosable without lowering the
log level and reproducing. It is passed through the same redaction as the
persisted resync detail, so an error echoing a credential or signed URL is
replaced with `[redacted sensitive resync error detail]`.

Raise `RUSTFS_OBS_LOGGER_LEVEL` to `warn` to additionally see the per-attempt
failure branches (target offline, HEAD failures, per-part errors) that sit
underneath this summary.

## Related

- [Replication target check](replication-check.md) — validate a target's
  configuration, versioning, and version fidelity before relying on it.
- [Presigned size limits](presigned-size-limits.md) — per-request and per-upload caps a backend can put on presigned uploads.
