# Replication outbound transport

**Use this when:** a bucket-replication or site-replication target rejects, corrupts, or silently transforms uploads from RustFS, or you need to know which integrity headers RustFS sends to a remote target and how to change them.
**Source of truth:** `crates/ecstore/src/bucket/remote_s3_client.rs` (`replication_request_checksum_calculation`), `crates/ecstore/src/bucket/bucket_target_sys.rs` (`TargetClient::put_object`, `PutObjectOptions::header`), `crates/ecstore/src/bucket/replication/replication_resyncer.rs` (`verify_single_part_replica`).

## What a replication PUT carries by default

- A plain signed body with an exact `Content-Length`. The SDK does not add a streaming trailer checksum, so the body is never wrapped in `aws-chunked` framing (rustfs#6853: a target that does not decode that framing stored the frames verbatim while RustFS recorded COMPLETED).
- For a single-part object, the checksum the source object was uploaded with, forwarded as its `x-amz-checksum-<algorithm>` header (the value the source verified on upload). A multipart replica is rebuilt through CreateMultipartUpload/UploadPart and carries no object-level checksum header. Managed-SSE objects forward none.
- On a PUT that carries Object Lock parameters and no forwarded checksum: `Content-MD5` derived from the source ETag, or an SDK CRC32 checksum when the ETag is not the MD5 of the wire bytes (rustfs#7082).
- The source ETag, mtime and version id on `x-rustfs-source-*` headers (with `x-minio-source-*` twins), and the Object Lock mode, retain-until date and legal hold of the source version when present.
- After the PUT, the target's ETag is compared with the source ETag when both are plain single-part MD5s; a mismatch fails the replication instead of reporting a corrupted replica as COMPLETED.

## Target classes and their known requirements

| Target behavior | Effect on RustFS replication | Detected by |
| --- | --- | --- |
| Rejects or mis-stores `aws-chunked` bodies (SeaweedFS 3.97) | Handled by the plain-payload default above. | Outbound target matrix, `RejectAwsChunked` mode |
| Requires `Content-MD5` or `x-amz-checksum-*` on a PutObject with Object Lock parameters (AWS S3, MinIO, Impossible Cloud, most compatible stores) | Satisfied: a locked single PUT carries `Content-MD5` derived from the source ETag (plaintext objects whose ETag is the MD5 of the wire bytes) or an SDK CRC32 checksum (multipart-layout ETags, managed SSE, SSE-C passthrough — this one is an `aws-chunked` trailer, so a target that also rejects that framing cannot take such objects). Releases before this fix (`1.0.0-rc.5`) need `RUSTFS_REPLICATION_STREAMING_CHECKSUMS=true` as a workaround. | Outbound target matrix, `RequireChecksumWithObjectLock` mode |
| Stores `x-amz-checksum-*` from a PutObject and returns it on `HEAD ?ChecksumMode=ENABLED` (AWS S3, Wasabi, RustFS) | Satisfied for single-part objects: the replica answers with the source's checksum. Before this fix (`1.0.0-rc.5`) the checksum left the source as `x-amz-meta-<algorithm>` user metadata and no replica carried it (rustfs/backlog#2340). | Outbound target matrix, `Checksummed` shape |
| Mints its own version ids (AWS S3, Wasabi, Impossible Cloud) | Data lands; version-addressed convergence does not. See rustfs/backlog#2085 and `docs/operations/replication-check.md` (VersionFidelity). | `replication-check`, outbound target matrix, `MintOwnVersionIds` mode |
| Returns an ETag that is not the content MD5 without announcing SSE | Every single-part object fails ETag verification. Set `RUSTFS_REPLICATION_REPLICA_ETAG_VERIFY=false`. | Replication status FAILED with `replica etag mismatch` |

## Environment knobs

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUSTFS_REPLICATION_STREAMING_CHECKSUMS` | unset (plain payloads) | `true` or `1` restores SDK trailer checksums (`RequestChecksumCalculation::WhenSupported`). Every streaming upload is then `aws-chunked` with an `x-amz-trailer`, except a single-part PUT that forwards the source's `x-amz-checksum-*` header, which is sent plain so the target does not receive a second algorithm; use only when every target decodes that framing. |
| `RUSTFS_REPLICATION_REPLICA_ETAG_VERIFY` | enabled | `false` or `0` disables the post-PUT ETag comparison for targets whose 32-hex ETags are legitimately not the content MD5. |

Both knobs are read by the RustFS process that owns the replication target, at client build time; restart the server after changing them.

### Remote tier transport timeouts

Remote tier S3-compatible clients use separate transport budgets. These settings do not change bucket or site replication clients.

| Variable | Default | Meaning |
| --- | --- | --- |
| `RUSTFS_TIER_REMOTE_CONNECT_TIMEOUT_SECS` | `10` | Maximum time to establish the remote tier TCP connection. |
| `RUSTFS_TIER_REMOTE_REQUEST_TIMEOUT_SECS` | `86400` | Maximum time for a remote tier request to reach response headers. The long default preserves large transition-upload headroom. |
| `RUSTFS_TIER_REMOTE_RESPONSE_BODY_IDLE_TIMEOUT_SECS` | `60` | Maximum time without a non-empty response-body chunk. Empty HTTP/2 frames do not count as progress. |

All three values must be positive integers. Zero fails tier client initialization instead of silently disabling the boundary. An invalid integer is logged and falls back to the default; very large values are accepted and provide a correspondingly long effective budget. The values are read when the tier client is built; recreate or reload the tier configuration after changing them.

## Before changing any of this

Follow the SOP in `docs/postmortems/2026-09-03-replication-checksum-default-regression.md`: inventory the target-side rules the current default satisfies, run the outbound target matrix, and document any new knob here in the same PR.
