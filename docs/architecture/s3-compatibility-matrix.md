# S3 Compatibility Matrix

**Use this when:** writing or checking a user-facing S3 compatibility claim, or moving a Ceph s3tests case between lists.
**Source of truth:** the test lists under `scripts/s3-tests/` and the runner `scripts/s3-tests/run.sh`; counts are derived from those files and are not recorded here.

## Current Claim

RustFS provides broad S3 API compatibility for supported features. It does not claim complete coverage of every standard or vendor-specific S3 behavior. The root README uses the same wording: supported S3-compatible clients and features are covered by the compatibility matrix and test lists.

## Test List Sources

| List | Purpose | Source |
|---|---|---|
| Implemented tests | Standard S3 tests expected to pass; the default local s3tests run. | `scripts/s3-tests/implemented_tests.txt` |
| Lifecycle behavior tests | Days-based expiration cases gated by the `s3-lifecycle-behavior-tests` lane in `.github/workflows/ci.yml`. | `scripts/s3-tests/lifecycle_behavior_tests.txt` |
| Unimplemented tests | Standard S3 features not yet passing. | `scripts/s3-tests/unimplemented_tests.txt` |
| Excluded tests | Vendor-specific or intentionally unsupported behavior excluded from RustFS gating. | `scripts/s3-tests/excluded_tests.txt` |

Counts ignore blank lines and comments; compute them from the files. The lifecycle lane runs separately because its cases need `RUSTFS_ILM_DEBUG_DAY_SECS` and an enabled scanner, and a global debug day would also shrink the `x-amz-expiration` header asserted by `test_lifecycle_expiration_header_*`; see `IMPLEMENTED_TESTS_FILE` in `scripts/s3-tests/run.sh`.

## Supported Coverage

| Area | Status | Evidence |
|---|---|---|
| Bucket create/delete/list/head | Supported | `implemented_tests.txt` |
| Object put/get/delete/copy/head | Supported | `implemented_tests.txt` |
| CopyObject checksums (CRC32, CRC32C, CRC64NVME, SHA1, SHA256, MD5, SHA512, XXHASH3, XXHASH64, XXHASH128), including source preservation and explicit override | Supported | `crates/e2e_test/src/copy_object_checksum_test.rs` |
| ListObjects/ListObjectsV2 prefix, delimiter, marker, max-keys | Supported | `implemented_tests.txt` |
| Multipart upload create/upload/complete/abort and selected multipart copy/checksum/object-attribute behavior | Supported | `implemented_tests.txt` |
| Bucket and object tagging | Supported | `implemented_tests.txt` |
| Bucket policy put/get/delete | Supported | `implemented_tests.txt` |
| Public access block put/get/delete | Supported | `implemented_tests.txt` |
| Presigned GET and PUT URLs | Supported | `implemented_tests.txt` |
| Range and conditional reads | Supported | `implemented_tests.txt` |
| Conditional bulk deletion of current objects | Supported with the limits below | `crates/e2e_test/src/conditional_delete_objects_test.rs` |
| User metadata | Supported | `implemented_tests.txt` |
| SSE-C and selected SSE-KMS edge cases | Supported | `implemented_tests.txt` |
| Selected versioning, object-lock, checksum, CORS, raw request, and conditional write behavior | Supported | `implemented_tests.txt` |

"Supported" for the SSE row means RustFS encrypts and decrypts its own objects. MinIO SSE objects (SSE-S3, SSE-KMS, SSE-C) are not readable in default builds; see [minio-file-format-compat.md Part C](minio-file-format-compat.md#part-c--server-side-encryption-sse) for the `rio-v2` migration build.

### Client metadata expectations

`CopyObject` with `MetadataDirective=REPLACE` clears standard metadata fields that the request omits, including `Content-Type`; it does not retain the source type or infer a default. Clients requiring a MIME type on the copied object must send `Content-Type` with the replacement metadata. This contract is covered by `crates/e2e_test/src/copy_object_metadata_test.rs`. A client test that expects an implicit `application/octet-stream` does not match this behavior.

The MinIO-style `metadata=true` listing extension returns user metadata names without the HTTP `x-amz-meta-` prefix. It is not the standard S3 `ListObjectsV2` response. Clients that expect canonical HTTP header names in `UserMetadata` must normalize the names at that boundary; ordinary HEAD/GET metadata is unaffected. See `rustfs/src/app/bucket_usecase.rs` and its serialization tests.

## Conditional Bulk Deletion

`DeleteObjects` accepts an optional `ETag` for each object in the XML body. A specific ETag matches the current live object; `*` requires that a current live object exists. Specific ETags require both `s3:DeleteObject` and `s3:GetObject`, while `*` requires only delete permission. Entries without an ETag retain the unconditional delete behavior.

An ETag mismatch returns a per-object `PreconditionFailed`; a missing object or a current delete marker returns `NoSuchKey`. Explicit empty or weak ETags return `InvalidArgument`. Supplying both ETag and any explicit `VersionId`, including `null`, returns `InvalidRequest`; this combination is outside the supported conditional-delete scope. Version-enabled and version-suspended buckets are supported when the request omits `VersionId`.

The response can contain successful and failed entries with HTTP 200. `Quiet=true` hides successful entries and preserves errors. Conditions on repeated keys are evaluated independently against one locked snapshot; another successful entry for the same key can still delete that object. Physical deletion, delete-marker creation and successful side effects are counted once per distinct deletion action.

Conditional deletion requires namespace locking and every participating storage peer to support guarded metadata mutations. Unsupported peers return `NotImplemented` before mutation, and requests never fall back to unconditional deletion. Complete the upgrade of all S3 ingress and storage nodes before clients use this capability. Conditional entries return `SlowDown` during pool rebalance. A generation change during commit returns the per-object concurrent-operation error `OperationAborted`. Bucket-policy enforcement through `s3:if-match` is outside this support statement.

## Replication Support Boundary

Site replication and bucket replication are not the same compatibility claim.
Site replication requires RustFS-compatible peer admin APIs and coordinates
IAM, topology, buckets, and metadata. A generic S3-compatible service can only
be a bucket-replication data target.

For a generic S3 target, RustFS supports object PUT/HEAD/DELETE, multipart
uploads, tags, version deletes, and Object Lock mutations when the target
implements the corresponding S3 APIs and has versioning enabled. Targets that
mint their own version IDs are supported through a per-target version ledger;
pre-ledger replicas are adopted only when exact key and ETag identify one
unambiguous target version. `NoSuchVersion` for an already absent addressed
replica is treated as converged.

The following are capability boundaries, not universal S3 claims:

- `GET /BUCKET?replication-check` must pass the phases required by the intended
  workload. `VersionFidelity` may report a minting target as mismatched even
  though ledger-addressed delete and Object Lock phases succeed.
- A target that rejects standard multipart constraints, required Object Lock
  integrity headers, or the configured checksum framing is unsupported until
  its transport settings are made compatible.
- SSE-S3 and SSE-KMS are decrypted at the source and re-encrypted by the
  destination's KMS. SSE-C uses ciphertext passthrough and requires target
  evidence. Unsupported or ambiguous encryption metadata fails closed.
- ACL authorization is intentionally unsupported, and generic targets never
  receive RustFS IAM/site-control-plane state.
- RustFS does not guess between multiple target versions with the same key and
  ETag. The mutation remains failed and retryable until repair establishes an
  unambiguous mapping.

See [site replication operations](../operations/site-replication-operations.md)
for health, recovery, and upgrade rules and [replication outbound transport](../operations/replication-outbound-transport.md)
for the tested target classes and knobs.

## Not Yet Passing

Standard S3 areas that must not be described as complete:

| Area | Status | Evidence |
|---|---|---|
| Bucket access logging | Handlers exist (`get_bucket_logging`, `put_bucket_logging` in `rustfs/src/storage/ecfs.rs`); the `test_*bucket_logging*` s3tests cases are still listed as unimplemented | `unimplemented_tests.txt` |
| POST Object form upload checksum handling | Not yet passing | `unimplemented_tests.txt` |
| Bucket ownership controls | No handler | `unimplemented_tests.txt` |
| Multipart upload listing and part lookup compatibility edge cases | Not part of default gate; ListParts returns HTTP 503 `SlowDownRead` when no directory was listed and any participant is unavailable, instead of treating one missing directory as an empty upload | `excluded_tests.txt`; `collect_list_parts_results_rejects_unavailable_empty_upload_dirs` |
| IAM-account or multi-storage-class dependent cases | Not part of default gate | `unimplemented_tests.txt` |
| Tenanted bucket policy edge cases | Needs investigation | `unimplemented_tests.txt` |

## Intentional Exclusions

`excluded_tests.txt` holds tests that must not block the compatibility gate: vendor-specific or non-portable behavior, and intentionally unsupported product behavior such as ACL authorization.

## Intentional Deviations From AWS S3

Object keys are stored as file-system paths under each drive (`{drive}/{bucket}/{object}/xl.meta`), the same layout MinIO uses. The rules below exist to keep that layout unambiguous and are not compatibility gaps to close; clients that need the AWS behavior must adapt on their side.

| Behavior | RustFS | AWS S3 | Why |
|---|---|---|---|
| Object key with a `.` or `..` path segment, or an empty segment (`//`), such as `a//b/./c/../d` | `400 InvalidArgument` (`check_object_args` in `crates/ecstore/src/bucket/utils.rs`, mirroring MinIO `IsValidObjectPrefix`) | Accepted as an opaque key | A `..` segment would resolve to a parent directory and `.`/`//` segments would alias other keys on disk; encoding them would change the MinIO-compatible on-disk format. |
| Directory marker (key ending in `/`, with or without a body) in a versioned bucket | Stored as the null version: `PutObject`, `CopyObject`, `HeadObject`, and `ListObjectVersions` report version id `null`, and a later PUT of the same key overwrites in place (`put_opts` in `rustfs/src/storage/options.rs`, mirroring MinIO `putOpts`: "for directory objects skip creating new versions") | A real version id per PUT, with a version history | The marker only exists to make an empty prefix listable; keeping a history for it would leave hidden versions behind every prefix delete. Replication still copies the marker as its null version (`test_bucket_replication_replicates_directory_marker_in_versioned_bucket` in `crates/e2e_test/src/replication_extension_test.rs`). |

## Request Validation and Operational Bounds

These behaviors are client-visible and differ from AWS S3 or MinIO. They bound hostile or malformed input; a client that depends on the permissive behavior must adapt.

| Behavior | RustFS | AWS S3 / MinIO | Why |
|---|---|---|---|
| SigV2 configuration PUT (`?acl`, `?policy`, `?lifecycle`, `?versioning`, `?tagging`, `?retention`, `?legal-hold`, and the other bucket/object configuration subresources) without `Content-MD5` | `400 MissingContentMD5` (`control_body_digest_required` in `rustfs/src/server/http.rs`) unless the request declares `Content-Length: 0`, which has nothing to replay; a body without that header is refused even when it turns out empty | AWS requires `Content-MD5` for most of these APIs; MinIO verifies it only when present | SigV2 signs headers, not the body, so an unsigned configuration body could be replayed with other content. SigV4 keeps the optional-MD5 contract because its payload hash binds the body. |
| `DeleteObjects` without `Content-MD5` | `400 MissingContentMD5`; under SigV4 an `x-amz-checksum-*` header substitutes for the digest, under SigV2 it does not | Same on AWS and MinIO (MinIO answers `MissingContentMD5`) | Parity; a supplied digest must match. |
| Object key containing a C0 control character other than tab, LF, or CR (`PutObject`, `POST`, `CopyObject` destination, `CreateMultipartUpload`, `CompleteMultipartUpload`) | `400 InvalidArgument` | Accepted as an opaque key | Such keys are unprintable in listings and logs and cannot round-trip through XML 1.0 responses. Existing stored keys still load; inbound replication and on-demand migration of such a key from another server are refused the same way. |
| Lifecycle rule `<ID>` containing a control character (including a tab or line break inside the element) | `400 InvalidArgument` on `PutBucketLifecycleConfiguration` | MinIO validates only the length | The ID is echoed in the `x-amz-expiration` response header; a line break there would inject a header. Stored configurations still load. |
| Bucket tag key or value outside the AWS tag character class (`[\p{L}\p{N}\p{Z}+\-=._:/@]`) | `400 InvalidTag` on `PutBucketTagging` | AWS enforces the same class; MinIO is more permissive | AWS parity. Legacy stored tags still load. |
| `CopyObject` with the default `x-amz-tagging-directive: COPY` from a source that carries tags | `403 AccessDenied` unless the caller also holds `s3:GetObjectTagging` (or `s3:GetObjectVersionTagging`) on the source; the destination's `s3:RequestObjectTag/*` conditions are evaluated against the copied tag set | AWS and MinIO copy the tags with `s3:GetObject` alone | A copy would otherwise disclose source tags to a principal that may not read them, and source tags could satisfy destination conditions the caller cannot assert (rustfs/backlog#3034). Use `REPLACE` with an explicit `x-amz-tagging` to copy without the tagging grant. |
| `ListObjectVersions` / `ListObjectsV2` with the include-deleted extension | Requires `s3:ListBucketVersions` | Vendor extension | Delete markers and non-current versions are version-listing data. |
| `DeleteObject` of a missing key that names a prefix | `204 NoContent` when the recovery probe of the prefix fits the request budget (`DELETE_RECOVERY_MAX_DISCOVERY_ENTRIES`, 4096 entries per drive, one entry per 64 plain children); `503 SlowDown` beyond that, deterministically, and for the duration of a rolling upgrade while a drive in the set still runs a release without the bounded recovery RPC | `204 NoContent` | The probe looks for interrupted delete transactions under the prefix and is bounded so a hostile prefix cannot pin the request. Prefixes with more than 262,144 direct children per drive, or roughly 1,400 to 4,096 UUID-named children, exceed the bound (rustfs/backlog#3038 residual). |
| `PutObject` / `UploadPart` body delivering less than 64 KiB per 60 seconds of server wait | `400 RequestTimeout` (the minimum-progress floor, logged as `upload_body_progress_stalled`); independent of `RUSTFS_HTTP_REQUEST_BODY_READ_TIMEOUT`, including when that timeout is 0 | No minimum upload rate | A trickle upload otherwise pins upload admission and its buffers indefinitely; roughly 1.1 KiB/s is below any practical client. |
| First write after `SetBucketQuota`, or after the quota changes, on a bucket without an authoritative usage ledger | `503 SlowDown` (retryable) until a background scan of the bucket publishes the exact ledger; a failed scan retries after 30 seconds | AWS has no bucket quota; MinIO admits with unknown usage | Quota admission uses only the authoritative ledger (scanner reporting is informational, see `reporting_retirement` tests), and the request must not hold its bucket metadata guard across a whole-bucket listing. The window is one bucket listing long. |

## Update Rule

When a feature starts passing, move its test entries from `unimplemented_tests.txt` to `implemented_tests.txt` and update the row here in the same PR. Do not change README wording beyond the supported coverage. Handler-level status (missing, stubbed, or diverging endpoints) is tracked in [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md).
