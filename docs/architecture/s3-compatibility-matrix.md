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
| User metadata | Supported | `implemented_tests.txt` |
| SSE-C and selected SSE-KMS edge cases | Supported | `implemented_tests.txt` |
| Selected versioning, object-lock, checksum, CORS, raw request, and conditional write behavior | Supported | `implemented_tests.txt` |

"Supported" for the SSE row means RustFS encrypts and decrypts its own objects. MinIO SSE objects (SSE-S3, SSE-KMS, SSE-C) are not readable in default builds; see [minio-file-format-compat.md Part C](minio-file-format-compat.md#part-c--server-side-encryption-sse) for the `rio-v2` migration build.

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
| Multipart upload listing and part lookup compatibility edge cases | Not part of default gate | `excluded_tests.txt` |
| IAM-account or multi-storage-class dependent cases | Not part of default gate | `unimplemented_tests.txt` |
| Tenanted bucket policy edge cases | Needs investigation | `unimplemented_tests.txt` |

## Intentional Exclusions

`excluded_tests.txt` holds tests that must not block the compatibility gate: vendor-specific or non-portable behavior, and intentionally unsupported product behavior such as ACL authorization.

## Intentional Deviations From AWS S3

Object keys are stored as file-system paths under each drive (`{drive}/{bucket}/{object}/xl.meta`), the same layout MinIO uses. The rules below exist to keep that layout unambiguous and are not compatibility gaps to close; clients that need the AWS behavior must adapt on their side.

| Behavior | RustFS | AWS S3 | Why |
|---|---|---|---|
| Object key with a `.` or `..` path segment, or an empty segment (`//`), such as `a//b/./c/../d` | `400 InvalidArgument` (`check_object_args` in `crates/ecstore/src/bucket/utils.rs`, mirroring MinIO `IsValidObjectPrefix`) | Accepted as an opaque key | A `..` segment would resolve to a parent directory and `.`/`//` segments would alias other keys on disk; encoding them would change the MinIO-compatible on-disk format. |
| Directory marker (key ending in `/`, with or without a body) in a versioned bucket | Stored as the null version: `PutObject`/`HeadObject` report version id `00000000-0000-0000-0000-000000000000`, `ListObjectVersions` reports `null`, and a later PUT of the same key overwrites in place (`put_opts` in `rustfs/src/storage/options.rs`, mirroring MinIO `putOpts`: "for directory objects skip creating new versions") | A real version id per PUT, with a version history | The marker only exists to make an empty prefix listable; keeping a history for it would leave hidden versions behind every prefix delete. Replication still copies the marker as its null version (`test_bucket_replication_replicates_directory_marker_in_versioned_bucket` in `crates/e2e_test/src/replication_extension_test.rs`). |

## Update Rule

When a feature starts passing, move its test entries from `unimplemented_tests.txt` to `implemented_tests.txt` and update the row here in the same PR. Do not change README wording beyond the supported coverage. Handler-level status (missing, stubbed, or diverging endpoints) is tracked in [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md).
