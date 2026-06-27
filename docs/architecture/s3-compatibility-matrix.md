# S3 Compatibility Matrix

This matrix records the user-facing S3 compatibility claim for RustFS and ties
it to the executable Ceph s3tests lists under `scripts/s3-tests/`.

## Current Claim

RustFS provides broad S3 API compatibility for supported features. It does not
claim complete coverage of every standard or vendor-specific S3 behavior.

The root README should use the same wording: supported S3-compatible clients and
features are covered by the compatibility matrix and test lists.

## Test List Sources

| List | Purpose | Current count | Source |
|---|---:|---:|---|
| Implemented tests | Standard S3 tests expected to pass and used by the default local s3tests run. | 449 | `scripts/s3-tests/implemented_tests.txt` |
| Unimplemented tests | Standard S3 features planned but not yet implemented. | 17 | `scripts/s3-tests/unimplemented_tests.txt` |
| Excluded tests | Vendor-specific or intentionally unsupported behavior excluded from RustFS compatibility gating. | 281 | `scripts/s3-tests/excluded_tests.txt` |

Counts ignore blank lines and comments.

## Supported Coverage

The implemented test list currently covers the common object-storage surface:

| Area | Status | Evidence |
|---|---|---|
| Bucket create/delete/list/head | Supported | `implemented_tests.txt` |
| Object put/get/delete/copy/head | Supported | `implemented_tests.txt` |
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

## Planned Standard Coverage

These are standard S3 areas that remain planned work and must not be described
as already complete:

| Area | Status | Evidence |
|---|---|---|
| Bucket access logging | Planned | `unimplemented_tests.txt` |
| POST Object form upload checksum handling | Planned | `unimplemented_tests.txt` |
| Bucket ownership controls | Planned | `unimplemented_tests.txt` |
| Multipart upload listing and part lookup compatibility edge cases | Not part of default gate | `excluded_tests.txt` |
| IAM-account or multi-storage-class dependent cases | Not part of default gate | `unimplemented_tests.txt` |
| Tenanted bucket policy edge cases | Needs investigation | `unimplemented_tests.txt` |

## Intentional Exclusions

`excluded_tests.txt` contains tests that should not block the RustFS
compatibility gate. They fall into two classes:

- vendor-specific or non-portable behavior not required for RustFS S3
  compatibility;
- intentionally unsupported product behavior, such as ACL authorization.

## Update Rule

When a planned S3 feature is implemented, move its passing test entries from
`unimplemented_tests.txt` to `implemented_tests.txt`, update this matrix, and
avoid changing README wording beyond the supported coverage.
