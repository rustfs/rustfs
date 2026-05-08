# Protocol E2E Tests

FTPS, WebDAV, and SFTP protocol end-to-end tests for RustFS.

## Prerequisites

No external SSH tooling is required. The test framework generates ed25519
host keys in-process via russh::keys under the per-test temp directory
before each SFTP server spawn, and russh-sftp drives the protocol from the
test process directly.

## Running Tests

Run all protocol tests (FTPS + WebDAV + SFTP):
```bash
RUSTFS_BUILD_FEATURES=ftps,webdav,sftp cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

Run FTPS tests only:
```bash
RUSTFS_BUILD_FEATURES=ftps cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

Run WebDAV tests only:
```bash
RUSTFS_BUILD_FEATURES=webdav cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

Run SFTP tests only (core + compliance + read-only + idle-timeout):
```bash
RUSTFS_BUILD_FEATURES=sftp cargo test --package e2e_test test_protocol_core_suite -- --test-threads=1 --nocapture
```

`--test-threads=1` is required because every entry spawns a rustfs server on
fixed bind ports; running entries in parallel would cause port collisions.

## Test Coverage

### FTPS Tests
- mkdir bucket
- cd to bucket
- put file
- ls list objects
- cd . (stay in current directory)
- cd / (return to root)
- cd nonexistent bucket (should fail)
- delete object
- cdup
- rmdir delete bucket

### WebDAV Tests
- PROPFIND at root (list buckets)
- MKCOL (create bucket)
- PUT (upload file)
- GET (download file)
- PROPFIND on bucket (list objects)
- DELETE file
- DELETE bucket
- Authentication failure test

### SFTP Tests

The SFTP suite lives in three entries plus a standalone idle-timeout case.
Every assertion runs against a freshly spawned rustfs binary with
`RUSTFS_SFTP_ENABLE=true`; the test framework also pins
`RUSTFS_SFTP_PART_SIZE=5242880` so the multipart boundary is deterministic.

#### sftp_core (`test_sftp_core_operations`)

Bind ports 9022 (SFTP) and 9200 (S3). 22 in-suite assertions covering the
core protocol surface plus cross-protocol consistency:

- Subsystem canary: SFTPv3 version exchange completes after password auth
- Bucket lifecycle: mkdir, root listing, rmdir, post-delete listing
- Small-file round-trip with SHA256 compare
- Stat on a file (size + file type) and on a bucket (directory)
- SETSTAT on a path returns ok
- Rename within bucket, listing reflects the rename
- Multipart-sized round-trip (just over 2 × part_size) with SHA256 compare
- Negative cases: symlink rejected, open of nonexistent file rejected,
  read_dir of nonexistent bucket rejected, path traversal rejected
- Spec-letter assertions: APPEND open returns an error, CREATE+EXCLUDE on an
  existing path returns an error, bad-password authentication is rejected
- Cross-protocol via aws-sdk-s3: SFTP write then S3 read with SHA256 match,
  S3 write then SFTP read with SHA256 match
- Cross-API directory visibility: SFTP-created sub-directory visible via S3
  ListObjectsV2, S3-created `__XLDIR__` marker visible via SFTP readdir as a
  directory entry

#### sftp_compliance (`test_sftp_compliance_suite`)

Bind ports 9024 (SFTP) and 9300 (S3). 14 compliance regression cases against
one shared server spawn. Each case carries a stable CMPTST-NN identifier:

- CMPTST-01: medium-binary upload then download with SHA256 compare
  (single-shot PutObject path below the multipart boundary)
- CMPTST-02: zero-byte upload, download, and stat-size match
- CMPTST-03: rm against a bucket path is rejected; the bucket is preserved
- CMPTST-04: rmdir against a non-empty bucket is rejected; the contained
  object survives
- CMPTST-05: rmdir against a non-empty sub-directory is rejected; the inner
  object survives
- CMPTST-06: open with a path-traversal pattern cannot leak a host file via
  SFTP read
- CMPTST-07: read_dir of `/..` either errors or returns a listing that
  contains no host system entries
- CMPTST-08: rename across buckets preserves payload and removes the source
  object
- CMPTST-09: paths with embedded spaces round-trip through the russh-sftp
  client
- CMPTST-10: read_link is rejected (S3 storage has no symlinks)
- CMPTST-11: SETSTAT on a path and FSETSTAT on a separate open handle both
  return ok (rsync, WinSCP transfer-success contract)
- CMPTST-12: rename to the same path is a no-op; the file persists with the
  original payload
- CMPTST-13: implicit-directory round-trip; uploading to a nested key
  creates the parent directory implicitly and three listing forms surface
  the inner file
- CMPTST-14: OPEN, WRITE, FSETSTAT, CLOSE on the same write handle all
  return ok (WinSCP wire shape)

#### sftp_compliance_readonly (`test_sftp_compliance_readonly`)

Bind ports 9025 (SFTP) and 9301 (S3). Spawns a second rustfs binary with
`RUSTFS_SFTP_READ_ONLY=true`; the S3 endpoint stays writable so the suite
can seed a bucket and a fixture object via aws-sdk-s3 before opening the
SFTP session. 7 compliance cases:

- CMPTST-15: put through SFTP is rejected
- CMPTST-16: rm through SFTP is rejected
- CMPTST-17: mkdir through SFTP is rejected
- CMPTST-18: rmdir through SFTP is rejected
- CMPTST-19: rename through SFTP is rejected
- CMPTST-20: ls through SFTP is allowed and lists the seeded bucket
- CMPTST-21: get through SFTP is allowed and returns the seeded payload
  byte-for-byte

The full case index lives at the top of `sftp_compliance.rs`; each helper's
log lines name its CMPTST-NN code so a failure in CI points at one named
property without consulting any external doc.

#### sftp_idle_timeout (`test_sftp_idle_timeout_disconnects`)

Bind ports 9023 (SFTP) and 9100 (S3). Spawns rustfs with
`RUSTFS_SFTP_IDLE_TIMEOUT=5`, sleeps 10 s past the timeout, then issues an
SFTP request and asserts the server has closed the session.

