# Compatibility Lens

- Internal metadata uses `metadata_compat` helpers for dual RustFS/MinIO keys,
  including mixed casing and removal of both twins.
- Binary UUID metadata treats absent, empty, and nil as no value. Unversioned
  remote tiers receive no `versionId`; versioned purge requests retain the real
  version ID.
- `xl.meta` changes preserve supported header/meta versions, recompute
  signatures, decode legacy fixtures, and remain readable by old RustFS/MinIO.
- Foreign/corrupt metadata validates parallel array lengths and missing fields;
  it returns a decode error rather than indexing, panicking, or fabricating data.
- Do not “correct” byte-for-byte MinIO ports without legacy fixture evidence.
  Bitrot framing, shard math, distribution, and inline prefixes are contracts.
- Client-visible metadata/events strip both internal prefixes
  case-insensitively.
- Proto fields are appended, never reused/renumbered; FlatBuffers tables extend
  compatibly and absent new fields fail closed where authorization/quorum is
  involved.
- Replay real client request shapes and exact pagination boundaries for S3
  handler changes.
- Bucket metadata/IAM/config parsing remains compatible with pinned real MinIO
  fixtures and encrypted migration data.
- Compatibility shims use `RUSTFS_COMPAT_TODO(<task-id>)`, have a removal
  condition, and default toward reading old data safely.

## Outbound targets

- A change to what the replication or migration client sends by default
  (checksum policy, payload framing, headers, version-id addressing) is judged
  against every target class, not the one it fixes. Name each target-side rule
  the current default satisfies — checksum required with Object Lock
  parameters, `aws-chunked` decoding, version-id adoption, ETag equals content
  MD5 — and show which cell of
  `crates/e2e_test/src/replication_target_matrix_test.rs` covers each.
- A test that asserts the fix ("no trailer header") is not evidence; the
  matrix cell that asserts the target accepted and stored the object is.
- Every new environment escape hatch appears in
  `docs/operations/replication-outbound-transport.md` in the same diff.
