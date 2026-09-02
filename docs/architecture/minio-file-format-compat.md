# MinIO On-Disk Format Interoperability

**Use this when:** deciding whether a MinIO drive set, bucket-metadata blob, or SSE object can be read or imported by a given RustFS build, or before touching any constant or codec listed under Version Anchors.
**Source of truth:** `crates/filemeta/src/filemeta.rs`, `crates/filemeta/src/filemeta/codec.rs`, `crates/ecstore/src/bucket/metadata.rs`, `crates/ecstore/src/bucket/migration.rs`, `rustfs/src/storage/sse.rs`, `rustfs/Cargo.toml` `[features]`, `.github/workflows/ci.yml`, `.github/workflows/minio-interop.yml`.

This is an interop contract, not a plan. Migration is one-way (MinIO to RustFS). Erasure-coding internals are owned by [erasure-coding.md](erasure-coding.md); this document owns the interop claim, the fixture evidence, and the out-of-scope list.

## Scope Matrix By Build Variant

Build variants are the `rustfs` crate features in `rustfs/Cargo.toml`: `default`, `full`, and `rio-v2` (which enables `rustfs-ecstore/rio-v2` and pulls in `crates/rio-v2`). `rio-v2` is absent from both `default` and `full`.

| MinIO artifact | `default` / `full` build | `rio-v2` build | Notes |
|---|:--:|:--:|---|
| Unencrypted `xl.meta` (meta_ver 1-3, inline, multipart, versioned, delete marker) | Read | Read | Part A. Normalized to meta_ver 3 on rewrite. |
| Transitioned (tiered) `xl.meta` | Not fixture-proven | Not fixture-proven | Out of scope; see erasure-coding.md for the tolerant `transitioned-versionID` read rule. |
| `.metadata.bin` bucket config | Read and imported | Read and imported | Part B. Importer reads a `.minio.sys` layout end to end. |
| IAM config under `config/iam/` | Imported | Imported | `try_migrate_iam_config`; legacy field aliases normalized. |
| SSE-S3 / SSE-KMS objects, MinIO builtin static KMS | Fail closed, diagnosed | Read | Part C. Requires the shared master key. |
| SSE-C objects | Fail closed, diagnosed | Read | Part C. Customer key supplied per request. |
| Any SSE object, MinIO backed by KES / KMS plugin / MinKMS | Fail closed | Fail closed | Not planned; the DEK is sealed by the KES service. |
| RustFS-written drive set read by a live MinIO binary | Unsupported | Unsupported | Set-level divergence: MinIO looks for `.minio.sys`, RustFS writes `.rustfs.sys`. |
| RustFS-written SSE objects read by MinIO | Unsupported | Unsupported | Part C, reverse direction. |

## Version Anchors

These constants are compatibility anchors. Bumping any of them requires a read-compat path for the prior value and a migration story, exactly as the meta_ver 2 to 3 read path provides. Values live in code; do not copy them elsewhere.

| Anchor | Symbol | File | Rule |
|---|---|---|---|
| `xl.meta` magic | `XL_FILE_HEADER` | `crates/filemeta/src/filemeta.rs` | Must equal MinIO's XL2 magic. |
| Container major / minor | `XL_FILE_VERSION_MAJOR`, `XL_FILE_VERSION_MINOR` | `crates/filemeta/src/filemeta.rs` | `check_xl2_v1` (`crates/filemeta/src/filemeta/codec.rs`) rejects `major > XL_FILE_VERSION_MAJOR`. |
| Header version | `XL_HEADER_VERSION` | `crates/filemeta/src/filemeta.rs` | `decode_xl_headers` rejects `header_ver > XL_HEADER_VERSION`. |
| Metadata version | `XL_META_VERSION` | `crates/filemeta/src/filemeta.rs` | Written by `FileMeta::new`; `decode_xl_headers` rejects `meta_ver > XL_META_VERSION` (accept-older, reject-newer). |
| Bucket metadata header | `BUCKET_METADATA_FORMAT`, `BUCKET_METADATA_VERSION` | `crates/ecstore/src/bucket/metadata.rs` | Checked by `check_header`; both match MinIO's `bucketMetadataFormat` / `bucketMetadataVersion`. |
| Erasure algorithm string | `ERASURE_ALGORITHM` | `crates/ecstore/src/object_api/mod.rs` | `rs-vandermonde`; enum `ErasureAlgo` in `crates/filemeta/src/fileinfo.rs`. |
| Meta bucket names | `RUSTFS_META_BUCKET`, `MIGRATING_META_BUCKET`, `BUCKET_META_PREFIX` | `crates/ecstore/src/disk/mod.rs` | `.rustfs.sys` is the live meta bucket; `.minio.sys` is the importer source. |

## Part A — `xl.meta` Object Format

| Aspect | Contract | Where |
|---|---|---|
| Version probe | `read_format_versions` returns `(major, minor, header_ver, meta_ver)` without a full parse | `crates/filemeta/src/filemeta/codec.rs` |
| Read compatibility | Accepts meta_ver 1-3 including legacy meta_ver 2 with legacy checksums (`uses_legacy_checksum`); `load_or_convert` normalizes on rewrite | `crates/filemeta/src/filemeta.rs`, `crates/ecstore/src/set_disk/read.rs` |
| Container layout | 8-byte header, bin-length-prefixed msgpack header block, CRC trailer, optional inline data (MinIO XL2 v1 shape) | `crates/filemeta/src/filemeta/codec.rs` |
| Erasure coding | Reed-Solomon Vandermonde, `rs-vandermonde` identifier, codec crate `rustfs-erasure-codec` | `Cargo.toml`, [erasure-coding.md](erasure-coding.md) |
| Bitrot | `HighwayHash256S` default; `HighwayHash256SLegacy` (fixed key) for older shards | `crates/ecstore/src/io_support/bitrot.rs`, `crates/ecstore/tests/legacy_bitrot_read_test.rs` |
| Inline data | Inline block after the CRC trailer; `null` / version-id keying via `data_key_for_version`; `physical_data_dir` accounting | `crates/filemeta/src/filemeta.rs`, `crates/filemeta/src/filemeta/inline_data.rs` |

MinIO stores an inlined object body as `[HighwayHash256 (32 B)][body]`. Feeding the raw inline shard through RustFS's `BitrotReader` with `HighwayHash256S` verifies the checksum and yields the exact payload; the bitrot prefix is not a format incompatibility.

## Part B — Bucket Metadata (`.metadata.bin`)

| Aspect | Contract | Where |
|---|---|---|
| Path | `buckets/{bucket}/.metadata.bin` under the meta bucket (`BUCKET_METADATA_FILE`, `save_file_path`) | `crates/ecstore/src/bucket/metadata.rs` |
| Header | 4 bytes: `format: u16 LE` + `version: u16 LE`, stripped before `unmarshal` | `check_header` in `crates/ecstore/src/bucket/metadata.rs` |
| Body | MessagePack-encoded `BucketMetadata`; field names map one-to-one onto MinIO's `bucketMetadata` (PascalCase on the wire) | `BucketMetadata` in `crates/ecstore/src/bucket/metadata.rs` |
| Per-config encoding | XML for S3-XML configs, JSON for policy / quota / targets / ACL; the per-config filename constants (`policy.json`, `lifecycle.xml`, ...) are `update_config` field-selector keys, not separate files | `update_config`, `parse_all_configs` in `crates/ecstore/src/bucket/metadata.rs` |
| RustFS-only fields | `bucket_targets_config_meta_json`, `table_bucket_config_json`; a MinIO reader ignores unknown msgpack fields | `crates/ecstore/src/bucket/metadata.rs` |
| Partial interop | `bucket_targets` meta side-channel is RustFS-specific; `bucket_acl` round-trips as a blob but only canned ACLs are enforced (see [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md)) | |

### Importer

`crates/ecstore/src/bucket/migration.rs` is a one-way, idempotent importer from a `MIGRATING_META_BUCKET` (`.minio.sys`) layout into `.rustfs.sys`, run at startup from `rustfs/src/startup_bucket_metadata.rs`:

| Function | Imports |
|---|---|
| `try_migrate_bucket_metadata` | `buckets/{bucket}/.metadata.bin` plus the replication resync blob (`normalize_bucket_meta_blob` via `ReplicationMigrationBridge`) |
| `try_migrate_iam_config` | `config/iam/` records; `normalize_iam_config_blob` rewrites legacy timestamp and policy-mapping aliases |

## Part C — Server-Side Encryption (SSE)

The `xl.meta` around a MinIO SSE object parses in every build, so such objects list, HEAD, and report plausible sizes; only payload readability depends on the build. KMS wire protocols (AWS `awsJson1_1` client in `crates/kms/src/backends/aws.rs`, MinIO KES) are non-targets.

| Object class | `default` / `full` | `rio-v2` | Requirement |
|---|:--:|:--:|---|
| SSE-S3 / SSE-KMS, MinIO builtin static KMS, single- and multipart | Fail closed, diagnosed | Read | `RUSTFS_SSE_S3_MASTER_KEY` (base64, 32 bytes) equal to the source MinIO's static secret. |
| SSE-C, MinIO-written | Fail closed, diagnosed | Read | Client supplies the customer key per request; MinIO stores no key MD5, so the AEAD unseal is the key proof. |
| Any SSE, MinIO backed by KES / KMS plugin / MinKMS | Fail closed | Fail closed | Not planned. Re-encrypt or decrypt on the MinIO side first. |
| Bucket default-encryption *configuration* | Round-trips | Round-trips | A config blob; it does not make existing ciphertext readable. |

### Seams

The cryptography (DARE v2 stream format, object-key derivation, sealing) was never the gap. Three metadata seams above it rejected MinIO objects; all are closed in `rio-v2` builds. Symbols are in `rustfs/src/storage/sse.rs` unless noted.

| Seam | Resolution |
|---|---|
| Managed-SSE detection required the persisted public `x-amz-server-side-encryption` key, which MinIO synthesizes at response time | `infer_minio_managed_sse_type` infers the scheme from which MinIO sealed-key slot is present; the slot also selects the sealing-key domain, so a wrong inference cannot derive a wrong key. |
| MinIO's wrapped-DEK ciphertext was accepted by no envelope parser | `decrypt_minio_kms_data_key` implements MinIO's builtin-KMS sealing for both the raw `sealed‖iv‖nonce` layout and the legacy `{"aead": ...}` JSON. Routing is by byte shape: `LocalSseDekEnvelope` (`deny_unknown_fields`) is recognized positively, everything else goes to the MinIO decoder. |
| SSE-C detection keyed on the stored customer-algorithm header, which MinIO also never persists, and demanded a stored key MD5 | `stored_ssec_metadata` accepts MinIO's SSE-C sealed-key slot (`rio-v2` only); `verify_ssec_key_match` tolerates a missing stored MD5 for exactly that shape. |
| Multipart classification used an ETag-length heuristic (MinIO stores encrypted ETags) | Trusts MinIO's own `X-Minio-Internal-Encrypted-Multipart` marker (`crates/utils/src/http/header_compat.rs`). |

### How default builds fail

`is_object_encryption_marker` (`crates/utils/src/http/header_compat.rs`) matches the whole `x-minio-internal-server-side-encryption-` prefix, so `ObjectInfo::is_encrypted()` is true and the read plan refuses to construct a reader without decryption material. The refusal is a typed error that names the MinIO-compatible sealed format and the `rio-v2` read path it requires, surfaced as S3 `InvalidObjectState` (non-retryable). Ciphertext is never served as plaintext.

### Reverse direction

Under `rio-v2` RustFS writes its own DEK envelope into MinIO's sealed-key metadata slots labelled with MinIO's seal algorithm, so the metadata is MinIO-shaped while the key bytes are not MinIO-openable. Default builds do not populate those slots. Treat RustFS-written SSE objects as readable only by RustFS. Known unverified edge: MinIO seals ETags on SSE objects; RustFS does not unseal them, so ETag display and `If-Match` on migrated SSE objects are not guaranteed to match MinIO.

### Migration options for encrypted objects

1. Static-KMS source: run the migration through a `rio-v2` build with the shared master key, serving in place or copying into a default-build cluster (the copy re-encrypts under RustFS's own KMS).
2. KES / MinKMS source, or no special-purpose build wanted: decrypt on the MinIO side (rewrite as plaintext, or copy out through MinIO's S3 endpoint) and let RustFS encrypt on ingest.
3. Leave encrypted objects on MinIO and migrate only unencrypted data.

Inventory the source first: bucket default encryption means objects can be encrypted without the uploader asking, so "we never set SSE headers" is not evidence that a bucket has no encrypted objects.

## rio-v2 variant lifecycle

`rio-v2` is a dormant, special-purpose migration variant tracked under `rustfs/backlog#1835`.

| Fact | Value |
|---|---|
| Shipping status | Ships in no default build: absent from `default` and `full` in `rustfs/Cargo.toml`; released binaries and container images never include it. Enable with `--features rio-v2`. |
| Pull-request coverage | `test-and-lint-rio-v2` in `.github/workflows/ci.yml`: clippy plus `cargo nextest` for `rustfs` and `rustfs-ecstore` with `--features rio-v2`. This is the cfg-seam guard; it keeps the feature compiling and its unit suite green on every pull request. |
| Full-suite lane | `build-rustfs-debug-binary-rio-v2` and `e2e-tests-rio-v2` in `ci.yml` run only on the weekly schedule and manual dispatch (`cache-warm.yml` keeps the `ci-feat-rio` cache warm so the scheduled build fits its timeout). |
| Interop evidence | `.github/workflows/minio-interop.yml` (nightly plus manual) regenerates real MinIO backend trees via `crates/rio-v2/tests/minio_fixture_lab/` and runs the `#[ignore]` reader tests in `rustfs/src/storage/minio_generated_read_test.rs` with `--features rio-v2`. Its freshness is tracked in `.github/scheduled-validations.json`. |
| Promote-or-delete condition | The variant stays dormant until one of two things happens. **Promote**: a release commits to shipping MinIO SSE migration as a supported capability; then `rio-v2` joins `default`/`full`, the scheduled lanes run on every pull request, and this section is rewritten. **Delete**: no release commits to it and the scheduled lanes are not kept green; then the feature flag, `crates/rio-v2`, the cfg seams in `rustfs/src/storage/sse.rs`, the three `ci.yml` jobs, the `cache-warm.yml` warm step, `minio-interop.yml`, and its `scheduled-validations.json` entry are removed in one change. Either outcome must update `ARCHITECTURE.md` and the `ci.yml` job comments that cite this section. |

## Fixture Evidence

Fixtures were captured from a real MinIO single-drive instance and live under `crates/filemeta/tests/fixtures/minio/` and `crates/ecstore/tests/fixtures/minio/`. These tests run in the normal `cargo test` / nextest lanes.

| Test | File | Proves |
|---|---|---|
| `parses_real_minio_object_xlmeta` | `crates/filemeta/src/filemeta.rs` | Inline, two-version plus delete-marker, and multipart `xl.meta` parse to the expected `FileInfo`. |
| `parses_real_minio_bucket_metadata_blob_without_loss` | `crates/ecstore/src/bucket/metadata.rs` | The msgpack blob decodes via MinIO's field names and `parse_all_configs` loads every config in the corpus, including MinIO's lifecycle `<ExpiryUpdatedAt>` and replication `DeleteMarkerReplication` / `ExistingObjectReplication` extensions. |
| `reads_minio_inline_bucket_metadata_via_bitrot` | `crates/ecstore/src/bucket/metadata.rs` | The inline shard's HighwayHash prefix verifies under `HighwayHash256S` and yields the exact `.metadata.bin` blob. |
| `migrates_real_minio_bucket_metadata_end_to_end` | `crates/ecstore/src/bucket/migration.rs` | A real `.metadata.bin` seeded under a `.minio.sys` layout is imported by `try_migrate_bucket_metadata` into `.rustfs.sys` byte-identical, through the object layer on a 4-drive `ECStore`. |
| `test_issue_2265_legacy_meta_v2_object_compatibility`, `test_issue_2288_legacy_xlmeta_compatibility` | `crates/filemeta/src/filemeta.rs` | Legacy meta_ver 2 objects with legacy checksums still read. |
| `minio_generated_read_test.rs` (`#[ignore]`, `rio-v2`) | `rustfs/src/storage/minio_generated_read_test.rs` | Byte-identical plaintext reconstruction of MinIO SSE-S3 / SSE-KMS fixtures; driven by `minio-interop.yml`. |

Not fixture-proven: transitioned `xl.meta`; CORS, public-access-block, and bucket-ACL configs (the SNSD corpus did not exercise them); bucket-targets credentials (MinIO stores them KMS-encrypted); the SSE-C fixture-lab lane (customer-key handout is not wired; SSE-C coverage is unit-level).

## Out Of Scope

- A live MinIO binary serving a RustFS-written drive set (set-level `.minio.sys` vs `.rustfs.sys` divergence). A bidirectional round-trip would require RustFS to optionally write the `.minio.sys` set layout, which is a separate feature.
- RustFS-written SSE objects readable by MinIO.
- KES / MinKMS / KMS-plugin-sealed MinIO objects.
- Objects sealed with pre-DARE-v2-HMAC MinIO seal algorithms; `parse_minio_managed_sealed_key` rejects unknown algorithms and the read fails closed.
- AWS KMS and KES wire-protocol compatibility.

## Guardrails

- Any change to `crates/filemeta` or `crates/ecstore/src/bucket` metadata encoding is a storage-format change and follows the migration and readiness contracts in [README.md](README.md) and the ecstore layout boundary rules.
- Do not bump a Version Anchor without a read path for the prior value; see [erasure-coding.md](erasure-coding.md) for the accept-older, reject-newer rule.
- `.github/workflows/ci.yml`, `.github/workflows/cache-warm.yml`, and `ARCHITECTURE.md` cite the [rio-v2 variant lifecycle](#rio-v2-variant-lifecycle) heading; keep it when editing this file.
