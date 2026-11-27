# MinIO On-Disk Format Interoperability

**Use this when:** deciding whether a MinIO drive set, bucket-metadata blob, or SSE object can be read or imported by a given RustFS build, or before touching any constant or codec listed under Version Anchors.
**Source of truth:** `crates/filemeta/src/filemeta.rs`, `crates/filemeta/src/filemeta/codec.rs`, `crates/ecstore/src/bucket/metadata.rs`, `crates/ecstore/src/bucket/migration.rs`, `crates/sse/src/core/`, `crates/sse/src/kms/`, `rustfs/Cargo.toml` `[features]`, `.github/workflows/ci.yml`, `.github/workflows/minio-interop.yml`.

This is an interop contract, not a plan. Migration is one-way (MinIO to RustFS). Erasure-coding internals are owned by [erasure-coding.md](erasure-coding.md); this document owns the interop claim, the fixture evidence, and the out-of-scope list.

## Scope Matrix By Build Variant

Build variants are the `rustfs` crate features in `rustfs/Cargo.toml`: `default`, `full`, and `m-compatible` (which enables `rustfs-ecstore/m-compatible` and `rustfs-sse/m-compatible`, and pulls in `crates/rio-v2`). `m-compatible` is absent from both `default` and `full`.

| MinIO artifact | `default` / `full` build | `m-compatible` build | Notes |
|---|:--:|:--:|---|
| Unencrypted `xl.meta` (meta_ver 1-3, inline, multipart, versioned, delete marker) | Read | Read | Part A. Normalized to meta_ver 3 on rewrite. |
| Transitioned (tiered) `xl.meta` | Not fixture-proven | Not fixture-proven | Out of scope; see erasure-coding.md for the tolerant `transitioned-versionID` read rule. |
| `.metadata.bin` bucket config | Read and imported | Read and imported | Part B. Importer reads a `.minio.sys` layout end to end. |
| IAM config under `config/iam/`, plaintext or encrypted with the root credentials | Imported | Imported | `try_migrate_iam_config`; legacy field aliases normalized. |
| IAM config under `config/iam/`, encrypted by MinIO's builtin static KMS | Startup fails, record named | Imported | Part B. MinIO encrypts every IAM record, `format.json` included, once a KMS is configured. Requires the shared master key. |
| SSE-S3 / SSE-KMS objects, MinIO builtin static KMS | Fail closed, diagnosed | Read | Part C. Requires the shared master key. |
| SSE-C objects | Fail closed, diagnosed | Read | Part C. Customer key supplied per request. |
| Any SSE object, MinIO backed by KES / KMS plugin / MinKMS | Fail closed | Fail closed | Not planned; the DEK is sealed by the KES service. |
| RustFS-written drive set read by a live MinIO binary | Unsupported | Unsupported | Set-level divergence: MinIO looks for `.minio.sys`, RustFS writes `.rustfs.sys`. |
| Objects RustFS wrote onto a MinIO-initialized drive set, read by MinIO afterwards | Unsupported | Read, SSE objects excepted | Part D. A default build stores compression markers and user metadata under keys MinIO does not look up. |
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

MinIO stores IAM records in one of three forms, and the importer decrypts before it normalizes (`decrypt_legacy_iam_blob` in `rustfs/src/storage/storage_api.rs`):

| Stored form | Reader | Builds |
|---|---|---|
| Plaintext JSON | none needed | all |
| Encrypted with a key derived from the root credentials (no KMS configured) | `rustfs_iam::try_decrypt_iam_blob` | all |
| Encrypted under a builtin-KMS data key bound to the record's path (`internal/config/crypto.go`: a 5-byte header, JSON metadata, a `sio-go` stream) | `decrypt_minio_kms_config` in `crates/sse/src/kms/minio.rs` | `m-compatible` |

A record no reader opens fails the migration with `incompatible legacy metadata: <path>` and the server does not start; it is never imported as if it were plaintext. KES and other external KMS backends are out of scope, as for objects. The server config blob (`config/config.json`), which MinIO encrypts the same way, is imported best-effort and is skipped when it does not decrypt.

## Part C — Server-Side Encryption (SSE)

The `xl.meta` around a MinIO SSE object parses in every build, so such objects list, HEAD, and report plausible sizes; only payload readability depends on the build. KMS wire protocols (AWS `awsJson1_1` client in `crates/kms/src/backends/aws.rs`, MinIO KES) are non-targets.

| Object class | `default` / `full` | `m-compatible` | Requirement |
|---|:--:|:--:|---|
| SSE-S3 / SSE-KMS, MinIO builtin static KMS, single- and multipart | Fail closed, diagnosed | Read | `RUSTFS_SSE_S3_MASTER_KEY` (base64, 32 bytes) equal to the source MinIO's static secret. |
| SSE-C, MinIO-written | Fail closed, diagnosed | Read | Client supplies the customer key per request; MinIO stores no key MD5, so the AEAD unseal is the key proof. |
| Any SSE, MinIO backed by KES / KMS plugin / MinKMS | Fail closed | Fail closed | Not planned. Re-encrypt or decrypt on the MinIO side first. |
| Bucket default-encryption *configuration* | Round-trips | Round-trips | A config blob; it does not make existing ciphertext readable. |

### Seams

The cryptography (DARE v2 stream format, object-key derivation, sealing) was never the gap. Three metadata seams above it rejected MinIO objects; all are closed in `m-compatible` builds. Symbols are under `crates/sse/src/core/` unless noted.

| Seam | Resolution |
|---|---|
| Managed-SSE detection required the persisted public `x-amz-server-side-encryption` key, which MinIO synthesizes at response time | `infer_minio_managed_sse_type` infers the scheme from which MinIO sealed-key slot is present; the slot also selects the sealing-key domain, so a wrong inference cannot derive a wrong key. |
| MinIO's wrapped-DEK ciphertext was accepted by no envelope parser | `decrypt_minio_kms_data_key` implements MinIO's builtin-KMS sealing for both the raw `sealed‖iv‖nonce` layout and the legacy `{"aead": ...}` JSON. Routing is by byte shape: `LocalSseDekEnvelope` (`deny_unknown_fields`) is recognized positively, everything else goes to the MinIO decoder. |
| SSE-C detection keyed on the stored customer-algorithm header, which MinIO also never persists, and demanded a stored key MD5 | `stored_ssec_metadata` accepts MinIO's SSE-C sealed-key slot (`m-compatible` only); `verify_ssec_key_match` tolerates a missing stored MD5 for exactly that shape. |
| Multipart classification used an ETag-length heuristic, and MinIO seals the ETag of a single-part SSE object into 96 hex characters, so every such object was read under part keys and failed authentication | For a sealed object key, `is_multipart_encrypted_object` (`crates/ecstore/src/object_api/readers.rs`) trusts MinIO's own `X-Minio-Internal-Encrypted-Multipart` marker, as MinIO's `crypto.IsMultiPart` does. |
| MinIO records no plaintext size for a single-part SSE object, so the stored size, DARE framing included, was reported as the object's length | `ObjectInfo::encryption_original_size` (`crates/ecstore/src/object_api/types.rs`) derives it from the DARE layout when nothing records it, per part for a marked multipart upload (`sio.DecryptedSize`). |
| HEAD answered only stored public SSE headers, which MinIO never persists | `classify_unrecorded_sse_response` (`read.rs`) classifies from the sealed-key slot. An `aws:kms` key id is answered in the `arn:aws:kms:` form MinIO used, on GET as well. |
| HEAD and GET answered the sealed ETag | `sealed_etag_for_response` (`read.rs`) answers what MinIO did: the unsealed content MD5 for SSE-S3, the last 32 hex characters for SSE-KMS and SSE-C. |

### How default builds fail

`is_object_encryption_marker` (`crates/utils/src/http/header_compat.rs`) matches the whole `x-minio-internal-server-side-encryption-` prefix, so `ObjectInfo::is_encrypted()` is true and the read plan refuses to construct a reader without decryption material. The refusal is a typed error that names the MinIO-compatible sealed format and the `m-compatible` read path it requires, surfaced as S3 `InvalidObjectState` (non-retryable). Ciphertext is never served as plaintext.

### Reverse direction

Under `m-compatible` RustFS writes its own DEK envelope into MinIO's sealed-key metadata slots labelled with MinIO's seal algorithm, so the metadata is MinIO-shaped while the key bytes are not MinIO-openable. Default builds do not populate those slots. Treat RustFS-written SSE objects as readable only by RustFS: MinIO answers HEAD for them and fails GET with `kms:KeyNotFound`.

### ETag limits

Two differences from MinIO remain on MinIO-written SSE objects. Conditional request headers (`If-Match`, `If-None-Match`) are evaluated against the stored, sealed ETag, not the one the response carries. Listings omit the ETag of every encrypted object that does not record RustFS's own protected-ETag format, which includes all MinIO-written ones; that is RustFS listing policy (`etag_can_be_listed`, `crates/utils/src/http/object_encryption_keys.rs`), not a read gap.

### Migration options for encrypted objects

1. Static-KMS source: run the migration through an `m-compatible` build with the shared master key, serving in place or copying into a default-build cluster (the copy re-encrypts under RustFS's own KMS).
2. KES / MinKMS source, or no special-purpose build wanted: decrypt on the MinIO side (rewrite as plaintext, or copy out through MinIO's S3 endpoint) and let RustFS encrypt on ingest.
3. Leave encrypted objects on MinIO and migrate only unencrypted data.

Inventory the source first: bucket default encryption means objects can be encrypted without the uploader asking, so "we never set SSE headers" is not evidence that a bucket has no encrypted objects.

## Part D — Objects RustFS Writes Onto A MinIO Drive Set

An `m-compatible` build serving a MinIO-initialized drive set writes objects MinIO can read once it serves the drives again. Two stored shapes differ from a default build for that reason, and only in `m-compatible`:

| Metadata | Default build | `m-compatible` build | Why |
|---|---|---|---|
| Compression marker and plaintext size (`compression`, `actual-size`) | `x-rustfs-internal-*` and `x-minio-internal-*` | `x-rustfs-internal-*` and `X-Minio-Internal-*` | MinIO looks these two keys up with an exact-case map access (`cmd/object-api-utils.go`, `IsCompressed` / `GetActualSize`); the lowercase twin is invisible to it, so a compressed object is served as its raw S2 stream. `minio_key` in `crates/utils/src/http/metadata_compat.rs`. |
| User metadata | bare key (`purpose`); reserved names keep `x-amz-meta-` | `x-amz-meta-<key>`; `internal-*` keys stay bare | MinIO answers a stored key without the prefix as an ordinary response header named after it, never as `x-amz-meta-*`. `stored_user_metadata_key` in `rustfs/src/storage/options.rs`. An `internal-*` key cannot be namespaced: `x-amz-meta-internal-*` is a legacy internal namespace the RustFS read side hides, so such a key is readable by RustFS only. |

RustFS readers accept both of these metadata shapes in every build. That covers these two shapes only: a default build still cannot read the sealed SSE objects or the S2-compressed objects on such a drive set (Part C, "How default builds fail"), and its replication comparison does not skip the prefixed user metadata, so moving a drive set back to a default build needs those objects rewritten first. Replication convergence checks in an `m-compatible` build leave user metadata out of the source/target comparison (`comparable_metadata`, `crates/replication/src/object.rs`): the target reports it with the prefix stripped, so a prefix-keyed comparison could never match. SSE objects are the exception to MinIO readability; see Part C, reverse direction.

## m-compatible variant lifecycle

`m-compatible` is a dormant, special-purpose migration variant tracked under `rustfs/backlog#1835`.

| Fact | Value |
|---|---|
| Shipping status | Ships in no default build: absent from `default` and `full` in `rustfs/Cargo.toml`; released binaries and container images never include it. Enable with `--features m-compatible`. |
| Pull-request coverage | `test-and-lint-m-compatible` in `.github/workflows/ci.yml`: clippy plus `cargo nextest` for `rustfs`, `rustfs-ecstore`, and `rustfs-sse` with `--features m-compatible`. This is the cfg-seam guard; it keeps the feature compiling and its unit suite green on every pull request. |
| Full-suite lane | `build-rustfs-debug-binary-m-compatible` and `e2e-tests-m-compatible` in `ci.yml` run only on the nightly schedule and manual dispatch (`cache-warm.yml` keeps the `ci-feat-rio` cache warm so the scheduled build fits its timeout). |
| Interop evidence | `.github/workflows/minio-interop.yml` (nightly plus manual) regenerates real MinIO backend trees via `crates/rio-v2/tests/minio_fixture_lab/` and runs the `#[ignore]` reader tests in `rustfs/src/storage/minio_generated_read_test.rs` with `--features m-compatible`. Its freshness is tracked in `.github/scheduled-validations.json`. |
| Promote-or-delete condition | The variant stays dormant until one of two things happens. **Promote**: a release commits to shipping MinIO SSE migration as a supported capability; then `m-compatible` joins `default`/`full`, the scheduled lanes run on every pull request, and this section is rewritten. **Delete**: no release commits to it and the scheduled lanes are not kept green; then the feature flag, `crates/rio-v2`, the variant modules and their selection points listed under "Where the variant lives in code", the three `ci.yml` jobs, the `cache-warm.yml` warm step, `minio-interop.yml`, and its `scheduled-validations.json` entry are removed in one change. Either outcome must update `ARCHITECTURE.md` and the `ci.yml` job comments that cite this section. |

### Where the variant lives in code

Code that differs between the two builds is kept in pairs of modules that export the same functions, selected in one place per pair. Call sites do not name the build.

| Crate | Selection point | Pair |
|---|---|---|
| `rustfs-ecstore` | `src/io_support/rio.rs` | `io_support/rio/native.rs`, `io_support/rio/m_compatible.rs`: stream readers and writers, compression scheme and index encoding. |
| `rustfs-ecstore` | `src/object_api/readers.rs` | `object_api/readers/native.rs`, `object_api/readers/m_compatible.rs`: range plans, multipart recognition and plaintext size of a stored layout. |
| `rustfs-sse` | `src/core/keying/mod.rs` | `core/keying/direct.rs`, `core/keying/object.rs`: the key mode a build writes; `src/kms/` holds the data-key formats. Rules in `crates/sse/AGENTS.md`. |

Outside the pairs the feature is named in single functions: `writes_canonical_case_twin` (`crates/utils/src/http/metadata_compat.rs`), `stored_user_metadata_key` (`rustfs/src/storage/options.rs`), `comparable_metadata` (`crates/replication/src/object.rs`), the message of the unresolved-decryption-material error (`rustfs/src/storage/sse.rs`), and the compiled-feature tables of the server binary.

## Fixture Evidence

Fixtures were captured from a real MinIO single-drive instance and live under `crates/filemeta/tests/fixtures/minio/` and `crates/ecstore/tests/fixtures/minio/`. These tests run in the normal `cargo test` / nextest lanes.

| Test | File | Proves |
|---|---|---|
| `parses_real_minio_object_xlmeta` | `crates/filemeta/src/filemeta.rs` | Inline, two-version plus delete-marker, and multipart `xl.meta` parse to the expected `FileInfo`. |
| `parses_real_minio_bucket_metadata_blob_without_loss` | `crates/ecstore/src/bucket/metadata.rs` | The msgpack blob decodes via MinIO's field names and `parse_all_configs` loads every config in the corpus, including MinIO's lifecycle `<ExpiryUpdatedAt>` and replication `DeleteMarkerReplication` / `ExistingObjectReplication` extensions. |
| `reads_minio_inline_bucket_metadata_via_bitrot` | `crates/ecstore/src/bucket/metadata.rs` | The inline shard's HighwayHash prefix verifies under `HighwayHash256S` and yields the exact `.metadata.bin` blob. |
| `migrates_real_minio_bucket_metadata_end_to_end` | `crates/ecstore/src/bucket/migration.rs` | A real `.metadata.bin` seeded under a `.minio.sys` layout is imported by `try_migrate_bucket_metadata` into `.rustfs.sys` byte-identical, through the object layer on a 4-drive `ECStore`. |
| `test_issue_2265_legacy_meta_v2_object_compatibility`, `test_issue_2288_legacy_xlmeta_compatibility` | `crates/filemeta/src/filemeta.rs` | Legacy meta_ver 2 objects with legacy checksums still read. |
| `minio_generated_read_test.rs` (`#[ignore]`, `m-compatible`) | `rustfs/src/storage/minio_generated_read_test.rs` | Byte-identical plaintext reconstruction of MinIO SSE-S3 / SSE-KMS fixtures; driven by `minio-interop.yml`. |

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
- `.github/workflows/ci.yml`, `.github/workflows/cache-warm.yml`, and `ARCHITECTURE.md` cite the [m-compatible variant lifecycle](#m-compatible-variant-lifecycle) heading; keep it when editing this file.

## Independent integrity upgrade contract

The optional shard-integrity extension preserves the release checksum algorithm
and payload framing. It does not bump XL container/header/metadata versions.
`ObjectPartInfo` extensions use named MessagePack fields; older eight-field
readers can ignore the new field, and new readers still decode old positional
arrays. This is a decoder compatibility claim, not certification of arbitrary
mixed-version write, repair, tiering, or downgrade workflows.

| Workflow | Contract |
|---|---|
| New reader, old object | Existing reads and traditional shard/explicit-version metadata recovery remain available; independent donor-substitution protection is absent until a trusted rewrite. |
| Old reader, new object | Existing checksum/frame and metadata decoders remain usable; old readers do not enforce the independent proof. |
| New coordinator, old disk server | GET/Deep verification runs at the coordinator. A protected UploadPart requires a write quorum that publishes its index. An old RenameFile response cannot acknowledge durable index repair. |
| Old write/repair coordinator | Unsupported for protected data: it may omit, discard, or preserve stale integrity metadata. New protection is disabled by default so the coordinator rollout can precede activation. |
| Pre-upgrade in-progress multipart upload | Remains legacy when completed. Reinitiate and reupload from a trusted source to obtain protection. |
| New multipart upload completed by an old coordinator | Unsupported; a retained upload marker without a completed descriptor fails validation. |
| Write switch changed during an upload | The persisted upload mode wins: legacy uploads remain legacy and protected uploads must finish with all proofs. |
| Write switch disabled after activation | Existing protected reads, repair and rewrites still require protection. This does not make an old binary safe to reintroduce. |
| Automatic COPY, materialization or data movement | Preserve the source protection mode. Rewriting legacy bytes is not independent evidence of their original content. |
| Full downgrade after protected writes | No blanket guarantee; preserve a snapshot and validate read, write, repair, COPY, and multipart behavior before considering a downgrade. |

Upgrade all readers, writers and background coordinators before explicitly
enabling both `RUSTFS_SHARD_INTEGRITY_WRITE` and
`RUSTFS_SHARD_INTEGRITY_FLEET_CONFIRMED`. Both default to false. The confirmation
is an operator attestation, not automatic capability discovery or an old-node
fence. Validate actual mixed-version writes and restart recovery before claiming
a rolling-upgrade or pre-activation rollback guarantee. See the
[rollout runbook](../operations/shard-integrity-rollout.md).
There is no automatic migration of existing payloads and no need
to rewrite them merely to preserve reading. To protect legacy objects, compare
against a separately trusted source or end-to-end digest and rewrite/reupload
through upgraded coordinators. Recomputing a hash from existing suspect shards,
or agreeing RS parity alone, does not establish their original identity.

Normal scans, tier metadata scans and unproven legacy scans cannot produce `VerifiedHealthy` or
`Repaired` integrity receipts. A protected object or delete marker may produce
`MetadataHealthy` only when its exact version was selected from authoritative
metadata quorum; that disposition proves metadata/presence health and never
payload integrity. Legacy objects remain without a positive receipt. Legacy repair still runs and reports actual
before/after drive changes; an unknown strong result does not mean the repair
was never attempted. Authoritative historical-version absence/cleanup proofs
remain separate from live payload verification. Retain trusted backups for
legacy recovery. These commitments protect against misplaced or corrupted
shards under an authoritative metadata quorum; they are not signatures against
an attacker who can replace that quorum too.

The public MinIO source inspected at
[`7aac2a2`](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/bitrot-streaming.go)
uses adjacent streaming HighwayHash checksums. Its
[XL metadata codec](https://github.com/minio/minio/blob/7aac2a2c5b7c882e68c1ce017d8256be2feea27f/cmd/xl-storage-format-v2.go)
does not establish the independent part-generation commitment described here.
This source comparison does not claim a live MinIO donor-shard reproduction or
behavior of proprietary MinIO editions.
