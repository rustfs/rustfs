# MinIO File-Format Interoperability — Gap Analysis & Phased Plan

Assesses how closely the RustFS on-disk format matches MinIO's, so that a
MinIO drive set can be read (and eventually served) by RustFS and vice versa.
This is a **plan and analysis document**. It changes no storage code. Every
claim below cites the code that backs it.

Scope: the two on-disk artifacts that matter for interop are the per-object
`xl.meta` (object metadata + inline data) and the per-bucket `.metadata.bin`
(bucket configuration blob). IAM/config layout is noted where it affects
bucket-metadata migration.

Refs rustfs/backlog#580.

## Executive Summary

- **`xl.meta`**: RustFS writes `XL_META_VERSION = 3` and reads meta_ver ≤ 3,
  including legacy meta_ver 2 objects with legacy checksums. Magic `XL2 `,
  erasure algorithm `rs-vandermonde` (Reed-Solomon), and HighwayHash256 bitrot
  all match MinIO. `xl.meta` interop is the **strong** part of the story.
- **`.metadata.bin`**: RustFS uses the same filename, the same 4-byte
  `format|version` header, the same MessagePack blob layout, and the same
  per-config field encodings (XML/JSON) as MinIO's `bucketMetadata`. The
  divergence is a small set of RustFS-only fields (table-bucket support,
  bucket-targets meta) — not a format mismatch.
- **Migration**: RustFS already ships a one-way importer that reads a legacy
  meta bucket and rewrites bucket-metadata + IAM config into the RustFS meta
  bucket (`crates/ecstore/src/bucket/migration.rs`).
- **Server-side encryption**: not covered by the above. Objects MinIO wrote with SSE-S3, SSE-KMS, or SSE-C are **not readable by RustFS** in any shipped build. See [Part C](#part-c--server-side-encryption-sse) before planning a migration that includes encrypted objects.

For unencrypted objects the remaining work is verification breadth and closing
per-config parsing gaps, not a format rewrite. Encrypted objects are a separate,
unsolved axis (rustfs/backlog#1638).

---

## Part A — `xl.meta` Object Format

### Version support

| Aspect | Value | Evidence |
|---|---|---|
| Write version (`meta_ver`) | 3 | `crates/filemeta/src/filemeta.rs:54` (`XL_META_VERSION = 3`), written in `FileMeta::new` at `crates/filemeta/src/filemeta.rs:121` |
| Read versions accepted | ≤ 3 (1, 2, 3) | Decode rejects only `meta_ver > XL_META_VERSION` — see `crates/filemeta/src/filemeta/codec.rs` (`decode_xl_headers`); `load_or_convert` doc at `crates/filemeta/src/filemeta.rs:864` |
| Legacy meta_ver 2 read | Supported (with legacy checksum) | Regression fixtures `test_issue_2265_legacy_meta_v2_object_compatibility` / `test_issue_2288_legacy_xlmeta_compatibility` at `crates/filemeta/src/filemeta.rs:1130`, `:1152`; `uses_legacy_checksum` asserted at `:1174` |

RustFS is a **read-forward-compatible** consumer of MinIO's `xl.meta`: it can
parse older MinIO objects and normalizes them to meta_ver 3 on rewrite. It does
not write MinIO's older versions.

### Container header

| Field | RustFS value | Evidence |
|---|---|---|
| Magic | `XL2 ` (`[b'X', b'L', b'2', b' ']`) | `crates/filemeta/src/filemeta.rs:46` |
| File version major / minor | 1 / 3 | `crates/filemeta/src/filemeta.rs:51-52` |
| Header version | 3 | `crates/filemeta/src/filemeta.rs:53` |
| Magic + version check (decode entry) | `check_xl2_v1` validates magic and rejects `major > 1` | `crates/filemeta/src/filemeta/codec.rs:45-61` |
| Version-only probe (no full parse) | `read_format_versions` returns `(major, minor, header_ver, meta_ver)` | `crates/filemeta/src/filemeta/codec.rs:30-43` |

The layout after the 8-byte header is `bin-length-prefixed msgpack header block`
followed by a CRC trailer and optional inline data — matching MinIO's XL2 v1
container.

### Erasure coding

| Aspect | Value | Evidence |
|---|---|---|
| Algorithm enum | `ErasureAlgo::ReedSolomon = 1` | `crates/filemeta/src/fileinfo.rs:83-106` |
| Algorithm string | `rs-vandermonde` | `crates/filemeta/src/fileinfo.rs:31` (`ERASURE_ALGORITHM`); also `crates/ecstore/src/object_api/mod.rs:52` |
| Codec crate | `rustfs-erasure-codec` (Reed-Solomon, SIMD) | `Cargo.toml:277` |

Same Reed-Solomon Vandermonde scheme and identifier string as MinIO.

### Bitrot / shard integrity

| Aspect | Value | Evidence |
|---|---|---|
| Default hash | `HashAlgorithm::HighwayHash256S` | Bitrot read/write paths in `crates/ecstore/src/io_support/bitrot.rs` (e.g. `:564`, `:767`) |
| Legacy variant | `HighwayHash256SLegacy` (fixed key) for old objects | referenced from `rustfs_utils::HashAlgorithm` (imported at `crates/ecstore/src/io_support/bitrot.rs:26`) |
| HighwayHash crate | `highway` 1.3.0 | `Cargo.toml:252` |
| Legacy bitrot read coverage | dedicated test | `crates/ecstore/tests/legacy_bitrot_read_test.rs` |

MinIO uses HighwayHash256 for bitrot; RustFS's default `HighwayHash256S` is
compatible, with a legacy-key variant retained for older shards.

### Inline data

Small objects are inlined into the `xl.meta` container after the CRC trailer
rather than written as a separate `part.1`. Handling lives in
`crates/filemeta/src/filemeta/inline_data.rs` (e.g. `physical_data_dir` and the
shared-data-dir accounting), and the inline block is appended/consumed by the
codec in `crates/filemeta/src/filemeta/codec.rs`. This mirrors MinIO's inline
data feature and the `null`/version-id keying used for the inline map
(`data_key_for_version` at `crates/filemeta/src/filemeta.rs:69`, legacy key at
`:77`).

### `xl.meta` interop verdict

| Item | Done | Partial | Todo |
|---|:--:|:--:|:--:|
| Read MinIO meta_ver ≤ 3 | ✅ | | |
| Legacy meta_ver 2 + legacy checksum read | ✅ | | |
| XL2 container magic/version parity | ✅ | | |
| Reed-Solomon `rs-vandermonde` parity | ✅ | | |
| HighwayHash256 bitrot parity | ✅ | | |
| Inline data parity | ✅ | | |
| Broad fixture corpus from real MinIO writers | | ⚠️ | |
| Write-back parity for round-trip (RustFS→MinIO read) | | ⚠️ | |

The two ⚠️ items are verification breadth, not known incompatibilities: the
current fixtures are targeted regressions (issues #2265, #2288), and there is no
CI job proving a MinIO binary can re-read a RustFS-written `xl.meta`.

---

## Part B — Bucket Metadata (`.metadata.bin`)

### On-disk layout

| Aspect | RustFS value | Evidence |
|---|---|---|
| Meta bucket | `.rustfs.sys` | `crates/ecstore/src/disk/mod.rs:29` (`RUSTFS_META_BUCKET`) |
| Bucket-config prefix | `buckets` | `crates/ecstore/src/disk/mod.rs:34` (`BUCKET_META_PREFIX`) |
| Blob file | `.metadata.bin` | `crates/ecstore/src/bucket/metadata.rs:227` (`BUCKET_METADATA_FILE`) |
| Full path | `buckets/{bucket}/.metadata.bin` | `crates/ecstore/src/bucket/metadata.rs:415-416` (`save_file_path`) |
| Header | `format: u16 LE` + `version: u16 LE`, both `= 1` | `crates/ecstore/src/bucket/metadata.rs:228-229`, checked in `check_header` at `:595-614` |
| Body | MessagePack-encoded `BucketMetadata` | `marshal_msg`/`unmarshal` at `crates/ecstore/src/bucket/metadata.rs:582-593`; read strips the 4-byte header (`unmarshal(&data[4..])` at `:1079`) |

This is the same design as MinIO's bucket metadata: a single
`.minio.sys/buckets/<bucket>/.metadata.bin` blob with a 4-byte
`bucketMetadataFormat|bucketMetadataVersion` header and a msgpack body. The
filename, header shape, and format/version values (`1`/`1`) all match. The
`BucketMetadata` field names correspond one-to-one to MinIO's `bucketMetadata`
struct (`policyConfigJSON`, `lifecycleConfigXML`, `objectLockConfigXML`, …).

> Correction to a common misconception: modern MinIO does **not** store each
> bucket config as a separate loose `versioning.json` / `lifecycle.json` file —
> it embeds them in the same `.metadata.bin` blob, with XML for the S3-XML
> configs and JSON for policy/quota/targets. The per-config filename constants
> in RustFS (`policy.json`, `lifecycle.xml`, …) are the **keys used by
> `update_config`** to select a field, not separate on-disk files.

### Interop matrix (backlog#580 items)

Field/constant references are in `crates/ecstore/src/bucket/metadata.rs`.
"Encoding" is the payload RustFS stores in that field and must match MinIO's for
byte-level interop. Getter functions live in
`crates/ecstore/src/bucket/metadata_sys.rs`.

| Config item | RustFS field / constant | Encoding | MinIO field | Status |
|---|---|---|---|---|
| versioning | `versioning_config_xml` / `BUCKET_VERSIONING_CONFIG` = `versioning.xml` | XML | versioningConfigXML | Done |
| quota | `quota_config_json` / `BUCKET_QUOTA_CONFIG_FILE` = `quota.json` | JSON | quotaConfigJSON | Done |
| object_lock | `object_lock_config_xml` / `OBJECT_LOCK_CONFIG` = `object-lock.xml` | XML | objectLockConfigXML | Done |
| replication | `replication_config_xml` / `BUCKET_REPLICATION_CONFIG` = `replication.xml` | XML | replicationConfigXML | Done |
| policy | `policy_config_json` / `BUCKET_POLICY_CONFIG` = `policy.json` | JSON | policyConfigJSON | Done |
| lifecycle | `lifecycle_config_xml` / `BUCKET_LIFECYCLE_CONFIG` = `lifecycle.xml` | XML | lifecycleConfigXML | Done |
| tagging | `tagging_config_xml` / `BUCKET_TAGGING_CONFIG` = `tagging.xml` | XML | taggingConfigXML | Done |
| bucket_targets | `bucket_targets_config_json` + `bucket_targets_config_meta_json` / `BUCKET_TARGETS_FILE` = `bucket-targets.json` | JSON | bucketTargetsConfigJSON (+ meta variant) | Partial |
| notification | `notification_config_xml` / `BUCKET_NOTIFICATION_CONFIG` = `notification.xml` | XML | notificationConfigXML | Done |
| encryption | `encryption_config_xml` / `BUCKET_SSECONFIG` = `bucket-encryption.xml` | XML | encryptionConfigXML | Done |
| cors | `cors_config_xml` / `BUCKET_CORS_CONFIG` = `cors.xml` | XML | corsConfigXML | Done |
| public_access | `public_access_block_config_xml` / `BUCKET_PUBLIC_ACCESS_BLOCK_CONFIG` = `public-access-block.xml` | XML | publicAccessBlockConfigXML | Done |
| bucket_acl | `bucket_acl_config_json` / `BUCKET_ACL_CONFIG` = `bucket-acl.json` | JSON | bucketACLConfigJSON | Partial |

Field definitions: `crates/ecstore/src/bucket/metadata.rs:274-336`. Constants:
`:227-247`. `update_config` field routing: `:678-761`. `parse_all_configs` is
invoked on load (`load_bucket_metadata_parse` at `:1043`).

Notes on the two "Partial" rows:

- **bucket_targets** — RustFS carries an extra `bucket_targets_config_meta_json`
  field (`:288`) beyond MinIO's single targets blob. The primary
  `bucket-targets.json` payload is interoperable; the meta side-channel is
  RustFS-specific and a MinIO reader would ignore it. ACL enforcement itself is
  bounded (S3 `PutBucketAcl`/`PutObjectAcl` accept canned ACLs only — see
  [minio-rustfs-router-compatibility.md](minio-rustfs-router-compatibility.md)).
- **bucket_acl** — stored and round-tripped in the blob, but ACL grant
  semantics are intentionally limited at the S3 layer.

RustFS also defines fields with no interop requirement from backlog#580 but
worth noting so a migration tool does not choke on them: `logging_config_xml`,
`website_config_xml`, `accelerate_config_xml`, `request_payment_config_xml`
(`:242-245`), and the RustFS-only `table_bucket_config_json`
(`BUCKET_TABLE_CONFIG` = `table-bucket.json`, `:248`). A MinIO reader that does
not know `table_bucket_config_json` will ignore the unknown msgpack field.

### Old-RustFS → new-RustFS migration

RustFS ships a one-way importer that reads a legacy meta bucket
(`MIGRATING_META_BUCKET`) and rewrites both bucket metadata and IAM config into
the current RustFS meta bucket, skipping entries that already exist
(idempotent). See `crates/ecstore/src/bucket/migration.rs`:

- `try_migrate_bucket_metadata` copies `buckets/{bucket}/.metadata.bin` and the
  replication resync blob for each bucket (`crates/ecstore/src/bucket/migration.rs:193`).
- `try_migrate_iam_config` walks `config/iam/` and normalizes legacy IAM
  records — legacy timestamp fields (`update_at` → `updatedAt`) and legacy
  policy-mapping field aliases (`policies` → `policy`) are rewritten
  (`normalize_iam_config_blob` at `:97`; regression test at `:428`).
- Bucket resync metadata is re-encoded through `ReplicationMigrationBridge`
  (`normalize_bucket_meta_blob` at `:178`).

This importer is the practical basis for a MinIO → RustFS bucket-metadata
migration: because the blob layout and field encodings already match, the
missing piece is a source adapter that points the importer at a MinIO
`.minio.sys` layout rather than the RustFS legacy layout.

### Bucket-metadata interop verdict

| Item | Done | Partial | Todo |
|---|:--:|:--:|:--:|
| `.metadata.bin` filename + header + msgpack layout parity | ✅ | | |
| Per-config field encodings (XML/JSON) match MinIO | ✅ | | |
| versioning/quota/object_lock/replication/policy/lifecycle/tagging/notification/encryption/cors/public_access round-trip | ✅ | | |
| bucket_targets primary blob | ✅ | | |
| bucket_targets meta side-channel + ACL grant semantics | | ⚠️ | |
| Old-RustFS → new-RustFS importer | ✅ | | |
| MinIO `.minio.sys` source adapter for the importer | | | ❌ |
| CI proof a MinIO-written `.metadata.bin` loads unchanged | | | ❌ |

---

## Part C — Server-Side Encryption (SSE)

Container-format parity does **not** extend to encrypted object payloads. RustFS currently does not support reading objects that MinIO wrote with server-side encryption — SSE-S3, SSE-KMS, or SSE-C. This is true of every released binary and container image. Tracked in rustfs/backlog#1638.

### Scope boundary: KMS wire protocols and the production gate

This document covers MinIO on-disk metadata and object-encryption seams only. The **AWS KMS wire protocol** and the **MinIO KES wire protocol** are explicit non-targets: RustFS's AWS backend uses the AWS SDK's `awsJson1_1` client path (`crates/kms/src/backends/aws.rs:830`), while KES compatibility is outside this interop work. Those ecosystem evaluations remain separate work in the [#1562 Production Ready exit gate](https://github.com/rustfs/backlog/issues/1562), whose compatibility criterion covers MinIO/RustFS SSE data and rolling upgrades. Closing #1638 does not by itself close that gate.

Note the asymmetry with Parts A and B: the `xl.meta` around a MinIO SSE object parses fine, so such objects list, HEAD, and report plausible sizes. Only the payload is unreadable.

### What can and cannot be migrated

| Object class | Readable after moving the drives / copying via S3 | Notes |
|---|:--:|---|
| Unencrypted objects | ✅ | Parts A and B apply. |
| Bucket metadata, IAM config | ✅ | Via the importer, once a `.minio.sys` source adapter exists (see Part B). |
| Bucket-level default-encryption *configuration* | ✅ | The `encryption` config blob round-trips as a blob; it does not make existing ciphertext readable. |
| MinIO-written SSE-S3 objects | ❌ | Seams 1 and 2 below. |
| MinIO-written SSE-KMS objects | ❌ | Seams 1 and 2 below. |
| MinIO-written SSE-C objects | ❌ | Seam 3 below. |
| RustFS-written SSE objects read back by MinIO | ❌ | See "Reverse direction". |

### Where the read path stops

The primitives match — RustFS implements the same DARE V2 stream format and the same object-key derivation and sealing, and a MinIO sealed-key parser exists (`parse_minio_managed_sealed_key`, `rustfs/src/storage/sse.rs:3195`). Three seams above the cryptography still reject MinIO-written objects.

| # | Seam | Evidence |
|---|---|---|
| 1 | The managed-SSE (SSE-S3 / SSE-KMS) read path returns "not encrypted" unless the object's *persisted* metadata carries the S3 response key `x-amz-server-side-encryption`. RustFS writes that key into metadata on PUT; MinIO's internal sealed-key headers alone do not satisfy the gate. | Gate: `rustfs/src/storage/sse.rs:2432`. RustFS write side: `rustfs/src/storage/sse.rs:391-400`. |
| 2 | MinIO's wrapped-DEK blob (`{"aead": ...}`) is neither produced nor accepted. `is_data_key_envelope` classifies that shape as not a RustFS envelope, and `LocalSseDekEnvelope` is `deny_unknown_fields`. | `crates/kms/src/encryption/dek.rs:425`, `:443`; `rustfs/src/storage/sse.rs:2784-2790`. Already documented for the static backend at `crates/kms/src/config.rs:304-308`. |
| 3 | SSE-C detection keys on `x-amz-server-side-encryption-customer-algorithm`, and `contains_managed_encryption_metadata` omits MinIO's SSE-C sealed-key header, so a MinIO SSE-C object matches neither detection branch. The unsealing code it would need is already written. | Detection: `rustfs/src/storage/sse.rs:2059` and `:3178-3184`; the omitted constant is `rustfs/src/storage/sse.rs:124`. Unsealing: `rustfs/src/storage/sse.rs:2236-2245`. |

### How it fails

The read fails closed: ciphertext is never served as plaintext. Seams 1 and 3 return `Ok(None)`, but that value does not reach the data path. `is_object_encryption_marker` matches the whole `x-minio-internal-server-side-encryption-` prefix (`crates/utils/src/http/header_compat.rs:50-67`), so `ObjectInfo::is_encrypted()` is true for these objects, and `crates/ecstore/src/object_api/readers.rs:559-568` turns the `Ok(None)` into `encrypted object metadata is incomplete` while constructing the reader. GET, CopyObject, replication and multipart sources all build the reader through that path. The inline fast path and the body cache both exclude encrypted objects explicitly, so neither bypasses it.

What migrates badly is the *diagnosis*, not the data. That error is not recognised by `map_get_object_reader_error` (`rustfs/src/storage/sse.rs:667`), so it surfaces as a 500 `InternalError` — which reads as a RustFS fault rather than "this object was encrypted by another implementation". List and HEAD still succeed, because `xl.meta` itself parses normally, so the object looks healthy until something reads it.

Seam 2 surfaces its own error, but only for objects that got past seam 1.

### The `rio-v2` feature does not change this

`rustfs/src/storage/sse.rs` contains MinIO-interop code behind `#[cfg(feature = "rio-v2")]`, which can give the impression that enabling the feature closes the gap. It does not, for two independent reasons.

- The feature is not compiled into anything that ships. `rio-v2` is absent from both `default` and `full` in `rustfs/Cargo.toml:39`, `:48`, `:51`; release binaries are built with no `--features` flag, and the published images install that binary rather than compiling their own.
- Seam 1 is not feature-gated and runs *before* the MinIO parser is consulted (`rustfs/src/storage/sse.rs:2432` precedes `:2458`). Even with `rio-v2` enabled, a MinIO-written managed-SSE object returns at the gate and never reaches `parse_minio_managed_sealed_key`.

The interop harness reflects this. The reader tests are `#[ignore]` (`rustfs/src/storage/minio_generated_read_test.rs:244`, `:250`), the workflow that would run them is disabled at the GitHub Actions level and states in its own header that end-to-end MinIO-to-RustFS SSE interop is not implemented (`.github/workflows/minio-interop.yml:24-29`, `:34-39`), and the fixture suite's scope note says the tests "do not yet validate full plaintext reconstruction from MinIO-written encrypted data" (`crates/rio-v2/tests/README.md:55`).

### `rio-v2` variant lifecycle

The variant is deliberately **dormant** until rustfs/backlog#1638 is
adjudicated. Dormant means:

- **Per-pull-request CI keeps one guard job.** Only `test-and-lint-rio-v2` in
  `.github/workflows/ci.yml` runs per pull request; its job is to keep the
  `#[cfg(feature = "rio-v2")]` seam compiling and its unit tests green so the
  variant does not bit-rot. The full-suite lanes —
  `build-rustfs-debug-binary-rio-v2` and `e2e-tests-rio-v2` — run only on the
  weekly `schedule` and on `workflow_dispatch`, not per pull request, per main
  push, or
  in the merge queue.
- **Post-1.0 the variant is promoted or deleted.** The #1638 adjudication
  converges on one implementation: either `rio-v2` becomes a shipped
  configuration, or the losing side is removed together with its feature seam
  and its gating CI jobs. Tracked as `rio-v2-dormant-variant` in
  [compat-cleanup-register.md](compat-cleanup-register.md).
- **DARE/S2 fixes land in both crates.** While both implementations exist,
  any fix to the DARE V2 stream format or the S2 compression framing/index
  must be applied to `crates/rio` **and** `crates/rio-v2` (each has its own
  `encrypt_reader.rs` and `compress_reader.rs`). Both implement the same
  stream primitives; a single-sided fix forks on-disk behavior between
  default and `rio-v2` builds and invalidates the dormant variant as an
  interop baseline — and with full-suite CI now weekly-only, the divergence
  could go unnoticed for up to a week.

### Reverse direction

Migrating back is also unsupported. Under `rio-v2` RustFS writes its own DEK envelope into MinIO's sealed-key metadata slots and labels it with MinIO's seal algorithm (`rustfs/src/storage/sse.rs:1830-1852`), so the metadata is MinIO-shaped while the key bytes are not MinIO-openable. Default builds do not populate those slots at all (`rustfs/src/storage/sse.rs:1796-1798`). Treat RustFS-written SSE objects as readable only by RustFS.

### Working around the limitation

Until rustfs/backlog#1638 lands, the options are:

- Decrypt on the MinIO side first: rewrite the affected objects as plaintext (or copy them out through MinIO's S3 endpoint, which decrypts on read) and migrate the plaintext, applying RustFS-side encryption afterwards.
- Copy through the S3 API rather than moving drives: a client that reads from MinIO and writes to RustFS gets plaintext from the source and lets RustFS encrypt with its own KMS. This re-encrypts rather than preserving ciphertext, and costs a full data transfer.
- Leave encrypted objects on MinIO and migrate only unencrypted data.

Inventory the source first — bucket default-encryption settings mean objects can be encrypted without the uploader having asked for it, so "we never set SSE headers" is not sufficient evidence that a bucket has no encrypted objects.

### SSE interop verdict

| Item | Done | Partial | Todo |
|---|:--:|:--:|:--:|
| DARE V2 stream format parity | ✅ | | |
| Object-key derivation / sealing parity | ✅ | | |
| MinIO sealed-key parser exists (behind `rio-v2`) | | ⚠️ | |
| Managed-SSE detection accepts MinIO-written metadata | | | ❌ |
| MinIO `{"aead": ...}` wrapped-DEK parser | | | ❌ |
| SSE-C detection accepts MinIO-written metadata | | | ❌ |
| Read MinIO-written SSE-S3 / SSE-KMS / SSE-C objects end to end | | | ❌ |
| RustFS-written SSE objects readable by MinIO | | | ❌ |
| CI proof of SSE read parity | | | ❌ |

---

## Phased Plan

The format is already close; the plan is verification, a source adapter, and
closing the two partial encodings — not a rewrite.

### Phase 1 — Read parity, proven (verification)

- Add a MinIO-writer fixture corpus for `xl.meta` (inline + multipart +
  versioned + delete-marker + transitioned) and assert RustFS parses each to a
  `FileInfo` equivalent to MinIO's, alongside the existing issue #2265 / #2288
  fixtures in `crates/filemeta/src/filemeta.rs`.
- Add a fixture `.metadata.bin` written by MinIO and assert
  `BucketMetadata::unmarshal` + `parse_all_configs` load every field without
  loss (`crates/ecstore/src/bucket/metadata.rs`).
- Exit criterion: a CI job that fails if a real MinIO-written object or bucket
  blob cannot be read.

#### Phase 1 status — first fixtures landed (verified 2026-07-07)

A real MinIO `RELEASE.2025-07-23` single-drive instance wrote a bucket with
versioning, object-lock (GOVERNANCE default), lifecycle, tagging, quota, and a
public-download policy, plus inline / versioned / multipart objects. The on-disk
`xl.meta` blobs are captured as hex fixtures
(`crates/filemeta/tests/fixtures/minio/`, `crates/ecstore/tests/fixtures/minio/`).

Proven by regression tests:

- **Object `xl.meta` read parity** — `parses_real_minio_object_xlmeta`
  (`crates/filemeta/src/filemeta.rs`): small inline, two-object-version + delete
  marker, and multipart objects all parse to the expected `FileInfo`.
- **Bucket-metadata parse parity** — `parses_real_minio_bucket_metadata_blob_without_loss`
  (`crates/ecstore/src/bucket/metadata.rs`): the msgpack blob decodes via the
  PascalCase MinIO field names, and `parse_all_configs` loads **all ten** config
  types present in the corpus without loss — policy, lifecycle (**including
  MinIO's `<ExpiryUpdatedAt>` extension**), object-lock, versioning, tagging,
  quota, notification, encryption (SSE-S3), and replication (**including the
  `DeleteMarkerReplication` / `ExistingObjectReplication` MinIO extensions**).
- **Inline bucket-metadata read parity** — `reads_minio_inline_bucket_metadata_via_bitrot`
  (`crates/ecstore/src/bucket/metadata.rs`): MinIO stores an inlined object body
  as `[HighwayHash256 (32B)][body]`. The "`inline_data` 前缀不同" that weisd
  raised on 2026-03-06 is exactly that bitrot prefix — **not** a format
  incompatibility. Feeding the raw inline shard through RustFS's `BitrotReader`
  with the default `HighwayHash256S` verifies the checksum (confirming RustFS's
  hash matches MinIO's) and yields the exact `.metadata.bin` blob, which then
  parses. So the object-layer inline read is compatible; the earlier "extract
  `fi.data` directly" concern was reading the shard before the bitrot layer
  strips its prefix.
- **End-to-end migration** — `migrates_real_minio_bucket_metadata_end_to_end`
  (`crates/ecstore/src/bucket/migration.rs`): on a throwaway 4-drive local
  `ECStore`, a real MinIO `.metadata.bin` seeded under a `.minio.sys` layout is
  migrated by `try_migrate_bucket_metadata` into `.rustfs.sys`, and the migrated
  blob carries every config (policy / lifecycle / object-lock / versioning /
  tagging / quota / notification / encryption / replication) byte-identical to
  the source. This exercises the Phase 2 source adapter
  (`MIGRATING_META_BUCKET = ".minio.sys"`) end-to-end through the object layer —
  proven, not just present.

Still to broaden: transitioned `xl.meta`; CORS, public-access-block, and bucket
ACL configs (the SNSD test binary/`mc` did not expose these); and bucket-targets
credentials, which MinIO stores KMS-encrypted (a documented partial). These run
as ordinary crate tests, so they already execute in the normal `cargo
test`/nextest CI jobs.

### Phase 2 — MinIO source adapter for migration

- Generalize the importer in `crates/ecstore/src/bucket/migration.rs` so the
  source can be a MinIO `.minio.sys/buckets/<bucket>/.metadata.bin` layout, not
  only the RustFS legacy meta bucket. Because the blob format matches, this is
  mostly source-path plumbing plus IAM record normalization reuse.
- Exit criterion: importing a MinIO backup reproduces all backlog#580
  bucket-config items with byte-identical config payloads.

### Phase 3 — Close the two partial encodings

- bucket_targets: document/normalize the RustFS-only
  `bucket_targets_config_meta_json` so a round-trip through MinIO and back does
  not silently drop it; or fold its content into a MinIO-compatible
  representation.
- bucket_acl: decide whether ACL grant semantics beyond canned ACLs are in
  scope; if not, keep the blob round-trippable but document the enforcement
  limit (already reflected in the router compatibility matrix).

### Phase 4 — Round-trip / write-back parity (non-goal for migration)

Proving a MinIO binary can re-read a *RustFS-written drive set* (the reverse
direction) is **out of scope for the migration use case**, which is one-way
MinIO → RustFS:

- RustFS's meta bucket is `.rustfs.sys` (`crates/ecstore/src/disk/mod.rs:29`);
  MinIO looks for `.minio.sys`. A MinIO binary pointed at a RustFS drive set
  does not find `format.json` or bucket configs and refuses the set — this is a
  set-level divergence, not an object-format one.
- The object-level `xl.meta` format *does* match (proven above), so the reverse
  direction is limited by drive-set discovery, not by per-object encoding.
- The supported flow is one-way: `try_migrate_bucket_metadata` /
  `try_migrate_iam_config` / `format.json` migration import a MinIO layout into
  RustFS. There is no requirement to keep a live MinIO able to serve
  RustFS-written drives.

If a true bidirectional round-trip is ever needed, it would require RustFS to
optionally write the `.minio.sys` set layout — a separate feature, not part of
the interop/migration story tracked here.

---

## Guardrails

- This document is analysis only. Any change to `crates/filemeta` or
  `crates/ecstore/src/bucket` metadata encoding is a storage-format change and
  must follow the migration and readiness contracts in
  [README.md](README.md) and the ecstore layout boundary rules.
- The version constants (`XL_META_VERSION`,
  `BUCKET_METADATA_FORMAT`/`BUCKET_METADATA_VERSION`) are compatibility anchors.
  Bumping any of them requires a read-compat path for the prior value and a
  migration story, exactly as the current meta_ver 2 → 3 read path provides.
