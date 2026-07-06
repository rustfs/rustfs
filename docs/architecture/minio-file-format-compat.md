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

The remaining work is verification breadth and closing per-config parsing gaps,
not a format rewrite.

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

### Phase 4 — Round-trip / write-back parity (stretch)

- Prove a MinIO binary can re-read a RustFS-written `xl.meta` and
  `.metadata.bin` (the reverse direction). This is a stretch goal because
  RustFS writes meta_ver 3 and MinIO's acceptance of specific v3 features must
  be validated per feature.
- Exit criterion: a MinIO instance mounted on a RustFS-written drive set serves
  objects and bucket configs without repair.

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
