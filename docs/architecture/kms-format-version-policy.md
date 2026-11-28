# KMS Format Version and Deprecation Policy

**Use this when:** adding, evolving, or deprecating a persisted KMS/SSE record format, an object ciphertext stream frame type, or a `/rustfs/admin/v3/kms/*` request/response schema, or before bumping any KMS format version constant.
**Source of truth:** `crates/kms/src/backup/manifest.rs`, `crates/kms/src/backup/local_restore.rs`, `crates/kms/src/backup/vault_restore.rs`, `crates/kms/src/backup/drill.rs`, `crates/kms/src/backends/local.rs`, `crates/kms/src/backends/vault.rs`, `crates/kms/src/backends/vault_transit.rs`, `crates/kms/src/encryption/dek.rs`, `crates/kms/src/config.rs`, `crates/kms/src/persisted_observability.rs`, `crates/kms/src/api_types.rs`, `crates/sse/src/kms/local.rs`, `crates/utils/src/http/object_encryption_keys.rs`, `crates/rio/src/encrypt_reader.rs`, and `scripts/check_kms_format_versions.py`.

This document defines the architectural persistence and API evolution contract for RustFS KMS and SSE. Operator rolling-upgrade procedures and deployment matrices live in [kms-backend-security.md](../operations/kms-backend-security.md); bulk envelope re-wrap contracts live in [kms-bulk-rekey-contract.md](kms-bulk-rekey-contract.md); MinIO on-disk SSE interoperability lives in [minio-file-format-compat.md](minio-file-format-compat.md).

## Persisted Format Inventory and Version Commitments

Every persisted KMS and SSE format in RustFS declares three concrete commitments: whether mixed-version reads are supported, what an older build does when it encounters a newer write, and what cluster upgrade order a format change requires. Adding a numeric `format_version` field is deliberate, not uniform: formats whose on-disk bytes or interoperability contract forbid a wrapper header (`.master-key.salt` and object SSE metadata keys) stay intentionally unversioned.

| Format | Code anchor | Version / discriminator | Unknown fields & unknown versions | Mixed-version read | Old reader on newer write | Upgrade requirement on bump |
| --- | --- | --- | --- | --- | --- | --- |
| Backup manifest | `BackupManifest` (`crates/kms/src/backup/manifest.rs`) | `format_version: u32` (`BackupManifest::FORMAT_VERSION`) plus `ReservedSlot` (`key_versions`, `capability_discovery`) | `deny_unknown_fields`; `ManifestProbe` checks exact equality (`!=` returns `BackupError::UnknownVersion { found, supported }`); populated `ReservedSlot` fails closed | No (offline bundle) | Fails closed with `UnknownVersion` before full schema decode or digest check | Upgrade the restore binary before restoring a bundle written at a higher format version |
| Local restore commit marker (`.restore-commit.json`) | `RestoreCommitMarker` (`crates/kms/src/backup/local_restore.rs`), filename `LOCAL_RESTORE_COMMIT_MARKER_FILE` (`crates/kms/src/backends/local.rs`) | `format_version: u32` (`RESTORE_MARKER_FORMAT_VERSION`) | `deny_unknown_fields`; exact equality (`!=` fails closed with `BackupError::corrupted`) | No (transient cutover marker) | Fails closed on startup, roll-forward, and abort | Finish or abort any in-progress Local restore before upgrading or downgrading across a marker version bump |
| Vault restore commit marker | `VaultRestoreCommitMarker` (`crates/kms/src/backup/vault_restore.rs`) | `format_version: u32` (`VAULT_RESTORE_MARKER_FORMAT_VERSION`) | `deny_unknown_fields`; exact equality (`!=` fails closed with `BackupError::corrupted`) | No (transient cutover marker) | Fails closed on roll-forward and abort | Finish or abort any in-progress Vault restore before upgrading or downgrading across a marker version bump |
| DR drill evidence | `DrillEvidence` (`crates/kms/src/backup/drill.rs`) | `format_version: u32` (`DRILL_EVIDENCE_FORMAT_VERSION`) | `deny_unknown_fields`; archived evidence record inspected by consumers against `format_version` | No (offline evidence artifact) | Consumer rejects unknown fields or unsupported `format_version` | Upgrade evidence consumers alongside any schema bump |
| Local SSE DEK envelope | `LocalSseDekEnvelope` (`crates/sse/src/kms/local.rs`) | `version: u8` (`LOCAL_SSE_DEK_FORMAT_VERSION`) | `deny_unknown_fields`; exact equality (`!=` returns `SseError::internal`); read-only legacy colon-delimited fallback (`RUSTFS_COMPAT_TODO(sse-local-dek-json-v1)`) | Only legacy colon-delimited -> v1 JSON; newer JSON versions fail closed | Fails closed (`Unsupported encrypted DEK format version`) | Upgrade every cluster node to read the new version before enabling writes via a staged write switch |
| Local KMS master key record (`<key_id>.key`) | `StoredMasterKey`, `StoredKeyProtection` (`crates/kms/src/backends/local.rs`) | `format_version: u32` (`STORED_MASTER_KEY_FORMAT_VERSION`, `serde(default)` -> 1) and `at_rest_protection` enum probed via `has_unknown_protection_marker` | Tolerates unknown fields via hand-written `Deserialize` with sampled field-name-only `warn` and `rustfs_kms_persisted_unknown_fields_total{record_kind="local-key-record"}`; `format_version > STORED_MASTER_KEY_FORMAT_VERSION` or unknown `at_rest_protection` fails closed as `KmsError::UnsupportedFormatVersion` across `decode_stored_key`, `ensure_missing_salt_can_be_generated`, `list_keys`, `crates/kms/src/backup/local_export.rs`, and `crates/kms/src/backup/local_restore.rs` | Newer builds read `<=` versions and pre-field records (defaulting to 1); older builds reject newer versions/markers | Fails closed as `UnsupportedFormatVersion` (and blocks missing-salt regeneration); never misclassified as corruption | Local backend is single-node only; never roll back past a written `format_version` or new `at_rest_protection` variant |
| Local KMS master key salt (`.master-key.salt`) | `LOCAL_KMS_MASTER_KEY_SALT_FILE`, `LOCAL_KMS_MASTER_KEY_SALT_LEN`, `LOCAL_KMS_ARGON2_*` (`crates/kms/src/backends/local.rs`) | None (intentionally unversioned raw 16-byte salt; Argon2id m/t/p are compile-time constants) | Rejects any byte length other than 16; missing salt with unreadable or encrypted `.key` files fails closed in `ensure_missing_salt_can_be_generated` | Yes while Argon2id parameters remain unchanged | Changing KDF constants without explicit migration makes existing encrypted key files undecryptable | Do not add a header to `.master-key.salt`; KDF parameter persistence belongs to Local durability (`rustfs/backlog#1568`) |
| DEK envelope (`DataKeyEnvelope`) | `DataKeyEnvelope`, `is_data_key_envelope`, `envelope_wrap_aad` (`crates/kms/src/encryption/dek.rs`) | No top-level schema version (`master_key_version` is KEK rotation version); opt-in `context_binding: Option<u8>` (`CONTEXT_BINDING_AAD_V1`) | Tolerates unknown fields via hand-written `Deserialize` with sampled field-name-only `warn` and `rustfs_kms_persisted_unknown_fields_total{record_kind="data-key-envelope"}`; unrecognized `context_binding` fails closed in `envelope_wrap_aad` | Additive optional fields work; `context_binding: 1` and post-rotation `master_key_version` require upgraded readers | Older nodes ignore unknown fields; for `context_binding: 1`, pre-AAD nodes fail AES-GCM unwrap; for rotated keys, pre-versioning nodes unwrap against current material | Upgrade every node before rotating keys or enabling `RUSTFS_KMS_ENVELOPE_AAD=true` (`RUSTFS_COMPAT_TODO(backlog-1667-kms-envelope-aad)`); update `is_data_key_envelope` (`DataKeyEnvelopeMarker`) in the same change if any breaking v2 shape retains the seven base fields |
| Object SSE metadata key set | `crates/utils/src/http/object_encryption_keys.rs`, `crates/sse/src/core/keying/direct.rs`, `crates/sse/src/core/keying/object.rs` | None (intentionally unversioned to preserve MinIO metadata compatibility); encrypted ETag listing uses `SUFFIX_OPAQUE_ENCRYPTED_ETAG` (`"v1"`) | Missing `INTERNAL_ENCRYPTION_ALGORITHM_HEADER` defaults to `"AES256"`; unknown MinIO seal algorithms return `Ok(None)` in `parse_minio_managed_sealed_key`; additive keys only | Yes for additive keys when old slots are preserved or neutralized on rewrite | Older nodes ignore unknown metadata keys and may take stale MinIO branches unless superseded slots are neutralized | Deploy readers across every cluster node and replication/tier target before writers emit new metadata keys |
| Vault KV2 key record | `VaultKeyData` (`crates/kms/src/backends/vault.rs`) | None (`version: u32` is KEK rotation version; `baseline_version: Option<u32>` uses `serde(default)`) | Tolerates unknown fields via hand-written `Deserialize` with sampled field-name-only `warn` and `rustfs_kms_persisted_unknown_fields_total{record_kind="vault-kv2-key"}` | Bidirectional reads work | Older nodes read safely, but **drop unknown fields (`baseline_version`, `wrap_budget_reserved`) on write-back** and write blind without CAS | Freeze KMS admin writes and upgrade every cluster node before first key rotation or before relying on new fields |
| Vault Transit key metadata | `TransitKeyMetadataPersisted`, `TransitKeyMetadata::synthesized` (`crates/kms/src/backends/vault_transit.rs`) | None (`current_version: u32` is Vault Transit key version); missing metadata synthesizes `KeyState::Enabled` | Tolerates unknown fields via hand-written `Deserialize` with sampled field-name-only `warn` and `rustfs_kms_persisted_unknown_fields_total{record_kind="vault-transit-key-metadata"}` | Bidirectional reads work | Older nodes drop unknown fields on write-back and retain unbounded metadata caches | Freeze KMS admin writes and upgrade every cluster node before relying on new metadata fields or CAS lifecycle writes |
| Persisted KMS configuration (`config/kms_config.json`) | `KmsConfig`, `kms_config_from_persisted_json` (`crates/kms/src/config.rs`), `KMS_CONFIG_PATH` (`rustfs/src/admin/handlers/kms_dynamic.rs`) | None (deliberately tolerant of unknown fields for rolling upgrades, pinned by `persisted_config_unknown_fields_remain_readable_and_are_observed`) | Loaded through `serde_ignored`, emitting sampled field-path-only `warn` and incrementing `rustfs_kms_persisted_unknown_fields_total{record_kind="kms-config"}`; never logs field values; retains pre-beta.9 Local config fallback (`RUSTFS_COMPAT_TODO(rustfs-5063)`) | Yes for additive optional fields with defaults | Older nodes ignore unknown fields and reject reload broadcasts predating KMS reload support | Confirm `cluster_config.consistent` after reconfiguration; never tighten `KmsConfig` to `deny_unknown_fields` |

## Object Ciphertext Stream Framing

Object payload encryption framing in `crates/rio/src/encrypt_reader.rs` evolves through explicit leading frame-type bytes rather than a file header so single-part and multipart segments can transition independently:

| Frame type | Constant | Value | Semantics and mixed-version rule |
| --- | --- | --- | --- |
| v1 data frame | `FRAME_TYPE_V1` | `0x00` | Default write layout; header and `0xFF` end marker sit outside AEAD. Legacy pre-`1.0.0-alpha.91` segment-nonce fallback is gated by `RUSTFS_ENCRYPTION_LEGACY_NONCE_FALLBACK` (`RUSTFS_COMPAT_TODO(backlog-2369-legacy-nonce-fallback)`). |
| v2 non-final data frame | `FRAME_TYPE_V2` | `0x01` | Fixed 8 KiB plaintext blocks with 8-byte header and frame index bound as AEAD AAD; read unconditionally, written only when `RUSTFS_ENCRYPTION_FRAME_V2=true` (off by default). |
| v2 final data frame | `FRAME_TYPE_V2_FINAL` | `0x02` | Authenticated final frame of a v2 segment, detecting trailing truncation; read unconditionally, written only when `RUSTFS_ENCRYPTION_FRAME_V2=true`. |
| Multipart segment-nonce frame | `FRAME_TYPE_SEGMENT_NONCE` | `0x03` | Authenticated 37-byte opening frame carrying a random 96-bit per-upload segment nonce for v1 or v2 multipart parts; read unconditionally, written only when `RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE=true` (`RUSTFS_COMPAT_TODO(backlog-2703-multipart-segment-nonce)`). Older nodes and ciphertext-passthrough targets without `0x03` read support reject it. |
| Segment end marker | `FRAME_TYPE_END` | `0xFF` | Delimits the end of an encrypted stream or multipart part segment. |

## Version Anchors

`scripts/check_kms_format_versions.py` parses the table below directly and asserts that every listed constant matches its Rust source definition and that no numeric format-version or context-binding constant under `crates/kms/src/` or `crates/sse/src/kms/` escapes documentation. Update this table in the same change before bumping or adding any constant.

| Format | File | Constant | Value | Enforcement |
| --- | --- | --- | --- | --- |
| Backup manifest | `crates/kms/src/backup/manifest.rs` | `BackupManifest::FORMAT_VERSION` | `1` | Exact equality (`!=` -> `BackupError::UnknownVersion`) |
| Local restore commit marker | `crates/kms/src/backup/local_restore.rs` | `RESTORE_MARKER_FORMAT_VERSION` | `1` | Exact equality (`!=` -> `BackupError::corrupted`) |
| Vault restore commit marker | `crates/kms/src/backup/vault_restore.rs` | `VAULT_RESTORE_MARKER_FORMAT_VERSION` | `1` | Exact equality (`!=` -> `BackupError::corrupted`) |
| DR drill evidence | `crates/kms/src/backup/drill.rs` | `DRILL_EVIDENCE_FORMAT_VERSION` | `1` | Explicit schema tag with `deny_unknown_fields` |
| Local KMS master key record | `crates/kms/src/backends/local.rs` | `STORED_MASTER_KEY_FORMAT_VERSION` | `1` | Upper bound (`>` -> `KmsError::UnsupportedFormatVersion`, missing defaults to `1`) |
| DEK envelope context binding | `crates/kms/src/encryption/dek.rs` | `CONTEXT_BINDING_AAD_V1` | `1` | Opt-in write switch (`RUSTFS_KMS_ENVELOPE_AAD`); unrecognized values fail closed |
| Local SSE DEK envelope | `crates/sse/src/kms/local.rs` | `LOCAL_SSE_DEK_FORMAT_VERSION` | `1` | Exact equality (`!=` -> `SseError::internal`, with `deny_unknown_fields`) |
| Ciphertext stream v1 frame | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_V1` | `0x00` | Default write layout; always read |
| Ciphertext stream v2 frame | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_V2` | `0x01` | Opt-in write (`RUSTFS_ENCRYPTION_FRAME_V2`); always read |
| Ciphertext stream v2 final frame | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_V2_FINAL` | `0x02` | Opt-in write (`RUSTFS_ENCRYPTION_FRAME_V2`); always read |
| Ciphertext stream segment-nonce frame | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_SEGMENT_NONCE` | `0x03` | Opt-in write (`RUSTFS_ENCRYPTION_MULTIPART_SEGMENT_NONCE`); always read |
| Ciphertext stream end marker | `crates/rio/src/encrypt_reader.rs` | `FRAME_TYPE_END` | `0xFF` | Segment terminator; always read |

## Reference Evolution Pattern

New strictly versioned formats should copy the `BackupManifest` pattern in `crates/kms/src/backup/manifest.rs` rather than inventing ad-hoc parsers:

1. **Probe before full decode.** Decode a minimal version/marker probe first so a record written by a newer build reports `UnknownVersion` or `UnsupportedFormatVersion` instead of generic corruption, which prevents operators from starting disaster recovery during a version mismatch.
2. **Enforce deterministic decode ordering.** Follow the manifest sequence: malformed framing -> version probe -> completeness/lifecycle marker -> schema decode -> raw-bytes integrity digest -> semantic validation. Verify integrity digests over the stored bytes as read, never by re-serializing parsed structs.
3. **Reserve future slots explicitly.** Use `ReservedSlot` (hard-rejecting non-null data in the current version) when a field name is reserved for a known upcoming contract so older builds never silently accept unimplemented semantics.
4. **Choose exact equality vs `<=` intentionally.** Use exact equality for offline bundles and transient cutover markers (`BackupManifest`, restore markers). Use `<=` with `serde(default)` for live stores that need backward-compatible reads (`StoredMasterKey`). Use an opt-in, default-off write switch with unconditional reads for cluster-shared formats that cannot tolerate an uncoordinated cutover (`DataKeyEnvelope::context_binding`, `FRAME_TYPE_V2`, `FRAME_TYPE_SEGMENT_NONCE`).
5. **Make tolerant formats observable without leaking secrets.** Formats that must stay tolerant of unknown fields during rolling upgrades (`KmsConfig`, `DataKeyEnvelope`, `StoredMasterKey`, `VaultKeyData`, `TransitKeyMetadataPersisted`) must never use silent `serde` ignores: they record bounded field names only (never field values) via `UnknownFieldSummary` in `crates/kms/src/persisted_observability.rs`, emitting power-of-two sampled `warn` logs and incrementing `rustfs_kms_persisted_unknown_fields_total{record_kind=...}`.

## Format Version Bump Checklist

Complete every step below before merging a change that bumps any KMS/SSE format version or adds a new persisted discriminator:

1. Update this policy document first — including the inventory table, `## Version Anchors`, mixed-version read rules, downgrade behavior, and the one-time upgrade path if existing records must be rewritten.
2. Keep existing persisted records readable during upgrades: any new field added to an existing tolerant record (`StoredMasterKey`, `VaultKeyData`, `TransitKeyMetadataPersisted`, `DataKeyEnvelope`, `KmsConfig`) must carry `serde(default)` so pre-existing records do not fail to deserialize.
3. Audit every format probe and classifier (`ManifestProbe`, `stored_master_key_format_version`, `has_unknown_protection_marker`, `is_data_key_envelope`, `is_local_sse_dek`, `parse_minio_managed_sealed_key`) so a future version is never misclassified as v1 or collapsed into generic corruption.
4. For cluster-shared formats, ship read support first with the write path behind a default-off switch, register that switch in [compat-cleanup-register.md](compat-cleanup-register.md) with a matching `RUSTFS_COMPAT_TODO(<id>)` source marker and explicit removal condition, and flip the default only in a later release after full fleet convergence.
5. Add negative regression tests proving an unknown version or unknown discriminator fails closed with the expected error classification, plus downgrade/legacy read tests proving existing records remain readable.
6. Run `python3 scripts/check_kms_format_versions.py` and `python3 scripts/test_check_kms_format_versions.py` (alongside `./scripts/check_architecture_migration_rules.sh`) to verify documentation-to-code parity.

## API Surface Stability and Deprecation Policy

The KMS administrative API is versioned in the URL path (`/rustfs/admin/v3/kms/*`) rather than inside JSON request/response bodies (`crates/kms/src/api_types.rs`, `rustfs/src/admin/handlers/kms*.rs`):

- **Request strictness policy.** Admin request structs reject unknown fields with `#[serde(deny_unknown_fields)]` so typos or unsupported security options fail closed with HTTP 400 rather than being silently ignored. The configure and start requests in `crates/kms/src/api_types.rs` and the handler-local request types in `rustfs/src/admin/handlers/kms_keys.rs` and `rustfs/src/admin/handlers/kms_backup.rs` already enforce this; `UpdateKeyDescriptionRequest`, `TagKeyRequest`, and `UntagKeyRequest` in `crates/kms/src/api_types.rs` remain open for strictness convergence under `rustfs/backlog#1641` once client compatibility is confirmed.
- **Response compatibility policy.** Response structs omit `deny_unknown_fields` so older clients can ignore additive fields, and their wire JSON shapes are pinned by `insta` snapshots under `crates/kms/src/snapshots/`.
- **Deprecation workflow.** Removing or renaming an accepted field follows four mandatory steps:
  1. Keep the deprecated field accepted on deserialization (never rely on `deny_unknown_fields` to silently break existing callers).
  2. Add a dedicated deserialization regression guard test verifying both presence and omission of the deprecated field, and register it in `### Active Deprecation Guards` below. The canonical example is `test_deserialize_vault_kv2_configure_request_mount_path_optional_but_accepted` in `crates/kms/src/api_types.rs` for the deprecated Vault `mount_path` field.
  3. When a temporary compatibility branch is slated for deletion, mark the source with `RUSTFS_COMPAT_TODO(<task-id>)` and record the exact removal condition in [compat-cleanup-register.md](compat-cleanup-register.md).
  4. Delete the deprecated field only in a dedicated cleanup change once its documented removal condition is satisfied.

### Active Deprecation Guards

`scripts/check_kms_format_versions.py` verifies that every test named below exists in its cited file so a deprecated-but-accepted field cannot lose its regression guard silently.

| Surface | Field | Test file | Regression guard |
| --- | --- | --- | --- |
| `ConfigureVaultKmsRequest` / `ConfigureKmsRequest` | `mount_path` | `crates/kms/src/api_types.rs` | `test_deserialize_vault_kv2_configure_request_mount_path_optional_but_accepted` |

## Why No Standalone Migration Framework Is Built

RustFS intentionally does **not** build a generic KMS format migration framework today (`rustfs/backlog#1641`):

- **Zero v1-to-v2 transitions exist yet.** Every explicitly versioned KMS format (`BackupManifest`, `RestoreCommitMarker`, `VaultRestoreCommitMarker`, `DrillEvidence`, `StoredMasterKey`, `LocalSseDekEnvelope`) is still at version 1, so a generic framework would have no real input and would freeze speculative abstractions before the first migration's real constraints are known.
- **Backup and restore already serve as the cross-version transport.** KMS backup export and restore (`crates/kms/src/backup/local_export.rs`, `crates/kms/src/backup/local_restore.rs`, `crates/kms/src/backup/vault_restore.rs`) already serialize KMS state into a self-describing, versioned, integrity-verified bundle and validate target compatibility before cutover.
- **Explicit trigger condition.** Instead of a pre-built framework, every format version bump must specify its downgrade behavior and concrete one-time upgrade path under the [Format Version Bump Checklist](#format-version-bump-checklist); a dedicated migration utility will be built alongside the first actual v1-to-v2 bump if backup/restore or online rewrite (such as `POST /rustfs/admin/v3/kms/keys/rekey`) cannot serve it directly.

## Verification

Run the drift guard and its negative-case unit tests whenever editing this document or any listed KMS/SSE/encryption format constant:

```bash
python3 scripts/check_kms_format_versions.py
python3 scripts/test_check_kms_format_versions.py
./scripts/check_architecture_migration_rules.sh
./scripts/check_doc_paths.sh
```
