# Bucket metadata diagnostics and recovery

`GET /rustfs/admin/v3/export-bucket-metadata` exports the supported bucket configurations as a backup. If any selected configuration is unreadable or changes between validation and export, the request fails instead of returning an archive with missing settings. Serialization and archive-writing failures also fail the request. The optional `bucket` query selects one bucket; omitting it selects all buckets.

To collect a shareable support artifact that identifies the failures without carrying parser detail, use the same authenticated endpoint with `?diagnostic=true`. This requires the existing `ExportBucketMetadataAction` permission. A successful response has:

- Filename `bucket-meta-diagnostic.zip` and header `x-rustfs-bucket-metadata-export: diagnostic`.
- Readable entries under `_diagnostic/<bucket>/<config>`; target credentials remain redacted.
- `_diagnostic-manifest.json`, containing `version: 1`, `mode: "diagnostic"`, `complete`, and an `errors` array. Each error identifies `bucket`, `config`, and the fixed code `configuration_unavailable`. The archive excludes unreadable payloads and parser error details.

`complete` reports whether all supported configuration reads succeeded. A diagnostic archive is never a restorable backup, including when `complete` is true. Import rejects the manifest or reserved directory before any bucket creation or configuration write. The reserved directory is not a valid bucket name, so older importers cannot restore diagnostic entries as ordinary bucket configurations.

Earlier partial exports containing `<bucket>/rustfs-unreadable-configs.json` are also rejected before any import changes. Their omitted settings could otherwise silently disappear when restoring into an empty cluster. Preserve these archives for diagnosis, repair the source configuration, and obtain a successful ordinary export before treating it as a backup.

## Recover unreadable replication targets

RustFS currently accepts the documented `{"targets": [...]}` object format. It cannot decrypt MinIO KMS-encrypted target metadata. Unreadable target payloads remain failures instead of being interpreted as an empty target set; diagnostic export and replacement import do not add MinIO KMS decryption support.

1. Inspect the diagnostic manifest to identify affected buckets. Preserve a separate backup of the original source configuration and any credentials needed for recovery.
2. Prepare a ZIP containing `<bucket>/bucket-targets.json` with a valid RustFS replacement, whose top-level shape is `{"targets": [...]}`. Supply the intended target settings and credentials; exported credentials are redacted. Use `{"targets": []}` only when intentionally clearing all targets, and reconcile any replication rules that reference removed targets.
3. Submit the ZIP to the existing authenticated `PUT /rustfs/admin/v3/import-bucket-metadata` endpoint with `ImportBucketMetadataAction` permission. Import validates the replacement and persists it against the bucket incarnation; it does not need to parse the old target payload successfully.
4. Verify target listing and the intended replication configuration. Retry the ordinary metadata export and confirm it succeeds.

Alternatively, use `PUT /rustfs/admin/v3/set-remote-target?bucket=<bucket>&replace-unreadable=true` with a complete target-create payload, including the endpoint, target bucket, target type, and credentials. This mode cannot be combined with `update=true`. Both `update` and `replace-unreadable` accept only a single `true` or `false` value; duplicate parameters and other values are rejected before any configuration write.

RustFS validates the replacement target, then reads and updates the latest persisted target set under the bucket metadata transaction lock. The flag authorizes discarding that set only if it is still unreadable at this point. A readable set, including a repair already committed by another node, is preserved and merged using the ordinary target-create identity and conflict checks. Success is returned after persistence, and an actual discard is audited after the commit. Without this opt-in, target writes retain the unreadable-configuration refusal.

Do not submit the diagnostic archive itself to the import endpoint. Copy only reviewed replacement entries into an ordinary import archive.
