# Bucket metadata diagnostics and recovery

`GET /rustfs/admin/v3/export-bucket-metadata` exports every configuration it can read. A configuration that is stored but unreadable is never exported and never replaced by a fabricated default; instead the bucket gains an entry `<bucket>/rustfs-unreadable-configs.json` of the shape `{"bucket": …, "unreadable": [{"config": …, "error": …}]}` naming each configuration that could not be read and why, and the export continues, so one bucket's undecodable payload cannot cost an operator the whole-cluster backup (rustfs/backlog#2309). The marker name is outside the configuration namespace importers dispatch on, so importing the archive back leaves the affected bucket's stored bytes untouched. A failure of the server's own serialization or archive writing still fails the export. The optional `bucket` query selects one bucket; omitting it selects all buckets.

To collect a shareable support artifact that identifies the failures without carrying parser detail, use the same authenticated endpoint with `?diagnostic=true`. This requires the existing `ExportBucketMetadataAction` permission. A successful response has:

- Filename `bucket-meta-diagnostic.zip` and header `x-rustfs-bucket-metadata-export: diagnostic`.
- Readable entries under `_diagnostic/<bucket>/<config>`; target credentials remain redacted.
- `_diagnostic-manifest.json`, containing `version: 1`, `mode: "diagnostic"`, `complete`, and an `errors` array. Each error identifies `bucket`, `config`, and the fixed code `configuration_unavailable`. The archive excludes unreadable payloads and parser error details, and therefore carries no `rustfs-unreadable-configs.json` marker: the manifest reports the same failures with less detail, which is what makes a diagnostic archive safe to hand out.

`complete` reports whether all supported configuration reads succeeded. A diagnostic archive is never a restorable backup, including when `complete` is true. Import rejects the manifest or reserved directory before any bucket creation or configuration write. The reserved directory is not a valid bucket name, so older importers cannot restore diagnostic entries as ordinary bucket configurations.

## Recover unreadable replication targets

RustFS currently accepts the documented `{"targets": [...]}` object format. It cannot decrypt MinIO KMS-encrypted target metadata. Unreadable target payloads remain failures instead of being interpreted as an empty target set; diagnostic export and replacement import do not add MinIO KMS decryption support.

1. Inspect the diagnostic manifest, or the `rustfs-unreadable-configs.json` marker in an ordinary export, to identify affected buckets. Preserve a separate backup of the original source configuration and any credentials needed for recovery.
2. Prepare a ZIP containing `<bucket>/bucket-targets.json` with a valid RustFS replacement, whose top-level shape is `{"targets": [...]}`. Supply the intended target settings and credentials; exported credentials are redacted. Use `{"targets": []}` only when intentionally clearing all targets, and reconcile any replication rules that reference removed targets.
3. Submit the ZIP to the existing authenticated `PUT /rustfs/admin/v3/import-bucket-metadata` endpoint with `ImportBucketMetadataAction` permission. Import validates the replacement and persists it against the bucket incarnation; it does not need to parse the old target payload successfully.
4. Verify target listing and the intended replication configuration. Retry the ordinary metadata export to confirm the bucket no longer carries an unreadable marker.

Alternatively, `PUT /rustfs/admin/v3/set-remote-target?replace-unreadable=true` discards an undecodable target set as part of setting a replacement target. The flag is the operator's explicit acknowledgement that the stored set is being thrown away; without it the request is refused rather than rewriting an unreadable set from a partial view.

Do not submit the diagnostic archive itself to the import endpoint. Copy only reviewed replacement entries into an ordinary import archive.
