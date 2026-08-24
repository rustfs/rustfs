# Rebalance Stored-Representation Impact Guide

This guide covers the historical data-movement read defect tracked by
[`rustfs/backlog#1850`](https://github.com/rustfs/backlog/issues/1850). It is an
impact-assessment and read-only triage guide. It does not repair, rewrite,
migrate, delete, or quarantine any object.

The defect affected data movement when the source reader returned logical
plaintext but the target writer preserved the source's stored-representation
metadata and sizes. Compressed objects could therefore be copied as plaintext
under compression metadata. Server-managed encrypted objects could be copied as
plaintext under encryption metadata. The forward rebalance fix reached `main`
in commit
[`e11fcfbd`](https://github.com/rustfs/rustfs/commit/e11fcfbd087f8a8dae2c0f2c62bc0f6e40e3f10a)
through [PR #6057](https://github.com/rustfs/rustfs/pull/6057).

Upgrading prevents this defect in later rebalance runs. It does not validate or
repair copies produced by an earlier run.

## Immediate Operator Decision

Treat a deployment as exposed when both conditions are true:

1. it ran rebalance in an affected build, or decommission in the narrower
   historical window described below; and
2. the operation could have selected compressed, SSE-S3, or SSE-KMS objects.

For an exposed deployment:

- preserve old pool media, snapshots, replicas, and backups before any pool is
  removed, reformatted, reused, or returned;
- stop destructive cleanup and do not use another rebalance or decommission run
  as a repair mechanism;
- inventory and validate candidates with read-only operations;
- handle SSE-S3 and SSE-KMS candidates as a confidentiality incident as well as
  a data-integrity incident;
- restore only from a separately verified source under an incident-specific
  recovery plan.

## Affected Versions

The release boundaries below were verified by tag ancestry. Commit
[`a236b0d0`](https://github.com/rustfs/rustfs/commit/a236b0d01d40a152309446a553756ea991c9f901)
introduced the merged rebalance and decommission implementation. Commit
[`2f25cf60`](https://github.com/rustfs/rustfs/commit/2f25cf606e5ca814fe992be6327a91e31fe066b3)
introduced the raw stored-representation read mode and wired it into
decommission. Commit `e11fcfbd` wired the same mode into rebalance.

| Release or commit range | Rebalance | Decommission | Operator classification |
| --- | --- | --- | --- |
| Through `1.0.0-alpha.90`, before `a236b0d0` | Path not present | Path not present | Not affected by this data-movement path |
| `1.0.0-alpha.91` through `1.0.0-beta.8`, from `a236b0d0` up to but excluding `2f25cf60` | Decoded read | Decoded read | Both operations require assessment |
| `1.0.0-beta.9` through `1.0.0-rc.1`, from `2f25cf60` up to but excluding `e11fcfbd` | Decoded read | Raw stored-representation read | Rebalance requires assessment; decommission is not affected by this defect |
| `1.0.0-rc.2` and later, at or after `e11fcfbd` | Raw stored-representation read | Raw stored-representation read | Forward-fixed; earlier copies still require assessment |

Preview tags follow the commit they reference. In particular, the `rc.1`
preview is affected and the `rc.2` preview contains the forward fix. For custom
or untagged builds, compare the deployed commit with the three commit boundaries
rather than inferring behavior from a version string.

The historical decommission result is narrower than the rebalance result but is
not empty. Before `2f25cf60`, decommission used the same ordinary decoded reader.
From `1.0.0-beta.9` onward it explicitly used `raw_data_movement_read: true`.
Any code change or automated remediation for the earlier decommission window is
outside this report and requires a separate issue.

## Why The Copy Could Be Accepted

The migration pipeline is a stored-representation copier. It preserves the
source ETag and internal metadata, uses stored `part.size` values to divide the
stream, and carries the decoded compression index. The affected rebalance read
options supplied only the version ID and lock setting, so the normal GET read
plan decompressed or decrypted the stream first. A target write could therefore
complete while its bytes no longer matched the metadata that described them.

Historical rebalance cleanup ran only after every version in an entry was
reported moved. It then deleted the source entry. A target write accepted as a
successful move could therefore be followed by source deletion even though a
later GET of the target would fail. Conversely, a source-read failure prevented
the version from being counted as moved and prevented normal source cleanup.

## Object Classification

| Stored object class | Affected read result | Risk | Triage priority |
| --- | --- | --- | --- |
| Plain, uncompressed, unencrypted | Stored bytes and logical bytes are the same | No corruption expected from this defect alone | Low; sample to validate the scope assumption |
| Compressed | Decompressed bytes were divided using compressed part sizes while compression metadata and indexes were retained | Silent truncation or malformed compressed representation; GET can fail or return truncated data | High |
| SSE-S3 | Decrypted plaintext could be written while encryption metadata and ciphertext sizes were retained | Plaintext at rest on the target plus later decrypt failure | Critical |
| SSE-KMS | Decrypted plaintext could be written while KMS/encryption metadata and ciphertext sizes were retained | Plaintext at rest on the target plus later decrypt failure | Critical |
| SSE-C | The migration request did not have the customer key, so the normal read failed closed | Migration failure and possible incomplete progress; no successful corrupting copy is expected from this path | Medium; confirm the source was retained |
| Any compressed and encrypted combination | Multiple stored-representation assumptions were violated | Confidentiality exposure and data corruption | Critical |

The classification is specific to this defect. A low-risk classification does
not certify an object against unrelated corruption.

## Read-Only Assessment Workflow

### 1. Establish The Operation Window

Record the exact RustFS version and commit for every node that participated.
Collect the authenticated rebalance status response, decommission status when
applicable, service logs, deployment change records, and release history.

Persisted rebalance metadata records the run ID, participating pools, start and
end state, bucket lists, counters, and the last bucket/object progress value. It
does not persist a complete per-object movement ledger. Status metadata can
prove that a run occurred and narrow time, pool, and bucket scope, but it cannot
by itself enumerate every moved object.

If no reliable operation record remains, assume that every object version in a
bucket present during the affected deployment interval is a candidate until
other evidence narrows the set.

### 2. Build A Candidate Inventory

Use read-only S3 list and list-object-versions operations for the buckets in
scope. Preserve bucket, key, version ID, last-modified time, size, ETag, storage
class, and any client-side content digest. Join that list with:

- upload records that identify compression settings or SSE mode;
- KMS audit history and application catalogs;
- replication inventory and external backup manifests;
- rebalance/decommission timestamps and source/target pool records;
- server access logs showing successful or failed reads after movement.

Do not use ETag equality as proof of content integrity. The migration writer
preserved the source ETag, including for a malformed target copy, and multipart
or encrypted ETags are not general-purpose content hashes.

### 3. Classify Stored Metadata On Evidence Copies

When API and application records cannot classify a candidate, copy `xl.meta`
from each relevant shard disk to a restricted evidence location and inspect the
copy on an offline host. Do not edit or decode metadata in place on a live data
path. Keep the evidence copies under the same access controls as the object.

The existing `rustfs-filemeta` example can decode an evidence copy. It prints
metadata values, some of which are sensitive encryption material, so redact
metadata values before they reach a terminal or report:

```bash
cargo run --quiet -p rustfs-filemeta --example dump_fileinfo -- /evidence/object/xl.meta |
  sed -E 's/^(meta\[[^]]+\])=.*/\1=<redacted>/'
```

Use the output only as a screen:

- either the `x-rustfs-internal-compression` or
  `x-minio-internal-compression` key marks a compressed representation;
- `actual-size`, per-part `size`/`actual_size`, and compression-index totals
  should be arithmetically consistent;
- SSE-C customer-algorithm/MD5 markers identify SSE-C;
- KMS key-ID/context markers identify SSE-KMS;
- a managed encryption envelope without SSE-C or KMS markers identifies an
  SSE-S3 candidate.

Never include encryption metadata values in tickets, logs, chat, or assessment
reports. Metadata consistency is necessary but not sufficient: the defect
preserved metadata, so plausible sizes and a decodable index do not prove that
the stored bytes match it.

### 4. Validate Logical Content Without Mutation

For each high- or critical-risk candidate, perform a complete authenticated GET
of the exact version into a restricted validation sink. Supply the customer key
only for an authorized SSE-C check. Record the status, byte count, and a
cryptographic digest calculated by the validation client. Compare it with a
digest from an independently trusted source, backup, replica, or application
record.

Interpret the result conservatively:

- a GET decode/decrypt error, unexpected EOF, or short byte count is a strong
  affected-copy signal, but may also have another corruption cause;
- a matching independent cryptographic digest validates that logical version;
- a successful GET without an independent digest proves readability, not
  identity;
- a matching ETag alone is inconclusive;
- an SSE-S3/KMS candidate moved in the affected window remains a confidentiality
  incident until storage-level review excludes plaintext target copies and
  derivative snapshots or backups.

Storage-level confirmation for managed-SSE candidates may expose plaintext and
sealed-key material. It must be performed only by the incident/security owner on
offline evidence copies. Do not print, upload, or serve raw shard bytes, and do
not bypass RustFS to return them to an application.

### 5. Record Confidence And Outcome

Record one result for every candidate version:

- `confirmed-good`: full logical bytes match an independent digest;
- `confirmed-affected`: target decode/decrypt/length evidence and a trusted
  source establish the mismatch, or authorized storage review confirms
  plaintext under managed-SSE metadata;
- `suspected`: the version and operation window match, but proof is incomplete;
- `not-applicable`: evidence proves the object was plain and uncompressed or was
  never selected by an affected operation;
- `unrecoverable-pending-source`: affected or suspected, with no verified source
  yet found.

Retain the evidence used for each decision. Do not collapse object versions with
the same key into one result.

## Source Retention And Recovery Limits

Successful historical migration could be followed by source-entry deletion.
Therefore, neither successful rebalance status nor absence from the old source
pool proves that the target bytes are sound. Recovery is possible only from a
separately verified source, such as:

- retained source-pool media or a snapshot taken before cleanup;
- an independently validated replica;
- an external backup;
- the original application or upstream source with a trusted digest.

SSE-C normally failed before the target copy was accepted because the migration
read had no customer key. That failure prevented normal source cleanup, but
operators must verify the exact version on retained source media rather than
assuming it is present.

If no verified source exists, mark the version unrecoverable for this incident.
Do not edit `xl.meta`, rewrite shard files, clear encryption/compression markers,
or overwrite the object in place. Those actions can destroy evidence, violate
retention/versioning policy, or turn a visible read failure into silent data
substitution. Any restoration or replacement procedure needs its own reviewed,
rollback-aware plan.

## Release Guidance

Release notes for `1.0.0-rc.2` and later should state:

> Rebalance now copies the stored object representation for compressed and
> encrypted objects. Deployments that ran rebalance on versions from
> `1.0.0-alpha.91` through `1.0.0-rc.1` should preserve old pool media and run
> the read-only assessment in this guide. Upgrading prevents new copies from
> this defect but does not repair historical copies. Deployments that ran
> decommission from `1.0.0-alpha.91` through `1.0.0-beta.8` require the same
> assessment. SSE-S3 and SSE-KMS candidates require security incident handling.

Do not recommend rerunning rebalance as remediation. Do not remove or repurpose
old pool media until high- and critical-risk candidates have a recorded outcome
and the incident owner has accepted the recovery limits.

## Evidence Audit

The conclusions above are grounded in these repository facts:

- `crates/ecstore/src/services/rebalance/migration.rs` now sets both
  `data_movement` and `raw_data_movement_read` for rebalance source reads;
- `crates/ecstore/src/core/pools.rs` sets the same flags for decommission source
  reads;
- `crates/ecstore/src/object_api/readers.rs` returns the stored byte range before
  compression or encryption transforms when `raw_data_movement_read` is set;
- `crates/ecstore/src/data_movement/mod.rs` preserves stored part sizes, ETags,
  indexes, and internal metadata during migration;
- the historical `a236b0d0` rebalance and decommission readers both used normal
  read options, while `2f25cf60` changed only decommission to the raw mode;
- the historical rebalance entry deleted its source prefix only after all
  versions were counted as moved;
- the tag ancestry boundaries are `1.0.0-alpha.91`, `1.0.0-beta.9`, and
  `1.0.0-rc.2` for the implementation, decommission raw-read fix, and rebalance
  raw-read fix respectively.
