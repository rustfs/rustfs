# ListObjectsV2 Baseline Fixtures and Metrics

Parent tracker: rustfs/backlog#840

Phase tracker: rustfs/backlog#841

## Purpose

This document defines the baseline fixture matrix and metrics that every
ListObjectsV2 optimization should use before comparing performance. The default
source mode remains the walker-backed path; index-backed modes must add their
own source label before they are evaluated.

## Metrics Added For Phase 0

The Phase 0 instrumentation emits low-cardinality metrics only. Bucket names,
prefix strings, and object keys are intentionally not used as metric labels.

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `rustfs_s3_list_objects_gather_total` | counter | `source`, `outcome`, `has_prefix`, `has_delimiter`, `has_marker` | Count gather phases by source and request shape. |
| `rustfs_s3_list_objects_gather_duration_ms` | histogram | `source`, `outcome` | Attribute gather latency to full page vs input-drained outcomes. |
| `rustfs_s3_list_objects_gather_scanned_entries` | histogram | `source` | Count entries consumed before a page is produced. |
| `rustfs_s3_list_objects_gather_returned_entries` | histogram | `source` | Count entries accepted by gather before pagination trimming. |
| `rustfs_s3_list_objects_gather_filtered_entries` | histogram | `source` | Estimate filter work inside gather. |
| `rustfs_s3_list_objects_gather_scan_amplification` | histogram | `source` | Ratio of scanned entries to returned entries. |
| `rustfs_s3_list_objects_gather_limit` | histogram | `source` | Track the internal requested page limit. |
| `rustfs_s3_list_objects_merge_fan_in` | histogram | `source`, `outcome` | Track merge input stream fan-in. |
| `rustfs_s3_list_objects_merge_read_quorum` | histogram | `source`, `outcome` | Track merge read quorum. |

Current source labels:

| Label | Meaning |
| --- | --- |
| `walker` | Default walker-backed ListObjects path. |

Expected future labels:

| Label | Meaning |
| --- | --- |
| `index_key_only` | Index proposes keys; live metadata remains authoritative. |
| `index_verified_page` | Index proposes pages; live metadata verification gates responses. |
| `index_metadata_fast` | Index provides metadata directly under an explicit eventual-consistency SLA. |

## Baseline Fixture Matrix

Use deterministic object names so page boundaries and diff reports are stable.

| Fixture | Shape | Required LIST Variants | Primary Metrics |
| --- | --- | --- | --- |
| `flat-large` | One prefix with many keys, e.g. `flat/00000000.dat` to `flat/00999999.dat`. | `list-type=2`, `max-keys=1000`, continuation-token loop. | scanned entries, scan amplification, p95/p99 gather duration. |
| `deep-prefix` | Multi-level prefixes, e.g. `deep/aa/bb/cc/000000.dat`. | prefix-only, delimiter `/`, recursive no-delimiter. | filtered entries, common-prefix page continuity. |
| `delete-marker-heavy` | Versioned bucket with overwrite/delete churn. | plain LIST, version-aware regression tests, continuation-token loop. | returned entries, filtered entries, stale-delete adversarial tests. |
| `mixed-version` | Multiple versions per key with latest and non-latest entries. | plain LIST and ListObjectVersions coverage. | merge read quorum, metadata decode behavior, returned entries. |
| `multipart-adjacent` | Completed multipart objects adjacent to normal objects by lexical order. | delimiter and recursive listing. | page continuity, merge fan-in, gather duration. |

## Capture Checklist

1. Record the git SHA, RustFS version, topology, disk type, and bucket fixture.
2. Warm up the target bucket with one full continuation-token listing pass.
3. Run at least three measured passes for each fixture and request shape.
4. Capture p50, p95, p99, scanned entries, returned entries, filtered entries,
   scan amplification, merge fan-in, and read quorum.
5. Attach the raw command log and metrics snapshot to the relevant issue.

## Validation Commands

Focused Phase 0 validation:

```bash
scripts/validate_issue_841_list_objects_observability.sh
```

Expanded local validation:

```bash
cargo test -p rustfs-io-metrics list_objects_metrics
cargo test -p rustfs-ecstore list_objects
cargo check -p rustfs
```

Full PR-ready validation remains:

```bash
make pre-pr
```
