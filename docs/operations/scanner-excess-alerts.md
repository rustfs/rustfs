# Scanner Excess Alerts: Metrics, S3 Events, and Thresholds

> 中文版：[scanner-excess-alerts_zh.md](scanner-excess-alerts_zh.md)

Date: 2026-08-18 (rustfs/backlog#1868 / HS-04; includes the HS-15 threshold-delta notes)

The background scanner detects three classes of "excess" conditions while it walks buckets and surfaces them as alerts. This page documents each alert's trigger condition, the subscribable S3 event, the cooldown semantics, and the threshold differences versus MinIO — for operators debugging alerts and for event consumers wiring up subscriptions.

## The three alerts

| Alert | Trigger (per scan cycle) | Metric | S3 event (RustFS wire name) | MinIO event name |
|---|---|---|---|---|
| Excess versions | Retained versions of one object ≥ `scanner:alert_excess_versions` | `rustfs_scanner_excess_object_versions_total{bucket}` | `s3:Scanner:ManyVersions` | `s3:ObjectManyVersions` |
| Excess version size | Cumulative bytes of all versions of one object ≥ `scanner:alert_excess_version_size` | `rustfs_scanner_excess_object_version_size_total{bucket}` | `s3:Scanner:LargeVersions` | `s3:ObjectLargeVersions` |
| Excess folders | Direct subfolders of one directory > `scanner:alert_excess_folders` | `rustfs_scanner_excess_folders_total{root}` | `s3:Scanner:BigPrefix` | `s3:PrefixManyFolders` |

Subscribe like any bucket notification: configure a notification on the target bucket with the RustFS wire name above (or the `s3:Scanner:*` wildcard). Events carry `UserAgent: Scanner` as their origin marker, and `req_params` holds the observed value and the threshold (`versions` / `cumulativeSize` / `folders` / `threshold`), so consumers can judge severity directly.

## Metrics and events fire on different cadences

- **Metrics and structured logs are level-triggered**: as long as the object stays over the threshold, every scan cycle counts and logs it (default cycle ≈ 60s; see `scanner:speed`).
- **S3 events are edge-triggered with a cooldown**: the same (alert kind, bucket, object) emits at most once per cooldown window — 24 hours by default (`RUSTFS_SCANNER_ALERT_COOLDOWN_SECS`; set it to 0 to emit every cycle). When the window lapses and the object is still over the threshold, the event fires again. The cooldown table lives in process memory with a 4096-entry hard cap; on overflow it is cleared and rebuilt (worst case: one extra emission per still-hot key).
- A process restart resets the cooldown (every still-over-threshold object emits once more after a restart) — deliberately: restarts usually accompany incident response, and the re-emission buys visibility.

## Threshold defaults and the MinIO deltas (HS-15)

| Config key | ENV | RustFS default | MinIO default | Notes |
|---|---|---|---|---|
| `scanner:alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | 100 | 100 | Identical |
| `scanner:alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | 1 TiB | 1 TB | Same order of magnitude; different unit basis (TiB vs TB) |
| `scanner:alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | 65538 | 50000 | **Deliberate divergence**: 65538 tolerates the Proxmox Backup Server chunk layout (65536 chunks per directory plus the directory's own entries); MinIO's 50000 would fire continuously for PBS users. Set it to 50000 explicitly to match MinIO behavior |

All three keys accept both env and admin config (`PUT /rustfs/admin/v3/config`, `scanner` subsystem); hot updates take effect immediately.

## Why the event names are mapped

RustFS's event enum (`rustfs_s3_types::EventName::ScannerManyVersions/LargeVersions/BigPrefix`) keeps the repo's established `s3:Scanner:*` wire names (literally different from MinIO's `s3:ObjectManyVersions`; the enum comments preserve the mapping). Subscribers should use the RustFS wire names in this page. If you need MinIO-literal compatibility, map the names on the console/consumer side — do not change the published wire names.
