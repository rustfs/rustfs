# Scanner Excess Alerts: Metrics, S3 Events, and Thresholds

**Use this when:** debugging an excess-versions / excess-version-size / excess-folders alert, wiring a notification subscriber for `s3:Scanner:*` events, or explaining why a threshold differs from MinIO.

**Source of truth:** `crates/scanner/src/scanner_folder.rs` (`EVENT_SCANNER_*`, `DEFAULT_SCANNER_ALERT_COOLDOWN_SECS`, `MAX_SCANNER_ALERT_COOLDOWN_KEYS`, `METRIC_SCANNER_EXCESS_*`), `crates/config/src/constants/scanner.rs` (`DEFAULT_SCANNER_ALERT_EXCESS_*`), `crates/s3-types/src/event_name.rs` (`EventName::Scanner*`).

The background scanner detects three "excess" conditions while it walks buckets and surfaces them as metrics, structured logs, and S3 events. Threshold keys are also listed in the runtime-controls table in [Scanner Runtime Controls](scanner-runtime-controls.md).

## The three alerts

| Alert | Trigger (per scan cycle) | Metric | S3 event (RustFS wire name) | MinIO event name |
|---|---|---|---|---|
| Excess versions | Retained versions of one object >= `scanner.alert_excess_versions` | `rustfs_scanner_excess_object_versions_total{bucket}` | `s3:Scanner:ManyVersions` | `s3:ObjectManyVersions` |
| Excess version size | Cumulative bytes of all versions of one object >= `scanner.alert_excess_version_size` | `rustfs_scanner_excess_object_version_size_total{bucket}` | `s3:Scanner:LargeVersions` | `s3:ObjectLargeVersions` |
| Excess folders | Direct subfolders of one directory > `scanner.alert_excess_folders` | `rustfs_scanner_excess_folders_total{root}` | `s3:Scanner:BigPrefix` | `s3:PrefixManyFolders` |

Subscribe like any bucket notification: configure a notification on the target bucket with the RustFS wire name above (or the `s3:Scanner:*` wildcard, `EventName::ObjectScannerAll`). Events carry `UserAgent: Scanner` as their origin marker, and `req_params` holds the observed value and the threshold (`versions` / `cumulativeSize` / `folders` / `threshold`), so consumers can judge severity directly.

## Metrics and events fire on different cadences

| Surface | Cadence |
|---|---|
| Metrics and structured logs | Level-triggered: as long as the object stays over the threshold, every scan cycle counts and logs it (default cycle about 60s; see `scanner.speed`). |
| S3 events | Edge-triggered with a cooldown: the same (alert kind, bucket, object) emits at most once per cooldown window, `RUSTFS_SCANNER_ALERT_COOLDOWN_SECS` (`DEFAULT_SCANNER_ALERT_COOLDOWN_SECS`, 86400; `0` emits every cycle). When the window lapses and the object is still over the threshold, the event fires again. |
| Cooldown table | Process memory, hard cap `MAX_SCANNER_ALERT_COOLDOWN_KEYS` (4096) distinct keys; on overflow it is cleared and rebuilt (worst case one extra emission per still-hot key). A process restart resets it, so every still-over-threshold object emits once more after a restart. |

## Threshold defaults and MinIO deltas

| Config key | ENV | RustFS default | MinIO default | Notes |
|---|---|---|---|---|
| `scanner.alert_excess_versions` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSIONS` | 100 (`DEFAULT_SCANNER_ALERT_EXCESS_VERSIONS`) | 100 | Identical. |
| `scanner.alert_excess_version_size` | `RUSTFS_SCANNER_ALERT_EXCESS_VERSION_SIZE` | 1 TiB, 1099511627776 (`DEFAULT_SCANNER_ALERT_EXCESS_VERSION_SIZE`) | 1 TB | Same order of magnitude; different unit basis (TiB vs TB). |
| `scanner.alert_excess_folders` | `RUSTFS_SCANNER_ALERT_EXCESS_FOLDERS` | 65538 (`DEFAULT_SCANNER_ALERT_EXCESS_FOLDERS`) | 50000 | Deliberate divergence: 65538 tolerates the Proxmox Backup Server chunk layout (65536 chunks per directory plus the directory's own entries); MinIO's 50000 would fire continuously for PBS users. Set 50000 explicitly to match MinIO. |

All three keys accept both the environment variable and the `scanner` admin config subsystem (`SCANNER_SUB_SYS`, applied through `apply_scanner_runtime_config`); config updates take effect without a restart.

## Why the event names are mapped

`EventName::ScannerManyVersions` / `ScannerLargeVersions` / `ScannerBigPrefix` keep the repository's established `s3:Scanner:*` wire names, which differ literally from MinIO's `s3:ObjectManyVersions` family; the enum comments preserve the mapping. Subscribers should use the RustFS wire names. If MinIO-literal compatibility is needed, map the names on the console or consumer side rather than changing the published wire names.
