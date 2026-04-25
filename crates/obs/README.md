# rustfs-obs

Observability library for [RustFS](https://github.com/rustfs/rustfs) providing structured JSON
logging, distributed tracing, metrics via OpenTelemetry, and continuous profiling via Pyroscope.

---

## Features

| Feature                      | Description                                                                  |
|------------------------------|------------------------------------------------------------------------------|
| **Structured logging**       | JSON-formatted logs via `tracing-subscriber`                                 |
| **Rolling-file logging**     | Daily / hourly rotation with automatic cleanup and high-precision timestamps |
| **Distributed tracing**      | OTLP/HTTP export to Jaeger, Tempo, or any OTel collector                     |
| **Metrics**                  | OTLP/HTTP export, bridged from the `metrics` crate facade                    |
| **Continuous Profiling**     | CPU/Memory profiling export to Pyroscope                                     |
| **Log cleanup**              | Background task: size limits, zstd/gzip compression, retention policies      |
| **GPU metrics** *(optional)* | Enable with the `gpu` feature flag                                           |

---

## Quick Start

```toml
# Cargo.toml
[dependencies]
rustfs-obs = { version = "0.0.5" }

# GPU metrics support
rustfs-obs = { version = "0.0.5", features = ["gpu"] }
```

```rust
use rustfs_obs::init_obs;

#[tokio::main]
async fn main() {
    // Build config from environment variables, then initialise all backends.
    let _guard = init_obs(None).await.expect("failed to initialise observability");

    tracing::info!("RustFS started");

    // _guard is dropped here — all providers are flushed and shut down.
}
```

> **Keep `_guard` alive** for the lifetime of your application. Dropping it
> triggers an ordered shutdown of every OpenTelemetry provider.

---

## Initialisation

### With an explicit OTLP endpoint

```rust
use rustfs_obs::init_obs;

let _guard = init_obs(Some("http://otel-collector:4318".to_string()))
.await
.expect("observability init failed");
```

### With a custom config struct

```rust
use rustfs_obs::{AppConfig, OtelConfig, init_obs_with_config};

let config = AppConfig::new_with_endpoint(Some("http://localhost:4318".to_string()));
let _guard = init_obs_with_config( & config.observability)
.await
.expect("observability init failed");
```

---

## Routing Logic

The library selects a backend automatically based on configuration:

```
1. Any OTLP endpoint set?
   └─ YES → Full OTLP/HTTP pipeline (traces + metrics + logs + profiling)

2. RUSTFS_OBS_LOG_DIRECTORY set to a non-empty path?
   └─ YES → Rolling-file JSON logging
            + Stdout mirror enabled if:
              - RUSTFS_OBS_LOG_STDOUT_ENABLED=true (explicit), OR
              - RUSTFS_OBS_ENVIRONMENT != "production" (automatic)

3. Default → Stdout-only JSON logging (all signals)
```

**Key Points:**

- When **no log directory** is configured, logs automatically go to **stdout only** (perfect for development)
- When a **log directory** is set, logs go to **rolling files** in that directory
- In **non-production environments**, stdout is automatically mirrored alongside file logging for visibility
- In **production** mode, you must explicitly set `RUSTFS_OBS_LOG_STDOUT_ENABLED=true` to see stdout in addition to
  files

---

## Environment Variables

All configuration is read from environment variables at startup.

### OTLP / Export

| Variable                              | Default   | Description                                                |
|---------------------------------------|-----------|------------------------------------------------------------|
| `RUSTFS_OBS_ENDPOINT`                 | _(empty)_ | Root OTLP/HTTP endpoint, e.g. `http://otel-collector:4318` |
| `RUSTFS_OBS_TRACE_ENDPOINT`           | _(empty)_ | Dedicated trace endpoint (overrides root + `/v1/traces`)   |
| `RUSTFS_OBS_METRIC_ENDPOINT`          | _(empty)_ | Dedicated metrics endpoint                                 |
| `RUSTFS_OBS_LOG_ENDPOINT`             | _(empty)_ | Dedicated log endpoint                                     |
| `RUSTFS_OBS_PROFILING_ENDPOINT`       | _(empty)_ | Dedicated profiling endpoint (e.g. Pyroscope)              |
| `RUSTFS_OBS_TRACES_EXPORT_ENABLED`    | `true`    | Toggle trace export                                        |
| `RUSTFS_OBS_METRICS_EXPORT_ENABLED`   | `true`    | Toggle metrics export                                      |
| `RUSTFS_OBS_LOGS_EXPORT_ENABLED`      | `true`    | Toggle OTLP log export                                     |
| `RUSTFS_OBS_PROFILING_EXPORT_ENABLED` | `true`    | Toggle profiling export                                    |
| `RUSTFS_OBS_USE_STDOUT`               | `false`   | Mirror all signals to stdout alongside OTLP                |
| `RUSTFS_OBS_SAMPLE_RATIO`             | `0.1`     | Trace sampling ratio `0.0`–`1.0`                           |
| `RUSTFS_OBS_METER_INTERVAL`           | `15`      | Metrics export interval (seconds)                          |

### Service identity

| Variable                     | Default           | Description                                             |
|------------------------------|-------------------|---------------------------------------------------------|
| `RUSTFS_OBS_SERVICE_NAME`    | `rustfs`          | OTel `service.name`                                     |
| `RUSTFS_OBS_SERVICE_VERSION` | _(crate version)_ | OTel `service.version`                                  |
| `RUSTFS_OBS_ENVIRONMENT`     | `development`     | Deployment environment (`production`, `development`, …) |

### Local logging

| Variable                        | Default      | Description                                                                                                                                                                                                                                                                                                                                                           |
|---------------------------------|--------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `RUSTFS_OBS_LOGGER_LEVEL`       | `info`       | Log level; `RUST_LOG` syntax supported                                                                                                                                                                                                                                                                                                                                |
| `RUSTFS_OBS_LOG_STDOUT_ENABLED` | `false`      | When file logging is active, also mirror to stdout                                                                                                                                                                                                                                                                                                                    |
| `RUSTFS_OBS_LOG_DIRECTORY`      | _(empty)_    | **Directory for rolling log files. When empty, logs go to stdout only**                                                                                                                                                                                                                                                                                               |
| `RUSTFS_OBS_LOG_FILENAME`       | `rustfs.log` | Base filename for rolling logs. Rotated archives include a high-precision timestamp and counter. With the default `RUSTFS_OBS_LOG_MATCH_MODE=suffix`, names look like `<timestamp>-<counter>.rustfs.log` (e.g., `20231027103001.123456-0.rustfs.log`); with `prefix`, they look like `rustfs.log.<timestamp>-<counter>` (e.g., `rustfs.log.20231027103001.123456-0`). |
| `RUSTFS_OBS_LOG_ROTATION_TIME`  | `hourly`     | Rotation granularity: `minutely`, `hourly`, or `daily`                                                                                                                                                                                                                                                                                                                |
| `RUSTFS_OBS_LOG_KEEP_FILES`     | `30`         | Number of rolling files to keep (also used by cleaner)                                                                                                                                                                                                                                                                                                                |
| `RUSTFS_OBS_LOG_MATCH_MODE`     | `suffix`     | File matching mode: `prefix` or `suffix`                                                                                                                                                                                                                                                                                                                              |

### Log cleanup

| Variable                                        | Default      | Description                                                 |
|-------------------------------------------------|--------------|-------------------------------------------------------------|
| `RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES`           | `2147483648` | Hard cap on total log directory size (2 GiB)                |
| `RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES`     | `0`          | Per-file size cap; `0` = unlimited                          |
| `RUSTFS_OBS_LOG_COMPRESS_OLD_FILES`             | `true`       | Compress files before deleting                              |
| `RUSTFS_OBS_LOG_GZIP_COMPRESSION_LEVEL`         | `6`          | Gzip level `1` (fastest) – `9` (best)                       |
| `RUSTFS_OBS_LOG_COMPRESSION_ALGORITHM`          | `zstd`       | Compression codec: `zstd` or `gzip`                         |
| `RUSTFS_OBS_LOG_PARALLEL_COMPRESS`              | `true`       | Enable work-stealing parallel compression                   |
| `RUSTFS_OBS_LOG_PARALLEL_WORKERS`               | `6`          | Number of cleaner worker threads                            |
| `RUSTFS_OBS_LOG_ZSTD_COMPRESSION_LEVEL`         | `8`          | Zstd level `1` (fastest) – `21` (best ratio)               |
| `RUSTFS_OBS_LOG_ZSTD_FALLBACK_TO_GZIP`          | `true`       | Fallback to gzip when zstd compression fails                |
| `RUSTFS_OBS_LOG_ZSTD_WORKERS`                   | `1`          | zstdmt worker threads per compression task                  |
| `RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS` | `30`         | Delete `.gz` / `.zst` archives older than N days; `0` = keep forever |
| `RUSTFS_OBS_LOG_EXCLUDE_PATTERNS`               | _(empty)_    | Comma-separated glob patterns to never clean up             |
| `RUSTFS_OBS_LOG_DELETE_EMPTY_FILES`             | `true`       | Remove zero-byte files                                      |
| `RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS`           | `3600`       | Minimum file age (seconds) before cleanup                   |
| `RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS`       | `1800`       | How often the cleanup task runs (0.5 hours)                 |
| `RUSTFS_OBS_LOG_DRY_RUN`                        | `false`      | Report deletions without actually removing files            |

---

## Cleaner & Rotation Metrics

The log rotation and cleanup pipeline emits these metrics (via the `metrics` facade):

| Metric | Type | Description |
|---|---|---|
| `rustfs_log_cleaner_deleted_files_total` | counter | Number of files deleted per cleanup pass |
| `rustfs_log_cleaner_freed_bytes_total` | counter | Bytes reclaimed by deletion |
| `rustfs_log_cleaner_compress_duration_seconds` | histogram | Compression stage duration |
| `rustfs_log_cleaner_steal_success_rate` | gauge | Work-stealing success ratio in parallel mode |
| `rustfs_log_cleaner_runs_total` | counter | Successful cleanup loop runs |
| `rustfs_log_cleaner_run_failures_total` | counter | Failed or panicked cleanup loop runs |
| `rustfs_log_cleaner_rotation_total` | counter | Successful file rotations |
| `rustfs_log_cleaner_rotation_failures_total` | counter | Failed file rotations |
| `rustfs_log_cleaner_rotation_duration_seconds` | histogram | Rotation latency |
| `rustfs_log_cleaner_active_file_size_bytes` | gauge | Current active log file size |

These metrics cover compression, cleanup, and file rotation end-to-end.

### Metric Semantics

- `deleted_files_total` and `freed_bytes_total` are emitted after each cleanup pass and include both normal log cleanup and expired compressed archive cleanup.
- `compress_duration_seconds` measures compression stage wall-clock time for both serial and parallel modes.
- `steal_success_rate` is updated by the parallel work-stealing path and remains at the last computed value.
- `rotation_*` metrics are emitted by `RollingAppender` and include retries; a failed final rotation increments `rotation_failures_total`.
- `active_file_size_bytes` is sampled on writes and after successful roll, so dashboards can track current active file growth.

### Grafana Dashboard JSON Draft (Ready to Import)

> Save this as `rustfs-log-cleaner-dashboard.json`, then import from Grafana UI.
> The canonical metric names use underscore notation, for example
> `rustfs_log_cleaner_deleted_files_total`.
>
> The same panels are now checked in at:
> `.docker/observability/grafana/dashboards/rustfs.json`
> (row title: `Log Cleaner`).

```json
{
  "uid": "rustfs-log-cleaner",
  "title": "RustFS Log Cleaner",
  "timezone": "browser",
  "schemaVersion": 39,
  "version": 1,
  "refresh": "10s",
  "tags": ["rustfs", "observability", "log-cleaner"],
  "time": {
    "from": "now-6h",
    "to": "now"
  },
  "panels": [
    {
      "id": 1,
      "title": "Cleanup Runs / Failures",
      "type": "timeseries",
      "targets": [
        { "refId": "A", "expr": "sum(rate(rustfs_log_cleaner_runs_total[5m]))", "legendFormat": "runs/s" },
        { "refId": "B", "expr": "sum(rate(rustfs_log_cleaner_run_failures_total[5m]))", "legendFormat": "failures/s" }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 0 }
    },
    {
      "id": 2,
      "title": "Freed Bytes / Deleted Files",
      "type": "timeseries",
      "targets": [
        { "refId": "A", "expr": "sum(rate(rustfs_log_cleaner_freed_bytes_total[15m]))", "legendFormat": "bytes/s" },
        { "refId": "B", "expr": "sum(rate(rustfs_log_cleaner_deleted_files_total[15m]))", "legendFormat": "files/s" }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 0 }
    },
    {
      "id": 3,
      "title": "Compression P95 Latency",
      "type": "timeseries",
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum(rate(rustfs_log_cleaner_compress_duration_seconds_bucket[5m])) by (le))",
          "legendFormat": "p95"
        }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 8 }
    },
    {
      "id": 4,
      "title": "Rotation Success / Failure",
      "type": "timeseries",
      "targets": [
        { "refId": "A", "expr": "sum(rate(rustfs_log_cleaner_rotation_total[5m]))", "legendFormat": "rotation/s" },
        { "refId": "B", "expr": "sum(rate(rustfs_log_cleaner_rotation_failures_total[5m]))", "legendFormat": "rotation_failures/s" }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 8 }
    },
    {
      "id": 5,
      "title": "Steal Success Rate",
      "type": "timeseries",
      "targets": [
        { "refId": "A", "expr": "max(rustfs_log_cleaner_steal_success_rate)", "legendFormat": "ratio" }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 0, "y": 16 }
    },
    {
      "id": 6,
      "title": "Active File Size",
      "type": "timeseries",
      "targets": [
        { "refId": "A", "expr": "max(rustfs_log_cleaner_active_file_size_bytes)", "legendFormat": "bytes" }
      ],
      "gridPos": { "h": 8, "w": 12, "x": 12, "y": 16 }
    }
  ]
}
```

### PromQL Templates

Use these templates directly in Grafana panels/alerts.

- **Cleanup run rate**
  - `sum(rate(rustfs_log_cleaner_runs_total[$__rate_interval]))`
- **Cleanup failure rate**
  - `sum(rate(rustfs_log_cleaner_run_failures_total[$__rate_interval]))`
- **Cleanup failure ratio**
  - `sum(rate(rustfs_log_cleaner_run_failures_total[$__rate_interval])) / clamp_min(sum(rate(rustfs_log_cleaner_runs_total[$__rate_interval])), 1e-9)`
- **Freed bytes throughput**
  - `sum(rate(rustfs_log_cleaner_freed_bytes_total[$__rate_interval]))`
- **Deleted files throughput**
  - `sum(rate(rustfs_log_cleaner_deleted_files_total[$__rate_interval]))`
- **Compression p95 latency**
  - `histogram_quantile(0.95, sum(rate(rustfs_log_cleaner_compress_duration_seconds_bucket[$__rate_interval])) by (le))`
- **Rotation failure ratio**
  - `sum(rate(rustfs_log_cleaner_rotation_failures_total[$__rate_interval])) / clamp_min(sum(rate(rustfs_log_cleaner_rotation_total[$__rate_interval])), 1e-9)`
- **Work-stealing efficiency (latest)**
  - `max(rustfs_log_cleaner_steal_success_rate)`
- **Active file size (latest)**
  - `max(rustfs_log_cleaner_active_file_size_bytes)`

### Suggested Alerts

- **CleanupFailureRatioHigh**: failure ratio > 0.05 for 10m.
- **CompressionLatencyP95High**: p95 above your baseline SLO for 15m.
- **RotationFailuresDetected**: rotation failure rate > 0 for 3 consecutive windows.
- **NoCleanupActivity**: runs rate == 0 for expected active environments.

### Metrics Compatibility

The project is currently in active development. Metric names and labels are updated directly when architecture evolves, and no backward-compatibility shim is maintained for old names.
Use the metric names documented in this README as the current source of truth.

---

## Examples

### Stdout-only (development default)

```bash
# No RUSTFS_OBS_LOG_DIRECTORY set → stdout JSON
RUSTFS_OBS_LOGGER_LEVEL=debug ./rustfs
```

### Rolling-file logging

```bash
export RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs
export RUSTFS_OBS_LOGGER_LEVEL=info
export RUSTFS_OBS_LOG_KEEP_FILES=30
export RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES=5368709120   # 5 GiB
./rustfs
```

### Full OTLP pipeline (production)

```bash
export RUSTFS_OBS_ENDPOINT=http://otel-collector:4318
export RUSTFS_OBS_ENVIRONMENT=production
export RUSTFS_OBS_SAMPLE_RATIO=0.05      # 5% trace sampling
export RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs
export RUSTFS_OBS_LOG_STDOUT_ENABLED=false
./rustfs
```

### Separate per-signal endpoints

```bash
export RUSTFS_OBS_TRACE_ENDPOINT=http://tempo:4318/v1/traces
export RUSTFS_OBS_METRIC_ENDPOINT=http://prometheus-otel:4318/v1/metrics
export RUSTFS_OBS_LOG_ENDPOINT=http://loki-otel:4318/v1/logs
./rustfs
```

### Dry-run cleanup audit

```bash
export RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs
export RUSTFS_OBS_LOG_DRY_RUN=true
./rustfs
# Observe log output — no files will actually be deleted.
```

### Parallel zstd cleanup (recommended production profile)

```bash
export RUSTFS_OBS_LOG_DIRECTORY=/var/log/rustfs
export RUSTFS_OBS_LOG_COMPRESSION_ALGORITHM=zstd
export RUSTFS_OBS_LOG_PARALLEL_COMPRESS=true
export RUSTFS_OBS_LOG_PARALLEL_WORKERS=6
export RUSTFS_OBS_LOG_ZSTD_COMPRESSION_LEVEL=8
export RUSTFS_OBS_LOG_ZSTD_FALLBACK_TO_GZIP=true
export RUSTFS_OBS_LOG_ZSTD_WORKERS=1
./rustfs
```

---

## Module Structure

```
rustfs-obs/src/
├── lib.rs                   # Crate root; public re-exports
├── config.rs                # OtelConfig + AppConfig; env-var loading
├── error.rs                 # TelemetryError type
├── global.rs                # init_obs / init_obs_with_config entry points
│
├── telemetry/               # Backend initialisation
│   ├── mod.rs               # init_telemetry routing logic
│   ├── guard.rs             # OtelGuard RAII lifecycle manager
│   ├── filter.rs            # EnvFilter construction helpers
│   ├── resource.rs          # OTel Resource builder
│   ├── local.rs             # Stdout-only and rolling-file backends
│   ├── otel.rs              # Full OTLP/HTTP pipeline
│   └── recorder.rs          # metrics-crate → OTel bridge (Recorder)
│
├── cleaner/                 # Background log-file cleanup subsystem
│   ├── mod.rs               # LogCleaner public API + tests
│   ├── types.rs             # Shared cleaner types (match mode, compression codec, FileInfo)
│   ├── scanner.rs           # Filesystem discovery
│   ├── compress.rs          # Gzip/Zstd compression helper
│   └── core.rs              # Selection, compression, deletion logic
│
└── system/                  # Host metrics (CPU, memory, disk, GPU)
    ├── mod.rs
    ├── attributes.rs
    ├── collector.rs
    ├── metrics.rs
    └── gpu.rs               # GPU metrics (feature = "gpu")
```

---

## Using `LogCleaner` Directly

```rust
use std::path::PathBuf;
use rustfs_obs::LogCleaner;
use rustfs_obs::types::FileMatchMode;

let cleaner = LogCleaner::builder(
PathBuf::from("/var/log/rustfs"),
"rustfs.log.".to_string(),  // file_pattern
"rustfs.log".to_string(),   // active_filename
)
.match_mode(FileMatchMode::Prefix)
.keep_files(10)
.max_total_size_bytes(2 * 1024 * 1024 * 1024) // 2 GiB
.max_single_file_size_bytes(0) // unlimited
.compress_old_files(true)
.gzip_compression_level(6)
.compressed_file_retention_days(7)
.exclude_patterns(vec!["current.log".to_string()])
.delete_empty_files(true)
.min_file_age_seconds(3600) // 1 hour
.dry_run(false)
.build();

let (deleted, freed_bytes) = cleaner.cleanup().expect("cleanup failed");
println!("Deleted {deleted} files, freed {freed_bytes} bytes");
```

---

## Feature Flags

| Flag        | Description                        |
|-------------|------------------------------------|
| _(default)_ | Core logging, tracing, and metrics |
| `gpu`       | GPU utilisation metrics via `nvml` |
| `full`      | All features enabled               |

```toml
# Enable GPU monitoring
rustfs-obs = { version = "0.0.5", features = ["gpu"] }

# Enable everything
rustfs-obs = { version = "0.0.5", features = ["full"] }
```

---

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
