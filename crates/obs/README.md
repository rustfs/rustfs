# rustfs-obs

Observability library for [RustFS](https://github.com/rustfs/rustfs) providing structured JSON
logging, distributed tracing, and metrics via OpenTelemetry.

---

## Features

| Feature | Description |
|---------|-------------|
| **Structured logging** | JSON-formatted logs via `tracing-subscriber` |
| **Rolling-file logging** | Daily / hourly rotation with automatic cleanup |
| **Distributed tracing** | OTLP/HTTP export to Jaeger, Tempo, or any OTel collector |
| **Metrics** | OTLP/HTTP export, bridged from the `metrics` crate facade |
| **Log cleanup** | Background task: size limits, gzip compression, retention policies |
| **GPU metrics** *(optional)* | Enable with the `gpu` feature flag |

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

> **Keep `_guard` alive** for the lifetime of your application.  Dropping it
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
let _guard = init_obs_with_config(&config.observability)
    .await
    .expect("observability init failed");
```

---

## Routing Logic

The library selects a backend automatically based on configuration:

```
1. Any OTLP endpoint set?
   └─ YES → Full OTLP/HTTP pipeline (traces + metrics + logs)

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
- In **production** mode, you must explicitly set `RUSTFS_OBS_LOG_STDOUT_ENABLED=true` to see stdout in addition to files

---

## Environment Variables

All configuration is read from environment variables at startup.

### OTLP / Export

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTFS_OBS_ENDPOINT` | _(empty)_ | Root OTLP/HTTP endpoint, e.g. `http://otel-collector:4318` |
| `RUSTFS_OBS_TRACE_ENDPOINT` | _(empty)_ | Dedicated trace endpoint (overrides root + `/v1/traces`) |
| `RUSTFS_OBS_METRIC_ENDPOINT` | _(empty)_ | Dedicated metrics endpoint |
| `RUSTFS_OBS_LOG_ENDPOINT` | _(empty)_ | Dedicated log endpoint |
| `RUSTFS_OBS_TRACES_EXPORT_ENABLED` | `true` | Toggle trace export |
| `RUSTFS_OBS_METRICS_EXPORT_ENABLED` | `true` | Toggle metrics export |
| `RUSTFS_OBS_LOGS_EXPORT_ENABLED` | `true` | Toggle OTLP log export |
| `RUSTFS_OBS_USE_STDOUT` | `false` | Mirror all signals to stdout alongside OTLP |
| `RUSTFS_OBS_SAMPLE_RATIO` | `0.1` | Trace sampling ratio `0.0`–`1.0` |
| `RUSTFS_OBS_METER_INTERVAL` | `15` | Metrics export interval (seconds) |

### Service identity

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTFS_OBS_SERVICE_NAME` | `rustfs` | OTel `service.name` |
| `RUSTFS_OBS_SERVICE_VERSION` | _(crate version)_ | OTel `service.version` |
| `RUSTFS_OBS_ENVIRONMENT` | `development` | Deployment environment (`production`, `development`, …) |

### Local logging

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTFS_OBS_LOGGER_LEVEL` | `info` | Log level; `RUST_LOG` syntax supported |
| `RUSTFS_OBS_LOG_STDOUT_ENABLED` | `false` | When file logging is active, also mirror to stdout |
| `RUSTFS_OBS_LOG_DIRECTORY` | _(empty)_ | **Directory for rolling log files. When empty, logs go to stdout only** |
| `RUSTFS_OBS_LOG_FILENAME` | `rustfs` | Base filename for rolling logs (date suffix added automatically) |
| `RUSTFS_OBS_LOG_ROTATION_TIME` | `hourly` | Rotation granularity: `minutely`, `hourly`, or `daily` |
| `RUSTFS_OBS_LOG_KEEP_FILES` | `30` | Number of rolling files to keep |

### Log cleanup

| Variable | Default | Description |
|----------|---------|-------------|
| `RUSTFS_OBS_LOG_KEEP_COUNT` | `10` | Minimum files the cleaner must always preserve |
| `RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES` | `2147483648` | Hard cap on total log directory size (2 GiB) |
| `RUSTFS_OBS_LOG_MAX_SINGLE_FILE_SIZE_BYTES` | `0` | Per-file size cap; `0` = unlimited |
| `RUSTFS_OBS_LOG_COMPRESS_OLD_FILES` | `true` | Gzip-compress files before deleting |
| `RUSTFS_OBS_LOG_GZIP_COMPRESSION_LEVEL` | `6` | Gzip level `1` (fastest) – `9` (best) |
| `RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS` | `30` | Delete `.gz` archives older than N days; `0` = keep forever |
| `RUSTFS_OBS_LOG_EXCLUDE_PATTERNS` | _(empty)_ | Comma-separated glob patterns to never clean up |
| `RUSTFS_OBS_LOG_DELETE_EMPTY_FILES` | `true` | Remove zero-byte files |
| `RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS` | `3600` | Minimum file age (seconds) before cleanup |
| `RUSTFS_OBS_LOG_CLEANUP_INTERVAL_SECONDS` | `21600` | How often the cleanup task runs (6 hours) |
| `RUSTFS_OBS_LOG_DRY_RUN` | `false` | Report deletions without actually removing files |


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
├── log_cleanup/             # Background log-file cleanup subsystem
│   ├── mod.rs               # LogCleaner public API + tests
│   ├── types.rs             # FileInfo shared type
│   ├── scanner.rs           # Filesystem discovery
│   ├── compress.rs          # Gzip compression helper
│   └── cleaner.rs           # Selection, compression, deletion logic
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

let cleaner = LogCleaner::new(
    PathBuf::from("/var/log/rustfs"),
    "rustfs.log.".to_string(),  // file_prefix
    10,                          // keep_count
    2 * 1024 * 1024 * 1024,      // max_total_size_bytes (2 GiB)
    0,                           // max_single_file_size_bytes (unlimited)
    true,                        // compress_old_files
    6,                           // gzip_compression_level
    30,                          // compressed_file_retention_days
    vec!["current.log".to_string()], // exclude_patterns
    true,                        // delete_empty_files
    3600,                        // min_file_age_seconds (1 hour)
    false,                       // dry_run
);

let (deleted, freed_bytes) = cleaner.cleanup().expect("cleanup failed");
println!("Deleted {deleted} files, freed {freed_bytes} bytes");
```

---

## Feature Flags

| Flag | Description |
|------|-------------|
| _(default)_ | Core logging, tracing, and metrics |
| `gpu` | GPU utilisation metrics via `nvml` |
| `full` | All features enabled |

```toml
# Enable GPU monitoring
rustfs-obs = { version = "0.0.5", features = ["gpu"] }

# Enable everything
rustfs-obs = { version = "0.0.5", features = ["full"] }
```

---

## License

Apache 2.0 — see [LICENSE](../../LICENSE).
