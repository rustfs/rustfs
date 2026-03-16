# Log Cleaner Subsystem

The `cleaner` module is a production-focused background lifecycle manager for RustFS log archives.
It periodically discovers rolled files, applies retention constraints, compresses candidates, and then deletes sources safely.

## Execution Pipeline

1. **Discovery (`scanner.rs`)**
   - Performs a shallow `read_dir` scan (no recursion) for predictable latency.
   - Excludes the active log file, exclusion-pattern matches, and files younger than the age threshold.
   - Classifies regular logs and compressed archives (`.gz` / `.zst`) in one pass.

2. **Selection (`core.rs`)**
   - Enforces keep-count first.
   - Applies total-size and single-file-size constraints to oldest files.
   - Produces an ordered list of files to process.

3. **Compression + Deletion (`core.rs` + `compress.rs`)**
   - Supports `zstd` and `gzip` codecs.
   - Uses parallel work stealing when enabled (`Injector + Worker::new_fifo + Stealer`).
   - Always deletes source files in a serial pass after compression to minimize file-lock race issues.

## Compression Modes

- **Primary codec**: `zstd` (default) for better ratio and faster decompression.
- **Fallback codec**: `gzip` when zstd fallback is enabled.
- **Dry-run**: reports planned compression/deletion operations without touching filesystem state.

## Work-Stealing Strategy

The parallel path in `core.rs` uses this fixed lookup sequence per worker:

1. `local_worker.pop()`
2. `injector.steal_batch_and_pop(&local_worker)`
3. randomized victim polling via `Steal::from_iter(...)`

This strategy keeps local cache affinity while still balancing stragglers.

## Metrics and Tracing

The cleaner emits tracing events and runtime metrics:

- `rustfs.log_cleaner.deleted_files_total` (counter)
- `rustfs.log_cleaner.freed_bytes_total` (counter)
- `rustfs.log_cleaner.compress_duration_seconds` (histogram)
- `rustfs.log_cleaner.steal_success_rate` (gauge)
- `rustfs.log_cleaner.rotation_total` (counter)
- `rustfs.log_cleaner.rotation_failures_total` (counter)
- `rustfs.log_cleaner.rotation_duration_seconds` (histogram)
- `rustfs.log_cleaner.active_file_size_bytes` (gauge)

These values can be wired into dashboards and alert rules for cleanup health.

## Key Environment Variables

| Env Var | Meaning |
|---|---|
| `RUSTFS_OBS_LOG_COMPRESSION_ALGORITHM` | `zstd` or `gzip` |
| `RUSTFS_OBS_LOG_PARALLEL_COMPRESS` | Enable work-stealing compression |
| `RUSTFS_OBS_LOG_PARALLEL_WORKERS` | Worker count for parallel compressor |
| `RUSTFS_OBS_LOG_ZSTD_COMPRESSION_LEVEL` | Zstd level (1-21) |
| `RUSTFS_OBS_LOG_ZSTD_FALLBACK_TO_GZIP` | Fallback switch on zstd failure |
| `RUSTFS_OBS_LOG_ZSTD_WORKERS` | zstdmt worker threads per compression task |
| `RUSTFS_OBS_LOG_DRY_RUN` | Dry-run mode |

## Builder Example

```rust
use rustfs_obs::LogCleaner;
use rustfs_obs::types::{CompressionAlgorithm, FileMatchMode};
use std::path::PathBuf;

let cleaner = LogCleaner::builder(
    PathBuf::from("/var/log/rustfs"),
    "rustfs.log".to_string(),
    "rustfs.log".to_string(),
)
.match_mode(FileMatchMode::Suffix)
.keep_files(30)
.max_total_size_bytes(2 * 1024 * 1024 * 1024)
.compress_old_files(true)
.compression_algorithm(CompressionAlgorithm::Zstd)
.parallel_compress(true)
.parallel_workers(6)
.zstd_compression_level(8)
.zstd_fallback_to_gzip(true)
.zstd_workers(1)
.dry_run(false)
.build();

let _ = cleaner.cleanup();
```
