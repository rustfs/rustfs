# Log Cleaner Subsystem

The `cleaner` module is a production-focused background lifecycle manager for RustFS log archives.
It periodically discovers rolled files, applies retention constraints, compresses candidates, and then deletes sources safely.

The subsystem is designed to be conservative by default:

- it never touches the currently active log file;
- it refuses symlink deletion during the destructive phase;
- it keeps compression and source deletion as separate steps;
- it supports a full dry-run mode for policy verification.

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

4. **Archive Expiry (`core.rs`)**
   - Applies a separate retention window to already-compressed files.
   - Keeps archive expiration independent from plain-log keep-count logic.

## Compression Modes

- **Primary codec**: `zstd` (default) for better ratio and faster decompression.
- **Fallback codec**: `gzip` when zstd fallback is enabled.
- **Dry-run**: reports planned compression/deletion operations without touching filesystem state.

## Safety Model

- **No recursive traversal**: the scanner only inspects the immediate log directory.
- **No symlink following**: filesystem metadata is collected with `symlink_metadata`.
- **Idempotent archives**: an existing `*.gz` or `*.zst` target means the file is treated as already compressed.
- **Best-effort cleanup**: individual file failures are logged and do not abort the whole maintenance pass.

## Work-Stealing Strategy

The parallel path in `core.rs` uses this fixed lookup sequence per worker:

1. `local_worker.pop()`
2. `injector.steal_batch_and_pop(&local_worker)`
3. randomized victim polling via `Steal::from_iter(...)`

This strategy keeps local cache affinity while still balancing stragglers.

## Metrics and Tracing

The cleaner emits tracing events and runtime metrics:

- `rustfs_log_cleaner_deleted_files_total` (counter)
- `rustfs_log_cleaner_freed_bytes_total` (counter)
- `rustfs_log_cleaner_compress_duration_seconds` (histogram)
- `rustfs_log_cleaner_steal_success_rate` (gauge)
- `rustfs_log_cleaner_rotation_total` (counter)
- `rustfs_log_cleaner_rotation_failures_total` (counter)
- `rustfs_log_cleaner_rotation_duration_seconds` (histogram)
- `rustfs_log_cleaner_active_file_size_bytes` (gauge)

These values can be wired into dashboards and alert rules for cleanup health.

## Retention Decision Order

For regular logs, the cleaner evaluates candidates in this order:

1. keep at least `keep_files` newest matching generations;
2. remove older files if total retained size still exceeds `max_total_size_bytes`;
3. remove any file whose individual size exceeds `max_single_file_size_bytes`;
4. if compression is enabled, archive before deletion;
5. delete the original file only after successful compression.

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
| `RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS` | Retention window for `*.gz` / `*.zst` archives |
| `RUSTFS_OBS_LOG_DELETE_EMPTY_FILES` | Remove zero-byte regular log files during scanning |
| `RUSTFS_OBS_LOG_MIN_FILE_AGE_SECONDS` | Minimum age for regular log eligibility |

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

## Operational Notes

- Prefer `FileMatchMode::Suffix` when rotations prepend timestamps to the filename.
- Prefer `FileMatchMode::Prefix` when rotations append counters or timestamps after a stable base name.
- Keep `parallel_workers` modest when `zstd_workers` is greater than `1`, because each compression task may already use internal codec threads.
