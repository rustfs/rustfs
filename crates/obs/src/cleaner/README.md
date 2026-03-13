# Log Cleaner Subsystem

The `cleaner` module provides a robust, background log-file lifecycle manager for RustFS. It is designed to run periodically to enforce retention policies, compress old logs, and prevent disk exhaustion.

## Architecture

The cleaner operates as a pipeline:

1.  **Discovery (`scanner.rs`)**: Scans the configured log directory for eligible files.
    *   **Non-recursive**: Only scans the top-level directory for safety.
    *   **Filtering**: Ignores the currently active log file, files matching exclude patterns, and files that do not match the configured prefix/suffix pattern.
    *   **Performance**: Uses `std::fs::read_dir` directly to minimize overhead and syscalls.

2.  **Selection (`core.rs`)**: Applies retention policies to select files for deletion.
    *   **Keep Count**: Ensures at least `N` recent files are kept.
    *   **Total Size**: Deletes oldest files if the total size exceeds the limit.
    *   **Single File Size**: Deletes individual files that exceed a size limit (e.g., runaway logs).

3.  **Action (`core.rs` / `compress.rs`)**:
    *   **Compression**: Optionally compresses selected files using Gzip (level 1-9) before deletion.
    *   **Deletion**: Removes the original file (and eventually the compressed archive based on retention days).

## Configuration

The cleaner is configured via `LogCleanerBuilder`. When initialized via `rustfs-obs::init_obs`, it reads from environment variables.

| Parameter | Env Var | Description |
|-----------|---------|-------------|
| `log_dir` | `RUSTFS_OBS_LOG_DIRECTORY` | The directory to scan. |
| `file_pattern` | `RUSTFS_OBS_LOG_FILENAME` | The base filename pattern (e.g., `rustfs.log`). |
| `active_filename` | (Derived) | The exact name of the currently active log file, excluded from cleanup. |
| `match_mode` | `RUSTFS_OBS_LOG_MATCH_MODE` | `prefix` or `suffix`. Determines how `file_pattern` is matched against filenames. |
| `keep_files` | `RUSTFS_OBS_LOG_KEEP_FILES` | Minimum number of rolling log files to keep. |
| `max_total_size_bytes` | `RUSTFS_OBS_LOG_MAX_TOTAL_SIZE_BYTES` | Maximum aggregate size of all log files. Oldest files are deleted to satisfy this. |
| `compress_old_files` | `RUSTFS_OBS_LOG_COMPRESS_OLD_FILES` | If `true`, files selected for removal are first gzipped. |
| `compressed_file_retention_days` | `RUSTFS_OBS_LOG_COMPRESSED_FILE_RETENTION_DAYS` | Age in days after which `.gz` files are deleted. |

## Timestamp Format & Rotation

The cleaner works in tandem with the `RollingAppender` in `telemetry/rolling.rs`.

*   **Rotation**: Logs are rotated based on time (Daily/Hourly/Minutely) or Size.
*   **Naming**: Archived logs use a high-precision timestamp format: `YYYYMMDDHHMMSS.uuuuuu` (microseconds), plus a unique counter to prevent collisions.
    *   **Suffix Mode**: `<timestamp>-<counter>.<filename>` (e.g., `20231027103001.123456-0.rustfs.log`)
    *   **Prefix Mode**: `<filename>.<timestamp>-<counter>` (e.g., `rustfs.log.20231027103001.123456-0`)

This high-precision naming ensures that files sort chronologically by name, and collisions are virtually impossible even under high load.

## Usage Example

```rust
use rustfs_obs::LogCleaner;
use rustfs_obs::types::FileMatchMode;
use std::path::PathBuf;

let cleaner = LogCleaner::builder(
    PathBuf::from("/var/log/rustfs"),
    "rustfs.log.".to_string(),
    "rustfs.log".to_string(),
)
.match_mode(FileMatchMode::Prefix)
.keep_files(10)
.max_total_size_bytes(1024 * 1024 * 100) // 100 MB
.compress_old_files(true)
.build();

// Run cleanup (blocking operation, spawn in a background task)
if let Ok((deleted, freed)) = cleaner.cleanup() {
    println!("Cleaned up {} files, freed {} bytes", deleted, freed);
}
```
