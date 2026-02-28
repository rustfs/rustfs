feat(obs): add advanced log cleanup configuration options

## What Changed
Enhanced rustfs-obs with 6 new LogConfig fields for production-grade log management:
- `max_single_file_size_bytes` - Limit individual file size
- `compressed_file_retention_days` - Auto-delete old .gz files
- `exclude_patterns` - Protect files via glob patterns
- `delete_empty_files` - Auto-cleanup zero-byte files
- `min_file_age_seconds` - Prevent accidental deletion of recent files
- `dry_run` - Test mode without actual deletion

## Why
Provides fine-grained control over log lifecycle, enabling:
- Flexible multi-level retention policies
- Safe file protection and testing capabilities
- Enterprise-grade storage management
- Better cost optimization for production systems

## How
- Updated `LogConfig` struct with 6 new optional fields (all have sensible defaults)
- Enhanced `LogCleaner` with glob pattern matching, age filtering, and dry-run support
- Added `is_excluded()` and `collect_compressed_files()` methods
- Glob patterns compiled once at construction for efficiency
- All existing tests updated and passing

## Compatibility
✅ Fully backward compatible - all new fields have default values

## Testing
- 14 unit tests pass
- 10 doc tests pass
- Clippy clean (no warnings with -D warnings)
- Code formatted with cargo fmt

## Files Changed
- crates/obs/src/log_config.rs (added 6 fields)
- crates/obs/src/log_cleanup.rs (enhanced cleanup logic)
- crates/obs/src/telemetry.rs (updated constructor call)
- crates/obs/Cargo.toml (added glob dependency)

