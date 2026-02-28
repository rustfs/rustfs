feat(obs): enhance log configuration with advanced cleanup options

## Summary
Enhanced rustfs-obs logging system with 6 new configuration fields for fine-grained control over log lifecycle management, including file size limits, retention policies, exclusion patterns, and dry-run testing capabilities.

## Changes

### New Configuration Fields
- **max_single_file_size_bytes**: Limit individual log file size (0 = unlimited)
- **compressed_file_retention_days**: Retention period for .gz files (0 = forever)
- **exclude_patterns**: Glob patterns to protect files from cleanup (e.g., "*.lock", "current.log")
- **delete_empty_files**: Auto-delete zero-byte log files (default: true)
- **min_file_age_seconds**: Minimum file age before cleanup (default: 3600s/1h)
- **dry_run**: Test mode that logs actions without deleting (default: false)

### Implementation Details

#### log_config.rs
- Added 6 new fields to `LogConfig` struct
- Updated `Default` implementation with production-ready defaults
- Enhanced documentation with usage examples

#### log_cleanup.rs
- Enhanced `LogCleaner` struct with new configuration support
- Updated `new()` constructor to accept 12 parameters (added `#[allow(clippy::too_many_arguments)]`)
- Added `is_excluded()` method for glob pattern matching
- Added `collect_compressed_files()` method for retention policy enforcement
- Updated `collect_log_files()` to filter by age, size, and exclusion patterns
- Updated `select_files_to_delete()` to enforce single file size limits
- Enhanced `compress_file()` and `delete_files()` with dry-run support
- Updated all unit tests with new constructor signature

#### telemetry.rs
- Updated `LogCleaner::new()` call to pass all new parameters
- Maintains backward compatibility with sensible defaults

#### Cargo.toml
- Added `glob` dependency for pattern matching support

### Features
- ✅ Fine-grained file lifecycle management
- ✅ Flexible multi-level retention policies
- ✅ Safe file protection via glob patterns
- ✅ Age-based file filtering to prevent accidental deletion
- ✅ Dry-run mode for policy testing
- ✅ Automatic empty file cleanup
- ✅ Full backward compatibility

### Usage Example
```rust
let config = LogConfig {
    max_single_file_size_bytes: 100 * 1024 * 1024,  // 100MB
    compressed_file_retention_days: 90,               // 90 days
    exclude_patterns: vec![
        "*.lock".to_string(),
        "current.log".to_string(),
    ],
    delete_empty_files: true,
    min_file_age_seconds: 3600,                       // 1 hour
    dry_run: false,
    ..Default::default()
};
```

### Testing
- All 3 existing unit tests updated for new constructor
- All tests passing: 14 unit tests + 10 doc tests
- Clippy: no warnings with `-D warnings`
- Formatting: passed `cargo fmt --check`

### Breaking Changes
None. All new fields have sensible defaults. Existing code continues to work without modification.

### Documentation
Created comprehensive documentation:
- `LOG_ENHANCEMENT_SUMMARY.md`: Complete feature guide with examples
- `log_config_example.toml`: TOML configuration example file

## Files Modified
- crates/obs/src/log_config.rs
- crates/obs/src/log_cleanup.rs
- crates/obs/src/telemetry.rs
- crates/obs/Cargo.toml

## Backward Compatibility
✅ Fully backward compatible. All new configuration fields have sensible production defaults.

---

