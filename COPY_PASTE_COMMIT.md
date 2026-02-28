## QUICK COPY-PASTE COMMIT MESSAGE

```
feat(obs): add 6 advanced log cleanup configuration fields

## Summary
Enhanced rustfs-obs with new LogConfig fields for production-grade log lifecycle management.

## New Fields
- max_single_file_size_bytes: Individual file size limit
- compressed_file_retention_days: Retention period for .gz files
- exclude_patterns: Glob patterns for file protection
- delete_empty_files: Auto-cleanup zero-byte files
- min_file_age_seconds: Minimum age before cleanup
- dry_run: Test mode without deletion

## Changes
- Enhanced LogConfig struct with 6 optional fields
- Added glob pattern matching to LogCleaner
- Added compress file retention enforcement
- Updated all cleanup logic to support new features
- All tests updated and passing (14 unit + 10 doc)

## Quality
✅ Fully backward compatible (sensible defaults)
✅ No breaking changes
✅ Clippy clean (-D warnings)
✅ Code formatted

## Files
- crates/obs/src/log_config.rs
- crates/obs/src/log_cleanup.rs
- crates/obs/src/telemetry.rs
- crates/obs/Cargo.toml
```

---

## Alternative Versions

### For PR Title Only:
```
feat(obs): add 6 advanced log cleanup configuration fields
```

### For GitHub PR with Why section:
```markdown
## What
Added 6 advanced configuration fields to rustfs-obs LogConfig for production-grade log management.

## Why
Enable fine-grained control over log file lifecycle, flexible retention policies, and safe testing.

## How
- Enhanced LogConfig struct with 6 optional fields
- Implemented glob pattern matching and age-based filtering
- All new fields have sensible defaults for backward compatibility

## Testing
✅ 14 unit tests pass
✅ 10 doc tests pass
✅ Clippy clean (-D warnings)
✅ Code formatted

## Files Changed
- crates/obs/src/log_config.rs
- crates/obs/src/log_cleanup.rs
- crates/obs/src/telemetry.rs
- crates/obs/Cargo.toml
```

---

## Using with Git CLI:

```bash
git add .
git commit -m "feat(obs): add 6 advanced log cleanup configuration fields" \
  -m "Enhanced rustfs-obs with new LogConfig fields for production-grade log lifecycle management.

## New Fields
- max_single_file_size_bytes: Individual file size limit
- compressed_file_retention_days: Retention period for .gz files
- exclude_patterns: Glob patterns for file protection
- delete_empty_files: Auto-cleanup zero-byte files
- min_file_age_seconds: Minimum age before cleanup
- dry_run: Test mode without deletion

## Changes
- Enhanced LogConfig struct with 6 optional fields
- Added glob pattern matching to LogCleaner
- Added compress file retention enforcement
- Updated all cleanup logic to support new features
- All tests updated and passing (14 unit + 10 doc)

## Quality
✅ Fully backward compatible (sensible defaults)
✅ No breaking changes
✅ Clippy clean (-D warnings)
✅ Code formatted"
```

---

Done! Just copy and paste above. 🎉

