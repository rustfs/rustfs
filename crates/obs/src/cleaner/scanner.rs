// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Filesystem scanner for discovering log files eligible for cleanup.
//!
//! This module is primarily read-only: it reports what files it found.
//! The one exception is zero-byte file removal — when `delete_empty_files`
//! is enabled, `scan_log_directory` removes empty regular files as part of
//! the scan so that they are not counted in retention calculations.
//!
//! The scanner is also the first safety boundary of the cleaner pipeline. It
//! performs a shallow directory walk, rejects symlinks by relying on
//! `symlink_metadata`, and separates plain logs from pre-compressed archives so
//! later stages can apply different retention rules without rescanning.

use super::types::{CompressionAlgorithm, FileInfo, FileMatchMode};
use std::fs;
use std::path::Path;
use std::time::Duration;
use std::time::SystemTime;
use tracing::debug;

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_LOG_CLEANER: &str = "log_cleaner";
const EVENT_LOG_CLEANER_SCAN_STATE: &str = "log_cleaner_scan_state";

/// Result of a single pass directory scan.
///
/// Separating regular logs from compressed archives keeps the selection logic
/// straightforward: active retention limits apply to the former, while archive
/// expiry rules apply to the latter.
pub(super) struct LogScanResult {
    /// Regular log files eligible for deletion/compression.
    pub logs: Vec<FileInfo>,
    /// Already compressed files eligible for expiry deletion.
    pub compressed_archives: Vec<FileInfo>,
}

/// Perform a single-pass scan of the log directory.
///
/// This function iterates over the directory entries once and categorizes them
/// into regular logs or compressed archives based on extensions and patterns.
///
/// # Arguments
/// * `log_dir` - Root directory to scan (depth 1 only, no recursion).
/// * `file_pattern` - Pattern string to match filenames.
/// * `active_filename` - The name of the currently active log file (to be excluded).
/// * `match_mode` - Whether to match by prefix or suffix.
/// * `exclude_patterns` - Compiled glob patterns; matching files are skipped.
/// * `min_file_age_seconds` - Files younger than this threshold are skipped (for regular logs).
/// * `delete_empty_files` - When `true`, zero-byte regular files that match
///   the pattern are deleted immediately inside this function and excluded
///   from the returned [`LogScanResult`].
/// * `dry_run` - When `true`, destructive actions are logged but not executed.
#[allow(clippy::too_many_arguments)]
pub(super) fn scan_log_directory(
    log_dir: &Path,
    file_pattern: &str,
    active_filename: Option<&str>,
    match_mode: FileMatchMode,
    exclude_patterns: &[glob::Pattern],
    min_file_age_seconds: u64,
    delete_empty_files: bool,
    dry_run: bool,
) -> Result<LogScanResult, std::io::Error> {
    let file_pattern = file_pattern.trim();
    if file_pattern.is_empty() {
        return Ok(LogScanResult {
            logs: Vec::new(),
            compressed_archives: Vec::new(),
        });
    }

    let mut logs = Vec::new();
    let mut compressed_archives = Vec::new();
    let now = SystemTime::now();

    // Use read_dir for a lightweight, non-recursive scan.
    let entries = match fs::read_dir(log_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // If the log directory does not exist (or was removed), treat this
            // as "no files found" instead of failing the whole cleanup pass.
            return Ok(LogScanResult {
                logs,
                compressed_archives,
            });
        }
        Err(e) => return Err(e),
    };

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue, // Skip unreadable entries
        };

        let path = entry.path();

        // We only care about regular files inside the log directory.
        // Use `fs::symlink_metadata` (which does *not* follow symlinks) for
        // both the file-type check *and* size/mtime collection below.  Using
        // `entry.metadata()` or `Path::is_file()` (both of which follow
        // symlinks) would allow a symlink placed in the log directory to reach
        // files outside the tree, and would introduce a TOCTOU window between
        // the type-check and the metadata read.
        let metadata = match fs::symlink_metadata(&path) {
            Ok(md) => md,
            Err(_) => continue,
        };
        let file_type = metadata.file_type();
        if !file_type.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(f) => f,
            None => continue,
        };

        // 1. Explicitly skip the active log file (if known).
        if let Some(active) = active_filename
            && filename == active
        {
            continue;
        }

        // 2. Check exclusion patterns early.
        if is_excluded(filename, exclude_patterns) {
            debug!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "excluded", filename = %filename, "log cleaner scan state changed");
            continue;
        }

        // 3. Classify file type and check pattern match.
        let matched_suffix = CompressionAlgorithm::compressed_suffixes()
            .into_iter()
            .find(|suffix| filename.ends_with(suffix));
        let matched_tmp_suffix = compressed_tmp_suffix(filename);
        let is_compressed = matched_suffix.is_some();
        let is_tmp_archive = matched_tmp_suffix.is_some();

        // Perform matching on the logical log filename.
        // Examples:
        // - regular log: `foo.log.1`   -> `foo.log.1`
        // - archive:     `foo.log.1.gz` -> `foo.log.1`
        // - temp archive:`foo.log.1.gz.tmp` -> `foo.log.1`
        // This allows the same include/exclude pattern configuration to apply
        // to both raw and already-compressed generations.
        let name_to_match = if let Some(suffix) = matched_suffix {
            &filename[..filename.len() - suffix.len()]
        } else if let Some(suffix) = matched_tmp_suffix {
            &filename[..filename.len() - suffix.len()]
        } else {
            filename
        };

        let matches = match match_mode {
            FileMatchMode::Prefix => name_to_match.starts_with(file_pattern),
            FileMatchMode::Suffix => name_to_match.ends_with(file_pattern),
        };

        if !matches {
            continue;
        }

        // 4. Gather size and mtime from the already-fetched symlink_metadata
        // (reuse; no second syscall, no symlink following).
        let file_size = metadata.len();
        let modified = match metadata.modified() {
            Ok(t) => t,
            Err(_) => continue, // Skip files where we can't read modification time
        };
        let age = now.duration_since(modified).ok();

        if is_tmp_archive {
            if !is_old_enough_for_cleanup(age, min_file_age_seconds) {
                continue;
            }

            if !dry_run {
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, result = "tmp_archive_delete_failed", path = ?path, error = %e, "log cleaner scan state changed");
                } else {
                    debug!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "tmp_archive_deleted", path = ?path, "log cleaner scan state changed");
                }
            } else {
                tracing::info!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "dry_run_tmp_archive_delete", path = ?path, "log cleaner scan state changed");
            }
            continue;
        }

        // 5. Handle zero-byte files (regular logs only).
        // Empty compressed artifacts are left alone here because they belong
        // to the archive-retention path and should not disappear outside that
        // explicit policy.
        if !is_compressed && file_size == 0 && delete_empty_files {
            if !is_old_enough_for_cleanup(age, min_file_age_seconds) {
                continue;
            }
            if !dry_run {
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, result = "empty_file_delete_failed", path = ?path, error = %e, "log cleaner scan state changed");
                } else {
                    debug!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "empty_file_deleted", path = ?path, "log cleaner scan state changed");
                }
            } else {
                tracing::info!(event = EVENT_LOG_CLEANER_SCAN_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "dry_run_empty_file_delete", path = ?path, "log cleaner scan state changed");
            }
            continue;
        }

        // 6. Age gate regular logs only.
        // Compressed files deliberately bypass this check because archive
        // expiry is driven by a dedicated retention horizon in the caller.
        if !is_compressed && !is_old_enough_for_cleanup(age, min_file_age_seconds) {
            // Too young to be touched.
            continue;
        }

        let info = FileInfo {
            path,
            size: file_size,
            projected_freed_bytes: file_size,
            modified,
        };

        if is_compressed {
            compressed_archives.push(info);
        } else {
            logs.push(info);
        }
    }

    Ok(LogScanResult {
        logs,
        compressed_archives,
    })
}

/// Returns `true` if `filename` matches any of the compiled exclusion patterns.
pub(super) fn is_excluded(filename: &str, patterns: &[glob::Pattern]) -> bool {
    patterns.iter().any(|p| p.matches(filename))
}

fn compressed_tmp_suffix(filename: &str) -> Option<&'static str> {
    [".gz.tmp", ".zst.tmp"].into_iter().find(|suffix| filename.ends_with(suffix))
}

fn is_old_enough_for_cleanup(age: Option<Duration>, min_file_age_seconds: u64) -> bool {
    min_file_age_seconds == 0 || age.is_some_and(|age| age.as_secs() >= min_file_age_seconds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use tempfile::TempDir;

    fn create_empty_file(dir: &Path, name: &str) -> std::io::Result<()> {
        File::create(dir.join(name)).map(|_| ())
    }

    #[test]
    fn empty_pattern_matches_nothing() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path();
        create_empty_file(dir, "random.log")?;

        let result = scan_log_directory(dir, "", Some("active.log"), FileMatchMode::Suffix, &[], 0, true, false)?;

        assert!(result.logs.is_empty());
        assert!(result.compressed_archives.is_empty());
        Ok(())
    }

    #[test]
    fn fresh_empty_file_respects_min_file_age() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path();
        let filename = "2026-07-08.rustfs.log";
        create_empty_file(dir, filename)?;

        let result = scan_log_directory(dir, ".rustfs.log", Some("active.log"), FileMatchMode::Suffix, &[], 3600, true, false)?;

        assert!(dir.join(filename).exists());
        assert!(result.logs.is_empty());
        Ok(())
    }

    #[test]
    fn orphan_tmp_archive_is_deleted() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path();
        let filename = "2026-07-08.rustfs.log.gz.tmp";
        create_empty_file(dir, filename)?;

        let _ = scan_log_directory(dir, ".rustfs.log", Some("active.log"), FileMatchMode::Suffix, &[], 0, true, false)?;

        assert!(!dir.join(filename).exists());
        Ok(())
    }

    #[test]
    fn cleanup_age_gate_requires_known_old_enough_age() {
        assert!(!is_old_enough_for_cleanup(Some(Duration::from_secs(0)), 1));
        assert!(is_old_enough_for_cleanup(Some(Duration::from_secs(1)), 1));
        assert!(!is_old_enough_for_cleanup(None, 1));
        assert!(is_old_enough_for_cleanup(None, 0));
    }
}
