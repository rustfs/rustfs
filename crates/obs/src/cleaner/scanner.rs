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
use std::time::SystemTime;
use tracing::debug;

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
            debug!("Excluding file from cleanup: {:?}", filename);
            continue;
        }

        // 3. Classify file type and check pattern match.
        let matched_suffix = CompressionAlgorithm::compressed_suffixes()
            .into_iter()
            .find(|suffix| filename.ends_with(suffix));
        let is_compressed = matched_suffix.is_some();

        // Perform matching on the logical log filename.
        // Examples:
        // - regular log: `foo.log.1`   -> `foo.log.1`
        // - archive:     `foo.log.1.gz` -> `foo.log.1`
        // This allows the same include/exclude pattern configuration to apply
        // to both raw and already-compressed generations.
        let name_to_match = if let Some(suffix) = matched_suffix {
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

        // 5. Handle zero-byte files (regular logs only).
        // Empty compressed artifacts are left alone here because they belong
        // to the archive-retention path and should not disappear outside that
        // explicit policy.
        if !is_compressed && file_size == 0 && delete_empty_files {
            if !dry_run {
                if let Err(e) = fs::remove_file(&path) {
                    tracing::warn!("Failed to delete empty file {:?}: {}", path, e);
                } else {
                    debug!("Deleted empty file: {:?}", path);
                }
            } else {
                tracing::info!("[DRY RUN] Would delete empty file: {:?}", path);
            }
            continue;
        }

        // 6. Age gate regular logs only.
        // Compressed files deliberately bypass this check because archive
        // expiry is driven by a dedicated retention horizon in the caller.
        if !is_compressed
            && let Ok(age) = now.duration_since(modified)
            && age.as_secs() < min_file_age_seconds
        {
            // Too young to be touched.
            continue;
        }

        let info = FileInfo {
            path,
            size: file_size,
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
