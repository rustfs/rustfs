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
//! This module is intentionally kept read-only: it does **not** delete or
//! compress any files — it only reports what it found.

use super::types::{FileInfo, FileMatchMode};
use std::path::Path;
use std::time::{Duration, SystemTime};
use tracing::debug;
use walkdir::WalkDir;

/// Collect all log files in `log_dir` whose name matches `file_pattern` based on `match_mode`.
///
/// Files that:
/// - are already compressed (`.gz` extension),
/// - are zero-byte and `delete_empty_files` is `true` (these are handled
///   immediately by the caller), or
/// - match one of the `exclude_patterns`,
/// - were modified more recently than `min_file_age_seconds` seconds ago,
///
/// are skipped and not returned in the result list.
///
/// # Arguments
/// * `log_dir` - Root directory to scan (depth 1 only, no recursion).
/// * `file_pattern` - Pattern string to match filenames.
/// * `match_mode` - Whether to match by prefix or suffix.
/// * `exclude_patterns` - Compiled glob patterns; matching files are skipped.
/// * `min_file_age_seconds` - Files younger than this threshold are skipped.
/// * `delete_empty_files` - When `true`, zero-byte files trigger an immediate
///   delete by the caller before the rest of cleanup runs.
pub(super) fn collect_log_files(
    log_dir: &Path,
    file_pattern: &str,
    match_mode: FileMatchMode,
    exclude_patterns: &[glob::Pattern],
    min_file_age_seconds: u64,
    delete_empty_files: bool,
    dry_run: bool,
) -> Result<Vec<FileInfo>, std::io::Error> {
    let mut files = Vec::new();
    let now = SystemTime::now();

    for entry in WalkDir::new(log_dir)
        .max_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(f) => f,
            None => continue,
        };

        // Match filename based on mode
        let matches = match match_mode {
            FileMatchMode::Prefix => filename.starts_with(file_pattern),
            FileMatchMode::Suffix => filename.ends_with(file_pattern),
        };

        if !matches {
            continue;
        }

        // Compressed files are handled by collect_compressed_files.
        if filename.ends_with(".gz") {
            continue;
        }

        // Honour exclusion patterns.
        if is_excluded(filename, exclude_patterns) {
            debug!("Excluding file from cleanup: {:?}", filename);
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        let modified = match metadata.modified() {
            Ok(t) => t,
            Err(_) => continue,
        };
        let file_size = metadata.len();

        // Delete zero-byte files immediately (outside the normal selection
        // logic) when the feature is enabled.
        if file_size == 0 && delete_empty_files {
            if !dry_run {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!("Failed to delete empty file {:?}: {}", path, e);
                } else {
                    debug!("Deleted empty file: {:?}", path);
                }
            } else {
                tracing::info!("[DRY RUN] Would delete empty file: {:?}", path);
            }
            continue;
        }

        // Skip files that are too young.
        if let Ok(age) = now.duration_since(modified)
            && age.as_secs() < min_file_age_seconds
        {
            debug!(
                "Skipping file (too new): {:?}, age: {}s, min_age: {}s",
                filename,
                age.as_secs(),
                min_file_age_seconds
            );
            continue;
        }

        files.push(FileInfo {
            path: path.to_path_buf(),
            size: file_size,
            modified,
        });
    }

    Ok(files)
}

/// Collect compressed `.gz` log files whose age exceeds the retention period.
///
/// When `compressed_file_retention_days` is `0` the function returns immediately
/// without collecting anything (files are kept indefinitely).
///
/// # Arguments
/// * `log_dir` - Root directory to scan.
/// * `file_pattern` - Pattern string to match filenames.
/// * `match_mode` - Whether to match by prefix or suffix.
/// * `compressed_file_retention_days` - Files older than this are eligible for
///   deletion; `0` means never delete compressed files.
pub(super) fn collect_expired_compressed_files(
    log_dir: &Path,
    file_pattern: &str,
    match_mode: FileMatchMode,
    compressed_file_retention_days: u64,
) -> Result<Vec<FileInfo>, std::io::Error> {
    if compressed_file_retention_days == 0 {
        return Ok(Vec::new());
    }

    let retention = Duration::from_secs(compressed_file_retention_days * 24 * 3600);
    let now = SystemTime::now();
    let mut files = Vec::new();

    for entry in WalkDir::new(log_dir)
        .max_depth(1)
        .follow_links(false)
        .into_iter()
        .filter_map(|e| e.ok())
    {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }

        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(f) => f,
            None => continue,
        };

        if !filename.ends_with(".gz") {
            continue;
        }

        // Check if the base filename (without .gz) matches the pattern
        let base_filename = &filename[..filename.len() - 3];
        let matches = match match_mode {
            FileMatchMode::Prefix => base_filename.starts_with(file_pattern),
            FileMatchMode::Suffix => base_filename.ends_with(file_pattern),
        };

        if !matches {
            continue;
        }

        let Ok(metadata) = entry.metadata() else { continue };
        let Ok(modified) = metadata.modified() else { continue };
        let Ok(age) = now.duration_since(modified) else { continue };

        if age > retention {
            files.push(FileInfo {
                path: path.to_path_buf(),
                size: metadata.len(),
                modified,
            });
        }
    }

    Ok(files)
}

/// Returns `true` if `filename` matches any of the compiled exclusion patterns.
pub(super) fn is_excluded(filename: &str, patterns: &[glob::Pattern]) -> bool {
    patterns.iter().any(|p| p.matches(filename))
}
