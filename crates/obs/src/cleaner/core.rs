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

//! Core log-file cleanup orchestration.
//!
//! [`LogCleaner`] is the public entry point for the cleanup subsystem.
//! Construct it with [`LogCleaner::builder`] and call [`LogCleaner::cleanup`]
//! periodically (e.g. from a `tokio::spawn`-ed loop).
//!
//! Internally the cleaner delegates to:
//! - [`super::scanner`] — to discover which files exist and which are eligible,
//! - [`super::compress`] — to gzip-compress files before they are deleted,
//! - [`LogCleaner::select_files_to_delete`] — to apply count / size limits.

use super::compress::compress_file;
use super::scanner::{LogScanResult, scan_log_directory};
use super::types::{FileInfo, FileMatchMode};
use rustfs_config::DEFAULT_LOG_KEEP_FILES;
use std::path::PathBuf;
use std::time::SystemTime;
use tracing::{debug, error, info};

/// Log-file lifecycle manager.
///
/// Holds all cleanup policy parameters and exposes a single [`cleanup`] method
/// that performs one full cleanup pass.
///
/// # Thread-safety
/// `LogCleaner` is `Send + Sync`.  Multiple callers can share a reference
/// (e.g. via `Arc`) and call `cleanup` concurrently without data races,
/// because no mutable state is mutated after construction.
pub struct LogCleaner {
    /// Directory containing the managed log files.
    pub(super) log_dir: PathBuf,
    /// Pattern string to match files (used as prefix or suffix).
    pub(super) file_pattern: String,
    /// Exact name of the active log file (to exclude from cleanup).
    pub(super) active_filename: String,
    /// Whether to match by prefix or suffix.
    pub(super) match_mode: FileMatchMode,
    /// The cleaner will never delete files if doing so would leave fewer than
    /// this many files in the directory.
    pub(super) keep_files: usize,
    /// Hard ceiling on the total bytes of all managed files; `0` = no limit.
    pub(super) max_total_size_bytes: u64,
    /// Hard ceiling on a single file's size; `0` = no per-file limit.
    pub(super) max_single_file_size_bytes: u64,
    /// Compress eligible files with gzip before removing them.
    pub(super) compress_old_files: bool,
    /// Gzip compression level (`1`–`9`, clamped on construction).
    pub(super) gzip_compression_level: u32,
    /// Delete compressed archives older than this many days; `0` = keep forever.
    pub(super) compressed_file_retention_days: u64,
    /// Compiled glob patterns for files that must never be cleaned up.
    pub(super) exclude_patterns: Vec<glob::Pattern>,
    /// Delete zero-byte files even when they are younger than `min_file_age_seconds`.
    pub(super) delete_empty_files: bool,
    /// Files younger than this threshold (in seconds) are never touched.
    pub(super) min_file_age_seconds: u64,
    /// When `true`, log what would be done without performing any destructive
    /// filesystem operations.
    pub(super) dry_run: bool,
}

impl LogCleaner {
    /// Create a builder to construct a `LogCleaner`.
    pub fn builder(
        log_dir: impl Into<PathBuf>,
        file_pattern: impl Into<String>,
        active_filename: impl Into<String>,
    ) -> LogCleanerBuilder {
        LogCleanerBuilder::new(log_dir, file_pattern, active_filename)
    }

    /// Perform one full cleanup pass.
    ///
    /// Steps:
    /// 1. Scan the log directory for managed files (excluding the active file).
    /// 2. Apply count/size policies to select files for deletion.
    /// 3. Optionally compress selected files, then delete them.
    /// 4. Collect and delete expired compressed archives.
    ///
    /// # Returns
    /// A tuple `(deleted_count, freed_bytes)` covering all deletions in this
    /// pass (both regular files and expired compressed archives).
    ///
    /// # Errors
    /// Returns an [`std::io::Error`] if the log directory cannot be read.
    pub fn cleanup(&self) -> Result<(usize, u64), std::io::Error> {
        if !self.log_dir.exists() {
            debug!("Log directory does not exist: {:?}", self.log_dir);
            return Ok((0, 0));
        }

        let mut total_deleted = 0usize;
        let mut total_freed = 0u64;

        // ── 1. Discover active log files (Archives only) ──────────────────────
        // We explicitly pass `active_filename` to exclude it from the list.
        let LogScanResult {
            mut logs,
            mut compressed_archives,
        } = scan_log_directory(
            &self.log_dir,
            &self.file_pattern,
            Some(&self.active_filename),
            self.match_mode,
            &self.exclude_patterns,
            self.min_file_age_seconds,
            self.delete_empty_files,
            self.dry_run,
        )?;

        // ── 2. Select + compress + delete (Regular Logs) ──────────────────────
        if !logs.is_empty() {
            logs.sort_by_key(|f| f.modified);
            let total_size: u64 = logs.iter().map(|f| f.size).sum();

            info!(
                "Found {} regular log files, total size: {} bytes ({:.2} MB)",
                logs.len(),
                total_size,
                total_size as f64 / 1024.0 / 1024.0
            );

            let to_delete = self.select_files_to_process(&logs, total_size);

            if !to_delete.is_empty() {
                let (d, f) = self.compress_and_delete(&to_delete)?;
                total_deleted += d;
                total_freed += f;
            }
        }

        // ── 3. Remove expired compressed archives ─────────────────────────────
        if !compressed_archives.is_empty() && self.compressed_file_retention_days > 0 {
            let expired = self.select_expired_compressed(&mut compressed_archives);
            if !expired.is_empty() {
                let (d, f) = self.delete_files(&expired)?;
                total_deleted += d;
                total_freed += f;
            }
        }

        if total_deleted > 0 || total_freed > 0 {
            info!(
                "Cleanup completed: deleted {} files, freed {} bytes ({:.2} MB)",
                total_deleted,
                total_freed,
                total_freed as f64 / 1024.0 / 1024.0
            );
        }

        Ok((total_deleted, total_freed))
    }

    // ─── Selection ────────────────────────────────────────────────────────────

    /// Choose which files from `files` (sorted oldest-first) should be deleted.
    ///
    /// The algorithm respects three constraints in order:
    /// 1. Always keep at least `keep_files` files (archives).
    /// 2. Delete old files while the total size exceeds `max_total_size_bytes`.
    /// 3. Delete any file whose individual size exceeds `max_single_file_size_bytes`.
    pub(super) fn select_files_to_process(&self, files: &[FileInfo], total_size: u64) -> Vec<FileInfo> {
        let mut to_delete = Vec::new();

        if files.is_empty() {
            return to_delete;
        }

        // Calculate how many files we *must* delete to satisfy keep_files.
        let must_delete_count = files.len().saturating_sub(self.keep_files);

        let mut current_size = total_size;

        for (idx, file) in files.iter().enumerate() {
            // Condition 1: Enforce keep_files.
            // If we are in the range of files that exceed the count limit, delete them.
            if idx < must_delete_count {
                current_size = current_size.saturating_sub(file.size);
                to_delete.push(file.clone());
                continue;
            }

            // Condition 2: Enforce max_total_size_bytes.
            let over_total = self.max_total_size_bytes > 0 && current_size > self.max_total_size_bytes;

            // Condition 3: Enforce max_single_file_size_bytes.
            // Note: Since active file is excluded, if an archive is > max_single, it means it
            // was rotated out being too large (likely) or we lowered the limit. It should be deleted.
            let over_single = self.max_single_file_size_bytes > 0 && file.size > self.max_single_file_size_bytes;

            if over_total {
                current_size = current_size.saturating_sub(file.size);
                to_delete.push(file.clone());
            } else if over_single {
                debug!(
                    "Archive exceeds single-file size limit: {:?} ({} > {} bytes). Deleting.",
                    file.path, file.size, self.max_single_file_size_bytes
                );
                current_size = current_size.saturating_sub(file.size);
                to_delete.push(file.clone());
            }
        }

        to_delete
    }

    /// Select compressed files that have exceeded the retention period.
    fn select_expired_compressed(&self, files: &mut [FileInfo]) -> Vec<FileInfo> {
        let retention = std::time::Duration::from_secs(self.compressed_file_retention_days * 24 * 3600);
        let now = SystemTime::now();
        let mut expired = Vec::new();

        for file in files {
            if let Ok(age) = now.duration_since(file.modified)
                && age > retention
            {
                expired.push(file.clone());
            }
        }

        expired
    }

    // ─── Compression + deletion ───────────────────────────────────────────────

    /// Optionally compress and then delete the given files.
    ///
    /// This function is synchronous and blocking. It should be called within a
    /// `spawn_blocking` task if running in an async context.
    fn compress_and_delete(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        let mut total_deleted = 0;
        let mut total_freed = 0;

        for f in files {
            let mut deleted_size = 0;
            if self.compress_old_files {
                match compress_file(&f.path, self.gzip_compression_level, self.dry_run) {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!("Failed to compress {:?}: {}", f.path, e);
                    }
                }
            }

            // Now delete
            if self.dry_run {
                info!("[DRY RUN] Would delete: {:?} ({} bytes)", f.path, f.size);
                deleted_size = f.size;
            } else {
                match std::fs::remove_file(&f.path) {
                    Ok(()) => {
                        debug!("Deleted: {:?}", f.path);
                        deleted_size = f.size;
                    }
                    Err(e) => {
                        error!("Failed to delete {:?}: {}", f.path, e);
                    }
                }
            }
            if deleted_size > 0 {
                total_deleted += 1;
                total_freed += deleted_size;
            }
        }

        Ok((total_deleted, total_freed))
    }

    /// Delete all files in `files`, logging each operation.
    ///
    /// Errors on individual files are logged but do **not** abort the loop.
    ///
    /// # Returns
    /// `(deleted_count, freed_bytes)`.
    pub(super) fn delete_files(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        let mut deleted = 0usize;
        let mut freed = 0u64;

        for f in files {
            if self.dry_run {
                info!("[DRY RUN] Would delete: {:?} ({} bytes)", f.path, f.size);
                deleted += 1;
                freed += f.size;
            } else {
                match std::fs::remove_file(&f.path) {
                    Ok(()) => {
                        debug!("Deleted: {:?}", f.path);
                        deleted += 1;
                        freed += f.size;
                    }
                    Err(e) => {
                        error!("Failed to delete {:?}: {}", f.path, e);
                    }
                }
            }
        }

        Ok((deleted, freed))
    }
}

/// Builder for [`LogCleaner`].
pub struct LogCleanerBuilder {
    log_dir: PathBuf,
    file_pattern: String,
    active_filename: String,
    match_mode: FileMatchMode,
    keep_files: usize,
    max_total_size_bytes: u64,
    max_single_file_size_bytes: u64,
    compress_old_files: bool,
    gzip_compression_level: u32,
    compressed_file_retention_days: u64,
    exclude_patterns: Vec<String>,
    delete_empty_files: bool,
    min_file_age_seconds: u64,
    dry_run: bool,
}

impl LogCleanerBuilder {
    pub fn new(log_dir: impl Into<PathBuf>, file_pattern: impl Into<String>, active_filename: impl Into<String>) -> Self {
        Self {
            log_dir: log_dir.into(),
            file_pattern: file_pattern.into(),
            active_filename: active_filename.into(),
            match_mode: FileMatchMode::Prefix,
            // Default to a safe non-zero value so that a builder created
            // without an explicit `keep_files()` call does not immediately
            // delete all matching log files.
            keep_files: DEFAULT_LOG_KEEP_FILES,
            max_total_size_bytes: 0,
            max_single_file_size_bytes: 0,
            compress_old_files: false,
            gzip_compression_level: 6,
            compressed_file_retention_days: 0,
            exclude_patterns: Vec::new(),
            delete_empty_files: false,
            min_file_age_seconds: 0,
            dry_run: false,
        }
    }

    pub fn match_mode(mut self, match_mode: FileMatchMode) -> Self {
        self.match_mode = match_mode;
        self
    }

    pub fn keep_files(mut self, keep_files: usize) -> Self {
        self.keep_files = keep_files;
        self
    }

    pub fn max_total_size_bytes(mut self, max_total_size_bytes: u64) -> Self {
        self.max_total_size_bytes = max_total_size_bytes;
        self
    }

    pub fn max_single_file_size_bytes(mut self, max_single_file_size_bytes: u64) -> Self {
        self.max_single_file_size_bytes = max_single_file_size_bytes;
        self
    }

    pub fn compress_old_files(mut self, compress_old_files: bool) -> Self {
        self.compress_old_files = compress_old_files;
        self
    }

    pub fn gzip_compression_level(mut self, gzip_compression_level: u32) -> Self {
        self.gzip_compression_level = gzip_compression_level;
        self
    }

    pub fn compressed_file_retention_days(mut self, days: u64) -> Self {
        self.compressed_file_retention_days = days;
        self
    }

    pub fn exclude_patterns(mut self, patterns: Vec<String>) -> Self {
        self.exclude_patterns = patterns;
        self
    }

    pub fn delete_empty_files(mut self, delete_empty_files: bool) -> Self {
        self.delete_empty_files = delete_empty_files;
        self
    }

    pub fn min_file_age_seconds(mut self, seconds: u64) -> Self {
        self.min_file_age_seconds = seconds;
        self
    }

    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn build(self) -> LogCleaner {
        let patterns = self
            .exclude_patterns
            .into_iter()
            .filter_map(|p| glob::Pattern::new(&p).ok())
            .collect();

        LogCleaner {
            log_dir: self.log_dir,
            file_pattern: self.file_pattern,
            active_filename: self.active_filename,
            match_mode: self.match_mode,
            keep_files: self.keep_files,
            max_total_size_bytes: self.max_total_size_bytes,
            max_single_file_size_bytes: self.max_single_file_size_bytes,
            compress_old_files: self.compress_old_files,
            gzip_compression_level: self.gzip_compression_level.clamp(1, 9),
            compressed_file_retention_days: self.compressed_file_retention_days,
            exclude_patterns: patterns,
            delete_empty_files: self.delete_empty_files,
            min_file_age_seconds: self.min_file_age_seconds,
            dry_run: self.dry_run,
        }
    }
}
