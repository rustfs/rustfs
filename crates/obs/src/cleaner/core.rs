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
//! Construct it with [`LogCleaner::new`] and call [`LogCleaner::cleanup`]
//! periodically (e.g. from a `tokio::spawn`-ed loop).
//!
//! Internally the cleaner delegates to:
//! - [`super::scanner`] — to discover which files exist and which are eligible,
//! - [`super::compress`] — to gzip-compress files before they are deleted,
//! - [`LogCleaner::select_files_to_delete`] — to apply count / size limits.

use super::compress::compress_file;
use super::scanner::{collect_expired_compressed_files, collect_log_files};
use super::types::{FileInfo, FileMatchMode};
use std::path::PathBuf;
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
    /// Whether to match by prefix or suffix.
    pub(super) match_mode: FileMatchMode,
    /// The cleaner will never delete files if doing so would leave fewer than
    /// this many files in the directory.
    pub(super) keep_count: usize,
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
    /// Build a new [`LogCleaner`] with the supplied policy parameters.
    ///
    /// `exclude_patterns` is a list of glob strings (e.g. `"*.lock"`).  Invalid
    /// glob patterns are silently ignored.
    ///
    /// `gzip_compression_level` is clamped to the range `[1, 9]`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_dir: PathBuf,
        file_pattern: String,
        match_mode: FileMatchMode,
        keep_count: usize,
        max_total_size_bytes: u64,
        max_single_file_size_bytes: u64,
        compress_old_files: bool,
        gzip_compression_level: u32,
        compressed_file_retention_days: u64,
        exclude_patterns: Vec<String>,
        delete_empty_files: bool,
        min_file_age_seconds: u64,
        dry_run: bool,
    ) -> Self {
        let patterns = exclude_patterns
            .into_iter()
            .filter_map(|p| glob::Pattern::new(&p).ok())
            .collect();

        Self {
            log_dir,
            file_pattern,
            match_mode,
            keep_count,
            max_total_size_bytes,
            max_single_file_size_bytes,
            compress_old_files,
            gzip_compression_level: gzip_compression_level.clamp(1, 9),
            compressed_file_retention_days,
            exclude_patterns: patterns,
            delete_empty_files,
            min_file_age_seconds,
            dry_run,
        }
    }

    /// Perform one full cleanup pass.
    ///
    /// Steps:
    /// 1. Scan the log directory for managed files.
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

        // ── 1. Discover active log files ──────────────────────────────────────
        let mut files = collect_log_files(
            &self.log_dir,
            &self.file_pattern,
            self.match_mode,
            &self.exclude_patterns,
            self.min_file_age_seconds,
            self.delete_empty_files,
            self.dry_run,
        )?;

        if files.is_empty() {
            debug!("No log files found in directory: {:?}", self.log_dir);
        } else {
            files.sort_by_key(|f| f.modified);
            let total_size: u64 = files.iter().map(|f| f.size).sum();
            info!(
                "Found {} log files, total size: {} bytes ({:.2} MB)",
                files.len(),
                total_size,
                total_size as f64 / 1024.0 / 1024.0
            );

            // ── 2. Select + compress + delete ─────────────────────────────────
            let (to_delete, to_rotate) = self.select_files_to_process(&files, total_size);

            // Handle rotation for active file if needed
            if let Some(active_file) = to_rotate
                && let Err(e) = self.rotate_active_file(&active_file)
            {
                error!("Failed to rotate active file {:?}: {}", active_file.path, e);
            }

            if !to_delete.is_empty() {
                let (d, f) = self.compress_and_delete(&to_delete)?;
                total_deleted += d;
                total_freed += f;
            }
        }

        // ── 3. Remove expired compressed archives ─────────────────────────────
        let expired_gz = collect_expired_compressed_files(
            &self.log_dir,
            &self.file_pattern,
            self.match_mode,
            self.compressed_file_retention_days,
        )?;
        if !expired_gz.is_empty() {
            let (d, f) = self.delete_files(&expired_gz)?;
            total_deleted += d;
            total_freed += f;
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

    /// Choose which files from `files` (sorted oldest-first) should be deleted or rotated.
    ///
    /// The algorithm respects three constraints in order:
    /// 1. Always keep at least `keep_count` files.
    /// 2. Delete old files while the total size exceeds `max_total_size_bytes`.
    /// 3. Delete any file whose individual size exceeds `max_single_file_size_bytes`.
    ///
    /// **Note**: The most recent file (assumed to be the active log) is exempt
    /// from size-based deletion. If it exceeds the size limit, it is returned
    /// as `to_rotate`.
    pub(super) fn select_files_to_process(&self, files: &[FileInfo], total_size: u64) -> (Vec<FileInfo>, Option<FileInfo>) {
        let mut to_delete = Vec::new();
        let mut to_rotate = None;

        if files.is_empty() {
            return (to_delete, to_rotate);
        }

        // Identify the index of the most recent file (last in the sorted list).
        // We will protect this file from size-based deletion.
        let active_file_idx = files.len() - 1;

        // The number of files we are allowed to delete.
        // Any file with index >= max_deletable_count is protected by keep_count.
        let max_deletable_count = files.len().saturating_sub(self.keep_count);

        let mut current_size = total_size;

        for (idx, file) in files.iter().enumerate() {
            // If we are in the protected range, we stop deleting.
            if idx >= max_deletable_count {
                // However, if the active file is too large, we might rotate it.
                if idx == active_file_idx {
                    let over_single = self.max_single_file_size_bytes > 0 && file.size > self.max_single_file_size_bytes;
                    if over_single {
                        to_rotate = Some(file.clone());
                    }
                }
                continue;
            }

            // We are in the deletable range. Check if we *should* delete.

            // Condition 2: Enforce max_total_size_bytes.
            let over_total = self.max_total_size_bytes > 0 && current_size > self.max_total_size_bytes;

            // Condition 3: Enforce max_single_file_size_bytes.
            let over_single = self.max_single_file_size_bytes > 0 && file.size > self.max_single_file_size_bytes;

            if over_total {
                // If we are over total size, we delete unless it's the active file.
                if idx == active_file_idx {
                    debug!(
                        "Active log file contributes to total size limit overflow, but skipping deletion to preserve current logs."
                    );
                } else {
                    current_size = current_size.saturating_sub(file.size);
                    to_delete.push(file.clone());
                }
            } else if over_single {
                // For single file limits, we MUST NOT delete the active file.
                if idx == active_file_idx {
                    // Mark active file for rotation instead of deletion
                    to_rotate = Some(file.clone());
                } else {
                    debug!(
                        "File exceeds single-file size limit: {:?} ({} > {} bytes)",
                        file.path, file.size, self.max_single_file_size_bytes
                    );
                    current_size = current_size.saturating_sub(file.size);
                    to_delete.push(file.clone());
                }
            }
        }

        (to_delete, to_rotate)
    }

    // ─── Rotation ─────────────────────────────────────────────────────────────

    /// Rotate the active file by renaming it with a timestamp suffix.
    /// The original filename will be recreated by the logging appender on next write.
    fn rotate_active_file(&self, file: &FileInfo) -> Result<(), std::io::Error> {
        if self.dry_run {
            info!("[DRY RUN] Would rotate active file: {:?} ({} bytes)", file.path, file.size);
            return Ok(());
        }

        // Generate timestamp: unix timestamp in seconds
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(std::io::Error::other)?
            .as_secs();

        let file_name = file
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid filename"))?;

        // Construct the rotated filename.
        // We must ensure the new filename still matches the file_pattern so it can be discovered
        // by the scanner in future runs (and eventually deleted).
        //
        // Suffix mode: Insert timestamp BEFORE the suffix.
        //   Example: "2026-03-01.rustfs.log" (pattern="rustfs.log")
        //   -> "2026-03-01.1740810000.rustfs.log"
        //
        // Prefix mode: Append timestamp at the end.
        //   Example: "app.log" (pattern="app")
        //   -> "app.log.1740810000"
        let rotated_name = match self.match_mode {
            FileMatchMode::Suffix => {
                if let Some(base) = file_name.strip_suffix(&self.file_pattern) {
                    let mut new_name = String::with_capacity(file_name.len() + 20);
                    new_name.push_str(base);

                    // Ensure separator between base and timestamp
                    if !base.is_empty() && !base.ends_with('.') {
                        new_name.push('.');
                    }
                    new_name.push_str(&timestamp.to_string());

                    // Ensure separator between timestamp and suffix
                    if !self.file_pattern.starts_with('.') {
                        new_name.push('.');
                    }
                    new_name.push_str(&self.file_pattern);

                    new_name
                } else {
                    // Should not happen if scanner works correctly, but fallback safely
                    format!("{}.{}", file_name, timestamp)
                }
            }
            FileMatchMode::Prefix => {
                // For prefix matching, appending to the end preserves the prefix.
                format!("{}.{}", file_name, timestamp)
            }
        };

        let rotated_path = file.path.with_file_name(&rotated_name);

        // Check if target already exists to avoid overwriting (unlikely with timestamp but possible)
        if rotated_path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("Rotated file already exists: {:?}", rotated_path),
            ));
        }

        info!("Rotating active log file: {:?} -> {:?}", file.path, rotated_path);

        // Rename the current active file to the rotated name.
        // The logging appender (tracing-appender) will automatically create a new file
        // with the original name when it next attempts to write.
        // Note: On Linux/Unix, this rename is atomic and safe even if the file is open.
        if let Err(e) = std::fs::rename(&file.path, &rotated_path) {
            // Add context to the error
            return Err(std::io::Error::new(
                e.kind(),
                format!("Failed to rename {:?} to {:?}: {}", file.path, rotated_path, e),
            ));
        }

        Ok(())
    }

    // ─── Compression + deletion ───────────────────────────────────────────────

    /// Optionally compress and then delete the given files.
    ///
    /// This function is synchronous and blocking. It should be called within a
    /// `spawn_blocking` task if running in an async context.
    fn compress_and_delete(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        if self.compress_old_files {
            for f in files {
                if let Err(e) = compress_file(&f.path, self.gzip_compression_level, self.dry_run) {
                    tracing::warn!("Failed to compress {:?}: {}", f.path, e);
                }
            }
        }
        self.delete_files(files)
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
