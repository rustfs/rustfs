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
//! This module connects scanning, retention selection, compression, and safe
//! deletion into one reusable service object. The public surface is intentionally
//! small: callers configure a [`LogCleaner`] once and then trigger discrete
//! cleanup passes whenever log rotation or background maintenance requires it.

use super::compress::{CompressionOptions, compress_file};
use super::scanner::{LogScanResult, scan_log_directory};
use super::types::{CompressionAlgorithm, FileInfo, FileMatchMode, default_parallel_workers};
use crate::global::{
    METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS, METRIC_LOG_CLEANER_DELETED_FILES_TOTAL, METRIC_LOG_CLEANER_FREED_BYTES_TOTAL,
    METRIC_LOG_CLEANER_STEAL_SUCCESS_RATE,
};
use crossbeam_channel::bounded;
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_utils::thread;
use metrics::{counter, gauge, histogram};
use rustfs_config::DEFAULT_LOG_KEEP_FILES;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

#[derive(Debug)]
struct CompressionTaskResult {
    /// Original file metadata so successful workers can be deleted later.
    file: FileInfo,
    /// Whether compression completed successfully for this file.
    compressed: bool,
}

/// Log-file lifecycle manager.
///
/// A cleaner instance is immutable after construction and therefore safe to
/// reuse across periodic background jobs. Each call to [`LogCleaner::cleanup`]
/// performs a fresh directory scan and applies the configured retention rules.
pub struct LogCleaner {
    /// Directory containing the active and rotated log files.
    pub(super) log_dir: PathBuf,
    /// Pattern used to recognize relevant log generations.
    pub(super) file_pattern: String,
    /// The currently active log file that must never be touched.
    pub(super) active_filename: String,
    /// Whether `file_pattern` is interpreted as a prefix or suffix.
    pub(super) match_mode: FileMatchMode,
    /// Minimum number of regular log files to keep regardless of size.
    pub(super) keep_files: usize,
    /// Optional cap for the cumulative size of regular logs.
    pub(super) max_total_size_bytes: u64,
    /// Optional cap for an individual regular log file.
    pub(super) max_single_file_size_bytes: u64,
    /// Whether selected regular logs should be compressed before deletion.
    pub(super) compress_old_files: bool,
    /// Gzip compression level used when gzip is selected or used as fallback.
    pub(super) gzip_compression_level: u32,
    /// Retention window for already compressed archives, expressed in days.
    pub(super) compressed_file_retention_days: u64,
    /// Glob patterns that are excluded before any cleanup decision is made.
    pub(super) exclude_patterns: Vec<glob::Pattern>,
    /// Whether zero-byte regular logs may be removed during scanning.
    pub(super) delete_empty_files: bool,
    /// Minimum age a regular log must reach before it becomes eligible.
    pub(super) min_file_age_seconds: u64,
    /// Dry-run mode reports intended actions without modifying files.
    pub(super) dry_run: bool,
    // Parallel compression controls while keeping backward compatibility with
    // the original serial cleaner behavior.
    /// Preferred archive codec for compression-enabled cleanup passes.
    pub(super) compression_algorithm: CompressionAlgorithm,
    /// Enables the work-stealing compression path when compression is active.
    pub(super) parallel_compress: bool,
    /// Number of worker threads used by the parallel compressor.
    pub(super) parallel_workers: usize,
    /// Zstd compression level when zstd is selected.
    pub(super) zstd_compression_level: i32,
    /// Whether a failed zstd attempt should retry with gzip.
    pub(super) zstd_fallback_to_gzip: bool,
    /// Number of internal threads requested from the zstd encoder.
    pub(super) zstd_workers: usize,
}

impl LogCleaner {
    /// Create a builder with the required path and filename matching inputs.
    pub fn builder(
        log_dir: impl Into<PathBuf>,
        file_pattern: impl Into<String>,
        active_filename: impl Into<String>,
    ) -> LogCleanerBuilder {
        LogCleanerBuilder::new(log_dir, file_pattern, active_filename)
    }

    /// Perform one full cleanup pass.
    pub fn cleanup(&self) -> Result<(usize, u64), std::io::Error> {
        if !self.log_dir.exists() {
            debug!("Log directory does not exist: {:?}", self.log_dir);
            return Ok((0, 0));
        }

        let mut total_deleted = 0usize;
        let mut total_freed = 0u64;

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

        if !logs.is_empty() {
            logs.sort_by_key(|f| f.modified);
            let total_size: u64 = logs.iter().map(|f| f.size).sum();

            info!(
                "Found {} regular log files, total size: {} bytes ({:.2} MB)",
                logs.len(),
                total_size,
                total_size as f64 / 1024.0 / 1024.0
            );

            // Select the oldest files first, then additionally trim any files
            // that still violate configured size constraints.
            let to_delete = self.select_files_to_process(&logs, total_size);
            if !to_delete.is_empty() {
                let (deleted, freed) = if self.parallel_compress && self.compress_old_files {
                    self.parallel_stealing_compress(&to_delete)?
                } else {
                    self.serial_compress_and_delete(&to_delete)?
                };
                total_deleted += deleted;
                total_freed += freed;
            }
        }

        if !compressed_archives.is_empty() && self.compressed_file_retention_days > 0 {
            let expired = self.select_expired_compressed(&mut compressed_archives);
            if !expired.is_empty() {
                let (d, f) = self.delete_files(&expired)?;
                total_deleted += d;
                total_freed += f;
            }
        }

        if total_deleted > 0 || total_freed > 0 {
            counter!(METRIC_LOG_CLEANER_DELETED_FILES_TOTAL).increment(total_deleted as u64);
            counter!(METRIC_LOG_CLEANER_FREED_BYTES_TOTAL).increment(total_freed);
            info!(
                "Cleanup completed: deleted {} files, freed {} bytes ({:.2} MB)",
                total_deleted,
                total_freed,
                total_freed as f64 / 1024.0 / 1024.0
            );
        }

        Ok((total_deleted, total_freed))
    }

    /// Choose regular log files that should be compressed and/or deleted.
    ///
    /// The `files` slice must already be sorted from oldest to newest. The
    /// method first preserves the newest `keep_files` generations, then applies
    /// total-size and per-file-size limits to the remaining tail.
    pub(super) fn select_files_to_process(&self, files: &[FileInfo], total_size: u64) -> Vec<FileInfo> {
        let mut to_delete = Vec::new();
        if files.is_empty() {
            return to_delete;
        }

        let must_delete_count = files.len().saturating_sub(self.keep_files);
        let mut current_size = total_size;

        for (idx, file) in files.iter().enumerate() {
            if idx < must_delete_count {
                current_size = current_size.saturating_sub(file.size);
                to_delete.push(file.clone());
                continue;
            }

            let over_total = self.max_total_size_bytes > 0 && current_size > self.max_total_size_bytes;
            let over_single = self.max_single_file_size_bytes > 0 && file.size > self.max_single_file_size_bytes;

            if over_total || over_single {
                current_size = current_size.saturating_sub(file.size);
                to_delete.push(file.clone());
            }
        }

        to_delete
    }

    /// Select compressed archives whose age exceeds the archive retention window.
    fn select_expired_compressed(&self, files: &mut [FileInfo]) -> Vec<FileInfo> {
        let retention = Duration::from_secs(self.compressed_file_retention_days * 24 * 3600);
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

    /// Parallel compressor with work stealing.
    ///
    /// The flow is intentionally split into "parallel compression" followed by
    /// "serial deletion" to reduce cross-platform file-locking failures.
    /// Compression workers only decide whether an archive was created; the main
    /// thread remains responsible for actual source removal so deletion policy
    /// and error reporting stay deterministic.
    fn parallel_stealing_compress(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        if files.len() <= 1 {
            return self.serial_compress_and_delete(files);
        }

        let worker_count = self.parallel_workers.min(files.len()).max(1);
        if worker_count <= 1 {
            return self.serial_compress_and_delete(files);
        }

        let compression_options = self.compression_options();
        let started_at = Instant::now();
        let injector = Arc::new(Injector::new());
        for file in files {
            injector.push(file.clone());
        }

        let mut workers = Vec::with_capacity(worker_count);
        let mut stealers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let worker = Worker::new_fifo();
            stealers.push(worker.stealer());
            workers.push(worker);
        }

        let stealers = Arc::new(stealers);
        let steal_attempts = Arc::new(AtomicU64::new(0));
        let steal_successes = Arc::new(AtomicU64::new(0));
        let (tx, rx) = bounded::<CompressionTaskResult>(worker_count.saturating_mul(2).max(8));

        // Spawn a fixed-size worker set in a scoped region so panics are
        // contained and can be downgraded to a serial fallback instead of
        // leaking detached threads.
        let scope_result = thread::scope(|scope| {
            for (worker_id, local_worker) in workers.into_iter().enumerate() {
                let tx = tx.clone();
                let injector = Arc::clone(&injector);
                let stealers = Arc::clone(&stealers);
                let options = compression_options.clone();
                let attempts = Arc::clone(&steal_attempts);
                let successes = Arc::clone(&steal_successes);

                scope.spawn(move |_| {
                    let mut seed = (worker_id as u64 + 1)
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);

                    loop {
                        // Search order: local FIFO -> global injector batch ->
                        // random victim stealers.
                        let task = if let Some(file) = local_worker.pop() {
                            Some(file)
                        } else {
                            match injector.steal_batch_and_pop(&local_worker) {
                                Steal::Success(file) => {
                                    attempts.fetch_add(1, Ordering::Relaxed);
                                    successes.fetch_add(1, Ordering::Relaxed);
                                    Some(file)
                                }
                                Steal::Retry => continue,
                                Steal::Empty => {
                                    let stolen = Self::steal_from_victims(
                                        worker_id,
                                        &local_worker,
                                        &stealers,
                                        &attempts,
                                        &successes,
                                        &mut seed,
                                    );
                                    // Exit only when all task sources are empty.
                                    if stolen.is_none()
                                        && injector.is_empty()
                                        && local_worker.is_empty()
                                        && stealers.iter().all(Stealer::is_empty)
                                    {
                                        break;
                                    }
                                    stolen
                                }
                            }
                        };

                        let Some(file) = task else {
                            std::thread::yield_now();
                            continue;
                        };

                        let compressed = match compress_file(&file.path, &options) {
                            Ok(output) => {
                                debug!(
                                    file = ?file.path,
                                    archive = ?output.archive_path,
                                    algorithm = %output.algorithm_used,
                                    input_bytes = output.input_bytes,
                                    output_bytes = output.output_bytes,
                                    "parallel compression done"
                                );
                                true
                            }
                            Err(err) => {
                                warn!(file = ?file.path, error = %err, "parallel compression failed");
                                false
                            }
                        };

                        if tx.send(CompressionTaskResult { file, compressed }).is_err() {
                            break;
                        }
                    }
                });
            }
        });
        drop(tx);

        // Any worker panic triggers deterministic fallback behavior.
        if scope_result.is_err() {
            warn!("parallel compression worker panicked, falling back to serial path");
            return self.serial_compress_and_delete(files);
        }

        let mut deletable = Vec::with_capacity(files.len());
        for result in rx {
            if result.compressed {
                deletable.push(result.file);
            }
        }

        let (deleted, freed) = self.delete_files(&deletable)?;
        let elapsed = started_at.elapsed().as_secs_f64();
        let attempts = steal_attempts.load(Ordering::Relaxed);
        let successes = steal_successes.load(Ordering::Relaxed);
        let success_rate = if attempts == 0 {
            0.0
        } else {
            successes as f64 / attempts as f64
        };

        // Emit post-run cleanup metrics for monitoring and alerting.
        histogram!(METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS).record(elapsed);
        gauge!(METRIC_LOG_CLEANER_STEAL_SUCCESS_RATE).set(success_rate);

        info!(
            workers = worker_count,
            algorithm = %self.compression_algorithm,
            deleted,
            freed,
            duration_seconds = elapsed,
            steal_attempts = attempts,
            steal_successes = successes,
            steal_success_rate = success_rate,
            "parallel cleanup finished"
        );

        Ok((deleted, freed))
    }

    /// Attempt to steal a task from peer workers using randomized victim order.
    fn steal_from_victims(
        worker_id: usize,
        local_worker: &Worker<FileInfo>,
        stealers: &[Stealer<FileInfo>],
        attempts: &AtomicU64,
        successes: &AtomicU64,
        seed: &mut u64,
    ) -> Option<FileInfo> {
        if stealers.len() <= 1 {
            return None;
        }

        // Xorshift step to randomize victim polling order and avoid convoying.
        *seed ^= *seed << 13;
        *seed ^= *seed >> 7;
        *seed ^= *seed << 17;
        let start = (*seed as usize) % stealers.len();

        let steal_result = Steal::from_iter((0..stealers.len()).map(|offset| {
            let victim = (start + offset) % stealers.len();
            if victim == worker_id {
                return Steal::Empty;
            }
            attempts.fetch_add(1, Ordering::Relaxed);
            stealers[victim].steal_batch_and_pop(local_worker)
        }));

        match steal_result {
            Steal::Success(file) => {
                successes.fetch_add(1, Ordering::Relaxed);
                Some(file)
            }
            Steal::Retry | Steal::Empty => None,
        }
    }

    /// Serial fallback path and non-parallel baseline.
    ///
    /// This path is also used whenever the task set is too small to benefit
    /// from worker orchestration.
    fn serial_compress_and_delete(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        let started_at = Instant::now();
        let mut deletable = Vec::with_capacity(files.len());

        if self.compress_old_files {
            let options = self.compression_options();
            for file in files {
                match compress_file(&file.path, &options) {
                    Ok(output) => {
                        debug!(
                            file = ?file.path,
                            archive = ?output.archive_path,
                            algorithm = %output.algorithm_used,
                            input_bytes = output.input_bytes,
                            output_bytes = output.output_bytes,
                            "serial compression done"
                        );
                        deletable.push(file.clone());
                    }
                    Err(err) => {
                        warn!(file = ?file.path, error = %err, "serial compression failed, source kept");
                    }
                }
            }
        } else {
            deletable.extend(files.iter().cloned());
        }

        let (deleted, freed) = self.delete_files(&deletable)?;
        histogram!(METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS).record(started_at.elapsed().as_secs_f64());

        Ok((deleted, freed))
    }

    /// Snapshot compression-related configuration for a single cleanup pass.
    fn compression_options(&self) -> CompressionOptions {
        CompressionOptions {
            algorithm: self.compression_algorithm,
            gzip_level: self.gzip_compression_level,
            zstd_level: self.zstd_compression_level,
            zstd_workers: self.zstd_workers,
            zstd_fallback_to_gzip: self.zstd_fallback_to_gzip,
            dry_run: self.dry_run,
        }
    }

    /// Delete a file while refusing symlinks and accommodating platform quirks.
    fn secure_delete(&self, path: &PathBuf) -> std::io::Result<()> {
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Security: refusing to delete symlink: {:?}", path),
            ));
        }

        #[cfg(windows)]
        {
            // Retry removes to mitigate transient handle races from external
            // scanners/AV software.
            let mut last_err: Option<std::io::Error> = None;
            for _ in 0..3 {
                match std::fs::remove_file(path) {
                    Ok(()) => return Ok(()),
                    Err(err) => {
                        last_err = Some(err);
                        std::thread::sleep(Duration::from_millis(20));
                    }
                }
            }
            if let Some(err) = last_err {
                return Err(err);
            }
            Ok(())
        }

        #[cfg(not(windows))]
        {
            std::fs::remove_file(path)
        }
    }

    /// Delete the supplied files and return `(deleted_count, freed_bytes)`.
    ///
    /// In dry-run mode the returned counters still reflect the projected work
    /// so callers and metrics can report what would have happened.
    pub(super) fn delete_files(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        let mut deleted = 0usize;
        let mut freed = 0u64;

        for f in files {
            if self.dry_run {
                info!("[DRY RUN] Would delete: {:?} ({} bytes)", f.path, f.size);
                deleted += 1;
                freed += f.size;
                continue;
            }

            match self.secure_delete(&f.path) {
                Ok(()) => {
                    deleted += 1;
                    freed += f.size;
                    debug!("Deleted: {:?}", f.path);
                }
                Err(e) => {
                    error!("Failed to delete {:?}: {}", f.path, e);
                }
            }
        }

        Ok((deleted, freed))
    }
}

/// Builder for [`LogCleaner`].
///
/// The builder keeps startup code readable when an application only needs to
/// override a subset of retention knobs.
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
    compression_algorithm: CompressionAlgorithm,
    parallel_compress: bool,
    parallel_workers: usize,
    zstd_compression_level: i32,
    zstd_fallback_to_gzip: bool,
    zstd_workers: usize,
}

impl LogCleanerBuilder {
    /// Create a builder with conservative defaults.
    pub fn new(log_dir: impl Into<PathBuf>, file_pattern: impl Into<String>, active_filename: impl Into<String>) -> Self {
        Self {
            log_dir: log_dir.into(),
            file_pattern: file_pattern.into(),
            active_filename: active_filename.into(),
            match_mode: FileMatchMode::Prefix,
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
            compression_algorithm: CompressionAlgorithm::default(),
            parallel_compress: true,
            parallel_workers: default_parallel_workers(),
            zstd_compression_level: 8,
            zstd_fallback_to_gzip: true,
            zstd_workers: 1,
        }
    }

    /// Configure whether `file_pattern` is matched as a prefix or suffix.
    pub fn match_mode(mut self, match_mode: FileMatchMode) -> Self {
        self.match_mode = match_mode;
        self
    }

    /// Preserve at least this many newest regular log files.
    pub fn keep_files(mut self, keep_files: usize) -> Self {
        self.keep_files = keep_files;
        self
    }

    /// Cap the aggregate size of retained regular log files.
    pub fn max_total_size_bytes(mut self, max_total_size_bytes: u64) -> Self {
        self.max_total_size_bytes = max_total_size_bytes;
        self
    }

    /// Cap the size of any individual regular log file.
    pub fn max_single_file_size_bytes(mut self, max_single_file_size_bytes: u64) -> Self {
        self.max_single_file_size_bytes = max_single_file_size_bytes;
        self
    }

    /// Enable archival compression before deleting selected source logs.
    pub fn compress_old_files(mut self, compress_old_files: bool) -> Self {
        self.compress_old_files = compress_old_files;
        self
    }

    /// Set the gzip compression level used for gzip output or gzip fallback.
    pub fn gzip_compression_level(mut self, gzip_compression_level: u32) -> Self {
        self.gzip_compression_level = gzip_compression_level;
        self
    }

    /// Set how long compressed archives may remain on disk.
    pub fn compressed_file_retention_days(mut self, days: u64) -> Self {
        self.compressed_file_retention_days = days;
        self
    }

    /// Exclude files matching these glob patterns from every cleanup pass.
    pub fn exclude_patterns(mut self, patterns: Vec<String>) -> Self {
        self.exclude_patterns = patterns;
        self
    }

    /// Allow the scanner to remove matching zero-byte regular logs immediately.
    pub fn delete_empty_files(mut self, delete_empty_files: bool) -> Self {
        self.delete_empty_files = delete_empty_files;
        self
    }

    /// Require regular log files to be at least this old before processing.
    pub fn min_file_age_seconds(mut self, seconds: u64) -> Self {
        self.min_file_age_seconds = seconds;
        self
    }

    /// Enable dry-run mode for scans, compression decisions, and deletion.
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Set the preferred compression algorithm explicitly.
    pub fn compression_algorithm(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.compression_algorithm = algorithm;
        self
    }

    /// Parse and set the compression algorithm from configuration text.
    pub fn compression_algorithm_str(mut self, algorithm: impl AsRef<str>) -> Self {
        self.compression_algorithm = CompressionAlgorithm::from_config_str(algorithm.as_ref());
        self
    }

    /// Enable or disable the parallel work-stealing compression path.
    pub fn parallel_compress(mut self, enabled: bool) -> Self {
        self.parallel_compress = enabled;
        self
    }

    /// Set the number of compression workers, clamped to at least one.
    pub fn parallel_workers(mut self, workers: usize) -> Self {
        self.parallel_workers = workers.max(1);
        self
    }

    /// Set the zstd compression level.
    pub fn zstd_compression_level(mut self, level: i32) -> Self {
        self.zstd_compression_level = level;
        self
    }

    /// Retry compression with gzip when zstd encoding fails.
    pub fn zstd_fallback_to_gzip(mut self, enabled: bool) -> Self {
        self.zstd_fallback_to_gzip = enabled;
        self
    }

    /// Set the number of internal worker threads requested from zstd.
    pub fn zstd_workers(mut self, workers: usize) -> Self {
        self.zstd_workers = workers.max(1);
        self
    }

    /// Finalize the builder into an immutable [`LogCleaner`].
    ///
    /// Invalid glob patterns are ignored rather than failing construction, and
    /// codec-related numeric values are clamped into safe ranges.
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
            compression_algorithm: self.compression_algorithm,
            parallel_compress: self.parallel_compress,
            parallel_workers: self.parallel_workers.max(1),
            zstd_compression_level: self.zstd_compression_level.clamp(1, 21),
            zstd_fallback_to_gzip: self.zstd_fallback_to_gzip,
            zstd_workers: self.zstd_workers.max(1),
        }
    }
}
