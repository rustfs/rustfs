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
use crossbeam_utils::{Backoff, thread};
use metrics::{counter, gauge, histogram};
use rustfs_config::DEFAULT_LOG_KEEP_FILES;
use rustfs_config::observability::DEFAULT_OBS_LOG_MAX_COMPRESS_FILES_PER_PASS;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_LOG_CLEANER: &str = "log_cleaner";
const EVENT_LOG_CLEANER_STATE: &str = "log_cleaner_state";
const SECONDS_PER_DAY: u64 = 24 * 60 * 60;
/// Absolute ceiling on parallel compression workers, independent of config, so
/// a misconfigured `parallel_workers` on a directory with thousands of rotated logs
/// cannot spawn thousands of threads (each of which may also start codec
/// threads).
const MAX_PARALLEL_COMPRESS_WORKERS: usize = 64;
const MAX_RETENTION_DAYS_BEFORE_SATURATION: u64 = u64::MAX / SECONDS_PER_DAY;

fn compressed_file_retention_window(days: u64) -> Duration {
    Duration::from_secs(days.saturating_mul(SECONDS_PER_DAY))
}

#[derive(Debug)]
struct CompressionTaskResult {
    /// Original file metadata so successful workers can be deleted later.
    file: FileInfo,
    /// Archive created on disk (absent in dry-run mode or on compression error).
    archive: Option<FileInfo>,
    /// Whether compression completed successfully for this file.
    compressed: bool,
}

#[derive(Debug, Default)]
struct BatchProcessOutcome {
    deleted_files: usize,
    freed_bytes: u64,
    deleted_source_bytes: u64,
    created_archives: Vec<FileInfo>,
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
    /// Maximum number of newest regular log files to keep.
    pub(super) keep_files: usize,
    /// Optional cap for the cumulative size of regular logs (and shared with
    /// compressed archives when `max_archive_size_bytes` is `None`).
    pub(super) max_total_size_bytes: u64,
    /// Optional dedicated byte budget for compressed archives.
    /// - `None`: archives share the remaining `max_total_size_bytes` budget
    ///   after surviving rolled logs are accounted for.
    /// - `Some(0)`: unlimited archive bytes (retention days still apply).
    /// - `Some(cap)`: independent archive byte budget `cap`.
    pub(super) max_archive_size_bytes: Option<u64>,
    /// Optional cap for an individual regular log file.
    pub(super) max_single_file_size_bytes: u64,
    /// Maximum regular logs compressed in a single cleanup pass (`0` = unlimited).
    pub(super) max_compress_files_per_pass: usize,
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

    /// Effective gzip level after clamping — what compression actually uses.
    pub fn effective_gzip_level(&self) -> u32 {
        self.gzip_compression_level
    }

    /// Effective zstd level after clamping — what compression actually uses.
    pub fn effective_zstd_level(&self) -> i32 {
        self.zstd_compression_level
    }

    /// Perform one full cleanup pass.
    pub fn cleanup(&self) -> Result<(usize, u64), std::io::Error> {
        if !self.log_dir.exists() {
            debug!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                state = "log_dir_missing",
                log_dir = ?self.log_dir,
                "log cleaner state changed"
            );
            return Ok((0, 0));
        }

        let mut total_deleted = 0usize;
        let mut total_freed = 0u64;

        let LogScanResult {
            mut logs,
            recent_logs,
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

        logs.sort_by_key(|f| f.modified);
        let aged_rolled_bytes: u64 = logs.iter().fold(0u64, |acc, f| acc.saturating_add(f.size));
        let recent_rolled_bytes: u64 = recent_logs.iter().fold(0u64, |acc, f| acc.saturating_add(f.size));
        let all_rolled_bytes = aged_rolled_bytes.saturating_add(recent_rolled_bytes);

        let mut to_delete = Vec::new();
        if !logs.is_empty() {
            info!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                state = "scan_completed",
                regular_log_files = logs.len(),
                total_bytes = aged_rolled_bytes,
                total_megabytes = aged_rolled_bytes as f64 / 1024.0 / 1024.0,
                "log cleaner state changed"
            );

            // Select the oldest files first, then additionally trim any files
            // that still violate configured size constraints.
            to_delete = self.select_files_to_process(&logs, aged_rolled_bytes);
        }

        // `min_file_age_seconds` keeps freshly rolled files readable for a
        // while, but those files fill the disk like any other. Once the rolled
        // files that would remain exceed the byte budget, the budget wins:
        // the oldest of them go, fresh or not, and only as many as it takes.
        let over_budget = self.select_files_over_budget(&logs, &to_delete, recent_logs);
        if !over_budget.is_empty() {
            warn!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                result = "byte_budget_overrides_min_file_age",
                log_files = over_budget.len(),
                max_total_size_bytes = self.max_total_size_bytes,
                "log cleaner state changed"
            );
            to_delete.extend(over_budget);
        }

        let mut deleted_source_bytes = 0u64;
        if !to_delete.is_empty() {
            // Sort oldest-first so per-pass compression capping and direct
            // over-budget eviction stay deterministic.
            to_delete.sort_by_key(|f| f.modified);
            let (to_compress, to_direct_delete) = self.partition_files_for_pass(&to_delete, all_rolled_bytes);

            if !to_direct_delete.is_empty() {
                warn!(
                    event = EVENT_LOG_CLEANER_STATE,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                    result = "compression_cap_bypassed_over_budget",
                    compressed_files = to_compress.len(),
                    direct_deleted_files = to_direct_delete.len(),
                    max_compress_files_per_pass = self.max_compress_files_per_pass,
                    max_total_size_bytes = self.max_total_size_bytes,
                    "log cleaner state changed"
                );
                let (deleted, freed, source_bytes) = self.delete_files_with_source_bytes(&to_direct_delete)?;
                total_deleted += deleted;
                total_freed += freed;
                deleted_source_bytes = deleted_source_bytes.saturating_add(source_bytes);
            }

            if !to_compress.is_empty() {
                let outcome = if self.parallel_compress && self.compress_old_files {
                    self.parallel_stealing_compress(&to_compress)?
                } else {
                    self.serial_compress_and_delete(&to_compress)?
                };
                total_deleted += outcome.deleted_files;
                total_freed += outcome.freed_bytes;
                deleted_source_bytes = deleted_source_bytes.saturating_add(outcome.deleted_source_bytes);
                for archive in outcome.created_archives {
                    if let Some(existing) = compressed_archives.iter_mut().find(|entry| entry.path == archive.path) {
                        *existing = archive;
                    } else {
                        compressed_archives.push(archive);
                    }
                }
            }
        }

        let surviving_rolled_bytes = all_rolled_bytes.saturating_sub(deleted_source_bytes);
        let effective_archive_budget = self.effective_archive_budget(surviving_rolled_bytes);

        if !compressed_archives.is_empty() {
            let to_delete = self.select_archives_to_delete(&mut compressed_archives, effective_archive_budget);
            if !to_delete.is_empty() {
                let (d, f, _) = self.delete_files_with_source_bytes(&to_delete)?;
                total_deleted += d;
                total_freed += f;
            }
        }

        if total_deleted > 0 || total_freed > 0 {
            counter!(METRIC_LOG_CLEANER_DELETED_FILES_TOTAL).increment(total_deleted as u64);
            counter!(METRIC_LOG_CLEANER_FREED_BYTES_TOTAL).increment(total_freed);
            info!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                state = "cleanup_completed",
                deleted_files = total_deleted,
                freed_bytes = total_freed,
                freed_megabytes = total_freed as f64 / 1024.0 / 1024.0,
                "log cleaner state changed"
            );
        }

        Ok((total_deleted, total_freed))
    }

    /// Byte-budget trim that also counts the rolled files still inside
    /// `min_file_age_seconds`.
    ///
    /// `aged` is sorted oldest first and `selected` is what the regular
    /// selection already took from it. When the files that would remain,
    /// aged and recent together, exceed `max_total_size_bytes`, the oldest of
    /// them are returned until the rest fits. `keep_files` and the per-file
    /// limit are not applied to recent files: only the bytes matter here.
    pub(super) fn select_files_over_budget(
        &self,
        aged: &[FileInfo],
        selected: &[FileInfo],
        mut recent: Vec<FileInfo>,
    ) -> Vec<FileInfo> {
        if self.max_total_size_bytes == 0 || recent.is_empty() {
            return Vec::new();
        }
        recent.sort_by_key(|file| file.modified);
        let selected: std::collections::HashSet<&std::path::Path> = selected.iter().map(|file| file.path.as_path()).collect();
        // Aged files are older than every recent one, so chaining keeps the
        // oldest-first order.
        let remaining: Vec<&FileInfo> = aged
            .iter()
            .filter(|file| !selected.contains(file.path.as_path()))
            .chain(recent.iter())
            .collect();
        let mut occupied = remaining.iter().fold(0u64, |total, file| total.saturating_add(file.size));
        let mut over_budget = Vec::new();
        for file in remaining {
            if occupied <= self.max_total_size_bytes {
                break;
            }
            occupied = occupied.saturating_sub(file.size);
            over_budget.push(file.clone());
        }
        over_budget
    }

    /// Choose regular log files that should be compressed and/or deleted.
    ///
    /// The `files` slice must already be sorted from oldest to newest. The
    /// method first enforces the `keep_files` ceiling, then applies total-size
    /// and per-file-size limits to the remaining tail.
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

    /// Partition selected regular logs into `(compress_and_delete, direct_delete)`
    /// while enforcing `max_compress_files_per_pass` when compression is active.
    ///
    /// - Under the byte budget: compresses at most `max_compress_files_per_pass`
    ///   files and defers the rest to subsequent cleanup passes so CPU spikes
    ///   are amortized across ticks.
    /// - Over the byte budget: compresses the newest `max_compress_files_per_pass`
    ///   mandatory files and directly deletes older mandatory excess files
    ///   without compression so the disk budget is enforced in a single pass.
    fn partition_files_for_pass(&self, sorted_candidates: &[FileInfo], all_rolled_bytes: u64) -> (Vec<FileInfo>, Vec<FileInfo>) {
        if !self.compress_old_files
            || self.max_compress_files_per_pass == 0
            || sorted_candidates.len() <= self.max_compress_files_per_pass
        {
            return (sorted_candidates.to_vec(), Vec::new());
        }

        let cap = self.max_compress_files_per_pass;
        if self.max_total_size_bytes == 0 {
            return (sorted_candidates[..cap].to_vec(), Vec::new());
        }

        // Compute how many oldest candidates must be evicted right now to bring
        // rolled logs back under `max_total_size_bytes`.
        let mut remaining = all_rolled_bytes;
        let mut mandatory_count = 0usize;
        for file in sorted_candidates {
            if remaining <= self.max_total_size_bytes {
                break;
            }
            remaining = remaining.saturating_sub(file.size);
            mandatory_count += 1;
        }

        if mandatory_count <= cap {
            // Compressing `cap` files already satisfies the byte budget; defer
            // any additional keep_files/single-file candidates to later passes.
            (sorted_candidates[..cap].to_vec(), Vec::new())
        } else {
            let direct_count = mandatory_count - cap;
            let direct = sorted_candidates[..direct_count].to_vec();
            let compress = sorted_candidates[direct_count..mandatory_count].to_vec();
            (compress, direct)
        }
    }

    /// Compute the effective byte budget applied to compressed archives.
    ///
    /// - Explicit `Some(0)`: unlimited archive bytes (retention days still apply).
    /// - Explicit `Some(cap)`: dedicated archive byte cap independent of rolled logs.
    /// - Unset (`None`) with `max_total_size_bytes > 0`: shares the remaining
    ///   `max_total_size_bytes` budget after `surviving_rolled_bytes` so rolled
    ///   logs + archives stay within the single configured total budget.
    /// - Unset (`None`) with `max_total_size_bytes == 0`: unlimited.
    fn effective_archive_budget(&self, surviving_rolled_bytes: u64) -> Option<u64> {
        match self.max_archive_size_bytes {
            Some(0) => None,
            Some(cap) => Some(cap),
            None if self.max_total_size_bytes > 0 => Some(self.max_total_size_bytes.saturating_sub(surviving_rolled_bytes)),
            None => None,
        }
    }

    /// Select compressed archives to delete: those older than the retention
    /// window, plus — regardless of retention — the oldest archives needed to
    /// bring the archive set back under `effective_archive_budget`.
    fn select_archives_to_delete(&self, files: &mut [FileInfo], effective_archive_budget: Option<u64>) -> Vec<FileInfo> {
        if self.compressed_file_retention_days > MAX_RETENTION_DAYS_BEFORE_SATURATION {
            warn!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                result = "retention_days_saturated",
                configured_days = self.compressed_file_retention_days,
                fallback_days = MAX_RETENTION_DAYS_BEFORE_SATURATION,
                "log cleaner state changed"
            );
        }

        // Oldest first so both age expiry and the byte-cap trim evict the
        // longest-retained archives before newer ones.
        files.sort_by_key(|f| f.modified);
        let now = SystemTime::now();
        let retention = compressed_file_retention_window(self.compressed_file_retention_days);
        let retention_active = self.compressed_file_retention_days > 0;

        let mut selected = vec![false; files.len()];

        // 1. Age-based expiry (only when retention is enabled).
        if retention_active {
            for (idx, file) in files.iter().enumerate() {
                if let Ok(age) = now.duration_since(file.modified)
                    && age > retention
                {
                    selected[idx] = true;
                }
            }
        }

        // 2. Byte-cap trim: drop the oldest surviving archives until the set
        //    fits under `effective_archive_budget`. `None` means "no byte cap".
        if let Some(archive_budget) = effective_archive_budget {
            let mut remaining: u64 = files
                .iter()
                .enumerate()
                .filter(|(idx, _)| !selected[*idx])
                .map(|(_, file)| file.size)
                .sum();
            for (idx, file) in files.iter().enumerate() {
                if remaining <= archive_budget {
                    break;
                }
                if selected[idx] {
                    continue;
                }
                selected[idx] = true;
                remaining = remaining.saturating_sub(file.size);
            }
        }

        files
            .iter()
            .enumerate()
            .filter(|(idx, _)| selected[*idx])
            .map(|(_, file)| file.clone())
            .collect()
    }

    /// Parallel compressor with work stealing.
    ///
    /// The flow is intentionally split into "parallel compression" followed by
    /// "serial deletion" to reduce cross-platform file-locking failures.
    /// Compression workers only decide whether an archive was created; the main
    /// thread remains responsible for actual source removal so deletion policy
    /// and error reporting stay deterministic.
    fn parallel_stealing_compress(&self, files: &[FileInfo]) -> Result<BatchProcessOutcome, std::io::Error> {
        if files.len() <= 1 {
            return self.serial_compress_and_delete(files);
        }

        let worker_count = self.parallel_workers.min(files.len()).clamp(1, MAX_PARALLEL_COMPRESS_WORKERS);
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

                    // Escalating spin->yield backoff for the transient windows
                    // where work is being redistributed, instead of a hot
                    // `continue`/`yield_now` loop that burns CPU.
                    let backoff = Backoff::new();

                    loop {
                        // Search order: local FIFO -> global injector batch ->
                        // random victim stealers.
                        let task = if let Some(file) = local_worker.pop() {
                            Some(file)
                        } else {
                            match injector.steal_batch_and_pop(&local_worker) {
                                Steal::Success(file) => {
                                    Some(file)
                                }
                                Steal::Retry => {
                                    backoff.snooze();
                                    continue;
                                }
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
                            backoff.snooze();
                            continue;
                        };

                        // Got work: reset the backoff so the next idle spell
                        // starts from a cheap spin again.
                        backoff.reset();
                        let mut file = file;
                        let (compressed, archive) = match compress_file(&file.path, &options) {
                            Ok(output) => {
                                debug!(
                                    event = EVENT_LOG_CLEANER_STATE,
                                    component = LOG_COMPONENT_OBS,
                                    subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                                    file = ?file.path,
                                    archive = ?output.archive_path,
                                    algorithm = %output.algorithm_used,
                                    input_bytes = output.input_bytes,
                                    output_bytes = output.output_bytes,
                                    state = "parallel_compression_done",
                                    "log cleaner state changed"
                                );
                                file.projected_freed_bytes = output.input_bytes.saturating_sub(output.output_bytes);
                                let archive = (!options.dry_run).then(|| FileInfo {
                                    path: output.archive_path,
                                    size: output.output_bytes,
                                    projected_freed_bytes: output.output_bytes,
                                    modified: SystemTime::now(),
                                });
                                (true, archive)
                            }
                            Err(err) => {
                                warn!(event = EVENT_LOG_CLEANER_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, file = ?file.path, error = %err, result = "parallel_compression_failed", "log cleaner state changed");
                                (false, None)
                            }
                        };

                        if tx.send(CompressionTaskResult { file, archive, compressed }).is_err() {
                            break;
                        }
                    }
                });
            }
            drop(tx);

            let mut deletable = Vec::with_capacity(files.len());
            let mut created_archives = Vec::new();
            for result in rx {
                if result.compressed {
                    deletable.push(result.file);
                    if let Some(archive) = result.archive {
                        created_archives.push(archive);
                    }
                }
            }

            (deletable, created_archives)
        });

        // Any worker panic triggers deterministic fallback behavior.
        let (deletable, created_archives) = match scope_result {
            Ok(outcome) => outcome,
            Err(_) => {
                warn!(
                    event = EVENT_LOG_CLEANER_STATE,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                    result = "parallel_worker_panicked",
                    fallback = "serial",
                    "log cleaner state changed"
                );
                return self.serial_compress_and_delete(files);
            }
        };

        let (deleted, freed, deleted_source_bytes) = self.delete_files_with_source_bytes(&deletable)?;
        let elapsed = started_at.elapsed().as_secs_f64();
        // NOTE on metric scope: attempts/successes count only *victim* steals
        // (not the injector `steal_batch_and_pop`), and a multi-task stolen
        // batch counts as a single success. So this rate reflects inter-worker
        // rebalancing efficiency, not total task acquisition — read it as a
        // relative health signal, not an absolute throughput measure.
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
            event = EVENT_LOG_CLEANER_STATE,
            component = LOG_COMPONENT_OBS,
            subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
            workers = worker_count,
            algorithm = %self.compression_algorithm,
            deleted,
            freed,
            duration_seconds = elapsed,
            steal_attempts = attempts,
            steal_successes = successes,
            steal_success_rate = success_rate,
            state = "parallel_cleanup_finished",
            "log cleaner state changed"
        );

        Ok(BatchProcessOutcome {
            deleted_files: deleted,
            freed_bytes: freed,
            deleted_source_bytes,
            created_archives,
        })
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
    fn serial_compress_and_delete(&self, files: &[FileInfo]) -> Result<BatchProcessOutcome, std::io::Error> {
        let started_at = Instant::now();
        let mut deletable = Vec::with_capacity(files.len());
        let mut created_archives = Vec::new();

        if self.compress_old_files {
            let options = self.compression_options();
            for file in files {
                match compress_file(&file.path, &options) {
                    Ok(output) => {
                        debug!(
                            event = EVENT_LOG_CLEANER_STATE,
                            component = LOG_COMPONENT_OBS,
                            subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                            file = ?file.path,
                            archive = ?output.archive_path,
                            algorithm = %output.algorithm_used,
                            input_bytes = output.input_bytes,
                            output_bytes = output.output_bytes,
                            state = "serial_compression_done",
                            "log cleaner state changed"
                        );
                        let mut file = file.clone();
                        file.projected_freed_bytes = output.input_bytes.saturating_sub(output.output_bytes);
                        deletable.push(file);
                        if !options.dry_run {
                            created_archives.push(FileInfo {
                                path: output.archive_path,
                                size: output.output_bytes,
                                projected_freed_bytes: output.output_bytes,
                                modified: SystemTime::now(),
                            });
                        }
                    }
                    Err(err) => {
                        warn!(event = EVENT_LOG_CLEANER_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, file = ?file.path, error = %err, result = "serial_compression_failed", "log cleaner state changed");
                    }
                }
            }
        } else {
            deletable.extend(files.iter().cloned());
        }

        let (deleted, freed, deleted_source_bytes) = self.delete_files_with_source_bytes(&deletable)?;
        histogram!(METRIC_LOG_CLEANER_COMPRESS_DURATION_SECONDS).record(started_at.elapsed().as_secs_f64());

        Ok(BatchProcessOutcome {
            deleted_files: deleted,
            freed_bytes: freed,
            deleted_source_bytes,
            created_archives,
        })
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

    /// Delete the supplied files and return
    /// `(deleted_count, freed_bytes, deleted_source_bytes)`.
    ///
    /// In dry-run mode the returned counters still reflect the projected work
    /// so callers and metrics can report what would have happened.
    fn delete_files_with_source_bytes(&self, files: &[FileInfo]) -> Result<(usize, u64, u64), std::io::Error> {
        let mut deleted = 0usize;
        let mut freed = 0u64;
        let mut source_bytes = 0u64;

        for f in files {
            if self.dry_run {
                info!(event = EVENT_LOG_CLEANER_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "dry_run_delete", file = ?f.path, bytes = f.size, "log cleaner state changed");
                deleted += 1;
                freed = freed.saturating_add(f.projected_freed_bytes);
                source_bytes = source_bytes.saturating_add(f.size);
                continue;
            }

            match self.secure_delete(&f.path) {
                Ok(()) => {
                    deleted += 1;
                    freed = freed.saturating_add(f.projected_freed_bytes);
                    source_bytes = source_bytes.saturating_add(f.size);
                    debug!(event = EVENT_LOG_CLEANER_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "deleted", file = ?f.path, bytes = f.size, projected_freed_bytes = f.projected_freed_bytes, "log cleaner state changed");
                }
                Err(e) => {
                    error!(event = EVENT_LOG_CLEANER_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, result = "delete_failed", file = ?f.path, error = %e, "log cleaner state changed");
                }
            }
        }

        // Durability: persist the directory entries so the unlinks cannot be
        // reordered ahead of the archive data that justified them. Best-effort;
        // opening a directory as a file is not portable (e.g. Windows).
        if !self.dry_run
            && deleted > 0
            && let Ok(dir) = std::fs::File::open(&self.log_dir)
        {
            let _ = dir.sync_all();
        }

        Ok((deleted, freed, source_bytes))
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
    max_archive_size_bytes: Option<u64>,
    max_single_file_size_bytes: u64,
    max_compress_files_per_pass: usize,
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
            max_archive_size_bytes: None,
            max_single_file_size_bytes: 0,
            max_compress_files_per_pass: DEFAULT_OBS_LOG_MAX_COMPRESS_FILES_PER_PASS,
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

    /// Keep at most this many newest regular log files.
    pub fn keep_files(mut self, keep_files: usize) -> Self {
        self.keep_files = keep_files;
        self
    }

    /// Cap the aggregate size of retained regular log files (and compressed
    /// archives when `max_archive_size_bytes` is unset).
    pub fn max_total_size_bytes(mut self, max_total_size_bytes: u64) -> Self {
        self.max_total_size_bytes = max_total_size_bytes;
        self
    }

    /// Set a dedicated byte budget for compressed archives independent of
    /// `max_total_size_bytes`. Pass `None` to share `max_total_size_bytes`,
    /// `Some(0)` for unlimited archive bytes, or `Some(cap)` for a separate cap.
    pub fn max_archive_size_bytes(mut self, max_archive_size_bytes: Option<u64>) -> Self {
        self.max_archive_size_bytes = max_archive_size_bytes;
        self
    }

    /// Cap the size of any individual regular log file.
    pub fn max_single_file_size_bytes(mut self, max_single_file_size_bytes: u64) -> Self {
        self.max_single_file_size_bytes = max_single_file_size_bytes;
        self
    }

    /// Cap how many rolled log files may be compressed in a single cleanup pass
    /// (`0` = unlimited).
    pub fn max_compress_files_per_pass(mut self, max_compress_files_per_pass: usize) -> Self {
        self.max_compress_files_per_pass = max_compress_files_per_pass;
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
        // Defense-in-depth for the active-file guard: the scanner protects the
        // live log by exact filename equality against `active_filename`. An
        // empty value silently disables that protection, so a non-empty
        // `file_pattern` could then make the live log a deletion candidate.
        // Warn instead of failing so misuse of the public builder is visible.
        if self.active_filename.trim().is_empty() && !self.file_pattern.trim().is_empty() {
            warn!(
                event = EVENT_LOG_CLEANER_STATE,
                component = LOG_COMPONENT_OBS,
                subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                result = "empty_active_filename",
                "active log filename is empty; the active-file exclusion is disabled and the live log may become a deletion candidate"
            );
        }

        // Surface, rather than silently drop, invalid exclude globs: a dropped
        // pattern turns "protect this file" into "delete this file", so an
        // operator's typo (or a literal comma splitting a char-class) must be
        // visible instead of failing open.
        let mut patterns = Vec::new();
        for raw in self.exclude_patterns {
            match glob::Pattern::new(&raw) {
                Ok(pattern) => patterns.push(pattern),
                Err(err) => warn!(
                    event = EVENT_LOG_CLEANER_STATE,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                    result = "invalid_exclude_pattern",
                    pattern = %raw,
                    error = %err,
                    "log cleaner state changed"
                ),
            }
        }

        LogCleaner {
            log_dir: self.log_dir,
            file_pattern: self.file_pattern,
            active_filename: self.active_filename,
            match_mode: self.match_mode,
            keep_files: self.keep_files,
            max_total_size_bytes: self.max_total_size_bytes,
            max_archive_size_bytes: self.max_archive_size_bytes,
            max_single_file_size_bytes: self.max_single_file_size_bytes,
            max_compress_files_per_pass: self.max_compress_files_per_pass,
            compress_old_files: self.compress_old_files,
            // gzip level 0 is a valid "store" (no compression) mode, so keep the
            // lower bound at 0 rather than silently bumping it to 1.
            gzip_compression_level: self.gzip_compression_level.clamp(0, 9),
            compressed_file_retention_days: self.compressed_file_retention_days,
            exclude_patterns: patterns,
            delete_empty_files: self.delete_empty_files,
            min_file_age_seconds: self.min_file_age_seconds,
            dry_run: self.dry_run,
            compression_algorithm: self.compression_algorithm,
            parallel_compress: self.parallel_compress,
            parallel_workers: self.parallel_workers.max(1),
            // zstd level 0 means "codec default"; the real maximum is 22, not
            // 21, so preserve both boundaries instead of silently narrowing.
            zstd_compression_level: self.zstd_compression_level.clamp(0, 22),
            zstd_fallback_to_gzip: self.zstd_fallback_to_gzip,
            zstd_workers: self.zstd_workers.max(1),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_RETENTION_DAYS_BEFORE_SATURATION, SECONDS_PER_DAY, compressed_file_retention_window};
    use std::time::Duration;

    #[test]
    fn compressed_file_retention_window_scales_days_without_wrap() {
        assert_eq!(compressed_file_retention_window(3), Duration::from_secs(3 * SECONDS_PER_DAY));
    }

    #[test]
    fn compressed_file_retention_window_saturates_on_large_values() {
        assert_eq!(compressed_file_retention_window(u64::MAX), Duration::from_secs(u64::MAX));
    }

    #[test]
    fn retention_day_saturation_boundary_is_safe() {
        assert_eq!(
            compressed_file_retention_window(MAX_RETENTION_DAYS_BEFORE_SATURATION),
            Duration::from_secs(MAX_RETENTION_DAYS_BEFORE_SATURATION * SECONDS_PER_DAY)
        );
        assert_eq!(
            compressed_file_retention_window(MAX_RETENTION_DAYS_BEFORE_SATURATION.saturating_add(1)),
            Duration::from_secs(u64::MAX)
        );
    }
}
