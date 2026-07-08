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

use super::capacity_manager::{
    CapacityUpdate, DiskCapacityUpdate, HybridCapacityManager, get_enable_dynamic_timeout, get_follow_symlinks,
    get_max_files_threshold, get_max_symlink_depth, get_max_timeout, get_min_timeout, get_sample_rate, get_stall_timeout,
    get_stat_timeout,
};
use super::types::{CapacityDiskRef, CapacityScanResult, CapacityScanSummary};
use crate::capacity_scope::CapacityScopeDisk;
use futures::{StreamExt, stream};
use rustfs_io_metrics::capacity_metrics::{
    record_capacity_dynamic_timeout, record_capacity_scan_disk, record_capacity_scan_mode, record_capacity_scan_sampling,
    record_capacity_stall_detected, record_capacity_symlink, record_capacity_timeout_fallback,
};
use std::collections::HashSet;
use std::ffi::OsStr;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use walkdir::WalkDir;

const MAX_CAPACITY_SCAN_CONCURRENCY: usize = 4;
const CAPACITY_PROGRESS_CHECK_STRIDE: usize = 512;
const LOG_COMPONENT_CAPACITY: &str = "capacity";
const LOG_SUBSYSTEM_SCAN: &str = "scan";
const LOG_SUBSYSTEM_SAMPLING: &str = "sampling";
const EVENT_CAPACITY_SCAN_DISK_COMPLETED: &str = "capacity_scan_disk_completed";
const EVENT_CAPACITY_SCAN_DISK_FAILED: &str = "capacity_scan_disk_failed";
const EVENT_CAPACITY_SCAN_SUMMARY: &str = "capacity_scan_summary";
const EVENT_CAPACITY_SCAN_SYMLINK_SKIPPED: &str = "capacity_scan_symlink_skipped";
const EVENT_CAPACITY_SCAN_SYMLINK_SUMMARY: &str = "capacity_scan_symlink_summary";
const EVENT_CAPACITY_SCAN_DYNAMIC_TIMEOUT: &str = "capacity_scan_dynamic_timeout";
const EVENT_CAPACITY_SCAN_TIMEOUT: &str = "capacity_scan_timeout";
const EVENT_CAPACITY_SCAN_STALL_DETECTED: &str = "capacity_scan_stall_detected";
const EVENT_CAPACITY_SCAN_SAMPLING_CLAMPED: &str = "capacity_scan_sampling_clamped";
const EVENT_CAPACITY_SCAN_TRAVERSAL_FAILED: &str = "capacity_scan_traversal_failed";
const EVENT_CAPACITY_SCAN_METADATA_FAILED: &str = "capacity_scan_metadata_failed";
const EVENT_CAPACITY_SCAN_SAMPLING_APPLIED: &str = "capacity_scan_sampling_applied";
const EVENT_CAPACITY_SCAN_EXACT_COMPLETED: &str = "capacity_scan_exact_completed";
const EVENT_CAPACITY_SCAN_HARD_TIMEOUT: &str = "capacity_scan_hard_timeout";
const RUSTFS_META_BUCKET: &str = ".rustfs.sys";
const RUSTFS_META_TMP_DIR: &str = "tmp";
const RUSTFS_META_TMP_TRASH_DIR: &str = ".trash";

#[derive(Debug)]
struct DiskScanOutcome {
    disk_label: String,
    drive_path: String,
    duration: Duration,
    result: Result<CapacityScanResult, std::io::Error>,
}

#[derive(Debug, Clone)]
struct DiskCapacityScanResult {
    disk: CapacityScopeDisk,
    scan: CapacityScanResult,
}

#[derive(Debug, Clone)]
struct CapacityScanReport {
    summary: CapacityScanResult,
    per_disk: Vec<DiskCapacityScanResult>,
}

impl CapacityScanReport {
    fn into_capacity_update(self, expected_disk_count: usize, replaces_disk_cache: bool) -> CapacityUpdate {
        let mut update = if self.summary.is_estimated {
            CapacityUpdate::estimated(self.summary.used_bytes, self.summary.file_count)
        } else {
            CapacityUpdate::exact(self.summary.used_bytes, self.summary.file_count)
        };

        if !self.summary.had_partial_errors && self.per_disk.len() == expected_disk_count {
            update.per_disk = self
                .per_disk
                .into_iter()
                .map(|entry| DiskCapacityUpdate {
                    disk: entry.disk,
                    used_bytes: entry.scan.used_bytes,
                    file_count: entry.scan.file_count,
                    is_estimated: entry.scan.is_estimated,
                })
                .collect();
            update.expected_disk_count = Some(expected_disk_count);
            update.replaces_disk_cache = replaces_disk_cache;
            update.clear_dirty_disks = update.per_disk.iter().map(|entry| entry.disk.clone()).collect();
        } else if self.summary.had_partial_errors {
            // Partial failure: mark the reading degraded and surface only the
            // disks whose own scan fully succeeded, so the manager can merge
            // them over a complete disk cache while failed disks keep their
            // last-known values. A disk with intra-disk errors under-counts,
            // so its stale cache entry is closer to the truth than its scan.
            // The cache is never replaced from a degraded refresh.
            update.degraded = true;
            update.per_disk = self
                .per_disk
                .into_iter()
                .filter(|entry| !entry.scan.had_partial_errors)
                .map(|entry| DiskCapacityUpdate {
                    disk: entry.disk,
                    used_bytes: entry.scan.used_bytes,
                    file_count: entry.scan.file_count,
                    is_estimated: entry.scan.is_estimated,
                })
                .collect();
            update.clear_dirty_disks = update.per_disk.iter().map(|entry| entry.disk.clone()).collect();
        }

        update
    }
}

fn disk_metric_label(disk: &CapacityDiskRef) -> String {
    let mount_name = Path::new(&disk.drive_path)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or(disk.drive_path.as_str());
    format!("{}:{mount_name}", disk.endpoint)
}

/// Extrapolate the total overflow bytes from a sample as
/// `sampled_bytes * overflow_count / sampled_count`.
///
/// The multiplication is performed in `u128` so the intermediate product cannot
/// saturate `u64` on very large disks. Doing `saturating_mul` in `u64` first and
/// then dividing (the previous approach) pins the numerator at `u64::MAX` once the
/// product overflows, which makes the estimate shrink as `sampled_count` grows —
/// i.e. the reported capacity monotonically *under*-counts as the disk gets
/// bigger. The final result is clamped to `u64::MAX`. See backlog#1012.
fn estimate_overflow_bytes(overflow_sampled_bytes: u64, overflow_count: u64, sampled_count: u64) -> u64 {
    let denom = (sampled_count.max(1)) as u128;
    let scaled = (overflow_sampled_bytes as u128 * overflow_count as u128) / denom;
    scaled.min(u64::MAX as u128) as u64
}

fn disk_scope_key(disk: &CapacityDiskRef) -> CapacityScopeDisk {
    CapacityScopeDisk {
        endpoint: disk.endpoint.clone(),
        drive_path: disk.drive_path.clone(),
    }
}

fn normal_component_eq(component: Option<Component<'_>>, expected: &str) -> bool {
    matches!(component, Some(Component::Normal(value)) if value == OsStr::new(expected))
}

fn is_tmp_trash_metadata_not_found(scan_root: &Path, entry_path: &Path, error_kind: Option<std::io::ErrorKind>) -> bool {
    if error_kind != Some(std::io::ErrorKind::NotFound) {
        return false;
    }

    let Ok(relative_path) = entry_path.strip_prefix(scan_root) else {
        return false;
    };

    let mut components = relative_path.components();
    normal_component_eq(components.next(), RUSTFS_META_BUCKET)
        && normal_component_eq(components.next(), RUSTFS_META_TMP_DIR)
        && normal_component_eq(components.next(), RUSTFS_META_TMP_TRASH_DIR)
        && matches!(components.next(), Some(Component::Normal(_)))
        && components.all(|component| matches!(component, Component::Normal(_)))
}

async fn scan_disk_used_capacity(disk: CapacityDiskRef) -> DiskScanOutcome {
    let disk_label = disk_metric_label(&disk);
    let drive_path = disk.drive_path.clone();
    let start = Instant::now();
    let result = get_dir_size_async(Path::new(&drive_path)).await;

    DiskScanOutcome {
        disk_label,
        drive_path,
        duration: start.elapsed(),
        result,
    }
}

async fn calculate_data_dir_used_capacity_report(
    disks: &[CapacityDiskRef],
) -> Result<CapacityScanReport, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let mut total_used = 0u64;
    let mut total_files = 0usize;
    let mut total_sampled = 0usize;
    let mut has_failure = false;
    let mut has_success = false;
    let mut is_estimated = false;
    let mut per_disk = Vec::with_capacity(disks.len());

    let concurrency_limit = disks.len().clamp(1, MAX_CAPACITY_SCAN_CONCURRENCY);
    let mut scans = stream::iter(disks.iter().cloned().map(scan_disk_used_capacity)).buffer_unordered(concurrency_limit);

    while let Some(outcome) = scans.next().await {
        match outcome.result {
            Ok(scan) => {
                record_capacity_scan_disk(
                    outcome.disk_label.as_str(),
                    outcome.duration,
                    scan.file_count,
                    scan.sampled_count,
                    scan.is_estimated,
                    scan.had_partial_errors,
                );
                debug!(
                    event = EVENT_CAPACITY_SCAN_DISK_COMPLETED,
                    component = LOG_COMPONENT_CAPACITY,
                    subsystem = LOG_SUBSYSTEM_SCAN,
                    result = "ok",
                    disk_label = %outcome.disk_label,
                    drive_path = %outcome.drive_path,
                    used_bytes = scan.used_bytes,
                    file_count = scan.file_count,
                    sampled_count = scan.sampled_count,
                    estimated = scan.is_estimated,
                    partial_errors = scan.had_partial_errors,
                    duration_ms = outcome.duration.as_millis() as u64,
                    "capacity scan disk completed"
                );
                total_used += scan.used_bytes;
                total_files += scan.file_count;
                total_sampled += scan.sampled_count;
                is_estimated |= scan.is_estimated;
                has_failure |= scan.had_partial_errors;
                has_success = true;
                if let Some(disk) = disks
                    .iter()
                    .find(|disk| disk.drive_path == outcome.drive_path && disk_metric_label(disk) == outcome.disk_label)
                {
                    per_disk.push(DiskCapacityScanResult {
                        disk: disk_scope_key(disk),
                        scan,
                    });
                }
            }
            Err(e) => {
                record_capacity_scan_disk(outcome.disk_label.as_str(), outcome.duration, 0, 0, false, true);
                warn!(
                    event = EVENT_CAPACITY_SCAN_DISK_FAILED,
                    component = LOG_COMPONENT_CAPACITY,
                    subsystem = LOG_SUBSYSTEM_SCAN,
                    result = "error",
                    disk_label = %outcome.disk_label,
                    drive_path = %outcome.drive_path,
                    duration_ms = outcome.duration.as_millis() as u64,
                    error = ?e,
                    "capacity scan disk failed"
                );
                has_failure = true;
            }
        }
    }

    if !has_success {
        return Err("All directories failed to calculate size".into());
    }

    if has_failure {
        warn!(
            event = EVENT_CAPACITY_SCAN_SUMMARY,
            component = LOG_COMPONENT_CAPACITY,
            subsystem = LOG_SUBSYSTEM_SCAN,
            result = "partial",
            disk_count = disks.len(),
            used_bytes = total_used,
            file_count = total_files,
            sampled_count = total_sampled,
            estimated = is_estimated,
            duration_ms = start.elapsed().as_millis() as u64,
            "capacity scan completed with partial failures"
        );
    }

    let mut summary = CapacityScanResult {
        used_bytes: total_used,
        file_count: total_files,
        sampled_count: total_sampled,
        is_estimated,
        scan_duration: start.elapsed(),
        had_partial_errors: false,
    };

    if has_failure {
        summary = summary.with_partial_errors();
    }

    Ok(CapacityScanReport { summary, per_disk })
}

/// Calculate actual used capacity of all data directories.
pub(crate) async fn calculate_data_dir_used_capacity(
    disks: &[CapacityDiskRef],
) -> Result<CapacityScanResult, Box<dyn std::error::Error + Send + Sync>> {
    Ok(calculate_data_dir_used_capacity_report(disks).await?.summary)
}

pub async fn select_capacity_refresh_disks(
    capacity_manager: &HybridCapacityManager,
    disks: &[CapacityDiskRef],
) -> (Vec<CapacityDiskRef>, bool) {
    if !capacity_manager.can_refresh_dirty_subset().await {
        return (disks.to_vec(), false);
    }

    let dirty_disks = capacity_manager.get_dirty_disks().await;
    if dirty_disks.is_empty() {
        return (disks.to_vec(), false);
    }

    let dirty_set: HashSet<CapacityScopeDisk> = dirty_disks.into_iter().collect();
    let selected: Vec<_> = disks
        .iter()
        .filter(|disk| dirty_set.contains(&disk_scope_key(disk)))
        .cloned()
        .collect();

    if selected.is_empty() || selected.len() >= disks.len() {
        (disks.to_vec(), false)
    } else {
        (selected, true)
    }
}

pub async fn refresh_capacity_with_scope(disks: Vec<CapacityDiskRef>, dirty_subset: bool) -> Result<CapacityUpdate, String> {
    let report = calculate_data_dir_used_capacity_report(&disks)
        .await
        .map_err(|e| e.to_string())?;

    if dirty_subset && report.summary.had_partial_errors {
        return Err("dirty subset refresh had partial errors".to_string());
    }

    Ok(report.into_capacity_update(disks.len(), !dirty_subset))
}

/// Scan the provided local disk roots and return a summarized used-capacity result.
///
/// This is primarily intended for benchmarks and operational tooling that need to exercise
/// the same scan path as admin capacity queries without going through the full admin stack.
pub async fn scan_used_capacity_disks(
    disks: &[CapacityDiskRef],
) -> Result<CapacityScanSummary, Box<dyn std::error::Error + Send + Sync>> {
    Ok(calculate_data_dir_used_capacity(disks).await?.into())
}

/// Tracker for symlink resolution with circular reference detection.
struct SymlinkTracker {
    visited: HashSet<PathBuf>,
    symlink_count: usize,
    symlink_size: u64,
    max_depth: u8,
}

impl SymlinkTracker {
    fn new(max_depth: u8) -> Self {
        Self {
            visited: HashSet::new(),
            symlink_count: 0,
            symlink_size: 0,
            max_depth,
        }
    }

    fn should_follow(&self, path: &Path, depth: u8) -> bool {
        if depth >= self.max_depth {
            debug!(
                event = EVENT_CAPACITY_SCAN_SYMLINK_SKIPPED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "skipped",
                reason = "depth_limit",
                depth,
                max_depth = self.max_depth,
                path = ?path,
                "capacity scan symlink skipped"
            );
            return false;
        }

        if self.visited.contains(path) {
            warn!(
                event = EVENT_CAPACITY_SCAN_SYMLINK_SKIPPED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "skipped",
                reason = "cycle_detected",
                path = ?path,
                "capacity scan symlink skipped"
            );
            return false;
        }

        true
    }

    fn record_symlink(&mut self, path: PathBuf, size: u64) {
        if self.visited.insert(path) {
            self.symlink_count += 1;
            self.symlink_size += size;
            record_capacity_symlink(size);
        }
    }

    fn get_stats(&self) -> (usize, u64) {
        (self.symlink_count, self.symlink_size)
    }
}

/// Monitor for directory traversal progress with timeout and stall detection.
struct ProgressMonitor {
    start_time: Instant,
    last_check: Instant,
    last_checkpoint_files: usize,
    timeout: Duration,
    min_timeout: Duration,
    max_timeout: Duration,
    stall_timeout: Duration,
    enable_dynamic_timeout: bool,
    used_dynamic_timeout: bool,
}

impl ProgressMonitor {
    fn new(
        base_timeout: Duration,
        min_timeout: Duration,
        max_timeout: Duration,
        stall_timeout: Duration,
        enable_dynamic: bool,
    ) -> Self {
        Self {
            start_time: Instant::now(),
            last_check: Instant::now(),
            last_checkpoint_files: 0,
            timeout: base_timeout,
            min_timeout,
            max_timeout,
            stall_timeout,
            enable_dynamic_timeout: enable_dynamic,
            used_dynamic_timeout: false,
        }
    }

    fn calculate_dynamic_timeout(&mut self, file_count: usize, avg_file_size: u64) -> Duration {
        if !self.enable_dynamic_timeout {
            return self.timeout;
        }

        self.used_dynamic_timeout = true;

        let file_factor = (file_count as f64).sqrt() * 0.01;
        let size_factor = if avg_file_size > 0 {
            (avg_file_size as f64).log(10.0) * 0.05
        } else {
            0.0
        };

        let multiplier = 1.0 + file_factor + size_factor;
        let adjusted_timeout = self.timeout.mul_f64(multiplier.min(5.0));
        let clamped_timeout = adjusted_timeout.max(self.min_timeout).min(self.max_timeout);

        debug!(
            event = EVENT_CAPACITY_SCAN_DYNAMIC_TIMEOUT,
            component = LOG_COMPONENT_CAPACITY,
            subsystem = LOG_SUBSYSTEM_SCAN,
            state = "calculated",
            file_count,
            avg_file_size,
            multiplier,
            base_timeout_secs = self.timeout.as_secs(),
            adjusted_timeout_secs = adjusted_timeout.as_secs(),
            clamped_timeout_secs = clamped_timeout.as_secs(),
            "capacity scan dynamic timeout calculated"
        );

        clamped_timeout
    }

    fn update_and_check_timeout(&mut self, files_processed: usize, avg_file_size: u64) -> Result<(), std::io::Error> {
        let elapsed = self.start_time.elapsed();
        let dynamic_timeout = if self.enable_dynamic_timeout {
            self.calculate_dynamic_timeout(files_processed, avg_file_size)
        } else {
            self.timeout
        };

        if elapsed >= dynamic_timeout {
            warn!(
                event = EVENT_CAPACITY_SCAN_TIMEOUT,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "timeout",
                file_count = files_processed,
                elapsed_ms = elapsed.as_millis() as u64,
                timeout_ms = dynamic_timeout.as_millis() as u64,
                dynamic_timeout_enabled = self.enable_dynamic_timeout,
                "capacity scan timed out"
            );

            if self.enable_dynamic_timeout {
                record_capacity_dynamic_timeout(dynamic_timeout);
            }

            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Timeout after {} files", files_processed),
            ));
        }

        let now = Instant::now();
        if now.duration_since(self.last_check) >= self.stall_timeout {
            let files_per_checkpoint = files_processed.saturating_sub(self.last_checkpoint_files);

            if files_per_checkpoint == 0 && files_processed > 0 {
                warn!(
                    event = EVENT_CAPACITY_SCAN_STALL_DETECTED,
                    component = LOG_COMPONENT_CAPACITY,
                    subsystem = LOG_SUBSYSTEM_SCAN,
                    result = "stall",
                    file_count = files_processed,
                    stall_timeout_ms = self.stall_timeout.as_millis() as u64,
                    "capacity scan stall detected"
                );

                record_capacity_stall_detected();

                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("Stall detected at {} files", files_processed),
                ));
            }

            self.last_check = now;
            self.last_checkpoint_files = files_processed;
        }

        Ok(())
    }

    /// True once at least half of the current (possibly dynamic) time budget
    /// has elapsed — the signal to stop growing the exact prefix and start
    /// sampling, so that when the timeout fires a usable sample exists even on
    /// disks too slow to enumerate the full exact prefix (backlog#1013 S03).
    fn half_budget_elapsed(&mut self, file_count: usize, avg_file_size: u64) -> bool {
        let budget = if self.enable_dynamic_timeout {
            self.calculate_dynamic_timeout(file_count, avg_file_size)
        } else {
            self.timeout
        };
        self.start_time.elapsed() >= budget / 2
    }
}

/// Scan configuration resolved from the environment, separated so the blocking
/// scanner can be exercised with explicit limits in tests.
#[derive(Debug, Clone)]
struct ScanLimits {
    max_files_threshold: usize,
    base_timeout: Duration,
    min_timeout: Duration,
    max_timeout: Duration,
    stall_timeout: Duration,
    sample_rate: usize,
    enable_dynamic_timeout: bool,
    follow_symlinks: bool,
    max_symlink_depth: u8,
}

impl ScanLimits {
    fn from_env() -> Self {
        let sample_rate = get_sample_rate();
        let effective_sample_rate = if sample_rate == 0 {
            warn!(
                event = EVENT_CAPACITY_SCAN_SAMPLING_CLAMPED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SAMPLING,
                result = "clamped",
                configured_sample_rate = 0,
                effective_sample_rate = 1,
                reason = "zero_sample_rate",
                "capacity scan sampling configuration clamped"
            );
            1
        } else {
            sample_rate
        };

        Self {
            max_files_threshold: get_max_files_threshold(),
            base_timeout: get_stat_timeout(),
            min_timeout: get_min_timeout(),
            max_timeout: get_max_timeout(),
            stall_timeout: get_stall_timeout(),
            sample_rate: effective_sample_rate,
            enable_dynamic_timeout: get_enable_dynamic_timeout(),
            follow_symlinks: get_follow_symlinks(),
            max_symlink_depth: get_max_symlink_depth(),
        }
    }
}

/// Filesystem-level used bytes for `path`, only when `path` is a dedicated
/// mount point. On a shared filesystem (several logical disks per filesystem,
/// common in dev deployments) statvfs counts unrelated data and would badly
/// overcount, so `None` is returned and callers keep scan-based estimates.
fn mount_point_used_bytes(path: &Path) -> Option<u64> {
    let parent = path.parent()?;
    let dedicated = !rustfs_utils::os::same_disk(path.to_str()?, parent.to_str()?).ok()?;
    if !dedicated {
        return None;
    }
    rustfs_utils::os::get_info(path).ok().map(|info| info.used)
}

/// Pick the timeout-fallback value from the two independent estimators.
///
/// The seen-files extrapolation guarantees at least the observed data; the
/// filesystem-level usage covers the walker's unseen tail. When both exist the
/// max wins (both are lower..upper bounds of the same truth from opposite
/// sides). With only filesystem usage, the exact prefix is still a hard floor.
/// `None` means no trustworthy estimator exists.
fn combine_timeout_estimates(seen_estimate: Option<u64>, disk_used: Option<u64>, exact_prefix_bytes: u64) -> Option<u64> {
    match (seen_estimate, disk_used) {
        (Some(seen), Some(disk)) => Some(seen.max(disk)),
        (Some(seen), None) => Some(seen),
        (None, Some(disk)) => Some(disk.max(exact_prefix_bytes)),
        (None, None) => None,
    }
}

/// Build the best available estimate when a scan times out or stalls, instead
/// of discarding the accumulated work with a hard error (backlog#1013 S03+S10).
///
/// The seen-files extrapolation only covers files the walker reached; the
/// filesystem-level usage covers the whole disk but is only trustworthy on a
/// dedicated mount. Taking the max compensates for the walker's unseen tail
/// without regressing shared-filesystem deployments. Returns `None` when
/// neither source is available (no sample and no dedicated mount) — the caller
/// then propagates the original error, as before.
#[allow(clippy::too_many_arguments)]
fn timeout_fallback_estimate(
    path: &Path,
    reason: &'static str,
    exact_prefix_bytes: u64,
    overflow_sampled_bytes: u64,
    file_count: usize,
    sampled_count: usize,
    effective_threshold: usize,
    had_partial_errors: bool,
    start_time: Instant,
) -> Option<CapacityScanResult> {
    let seen_estimate = (sampled_count > 0).then(|| {
        let overflow_count = file_count.saturating_sub(effective_threshold);
        let estimated_overflow = estimate_overflow_bytes(overflow_sampled_bytes, overflow_count as u64, sampled_count as u64);
        exact_prefix_bytes.saturating_add(estimated_overflow)
    });
    let disk_used = mount_point_used_bytes(path);
    let used_bytes = combine_timeout_estimates(seen_estimate, disk_used, exact_prefix_bytes)?;

    info!(
        event = EVENT_CAPACITY_SCAN_SAMPLING_APPLIED,
        component = LOG_COMPONENT_CAPACITY,
        subsystem = LOG_SUBSYSTEM_SAMPLING,
        result = "fallback_estimate",
        reason,
        file_count,
        exact_prefix_bytes,
        sampled_count,
        seen_estimate_bytes = seen_estimate,
        disk_used_bytes = disk_used,
        estimated_total_bytes = used_bytes,
        "capacity scan sampling applied"
    );
    record_capacity_timeout_fallback();
    record_capacity_scan_sampling(sampled_count, true);
    record_capacity_scan_mode("timeout_fallback");

    Some(CapacityScanResult {
        used_bytes,
        file_count,
        sampled_count,
        is_estimated: true,
        scan_duration: start_time.elapsed(),
        had_partial_errors,
    })
}

/// Hard wall-clock ceiling for one disk scan, enforced from async context.
///
/// The in-scan `ProgressMonitor` checks are cooperative — they only run
/// between walker entries, so a stat/readdir blocked on a dying disk or hung
/// NFS mount never returns control to them. Twice the cooperative budget
/// leaves room for a slow-but-alive walker to reach its own timeout fallback
/// first; only a truly wedged scan trips the outer ceiling.
fn outer_scan_budget(limits: &ScanLimits) -> Duration {
    limits.max_timeout.saturating_mul(2).max(Duration::from_secs(5))
}

async fn get_dir_size_async(path: &Path) -> Result<CapacityScanResult, std::io::Error> {
    let path = path.to_path_buf();
    let limits = ScanLimits::from_env();
    let budget = outer_scan_budget(&limits);

    // The blocking thread cannot be killed; on ceiling expiry the caller (and
    // with it the refresh singleflight) is released with an error while
    // `cancelled` asks the walker to exit at its next entry, bounding the
    // thread leak to the single wedged syscall (backlog#1017 S02).
    let cancelled = Arc::new(AtomicBool::new(false));
    let scan_cancelled = cancelled.clone();
    let scan = tokio::task::spawn_blocking(move || scan_dir_blocking(&path, &limits, &scan_cancelled));

    match tokio::time::timeout(budget, scan).await {
        Ok(join_result) => join_result.map_err(std::io::Error::other)?,
        Err(_) => {
            cancelled.store(true, Ordering::Relaxed);
            warn!(
                event = EVENT_CAPACITY_SCAN_HARD_TIMEOUT,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "hard_timeout",
                budget_ms = budget.as_millis() as u64,
                "capacity scan exceeded hard wall-clock budget; blocking walker asked to stop"
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("capacity scan exceeded hard wall-clock budget of {budget:?}"),
            ))
        }
    }
}

fn scan_dir_blocking(path: &Path, limits: &ScanLimits, cancelled: &AtomicBool) -> Result<CapacityScanResult, std::io::Error> {
    let ScanLimits {
        max_files_threshold,
        base_timeout,
        min_timeout,
        max_timeout,
        stall_timeout,
        sample_rate: effective_sample_rate,
        enable_dynamic_timeout,
        follow_symlinks,
        max_symlink_depth,
    } = *limits;
    {
        if !path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Directory not found: {:?}", path),
            ));
        }

        let start_time = Instant::now();
        let mut exact_prefix_bytes = 0u64;
        let mut overflow_sampled_bytes = 0u64;
        let mut file_count = 0usize;
        let mut sampled_count = 0usize;
        let mut had_partial_errors = false;
        let mut last_progress_check_files = 0usize;
        // Lowered from the configured threshold when half the time budget
        // elapses before the exact prefix fills (early sampling entry).
        let mut effective_threshold = max_files_threshold;

        let mut symlink_tracker = SymlinkTracker::new(max_symlink_depth);
        let mut progress_monitor =
            ProgressMonitor::new(base_timeout, min_timeout, max_timeout, stall_timeout, enable_dynamic_timeout);

        let walker = WalkDir::new(path)
            .follow_links(follow_symlinks)
            .follow_root_links(follow_symlinks)
            .into_iter();

        for entry_result in walker {
            if cancelled.load(Ordering::Relaxed) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("capacity scan cancelled after hard budget expiry ({file_count} files seen)"),
                ));
            }

            let entry = match entry_result {
                Ok(entry) => entry,
                Err(err) => {
                    warn!(
                        event = EVENT_CAPACITY_SCAN_TRAVERSAL_FAILED,
                        component = LOG_COMPONENT_CAPACITY,
                        subsystem = LOG_SUBSYSTEM_SCAN,
                        result = "partial",
                        root_path = ?path,
                        file_count,
                        error = %err,
                        "capacity scan traversal failed"
                    );
                    had_partial_errors = true;
                    continue;
                }
            };

            if follow_symlinks
                && entry.path_is_symlink()
                && let Ok(target) = std::fs::read_link(entry.path())
                && symlink_tracker.should_follow(&target, entry.depth().min(u8::MAX as usize) as u8)
            {
                symlink_tracker.record_symlink(target, 0);
            }

            let file_type = entry.file_type();
            if file_type.is_dir() {
                continue;
            }

            if file_type.is_symlink() || !file_type.is_file() {
                continue;
            }

            let metadata = match entry.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    if is_tmp_trash_metadata_not_found(path, entry.path(), err.io_error().map(|err| err.kind())) {
                        debug!(
                            event = EVENT_CAPACITY_SCAN_METADATA_FAILED,
                            component = LOG_COMPONENT_CAPACITY,
                            subsystem = LOG_SUBSYSTEM_SCAN,
                            result = "ignored",
                            reason = "tmp_trash_not_found",
                            entry_path = ?entry.path(),
                            file_count,
                            "capacity scan ignored tmp trash metadata race"
                        );
                        continue;
                    }

                    warn!(
                        event = EVENT_CAPACITY_SCAN_METADATA_FAILED,
                        component = LOG_COMPONENT_CAPACITY,
                        subsystem = LOG_SUBSYSTEM_SCAN,
                        result = "partial",
                        entry_path = ?entry.path(),
                        file_count,
                        error = %err,
                        "capacity scan metadata failed"
                    );
                    had_partial_errors = true;
                    continue;
                }
            };

            file_count += 1;
            let exact_count = file_count.min(effective_threshold);
            let avg_size = if exact_count > 0 {
                exact_prefix_bytes / exact_count as u64
            } else {
                0
            };

            let should_check_progress =
                file_count == 1 || file_count.saturating_sub(last_progress_check_files) >= CAPACITY_PROGRESS_CHECK_STRIDE;

            if should_check_progress {
                if let Err(e) = progress_monitor.update_and_check_timeout(file_count, avg_size) {
                    if let Some(result) = timeout_fallback_estimate(
                        path,
                        "timeout_or_stall",
                        exact_prefix_bytes,
                        overflow_sampled_bytes,
                        file_count,
                        sampled_count,
                        effective_threshold,
                        had_partial_errors,
                        start_time,
                    ) {
                        return Ok(result);
                    }
                    return Err(e);
                }

                // Half the time budget is gone and the exact prefix hasn't
                // filled: freeze the threshold at the current position so the
                // remaining budget collects overflow samples. Without this, a
                // disk too slow to enumerate the full prefix within the budget
                // reaches the timeout with sampled_count == 0 and used to lose
                // the whole scan (backlog#1013 S03).
                if file_count < effective_threshold && progress_monitor.half_budget_elapsed(file_count, avg_size) {
                    effective_threshold = file_count;
                    info!(
                        event = EVENT_CAPACITY_SCAN_SAMPLING_APPLIED,
                        component = LOG_COMPONENT_CAPACITY,
                        subsystem = LOG_SUBSYSTEM_SAMPLING,
                        result = "early_sampling",
                        reason = "time_budget",
                        file_count,
                        exact_prefix_bytes,
                        frozen_threshold = effective_threshold,
                        configured_threshold = max_files_threshold,
                        "capacity scan sampling applied"
                    );
                    record_capacity_scan_mode("early_sampling");
                }

                last_progress_check_files = file_count;
            }

            if file_count <= effective_threshold {
                exact_prefix_bytes += metadata.len();
            } else {
                let overflow_index = file_count - effective_threshold;
                if overflow_index.is_multiple_of(effective_sample_rate) {
                    overflow_sampled_bytes += metadata.len();
                    sampled_count += 1;
                }

                if file_count.is_multiple_of(100_000) {
                    debug!(
                        event = EVENT_CAPACITY_SCAN_SAMPLING_APPLIED,
                        component = LOG_COMPONENT_CAPACITY,
                        subsystem = LOG_SUBSYSTEM_SAMPLING,
                        state = "progress",
                        file_count,
                        exact_prefix_bytes,
                        sampled_count,
                        sampled_overflow_bytes = overflow_sampled_bytes,
                        "capacity scan sampling progress"
                    );
                }
            }
        }

        if file_count > last_progress_check_files {
            let exact_count = file_count.min(effective_threshold);
            let avg_size = if exact_count > 0 {
                exact_prefix_bytes / exact_count as u64
            } else {
                0
            };

            if let Err(e) = progress_monitor.update_and_check_timeout(file_count, avg_size) {
                if let Some(result) = timeout_fallback_estimate(
                    path,
                    "final_timeout_or_stall",
                    exact_prefix_bytes,
                    overflow_sampled_bytes,
                    file_count,
                    sampled_count,
                    effective_threshold,
                    had_partial_errors,
                    start_time,
                ) {
                    return Ok(result);
                }
                return Err(e);
            }
        }

        let (symlink_count, symlink_size) = symlink_tracker.get_stats();
        if symlink_count > 0 {
            info!(
                event = EVENT_CAPACITY_SCAN_SYMLINK_SUMMARY,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "observed",
                symlink_count,
                tracked_bytes = symlink_size,
                "capacity scan symlink summary"
            );
        }

        if file_count > effective_threshold && sampled_count > 0 {
            let overflow_count = file_count - effective_threshold;
            let estimated_overflow = estimate_overflow_bytes(overflow_sampled_bytes, overflow_count as u64, sampled_count as u64);
            let estimated_size = exact_prefix_bytes.saturating_add(estimated_overflow);
            info!(
                event = EVENT_CAPACITY_SCAN_SAMPLING_APPLIED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SAMPLING,
                result = "estimated",
                reason = "overflow_sampling",
                file_count,
                threshold = effective_threshold,
                exact_prefix_bytes,
                overflow_count,
                sampled_count,
                estimated_overflow_bytes = estimated_overflow,
                estimated_total_bytes = estimated_size,
                "capacity scan sampling applied"
            );
            record_capacity_scan_sampling(sampled_count, true);
            record_capacity_scan_mode("estimated");
            Ok(CapacityScanResult {
                used_bytes: estimated_size,
                file_count,
                sampled_count,
                is_estimated: true,
                scan_duration: start_time.elapsed(),
                had_partial_errors,
            })
        } else if file_count > effective_threshold {
            let overflow_count = file_count - effective_threshold;
            let exact_prefix_count = file_count.min(effective_threshold) as u64;
            let avg_prefix_size = exact_prefix_bytes.checked_div(exact_prefix_count).unwrap_or(0);
            let estimated_overflow = avg_prefix_size.saturating_mul(overflow_count as u64);
            let estimated_size = exact_prefix_bytes.saturating_add(estimated_overflow);
            info!(
                event = EVENT_CAPACITY_SCAN_SAMPLING_APPLIED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SAMPLING,
                result = "estimated",
                reason = "prefix_average",
                file_count,
                threshold = effective_threshold,
                exact_prefix_bytes,
                overflow_count,
                sampled_count = 0,
                avg_prefix_size,
                estimated_overflow_bytes = estimated_overflow,
                estimated_total_bytes = estimated_size,
                "capacity scan sampling applied"
            );
            record_capacity_scan_sampling(0, true);
            record_capacity_scan_mode("estimated");
            Ok(CapacityScanResult {
                used_bytes: estimated_size,
                file_count,
                sampled_count: 0,
                is_estimated: true,
                scan_duration: start_time.elapsed(),
                had_partial_errors,
            })
        } else {
            record_capacity_scan_sampling(0, false);
            debug!(
                event = EVENT_CAPACITY_SCAN_EXACT_COMPLETED,
                component = LOG_COMPONENT_CAPACITY,
                subsystem = LOG_SUBSYSTEM_SCAN,
                result = "exact",
                file_count,
                used_bytes = exact_prefix_bytes,
                duration_ms = start_time.elapsed().as_millis() as u64,
                "capacity scan exact completed"
            );
            record_capacity_scan_mode("exact");
            Ok(CapacityScanResult {
                used_bytes: exact_prefix_bytes,
                file_count,
                sampled_count,
                is_estimated: false,
                scan_duration: start_time.elapsed(),
                had_partial_errors,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity_manager::{DataSource, HybridStrategyConfig, create_isolated_manager};
    use crate::capacity_scope::{CapacityScope, CapacityScopeDisk};
    #[cfg(unix)]
    use rustfs_config::ENV_CAPACITY_FOLLOW_SYMLINKS;
    use serial_test::serial;

    /// Reference implementation using unbounded `u128` arithmetic, clamped to
    /// `u64::MAX`, used as the source of truth for the sampling extrapolation.
    fn reference_estimate(overflow_sampled_bytes: u64, overflow_count: u64, sampled_count: u64) -> u64 {
        let denom = (sampled_count.max(1)) as u128;
        ((overflow_sampled_bytes as u128 * overflow_count as u128) / denom).min(u64::MAX as u128) as u64
    }

    #[test]
    fn test_estimate_overflow_bytes_small_values_unchanged() {
        // Typical, non-overflowing inputs: result must match the plain
        // `bytes * count / sampled` computation exactly.
        let overflow_sampled_bytes = 5_000_000u64;
        let overflow_count = 1_000_000u64;
        let sampled_count = 5_000u64;
        let expected = overflow_sampled_bytes * overflow_count / sampled_count;
        assert_eq!(estimate_overflow_bytes(overflow_sampled_bytes, overflow_count, sampled_count), expected);
        assert_eq!(
            estimate_overflow_bytes(overflow_sampled_bytes, overflow_count, sampled_count),
            reference_estimate(overflow_sampled_bytes, overflow_count, sampled_count)
        );
    }

    #[test]
    fn test_estimate_overflow_bytes_realistic_large_disk() {
        // backlog#1012 scenario: ~60M files, ~1.75 MiB average (~105 TB). The old
        // `saturating_mul` path saturates to u64::MAX before dividing and reports
        // roughly half the true value; the u128 path stays correct.
        let overflow_sampled_bytes = 549_000_000_000u64; // ~5.49e11
        let overflow_count = 59_800_000u64;
        let sampled_count = 299_000u64;

        // Old behaviour: product overflows u64, saturates, then divides.
        let saturated = overflow_sampled_bytes.saturating_mul(overflow_count) / sampled_count;
        assert!(
            overflow_sampled_bytes.checked_mul(overflow_count).is_none(),
            "test inputs must overflow u64 to exercise the regression"
        );

        let expected = reference_estimate(overflow_sampled_bytes, overflow_count, sampled_count);
        let actual = estimate_overflow_bytes(overflow_sampled_bytes, overflow_count, sampled_count);
        assert_eq!(actual, expected);
        // The fixed estimate must be substantially larger than the saturated garbage.
        assert!(actual > saturated * 3 / 2, "fixed estimate {actual} should dwarf saturated {saturated}");
    }

    #[test]
    fn test_estimate_overflow_bytes_not_monotonically_shrinking() {
        // The core defect: with saturation, a *larger* disk (bigger sampled_count
        // and proportionally bigger bytes/count) reports a *smaller* total. Verify
        // the fix scales monotonically instead of collapsing.
        // Disk A and disk B have identical average file size; B is 10x bigger, so
        // B's estimate must be ~10x A's, never smaller.
        let a = estimate_overflow_bytes(2_000_000_000, 6_000_000, 30_000);
        let b = estimate_overflow_bytes(20_000_000_000, 60_000_000, 300_000);
        assert!(b > a, "larger disk estimate {b} must exceed smaller disk {a}");
        assert_eq!(b, reference_estimate(20_000_000_000, 60_000_000, 300_000));
    }

    #[test]
    fn test_estimate_overflow_bytes_extreme_values_clamp() {
        // Both factors near the u64 ceiling: u128 product is astronomically larger
        // than u64::MAX, so the result must clamp to u64::MAX without panicking.
        let huge = u64::MAX - 1;
        assert_eq!(estimate_overflow_bytes(huge, huge, 1), u64::MAX);
        assert_eq!(estimate_overflow_bytes(huge, huge, 1), reference_estimate(huge, huge, 1));
    }

    #[test]
    fn test_estimate_overflow_bytes_zero_sampled_count_guarded() {
        // `sampled_count == 0` must not divide by zero; `.max(1)` guards it.
        assert_eq!(estimate_overflow_bytes(100, 10, 0), 1_000);
        assert_eq!(estimate_overflow_bytes(0, 10, 0), 0);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_empty_directory() {
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 0);
        assert_eq!(size.file_count, 0);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_single_file() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"Hello, World!").unwrap();
        drop(file);

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 13);
        assert_eq!(size.file_count, 1);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_multiple_files() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("file_{}.txt", i));
            let mut file = File::create(&file_path).unwrap();
            file.write_all(b"test").unwrap();
        }

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 40);
        assert_eq!(size.file_count, 10);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_nested_directories() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let subdir = temp_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        let mut f1 = File::create(&file1).unwrap();
        f1.write_all(b"content1").unwrap();
        drop(f1);

        let file2 = subdir.join("file2.txt");
        let mut f2 = File::create(&file2).unwrap();
        f2.write_all(b"content2").unwrap();
        drop(f2);

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 16);
        assert_eq!(size.file_count, 2);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_dir_size_async_nonexistent_directory() {
        let result = get_dir_size_async(Path::new("/nonexistent/path")).await;
        assert!(result.is_err());
    }

    #[test]
    fn test_tmp_trash_metadata_not_found_predicate_accepts_trash_not_found() {
        let scan_root = Path::new("/disk");
        let entry_path = scan_root
            .join(RUSTFS_META_BUCKET)
            .join(RUSTFS_META_TMP_DIR)
            .join(RUSTFS_META_TMP_TRASH_DIR)
            .join("cleanup-id")
            .join("part.1");

        assert!(is_tmp_trash_metadata_not_found(
            scan_root,
            &entry_path,
            Some(std::io::ErrorKind::NotFound)
        ));
    }

    #[test]
    fn test_tmp_trash_metadata_not_found_predicate_rejects_non_trash_cases() {
        let scan_root = Path::new("/disk");
        let ordinary_object = scan_root.join("bucket").join("object").join("part.1");
        let tmp_non_trash = scan_root
            .join(RUSTFS_META_BUCKET)
            .join(RUSTFS_META_TMP_DIR)
            .join("upload-id")
            .join("part.1");
        let outside_scan_root = Path::new("/outside")
            .join(RUSTFS_META_BUCKET)
            .join(RUSTFS_META_TMP_DIR)
            .join(RUSTFS_META_TMP_TRASH_DIR)
            .join("cleanup-id")
            .join("part.1");
        let trash_permission_denied = scan_root
            .join(RUSTFS_META_BUCKET)
            .join(RUSTFS_META_TMP_DIR)
            .join(RUSTFS_META_TMP_TRASH_DIR)
            .join("cleanup-id")
            .join("part.1");

        assert!(!is_tmp_trash_metadata_not_found(
            scan_root,
            &ordinary_object,
            Some(std::io::ErrorKind::NotFound)
        ));
        assert!(!is_tmp_trash_metadata_not_found(
            scan_root,
            &tmp_non_trash,
            Some(std::io::ErrorKind::NotFound)
        ));
        assert!(!is_tmp_trash_metadata_not_found(
            scan_root,
            &outside_scan_root,
            Some(std::io::ErrorKind::NotFound)
        ));
        assert!(!is_tmp_trash_metadata_not_found(
            scan_root,
            &trash_permission_denied,
            Some(std::io::ErrorKind::PermissionDenied)
        ));
    }

    #[tokio::test]
    async fn test_calculate_data_dir_used_capacity_returns_partial_success() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(b"Hello, World!").unwrap();
        drop(file);

        let disks = vec![
            CapacityDiskRef {
                endpoint: "disk-1".to_string(),
                drive_path: temp_dir.path().to_string_lossy().into_owned(),
            },
            CapacityDiskRef {
                endpoint: "disk-2".to_string(),
                drive_path: "/nonexistent/path".to_string(),
            },
        ];

        let result = calculate_data_dir_used_capacity(&disks).await.unwrap();
        assert_eq!(result.used_bytes, 13);
        assert_eq!(result.file_count, 1);
        assert!(result.had_partial_errors);
    }

    #[test]
    fn test_combine_timeout_estimates_covers_all_sources() {
        // Both estimators available: max wins in either order.
        assert_eq!(combine_timeout_estimates(Some(100), Some(300), 40), Some(300));
        assert_eq!(combine_timeout_estimates(Some(500), Some(300), 40), Some(500));
        // Seen-files extrapolation alone (shared filesystem, statvfs untrusted).
        assert_eq!(combine_timeout_estimates(Some(100), None, 40), Some(100));
        // Filesystem usage alone: the exact prefix is still a hard floor.
        assert_eq!(combine_timeout_estimates(None, Some(300), 40), Some(300));
        assert_eq!(combine_timeout_estimates(None, Some(30), 40), Some(40));
        // No estimator: caller must propagate the original error.
        assert_eq!(combine_timeout_estimates(None, None, 40), None);
    }

    #[test]
    fn test_mount_point_used_bytes_rejects_shared_filesystem_path() {
        // A tempdir lives on the same filesystem as its parent, so statvfs
        // must not be trusted as a per-disk proxy there.
        let temp_dir = tempfile::TempDir::new().unwrap();
        assert_eq!(mount_point_used_bytes(temp_dir.path()), None);
    }

    #[test]
    fn test_half_budget_elapsed_boundary() {
        let budget = Duration::from_secs(60);
        let mut monitor = ProgressMonitor::new(budget, Duration::from_secs(1), budget, Duration::from_secs(600), false);
        assert!(!monitor.half_budget_elapsed(10, 1024));

        // Backdate the start beyond half the budget: sampling must engage.
        monitor.start_time = Instant::now() - Duration::from_secs(31);
        assert!(monitor.half_budget_elapsed(10, 1024));
    }

    fn tight_limits(base_timeout: Duration) -> ScanLimits {
        ScanLimits {
            max_files_threshold: 100_000,
            base_timeout,
            min_timeout: Duration::from_millis(0),
            max_timeout: base_timeout,
            stall_timeout: Duration::from_secs(600),
            sample_rate: 1,
            enable_dynamic_timeout: false,
            follow_symlinks: false,
            max_symlink_depth: 40,
        }
    }

    #[test]
    fn test_scan_dir_blocking_zero_budget_without_estimators_still_errors() {
        use std::fs::File;
        use std::io::Write;

        let temp_dir = tempfile::TempDir::new().unwrap();
        for i in 0..3 {
            let mut file = File::create(temp_dir.path().join(format!("f{i}"))).unwrap();
            file.write_all(b"x").unwrap();
        }

        // Zero budget times out at the first progress check with no samples;
        // a tempdir is not a dedicated mount, so no estimator exists and the
        // original timeout error must still surface (last-resort behavior).
        let result = scan_dir_blocking(temp_dir.path(), &tight_limits(Duration::ZERO), &AtomicBool::new(false));
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_scan_dir_blocking_stops_when_cancelled() {
        use std::fs::File;
        use std::io::Write;

        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut file = File::create(temp_dir.path().join("f0")).unwrap();
        file.write_all(b"x").unwrap();

        // A pre-cancelled scan must exit at the first walker entry instead of
        // completing, so a wedged-then-released walker doesn't keep scanning
        // long after the outer budget already failed the refresh.
        let result = scan_dir_blocking(temp_dir.path(), &tight_limits(Duration::from_secs(600)), &AtomicBool::new(true));
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    }

    #[test]
    fn test_outer_scan_budget_bounds() {
        // Twice the cooperative ceiling, and never below the 5s floor even for
        // degenerate max_timeout values.
        assert_eq!(outer_scan_budget(&tight_limits(Duration::from_secs(15))), Duration::from_secs(30));
        assert_eq!(outer_scan_budget(&tight_limits(Duration::ZERO)), Duration::from_secs(5));
    }

    #[test]
    fn test_scan_dir_blocking_generous_budget_still_exact() {
        use std::fs::File;
        use std::io::Write;

        let temp_dir = tempfile::TempDir::new().unwrap();
        for i in 0..5 {
            let mut file = File::create(temp_dir.path().join(format!("f{i}"))).unwrap();
            file.write_all(b"hello").unwrap();
        }

        let result =
            scan_dir_blocking(temp_dir.path(), &tight_limits(Duration::from_secs(600)), &AtomicBool::new(false)).unwrap();
        assert_eq!(result.used_bytes, 25);
        assert_eq!(result.file_count, 5);
        assert!(!result.is_estimated);
        assert!(!result.had_partial_errors);
    }

    #[tokio::test]
    async fn test_partial_scan_report_produces_degraded_update() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let mut file = File::create(temp_dir.path().join("test.txt")).unwrap();
        file.write_all(b"Hello, World!").unwrap();
        drop(file);

        let good_disk = CapacityDiskRef {
            endpoint: "disk-1".to_string(),
            drive_path: temp_dir.path().to_string_lossy().into_owned(),
        };
        let disks = vec![
            good_disk.clone(),
            CapacityDiskRef {
                endpoint: "disk-2".to_string(),
                drive_path: "/nonexistent/path".to_string(),
            },
        ];

        let report = calculate_data_dir_used_capacity_report(&disks).await.unwrap();
        let update = report.into_capacity_update(disks.len(), true);

        // A partial full refresh is degraded and must never replace the disk
        // cache; only the surviving disk is surfaced for cache merging.
        assert!(update.degraded);
        assert!(!update.replaces_disk_cache);
        assert_eq!(update.expected_disk_count, None);
        assert_eq!(update.per_disk.len(), 1);
        assert_eq!(update.per_disk[0].disk, disk_scope_key(&good_disk));
        assert_eq!(update.clear_dirty_disks, vec![disk_scope_key(&good_disk)]);
    }

    #[test]
    fn test_into_capacity_update_partial_failure_filters_intra_disk_errors() {
        let clean = DiskCapacityScanResult {
            disk: CapacityScopeDisk {
                endpoint: "node-a".to_string(),
                drive_path: "/tmp/disk-a".to_string(),
            },
            scan: CapacityScanResult {
                used_bytes: 100,
                file_count: 1,
                ..Default::default()
            },
        };
        let partial = DiskCapacityScanResult {
            disk: CapacityScopeDisk {
                endpoint: "node-b".to_string(),
                drive_path: "/tmp/disk-b".to_string(),
            },
            scan: CapacityScanResult {
                used_bytes: 40,
                file_count: 1,
                had_partial_errors: true,
                ..Default::default()
            },
        };
        let report = CapacityScanReport {
            summary: CapacityScanResult {
                used_bytes: 140,
                file_count: 2,
                had_partial_errors: true,
                ..Default::default()
            },
            per_disk: vec![clean.clone(), partial],
        };

        let update = report.into_capacity_update(3, true);

        // A disk whose own scan had intra-disk errors under-counts, so it must
        // not overwrite its last-known cached value.
        assert!(update.degraded);
        assert_eq!(update.per_disk.len(), 1);
        assert_eq!(update.per_disk[0].disk, clean.disk);
        assert_eq!(update.per_disk[0].used_bytes, 100);
        assert!(!update.replaces_disk_cache);
    }

    #[tokio::test]
    async fn test_select_capacity_refresh_disks_returns_full_when_disk_cache_incomplete() {
        let manager = create_isolated_manager(HybridStrategyConfig::default());
        manager
            .mark_dirty_scope(&CapacityScope {
                disks: vec![CapacityScopeDisk {
                    endpoint: "disk-1".to_string(),
                    drive_path: "/tmp/disk-1".to_string(),
                }],
            })
            .await;

        let disks = vec![
            CapacityDiskRef {
                endpoint: "disk-1".to_string(),
                drive_path: "/tmp/disk-1".to_string(),
            },
            CapacityDiskRef {
                endpoint: "disk-2".to_string(),
                drive_path: "/tmp/disk-2".to_string(),
            },
        ];

        let (selected, dirty_subset) = select_capacity_refresh_disks(manager.as_ref(), &disks).await;
        assert!(!dirty_subset);
        assert_eq!(selected.len(), 2);
    }

    #[tokio::test]
    async fn test_select_capacity_refresh_disks_returns_dirty_subset_when_cache_complete() {
        let manager = create_isolated_manager(HybridStrategyConfig::default());
        manager
            .update_capacity(
                CapacityUpdate {
                    total_used: 300,
                    file_count: 3,
                    is_estimated: false,
                    degraded: false,
                    per_disk: vec![
                        DiskCapacityUpdate {
                            disk: CapacityScopeDisk {
                                endpoint: "disk-1".to_string(),
                                drive_path: "/tmp/disk-1".to_string(),
                            },
                            used_bytes: 100,
                            file_count: 1,
                            is_estimated: false,
                        },
                        DiskCapacityUpdate {
                            disk: CapacityScopeDisk {
                                endpoint: "disk-2".to_string(),
                                drive_path: "/tmp/disk-2".to_string(),
                            },
                            used_bytes: 200,
                            file_count: 2,
                            is_estimated: false,
                        },
                    ],
                    expected_disk_count: Some(2),
                    replaces_disk_cache: true,
                    clear_dirty_disks: Vec::new(),
                },
                DataSource::RealTime,
            )
            .await;
        manager
            .mark_dirty_scope(&CapacityScope {
                disks: vec![CapacityScopeDisk {
                    endpoint: "disk-2".to_string(),
                    drive_path: "/tmp/disk-2".to_string(),
                }],
            })
            .await;

        let disks = vec![
            CapacityDiskRef {
                endpoint: "disk-1".to_string(),
                drive_path: "/tmp/disk-1".to_string(),
            },
            CapacityDiskRef {
                endpoint: "disk-2".to_string(),
                drive_path: "/tmp/disk-2".to_string(),
            },
        ];

        let (selected, dirty_subset) = select_capacity_refresh_disks(manager.as_ref(), &disks).await;
        assert!(dirty_subset);
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].endpoint, "disk-2");
        assert_eq!(selected[0].drive_path, "/tmp/disk-2");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn test_get_dir_size_async_ignores_symlink_targets_when_follow_disabled() {
        use std::fs::File;
        use std::io::Write;
        use std::os::unix::fs::symlink;
        use tempfile::TempDir;

        let scan_dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();
        let target_path = target_dir.path().join("external.txt");
        let mut file = File::create(&target_path).unwrap();
        file.write_all(b"external-bytes").unwrap();
        symlink(&target_path, scan_dir.path().join("external-link")).unwrap();

        let size = temp_env::async_with_vars([(ENV_CAPACITY_FOLLOW_SYMLINKS, Some("false"))], async {
            get_dir_size_async(scan_dir.path()).await
        })
        .await
        .unwrap();

        assert_eq!(size.used_bytes, 0);
        assert_eq!(size.file_count, 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn test_get_dir_size_async_counts_symlink_targets_when_follow_enabled() {
        use std::fs::File;
        use std::io::Write;
        use std::os::unix::fs::symlink;
        use tempfile::TempDir;

        let scan_dir = TempDir::new().unwrap();
        let target_dir = TempDir::new().unwrap();
        let target_path = target_dir.path().join("external.txt");
        let mut file = File::create(&target_path).unwrap();
        file.write_all(b"external-bytes").unwrap();
        symlink(&target_path, scan_dir.path().join("external-link")).unwrap();

        let size = temp_env::async_with_vars([(ENV_CAPACITY_FOLLOW_SYMLINKS, Some("true"))], async {
            get_dir_size_async(scan_dir.path()).await
        })
        .await
        .unwrap();

        assert_eq!(size.used_bytes, "external-bytes".len() as u64);
        assert_eq!(size.file_count, 1);
    }
}
