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
use futures::{StreamExt, stream};
use rustfs_common::capacity_scope::CapacityScopeDisk;
use rustfs_io_metrics::capacity_metrics::{
    record_capacity_dynamic_timeout, record_capacity_scan_disk, record_capacity_scan_mode, record_capacity_scan_sampling,
    record_capacity_stall_detected, record_capacity_symlink, record_capacity_timeout_fallback,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use walkdir::WalkDir;

const MAX_CAPACITY_SCAN_CONCURRENCY: usize = 4;
const CAPACITY_PROGRESS_CHECK_STRIDE: usize = 512;

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

fn disk_scope_key(disk: &CapacityDiskRef) -> CapacityScopeDisk {
    CapacityScopeDisk {
        endpoint: disk.endpoint.clone(),
        drive_path: disk.drive_path.clone(),
    }
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
                    "Data directory {} size: {} bytes, files={}, sampled={}, estimated={}, duration={:?}",
                    outcome.drive_path, scan.used_bytes, scan.file_count, scan.sampled_count, scan.is_estimated, outcome.duration
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
                warn!("Failed to get size for directory {}: {:?}", outcome.drive_path, e);
                has_failure = true;
            }
        }
    }

    if !has_success {
        return Err("All directories failed to calculate size".into());
    }

    if has_failure {
        warn!("Some directories failed to calculate size, result may be incomplete");
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
            debug!("Symlink depth limit reached: {} >= {}, not following {:?}", depth, self.max_depth, path);
            return false;
        }

        if self.visited.contains(path) {
            warn!("Circular symlink reference detected: {:?}, skipping", path);
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
            "Dynamic timeout calculation: files={}, avg_size={}, multiplier={:.2}, base_timeout={:?}, adjusted_timeout={:?}, clamped_timeout={:?}",
            file_count, avg_file_size, multiplier, self.timeout, adjusted_timeout, clamped_timeout
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
                "Directory size calculation timeout after {} files, elapsed: {:?}, timeout: {:?}",
                files_processed, elapsed, dynamic_timeout
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
                    "No progress detected for {:?}, possible stall at {} files",
                    self.stall_timeout, files_processed
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

    fn record_timeout_fallback(&self) {
        record_capacity_timeout_fallback();
    }
}

async fn get_dir_size_async(path: &Path) -> Result<CapacityScanResult, std::io::Error> {
    let path = path.to_path_buf();

    let max_files_threshold = get_max_files_threshold();
    let base_timeout = get_stat_timeout();
    let min_timeout = get_min_timeout();
    let max_timeout = get_max_timeout();
    let stall_timeout = get_stall_timeout();
    let sample_rate = get_sample_rate();
    let enable_dynamic_timeout = get_enable_dynamic_timeout();
    let follow_symlinks = get_follow_symlinks();
    let max_symlink_depth = get_max_symlink_depth();

    let effective_sample_rate = if sample_rate == 0 {
        warn!("Invalid sampling configuration: sample_rate=0. Clamping to 1 to avoid panic.");
        1
    } else {
        sample_rate
    };

    tokio::task::spawn_blocking(move || {
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

        let mut symlink_tracker = SymlinkTracker::new(max_symlink_depth);
        let mut progress_monitor =
            ProgressMonitor::new(base_timeout, min_timeout, max_timeout, stall_timeout, enable_dynamic_timeout);

        let walker = WalkDir::new(&path)
            .follow_links(follow_symlinks)
            .follow_root_links(follow_symlinks)
            .into_iter();

        for entry_result in walker {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(err) => {
                    warn!("Failed to traverse directory entry under {:?}: {}", path, err);
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
                    warn!("Failed to get metadata for {:?}: {}", entry.path(), err);
                    had_partial_errors = true;
                    continue;
                }
            };

            file_count += 1;
            let exact_count = file_count.min(max_files_threshold);
            let avg_size = if exact_count > 0 {
                exact_prefix_bytes / exact_count as u64
            } else {
                0
            };

            let should_check_progress =
                file_count == 1 || file_count.saturating_sub(last_progress_check_files) >= CAPACITY_PROGRESS_CHECK_STRIDE;

            if should_check_progress && let Err(e) = progress_monitor.update_and_check_timeout(file_count, avg_size) {
                if sampled_count > 0 {
                    let overflow_count = file_count.saturating_sub(max_files_threshold);
                    let estimated_overflow = overflow_sampled_bytes.saturating_mul(overflow_count as u64) / sampled_count as u64;
                    let estimated_total = exact_prefix_bytes.saturating_add(estimated_overflow);
                    info!(
                        "Timeout/stall at {} files, using sampled estimate: exact_prefix={} overflow_estimate={} sampled={}",
                        file_count, exact_prefix_bytes, estimated_overflow, sampled_count
                    );
                    progress_monitor.record_timeout_fallback();
                    record_capacity_scan_sampling(sampled_count, true);
                    record_capacity_scan_mode("timeout_fallback");
                    return Ok(CapacityScanResult {
                        used_bytes: estimated_total,
                        file_count,
                        sampled_count,
                        is_estimated: true,
                        scan_duration: start_time.elapsed(),
                        had_partial_errors,
                    });
                }
                return Err(e);
            }
            if should_check_progress {
                last_progress_check_files = file_count;
            }

            if file_count <= max_files_threshold {
                exact_prefix_bytes += metadata.len();
            } else {
                let overflow_index = file_count - max_files_threshold;
                if overflow_index.is_multiple_of(effective_sample_rate) {
                    overflow_sampled_bytes += metadata.len();
                    sampled_count += 1;
                }

                if file_count.is_multiple_of(100_000) {
                    debug!(
                        "Processed {} files, exact_prefix_bytes={}, sampled_overflow={} files/{} bytes",
                        file_count, exact_prefix_bytes, sampled_count, overflow_sampled_bytes
                    );
                }
            }
        }

        if file_count > last_progress_check_files {
            let exact_count = file_count.min(max_files_threshold);
            let avg_size = if exact_count > 0 {
                exact_prefix_bytes / exact_count as u64
            } else {
                0
            };

            if let Err(e) = progress_monitor.update_and_check_timeout(file_count, avg_size) {
                if sampled_count > 0 {
                    let overflow_count = file_count.saturating_sub(max_files_threshold);
                    let estimated_overflow = overflow_sampled_bytes.saturating_mul(overflow_count as u64) / sampled_count as u64;
                    let estimated_total = exact_prefix_bytes.saturating_add(estimated_overflow);
                    info!(
                        "Timeout/stall at {} files during final check, using sampled estimate: exact_prefix={} overflow_estimate={} sampled={}",
                        file_count, exact_prefix_bytes, estimated_overflow, sampled_count
                    );
                    progress_monitor.record_timeout_fallback();
                    record_capacity_scan_sampling(sampled_count, true);
                    record_capacity_scan_mode("timeout_fallback");
                    return Ok(CapacityScanResult {
                        used_bytes: estimated_total,
                        file_count,
                        sampled_count,
                        is_estimated: true,
                        scan_duration: start_time.elapsed(),
                        had_partial_errors,
                    });
                }
                return Err(e);
            }
        }

        let (symlink_count, symlink_size) = symlink_tracker.get_stats();
        if symlink_count > 0 {
            info!(
                "Symlink tracking: {} symlinks processed, total tracked size: {} bytes",
                symlink_count, symlink_size
            );
        }

        if file_count > max_files_threshold && sampled_count > 0 {
            let overflow_count = file_count - max_files_threshold;
            let estimated_overflow = overflow_sampled_bytes.saturating_mul(overflow_count as u64) / sampled_count as u64;
            let estimated_size = exact_prefix_bytes.saturating_add(estimated_overflow);
            info!(
                "Large directory detected: {} files, estimated size: {} bytes (exact prefix: {}, sampled overflow {}/{})",
                file_count, estimated_size, exact_prefix_bytes, sampled_count, overflow_count
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
        } else if file_count > max_files_threshold {
            let overflow_count = file_count - max_files_threshold;
            let exact_prefix_count = file_count.min(max_files_threshold) as u64;
            let avg_prefix_size = exact_prefix_bytes.checked_div(exact_prefix_count).unwrap_or(0);
            let estimated_overflow = avg_prefix_size.saturating_mul(overflow_count as u64);
            let estimated_size = exact_prefix_bytes.saturating_add(estimated_overflow);
            info!(
                "Large directory detected: {} files, estimated size: {} bytes (no overflow samples, used prefix average {} bytes/file)",
                file_count, estimated_size, avg_prefix_size
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
                "Directory size calculation completed: {} files, {} bytes, took {:?}",
                file_count,
                exact_prefix_bytes,
                start_time.elapsed()
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
    })
    .await
    .map_err(std::io::Error::other)?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capacity_manager::{DataSource, HybridStrategyConfig, create_isolated_manager};
    use rustfs_common::capacity_scope::{CapacityScope, CapacityScopeDisk};
    #[cfg(unix)]
    use rustfs_config::ENV_CAPACITY_FOLLOW_SYMLINKS;
    use serial_test::serial;

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
