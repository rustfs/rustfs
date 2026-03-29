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

//! Admin application use-case contracts.

use crate::app::context::{AppContext, get_global_app_context};
use crate::capacity::capacity_manager::{
    CapacityUpdate, DataSource, get_capacity_manager, get_enable_dynamic_timeout, get_follow_symlinks, get_max_files_threshold,
    get_max_symlink_depth, get_max_timeout, get_min_timeout, get_sample_rate, get_stall_timeout, get_stat_timeout,
};
use crate::error::ApiError;
use rustfs_common::data_usage::DataUsageInfo;
use rustfs_ecstore::admin_server_info::get_server_info;
use rustfs_ecstore::data_usage::load_data_usage_from_backend;
use rustfs_ecstore::endpoints::EndpointServerPools;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::pools::{PoolStatus, get_total_usable_capacity, get_total_usable_capacity_free};
use rustfs_ecstore::store_api::StorageAPI;
use rustfs_io_metrics::{
    record_capacity_dynamic_timeout, record_capacity_scan_sampling, record_capacity_stall_detected, record_capacity_symlink,
    record_capacity_timeout_fallback,
};
use rustfs_madmin::{InfoMessage, StorageInfo};
use s3s::S3ErrorCode;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

pub type AdminUsecaseResult<T> = Result<T, ApiError>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryServerInfoRequest {
    pub include_pools: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CapacityScanResult {
    pub used_bytes: u64,
    pub file_count: usize,
    pub sampled_count: usize,
    pub is_estimated: bool,
    pub scan_duration: Duration,
    pub had_partial_errors: bool,
}

impl CapacityScanResult {
    fn with_partial_errors(mut self) -> Self {
        self.had_partial_errors = true;
        self
    }

    pub(crate) fn to_capacity_update(self) -> CapacityUpdate {
        if self.is_estimated {
            CapacityUpdate::estimated(self.used_bytes, self.file_count)
        } else {
            CapacityUpdate::exact(self.used_bytes, self.file_count)
        }
    }
}

pub struct QueryServerInfoResponse {
    pub info: InfoMessage,
}

impl std::fmt::Debug for QueryServerInfoResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryServerInfoResponse").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DependencyReadiness {
    pub storage_ready: bool,
    pub iam_ready: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QueryPoolStatusRequest {
    pub pool: String,
    pub by_id: bool,
}

/// Calculate actual used capacity of all data directories
pub(crate) async fn calculate_data_dir_used_capacity(
    disks: &[rustfs_madmin::Disk],
) -> Result<CapacityScanResult, Box<dyn std::error::Error + Send + Sync>> {
    let start = Instant::now();
    let mut total_used = 0u64;
    let mut total_files = 0usize;
    let mut total_sampled = 0usize;
    let mut has_failure = false;
    let mut has_success = false;
    let mut is_estimated = false;

    for disk in disks {
        let path = Path::new(&disk.drive_path);

        if !path.exists() {
            warn!("Data directory does not exist: {}", disk.drive_path);
            has_failure = true;
            continue;
        }

        match get_dir_size_async(path).await {
            Ok(scan) => {
                debug!(
                    "Data directory {} size: {} bytes, files={}, sampled={}, estimated={}",
                    disk.drive_path, scan.used_bytes, scan.file_count, scan.sampled_count, scan.is_estimated
                );
                total_used += scan.used_bytes;
                total_files += scan.file_count;
                total_sampled += scan.sampled_count;
                is_estimated |= scan.is_estimated;
                has_failure |= scan.had_partial_errors;
                has_success = true;
            }
            Err(e) => {
                warn!("Failed to get size for directory {}: {:?}", disk.drive_path, e);
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

    let mut result = CapacityScanResult {
        used_bytes: total_used,
        file_count: total_files,
        sampled_count: total_sampled,
        is_estimated,
        scan_duration: start.elapsed(),
        had_partial_errors: false,
    };

    if has_failure {
        result = result.with_partial_errors();
    }

    Ok(result)
}

// ============================================================================
// Symlink Tracker for Circular Reference Detection
// ============================================================================

/// Tracker for symlink resolution with circular reference detection
struct SymlinkTracker {
    /// Set of visited symlink paths to detect circular references
    visited: HashSet<PathBuf>,
    /// Count of symlinks encountered
    symlink_count: usize,
    /// Total size of symlink targets
    symlink_size: u64,
    /// Maximum symlink depth to follow
    max_depth: u8,
}

impl SymlinkTracker {
    /// Create a new symlink tracker
    fn new(max_depth: u8) -> Self {
        Self {
            visited: HashSet::new(),
            symlink_count: 0,
            symlink_size: 0,
            max_depth,
        }
    }

    /// Check if we should follow a symlink at the given depth
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

    /// Record a visited symlink path and update metrics
    fn record_symlink(&mut self, path: PathBuf, size: u64) {
        self.visited.insert(path);
        self.symlink_count += 1;
        self.symlink_size += size;
        record_capacity_symlink(size);
    }

    /// Get symlink statistics
    fn get_stats(&self) -> (usize, u64) {
        (self.symlink_count, self.symlink_size)
    }
}

// ============================================================================
// Progress Monitor for Timeout and Stall Detection
// ============================================================================

/// Monitor for directory traversal progress with timeout and stall detection
struct ProgressMonitor {
    /// Start time of the operation
    start_time: Instant,
    /// Last check time for stall detection
    last_check: Instant,
    /// Number of files processed at last checkpoint
    last_checkpoint_files: usize,
    /// Base timeout for this operation
    timeout: Duration,
    /// Minimum allowed timeout
    min_timeout: Duration,
    /// Maximum allowed timeout
    max_timeout: Duration,
    /// Stall detection timeout
    stall_timeout: Duration,
    /// Enable dynamic timeout calculation
    enable_dynamic_timeout: bool,
    /// Track if dynamic timeout was used
    used_dynamic_timeout: bool,
}

impl ProgressMonitor {
    /// Create a new progress monitor
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

    /// Calculate dynamic timeout based on directory characteristics
    fn calculate_dynamic_timeout(&mut self, file_count: usize, avg_file_size: u64) -> Duration {
        if !self.enable_dynamic_timeout {
            return self.timeout;
        }

        // Mark that we're using dynamic timeout
        self.used_dynamic_timeout = true;

        // Calculate multipliers based on directory characteristics
        let file_factor = (file_count as f64).sqrt() * 0.01; // File count influence
        let size_factor = if avg_file_size > 0 {
            (avg_file_size as f64).log(10.0) * 0.05 // File size influence
        } else {
            0.0
        };

        let multiplier = 1.0 + file_factor + size_factor;
        let adjusted_timeout = self.timeout.mul_f64(multiplier.min(5.0)); // Max 5x multiplier

        // Clamp to min/max bounds
        let clamped_timeout = adjusted_timeout.max(self.min_timeout).min(self.max_timeout);

        debug!(
            "Dynamic timeout calculation: files={}, avg_size={}, multiplier={:.2}, base_timeout={:?}, adjusted_timeout={:?}, clamped_timeout={:?}",
            file_count, avg_file_size, multiplier, self.timeout, adjusted_timeout, clamped_timeout
        );

        clamped_timeout
    }

    /// Update and check for timeout or stall
    fn update_and_check_timeout(&mut self, files_processed: usize, avg_file_size: u64) -> Result<(), std::io::Error> {
        let elapsed = self.start_time.elapsed();

        // Calculate dynamic timeout based on current state
        let dynamic_timeout = if self.enable_dynamic_timeout {
            self.calculate_dynamic_timeout(files_processed, avg_file_size)
        } else {
            self.timeout
        };

        // Check for hard timeout
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

        // Check for stall (no progress)
        let now = Instant::now();
        if now.duration_since(self.last_check) >= self.stall_timeout {
            let files_per_checkpoint = files_processed.saturating_sub(self.last_checkpoint_files);

            if files_per_checkpoint == 0 && files_processed > 0 {
                // No progress for stall_timeout duration
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

    /// Record timeout fallback to sampling
    fn record_timeout_fallback(&self) {
        record_capacity_timeout_fallback();
    }
}

/// Asynchronously get directory size with enhanced symlink handling and dynamic timeout
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

    if !path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Directory not found: {:?}", path),
        ));
    }

    tokio::task::spawn_blocking(move || {
        let start_time = Instant::now();
        let mut exact_prefix_bytes = 0u64;
        let mut overflow_sampled_bytes = 0u64;
        let mut file_count = 0usize;
        let mut sampled_count = 0usize;
        let mut had_partial_errors = false;

        let mut symlink_tracker = if follow_symlinks {
            Some(SymlinkTracker::new(max_symlink_depth))
        } else {
            None
        };

        let mut progress_monitor =
            ProgressMonitor::new(base_timeout, min_timeout, max_timeout, stall_timeout, enable_dynamic_timeout);

        let mut walker_builder = WalkDir::new(&path);
        if !follow_symlinks {
            walker_builder = walker_builder.follow_links(false);
        }
        let walker = walker_builder.into_iter();

        for entry_result in walker {
            let entry = match entry_result {
                Ok(entry) => entry,
                Err(err) => {
                    warn!("Failed to traverse directory entry under {:?}: {}", path, err);
                    had_partial_errors = true;
                    continue;
                }
            };

            let metadata = match entry.metadata() {
                Ok(meta) => meta,
                Err(err) => {
                    warn!("Failed to get metadata for {:?}: {}", entry.path(), err);
                    had_partial_errors = true;
                    continue;
                }
            };

            if metadata.is_symlink() {
                if let Some(ref mut tracker) = symlink_tracker
                    && let Ok(target) = std::fs::read_link(entry.path())
                    && tracker.should_follow(&target, 0)
                {
                    tracker.record_symlink(target, metadata.len());
                }
                continue;
            }

            if !metadata.is_file() {
                continue;
            }

            file_count += 1;
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
                        "Timeout/stall at {} files, using sampled estimate: exact_prefix={} overflow_estimate={} sampled={}",
                        file_count, exact_prefix_bytes, estimated_overflow, sampled_count
                    );
                    progress_monitor.record_timeout_fallback();
                    record_capacity_scan_sampling(sampled_count, true);
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

        if let Some(tracker) = symlink_tracker {
            let (count, size) = tracker.get_stats();
            if count > 0 {
                info!("Symlink tracking: {} symlinks processed, total target size: {} bytes", count, size);
            }
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
            Ok(CapacityScanResult {
                used_bytes: estimated_size,
                file_count,
                sampled_count,
                is_estimated: true,
                scan_duration: start_time.elapsed(),
                had_partial_errors,
            })
        } else if file_count > max_files_threshold {
            // sampled_count == 0: too few overflow files to reach the sample rate threshold.
            // Fall back to estimating the overflow using the average file size from the exact
            // prefix so that overflow files are not silently dropped from the total.
            let overflow_count = file_count - max_files_threshold;
            // Use the actual number of files counted in the exact prefix, not the threshold
            // value, to avoid a divide-by-zero or incorrect average when fewer files were
            // processed than max_files_threshold.
            let exact_prefix_count = file_count.min(max_files_threshold) as u64;
            let avg_prefix_size = if exact_prefix_count > 0 {
                exact_prefix_bytes / exact_prefix_count
            } else {
                0
            };
            let estimated_overflow = avg_prefix_size.saturating_mul(overflow_count as u64);
            let estimated_size = exact_prefix_bytes.saturating_add(estimated_overflow);
            info!(
                "Large directory detected: {} files, estimated size: {} bytes (no overflow samples, used prefix average {} bytes/file)",
                file_count, estimated_size, avg_prefix_size
            );
            record_capacity_scan_sampling(0, true);
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

#[derive(Clone, Default)]
pub struct DefaultAdminUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultAdminUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    fn endpoints(&self) -> Option<EndpointServerPools> {
        self.context.as_ref().and_then(|context| context.endpoints().handle())
    }

    fn app_error(code: S3ErrorCode, message: impl Into<String>) -> ApiError {
        ApiError {
            code,
            message: message.into(),
            source: None,
        }
    }

    fn app_error_default(code: S3ErrorCode) -> ApiError {
        let message = ApiError::error_code_to_message(&code);
        Self::app_error(code, message)
    }

    pub async fn execute_query_server_info(&self, req: QueryServerInfoRequest) -> AdminUsecaseResult<QueryServerInfoResponse> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let info = get_server_info(req.include_pools).await;
        Ok(QueryServerInfoResponse { info })
    }

    pub async fn execute_query_storage_info(&self) -> AdminUsecaseResult<StorageInfo> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        Ok(store.storage_info().await)
    }

    pub async fn execute_query_data_usage_info(&self) -> AdminUsecaseResult<DataUsageInfo> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let mut info = load_data_usage_from_backend(store.clone()).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            Self::app_error(S3ErrorCode::InternalError, "load_data_usage_from_backend failed")
        })?;

        let storage_info = store.storage_info().await;

        // Keep the same capacity correction behavior as the previous admin handler implementation.
        const MAX_REASONABLE_CAPACITY: u64 = 100_000 * 1024 * 1024 * 1024 * 1024; // 100 PiB
        const MIN_REASONABLE_CAPACITY: u64 = 1024 * 1024 * 1024; // 1 GiB

        let total_u64 = get_total_usable_capacity(&storage_info.disks, &storage_info) as u64;
        let free_u64 = get_total_usable_capacity_free(&storage_info.disks, &storage_info) as u64;

        if total_u64 > MAX_REASONABLE_CAPACITY {
            error!(
                "Abnormal total capacity detected: {} bytes ({:.2} TiB), capping to physical capacity",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(4))
            );

            let disk_count = storage_info.disks.len();
            if disk_count > 0 {
                use std::collections::HashSet;
                let unique_disks: HashSet<String> = storage_info
                    .disks
                    .iter()
                    .map(|disk| format!("{}|{}", disk.endpoint, disk.drive_path))
                    .collect();

                let actual_disk_count = unique_disks.len();

                if let Some(first_disk) = storage_info.disks.first() {
                    info.total_capacity = first_disk.total_space * actual_disk_count as u64;
                    info.total_free_capacity = first_disk.available_space * actual_disk_count as u64;

                    info!(
                        "Applied capacity correction: {} unique disks, capacity per disk: {} bytes",
                        actual_disk_count, first_disk.total_space
                    );
                } else {
                    info.total_capacity = 0;
                    info.total_free_capacity = 0;
                }
            } else {
                info.total_capacity = 0;
                info.total_free_capacity = 0;
            }
        } else if total_u64 < MIN_REASONABLE_CAPACITY && total_u64 > 0 {
            warn!(
                "Unusually small total capacity: {} bytes ({:.2} GiB)",
                total_u64,
                total_u64 as f64 / (1024.0_f64.powi(3))
            );
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        } else {
            info.total_capacity = total_u64;
            info.total_free_capacity = free_u64;
        }

        // Use hybrid strategy for capacity calculation
        let capacity_manager = get_capacity_manager();

        // Check if we have a valid cache
        if let Some(cached) = capacity_manager.get_capacity().await {
            let cache_age = cached.last_update.elapsed();
            let fast_update_threshold = capacity_manager.get_config().fast_update_threshold;

            // If cache is fresh (< fast_update_threshold), use it directly
            if cache_age < fast_update_threshold {
                info.total_used_capacity = cached.total_used;
                debug!(
                    "Using cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={})",
                    cached.total_used, cache_age, cached.source, cached.file_count, cached.is_estimated
                );
            } else {
                // Cache is stale, check if we need fast update
                let needs_update = capacity_manager.needs_fast_update().await;
                let should_block = capacity_manager.should_block_on_refresh(cache_age);

                if needs_update && should_block {
                    let start = Instant::now();
                    match capacity_manager
                        .refresh_or_join(DataSource::WriteTriggered, || async {
                            calculate_data_dir_used_capacity(&storage_info.disks)
                                .await
                                .map(|scan| scan.to_capacity_update())
                                .map_err(|e| e.to_string())
                        })
                        .await
                    {
                        Ok(update) => {
                            info.total_used_capacity = update.total_used;

                            let elapsed = start.elapsed();
                            debug!(
                                "Foreground capacity refresh completed in {:?} (files={}, estimated={})",
                                elapsed, update.file_count, update.is_estimated
                            );
                        }
                        Err(e) => {
                            warn!("Foreground capacity refresh failed: {}, using cached value", e);
                            info.total_used_capacity = cached.total_used;
                        }
                    }
                } else {
                    info.total_used_capacity = cached.total_used;
                    debug!(
                        "Using stale cached capacity: {} bytes (age: {:?}, source: {:?}, files={}, estimated={}, needs_update={}, blocking={})",
                        cached.total_used,
                        cache_age,
                        cached.source,
                        cached.file_count,
                        cached.is_estimated,
                        needs_update,
                        should_block
                    );

                    let disks = storage_info.disks.clone();
                    let manager = capacity_manager.clone();
                    if manager
                        .clone()
                        .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                            calculate_data_dir_used_capacity(&disks)
                                .await
                                .map(|scan| scan.to_capacity_update())
                                .map_err(|e| e.to_string())
                        })
                        .await
                    {
                        debug!("Background capacity update started");
                    } else {
                        debug!("Background update already in progress, skipping spawn");
                    }
                }
            }
        } else {
            // No cache, perform initial calculation
            let start = Instant::now();
            match capacity_manager
                .refresh_or_join(DataSource::RealTime, || async {
                    calculate_data_dir_used_capacity(&storage_info.disks)
                        .await
                        .map(|scan| scan.to_capacity_update())
                        .map_err(|e| e.to_string())
                })
                .await
            {
                Ok(update) => {
                    info.total_used_capacity = update.total_used;

                    let elapsed = start.elapsed();
                    info!(
                        "Initial capacity calculation completed: {} bytes in {:?} (files={}, estimated={})",
                        update.total_used, elapsed, update.file_count, update.is_estimated
                    );
                }
                Err(e) => {
                    warn!(
                        "Failed to calculate data directory used capacity: {}, falling back to disk used capacity",
                        e
                    );
                    info.total_used_capacity = info.total_capacity.saturating_sub(info.total_free_capacity);
                    capacity_manager
                        .update_capacity(CapacityUpdate::fallback(info.total_used_capacity), DataSource::Fallback)
                        .await;
                }
            }
        }
        debug!(
            "Capacity statistics: total={:.2} TiB, free={:.2} TiB, used={:.2} TiB",
            info.total_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_free_capacity as f64 / (1024.0_f64.powi(4)),
            info.total_used_capacity as f64 / (1024.0_f64.powi(4))
        );

        Ok(info)
    }

    pub async fn execute_list_pool_statuses(&self) -> AdminUsecaseResult<Vec<PoolStatus>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let mut pool_statuses = Vec::new();
        for (idx, _) in endpoints.as_ref().iter().enumerate() {
            let state = store.status(idx).await.map_err(ApiError::from)?;
            pool_statuses.push(state);
        }

        Ok(pool_statuses)
    }

    pub async fn execute_query_pool_status(&self, req: QueryPoolStatusRequest) -> AdminUsecaseResult<PoolStatus> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let Some(endpoints) = self.endpoints() else {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        };

        if endpoints.legacy() {
            return Err(Self::app_error_default(S3ErrorCode::NotImplemented));
        }

        let has_idx = if req.by_id {
            let idx = req.pool.parse::<usize>().unwrap_or_default();
            if idx < endpoints.as_ref().len() { Some(idx) } else { None }
        } else {
            endpoints.get_pool_idx(&req.pool)
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", req.pool);
            return Err(Self::app_error_default(S3ErrorCode::InvalidArgument));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(Self::app_error(S3ErrorCode::InternalError, "Not init"));
        };

        store.status(idx).await.map_err(ApiError::from)
    }

    pub fn execute_collect_dependency_readiness(&self) -> DependencyReadiness {
        let iam_ready = self
            .context
            .as_ref()
            .map(|context| {
                let _ = context.object_store();
                context.iam().is_ready()
            })
            .unwrap_or(false);

        DependencyReadiness {
            storage_ready: new_object_layer_fn().is_some(),
            iam_ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    async fn execute_query_storage_info_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultAdminUsecase::without_context();

        let err = usecase.execute_query_storage_info().await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_query_data_usage_info_returns_internal_error_when_store_uninitialized() {
        let usecase = DefaultAdminUsecase::without_context();

        let err = usecase.execute_query_data_usage_info().await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn execute_collect_dependency_readiness_returns_state_flags() {
        let usecase = DefaultAdminUsecase::without_context();

        let readiness = usecase.execute_collect_dependency_readiness();
        let _ = readiness.storage_ready;
        let _ = readiness.iam_ready;
    }

    // Tests for directory size calculation functions
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

        // Create multiple files
        for i in 0..10 {
            let file_path = temp_dir.path().join(format!("file_{}.txt", i));
            let mut file = File::create(&file_path).unwrap();
            file.write_all(b"test").unwrap();
        }

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 40); // 10 files * 4 bytes
        assert_eq!(size.file_count, 10);
    }

    #[tokio::test]
    async fn test_get_dir_size_async_nested_directories() {
        use std::fs::File;
        use std::io::Write;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();

        // Create nested directories and files
        let subdir = temp_dir.path().join("subdir");
        std::fs::create_dir(&subdir).unwrap();

        let file1 = temp_dir.path().join("file1.txt");
        let mut f1 = File::create(&file1).unwrap();
        f1.write_all(b"content1").unwrap();

        let file2 = subdir.join("file2.txt");
        let mut f2 = File::create(&file2).unwrap();
        f2.write_all(b"content2").unwrap();

        let size = get_dir_size_async(temp_dir.path()).await.unwrap();
        assert_eq!(size.used_bytes, 16); // "content1" (8) + "content2" (8)
        assert_eq!(size.file_count, 2);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_dir_size_async_nonexistent_directory() {
        let result = get_dir_size_async(Path::new("/nonexistent/path")).await;
        assert!(result.is_err());
    }
}
