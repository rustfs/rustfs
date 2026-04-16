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

//! Hybrid Capacity Manager for efficient capacity statistics

use super::scan::refresh_capacity_with_scope;
use super::types::CapacityDiskRef;
use futures::FutureExt;
use rustfs_common::capacity_scope::{CapacityScope, CapacityScopeDisk, drain_global_dirty_scopes, take_capacity_scope};
use rustfs_config::{
    DEFAULT_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, DEFAULT_CAPACITY_FOLLOW_SYMLINKS, DEFAULT_CAPACITY_MAX_SYMLINK_DEPTH,
    DEFAULT_CAPACITY_MAX_TIMEOUT_SECS, DEFAULT_CAPACITY_MIN_TIMEOUT_SECS, DEFAULT_CAPACITY_SCAN_CONCURRENCY,
    DEFAULT_CAPACITY_STALL_TIMEOUT_SECS, DEFAULT_FAST_UPDATE_THRESHOLD_SECS, DEFAULT_MAX_FILES_THRESHOLD, DEFAULT_SAMPLE_RATE,
    DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS, DEFAULT_STAT_TIMEOUT_SECS, DEFAULT_WRITE_FREQUENCY_THRESHOLD,
    DEFAULT_WRITE_TRIGGER_DELAY_SECS, ENV_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, ENV_CAPACITY_FAST_UPDATE_THRESHOLD,
    ENV_CAPACITY_FOLLOW_SYMLINKS, ENV_CAPACITY_MAX_FILES_THRESHOLD, ENV_CAPACITY_MAX_SYMLINK_DEPTH, ENV_CAPACITY_MAX_TIMEOUT,
    ENV_CAPACITY_MIN_TIMEOUT, ENV_CAPACITY_SAMPLE_RATE, ENV_CAPACITY_SCAN_CONCURRENCY, ENV_CAPACITY_SCHEDULED_INTERVAL,
    ENV_CAPACITY_STALL_TIMEOUT, ENV_CAPACITY_STAT_TIMEOUT, ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD,
    ENV_CAPACITY_WRITE_TRIGGER_DELAY,
};
use rustfs_io_metrics::capacity_metrics::{
    record_capacity_current_bytes, record_capacity_dirty_disk_count, record_capacity_refresh_inflight,
    record_capacity_refresh_joiner, record_capacity_refresh_result, record_capacity_update_completed,
    record_capacity_update_failed, record_capacity_write_operation,
};
use rustfs_utils::{get_env_bool, get_env_u64, get_env_usize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{Mutex, RwLock, watch};
use tracing::{debug, info, warn};

// ============================================================================
// Configuration Functions
// ============================================================================

/// Cached capacity configuration to avoid repeated environment variable reads
#[derive(Clone, Debug)]
struct CachedCapacityConfig {
    /// Scheduled update interval
    scheduled_update_interval: Duration,
    /// Write trigger delay
    write_trigger_delay: Duration,
    /// Write frequency threshold
    write_frequency_threshold: usize,
    /// Fast update threshold
    fast_update_threshold: Duration,
    /// Max files threshold for sampling
    max_files_threshold: usize,
    /// Stat timeout
    stat_timeout: Duration,
    /// Sample rate
    sample_rate: usize,
    /// Follow symlinks flag
    follow_symlinks: bool,
    /// Max symlink depth
    max_symlink_depth: u8,
    /// Enable dynamic timeout flag
    enable_dynamic_timeout: bool,
    /// Min timeout
    min_timeout: Duration,
    /// Max timeout
    max_timeout: Duration,
    /// Stall timeout
    stall_timeout: Duration,
    /// Concurrent disk walk tasks per capacity scan
    scan_concurrency: usize,
}

impl CachedCapacityConfig {
    /// Build configuration from environment variables
    fn from_env() -> Self {
        Self {
            scheduled_update_interval: Duration::from_secs(get_env_u64(
                ENV_CAPACITY_SCHEDULED_INTERVAL,
                DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS,
            )),
            write_trigger_delay: Duration::from_secs(get_env_u64(
                ENV_CAPACITY_WRITE_TRIGGER_DELAY,
                DEFAULT_WRITE_TRIGGER_DELAY_SECS,
            )),
            write_frequency_threshold: get_env_usize(ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, DEFAULT_WRITE_FREQUENCY_THRESHOLD),
            fast_update_threshold: Duration::from_secs(get_env_u64(
                ENV_CAPACITY_FAST_UPDATE_THRESHOLD,
                DEFAULT_FAST_UPDATE_THRESHOLD_SECS,
            )),
            max_files_threshold: get_env_usize(ENV_CAPACITY_MAX_FILES_THRESHOLD, DEFAULT_MAX_FILES_THRESHOLD),
            stat_timeout: Duration::from_secs(get_env_u64(ENV_CAPACITY_STAT_TIMEOUT, DEFAULT_STAT_TIMEOUT_SECS)),
            sample_rate: get_env_usize(ENV_CAPACITY_SAMPLE_RATE, DEFAULT_SAMPLE_RATE),
            follow_symlinks: get_env_bool(ENV_CAPACITY_FOLLOW_SYMLINKS, DEFAULT_CAPACITY_FOLLOW_SYMLINKS),
            max_symlink_depth: get_env_u64(ENV_CAPACITY_MAX_SYMLINK_DEPTH, DEFAULT_CAPACITY_MAX_SYMLINK_DEPTH as u64) as u8,
            enable_dynamic_timeout: get_env_bool(ENV_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, DEFAULT_CAPACITY_ENABLE_DYNAMIC_TIMEOUT),
            min_timeout: Duration::from_secs(get_env_u64(ENV_CAPACITY_MIN_TIMEOUT, DEFAULT_CAPACITY_MIN_TIMEOUT_SECS)),
            max_timeout: Duration::from_secs(get_env_u64(ENV_CAPACITY_MAX_TIMEOUT, DEFAULT_CAPACITY_MAX_TIMEOUT_SECS)),
            stall_timeout: Duration::from_secs(get_env_u64(ENV_CAPACITY_STALL_TIMEOUT, DEFAULT_CAPACITY_STALL_TIMEOUT_SECS)),
            scan_concurrency: get_env_usize(ENV_CAPACITY_SCAN_CONCURRENCY, DEFAULT_CAPACITY_SCAN_CONCURRENCY).max(1),
        }
    }
}

/// Get cached capacity configuration (reads environment variables once)
#[cfg(not(test))]
fn get_cached_config() -> &'static CachedCapacityConfig {
    static CONFIG: std::sync::OnceLock<CachedCapacityConfig> = std::sync::OnceLock::new();
    CONFIG.get_or_init(CachedCapacityConfig::from_env)
}

#[cfg(test)]
fn get_cached_config() -> CachedCapacityConfig {
    // Don't cache in tests to allow temp_env::with_var to work
    CachedCapacityConfig::from_env()
}

/// Get scheduled update interval from environment or default
#[cfg(not(test))]
pub fn get_scheduled_update_interval() -> Duration {
    get_cached_config().scheduled_update_interval
}

/// Get scheduled update interval from environment or default (test mode)
#[cfg(test)]
pub fn get_scheduled_update_interval() -> Duration {
    get_cached_config().scheduled_update_interval
}

/// Get write trigger delay from environment or default
#[cfg(not(test))]
pub fn get_write_trigger_delay() -> Duration {
    get_cached_config().write_trigger_delay
}

/// Get write trigger delay from environment or default (test mode)
#[cfg(test)]
pub fn get_write_trigger_delay() -> Duration {
    get_cached_config().write_trigger_delay
}

/// Get write frequency threshold from environment or default
#[cfg(not(test))]
pub fn get_write_frequency_threshold() -> usize {
    get_cached_config().write_frequency_threshold
}

/// Get write frequency threshold from environment or default (test mode)
#[cfg(test)]
pub fn get_write_frequency_threshold() -> usize {
    get_cached_config().write_frequency_threshold
}

/// Get fast update threshold from environment or default
#[cfg(not(test))]
pub fn get_fast_update_threshold() -> Duration {
    get_cached_config().fast_update_threshold
}

/// Get fast update threshold from environment or default (test mode)
#[cfg(test)]
pub fn get_fast_update_threshold() -> Duration {
    get_cached_config().fast_update_threshold
}

/// Get max files threshold from environment or default
#[cfg(not(test))]
pub fn get_max_files_threshold() -> usize {
    get_cached_config().max_files_threshold
}

/// Get max files threshold from environment or default (test mode)
#[cfg(test)]
pub fn get_max_files_threshold() -> usize {
    get_cached_config().max_files_threshold
}

/// Get stat timeout from environment or default
#[cfg(not(test))]
pub fn get_stat_timeout() -> Duration {
    get_cached_config().stat_timeout
}

/// Get stat timeout from environment or default (test mode)
#[cfg(test)]
pub fn get_stat_timeout() -> Duration {
    get_cached_config().stat_timeout
}

/// Get sample rate from environment or default
#[cfg(not(test))]
pub fn get_sample_rate() -> usize {
    get_cached_config().sample_rate
}

/// Get sample rate from environment or default (test mode)
#[cfg(test)]
pub fn get_sample_rate() -> usize {
    get_cached_config().sample_rate
}

/// Get follow symlinks flag from environment or default
#[cfg(not(test))]
pub fn get_follow_symlinks() -> bool {
    get_cached_config().follow_symlinks
}

/// Get follow symlinks flag from environment or default (test mode)
#[cfg(test)]
pub fn get_follow_symlinks() -> bool {
    get_cached_config().follow_symlinks
}

/// Get max symlink depth from environment or default
#[cfg(not(test))]
pub fn get_max_symlink_depth() -> u8 {
    get_cached_config().max_symlink_depth
}

/// Get max symlink depth from environment or default (test mode)
#[cfg(test)]
pub fn get_max_symlink_depth() -> u8 {
    get_cached_config().max_symlink_depth
}

/// Get enable dynamic timeout flag from environment or default
#[cfg(not(test))]
pub fn get_enable_dynamic_timeout() -> bool {
    get_cached_config().enable_dynamic_timeout
}

/// Get enable dynamic timeout flag from environment or default (test mode)
#[cfg(test)]
pub fn get_enable_dynamic_timeout() -> bool {
    get_cached_config().enable_dynamic_timeout
}

/// Get min timeout from environment or default
#[cfg(not(test))]
pub fn get_min_timeout() -> Duration {
    get_cached_config().min_timeout
}

/// Get min timeout from environment or default (test mode)
#[cfg(test)]
pub fn get_min_timeout() -> Duration {
    get_cached_config().min_timeout
}

/// Get max timeout from environment or default
#[cfg(not(test))]
pub fn get_max_timeout() -> Duration {
    get_cached_config().max_timeout
}

/// Get max timeout from environment or default (test mode)
#[cfg(test)]
pub fn get_max_timeout() -> Duration {
    get_cached_config().max_timeout
}

/// Get stall timeout from environment or default
#[cfg(not(test))]
pub fn get_stall_timeout() -> Duration {
    get_cached_config().stall_timeout
}

/// Get stall timeout from environment or default (test mode)
#[cfg(test)]
pub fn get_stall_timeout() -> Duration {
    get_cached_config().stall_timeout
}

/// Get per-scan disk walk concurrency from environment or default.
///
/// Lower values reduce peak iowait when multiple data directories share the
/// same physical disk (e.g. `/data/rustfs{0..3}` on a single PVC). The scan
/// loop always clamps the effective value to `[1, number_of_disks]`.
#[cfg(not(test))]
pub fn get_scan_concurrency() -> usize {
    get_cached_config().scan_concurrency
}

/// Get per-scan disk walk concurrency from environment or default (test mode)
#[cfg(test)]
pub fn get_scan_concurrency() -> usize {
    get_cached_config().scan_concurrency
}

// ============================================================================
// Data Structures
// ============================================================================

/// Cached capacity data
#[derive(Clone, Debug)]
pub struct CachedCapacity {
    /// Total used capacity in bytes
    pub total_used: u64,
    /// Last update time
    pub last_update: Instant,
    /// File count (optional)
    pub file_count: usize,
    /// Whether it's an estimated value
    pub is_estimated: bool,
    /// Data source
    pub source: DataSource,
}

/// Structured capacity update payload.
#[derive(Clone, Debug)]
pub struct CapacityUpdate {
    /// Total used capacity in bytes.
    pub total_used: u64,
    /// Number of files observed during scan.
    pub file_count: usize,
    /// Whether the value is estimated instead of exact.
    pub is_estimated: bool,
    /// Per-disk breakdown captured from a successful refresh.
    pub per_disk: Vec<DiskCapacityUpdate>,
    /// Expected disk count for a complete disk cache.
    pub expected_disk_count: Option<usize>,
    /// Whether this update should replace the current disk cache.
    pub replaces_disk_cache: bool,
    /// Dirty disks that can be cleared after the update is committed.
    pub clear_dirty_disks: Vec<CapacityScopeDisk>,
}

impl CapacityUpdate {
    /// Create an exact capacity update.
    pub fn exact(total_used: u64, file_count: usize) -> Self {
        Self {
            total_used,
            file_count,
            is_estimated: false,
            per_disk: Vec::new(),
            expected_disk_count: None,
            replaces_disk_cache: false,
            clear_dirty_disks: Vec::new(),
        }
    }

    /// Create an estimated capacity update.
    pub fn estimated(total_used: u64, file_count: usize) -> Self {
        Self {
            total_used,
            file_count,
            is_estimated: true,
            per_disk: Vec::new(),
            expected_disk_count: None,
            replaces_disk_cache: false,
            clear_dirty_disks: Vec::new(),
        }
    }

    /// Create a fallback capacity update.
    pub fn fallback(total_used: u64) -> Self {
        Self {
            total_used,
            file_count: 0,
            is_estimated: true,
            per_disk: Vec::new(),
            expected_disk_count: None,
            replaces_disk_cache: false,
            clear_dirty_disks: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DiskCapacityUpdate {
    pub disk: CapacityScopeDisk,
    pub used_bytes: u64,
    pub file_count: usize,
    pub is_estimated: bool,
}

#[derive(Clone, Debug)]
struct CachedDiskCapacity {
    used_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Copy, Eq)]
pub enum DataSource {
    /// Real-time statistics
    RealTime,
    /// Scheduled update
    Scheduled,
    /// Write triggered
    WriteTriggered,
    /// Fallback value
    #[allow(dead_code)]
    Fallback,
}

impl DataSource {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::RealTime => "realtime",
            Self::Scheduled => "scheduled",
            Self::WriteTriggered => "write_triggered",
            Self::Fallback => "fallback",
        }
    }
}

const WRITE_WINDOW_SECS: u64 = 60;
const WRITE_WINDOW_BUCKETS: usize = WRITE_WINDOW_SECS as usize;

#[derive(Clone, Copy, Debug, Default)]
struct WriteBucket {
    second: u64,
    count: usize,
}

/// Write record for tracking write operations
#[derive(Debug)]
pub struct WriteRecord {
    /// Last write time
    pub last_write_time: Option<Instant>,
    /// Write count
    pub write_count: usize,
    /// Fixed-size time buckets for the recent write window.
    write_buckets: [WriteBucket; WRITE_WINDOW_BUCKETS],
}

impl WriteRecord {
    fn current_unix_second() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs()
    }

    fn recent_write_count(&self, now_second: u64) -> usize {
        self.write_buckets
            .iter()
            .filter(|bucket| {
                bucket.count > 0 && bucket.second <= now_second && now_second.saturating_sub(bucket.second) < WRITE_WINDOW_SECS
            })
            .map(|bucket| bucket.count)
            .sum()
    }

    fn record_write(&mut self, now: Instant) -> usize {
        let now_second = Self::current_unix_second();
        let bucket_idx = (now_second % WRITE_WINDOW_BUCKETS as u64) as usize;
        let bucket = &mut self.write_buckets[bucket_idx];

        if bucket.second != now_second {
            *bucket = WriteBucket {
                second: now_second,
                count: 0,
            };
        }

        bucket.count = bucket.count.saturating_add(1);
        self.last_write_time = Some(now);
        self.write_count = self.write_count.saturating_add(1);

        self.recent_write_count(now_second)
    }
}

/// Hybrid strategy configuration
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HybridStrategyConfig {
    /// Scheduled update interval
    pub scheduled_update_interval: Duration,
    /// Write trigger delay
    pub write_trigger_delay: Duration,
    /// Write frequency threshold (writes/minute)
    pub write_frequency_threshold: usize,
    /// Fast update threshold
    pub fast_update_threshold: Duration,
    /// Enable smart update
    pub enable_smart_update: bool,
    /// Enable write trigger
    pub enable_write_trigger: bool,
}

impl Default for HybridStrategyConfig {
    fn default() -> Self {
        Self {
            scheduled_update_interval: get_scheduled_update_interval(),
            write_trigger_delay: get_write_trigger_delay(),
            write_frequency_threshold: get_write_frequency_threshold(),
            fast_update_threshold: get_fast_update_threshold(),
            enable_smart_update: true,
            enable_write_trigger: true,
        }
    }
}

impl HybridStrategyConfig {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        Self::default()
    }
}

// ============================================================================
// Hybrid Capacity Manager
// ============================================================================

struct RefreshState {
    running: bool,
    /// Sender for the current refresh cycle. Joiners subscribe to this before releasing the
    /// mutex so they cannot miss the completion notification. A new channel is created at the
    /// start of every refresh cycle so stale subscribers from previous cycles are not confused
    /// by results that were already published.
    result_tx: watch::Sender<Option<Result<CapacityUpdate, String>>>,
}

impl Default for RefreshState {
    fn default() -> Self {
        let (tx, _) = watch::channel(None);
        Self {
            running: false,
            result_tx: tx,
        }
    }
}

/// Hybrid capacity manager
pub struct HybridCapacityManager {
    /// Capacity cache
    cache: Arc<RwLock<Option<CachedCapacity>>>,
    /// Write record
    write_record: Arc<RwLock<WriteRecord>>,
    /// Dirty disks recorded from write-side scope propagation.
    dirty_disks: Arc<RwLock<HashSet<CapacityScopeDisk>>>,
    /// Per-disk cache populated after a successful full refresh and updated by dirty subset refreshes.
    disk_cache: Arc<RwLock<HashMap<CapacityScopeDisk, CachedDiskCapacity>>>,
    /// Whether the per-disk cache currently covers all known disks.
    disk_cache_complete: Arc<RwLock<bool>>,
    /// Configuration
    config: HybridStrategyConfig,
    /// Shared singleflight refresh state
    refresh_state: Arc<Mutex<RefreshState>>,
}

impl HybridCapacityManager {
    async fn sync_global_dirty_scopes(&self) {
        let scopes = drain_global_dirty_scopes();
        if scopes.is_empty() {
            return;
        }

        let mut dirty_disks = self.dirty_disks.write().await;
        dirty_disks.extend(scopes);
        record_capacity_dirty_disk_count(dirty_disks.len());
    }

    fn max_stale_age(&self) -> Duration {
        self.config
            .scheduled_update_interval
            .max(self.config.fast_update_threshold.checked_mul(3).unwrap_or(Duration::MAX))
    }

    /// Create a new hybrid capacity manager
    pub fn new(config: HybridStrategyConfig) -> Self {
        Self {
            cache: Arc::new(RwLock::new(None)),
            write_record: Arc::new(RwLock::new(WriteRecord {
                last_write_time: None,
                write_count: 0,
                write_buckets: [WriteBucket::default(); WRITE_WINDOW_BUCKETS],
            })),
            dirty_disks: Arc::new(RwLock::new(HashSet::new())),
            disk_cache: Arc::new(RwLock::new(HashMap::new())),
            disk_cache_complete: Arc::new(RwLock::new(false)),
            config,
            refresh_state: Arc::new(Mutex::new(RefreshState::default())),
        }
    }

    /// Create with default config from environment
    pub fn from_env() -> Self {
        Self::new(HybridStrategyConfig::from_env())
    }

    /// Get capacity (core method)
    pub async fn get_capacity(&self) -> Option<CachedCapacity> {
        let cache = self.cache.read().await;
        cache.clone()
    }

    /// Update capacity
    pub async fn update_capacity(&self, update: CapacityUpdate, source: DataSource) {
        let start = Instant::now();
        let mut total_used = update.total_used;

        if !update.per_disk.is_empty() {
            let mut disk_cache = self.disk_cache.write().await;
            let mut disk_cache_complete = self.disk_cache_complete.write().await;

            if update.replaces_disk_cache && update.expected_disk_count == Some(update.per_disk.len()) {
                disk_cache.clear();
                for entry in &update.per_disk {
                    disk_cache.insert(
                        entry.disk.clone(),
                        CachedDiskCapacity {
                            used_bytes: entry.used_bytes,
                        },
                    );
                }
                *disk_cache_complete = true;
                total_used = disk_cache.values().map(|entry| entry.used_bytes).sum();
            } else if *disk_cache_complete {
                for entry in &update.per_disk {
                    disk_cache.insert(
                        entry.disk.clone(),
                        CachedDiskCapacity {
                            used_bytes: entry.used_bytes,
                        },
                    );
                }
                total_used = disk_cache.values().map(|entry| entry.used_bytes).sum();
            }
        }

        let mut cache = self.cache.write().await;
        *cache = Some(CachedCapacity {
            total_used,
            last_update: Instant::now(),
            file_count: update.file_count,
            is_estimated: update.is_estimated,
            source,
        });

        if !update.clear_dirty_disks.is_empty() {
            let mut dirty_disks = self.dirty_disks.write().await;
            for disk in &update.clear_dirty_disks {
                dirty_disks.remove(disk);
            }
            record_capacity_dirty_disk_count(dirty_disks.len());
        }

        debug!(
            "Capacity updated: {} bytes, files={}, estimated={}, source: {:?}",
            total_used, update.file_count, update.is_estimated, source
        );
        record_capacity_current_bytes(total_used);
        record_capacity_update_completed(source.as_metric_label(), start.elapsed(), total_used, update.is_estimated);
    }

    /// Record write operation
    pub async fn record_write_operation(&self) {
        let mut record = self.write_record.write().await;
        let now = Instant::now();
        let recent_write_count = record.record_write(now);

        record_capacity_write_operation(recent_write_count);
        debug!(
            "Write operation recorded: total writes = {}, recent writes = {}",
            record.write_count, recent_write_count
        );
    }

    /// Record write scope propagated from the storage layer.
    pub async fn mark_dirty_scope(&self, scope: &CapacityScope) {
        if scope.disks.is_empty() {
            return;
        }

        let mut dirty_disks = self.dirty_disks.write().await;
        dirty_disks.extend(scope.disks.iter().cloned());
        record_capacity_dirty_disk_count(dirty_disks.len());
    }

    /// Record a write operation and consume any propagated disk scope bound to the token.
    pub async fn record_write_operation_with_scope_token(&self, scope_token: Option<uuid::Uuid>) {
        if let Some(token) = scope_token
            && let Some(scope) = take_capacity_scope(token)
        {
            self.mark_dirty_scope(&scope).await;
        }

        self.record_write_operation().await;
    }

    /// Check if fast update is needed
    pub async fn needs_fast_update(&self) -> bool {
        if !self.config.enable_smart_update {
            return false;
        }

        let cache = self.cache.read().await;
        if let Some(cached) = cache.as_ref() {
            let cache_age = cached.last_update.elapsed();

            // Cache is fresh, no need to update
            if cache_age < self.config.fast_update_threshold {
                return false;
            }

            if !self.config.enable_write_trigger {
                return false;
            }

            let write_record = self.write_record.read().await;
            let write_frequency = write_record.recent_write_count(WriteRecord::current_unix_second());
            if write_frequency <= self.config.write_frequency_threshold {
                return false;
            }

            if let Some(last_write_time) = write_record.last_write_time {
                let time_since_write = last_write_time.elapsed();

                if time_since_write < self.config.write_trigger_delay {
                    debug!(
                        "Write-triggered refresh still debounced ({:?} ago, trigger_delay={:?}, writes/min={})",
                        time_since_write, self.config.write_trigger_delay, write_frequency
                    );
                    return false;
                }

                debug!(
                    "Write-triggered refresh eligible after debounce ({:?} ago, trigger_delay={:?}, writes/min={})",
                    time_since_write, self.config.write_trigger_delay, write_frequency
                );
                return true;
            }
        }

        false
    }

    /// Get cache age
    #[allow(dead_code)]
    pub async fn get_cache_age(&self) -> Option<Duration> {
        let cache = self.cache.read().await;
        cache.as_ref().map(|c| c.last_update.elapsed())
    }

    /// Get write frequency (writes/minute)
    #[allow(dead_code)]
    pub async fn get_write_frequency(&self) -> usize {
        let record = self.write_record.read().await;
        record.recent_write_count(WriteRecord::current_unix_second())
    }

    /// Snapshot the currently dirty disks recorded from write-side scope propagation.
    pub async fn get_dirty_disks(&self) -> Vec<CapacityScopeDisk> {
        self.sync_global_dirty_scopes().await;
        let dirty_disks = self.dirty_disks.read().await;
        dirty_disks.iter().cloned().collect()
    }

    /// Returns true if the manager has a complete per-disk cache and can safely refresh only dirty disks.
    pub async fn can_refresh_dirty_subset(&self) -> bool {
        *self.disk_cache_complete.read().await
    }

    /// Run a singleflight refresh. Callers either join an existing in-flight refresh or become the leader.
    ///
    /// Joiners subscribe to the watch channel *before* releasing the mutex, which guarantees
    /// they cannot miss the completion notification even if the leader finishes very quickly.
    pub async fn refresh_or_join<F, Fut>(&self, source: DataSource, refresh_fn: F) -> Result<CapacityUpdate, String>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<CapacityUpdate, String>>,
    {
        let maybe_rx = {
            let mut state = self.refresh_state.lock().await;
            if state.running {
                // Subscribe while holding the lock so the send that completes the current
                // refresh cycle cannot happen before we are subscribed.
                record_capacity_refresh_joiner(source.as_metric_label());
                Some(state.result_tx.subscribe())
            } else {
                // Become the leader. Create a fresh channel so that joiners from a previous
                // cycle cannot observe the result that was published for the new cycle.
                let (tx, _) = watch::channel(None);
                state.result_tx = tx;
                state.running = true;
                record_capacity_refresh_inflight(1);
                None
            }
        };

        if let Some(mut result_rx) = maybe_rx {
            // Wait until the leader publishes Some(result). Because we subscribed before
            // releasing the mutex, we cannot miss the notification.
            if result_rx.wait_for(|v| v.is_some()).await.is_err() {
                // The leader's sender was dropped (e.g. due to a panic) without publishing
                // a result. Surface a clear error rather than silently returning the default.
                return Err("capacity refresh leader exited without publishing a result".to_string());
            }
            return result_rx
                .borrow()
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Err("capacity refresh completed without a result".to_string()));
        }

        let refresh_start = Instant::now();
        let result = AssertUnwindSafe(refresh_fn()).catch_unwind().await.unwrap_or_else(|err| {
            warn!(error = ?err, "capacity refresh function panicked");
            Err("capacity refresh panicked".to_string())
        });
        if let Ok(update) = &result {
            self.update_capacity(update.clone(), source).await;
        }
        let refresh_duration = refresh_start.elapsed();
        if result.is_err() {
            record_capacity_update_failed(source.as_metric_label());
        }
        record_capacity_refresh_result(
            source.as_metric_label(),
            if result.is_ok() { "success" } else { "error" },
            refresh_duration,
        );

        {
            let mut state = self.refresh_state.lock().await;
            state.running = false;
            record_capacity_refresh_inflight(0);
            let _ = state.result_tx.send(Some(result.clone()));
        }

        result
    }

    /// Start a background refresh if one is not already in flight.
    pub async fn spawn_refresh_if_needed<F, Fut>(self: Arc<Self>, source: DataSource, refresh_fn: F) -> bool
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<CapacityUpdate, String>> + Send + 'static,
    {
        let should_spawn = {
            let mut state = self.refresh_state.lock().await;
            if state.running {
                false
            } else {
                let (tx, _) = watch::channel(None);
                state.result_tx = tx;
                state.running = true;
                record_capacity_refresh_inflight(1);
                true
            }
        };

        if !should_spawn {
            return false;
        }

        tokio::spawn(async move {
            let refresh_start = Instant::now();
            let result = AssertUnwindSafe(refresh_fn()).catch_unwind().await.unwrap_or_else(|err| {
                warn!(error = ?err, "capacity refresh function panicked");
                Err("capacity refresh panicked".to_string())
            });
            if let Ok(update) = &result {
                self.update_capacity(update.clone(), source).await;
            }
            let refresh_duration = refresh_start.elapsed();
            if result.is_err() {
                record_capacity_update_failed(source.as_metric_label());
            }
            record_capacity_refresh_result(
                source.as_metric_label(),
                if result.is_ok() { "success" } else { "error" },
                refresh_duration,
            );

            let mut state = self.refresh_state.lock().await;
            state.running = false;
            record_capacity_refresh_inflight(0);
            let _ = state.result_tx.send(Some(result));
        });

        true
    }

    /// Get config
    pub fn get_config(&self) -> &HybridStrategyConfig {
        &self.config
    }

    /// Check if the cache is too stale to keep serving without a foreground refresh.
    pub fn should_block_on_refresh(&self, cache_age: Duration) -> bool {
        cache_age >= self.max_stale_age()
    }

    /// Return whether a refresh is currently in flight.
    pub async fn refresh_in_progress(&self) -> bool {
        self.refresh_state.lock().await.running
    }
}

/// Global capacity manager instance
static GLOBAL_CAPACITY_MANAGER: std::sync::OnceLock<Arc<HybridCapacityManager>> = std::sync::OnceLock::new();

/// Get or initialize the global capacity manager
pub fn get_capacity_manager() -> Arc<HybridCapacityManager> {
    GLOBAL_CAPACITY_MANAGER
        .get_or_init(|| Arc::new(HybridCapacityManager::from_env()))
        .clone()
}

/// Create an isolated capacity manager instance for testing
///
/// This factory function allows tests to create independent instances
/// without affecting the global singleton, avoiding test pollution.
///
/// # Example
/// ```ignore
/// let manager = create_isolated_manager(HybridStrategyConfig::default());
/// manager
///     .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
///     .await;
/// ```
#[allow(dead_code)]
pub fn create_isolated_manager(config: HybridStrategyConfig) -> Arc<HybridCapacityManager> {
    Arc::new(HybridCapacityManager::new(config))
}

/// Start background update task
pub async fn start_background_task(disks: Vec<CapacityDiskRef>) {
    let manager = get_capacity_manager();
    let mut interval = manager.get_config().scheduled_update_interval;

    // Prevent panic in tokio::time::interval when misconfigured to 0
    if interval.is_zero() {
        warn!("RUSTFS_CAPACITY_SCHEDULED_INTERVAL is configured as 0; clamping to 1s to avoid panic");
        interval = Duration::from_secs(1);
    }

    tokio::spawn(async move {
        let mut timer = tokio::time::interval(interval);

        loop {
            timer.tick().await;

            info!("Starting scheduled capacity update");
            let start = Instant::now();
            let manager = manager.clone();
            let disks = disks.clone();
            let started = manager
                .clone()
                .spawn_refresh_if_needed(
                    DataSource::Scheduled,
                    move || async move { refresh_capacity_with_scope(disks, false).await },
                )
                .await;

            if started {
                debug!("Scheduled capacity refresh started in {:?}", start.elapsed());
            } else {
                debug!("Scheduled capacity refresh skipped because another refresh is already in progress");
            }
        }
    });
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_common::capacity_scope::{CapacityScope, CapacityScopeDisk, record_capacity_scope, record_global_dirty_scope};
    use rustfs_config::{
        ENV_CAPACITY_FAST_UPDATE_THRESHOLD, ENV_CAPACITY_MAX_FILES_THRESHOLD, ENV_CAPACITY_SAMPLE_RATE,
        ENV_CAPACITY_SCAN_CONCURRENCY, ENV_CAPACITY_STAT_TIMEOUT, ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD,
        ENV_CAPACITY_WRITE_TRIGGER_DELAY,
    };
    use serial_test::serial;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    #[serial]
    fn test_get_scheduled_update_interval() {
        let interval = get_scheduled_update_interval();
        assert_eq!(interval, Duration::from_secs(120));
    }

    #[test]
    #[serial]
    fn test_get_write_trigger_delay() {
        let delay = get_write_trigger_delay();
        assert_eq!(delay, Duration::from_secs(5));
    }

    #[test]
    #[serial]
    fn test_get_write_frequency_threshold() {
        let threshold = get_write_frequency_threshold();
        assert_eq!(threshold, 5);
    }

    #[test]
    #[serial]
    fn test_get_fast_update_threshold() {
        let threshold = get_fast_update_threshold();
        assert_eq!(threshold, Duration::from_secs(30));
    }

    #[test]
    #[serial]
    fn test_get_max_files_threshold() {
        let threshold = get_max_files_threshold();
        assert_eq!(threshold, 200_000);
    }

    #[test]
    #[serial]
    fn test_get_stat_timeout() {
        let timeout = get_stat_timeout();
        assert_eq!(timeout, Duration::from_secs(3));
    }

    #[test]
    #[serial]
    fn test_get_sample_rate() {
        let rate = get_sample_rate();
        assert_eq!(rate, 200);
    }

    #[test]
    #[serial]
    fn test_env_var_override_scheduled_interval() {
        temp_env::with_var(ENV_CAPACITY_SCHEDULED_INTERVAL, Some("600"), || {
            let interval = get_scheduled_update_interval();
            assert_eq!(interval, Duration::from_secs(600));
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_write_trigger_delay() {
        temp_env::with_var(ENV_CAPACITY_WRITE_TRIGGER_DELAY, Some("20"), || {
            let delay = get_write_trigger_delay();
            assert_eq!(delay, Duration::from_secs(20));
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_write_frequency_threshold() {
        temp_env::with_var(ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, Some("20"), || {
            let threshold = get_write_frequency_threshold();
            assert_eq!(threshold, 20);
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_fast_update_threshold() {
        temp_env::with_var(ENV_CAPACITY_FAST_UPDATE_THRESHOLD, Some("120"), || {
            let threshold = get_fast_update_threshold();
            assert_eq!(threshold, Duration::from_secs(120));
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_max_files_threshold() {
        temp_env::with_var(ENV_CAPACITY_MAX_FILES_THRESHOLD, Some("2000000"), || {
            let threshold = get_max_files_threshold();
            assert_eq!(threshold, 2_000_000);
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_stat_timeout() {
        temp_env::with_var(ENV_CAPACITY_STAT_TIMEOUT, Some("10"), || {
            let timeout = get_stat_timeout();
            assert_eq!(timeout, Duration::from_secs(10));
        });
    }

    #[test]
    #[serial]
    fn test_env_var_override_sample_rate() {
        temp_env::with_var(ENV_CAPACITY_SAMPLE_RATE, Some("200"), || {
            let rate = get_sample_rate();
            assert_eq!(rate, 200);
        });
    }

    #[test]
    #[serial]
    fn test_get_scan_concurrency_default() {
        let concurrency = get_scan_concurrency();
        assert_eq!(concurrency, 4);
    }

    #[test]
    #[serial]
    fn test_env_var_override_scan_concurrency() {
        temp_env::with_var(ENV_CAPACITY_SCAN_CONCURRENCY, Some("1"), || {
            assert_eq!(get_scan_concurrency(), 1);
        });
        temp_env::with_var(ENV_CAPACITY_SCAN_CONCURRENCY, Some("8"), || {
            assert_eq!(get_scan_concurrency(), 8);
        });
    }

    #[test]
    #[serial]
    fn test_scan_concurrency_clamps_zero_to_one() {
        temp_env::with_var(ENV_CAPACITY_SCAN_CONCURRENCY, Some("0"), || {
            assert_eq!(get_scan_concurrency(), 1, "0 must be clamped to 1 to avoid buffer_unordered panic");
        });
    }

    #[tokio::test]
    #[serial]
    async fn test_capacity_manager_creation() {
        let config = HybridStrategyConfig::default();
        let manager = HybridCapacityManager::new(config);

        assert!(manager.get_capacity().await.is_none());
    }

    #[tokio::test]
    #[serial]
    async fn test_update_capacity() {
        let manager = HybridCapacityManager::from_env();

        manager
            .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
            .await;

        let cached = manager.get_capacity().await;
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().total_used, 1000);
    }

    #[tokio::test]
    #[serial]
    async fn test_update_capacity_preserves_retrieval_metadata() {
        let manager = HybridCapacityManager::from_env();

        manager
            .update_capacity(CapacityUpdate::exact(1000, 10), DataSource::RealTime)
            .await;

        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 1000);
        assert_eq!(cached.file_count, 10);
        assert_eq!(cached.source, DataSource::RealTime);
        assert!(!cached.is_estimated);
    }

    #[tokio::test]
    #[serial]
    async fn test_record_write_operation() {
        let manager = HybridCapacityManager::from_env();

        manager.record_write_operation().await;

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_write_frequency_window() {
        let manager = HybridCapacityManager::from_env();

        for _ in 0..20 {
            manager.record_write_operation().await;
        }

        assert_eq!(manager.get_write_frequency().await, 20);
    }

    #[test]
    #[serial]
    fn test_recent_write_count_ignores_future_buckets() {
        let mut record = WriteRecord {
            last_write_time: None,
            write_count: 1,
            write_buckets: [WriteBucket::default(); WRITE_WINDOW_BUCKETS],
        };

        record.write_buckets[0] = WriteBucket { second: 120, count: 3 };
        record.write_buckets[1] = WriteBucket { second: 90, count: 2 };

        assert_eq!(
            record.recent_write_count(100),
            2,
            "buckets from future seconds should not inflate recent write frequency"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_needs_fast_update() {
        let manager = HybridCapacityManager::from_env();

        // No cache, should not need update
        assert!(!manager.needs_fast_update().await);

        // Update cache
        manager
            .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
            .await;

        // Fresh cache, should not need update
        assert!(!manager.needs_fast_update().await);
    }

    #[tokio::test]
    #[serial]
    async fn test_cache_age_tracking() {
        let manager = HybridCapacityManager::from_env();

        assert!(manager.get_cache_age().await.is_none());

        manager
            .update_capacity(CapacityUpdate::exact(1000, 1), DataSource::RealTime)
            .await;

        let age = manager.get_cache_age().await.unwrap();
        assert!(age < Duration::from_secs(1));

        tokio::time::sleep(Duration::from_millis(100)).await;

        let age = manager.get_cache_age().await.unwrap();
        assert!(age >= Duration::from_millis(100));
    }

    #[tokio::test]
    #[serial]
    async fn test_data_source_tracking() {
        let manager = HybridCapacityManager::from_env();

        for source in [
            DataSource::RealTime,
            DataSource::Scheduled,
            DataSource::WriteTriggered,
            DataSource::Fallback,
        ] {
            manager.update_capacity(CapacityUpdate::exact(1000, 1), source).await;
            assert_eq!(manager.get_capacity().await.unwrap().source, source);
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_needs_fast_update_waits_for_write_trigger_delay() {
        let manager = create_isolated_manager(HybridStrategyConfig {
            scheduled_update_interval: Duration::from_secs(60),
            write_trigger_delay: Duration::from_millis(50),
            write_frequency_threshold: 1,
            fast_update_threshold: Duration::from_millis(10),
            enable_smart_update: true,
            enable_write_trigger: true,
        });

        manager
            .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
            .await;
        tokio::time::sleep(Duration::from_millis(15)).await;

        manager.record_write_operation().await;
        manager.record_write_operation().await;
        tokio::time::sleep(Duration::from_millis(5)).await;

        assert!(
            !manager.needs_fast_update().await,
            "write-triggered refresh should wait for debounce delay after a qualifying burst"
        );

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(manager.needs_fast_update().await);
    }

    #[tokio::test]
    #[serial]
    async fn test_needs_fast_update_respects_enable_write_trigger() {
        let manager = create_isolated_manager(HybridStrategyConfig {
            scheduled_update_interval: Duration::from_secs(60),
            write_trigger_delay: Duration::from_secs(60),
            write_frequency_threshold: 1,
            fast_update_threshold: Duration::from_millis(10),
            enable_smart_update: true,
            enable_write_trigger: false,
        });

        manager
            .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
            .await;
        tokio::time::sleep(Duration::from_millis(15)).await;

        manager.record_write_operation().await;
        manager.record_write_operation().await;

        assert!(
            !manager.needs_fast_update().await,
            "write-triggered refresh should be disabled when enable_write_trigger is false"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrent_access() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let mut handles = Vec::new();

        for i in 0..10 {
            let mgr = manager.clone();
            handles.push(tokio::spawn(async move {
                mgr.update_capacity(CapacityUpdate::exact(i as u64 * 100, i), DataSource::RealTime)
                    .await;
                mgr.record_write_operation().await;
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert!(manager.get_capacity().await.is_some());
        assert_eq!(manager.get_write_frequency().await, 10);
    }

    #[tokio::test]
    #[serial]
    async fn test_performance_overhead() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let start = Instant::now();

        for i in 0..1000 {
            manager
                .update_capacity(CapacityUpdate::exact(i as u64, i), DataSource::RealTime)
                .await;
            manager.record_write_operation().await;
            let _ = manager.get_capacity().await;
        }

        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    #[serial]
    async fn test_refresh_or_join_singleflight() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let calls = Arc::new(AtomicUsize::new(0));

        let mgr1 = manager.clone();
        let calls1 = calls.clone();
        let first = tokio::spawn(async move {
            mgr1.refresh_or_join(DataSource::Scheduled, move || async move {
                calls1.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(CapacityUpdate::exact(2048, 8))
            })
            .await
        });

        tokio::time::sleep(Duration::from_millis(10)).await;

        let mgr2 = manager.clone();
        let calls2 = calls.clone();
        let second = tokio::spawn(async move {
            mgr2.refresh_or_join(DataSource::WriteTriggered, move || async move {
                calls2.fetch_add(1, Ordering::SeqCst);
                Ok(CapacityUpdate::exact(4096, 16))
            })
            .await
        });

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(first.total_used, 2048);
        assert_eq!(second.total_used, 2048);
        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 2048);
        assert_eq!(cached.file_count, 8);
    }

    #[tokio::test]
    #[serial]
    async fn test_spawn_refresh_if_needed_deduplicates_background_refresh() {
        let manager = Arc::new(HybridCapacityManager::from_env());
        let calls = Arc::new(AtomicUsize::new(0));

        let first_manager = manager.clone();
        let first_calls = calls.clone();
        let started = first_manager
            .clone()
            .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                first_calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(CapacityUpdate::estimated(8192, 32))
            })
            .await;
        assert!(started);

        let second_manager = manager.clone();
        let second_calls = calls.clone();
        let started = second_manager
            .clone()
            .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                second_calls.fetch_add(1, Ordering::SeqCst);
                Ok(CapacityUpdate::exact(1, 1))
            })
            .await;
        assert!(!started);

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!manager.refresh_in_progress().await);
        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 8192);
        assert!(cached.is_estimated);
    }

    #[tokio::test]
    #[serial]
    async fn test_record_write_operation_with_scope_token_marks_dirty_disks() {
        let manager = create_isolated_manager(HybridStrategyConfig::default());
        let token = uuid::Uuid::new_v4();
        record_capacity_scope(
            token,
            CapacityScope {
                disks: vec![CapacityScopeDisk {
                    endpoint: "node-a".to_string(),
                    drive_path: "/tmp/disk-a".to_string(),
                }],
            },
        );

        manager.record_write_operation_with_scope_token(Some(token)).await;

        let dirty_disks = manager.get_dirty_disks().await;
        assert_eq!(dirty_disks.len(), 1);
        assert_eq!(dirty_disks[0].endpoint, "node-a");
        assert_eq!(dirty_disks[0].drive_path, "/tmp/disk-a");
        assert_eq!(manager.get_write_frequency().await, 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_get_dirty_disks_drains_global_dirty_scope_registry() {
        let manager = create_isolated_manager(HybridStrategyConfig::default());
        record_global_dirty_scope(CapacityScope {
            disks: vec![CapacityScopeDisk {
                endpoint: "node-bg".to_string(),
                drive_path: "/tmp/disk-bg".to_string(),
            }],
        });

        let dirty_disks = manager.get_dirty_disks().await;
        assert_eq!(dirty_disks.len(), 1);
        assert_eq!(dirty_disks[0].endpoint, "node-bg");
        assert_eq!(dirty_disks[0].drive_path, "/tmp/disk-bg");

        let second_read = manager.get_dirty_disks().await;
        assert_eq!(second_read.len(), 1);
    }

    #[tokio::test]
    #[serial]
    async fn test_update_capacity_recomputes_total_from_disk_cache_for_subset_refresh() {
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
                                endpoint: "node-a".to_string(),
                                drive_path: "/tmp/disk-a".to_string(),
                            },
                            used_bytes: 100,
                            file_count: 1,
                            is_estimated: false,
                        },
                        DiskCapacityUpdate {
                            disk: CapacityScopeDisk {
                                endpoint: "node-b".to_string(),
                                drive_path: "/tmp/disk-b".to_string(),
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
            .update_capacity(
                CapacityUpdate {
                    total_used: 150,
                    file_count: 1,
                    is_estimated: false,
                    per_disk: vec![DiskCapacityUpdate {
                        disk: CapacityScopeDisk {
                            endpoint: "node-a".to_string(),
                            drive_path: "/tmp/disk-a".to_string(),
                        },
                        used_bytes: 150,
                        file_count: 1,
                        is_estimated: false,
                    }],
                    expected_disk_count: Some(1),
                    replaces_disk_cache: false,
                    clear_dirty_disks: Vec::new(),
                },
                DataSource::WriteTriggered,
            )
            .await;

        let cached = manager.get_capacity().await.unwrap();
        assert_eq!(cached.total_used, 350);
    }

    #[tokio::test]
    #[serial]
    async fn test_config_from_env() {
        let config = HybridStrategyConfig::from_env();

        // Check default values
        assert_eq!(config.scheduled_update_interval, Duration::from_secs(120));
        assert_eq!(config.write_trigger_delay, Duration::from_secs(5));
        assert_eq!(config.write_frequency_threshold, 5);
        assert_eq!(config.fast_update_threshold, Duration::from_secs(30));
        assert!(config.enable_smart_update);
        assert!(config.enable_write_trigger);
    }

    #[tokio::test]
    #[serial]
    async fn test_config_from_env_with_override() {
        temp_env::with_var(ENV_CAPACITY_SCHEDULED_INTERVAL, Some("600"), || {
            let config = HybridStrategyConfig::from_env();
            assert_eq!(config.scheduled_update_interval, Duration::from_secs(600));
        });
    }
}
