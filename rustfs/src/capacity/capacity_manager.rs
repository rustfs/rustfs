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

use crate::app::admin_usecase::calculate_data_dir_used_capacity;
use futures::FutureExt;
use rustfs_config::{
    DEFAULT_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, DEFAULT_CAPACITY_FOLLOW_SYMLINKS, DEFAULT_CAPACITY_MAX_SYMLINK_DEPTH,
    DEFAULT_CAPACITY_MAX_TIMEOUT_SECS, DEFAULT_CAPACITY_MIN_TIMEOUT_SECS, DEFAULT_CAPACITY_STALL_TIMEOUT_SECS,
    DEFAULT_FAST_UPDATE_THRESHOLD_SECS, DEFAULT_MAX_FILES_THRESHOLD, DEFAULT_SAMPLE_RATE, DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS,
    DEFAULT_STAT_TIMEOUT_SECS, DEFAULT_WRITE_FREQUENCY_THRESHOLD, DEFAULT_WRITE_TRIGGER_DELAY_SECS,
    ENV_CAPACITY_ENABLE_DYNAMIC_TIMEOUT, ENV_CAPACITY_FAST_UPDATE_THRESHOLD, ENV_CAPACITY_FOLLOW_SYMLINKS,
    ENV_CAPACITY_MAX_FILES_THRESHOLD, ENV_CAPACITY_MAX_SYMLINK_DEPTH, ENV_CAPACITY_MAX_TIMEOUT, ENV_CAPACITY_MIN_TIMEOUT,
    ENV_CAPACITY_SAMPLE_RATE, ENV_CAPACITY_SCHEDULED_INTERVAL, ENV_CAPACITY_STALL_TIMEOUT, ENV_CAPACITY_STAT_TIMEOUT,
    ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, ENV_CAPACITY_WRITE_TRIGGER_DELAY,
};
use rustfs_io_metrics::{record_capacity_current_bytes, record_capacity_update_completed, record_capacity_write_operation};
use rustfs_utils::{get_env_bool, get_env_u64, get_env_usize};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};
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
}

impl CapacityUpdate {
    /// Create an exact capacity update.
    pub fn exact(total_used: u64, file_count: usize) -> Self {
        Self {
            total_used,
            file_count,
            is_estimated: false,
        }
    }

    /// Create an estimated capacity update.
    pub fn estimated(total_used: u64, file_count: usize) -> Self {
        Self {
            total_used,
            file_count,
            is_estimated: true,
        }
    }

    /// Create a fallback capacity update.
    pub fn fallback(total_used: u64) -> Self {
        Self {
            total_used,
            file_count: 0,
            is_estimated: true,
        }
    }
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
    fn as_metric_label(self) -> &'static str {
        match self {
            Self::RealTime => "realtime",
            Self::Scheduled => "scheduled",
            Self::WriteTriggered => "write_triggered",
            Self::Fallback => "fallback",
        }
    }
}

/// Write record for tracking write operations
#[derive(Debug)]
pub struct WriteRecord {
    /// Last write time
    pub last_write_time: Instant,
    /// Write count
    pub write_count: usize,
    /// Write time window (for frequency calculation)
    pub write_window: Vec<Instant>,
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
    /// Configuration
    config: HybridStrategyConfig,
    /// Shared singleflight refresh state
    refresh_state: Arc<Mutex<RefreshState>>,
}

impl HybridCapacityManager {
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
                last_write_time: Instant::now(),
                write_count: 0,
                write_window: Vec::new(),
            })),
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
        let mut cache = self.cache.write().await;
        *cache = Some(CachedCapacity {
            total_used: update.total_used,
            last_update: Instant::now(),
            file_count: update.file_count,
            is_estimated: update.is_estimated,
            source,
        });

        debug!(
            "Capacity updated: {} bytes, files={}, estimated={}, source: {:?}",
            update.total_used, update.file_count, update.is_estimated, source
        );
        record_capacity_current_bytes(update.total_used);
        record_capacity_update_completed(source.as_metric_label(), start.elapsed(), update.total_used, update.is_estimated);
    }

    /// Record write operation
    pub async fn record_write_operation(&self) {
        let mut record = self.write_record.write().await;
        record.last_write_time = Instant::now();
        record.write_count += 1;

        // Maintain write time window (keep last 1 minute)
        // Cap the window size to prevent unbounded memory growth at high write rates
        const MAX_WRITE_WINDOW_SIZE: usize = 10000;
        let now = Instant::now();
        record
            .write_window
            .retain(|&t| now.duration_since(t) < Duration::from_secs(60));
        // Only push if under the cap to prevent unbounded growth
        if record.write_window.len() < MAX_WRITE_WINDOW_SIZE {
            record.write_window.push(now);
        }

        record_capacity_write_operation(record.write_window.len());
        debug!(
            "Write operation recorded: total writes = {}, recent writes = {}",
            record.write_count,
            record.write_window.len()
        );
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

            let write_record = self.write_record.read().await;
            let time_since_write = write_record.last_write_time.elapsed();

            // Recent write, trigger fast update
            if time_since_write < self.config.fast_update_threshold {
                debug!("Recent write detected ({:?} ago), needs fast update", time_since_write);
                return true;
            }

            // High write frequency, trigger update
            let write_frequency = write_record.write_window.len();
            if write_frequency > self.config.write_frequency_threshold {
                debug!("High write frequency detected ({} writes/min), needs fast update", write_frequency);
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
        record.write_window.len()
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
                Some(state.result_tx.subscribe())
            } else {
                // Become the leader. Create a fresh channel so that joiners from a previous
                // cycle cannot observe the result that was published for the new cycle.
                let (tx, _) = watch::channel(None);
                state.result_tx = tx;
                state.running = true;
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

        let result = AssertUnwindSafe(refresh_fn()).catch_unwind().await.unwrap_or_else(|err| {
            warn!(error = ?err, "capacity refresh function panicked");
            Err("capacity refresh panicked".to_string())
        });
        if let Ok(update) = &result {
            self.update_capacity(update.clone(), source).await;
        }

        {
            let mut state = self.refresh_state.lock().await;
            state.running = false;
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
                true
            }
        };

        if !should_spawn {
            return false;
        }

        tokio::spawn(async move {
            let result = AssertUnwindSafe(refresh_fn()).catch_unwind().await.unwrap_or_else(|err| {
                warn!(error = ?err, "capacity refresh function panicked");
                Err("capacity refresh panicked".to_string())
            });
            if let Ok(update) = &result {
                self.update_capacity(update.clone(), source).await;
            }

            let mut state = self.refresh_state.lock().await;
            state.running = false;
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
    #[cfg(test)]
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
/// ```no_run
/// let manager = create_isolated_manager(HybridStrategyConfig::default());
/// manager
///     .update_capacity(CapacityUpdate::exact(1000, 0), DataSource::RealTime)
///     .await;
/// ```
#[cfg(test)]
#[allow(dead_code)]
pub fn create_isolated_manager(config: HybridStrategyConfig) -> Arc<HybridCapacityManager> {
    Arc::new(HybridCapacityManager::new(config))
}

/// Start background update task
pub async fn start_background_task(disks: Vec<rustfs_madmin::Disk>) {
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
                .spawn_refresh_if_needed(DataSource::Scheduled, move || async move {
                    calculate_data_dir_used_capacity(&disks)
                        .await
                        .map(|scan| scan.to_capacity_update())
                        .map_err(|e| e.to_string())
                })
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
    use rustfs_config::{
        ENV_CAPACITY_FAST_UPDATE_THRESHOLD, ENV_CAPACITY_MAX_FILES_THRESHOLD, ENV_CAPACITY_SAMPLE_RATE,
        ENV_CAPACITY_STAT_TIMEOUT, ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, ENV_CAPACITY_WRITE_TRIGGER_DELAY,
    };
    use serial_test::serial;

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
    async fn test_record_write_operation() {
        let manager = HybridCapacityManager::from_env();

        manager.record_write_operation().await;

        let frequency = manager.get_write_frequency().await;
        assert_eq!(frequency, 1);
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
