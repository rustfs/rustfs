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
use metrics::{counter, gauge};
use rustfs_config::{
    DEFAULT_FAST_UPDATE_THRESHOLD_SECS, DEFAULT_MAX_FILES_THRESHOLD, DEFAULT_SAMPLE_RATE, DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS,
    DEFAULT_STAT_TIMEOUT_SECS, DEFAULT_WRITE_FREQUENCY_THRESHOLD, DEFAULT_WRITE_TRIGGER_DELAY_SECS,
    ENV_CAPACITY_FAST_UPDATE_THRESHOLD, ENV_CAPACITY_MAX_FILES_THRESHOLD, ENV_CAPACITY_SAMPLE_RATE,
    ENV_CAPACITY_SCHEDULED_INTERVAL, ENV_CAPACITY_STAT_TIMEOUT, ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD,
    ENV_CAPACITY_WRITE_TRIGGER_DELAY,
};
use rustfs_utils::{get_env_u64, get_env_usize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};
// ============================================================================
// Configuration Functions
// ============================================================================

/// Get scheduled update interval from environment or default
pub fn get_scheduled_update_interval() -> Duration {
    Duration::from_secs(get_env_u64(ENV_CAPACITY_SCHEDULED_INTERVAL, DEFAULT_SCHEDULED_UPDATE_INTERVAL_SECS))
}

/// Get write trigger delay from environment or default
pub fn get_write_trigger_delay() -> Duration {
    Duration::from_secs(get_env_u64(ENV_CAPACITY_WRITE_TRIGGER_DELAY, DEFAULT_WRITE_TRIGGER_DELAY_SECS))
}

/// Get write frequency threshold from environment or default
pub fn get_write_frequency_threshold() -> usize {
    get_env_usize(ENV_CAPACITY_WRITE_FREQUENCY_THRESHOLD, DEFAULT_WRITE_FREQUENCY_THRESHOLD)
}

/// Get fast update threshold from environment or default
pub fn get_fast_update_threshold() -> Duration {
    Duration::from_secs(get_env_u64(ENV_CAPACITY_FAST_UPDATE_THRESHOLD, DEFAULT_FAST_UPDATE_THRESHOLD_SECS))
}

/// Get max files threshold from environment or default
pub fn get_max_files_threshold() -> usize {
    get_env_usize(ENV_CAPACITY_MAX_FILES_THRESHOLD, DEFAULT_MAX_FILES_THRESHOLD)
}

/// Get stat timeout from environment or default
pub fn get_stat_timeout() -> Duration {
    Duration::from_secs(get_env_u64(ENV_CAPACITY_STAT_TIMEOUT, DEFAULT_STAT_TIMEOUT_SECS))
}

/// Get sample rate from environment or default
pub fn get_sample_rate() -> usize {
    get_env_usize(ENV_CAPACITY_SAMPLE_RATE, DEFAULT_SAMPLE_RATE)
}

// ============================================================================
// Data Structures
// ============================================================================

/// Cached capacity data
#[derive(Clone, Debug)]
#[allow(dead_code)]
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

#[derive(Clone, Debug, PartialEq, Copy, Eq)]
#[allow(dead_code)]
pub enum DataSource {
    /// Real-time statistics
    RealTime,
    /// Scheduled update
    Scheduled,
    /// Write triggered
    WriteTriggered,
    /// Fallback value
    Fallback,
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

/// Hybrid capacity manager
pub struct HybridCapacityManager {
    /// Capacity cache
    cache: Arc<RwLock<Option<CachedCapacity>>>,
    /// Write record
    write_record: Arc<RwLock<WriteRecord>>,
    /// Configuration
    config: HybridStrategyConfig,
    /// Background update in progress flag
    update_in_progress: Arc<AtomicBool>,
}

impl HybridCapacityManager {
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
            update_in_progress: Arc::new(AtomicBool::new(false)),
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
    pub async fn update_capacity(&self, capacity: u64, source: DataSource) {
        let mut cache = self.cache.write().await;
        *cache = Some(CachedCapacity {
            total_used: capacity,
            last_update: Instant::now(),
            file_count: 0,
            is_estimated: false,
            source,
        });

        debug!("Capacity updated: {} bytes, source: {:?}", capacity, source);
        // Update metrics
        gauge!("rustfs.capacity.current").set(capacity as f64);
        match source {
            DataSource::RealTime => counter!("rustfs.capacity.update.realtime").increment(1),
            DataSource::Scheduled => counter!("rustfs.capacity.update.scheduled").increment(1),
            DataSource::WriteTriggered => counter!("rustfs.capacity.update.write_triggered").increment(1),
            DataSource::Fallback => counter!("rustfs.capacity.update.fallback").increment(1),
        }
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

        counter!("rustfs.capacity.write.operations").increment(1);
        gauge!("rustfs.capacity.write.frequency").set(record.write_window.len() as f64);
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

    /// Get config
    pub fn get_config(&self) -> &HybridStrategyConfig {
        &self.config
    }

    /// Try to start a background update, returns true if update was started (false if already in progress)
    pub fn try_start_background_update(&self) -> bool {
        self.update_in_progress
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Mark background update as complete
    pub fn complete_background_update(&self) {
        self.update_in_progress.store(false, Ordering::Release);
    }
}

/// Global capacity manager instance
static CAPACITY_MANAGER: std::sync::OnceLock<Arc<HybridCapacityManager>> = std::sync::OnceLock::new();

/// Get or initialize the global capacity manager
pub fn get_capacity_manager() -> Arc<HybridCapacityManager> {
    CAPACITY_MANAGER
        .get_or_init(|| Arc::new(HybridCapacityManager::from_env()))
        .clone()
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

            // Import the calculate function
            match calculate_data_dir_used_capacity(&disks).await {
                Ok(new_capacity) => {
                    let elapsed = start.elapsed();
                    info!("Scheduled update completed: {} bytes in {:?}", new_capacity, elapsed);
                    manager.update_capacity(new_capacity, DataSource::Scheduled).await;
                }
                Err(e) => {
                    error!("Scheduled update failed: {:?}", e);
                }
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
        assert_eq!(interval, Duration::from_secs(300));
    }

    #[test]
    #[serial]
    fn test_get_write_trigger_delay() {
        let delay = get_write_trigger_delay();
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    #[serial]
    fn test_get_write_frequency_threshold() {
        let threshold = get_write_frequency_threshold();
        assert_eq!(threshold, 10);
    }

    #[test]
    #[serial]
    fn test_get_fast_update_threshold() {
        let threshold = get_fast_update_threshold();
        assert_eq!(threshold, Duration::from_secs(60));
    }

    #[test]
    #[serial]
    fn test_get_max_files_threshold() {
        let threshold = get_max_files_threshold();
        assert_eq!(threshold, 1_000_000);
    }

    #[test]
    #[serial]
    fn test_get_stat_timeout() {
        let timeout = get_stat_timeout();
        assert_eq!(timeout, Duration::from_secs(5));
    }

    #[test]
    #[serial]
    fn test_get_sample_rate() {
        let rate = get_sample_rate();
        assert_eq!(rate, 100);
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

        manager.update_capacity(1000, DataSource::RealTime).await;

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
        manager.update_capacity(1000, DataSource::RealTime).await;

        // Fresh cache, should not need update
        assert!(!manager.needs_fast_update().await);
    }

    #[tokio::test]
    #[serial]
    async fn test_config_from_env() {
        let config = HybridStrategyConfig::from_env();

        // Check default values
        assert_eq!(config.scheduled_update_interval, Duration::from_secs(300));
        assert_eq!(config.write_trigger_delay, Duration::from_secs(10));
        assert_eq!(config.write_frequency_threshold, 10);
        assert_eq!(config.fast_update_threshold, Duration::from_secs(60));
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
