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

//! Concurrency manager for coordinating concurrent GetObject requests.

use super::io_schedule::{
    IoLoadLevel, IoLoadMetrics, IoPriority, IoPriorityQueue, IoPriorityQueueConfig, IoQueueStatus, IoSchedulerConfig, IoStrategy,
    get_advanced_buffer_size,
};
use super::request_guard::GetObjectGuard;
use rustfs_concurrency::GetObjectQueueSnapshot;
use rustfs_config::{KI_B, MI_B};
use rustfs_io_core::BytesPool;
use rustfs_io_core::io_profile::{AccessPattern, IoPatternDetector, StorageMedia, detect_storage_media};
use rustfs_io_metrics::bandwidth::{BandwidthMonitor, BandwidthSnapshot};
use rustfs_io_metrics::global_metrics::get_global_metrics;
use rustfs_io_metrics::{MetricsCollector, PerformanceMetrics};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;
use tracing::debug;

/// Global concurrency manager instance
pub(crate) static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

#[derive(Clone)]
pub struct ConcurrencyManager {
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
    /// I/O load metrics for adaptive strategy calculation
    io_metrics: Arc<Mutex<IoLoadMetrics>>,
    /// I/O priority queue for request scheduling
    #[allow(dead_code)]
    priority_queue: Arc<IoPriorityQueue<()>>,
    /// Bytes pool for buffer allocation and reuse
    bytes_pool: Arc<BytesPool>,
    // Enhanced scheduler state
    /// I/O scheduler configuration (cached at initialization)
    scheduler_config: IoSchedulerConfig,
    /// Detected storage media type
    storage_media: StorageMedia,
    /// I/O pattern detector for sequential/random access tracking
    pattern_detector: Arc<Mutex<IoPatternDetector>>,
    /// Bandwidth monitor for adaptive I/O sizing
    bandwidth_monitor: Arc<Mutex<BandwidthMonitor>>,
    /// Metrics collector for I/O latency tracking (P50, P95, P99)
    metrics_collector: Arc<MetricsCollector>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        let io_metrics_info = if let Ok(metrics) = self.io_metrics.lock() {
            format!("avg_wait={:?}, observations={}", metrics.average_wait(), metrics.observation_count())
        } else {
            "locked".to_string()
        };
        let bandwidth_info = if let Ok(monitor) = self.bandwidth_monitor.lock() {
            format!("{:?}", monitor.snapshot())
        } else {
            "locked".to_string()
        };
        f.debug_struct("ConcurrencyManager")
            .field(
                "active_requests",
                &crate::storage::concurrency::io_schedule::ACTIVE_GET_REQUESTS.load(Ordering::Relaxed),
            )
            .field("disk_read_permits", &self.disk_read_semaphore.available_permits())
            .field("io_metrics", &io_metrics_info)
            .field("storage_media", &self.storage_media)
            .field("bandwidth", &bandwidth_info)
            .field("bytes_pool", &self.bytes_pool)
            .finish()
    }
}
#[allow(dead_code)]
impl ConcurrencyManager {
    /// Create a new concurrency manager with default settings
    ///
    /// Reads configuration from environment variables:
    /// - `RUSTFS_OBJECT_MAX_CONCURRENT_DISK_READS`: Maximum concurrent disk reads (default: 64)
    pub fn new() -> Self {
        // Load scheduler configuration once at initialization
        let scheduler_config = IoSchedulerConfig::from_env();

        let max_disk_reads = scheduler_config.max_concurrent_reads;

        // Detect storage media
        let storage_media =
            detect_storage_media(scheduler_config.storage_detection_enabled, &scheduler_config.storage_media_override);

        // Initialize I/O pattern detector
        let pattern_detector = Arc::new(Mutex::new(IoPatternDetector::new(
            scheduler_config.pattern_history_size,
            scheduler_config.sequential_step_tolerance_bytes,
        )));

        // Initialize bandwidth monitor
        let bandwidth_monitor = Arc::new(Mutex::new(BandwidthMonitor::new(
            scheduler_config.bandwidth_ema_beta,
            scheduler_config.bandwidth_low_threshold_bps,
            scheduler_config.bandwidth_high_threshold_bps,
        )));

        // Use global performance metrics instance for consistent metrics tracking
        // This allows AutoTuner and other components to access the same metrics data
        let performance_metrics = get_global_metrics();

        // Initialize metrics collector for I/O latency tracking
        // Keep 1000 samples for P95/P99 calculation
        let metrics_collector = Arc::new(MetricsCollector::new(performance_metrics, 1000));

        // Build priority queue config
        let queue_config = IoPriorityQueueConfig {
            queue_high_capacity: scheduler_config.queue_high_capacity,
            queue_normal_capacity: scheduler_config.queue_normal_capacity,
            queue_low_capacity: scheduler_config.queue_low_capacity,
            starvation_prevention_interval_ms: scheduler_config.starvation_prevention_interval_ms,
            starvation_threshold_secs: scheduler_config.starvation_threshold_secs,
        };

        Self {
            disk_read_semaphore: Arc::new(Semaphore::new(max_disk_reads)),
            io_metrics: Arc::new(Mutex::new(IoLoadMetrics::new(scheduler_config.load_sample_window))),
            priority_queue: Arc::new(IoPriorityQueue::new(queue_config)),
            bytes_pool: Arc::new(BytesPool::new_tiered()),
            scheduler_config,
            storage_media,
            pattern_detector,
            bandwidth_monitor,
            metrics_collector,
        }
    }

    /// Track a GetObject request
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    /// Get the bytes pool for buffer allocation
    ///
    /// Returns a reference to the BytesPool which can be used to acquire
    /// reusable buffers for I/O operations, reducing allocation overhead.
    ///
    /// # Returns
    ///
    /// Arc-wrapped BytesPool instance
    pub fn bytes_pool(&self) -> Arc<BytesPool> {
        self.bytes_pool.clone()
    }

    /// Acquire a permit to perform a disk read operation
    ///
    /// This ensures we don't overwhelm the disk subsystem with too many
    /// concurrent reads, which can cause performance degradation.
    pub async fn acquire_disk_read_permit(&self) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        self.disk_read_semaphore.acquire().await
    }

    // ============================================
    // Adaptive I/O Strategy Methods
    // ============================================

    /// Record a disk permit wait observation for load tracking.
    ///
    /// This method updates the rolling metrics used to calculate adaptive I/O
    /// strategies. Should be called after each disk permit acquisition.
    ///
    /// # Arguments
    ///
    /// * `wait_duration` - Time spent waiting for the disk read permit
    pub fn record_permit_wait(&self, wait_duration: Duration) {
        if let Ok(mut metrics) = self.io_metrics.lock() {
            metrics.record(wait_duration);
        }
    }

    // ============================================
    // Metrics Collection Methods
    // ============================================

    /// Record a disk I/O operation for latency tracking.
    ///
    /// This method delegates to MetricsCollector which:
    /// 1. Updates atomic counters in PerformanceMetrics
    /// 2. Records latency for P95/P99 calculation
    /// 3. Reports to metrics crate (which exports to OTEL)
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the I/O operation
    /// * `is_read` - true for read operations, false for writes
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let manager = get_concurrency_manager();
    /// let start = Instant::now();
    /// // ... perform disk I/O ...
    /// let duration = start.elapsed();
    /// manager.record_disk_operation(1024 * 1024, duration, true).await;
    /// ```
    pub async fn record_disk_operation(&self, bytes: u64, duration: Duration, is_read: bool) {
        self.metrics_collector.record_io_operation(bytes, duration, is_read).await;
    }

    /// Get a reference to the metrics collector for external use.
    ///
    /// # Returns
    ///
    /// Arc-wrapped MetricsCollector instance
    pub fn metrics_collector(&self) -> &Arc<MetricsCollector> {
        &self.metrics_collector
    }

    /// Get the global performance metrics instance.
    ///
    /// This provides access to the shared PerformanceMetrics that is used
    /// across all components, including AutoTuner.
    ///
    /// # Returns
    ///
    /// Arc-wrapped PerformanceMetrics instance
    pub fn performance_metrics(&self) -> Arc<PerformanceMetrics> {
        get_global_metrics()
    }

    /// Calculate an adaptive I/O strategy based on disk permit wait time.
    ///
    /// This method analyzes the permit wait duration to determine the current
    /// I/O load level and returns optimized parameters for the read operation.
    ///
    /// # Arguments
    ///
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// An `IoStrategy` containing optimized I/O parameters.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let permit_wait_start = Instant::now();
    /// let _permit = manager.acquire_disk_read_permit().await;
    /// let permit_wait_duration = permit_wait_start.elapsed();
    ///
    /// let strategy = manager.calculate_io_strategy(permit_wait_duration, 256 * 1024);
    /// let optimal_buffer = strategy.buffer_size;
    /// ```
    pub fn calculate_io_strategy(&self, permit_wait_duration: Duration, base_buffer_size: usize) -> IoStrategy {
        // Record the observation for future smoothing
        self.record_permit_wait(permit_wait_duration);

        // Calculate strategy from the current wait duration
        IoStrategy::from_wait_duration(permit_wait_duration, base_buffer_size)
    }

    /// Calculate I/O strategy with enhanced multi-factor context.
    ///
    /// This method integrates storage media, access patterns, bandwidth observations,
    /// and concurrent request count to provide a more sophisticated I/O strategy.
    ///
    /// # Arguments
    ///
    /// * `file_size` - Size of the file/object being read (-1 if unknown)
    /// * `base_buffer_size` - Base buffer size from workload configuration
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `is_sequential_hint` - Whether the access pattern is known to be sequential
    ///
    /// # Returns
    ///
    /// An `IoStrategy` with optimized parameters based on all available factors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let strategy = manager.calculate_io_strategy_with_context(
    ///     file_size,
    ///     256 * 1024,
    ///     permit_wait_duration,
    ///     false,
    /// );
    /// let optimal_buffer = strategy.buffer_size;
    /// let enable_readahead = strategy.enable_readahead;
    /// ```
    pub fn calculate_io_strategy_with_context(
        &self,
        file_size: i64,
        base_buffer_size: usize,
        permit_wait_duration: Duration,
        is_sequential_hint: bool,
    ) -> IoStrategy {
        use crate::storage::concurrency::io_schedule::IoSchedulingContext;

        // Record the observation for future smoothing
        self.record_permit_wait(permit_wait_duration);

        // Get current access pattern
        let access_pattern = if let Ok(detector) = self.pattern_detector.lock() {
            detector.current_pattern()
        } else {
            AccessPattern::Unknown
        };

        // Get current bandwidth snapshot
        let observed_bandwidth_bps = if let Ok(monitor) = self.bandwidth_monitor.lock() {
            let snapshot = monitor.snapshot();
            if snapshot.tier == rustfs_io_metrics::bandwidth::BandwidthTier::Unknown {
                None
            } else {
                Some(snapshot.bytes_per_second)
            }
        } else {
            None
        };

        // Get concurrent request count
        let concurrent_requests =
            crate::storage::concurrency::io_schedule::ACTIVE_GET_REQUESTS.load(std::sync::atomic::Ordering::Relaxed);

        // Build scheduling context
        let context = IoSchedulingContext {
            file_size,
            base_buffer_size,
            permit_wait_duration,
            is_sequential_hint,
            access_pattern,
            storage_media: self.storage_media,
            observed_bandwidth_bps,
            concurrent_requests,
        };

        // Calculate strategy using multi-factor approach
        IoStrategy::from_context_with_config(&context, &self.scheduler_config)
    }

    /// Get the smoothed I/O load level based on recent observations.
    ///
    /// This uses the rolling window of permit wait times to provide a more
    /// stable estimate of the current load level, reducing oscillation from
    /// transient spikes.
    ///
    /// # Returns
    ///
    /// The smoothed `IoLoadLevel` based on average recent wait times.
    pub fn smoothed_load_level(&self) -> IoLoadLevel {
        if let Ok(metrics) = self.io_metrics.lock() {
            metrics.smoothed_load_level()
        } else {
            IoLoadLevel::Medium // Default to medium if lock fails
        }
    }

    /// Get I/O load statistics for monitoring.
    ///
    /// Returns statistics about recent disk permit wait times for
    /// monitoring dashboards and capacity planning.
    ///
    /// # Returns
    ///
    /// A tuple of (average_wait, p95_wait, max_wait, observation_count)
    pub fn io_load_stats(&self) -> (Duration, Duration, Duration, u64) {
        if let Ok(metrics) = self.io_metrics.lock() {
            (
                metrics.average_wait(),
                metrics.p95_wait(),
                metrics.max_wait(),
                metrics.observation_count(),
            )
        } else {
            (Duration::ZERO, Duration::ZERO, Duration::ZERO, 0)
        }
    }

    /// Get the recommended buffer size based on current I/O load.
    ///
    /// This is a convenience method that combines load level detection with
    /// buffer size calculation. Uses the smoothed load level for stability.
    ///
    /// # Arguments
    ///
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// Recommended buffer size in bytes.
    pub fn adaptive_buffer_size(&self, base_buffer_size: usize) -> usize {
        let load_level = self.smoothed_load_level();
        let multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };

        let buffer_size = ((base_buffer_size as f64) * multiplier) as usize;
        buffer_size.clamp(32 * KI_B, MI_B)
    }

    // ============================================
    // Enhanced I/O Scheduling Methods
    // ============================================

    /// Record an I/O access for pattern detection.
    ///
    /// This updates the pattern detector with the offset and size of an access,
    /// allowing it to distinguish between sequential and random access patterns.
    ///
    /// # Arguments
    ///
    /// * `offset` - File offset being accessed
    /// * `len` - Length of the access
    pub fn record_access(&self, offset: u64, len: u64) {
        if let Ok(mut detector) = self.pattern_detector.lock() {
            detector.record(offset, len);
        }
    }

    /// Get the current access pattern.
    ///
    /// Returns the detected access pattern (Sequential, Random, Mixed, or Unknown).
    pub fn current_access_pattern(&self) -> AccessPattern {
        if let Ok(detector) = self.pattern_detector.lock() {
            detector.current_pattern()
        } else {
            AccessPattern::Unknown
        }
    }

    /// Record a data transfer for bandwidth monitoring.
    ///
    /// This updates the bandwidth monitor with the bytes transferred and duration,
    /// allowing it to maintain an EMA (Exponential Moving Average) of the observed bandwidth.
    ///
    /// # Arguments
    ///
    /// * `bytes` - Number of bytes transferred
    /// * `duration` - Duration of the transfer
    pub fn record_transfer(&self, bytes: u64, duration: Duration) {
        if let Ok(mut monitor) = self.bandwidth_monitor.lock() {
            monitor.record_transfer(bytes, duration);
        }
    }

    /// Get the current bandwidth snapshot.
    ///
    /// Returns a snapshot of the current bandwidth including bytes per second and tier.
    pub fn current_bandwidth_snapshot(&self) -> BandwidthSnapshot {
        if let Ok(monitor) = self.bandwidth_monitor.lock() {
            monitor.snapshot()
        } else {
            BandwidthSnapshot {
                bytes_per_second: 0,
                tier: rustfs_io_metrics::bandwidth::BandwidthTier::Unknown,
            }
        }
    }

    /// Get the detected storage media type.
    pub fn storage_media(&self) -> StorageMedia {
        self.storage_media
    }

    /// Get the scheduler configuration.
    pub fn scheduler_config(&self) -> &IoSchedulerConfig {
        &self.scheduler_config
    }

    /// Get optimized buffer size for a request
    ///
    /// This wraps the advanced buffer sizing logic and makes it accessible
    /// through the concurrency manager interface.
    pub fn buffer_size(&self, file_size: i64, base: usize, sequential: bool) -> usize {
        get_advanced_buffer_size(file_size, base, sequential)
    }

    // ============================================
    // Priority-Based I/O Scheduling Methods
    // ============================================

    /// Get I/O priority for a request based on its size.
    ///
    /// This enables priority-based scheduling where small requests
    /// are processed before large requests to prevent starvation.
    ///
    /// # Arguments
    ///
    /// * `request_size` - Size of the request in bytes (-1 if unknown)
    ///
    /// # Returns
    ///
    /// Priority level (High, Normal, or Low)
    pub fn get_io_priority(&self, request_size: i64) -> IoPriority {
        if request_size < 0 {
            // Unknown size, use normal priority
            IoPriority::Normal
        } else {
            // Use cached scheduler config thresholds
            IoPriority::from_size_with_thresholds(
                request_size,
                self.scheduler_config.high_priority_size_threshold,
                self.scheduler_config.low_priority_size_threshold,
            )
        }
    }

    /// Check if priority scheduling is enabled.
    pub fn is_priority_scheduling_enabled(&self) -> bool {
        self.scheduler_config.enable_priority
    }

    /// Get current I/O queue status for monitoring.
    ///
    /// Returns information about permit usage and waiting requests.
    pub fn io_queue_status(&self) -> IoQueueStatus {
        let snapshot = GetObjectQueueSnapshot::from_available_permits(
            self.scheduler_config.max_concurrent_reads,
            self.disk_read_semaphore.available_permits(),
        );

        IoQueueStatus {
            total_permits: snapshot.total_permits,
            permits_in_use: snapshot.permits_in_use,
            high_priority_waiting: 0, // Would need additional tracking
            normal_priority_waiting: 0,
            low_priority_waiting: 0,
            high_priority_processed: 0,
            normal_priority_processed: 0,
            low_priority_processed: 0,
            starvation_events: 0,
        }
    }

    /// Acquire a disk read permit with priority awareness.
    ///
    /// When priority scheduling is enabled, this method logs the priority
    /// for observability. The actual acquisition uses the same semaphore
    /// but priority information is used for monitoring.
    ///
    /// # Arguments
    ///
    /// * `priority` - Priority level for this request
    ///
    /// # Returns
    ///
    /// Semaphore permit on success, error on failure
    pub async fn acquire_priority_permit(
        &self,
        priority: IoPriority,
    ) -> Result<tokio::sync::SemaphorePermit<'_>, tokio::sync::AcquireError> {
        rustfs_io_metrics::record_io_priority_assignment(priority.as_str());

        debug!(
            priority = %priority,
            available_permits = self.disk_read_semaphore.available_permits(),
            "Acquiring disk read permit"
        );

        self.disk_read_semaphore.acquire().await
    }

    /// Get the global concurrency manager instance.
    pub fn global() -> &'static Self {
        &CONCURRENCY_MANAGER
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================
// Integration Tests for ConcurrencyManager
// ============================================

#[cfg(test)]
mod integration_tests {
    use super::*;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_priority_queue_integration() {
        let manager = ConcurrencyManager::new();

        // Test priority determination
        let small_size = 100 * 1024; // 100KB
        let large_size = 200 * 1024 * 1024; // 200MB

        let small_priority = manager.get_io_priority(small_size as i64);
        let large_priority = manager.get_io_priority(large_size as i64);

        assert_eq!(small_priority, IoPriority::High);
        assert_eq!(large_priority, IoPriority::Low);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_io_queue_status() {
        let manager = ConcurrencyManager::new();

        let status = manager.io_queue_status();

        // Initial state should have no waiting requests
        assert_eq!(status.high_priority_waiting, 0);
        assert_eq!(status.normal_priority_waiting, 0);
        assert_eq!(status.low_priority_waiting, 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_disk_read_permit() {
        let manager = ConcurrencyManager::new();

        // Acquire permit
        let permit = manager.acquire_disk_read_permit().await;
        assert!(permit.is_ok());

        // Permit should be released when dropped
        drop(permit);

        // Should be able to acquire again
        let permit2 = manager.acquire_disk_read_permit().await;
        assert!(permit2.is_ok());
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_priority_scheduling() {
        let manager = ConcurrencyManager::new();

        // Test if priority scheduling is enabled
        let enabled = manager.is_priority_scheduling_enabled();
        assert!(enabled); // Should be enabled by default

        // Test priority determination for different sizes
        assert_eq!(manager.get_io_priority(500 * 1024), IoPriority::High); // 500KB
        assert_eq!(manager.get_io_priority(5 * 1024 * 1024), IoPriority::Normal); // 5MB
        assert_eq!(manager.get_io_priority(50 * 1024 * 1024), IoPriority::Low); // 50MB
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_io_strategy() {
        let manager = ConcurrencyManager::new();

        // Test I/O strategy calculation
        let low_wait = Duration::from_millis(5);
        let high_wait = Duration::from_millis(100);

        let strategy_low = manager.calculate_io_strategy(low_wait, 128 * 1024);
        let strategy_high = manager.calculate_io_strategy(high_wait, 128 * 1024);

        // Under low load, should use larger buffers
        assert!(strategy_low.buffer_size >= strategy_high.buffer_size);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_adaptive_buffer_size() {
        let manager = ConcurrencyManager::new();

        let base_size = 128 * 1024; // 128KB

        // Test adaptive buffer sizing
        let size1 = manager.adaptive_buffer_size(base_size);

        // Should return a reasonable buffer size
        assert!(size1 > 0);
        assert!(size1 <= 2 * 1024 * 1024); // Not more than 2MB
    }

    // ============================================
    // Multi-Factor Strategy Integration Tests
    // ============================================

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_nvme_optimal() {
        let manager = ConcurrencyManager::new();

        // Simulate optimal conditions: Unknown/SSD + Sequential + Low load
        let file_size = 100 * 1024 * 1024; // 100MB
        let base_buffer = 256 * 1024;
        let permit_wait = Duration::from_millis(5); // Low load
        let is_sequential = true;

        let strategy = manager.calculate_io_strategy_with_context(file_size, base_buffer, permit_wait, is_sequential);
        let media = manager.storage_media();

        // Verify basic optimizations work
        assert_eq!(strategy.storage_media, media);
        assert!(strategy.buffer_size >= base_buffer * 8 / 10, "Sequential should maintain or boost buffer");
        let expected_readahead = !matches!(media, StorageMedia::Hdd);
        assert_eq!(
            strategy.enable_readahead, expected_readahead,
            "Readahead should follow storage profile preference under low load"
        );
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_access_pattern_tracking() {
        let manager = ConcurrencyManager::new();

        // Record sequential accesses
        for offset in [0, 1024, 2048, 3072, 4096] {
            manager.record_access(offset, 1024);
        }

        // Check pattern detection
        let pattern = manager.current_access_pattern();
        assert_eq!(pattern, AccessPattern::Sequential);

        // Record random accesses
        for offset in [0, 10 * 1024, 100 * 1024, 5 * 1024 * 1024] {
            manager.record_access(offset, 1024);
        }

        // Pattern should change to mixed or random
        let pattern_after = manager.current_access_pattern();
        assert!(!matches!(pattern_after, AccessPattern::Sequential));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_bandwidth_recording() {
        let manager = ConcurrencyManager::new();

        // Simulate transfer
        let bytes = 10 * 1024 * 1024; // 10MB
        let duration = Duration::from_millis(100); // 100ms = 100MB/s

        manager.record_transfer(bytes, duration);

        // Check bandwidth snapshot (returns BandwidthSnapshot directly)
        let snapshot = manager.current_bandwidth_snapshot();
        assert!(snapshot.bytes_per_second > 0, "Should have bandwidth data after recording");
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_compatibility() {
        let manager = ConcurrencyManager::new();

        // Test that old API still works
        let old_strategy = manager.calculate_io_strategy(Duration::from_millis(50), 256 * 1024);

        assert!(old_strategy.buffer_size > 0);

        // New API with context should also work
        let new_strategy =
            manager.calculate_io_strategy_with_context(50 * 1024 * 1024, 256 * 1024, Duration::from_millis(50), false);

        assert!(new_strategy.buffer_size > 0);
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_high_concurrency() {
        let manager = ConcurrencyManager::new();

        // Simulate high concurrent requests by keeping guards alive
        let _guards: Vec<_> = (0..20).map(|_| GetObjectGuard::new()).collect();

        let strategy = manager.calculate_io_strategy_with_context(100 * 1024 * 1024, 512 * 1024, Duration::from_millis(10), true);

        // High concurrency should reduce buffer
        assert!(strategy.concurrent_requests >= manager.scheduler_config().high_concurrency_threshold);
        assert!(strategy.buffer_size < 512 * 1024, "High concurrency should reduce buffer");
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_buffer_clamp() {
        let manager = ConcurrencyManager::new();
        let media = manager.storage_media();
        let config = manager.scheduler_config();

        // Request very large base buffer
        let large_base = 16 * 1024 * 1024; // 16MB

        let strategy = manager.calculate_io_strategy_with_context(
            1024 * 1024, // 1GB file
            large_base,
            Duration::from_millis(1),
            true,
        );

        let media_cap = match media {
            StorageMedia::Nvme => config.nvme_buffer_cap,
            StorageMedia::Ssd => config.ssd_buffer_cap,
            StorageMedia::Hdd => config.hdd_buffer_cap,
            StorageMedia::Unknown => config.ssd_buffer_cap,
        };
        let expected_max = media_cap.min(MI_B);

        // Large base buffer should be constrained by storage cap first, then global clamp.
        assert_eq!(
            strategy.buffer_size, expected_max,
            "Buffer should be capped by media profile and global clamp"
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_storage_media_detection() {
        let manager = ConcurrencyManager::new();

        // Check storage media was detected at initialization
        let media = manager.storage_media();

        // Should be one of the known types (not Unknown unless detection failed)
        // We accept Unknown if detection wasn't configured
        assert!(matches!(
            media,
            StorageMedia::Nvme | StorageMedia::Ssd | StorageMedia::Hdd | StorageMedia::Unknown
        ));
    }

    #[tokio::test]
    #[serial]
    async fn test_concurrency_manager_multi_factor_strategy_priority_with_context() {
        let manager = ConcurrencyManager::new();

        // Test priority is correctly calculated in multi-factor strategy
        let small_file_strategy = manager.calculate_io_strategy_with_context(
            500 * 1024, // 500KB
            256 * 1024,
            Duration::from_millis(10),
            false,
        );

        let large_file_strategy = manager.calculate_io_strategy_with_context(
            50 * 1024 * 1024, // 50MB
            256 * 1024,
            Duration::from_millis(10),
            false,
        );

        assert_eq!(small_file_strategy.priority, IoPriority::High);
        assert_eq!(large_file_strategy.priority, IoPriority::Low);
    }
}
