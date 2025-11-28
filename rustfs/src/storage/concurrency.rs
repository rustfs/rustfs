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

//! Concurrency optimization module for high-performance object retrieval.
//!
//! This module provides intelligent concurrency management to prevent performance
//! degradation when multiple concurrent GetObject requests are processed. It addresses
//! the core issue where increasing concurrency from 1→2→4 requests caused latency to
//! degrade exponentially (59ms → 110ms → 200ms).
//!
//! # Key Features
//!
//! - **Adaptive Buffer Sizing**: Dynamically adjusts buffer sizes based on concurrent load
//!   to prevent memory contention and thrashing under high concurrency.
//! - **Moka Cache Integration**: Lock-free hot object caching with automatic TTL/TTI expiration
//!   for frequently accessed objects, providing sub-5ms response times on cache hits.
//! - **I/O Rate Limiting**: Semaphore-based disk read throttling prevents I/O queue saturation
//!   and ensures fair resource allocation across concurrent requests.
//! - **Comprehensive Metrics**: Prometheus-compatible metrics for monitoring cache hit rates,
//!   request latency, concurrency levels, and disk wait times.
//!
//! # Performance Characteristics
//!
//! - Low concurrency (1-2 requests): Optimizes for throughput with larger buffers (100%)
//! - Medium concurrency (3-4 requests): Balances throughput and fairness (75% buffers)
//! - High concurrency (5-8 requests): Optimizes for fairness (50% buffers)
//! - Very high concurrency (>8 requests): Ensures predictable latency (40% buffers)
//!
//! # Expected Performance Improvements
//!
//! | Concurrent Requests | Before | After | Improvement |
//! |---------------------|--------|-------|-------------|
//! | 2 requests          | 110ms  | 60-70ms | ~40% faster |
//! | 4 requests          | 200ms  | 75-90ms | ~55% faster |
//! | 8 requests          | 400ms  | 90-120ms | ~70% faster |
//!
//! # Usage Example
//!
//! ```ignore
//! use crate::storage::concurrency::ConcurrencyManager;
//!
//! async fn handle_get_object() {
//!     // Automatic request tracking with RAII guard
//!     let _guard = ConcurrencyManager::track_request();
//!     
//!     // Try cache first (sub-5ms if hit)
//!     if let Some(data) = manager.get_cached(&key).await {
//!         return serve_from_cache(data);
//!     }
//!     
//!     // Rate-limited disk read
//!     let _permit = manager.acquire_disk_read_permit().await;
//!     
//!     // Use adaptive buffer size
//!     let buffer_size = get_concurrency_aware_buffer_size(file_size, base_buffer);
//!     // ... read from disk ...
//! }
//! ```

use moka::future::Cache;
use rustfs_config::{KI_B, MI_B};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

// ============================================
// Adaptive I/O Strategy Types
// ============================================

/// Load level classification based on disk permit wait times.
///
/// This enum represents the current I/O load on the system, determined by
/// analyzing disk permit acquisition wait times. Longer wait times indicate
/// higher contention and system load.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoLoadLevel {
    /// Low load: wait time < 10ms. System has ample I/O capacity.
    Low,
    /// Medium load: wait time 10-50ms. System is moderately loaded.
    Medium,
    /// High load: wait time 50-200ms. System is under significant load.
    High,
    /// Critical load: wait time > 200ms. System is heavily congested.
    Critical,
}

impl IoLoadLevel {
    /// Determine load level from disk permit wait duration.
    ///
    /// Thresholds are based on typical NVMe SSD characteristics:
    /// - Low: < 10ms (normal operation)
    /// - Medium: 10-50ms (moderate contention)
    /// - High: 50-200ms (significant contention)
    /// - Critical: > 200ms (severe congestion)
    pub fn from_wait_duration(wait: Duration) -> Self {
        let wait_ms = wait.as_millis();
        if wait_ms < 10 {
            IoLoadLevel::Low
        } else if wait_ms < 50 {
            IoLoadLevel::Medium
        } else if wait_ms < 200 {
            IoLoadLevel::High
        } else {
            IoLoadLevel::Critical
        }
    }
}

/// Adaptive I/O strategy calculated from current system load.
///
/// This structure provides optimized I/O parameters based on the observed
/// disk permit wait times. It helps balance throughput vs. latency and
/// prevents I/O saturation under high load.
///
/// # Usage Example
///
/// ```ignore
/// let strategy = manager.calculate_io_strategy(permit_wait_duration);
///
/// // Apply strategy to I/O operations
/// let buffer_size = strategy.buffer_size;
/// let enable_readahead = strategy.enable_readahead;
/// let enable_cache_writeback = strategy.cache_writeback_enabled;
/// ```
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct IoStrategy {
    /// Recommended buffer size for I/O operations (in bytes).
    ///
    /// Under high load, this is reduced to improve fairness and reduce memory pressure.
    /// Under low load, this is maximized for throughput.
    pub buffer_size: usize,

    /// Buffer size multiplier (0.4 - 1.0) applied to base buffer size.
    ///
    /// - 1.0: Low load - use full buffer
    /// - 0.75: Medium load - slightly reduced
    /// - 0.5: High load - significantly reduced
    /// - 0.4: Critical load - minimal buffer
    pub buffer_multiplier: f64,

    /// Whether to enable aggressive read-ahead for sequential reads.
    ///
    /// Disabled under high load to reduce I/O amplification.
    pub enable_readahead: bool,

    /// Whether to enable cache writeback for this request.
    ///
    /// May be disabled under extreme load to reduce memory pressure.
    pub cache_writeback_enabled: bool,

    /// Whether to use tokio BufReader for improved async I/O.
    ///
    /// Always enabled for better async performance.
    pub use_buffered_io: bool,

    /// The detected I/O load level.
    pub load_level: IoLoadLevel,

    /// The raw permit wait duration that was used to calculate this strategy.
    pub permit_wait_duration: Duration,
}

impl IoStrategy {
    /// Create a new IoStrategy from disk permit wait time and base buffer size.
    ///
    /// This analyzes the wait duration to determine the current I/O load level
    /// and calculates appropriate I/O parameters.
    ///
    /// # Arguments
    ///
    /// * `permit_wait_duration` - Time spent waiting for disk read permit
    /// * `base_buffer_size` - Base buffer size from workload configuration
    ///
    /// # Returns
    ///
    /// An IoStrategy with optimized parameters for the current load level.
    pub fn from_wait_duration(permit_wait_duration: Duration, base_buffer_size: usize) -> Self {
        let load_level = IoLoadLevel::from_wait_duration(permit_wait_duration);

        // Calculate buffer multiplier based on load level
        let buffer_multiplier = match load_level {
            IoLoadLevel::Low => 1.0,
            IoLoadLevel::Medium => 0.75,
            IoLoadLevel::High => 0.5,
            IoLoadLevel::Critical => 0.4,
        };

        // Calculate actual buffer size
        let buffer_size = ((base_buffer_size as f64) * buffer_multiplier) as usize;
        let buffer_size = buffer_size.clamp(32 * KI_B, MI_B);

        // Determine feature toggles based on load
        let enable_readahead = match load_level {
            IoLoadLevel::Low | IoLoadLevel::Medium => true,
            IoLoadLevel::High | IoLoadLevel::Critical => false,
        };

        let cache_writeback_enabled = match load_level {
            IoLoadLevel::Low | IoLoadLevel::Medium | IoLoadLevel::High => true,
            IoLoadLevel::Critical => false, // Disable under extreme load
        };

        Self {
            buffer_size,
            buffer_multiplier,
            enable_readahead,
            cache_writeback_enabled,
            use_buffered_io: true, // Always enabled
            load_level,
            permit_wait_duration,
        }
    }

    /// Get a human-readable description of the current I/O strategy.
    #[allow(dead_code)]
    pub fn description(&self) -> String {
        format!(
            "IoStrategy[{:?}]: buffer={}KB, multiplier={:.2}, readahead={}, cache_wb={}, wait={:?}",
            self.load_level,
            self.buffer_size / 1024,
            self.buffer_multiplier,
            self.enable_readahead,
            self.cache_writeback_enabled,
            self.permit_wait_duration
        )
    }
}

/// Rolling window metrics for I/O load tracking.
///
/// This structure maintains a sliding window of recent disk permit wait times
/// to provide smoothed load level estimates. This helps prevent strategy
/// oscillation from transient load spikes.
#[allow(dead_code)]
#[derive(Debug)]
struct IoLoadMetrics {
    /// Recent permit wait durations (sliding window)
    recent_waits: Vec<Duration>,
    /// Maximum samples to keep in the window
    max_samples: usize,
    /// Total wait time observed (for averaging)
    total_wait_ns: AtomicU64,
    /// Total number of observations
    observation_count: AtomicU64,
}

#[allow(dead_code)]
impl IoLoadMetrics {
    fn new(max_samples: usize) -> Self {
        Self {
            recent_waits: Vec::with_capacity(max_samples),
            max_samples,
            total_wait_ns: AtomicU64::new(0),
            observation_count: AtomicU64::new(0),
        }
    }

    /// Record a new permit wait observation
    fn record(&mut self, wait: Duration) {
        // Add to recent waits (with eviction if full)
        if self.recent_waits.len() >= self.max_samples {
            self.recent_waits.remove(0);
        }
        self.recent_waits.push(wait);

        // Update totals for overall statistics
        self.total_wait_ns.fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
        self.observation_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the average wait duration over the recent window
    fn average_wait(&self) -> Duration {
        if self.recent_waits.is_empty() {
            return Duration::ZERO;
        }
        let total: Duration = self.recent_waits.iter().sum();
        total / self.recent_waits.len() as u32
    }

    /// Get the maximum wait duration in the recent window
    fn max_wait(&self) -> Duration {
        self.recent_waits.iter().copied().max().unwrap_or(Duration::ZERO)
    }

    /// Get the P95 wait duration from the recent window
    fn p95_wait(&self) -> Duration {
        if self.recent_waits.is_empty() {
            return Duration::ZERO;
        }
        let mut sorted = self.recent_waits.clone();
        sorted.sort();
        let p95_idx = ((sorted.len() as f64) * 0.95) as usize;
        sorted.get(p95_idx.min(sorted.len() - 1)).copied().unwrap_or(Duration::ZERO)
    }

    /// Get the smoothed load level based on recent observations
    fn smoothed_load_level(&self) -> IoLoadLevel {
        IoLoadLevel::from_wait_duration(self.average_wait())
    }

    /// Get the overall average wait since startup
    fn lifetime_average_wait(&self) -> Duration {
        let total = self.total_wait_ns.load(Ordering::Relaxed);
        let count = self.observation_count.load(Ordering::Relaxed);
        if count == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(total / count)
        }
    }

    /// Get the total observation count
    fn observation_count(&self) -> u64 {
        self.observation_count.load(Ordering::Relaxed)
    }
}

/// Global concurrent request counter for adaptive buffer sizing.
///
/// This atomic counter tracks the number of active GetObject requests in real-time.
/// It's used by the buffer sizing algorithm to dynamically adjust memory allocation
/// based on current system load, preventing memory contention under high concurrency.
///
/// Access pattern: Lock-free atomic operations (Relaxed ordering for performance).
static ACTIVE_GET_REQUESTS: AtomicUsize = AtomicUsize::new(0);

/// Global concurrency manager instance
static CONCURRENCY_MANAGER: LazyLock<ConcurrencyManager> = LazyLock::new(ConcurrencyManager::new);

/// RAII guard for tracking active GetObject requests.
///
/// This guard automatically increments the concurrent request counter when created
/// and decrements it when dropped. This ensures accurate tracking even if requests
/// fail or panic, preventing counter leaks that could permanently degrade performance.
///
/// # Thread Safety
///
/// Safe to use across threads. The underlying atomic counter uses Relaxed ordering
/// for performance since exact synchronization isn't required for buffer sizing hints.
///
/// # Metrics
///
/// On drop, automatically records request completion and duration metrics (when the
/// "metrics" feature is enabled) for Prometheus monitoring and alerting.
///
/// # Example
///
/// ```ignore
/// async fn get_object() {
///     let _guard = GetObjectGuard::new();
///     // Request counter incremented automatically
///     // ... process request ...
///     // Counter decremented automatically when guard drops
/// }
/// ```
#[derive(Debug)]
pub struct GetObjectGuard {
    /// Track when the request started for metrics collection.
    /// Used to calculate end-to-end request latency in the Drop implementation.
    start_time: Instant,
    /// Reference to the concurrency manager for cleanup operations.
    /// The underscore prefix indicates this is used implicitly (for type safety).
    _manager: &'static ConcurrencyManager,
}

impl GetObjectGuard {
    /// Create a new guard, incrementing the active request counter atomically.
    ///
    /// This method is called automatically by `ConcurrencyManager::track_request()`.
    /// The counter increment is guaranteed to be visible to concurrent readers
    /// immediately due to atomic operations.
    fn new() -> Self {
        ACTIVE_GET_REQUESTS.fetch_add(1, Ordering::Relaxed);
        Self {
            start_time: Instant::now(),
            _manager: &CONCURRENCY_MANAGER,
        }
    }

    /// Get the elapsed time since the request started.
    ///
    /// Useful for logging or metrics collection during request processing.
    /// Called automatically in the Drop implementation for duration tracking.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Get the current concurrent request count.
    ///
    /// Returns the instantaneous number of active GetObject requests across all threads.
    /// This value is used by buffer sizing algorithms to adapt to current system load.
    ///
    /// # Returns
    ///
    /// Current number of concurrent requests (including this one)
    pub fn concurrent_requests() -> usize {
        ACTIVE_GET_REQUESTS.load(Ordering::Relaxed)
    }
}

impl Drop for GetObjectGuard {
    /// Automatically called when the guard goes out of scope.
    ///
    /// Performs cleanup operations:
    /// 1. Decrements the concurrent request counter atomically
    /// 2. Records completion and duration metrics (if metrics feature enabled)
    ///
    /// This ensures accurate tracking even in error/panic scenarios, as Drop
    /// is called during stack unwinding (unless explicitly forgotten).
    fn drop(&mut self) {
        // Decrement concurrent request counter
        ACTIVE_GET_REQUESTS.fetch_sub(1, Ordering::Relaxed);

        // Record Prometheus metrics for monitoring and alerting
        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, histogram};
            // Track total completed requests for throughput calculation
            counter!("rustfs.get.object.requests.completed").increment(1);
            // Track request duration histogram for latency percentiles (P50, P95, P99)
            histogram!("rustfs.get.object.duration.seconds").record(self.elapsed().as_secs_f64());
        }
    }
}

/// Concurrency-aware buffer size calculator
///
/// This function adapts buffer sizes based on the current concurrent request load
/// to optimize for both throughput and fairness.
///
/// # Strategy
///
/// - **Low concurrency (1-2)**: Use large buffers (512KB-1MB) for maximum throughput
/// - **Medium concurrency (3-8)**: Use moderate buffers (128KB-256KB) for balanced performance
/// - **High concurrency (>8)**: Use smaller buffers (64KB-128KB) for fairness and memory efficiency
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
///
/// # Returns
///
/// Optimized buffer size in bytes for the current concurrency level
pub fn get_concurrency_aware_buffer_size(file_size: i64, base_buffer_size: usize) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // Record concurrent request metrics
    #[cfg(feature = "metrics")]
    {
        use metrics::gauge;
        gauge!("rustfs.concurrent.get.requests").set(concurrent_requests as f64);
    }

    // For low concurrency, use the base buffer size for maximum throughput
    if concurrent_requests <= 1 {
        return base_buffer_size;
    }
    let medium_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
    );
    let high_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
    );

    // Calculate adaptive multiplier based on concurrency level
    let adaptive_multiplier = if concurrent_requests <= 2 {
        // Low concurrency (1-2): use full buffer for maximum throughput
        1.0
    } else if concurrent_requests <= medium_threshold {
        // Medium concurrency (3-4): slightly reduce buffer size (75% of base)
        0.75
    } else if concurrent_requests <= high_threshold {
        // Higher concurrency (5-8): more aggressive reduction (50% of base)
        0.5
    } else {
        // Very high concurrency (>8): minimize memory per request (40% of base)
        0.4
    };

    // Calculate the adjusted buffer size
    let adjusted_size = (base_buffer_size as f64 * adaptive_multiplier) as usize;

    // Ensure we stay within reasonable bounds
    let min_buffer = if file_size > 0 && file_size < 100 * KI_B as i64 {
        32 * KI_B // For very small files, use minimum buffer
    } else {
        64 * KI_B // Standard minimum buffer size
    };

    let max_buffer = if concurrent_requests > high_threshold {
        256 * KI_B // Cap at 256KB for high concurrency
    } else {
        MI_B // Cap at 1MB for lower concurrency
    };

    adjusted_size.clamp(min_buffer, max_buffer)
}

/// Advanced concurrency-aware buffer sizing with file size optimization
///
/// This enhanced version considers both concurrency level and file size patterns
/// to provide even better performance characteristics.
///
/// # Arguments
///
/// * `file_size` - The size of the file being read, or -1 if unknown
/// * `base_buffer_size` - The baseline buffer size from workload profile
/// * `is_sequential` - Whether this is a sequential read (hint for optimization)
///
/// # Returns
///
/// Optimized buffer size in bytes
///
/// # Examples
///
/// ```ignore
/// let buffer_size = get_advanced_buffer_size(
///     32 * 1024 * 1024,  // 32MB file
///     256 * 1024,        // 256KB base buffer
///     true               // sequential read
/// );
/// ```
#[allow(dead_code)]
pub fn get_advanced_buffer_size(file_size: i64, base_buffer_size: usize, is_sequential: bool) -> usize {
    let concurrent_requests = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);

    // For very small files, use smaller buffers regardless of concurrency
    // Replace manual max/min chain with clamp
    if file_size > 0 && file_size < 256 * KI_B as i64 {
        return (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
    }

    // Base calculation from standard function
    let standard_size = get_concurrency_aware_buffer_size(file_size, base_buffer_size);
    let medium_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_MEDIUM_CONCURRENCY_THRESHOLD,
    );
    let high_threshold = rustfs_utils::get_env_usize(
        rustfs_config::ENV_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
        rustfs_config::DEFAULT_OBJECT_HIGH_CONCURRENCY_THRESHOLD,
    );
    // For sequential reads, we can be more aggressive with buffer sizes
    if is_sequential && concurrent_requests <= medium_threshold {
        return ((standard_size as f64 * 1.5) as usize).min(2 * MI_B);
    }

    // For high concurrency with large files, optimize for parallel processing
    if concurrent_requests > high_threshold && file_size > 10 * MI_B as i64 {
        // Use smaller, more numerous buffers for better parallelism
        return (standard_size as f64 * 0.8) as usize;
    }

    standard_size
}

/// High-performance cache for hot objects using Moka
///
/// This cache uses Moka for superior concurrent performance with features like:
/// - Lock-free reads and writes
/// - Automatic TTL and TTI expiration
/// - Size-based eviction with weigher function
/// - Built-in metrics collection
///
/// # Dual Cache Architecture
///
/// The cache maintains two separate Moka cache instances:
/// 1. `cache` - Simple byte array cache for raw object data (legacy support)
/// 2. `response_cache` - Full GetObject response cache with metadata
///
/// The response cache is preferred for new code as it stores complete response
/// metadata, enabling cache hits to bypass metadata lookups entirely.
#[derive(Clone)]
struct HotObjectCache {
    /// Moka cache instance for simple byte data (legacy)
    cache: Cache<String, Arc<CachedObject>>,
    /// Moka cache instance for full GetObject responses with metadata
    response_cache: Cache<String, Arc<CachedGetObjectInternal>>,
    /// Maximum size of individual objects to cache (10MB by default)
    max_object_size: usize,
    /// Global cache hit counter
    hit_count: Arc<AtomicU64>,
    /// Global cache miss counter
    miss_count: Arc<AtomicU64>,
}

impl std::fmt::Debug for HotObjectCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        f.debug_struct("HotObjectCache")
            .field("max_object_size", &self.max_object_size)
            .field("hit_count", &self.hit_count.load(Ordering::Relaxed))
            .field("miss_count", &self.miss_count.load(Ordering::Relaxed))
            .finish()
    }
}

/// A cached object with metadata and metrics
#[derive(Clone)]
struct CachedObject {
    /// The object data
    data: Arc<Vec<u8>>,
    /// When this object was cached
    cached_at: Instant,
    /// Object size in bytes
    size: usize,
    /// Number of times this object has been accessed
    access_count: Arc<AtomicU64>,
}

/// Comprehensive cached object with full response metadata for GetObject operations.
///
/// This structure stores all necessary fields to reconstruct a complete GetObjectOutput
/// response from cache, avoiding repeated disk reads and metadata lookups for hot objects.
///
/// # Fields
///
/// All time fields are serialized as RFC3339 strings to avoid parsing issues with
/// `Last-Modified` and other time headers.
///
/// # Usage
///
/// ```ignore
/// let cached = CachedGetObject {
///     body: Bytes::from(data),
///     content_length: data.len() as i64,
///     content_type: Some("application/octet-stream".to_string()),
///     e_tag: Some("\"abc123\"".to_string()),
///     last_modified: Some("2024-01-01T00:00:00Z".to_string()),
///     ..Default::default()
/// };
/// manager.put_cached_object(cache_key, cached).await;
/// ```
#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct CachedGetObject {
    /// The object body data
    pub body: bytes::Bytes,
    /// Content length in bytes
    pub content_length: i64,
    /// MIME content type
    pub content_type: Option<String>,
    /// Entity tag for the object
    pub e_tag: Option<String>,
    /// Last modified time as RFC3339 string (e.g., "2024-01-01T12:00:00Z")
    pub last_modified: Option<String>,
    /// Expiration time as RFC3339 string
    pub expires: Option<String>,
    /// Cache-Control header value
    pub cache_control: Option<String>,
    /// Content-Disposition header value
    pub content_disposition: Option<String>,
    /// Content-Encoding header value
    pub content_encoding: Option<String>,
    /// Content-Language header value
    pub content_language: Option<String>,
    /// Storage class (STANDARD, REDUCED_REDUNDANCY, etc.)
    pub storage_class: Option<String>,
    /// Version ID for versioned objects
    pub version_id: Option<String>,
    /// Whether this is a delete marker (for versioned buckets)
    pub delete_marker: bool,
    /// Number of tags associated with the object
    pub tag_count: Option<i32>,
    /// Replication status
    pub replication_status: Option<String>,
    /// User-defined metadata (x-amz-meta-*)
    pub user_metadata: std::collections::HashMap<String, String>,
    /// When this object was cached (for internal use, automatically set)
    cached_at: Option<Instant>,
    /// Access count for hot key tracking (automatically managed)
    access_count: Arc<AtomicU64>,
}

impl Default for CachedGetObject {
    fn default() -> Self {
        Self {
            body: bytes::Bytes::new(),
            content_length: 0,
            content_type: None,
            e_tag: None,
            last_modified: None,
            expires: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            storage_class: None,
            version_id: None,
            delete_marker: false,
            tag_count: None,
            replication_status: None,
            user_metadata: std::collections::HashMap::new(),
            cached_at: None,
            access_count: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl CachedGetObject {
    /// Create a new CachedGetObject with the given body and content length
    pub fn new(body: bytes::Bytes, content_length: i64) -> Self {
        Self {
            body,
            content_length,
            cached_at: Some(Instant::now()),
            access_count: Arc::new(AtomicU64::new(0)),
            ..Default::default()
        }
    }

    /// Builder method to set content_type
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.content_type = Some(content_type);
        self
    }

    /// Builder method to set e_tag
    pub fn with_e_tag(mut self, e_tag: String) -> Self {
        self.e_tag = Some(e_tag);
        self
    }

    /// Builder method to set last_modified
    pub fn with_last_modified(mut self, last_modified: String) -> Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// Builder method to set cache_control
    #[allow(dead_code)]
    pub fn with_cache_control(mut self, cache_control: String) -> Self {
        self.cache_control = Some(cache_control);
        self
    }

    /// Builder method to set storage_class
    #[allow(dead_code)]
    pub fn with_storage_class(mut self, storage_class: String) -> Self {
        self.storage_class = Some(storage_class);
        self
    }

    /// Builder method to set version_id
    #[allow(dead_code)]
    pub fn with_version_id(mut self, version_id: String) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Get the size in bytes for cache eviction calculations
    pub fn size(&self) -> usize {
        self.body.len()
    }

    /// Increment access count and return the new value
    pub fn increment_access(&self) -> u64 {
        self.access_count.fetch_add(1, Ordering::Relaxed) + 1
    }
}

/// Internal wrapper for CachedGetObject in the Moka cache
#[derive(Clone)]
struct CachedGetObjectInternal {
    /// The cached response data
    data: Arc<CachedGetObject>,
    /// When this object was cached
    cached_at: Instant,
    /// Size in bytes for weigher function
    size: usize,
}

impl HotObjectCache {
    /// Create a new hot object cache with Moka
    ///
    /// Configures Moka with:
    /// - Size-based eviction (100MB max)
    /// - TTL of 5 minutes
    /// - TTI of 2 minutes
    /// - Weigher function for accurate size tracking
    fn new() -> Self {
        let max_capacity = rustfs_utils::get_env_u64(
            rustfs_config::ENV_OBJECT_CACHE_CAPACITY_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_CAPACITY_MB,
        );
        let cache_tti_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTI_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTI_SECS);
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);

        // Legacy simple byte cache
        let cache = Cache::builder()
            .max_capacity(max_capacity * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedObject>| -> u32 {
                // Weight based on actual data size
                value.size.min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();

        // Full response cache with metadata
        let response_cache = Cache::builder()
            .max_capacity(max_capacity * MI_B as u64)
            .weigher(|_key: &String, value: &Arc<CachedGetObjectInternal>| -> u32 {
                // Weight based on actual data size
                value.size.min(u32::MAX as usize) as u32
            })
            .time_to_live(Duration::from_secs(cache_ttl_secs))
            .time_to_idle(Duration::from_secs(cache_tti_secs))
            .build();
        let max_object_size = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
            rustfs_config::DEFAULT_OBJECT_CACHE_MAX_OBJECT_SIZE_MB,
        ) * MI_B;
        Self {
            cache,
            response_cache,
            max_object_size,
            hit_count: Arc::new(AtomicU64::new(0)),
            miss_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Soft expiration determination, the number of hits is insufficient and exceeds the soft TTL
    fn should_expire(&self, obj: &Arc<CachedObject>) -> bool {
        let age_secs = obj.cached_at.elapsed().as_secs();
        let cache_ttl_secs =
            rustfs_utils::get_env_u64(rustfs_config::ENV_OBJECT_CACHE_TTL_SECS, rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS);
        let hot_object_min_hits_to_extend = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND,
            rustfs_config::DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND,
        );
        if age_secs >= cache_ttl_secs {
            let hits = obj.access_count.load(Ordering::Relaxed);
            return hits < hot_object_min_hits_to_extend as u64;
        }
        false
    }

    /// Get an object from cache with lock-free concurrent access
    ///
    /// Moka provides lock-free reads, significantly improving concurrent performance.
    async fn get(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        match self.cache.get(key).await {
            Some(cached) => {
                if self.should_expire(&cached) {
                    self.cache.invalidate(key).await;
                    self.miss_count.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                // Update access count
                cached.access_count.fetch_add(1, Ordering::Relaxed);
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs.object.cache.hits").increment(1);
                    counter!("rustfs.object.cache.access.count", "key" => key.to_string()).increment(1);
                }

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs.object.cache.misses").increment(1);
                }

                None
            }
        }
    }

    /// Put an object into cache with automatic size-based eviction
    ///
    /// Moka handles eviction automatically based on the weigher function.
    #[allow(dead_code)]
    async fn put(&self, key: String, data: Vec<u8>) {
        let size = data.len();

        // Only cache objects smaller than max_object_size
        if size == 0 || size > self.max_object_size {
            return;
        }

        let cached_obj = Arc::new(CachedObject {
            data: Arc::new(data),
            cached_at: Instant::now(),
            size,
            access_count: Arc::new(AtomicU64::new(0)),
        });

        self.cache.insert(key.clone(), cached_obj).await;

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge};
            counter!("rustfs.object.cache.insertions").increment(1);
            gauge!("rustfs_object_cache_size_bytes").set(self.cache.weighted_size() as f64);
            gauge!("rustfs_object_cache_entry_count").set(self.cache.entry_count() as f64);
        }
    }

    /// Clear all cached objects
    #[allow(dead_code)]
    async fn clear(&self) {
        self.cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
    }

    /// Get cache statistics for monitoring
    #[allow(dead_code)]
    async fn stats(&self) -> CacheStats {
        // Ensure pending tasks are processed for accurate stats
        self.cache.run_pending_tasks().await;
        let mut total_ms: u128 = 0;
        let mut cnt: u64 = 0;
        self.cache.iter().for_each(|(_, v)| {
            total_ms += v.cached_at.elapsed().as_millis();
            cnt += 1;
        });
        let avg_age_secs = if cnt == 0 {
            0.0
        } else {
            (total_ms as f64 / cnt as f64) / 1000.0
        };
        CacheStats {
            size: self.cache.weighted_size() as usize,
            entries: self.cache.entry_count() as usize,
            max_size: 100 * MI_B,
            max_object_size: self.max_object_size,
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
            avg_age_secs,
        }
    }

    /// Check if a key exists in cache (lock-free)
    #[allow(dead_code)]
    async fn contains(&self, key: &str) -> bool {
        self.cache.contains_key(key)
    }

    /// Get multiple objects from cache in parallel
    ///
    /// Leverages Moka's lock-free design for true parallel access.
    #[allow(dead_code)]
    async fn get_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key in keys {
            results.push(self.get(key).await);
        }
        results
    }

    /// Remove a specific key from cache
    #[allow(dead_code)]
    async fn remove(&self, key: &str) -> bool {
        let had_key = self.cache.contains_key(key);
        self.cache.invalidate(key).await;
        had_key
    }

    /// Get the most frequently accessed keys
    ///
    /// Returns up to `limit` keys sorted by access count in descending order.
    #[allow(dead_code)]
    async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        // Run pending tasks to ensure accurate entry count
        self.cache.run_pending_tasks().await;

        let mut entries: Vec<(String, u64)> = Vec::new();

        // Iterate through cache entries
        self.cache.iter().for_each(|(key, value)| {
            entries.push((key.to_string(), value.access_count.load(Ordering::Relaxed)));
        });

        entries.sort_by(|a, b| b.1.cmp(&a.1));
        entries.truncate(limit);
        entries
    }

    /// Warm up cache with a batch of objects
    #[allow(dead_code)]
    async fn warm(&self, objects: Vec<(String, Vec<u8>)>) {
        for (key, data) in objects {
            self.put(key, data).await;
        }
    }

    /// Get hit rate percentage
    #[allow(dead_code)]
    fn hit_rate(&self) -> f64 {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }

    // ============================================
    // Response Cache Methods (CachedGetObject)
    // ============================================

    /// Get a cached GetObject response with full metadata
    ///
    /// This method retrieves a complete GetObject response from the response cache,
    /// including body data and all response metadata (e_tag, last_modified, etc.).
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    ///
    /// # Returns
    ///
    /// * `Some(Arc<CachedGetObject>)` - Cached response data if found and not expired
    /// * `None` - Cache miss
    #[allow(dead_code)]
    async fn get_response(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        match self.response_cache.get(key).await {
            Some(cached) => {
                // Check soft expiration
                let age_secs = cached.cached_at.elapsed().as_secs();
                let cache_ttl_secs = rustfs_utils::get_env_u64(
                    rustfs_config::ENV_OBJECT_CACHE_TTL_SECS,
                    rustfs_config::DEFAULT_OBJECT_CACHE_TTL_SECS,
                );
                let hot_object_min_hits = rustfs_utils::get_env_usize(
                    rustfs_config::ENV_OBJECT_HOT_MIN_HITS_TO_EXTEND,
                    rustfs_config::DEFAULT_OBJECT_HOT_MIN_HITS_TO_EXTEND,
                );

                if age_secs >= cache_ttl_secs {
                    let hits = cached.data.access_count.load(Ordering::Relaxed);
                    if hits < hot_object_min_hits as u64 {
                        self.response_cache.invalidate(key).await;
                        self.miss_count.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                }

                // Update access count
                cached.data.increment_access();
                self.hit_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_response_cache_hits").increment(1);
                    counter!("rustfs_object_cache_access_count", "key" => key.to_string()).increment(1);
                }

                Some(Arc::clone(&cached.data))
            }
            None => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);

                #[cfg(feature = "metrics")]
                {
                    use metrics::counter;
                    counter!("rustfs_object_response_cache_misses").increment(1);
                }

                None
            }
        }
    }

    /// Put a GetObject response into the response cache
    ///
    /// This method caches a complete GetObject response including body and metadata.
    /// Objects larger than `max_object_size` or empty objects are not cached.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    /// * `response` - The complete cached response to store
    #[allow(dead_code)]
    async fn put_response(&self, key: String, response: CachedGetObject) {
        let size = response.size();

        // Only cache objects smaller than max_object_size
        if size == 0 || size > self.max_object_size {
            return;
        }

        let cached_internal = Arc::new(CachedGetObjectInternal {
            data: Arc::new(response),
            cached_at: Instant::now(),
            size,
        });

        self.response_cache.insert(key.clone(), cached_internal).await;

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge};
            counter!("rustfs_object_response_cache_insertions").increment(1);
            gauge!("rustfs_object_response_cache_size_bytes").set(self.response_cache.weighted_size() as f64);
            gauge!("rustfs_object_response_cache_entry_count").set(self.response_cache.entry_count() as f64);
        }
    }

    /// Invalidate a cache entry for a specific object
    ///
    /// This method removes both the simple byte cache entry and the response cache entry
    /// for the given key. Used when objects are modified or deleted.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key to invalidate (e.g., "{bucket}/{key}")
    #[allow(dead_code)]
    async fn invalidate(&self, key: &str) {
        // Invalidate both caches
        self.cache.invalidate(key).await;
        self.response_cache.invalidate(key).await;

        #[cfg(feature = "metrics")]
        {
            use metrics::counter;
            counter!("rustfs_object_cache_invalidations").increment(1);
        }
    }

    /// Invalidate cache entries for an object and its latest version
    ///
    /// For versioned buckets, this invalidates both:
    /// - The specific version key: "{bucket}/{key}?versionId={version_id}"
    /// - The latest version key: "{bucket}/{key}"
    ///
    /// This ensures that after a write/delete, clients don't receive stale data.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID (if None, only invalidates the base key)
    #[allow(dead_code)]
    async fn invalidate_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        // Always invalidate the latest version key
        let base_key = format!("{}/{}", bucket, key);
        self.invalidate(&base_key).await;

        // Also invalidate the specific version if provided
        if let Some(vid) = version_id {
            let versioned_key = format!("{}?versionId={}", base_key, vid);
            self.invalidate(&versioned_key).await;
        }
    }

    /// Clear all cached objects from both caches
    #[allow(dead_code)]
    async fn clear_all(&self) {
        self.cache.invalidate_all();
        self.response_cache.invalidate_all();
        // Sync to ensure all entries are removed
        self.cache.run_pending_tasks().await;
        self.response_cache.run_pending_tasks().await;
    }
}

/// Cache statistics for monitoring and debugging
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CacheStats {
    /// Current total size of cached objects in bytes
    pub size: usize,
    /// Number of cached entries
    pub entries: usize,
    /// Maximum allowed cache size in bytes
    pub max_size: usize,
    /// Maximum allowed object size in bytes
    pub max_object_size: usize,
    /// Total number of cache hits
    pub hit_count: u64,
    /// Total number of cache misses
    pub miss_count: u64,
    /// Average cache object age (seconds)
    pub avg_age_secs: f64,
}

/// Concurrency manager for coordinating concurrent GetObject requests
///
/// This manager provides:
/// - Adaptive I/O strategy based on disk permit wait times
/// - Hot object caching with Moka
/// - Disk read permit management to prevent I/O saturation
/// - Rolling metrics for load level smoothing
#[allow(dead_code)]
#[derive(Clone)]
pub struct ConcurrencyManager {
    /// Hot object cache for frequently accessed objects
    cache: Arc<HotObjectCache>,
    /// Semaphore to limit concurrent disk reads
    disk_read_semaphore: Arc<Semaphore>,
    /// Whether object caching is enabled (from RUSTFS_OBJECT_CACHE_ENABLE env var)
    cache_enabled: bool,
    /// I/O load metrics for adaptive strategy calculation
    io_metrics: Arc<Mutex<IoLoadMetrics>>,
}

impl std::fmt::Debug for ConcurrencyManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::sync::atomic::Ordering;
        let io_metrics_info = if let Ok(metrics) = self.io_metrics.lock() {
            format!("avg_wait={:?}, observations={}", metrics.average_wait(), metrics.observation_count())
        } else {
            "locked".to_string()
        };
        f.debug_struct("ConcurrencyManager")
            .field("active_requests", &ACTIVE_GET_REQUESTS.load(Ordering::Relaxed))
            .field("disk_read_permits", &self.disk_read_semaphore.available_permits())
            .field("io_metrics", &io_metrics_info)
            .finish()
    }
}

impl ConcurrencyManager {
    /// Create a new concurrency manager with default settings
    ///
    /// Reads configuration from environment variables:
    /// - `RUSTFS_OBJECT_CACHE_ENABLE`: Enable/disable object caching (default: false)
    pub fn new() -> Self {
        let cache_enabled =
            rustfs_utils::get_env_bool(rustfs_config::ENV_OBJECT_CACHE_ENABLE, rustfs_config::DEFAULT_OBJECT_CACHE_ENABLE);

        let max_disk_reads = rustfs_utils::get_env_usize(
            rustfs_config::ENV_OBJECT_MAX_CONCURRENT_DISK_READS,
            rustfs_config::DEFAULT_OBJECT_MAX_CONCURRENT_DISK_READS,
        );

        Self {
            cache: Arc::new(HotObjectCache::new()),
            disk_read_semaphore: Arc::new(Semaphore::new(max_disk_reads)),
            cache_enabled,
            io_metrics: Arc::new(Mutex::new(IoLoadMetrics::new(100))), // Keep last 100 observations
        }
    }

    /// Check if object caching is enabled
    ///
    /// Returns true if the `RUSTFS_OBJECT_CACHE_ENABLE` environment variable
    /// is set to "true" (case-insensitive). When disabled, cache lookups and
    /// writebacks are skipped, reducing memory usage at the cost of repeated
    /// disk reads for the same objects.
    ///
    /// # Returns
    ///
    /// `true` if caching is enabled, `false` otherwise
    pub fn is_cache_enabled(&self) -> bool {
        self.cache_enabled
    }

    /// Track a GetObject request
    pub fn track_request() -> GetObjectGuard {
        GetObjectGuard::new()
    }

    /// Try to get an object from cache
    #[allow(dead_code)]
    pub async fn get_cached(&self, key: &str) -> Option<Arc<Vec<u8>>> {
        self.cache.get(key).await
    }

    /// Cache an object for future retrievals
    #[allow(dead_code)]
    pub async fn cache_object(&self, key: String, data: Vec<u8>) {
        self.cache.put(key, data).await;
    }

    /// Acquire a permit to perform a disk read operation
    ///
    /// This ensures we don't overwhelm the disk subsystem with too many
    /// concurrent reads, which can cause performance degradation.
    pub async fn acquire_disk_read_permit(&self) -> tokio::sync::SemaphorePermit<'_> {
        self.disk_read_semaphore
            .acquire()
            .await
            .expect("semaphore closed unexpectedly")
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

        // Record histogram metric for Prometheus
        #[cfg(feature = "metrics")]
        {
            use metrics::histogram;
            histogram!("rustfs.disk.permit.wait.duration.seconds").record(wait_duration.as_secs_f64());
        }
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

    /// Get the smoothed I/O load level based on recent observations.
    ///
    /// This uses the rolling window of permit wait times to provide a more
    /// stable estimate of the current load level, reducing oscillation from
    /// transient spikes.
    ///
    /// # Returns
    ///
    /// The smoothed `IoLoadLevel` based on average recent wait times.
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
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

    /// Get cache statistics
    #[allow(dead_code)]
    pub async fn cache_stats(&self) -> CacheStats {
        self.cache.stats().await
    }

    /// Clear all cached objects
    #[allow(dead_code)]
    pub async fn clear_cache(&self) {
        self.cache.clear().await;
    }

    /// Check if a key is cached
    #[allow(dead_code)]
    pub async fn is_cached(&self, key: &str) -> bool {
        self.cache.contains(key).await
    }

    /// Get multiple cached objects in a single operation
    #[allow(dead_code)]
    pub async fn get_cached_batch(&self, keys: &[String]) -> Vec<Option<Arc<Vec<u8>>>> {
        self.cache.get_batch(keys).await
    }

    /// Remove a specific object from cache
    #[allow(dead_code)]
    pub async fn remove_cached(&self, key: &str) -> bool {
        self.cache.remove(key).await
    }

    /// Get the most frequently accessed keys
    #[allow(dead_code)]
    pub async fn get_hot_keys(&self, limit: usize) -> Vec<(String, u64)> {
        self.cache.get_hot_keys(limit).await
    }

    /// Get cache hit rate percentage
    #[allow(dead_code)]
    pub fn cache_hit_rate(&self) -> f64 {
        self.cache.hit_rate()
    }

    /// Warm up cache with frequently accessed objects
    ///
    /// This can be called during server startup or maintenance windows
    /// to pre-populate the cache with known hot objects.
    #[allow(dead_code)]
    pub async fn warm_cache(&self, objects: Vec<(String, Vec<u8>)>) {
        self.cache.warm(objects).await;
    }

    /// Get optimized buffer size for a request
    ///
    /// This wraps the advanced buffer sizing logic and makes it accessible
    /// through the concurrency manager interface.
    #[allow(dead_code)]
    pub fn buffer_size(&self, file_size: i64, base: usize, sequential: bool) -> usize {
        get_advanced_buffer_size(file_size, base, sequential)
    }

    // ============================================
    // Response Cache Methods (CachedGetObject)
    // ============================================

    /// Get a cached GetObject response with full metadata
    ///
    /// This method retrieves a complete GetObject response from the response cache,
    /// including body data and all response metadata (e_tag, last_modified, content_type, etc.).
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    ///
    /// # Returns
    ///
    /// * `Some(Arc<CachedGetObject>)` - Cached response data if found and not expired
    /// * `None` - Cache miss
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cache_key = format!("{}/{}", bucket, key);
    /// if let Some(cached) = manager.get_cached_object(&cache_key).await {
    ///     // Build response from cached data
    ///     let output = GetObjectOutput {
    ///         body: Some(StreamingBlob::from(cached.body.clone())),
    ///         content_length: Some(cached.content_length),
    ///         e_tag: cached.e_tag.clone(),
    ///         last_modified: cached.last_modified.as_ref().map(|s| parse_rfc3339(s)),
    ///         ..Default::default()
    ///     };
    /// }
    /// ```
    #[allow(dead_code)]
    pub async fn get_cached_object(&self, key: &str) -> Option<Arc<CachedGetObject>> {
        self.cache.get_response(key).await
    }

    /// Cache a complete GetObject response for future retrievals
    ///
    /// This method caches a complete GetObject response including body and all metadata.
    /// Objects larger than the maximum cache size (10MB by default) or empty objects
    /// are not cached.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key in the format "{bucket}/{key}" or "{bucket}/{key}?versionId={version_id}"
    /// * `response` - The complete cached response to store
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cached = CachedGetObject {
    ///     body: Bytes::from(data),
    ///     content_length: data.len() as i64,
    ///     content_type: Some("application/octet-stream".to_string()),
    ///     e_tag: Some("\"abc123\"".to_string()),
    ///     last_modified: Some("2024-01-01T00:00:00Z".to_string()),
    ///     ..Default::default()
    /// };
    /// manager.put_cached_object(cache_key, cached).await;
    /// ```
    #[allow(dead_code)]
    pub async fn put_cached_object(&self, key: String, response: CachedGetObject) {
        self.cache.put_response(key, response).await;
    }

    /// Invalidate cache entries for a specific object
    ///
    /// This method removes both simple byte cache and response cache entries
    /// for the given key. Should be called after write operations (put_object,
    /// copy_object, delete_object, etc.) to prevent stale data from being served.
    ///
    /// # Arguments
    ///
    /// * `key` - Cache key to invalidate (e.g., "{bucket}/{key}")
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After put_object succeeds
    /// let cache_key = format!("{}/{}", bucket, key);
    /// manager.invalidate_cache(&cache_key).await;
    /// ```
    #[allow(dead_code)]
    pub async fn invalidate_cache(&self, key: &str) {
        self.cache.invalidate(key).await;
    }

    /// Invalidate cache entries for an object and its latest version
    ///
    /// For versioned buckets, this invalidates both:
    /// - The specific version key: "{bucket}/{key}?versionId={version_id}"
    /// - The latest version key: "{bucket}/{key}"
    ///
    /// This ensures that after a write/delete, clients don't receive stale data.
    /// Should be called after any write operation that modifies object data or creates
    /// new versions.
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID (if None, only invalidates the base key)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // After delete_object with version
    /// manager.invalidate_cache_versioned(&bucket, &key, Some(&version_id)).await;
    ///
    /// // After put_object (invalidates latest)
    /// manager.invalidate_cache_versioned(&bucket, &key, None).await;
    /// ```
    #[allow(dead_code)]
    pub async fn invalidate_cache_versioned(&self, bucket: &str, key: &str, version_id: Option<&str>) {
        self.cache.invalidate_versioned(bucket, key, version_id).await;
    }

    /// Generate a cache key for an object
    ///
    /// Creates a cache key in the appropriate format based on whether a version ID
    /// is specified. For versioned requests, uses "{bucket}/{key}?versionId={version_id}".
    /// For non-versioned requests, uses "{bucket}/{key}".
    ///
    /// # Arguments
    ///
    /// * `bucket` - Bucket name
    /// * `key` - Object key
    /// * `version_id` - Optional version ID
    ///
    /// # Returns
    ///
    /// Cache key string
    pub fn make_cache_key(bucket: &str, key: &str, version_id: Option<&str>) -> String {
        match version_id {
            Some(vid) => format!("{}/{}?versionId={}", bucket, key, vid),
            None => format!("{}/{}", bucket, key),
        }
    }

    /// Get maximum cacheable object size
    ///
    /// Returns the maximum size in bytes for objects that can be cached.
    /// Objects larger than this size are not cached to prevent memory exhaustion.
    pub fn max_object_size(&self) -> usize {
        self.cache.max_object_size
    }
}

impl Default for ConcurrencyManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Get the global concurrency manager instance
pub fn get_concurrency_manager() -> &'static ConcurrencyManager {
    &CONCURRENCY_MANAGER
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_concurrent_request_tracking() {
        // Ensure we start from a clean state
        assert_eq!(GetObjectGuard::concurrent_requests(), 0);

        let _guard1 = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_requests(), 1);

        let _guard2 = GetObjectGuard::new();
        assert_eq!(GetObjectGuard::concurrent_requests(), 2);

        drop(_guard1);
        assert_eq!(GetObjectGuard::concurrent_requests(), 1);

        drop(_guard2);
        assert_eq!(GetObjectGuard::concurrent_requests(), 0);
    }

    #[test]
    fn test_adaptive_buffer_sizing() {
        // Reset concurrent requests
        ACTIVE_GET_REQUESTS.store(0, Ordering::Relaxed);
        let base_buffer = 256 * KI_B;

        // Test low concurrency (1 request)
        ACTIVE_GET_REQUESTS.store(1, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert_eq!(result, base_buffer, "Single request should use full buffer");

        // Test medium concurrency (3 requests)
        ACTIVE_GET_REQUESTS.store(3, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result < base_buffer && result >= base_buffer / 2);

        // Test higher concurrency (6 requests)
        ACTIVE_GET_REQUESTS.store(6, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result <= base_buffer / 2 && result >= base_buffer / 3);

        // Test very high concurrency (10 requests)
        ACTIVE_GET_REQUESTS.store(10, Ordering::Relaxed);
        let result = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);
        assert!(result <= base_buffer / 2 && result >= 64 * KI_B);
    }

    #[tokio::test]
    async fn test_hot_object_cache() {
        let cache = HotObjectCache::new();
        let test_data = vec![1u8; 1024];

        // Test basic put and get
        cache.put("test_key".to_string(), test_data.clone()).await;
        let retrieved = cache.get("test_key").await;
        assert!(retrieved.is_some());
        assert_eq!(*retrieved.unwrap(), test_data);

        // Test cache miss
        let missing = cache.get("nonexistent").await;
        assert!(missing.is_none());
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let cache = HotObjectCache::new();

        // Fill cache with objects
        for i in 0..200 {
            let data = vec![0u8; 64 * KI_B];
            cache.put(format!("key_{}", i), data).await;
        }

        let stats = cache.stats().await;
        assert!(
            stats.size <= stats.max_size,
            "Cache size {} should not exceed max {}",
            stats.size,
            stats.max_size
        );
    }

    #[tokio::test]
    async fn test_cache_reject_large_objects() {
        let cache = HotObjectCache::new();
        let large_data = vec![0u8; 11 * MI_B]; // Larger than max_object_size

        cache.put("large_object".to_string(), large_data).await;
        let retrieved = cache.get("large_object").await;
        assert!(retrieved.is_none(), "Large objects should not be cached");
    }

    #[test]
    fn test_concurrency_manager_creation() {
        let manager = ConcurrencyManager::new();
        assert_eq!(
            manager.disk_read_semaphore.available_permits(),
            64,
            "Should start with 64 available disk read permits"
        );
    }

    #[tokio::test]
    async fn test_disk_read_permits() {
        let manager = ConcurrencyManager::new();

        let permit1 = manager.acquire_disk_read_permit().await;
        assert_eq!(manager.disk_read_semaphore.available_permits(), 63);

        let permit2 = manager.acquire_disk_read_permit().await;
        assert_eq!(manager.disk_read_semaphore.available_permits(), 62);

        drop(permit1);
        assert_eq!(manager.disk_read_semaphore.available_permits(), 63);

        drop(permit2);
        assert_eq!(manager.disk_read_semaphore.available_permits(), 64);
    }

    #[test]
    fn test_advanced_buffer_size_small_files() {
        ACTIVE_GET_REQUESTS.store(1, Ordering::Relaxed);

        // Test small file optimization
        let result = get_advanced_buffer_size(32 * KI_B as i64, 256 * KI_B, true);
        assert!(
            (16 * KI_B..=64 * KI_B).contains(&result),
            "Small files should use reduced buffer: {}",
            result
        );
    }

    #[test]
    fn test_clamp_behavior() {
        // Test the clamp replacement
        let file_size = 100 * KI_B as i64;
        let result = (file_size as usize / 4).clamp(16 * KI_B, 64 * KI_B);
        assert!((16 * KI_B..=64 * KI_B).contains(&result));
    }

    #[tokio::test]
    async fn test_hot_keys_tracking() {
        let manager = ConcurrencyManager::new();

        // Cache some objects with different access patterns
        manager.cache_object("hot1".to_string(), vec![1u8; 100]).await;
        manager.cache_object("hot2".to_string(), vec![2u8; 100]).await;

        // Access them multiple times to build hit counts
        for _ in 0..5 {
            let _ = manager.get_cached("hot1").await;
        }
        for _ in 0..3 {
            let _ = manager.get_cached("hot2").await;
        }

        let hot_keys = manager.get_hot_keys(2).await;
        assert!(!hot_keys.is_empty(), "Should have hot keys");
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let manager = ConcurrencyManager::new();

        manager.cache_object("key1".to_string(), vec![1u8; 100]).await;
        manager.cache_object("key2".to_string(), vec![2u8; 100]).await;
        manager.cache_object("key3".to_string(), vec![3u8; 100]).await;

        let keys = vec!["key1".to_string(), "key2".to_string(), "key4".to_string()];
        let results = manager.get_cached_batch(&keys).await;

        assert_eq!(results.len(), 3);
        assert!(results[0].is_some());
        assert!(results[1].is_some());
        assert!(results[2].is_none()); // key4 doesn't exist
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let manager = ConcurrencyManager::new();

        manager.cache_object("key1".to_string(), vec![1u8; 1024]).await;
        manager.cache_object("key2".to_string(), vec![2u8; 1024]).await;

        let stats_before = manager.cache_stats().await;
        assert!(stats_before.entries > 0);

        manager.clear_cache().await;

        let stats_after = manager.cache_stats().await;
        assert_eq!(stats_after.entries, 0);
        assert_eq!(stats_after.size, 0);
    }

    #[tokio::test]
    async fn test_warm_cache() {
        let manager = ConcurrencyManager::new();

        let objects = vec![
            ("warm1".to_string(), vec![1u8; 100]),
            ("warm2".to_string(), vec![2u8; 100]),
            ("warm3".to_string(), vec![3u8; 100]),
        ];

        manager.warm_cache(objects).await;

        assert!(manager.is_cached("warm1").await);
        assert!(manager.is_cached("warm2").await);
        assert!(manager.is_cached("warm3").await);
    }
}
