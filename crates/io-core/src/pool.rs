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

//! Tiered buffer pool for zero-copy buffer management.
//!
//! Migrated from rustfs-ecstore to provide unified buffer pooling
//! across rustfs and rustfs-ecstore without cyclic dependencies.

use bytes::BytesMut;
use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

// Tier size thresholds
const SMALL_MAX: usize = 64 * 1024;
const MEDIUM_MAX: usize = 512 * 1024;
const LARGE_MAX: usize = 4 * 1024 * 1024;

/// Tiered buffer pool for zero-copy buffer management.
///
/// This pool provides 4 tiers of buffers for different size ranges:
/// - Small: 4KB - 64KB
/// - Medium: 64KB - 512KB
/// - Large: 512KB - 4MB
/// - XLarge: > 4MB
///
/// Buffers are automatically reused when returned to the pool.
///
/// # Example
///
/// ```ignore
/// let pool = BytesPool::new_tiered();
///
/// // Acquire a buffer (automatically selects tier based on size)
/// let mut buffer = pool.acquire_buffer(8192).await;
///
/// // Use the buffer...
/// buffer.put_slice(b"hello world");
///
/// // Return to pool (automatic when dropped)
/// drop(buffer);
///
/// // Next acquisition will reuse the buffer
/// let mut buffer2 = pool.acquire_buffer(8192).await;
/// assert!(pool.hit_rate() > 0.0); // Buffer was reused!
/// ```
#[derive(Clone)]
pub struct BytesPool {
    /// Small object pool (4KB - 64KB)
    small_pool: Arc<PoolTier>,
    /// Medium object pool (64KB - 512KB)
    medium_pool: Arc<PoolTier>,
    /// Large object pool (512KB - 4MB)
    large_pool: Arc<PoolTier>,
    /// Extra large pool (> 4MB)
    xlarge_pool: Arc<PoolTier>,
    /// Pool metrics
    metrics: Arc<BytesPoolMetrics>,
}

/// Single pool tier with concurrent access control and buffer reuse.
struct PoolTier {
    /// Buffer size for this tier
    buffer_size: usize,
    /// Maximum concurrent buffers
    max_buffers: usize,
    /// Semaphore for concurrency control
    semaphore: Arc<Semaphore>,
    /// Pool name for metrics
    name: &'static str,
    /// Queue of available buffers for reuse
    available_buffers: Mutex<Vec<BytesMut>>,
    /// Metrics for tracking this tier
    metrics: Mutex<Option<Arc<BytesPoolMetrics>>>,
    /// Total acquisitions for this tier
    tier_total_acquires: AtomicU64,
    /// Total hits for this tier
    tier_pool_hits: AtomicU64,
    /// Current allocated bytes for this tier
    tier_current_allocated_bytes: AtomicU64,
}

/// Pool metrics for monitoring and optimization.
///
/// Tracks acquisition patterns and memory usage.
#[derive(Debug, Default)]
pub struct BytesPoolMetrics {
    /// Total buffer acquisitions
    pub total_acquires: AtomicU64,
    /// Pool hits (buffer reused)
    pub pool_hits: AtomicU64,
    /// Pool misses (new allocation)
    pub pool_misses: AtomicU64,
    /// Total bytes allocated
    pub total_bytes_allocated: AtomicU64,
    /// Current allocated bytes
    pub current_allocated_bytes: AtomicU64,
    /// Current available buffers in pool
    pub available_buffers: AtomicU64,
}

/// A buffer managed by the BytesPool.
///
/// When dropped, the buffer is automatically returned to the pool for reuse.
pub struct PooledBuffer {
    /// The underlying buffer (ManuallyDrop to allow taking on drop)
    pub buffer: ManuallyDrop<BytesMut>,
    /// Reference to pool tier for returning buffer
    tier: Option<Arc<PoolTier>>,
    /// The semaphore permit (must be dropped last to release slot)
    _permit: Option<OwnedSemaphorePermit>,
}

/// BytesPool configuration.
///
/// Allows customization of buffer sizes and limits for each tier.
pub struct BytesPoolConfig {
    pub small_size: usize,
    pub small_max: usize,
    pub medium_size: usize,
    pub medium_max: usize,
    pub large_size: usize,
    pub large_max: usize,
    pub xlarge_size: usize,
    pub xlarge_max: usize,
}

impl Default for BytesPoolConfig {
    fn default() -> Self {
        Self {
            small_size: 4 * 1024,
            small_max: 1000,
            medium_size: 64 * 1024,
            medium_max: 500,
            large_size: 512 * 1024,
            large_max: 100,
            xlarge_size: 4 * 1024 * 1024,
            xlarge_max: 25,
        }
    }
}

impl BytesPool {
    /// Create new tiered pool with default configuration.
    ///
    /// # Tier Configuration
    ///
    /// - Small: 4KB buffers, max 1000 concurrent
    /// - Medium: 64KB buffers, max 500 concurrent
    /// - Large: 512KB buffers, max 100 concurrent
    /// - XLarge: 4MB buffers, max 25 concurrent
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pool = BytesPool::new_tiered();
    /// ```
    pub fn new_tiered() -> Self {
        Self::with_config(BytesPoolConfig::default())
    }

    /// Create pool with custom configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let config = BytesPoolConfig {
    ///     small_size: 8 * 1024,  // 8KB small buffers
    ///     small_max: 2000,
    ///     ..Default::default()
    /// };
    /// let pool = BytesPool::with_config(config);
    /// ```
    pub fn with_config(config: BytesPoolConfig) -> Self {
        let metrics = Arc::new(BytesPoolMetrics::default());
        let small_pool = Arc::new(PoolTier::new(config.small_size, config.small_max, "small"));
        let medium_pool = Arc::new(PoolTier::new(config.medium_size, config.medium_max, "medium"));
        let large_pool = Arc::new(PoolTier::new(config.large_size, config.large_max, "large"));
        let xlarge_pool = Arc::new(PoolTier::new(config.xlarge_size, config.xlarge_max, "xlarge"));

        // Set metrics reference in all tiers
        small_pool.set_metrics(Arc::clone(&metrics));
        medium_pool.set_metrics(Arc::clone(&metrics));
        large_pool.set_metrics(Arc::clone(&metrics));
        xlarge_pool.set_metrics(Arc::clone(&metrics));

        Self {
            small_pool,
            medium_pool,
            large_pool,
            xlarge_pool,
            metrics,
        }
    }

    /// Acquire buffer with automatic tier selection.
    ///
    /// Selects the appropriate tier based on requested size and blocks
    /// until a buffer is available. Reuses returned buffers when available.
    ///
    /// # Arguments
    ///
    /// * `size` - Minimum capacity for the buffer
    ///
    /// # Returns
    ///
    /// A PooledBuffer that releases the permit and returns buffer to pool when dropped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut buffer = pool.acquire_buffer(8192).await;
    /// ```
    pub async fn acquire_buffer(&self, size: usize) -> PooledBuffer {
        let tier = self.select_tier(size);
        let mut buffer = tier.acquire_buffer(size, &self.metrics).await;
        // Set tier reference for return on drop
        buffer.tier = Some(Arc::clone(tier));
        buffer
    }

    /// Try to acquire buffer without blocking.
    ///
    /// # Arguments
    ///
    /// * `size` - Minimum capacity for the buffer
    ///
    /// # Returns
    ///
    /// * `Some(buffer)` - If a buffer was available
    /// * `None` - If the pool is at capacity
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(mut buffer) = pool.try_acquire_buffer(8192) {
    ///     // Use buffer...
    /// }
    /// ```
    pub fn try_acquire_buffer(&self, size: usize) -> Option<PooledBuffer> {
        let tier = self.select_tier(size);
        let mut buffer = tier.try_acquire_buffer(size, &self.metrics)?;
        // Set tier reference for return on drop
        buffer.tier = Some(Arc::clone(tier));
        Some(buffer)
    }

    /// Select appropriate tier based on size.
    fn select_tier(&self, size: usize) -> &Arc<PoolTier> {
        if size <= SMALL_MAX {
            &self.small_pool
        } else if size <= MEDIUM_MAX {
            &self.medium_pool
        } else if size <= LARGE_MAX {
            &self.large_pool
        } else {
            &self.xlarge_pool
        }
    }

    /// Get pool metrics.
    pub fn metrics(&self) -> &BytesPoolMetrics {
        &self.metrics
    }

    /// Get pool hit rate (0.0 - 1.0).
    pub fn hit_rate(&self) -> f64 {
        let hits = self.metrics.pool_hits.load(Ordering::Relaxed);
        let total = self.metrics.total_acquires.load(Ordering::Relaxed);
        if total == 0 { 0.0 } else { hits as f64 / total as f64 }
    }

    /// Get the number of available buffers in the pool.
    pub fn available_buffers(&self) -> u64 {
        self.metrics.available_buffers.load(Ordering::Relaxed)
    }
}

impl PoolTier {
    fn new(buffer_size: usize, max_buffers: usize, name: &'static str) -> Self {
        Self {
            buffer_size,
            max_buffers,
            semaphore: Arc::new(Semaphore::new(max_buffers)),
            name,
            available_buffers: Mutex::new(Vec::new()),
            metrics: Mutex::new(None),
            tier_total_acquires: AtomicU64::new(0),
            tier_pool_hits: AtomicU64::new(0),
            tier_current_allocated_bytes: AtomicU64::new(0),
        }
    }

    fn set_metrics(&self, metrics: Arc<BytesPoolMetrics>) {
        *self.metrics.lock().unwrap() = Some(metrics);
    }

    fn take_or_allocate_buffer(&self, size: usize, pool_metrics: &BytesPoolMetrics) -> (BytesMut, bool) {
        let buffer_opt = {
            let mut available = self.available_buffers.lock().unwrap();
            available.pop()
        };
        let was_reused = buffer_opt.is_some();

        let buffer = if let Some(mut buf) = buffer_opt {
            let previous_capacity = buf.capacity();
            buf.clear();
            if previous_capacity < size {
                buf.reserve(size - previous_capacity);
            }
            let current_capacity = buf.capacity();
            if current_capacity > previous_capacity {
                let delta = (current_capacity - previous_capacity) as u64;
                pool_metrics.total_bytes_allocated.fetch_add(delta, Ordering::Relaxed);
                pool_metrics.current_allocated_bytes.fetch_add(delta, Ordering::Relaxed);
                self.tier_current_allocated_bytes.fetch_add(delta, Ordering::Relaxed);
            }
            buf
        } else {
            let buf = BytesMut::with_capacity(size.max(self.buffer_size));
            let allocated_bytes = buf.capacity() as u64;
            pool_metrics
                .total_bytes_allocated
                .fetch_add(allocated_bytes, Ordering::Relaxed);
            pool_metrics
                .current_allocated_bytes
                .fetch_add(allocated_bytes, Ordering::Relaxed);
            self.tier_current_allocated_bytes
                .fetch_add(allocated_bytes, Ordering::Relaxed);
            buf
        };

        (buffer, was_reused)
    }

    fn record_acquire_metrics(&self, pool_metrics: &BytesPoolMetrics, buffer_capacity: usize, was_reused: bool) {
        rustfs_io_metrics::record_bytes_pool_acquire(self.name, buffer_capacity, was_reused);

        if was_reused {
            pool_metrics.pool_hits.fetch_add(1, Ordering::Relaxed);
            self.tier_pool_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            pool_metrics.pool_misses.fetch_add(1, Ordering::Relaxed);
        }

        let tier_total_acquires = self.tier_total_acquires.load(Ordering::Relaxed);
        let tier_pool_hits = self.tier_pool_hits.load(Ordering::Relaxed);
        let tier_hit_rate = if tier_total_acquires == 0 {
            0.0
        } else {
            tier_pool_hits as f64 / tier_total_acquires as f64
        };
        rustfs_io_metrics::record_bytes_pool_hit_rate(self.name, tier_hit_rate);
        rustfs_io_metrics::record_bytes_pool_allocated(self.name, self.tier_current_allocated_bytes.load(Ordering::Relaxed));
    }

    async fn acquire_buffer(&self, size: usize, pool_metrics: &BytesPoolMetrics) -> PooledBuffer {
        // Acquire semaphore permit (owned for storage in PooledBuffer)
        let permit = Arc::clone(&self.semaphore).acquire_owned().await.unwrap();

        // Use the pool's shared metrics for recording
        let _metrics_lock = self.metrics.lock().unwrap();
        let _metrics = _metrics_lock.as_ref().unwrap();

        // Record acquisition
        pool_metrics.total_acquires.fetch_add(1, Ordering::Relaxed);
        self.tier_total_acquires.fetch_add(1, Ordering::Relaxed);

        let (buffer, was_reused) = self.take_or_allocate_buffer(size, pool_metrics);
        let buffer_capacity = buffer.capacity();
        self.record_acquire_metrics(pool_metrics, buffer_capacity, was_reused);

        PooledBuffer {
            buffer: ManuallyDrop::new(buffer),
            tier: None, // Will be set after creating Arc<PoolTier>
            _permit: Some(permit),
        }
    }

    fn try_acquire_buffer(&self, size: usize, pool_metrics: &BytesPoolMetrics) -> Option<PooledBuffer> {
        // Try to acquire permit without blocking
        let permit = Arc::clone(&self.semaphore).try_acquire_owned().ok()?;

        // Use the pool's shared metrics for recording
        let _metrics_lock = self.metrics.lock().unwrap();
        let _metrics = _metrics_lock.as_ref().unwrap();

        // Record acquisition
        pool_metrics.total_acquires.fetch_add(1, Ordering::Relaxed);
        self.tier_total_acquires.fetch_add(1, Ordering::Relaxed);

        let (buffer, was_reused) = self.take_or_allocate_buffer(size, pool_metrics);
        let buffer_capacity = buffer.capacity();
        self.record_acquire_metrics(pool_metrics, buffer_capacity, was_reused);

        Some(PooledBuffer {
            buffer: ManuallyDrop::new(buffer),
            tier: None,
            _permit: Some(permit),
        })
    }

    /// Return a buffer to the pool for reuse.
    fn return_buffer(&self, buffer: BytesMut) {
        let mut available = self.available_buffers.lock().unwrap();
        // Limit the size of the pool to prevent unbounded growth
        if available.len() < self.max_buffers {
            available.push(buffer);
            if let Some(ref metrics) = *self.metrics.lock().unwrap() {
                metrics.available_buffers.fetch_add(1, Ordering::Relaxed);
            }
        } else {
            let released_bytes = buffer.capacity() as u64;
            self.tier_current_allocated_bytes
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    Some(current.saturating_sub(released_bytes))
                })
                .ok();
            if let Some(ref metrics) = *self.metrics.lock().unwrap() {
                metrics
                    .current_allocated_bytes
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                        Some(current.saturating_sub(released_bytes))
                    })
                    .ok();
            }
        }
        // If pool is full, buffer is dropped and memory is freed
        rustfs_io_metrics::record_bytes_pool_allocated(self.name, self.tier_current_allocated_bytes.load(Ordering::Relaxed));
    }
}

impl Drop for PooledBuffer {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // Return buffer to pool if tier reference exists
        if let Some(ref tier) = self.tier {
            // Safety: We're in drop(), so this is the last use of the buffer
            // ManuallyDrop allows us to take the value without running BytesMut's drop
            let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
            tier.return_buffer(buffer);
        }
        // The permit is automatically dropped here, releasing the semaphore slot
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl std::fmt::Debug for BytesPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesPool")
            .field("small_pool", &self.small_pool)
            .field("medium_pool", &self.medium_pool)
            .field("large_pool", &self.large_pool)
            .field("xlarge_pool", &self.xlarge_pool)
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl std::fmt::Debug for PoolTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolTier")
            .field("name", &self.name)
            .field("buffer_size", &self.buffer_size)
            .field("max_buffers", &self.max_buffers)
            .field("available_permits", &self.semaphore.available_permits())
            .field("available_buffers", &self.available_buffers.lock().unwrap().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_tiered() {
        let pool = BytesPool::new_tiered();
        assert_eq!(pool.small_pool.buffer_size, 4 * 1024);
        assert_eq!(pool.small_pool.max_buffers, 1000);
    }

    #[tokio::test]
    async fn test_acquire_buffer() {
        let pool = BytesPool::new_tiered();
        let buffer = pool.acquire_buffer(2048).await;
        assert!(buffer.capacity() >= 2048);
    }

    #[tokio::test]
    async fn test_tier_selection() {
        let pool = BytesPool::new_tiered();

        // Small buffer (4KB - 64KB)
        let buf1 = pool.acquire_buffer(1024).await;
        assert_eq!(buf1.capacity(), 4 * 1024);

        // Medium buffer (64KB - 512KB) - capacity is max(requested, tier_size)
        let buf2 = pool.acquire_buffer(100 * 1024).await;
        assert_eq!(buf2.capacity(), 100 * 1024); // Requested size

        // Large buffer (512KB - 4MB)
        let buf3 = pool.acquire_buffer(1024 * 1024).await;
        assert_eq!(buf3.capacity(), 1024 * 1024); // Requested size

        // XLarge buffer (> 4MB)
        let buf4 = pool.acquire_buffer(8 * 1024 * 1024).await;
        assert_eq!(buf4.capacity(), 8 * 1024 * 1024); // Requested size
    }

    #[tokio::test]
    async fn test_try_acquire_buffer() {
        let pool = BytesPool::with_config(BytesPoolConfig {
            small_size: 1024,
            small_max: 1,
            ..Default::default()
        });

        // First acquisition should succeed
        let buffer1 = pool.try_acquire_buffer(512);
        assert!(buffer1.is_some());

        // Second should fail (pool at capacity)
        let buffer2 = pool.try_acquire_buffer(512);
        assert!(buffer2.is_none());
    }

    #[tokio::test]
    async fn test_metrics() {
        let pool = BytesPool::new_tiered();
        let _buffer = pool.acquire_buffer(1024).await;
        drop(_buffer);

        let metrics = pool.metrics();
        assert!(metrics.total_acquires.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn test_hit_rate() {
        let pool = BytesPool::new_tiered();
        assert_eq!(pool.hit_rate(), 0.0); // No acquisitions yet

        let _buffer = pool.acquire_buffer(1024).await;
        drop(_buffer);

        // First acquire is a miss (no buffers available yet)
        assert_eq!(pool.hit_rate(), 0.0);
    }

    #[tokio::test]
    async fn test_available_buffers() {
        let pool = BytesPool::new_tiered();
        assert_eq!(pool.available_buffers(), 0);

        let _buffer = pool.acquire_buffer(1024).await;
        drop(_buffer);

        // After drop, buffer should be returned to pool
        assert_eq!(pool.available_buffers(), 1);
    }

    #[tokio::test]
    async fn test_buffer_reuse() {
        // This test verifies that buffers are reused when returned to the pool
        let pool = BytesPool::with_config(BytesPoolConfig {
            small_size: 1024,
            small_max: 2,
            ..Default::default()
        });

        // Record initial state
        let initial_acquires = pool.metrics().total_acquires.load(Ordering::Relaxed);
        let initial_hits = pool.metrics().pool_hits.load(Ordering::Relaxed);
        assert_eq!(initial_acquires, 0);

        // First acquisition - should allocate new (miss)
        let buffer1 = pool.acquire_buffer(512).await;
        let initial_bytes_allocated = pool.metrics().total_bytes_allocated.load(Ordering::Relaxed);
        assert!(initial_bytes_allocated >= 1024);

        // Return buffer (by dropping)
        drop(buffer1);

        // Second acquisition - should reuse (hit)
        let _buffer2 = pool.acquire_buffer(512).await;
        let bytes_after_reuse = pool.metrics().total_bytes_allocated.load(Ordering::Relaxed);

        // Bytes allocated should be the same (buffer was reused)
        assert_eq!(initial_bytes_allocated, bytes_after_reuse);

        // Total acquires should be 2
        let total_acquires = pool.metrics().total_acquires.load(Ordering::Relaxed) - initial_acquires;
        assert_eq!(total_acquires, 2);

        // Pool hits should be 1
        let delta_hits = pool.metrics().pool_hits.load(Ordering::Relaxed) - initial_hits;
        assert_eq!(delta_hits, 1);
    }

    #[tokio::test]
    async fn test_tier_allocated_bytes_tracks_real_allocations() {
        let pool = BytesPool::with_config(BytesPoolConfig {
            small_size: 1024,
            small_max: 2,
            ..Default::default()
        });

        // First acquire allocates one small-tier buffer.
        let buf1 = pool.acquire_buffer(512).await;
        assert_eq!(pool.small_pool.tier_current_allocated_bytes.load(Ordering::Relaxed), 1024);

        // Return and reuse should not increase allocated bytes.
        drop(buf1);
        let buf2 = pool.acquire_buffer(512).await;
        assert_eq!(pool.small_pool.tier_current_allocated_bytes.load(Ordering::Relaxed), 1024);

        // A second in-flight buffer forces one more allocation.
        let _buf3 = pool.acquire_buffer(512).await;
        assert_eq!(pool.small_pool.tier_current_allocated_bytes.load(Ordering::Relaxed), 2048);

        drop(buf2);
    }

    #[tokio::test]
    async fn test_tier_hit_rate_counters_track_reuse() {
        let pool = BytesPool::with_config(BytesPoolConfig {
            small_size: 1024,
            small_max: 2,
            ..Default::default()
        });

        // First acquire is miss.
        let buf1 = pool.acquire_buffer(512).await;
        drop(buf1);

        // Second acquire reuses previous buffer and counts as hit.
        let _buf2 = pool.acquire_buffer(512).await;

        assert_eq!(pool.small_pool.tier_total_acquires.load(Ordering::Relaxed), 2);
        assert_eq!(pool.small_pool.tier_pool_hits.load(Ordering::Relaxed), 1);
    }
}
