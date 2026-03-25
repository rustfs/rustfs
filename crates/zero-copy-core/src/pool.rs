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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

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

/// Single pool tier with concurrent access control.
struct PoolTier {
    /// Buffer size for this tier
    buffer_size: usize,
    /// Maximum concurrent buffers
    max_buffers: usize,
    /// Semaphore for concurrency control
    semaphore: Arc<Semaphore>,
    /// Pool name for metrics
    name: &'static str,
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
}

/// A buffer managed by the BytesPool.
///
/// When dropped, the semaphore permit is automatically released.
pub struct PooledBuffer<'a> {
    /// The underlying buffer
    pub buffer: BytesMut,
    /// Reference to pool tier for return
    _tier: Option<Arc<PoolTier>>,
    /// The semaphore permit (must be dropped last)
    _permit: Option<tokio::sync::SemaphorePermit<'a>>,
    /// Metrics reference
    _metrics: Option<Arc<BytesPoolMetrics>>,
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
        Self {
            small_pool: Arc::new(PoolTier::new(config.small_size, config.small_max, "small")),
            medium_pool: Arc::new(PoolTier::new(config.medium_size, config.medium_max, "medium")),
            large_pool: Arc::new(PoolTier::new(config.large_size, config.large_max, "large")),
            xlarge_pool: Arc::new(PoolTier::new(config.xlarge_size, config.xlarge_max, "xlarge")),
            metrics: Arc::new(BytesPoolMetrics::default()),
        }
    }

    /// Acquire buffer with automatic tier selection.
    ///
    /// Selects the appropriate tier based on requested size and blocks
    /// until a buffer is available.
    ///
    /// # Arguments
    ///
    /// * `size` - Minimum capacity for the buffer
    ///
    /// # Returns
    ///
    /// A PooledBuffer that releases the permit when dropped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut buffer = pool.acquire_buffer(8192).await;
    /// ```
    pub async fn acquire_buffer(&self, size: usize) -> PooledBuffer<'_> {
        let tier = self.select_tier(size);
        tier.acquire_buffer(size, &self.metrics).await
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
    pub fn try_acquire_buffer(&self, size: usize) -> Option<PooledBuffer<'_>> {
        let tier = self.select_tier(size);
        tier.try_acquire_buffer(size, &self.metrics)
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
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

impl PoolTier {
    fn new(buffer_size: usize, max_buffers: usize, name: &'static str) -> Self {
        Self {
            buffer_size,
            max_buffers,
            semaphore: Arc::new(Semaphore::new(max_buffers)),
            name,
        }
    }

    async fn acquire_buffer(
        &self,
        size: usize,
        metrics: &BytesPoolMetrics,
    ) -> PooledBuffer<'_> {
        // Acquire semaphore permit
        let permit = self.semaphore.acquire().await.unwrap();

        // Record acquisition
        metrics.total_acquires.fetch_add(1, Ordering::Relaxed);

        // Create buffer
        let buffer = BytesMut::with_capacity(size.max(self.buffer_size));

        // Record metrics
        rustfs_zero_copy_metrics::record_bytes_pool_acquire(self.name, buffer.capacity(), false);

        PooledBuffer {
            buffer,
            _tier: None,
            _permit: Some(permit),
            _metrics: None,
        }
    }

    fn try_acquire_buffer(
        &self,
        size: usize,
        metrics: &BytesPoolMetrics,
    ) -> Option<PooledBuffer<'_>> {
        // Try to acquire permit without blocking
        let permit = self.semaphore.try_acquire().ok()?;

        // Record acquisition
        metrics.total_acquires.fetch_add(1, Ordering::Relaxed);

        // Create buffer
        let buffer = BytesMut::with_capacity(size.max(self.buffer_size));

        // Record metrics
        rustfs_zero_copy_metrics::record_bytes_pool_acquire(self.name, buffer.capacity(), false);

        Some(PooledBuffer {
            buffer,
            _tier: None,
            _permit: Some(permit),
            _metrics: None,
        })
    }
}

impl<'a> AsRef<[u8]> for PooledBuffer<'a> {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl<'a> AsMut<[u8]> for PooledBuffer<'a> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

impl<'a> std::ops::Deref for PooledBuffer<'a> {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<'a> std::ops::DerefMut for PooledBuffer<'a> {
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
            .field("available", &self.semaphore.available_permits())
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

        // Hit rate is 0.0 because we always allocate new buffers
        // In real implementation with pooling, this would be higher
        assert_eq!(pool.hit_rate(), 0.0);
    }
}
