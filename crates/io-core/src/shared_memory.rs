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

//! Shared memory pool for zero-copy data sharing.
//!
//! This module provides Arc-based shared memory management for
//! efficient cross-task data passing without serialization.

use std::convert::AsRef;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Shared memory pool configuration.
#[derive(Debug, Clone)]
pub struct SharedMemoryConfig {
    /// Whether shared memory is enabled
    pub enabled: bool,

    /// Maximum pool size in bytes
    pub max_pool_size: usize,

    /// Maximum object size in bytes
    pub max_object_size: usize,
}

impl Default for SharedMemoryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_pool_size: 100 * 1024 * 1024,  // 100MB
            max_object_size: 10 * 1024 * 1024, // 10MB
        }
    }
}

/// Shared memory pool statistics.
#[derive(Debug, Default)]
pub struct SharedMemoryStats {
    /// Total number of objects created
    pub total_objects: AtomicU64,

    /// Total number of shared references
    pub total_shared_refs: AtomicU64,

    /// Current memory usage in bytes
    pub current_memory: AtomicU64,

    /// Peak memory usage in bytes
    pub peak_memory: AtomicU64,
}

/// Arc data metadata.
#[derive(Clone, Debug)]
pub struct ArcMetadata {
    /// Size of the data (if measurable)
    pub size: Option<usize>,

    /// Creation timestamp
    pub created_at: Instant,
}

/// Arc-based data wrapper for zero-copy sharing.
///
/// This wrapper uses Arc to enable shared ownership of data
/// across multiple tasks without copying.
pub struct ArcData<T> {
    /// The wrapped data
    inner: Arc<T>,

    /// Metadata about the data
    metadata: ArcMetadata,
}

impl<T> Clone for ArcData<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            metadata: self.metadata.clone(),
        }
    }
}

impl<T> ArcData<T> {
    /// Create a new ArcData wrapper.
    pub fn new(data: T) -> Self {
        ArcData {
            inner: Arc::new(data),
            metadata: ArcMetadata {
                size: None,
                created_at: Instant::now(),
            },
        }
    }

    /// Create a new ArcData wrapper with known size.
    pub fn with_size(data: T, size: usize) -> Self {
        ArcData {
            inner: Arc::new(data),
            metadata: ArcMetadata {
                size: Some(size),
                created_at: Instant::now(),
            },
        }
    }

    /// Get the reference count.
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// Convert into the underlying Arc.
    pub fn into_arc(self) -> Arc<T> {
        self.inner
    }

    /// Get the metadata.
    pub fn metadata(&self) -> &ArcMetadata {
        &self.metadata
    }

    /// Get the size if known.
    pub fn size(&self) -> Option<usize> {
        self.metadata.size
    }
}

impl<T> AsRef<T> for ArcData<T> {
    fn as_ref(&self) -> &T {
        &self.inner
    }
}

impl<T> Deref for ArcData<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::fmt::Debug for ArcData<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArcData")
            .field("ref_count", &self.ref_count())
            .field("metadata", &self.metadata)
            .finish()
    }
}

/// Shared memory pool for managing Arc-based shared data.
pub struct SharedMemoryPool {
    config: SharedMemoryConfig,
    stats: SharedMemoryStats,
}

impl SharedMemoryPool {
    /// Create a new shared memory pool with the given configuration.
    pub fn new(config: SharedMemoryConfig) -> Self {
        Self {
            config,
            stats: SharedMemoryStats::default(),
        }
    }

    /// Create a new shared memory pool with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(SharedMemoryConfig::default())
    }

    /// Create shared data.
    ///
    /// This method wraps the data in an ArcData for zero-copy sharing.
    pub fn create<T>(&self, data: T) -> ArcData<T> {
        self.stats.total_objects.fetch_add(1, Ordering::Relaxed);
        ArcData::new(data)
    }

    /// Create shared data with known size.
    ///
    /// This method tracks memory usage for statistics.
    pub fn create_with_size<T>(&self, data: T, size: usize) -> ArcData<T> {
        self.stats.total_objects.fetch_add(1, Ordering::Relaxed);

        // Update memory statistics
        self.stats.current_memory.fetch_add(size as u64, Ordering::Relaxed);

        // Update peak memory
        let current = self.stats.current_memory.load(Ordering::Relaxed);
        let mut peak = self.stats.peak_memory.load(Ordering::Relaxed);
        if current > peak {
            peak = current;
            self.stats.peak_memory.store(peak, Ordering::Relaxed);
        }

        ArcData::with_size(data, size)
    }

    /// Share data by increasing reference count.
    ///
    /// This method creates a new ArcData that shares the underlying data
    /// without copying.
    pub fn share<T>(&self, data: &ArcData<T>) -> ArcData<T> {
        self.stats.total_shared_refs.fetch_add(1, Ordering::Relaxed);
        data.clone()
    }

    /// Get the statistics for this pool.
    pub fn stats(&self) -> &SharedMemoryStats {
        &self.stats
    }

    /// Get the configuration for this pool.
    pub fn config(&self) -> &SharedMemoryConfig {
        &self.config
    }

    /// Check if the pool is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arc_data_new() {
        let data = vec![1u8, 2, 3, 4, 5];
        let arc_data = ArcData::new(data.clone());

        assert_eq!(arc_data.as_ref(), &data);
        assert_eq!(arc_data.ref_count(), 1);
    }

    #[test]
    fn test_arc_data_clone() {
        let data = vec![1u8, 2, 3, 4, 5];
        let arc_data = ArcData::new(data);

        assert_eq!(arc_data.ref_count(), 1);

        let arc_data2 = arc_data.clone();
        assert_eq!(arc_data.ref_count(), 2);
        assert_eq!(arc_data2.ref_count(), 2);

        let arc_data3 = arc_data.clone();
        assert_eq!(arc_data.ref_count(), 3);
        assert_eq!(arc_data2.ref_count(), 3);
        assert_eq!(arc_data3.ref_count(), 3);
    }

    #[test]
    fn test_arc_data_deref() {
        let data = vec![1u8, 2, 3, 4, 5];
        let arc_data = ArcData::new(data);

        // Test Deref trait
        assert_eq!(arc_data.len(), 5);
        assert_eq!(arc_data[0], 1);
    }

    #[test]
    fn test_shared_memory_pool_create() {
        let pool = SharedMemoryPool::with_defaults();
        let data = vec![1u8, 2, 3, 4, 5];

        let arc_data = pool.create(data.clone());

        assert_eq!(arc_data.as_ref(), &data);
        assert_eq!(pool.stats().total_objects.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_shared_memory_pool_share() {
        let pool = SharedMemoryPool::with_defaults();
        let data = vec![1u8, 2, 3, 4, 5];

        let arc_data = pool.create(data);
        assert_eq!(arc_data.ref_count(), 1);

        let shared = pool.share(&arc_data);
        assert_eq!(arc_data.ref_count(), 2);
        assert_eq!(shared.ref_count(), 2);
        assert_eq!(pool.stats().total_shared_refs.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_shared_memory_pool_with_size() {
        let pool = SharedMemoryPool::with_defaults();
        let data = vec![1u8; 1024];

        let arc_data = pool.create_with_size(data, 1024);

        assert_eq!(arc_data.size(), Some(1024));
        assert_eq!(pool.stats().current_memory.load(Ordering::Relaxed), 1024);
    }

    #[test]
    fn test_default_config() {
        let config = SharedMemoryConfig::default();

        assert!(config.enabled);
        assert_eq!(config.max_pool_size, 100 * 1024 * 1024);
        assert_eq!(config.max_object_size, 10 * 1024 * 1024);
    }
}
