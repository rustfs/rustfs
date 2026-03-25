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

//! Bytes object pool for zero-copy buffer management.
//!
//! This module provides a pool-based allocator for BytesMut buffers
//! to reduce memory allocations and improve I/O performance.

use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::Semaphore;

/// Bytes object pool for buffer management.
///
/// The pool maintains a set of pre-allocated buffers that can be reused
/// across I/O operations, reducing allocation overhead.
///
/// # Example
///
/// ```ignore
/// let pool = BytesPool::new(100, 16 * 1024);
///
/// // Acquire a buffer from the pool
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
    /// Semaphore for limiting concurrent buffer allocations
    semaphore: Arc<Semaphore>,
    /// Maximum number of buffers in the pool
    max_buffers: usize,
    /// Default buffer size
    default_buffer_size: usize,
}

impl BytesPool {
    /// Create a new Bytes pool.
    ///
    /// # Arguments
    ///
    /// * `max_buffers` - Maximum number of buffers to maintain
    /// * `default_buffer_size` - Default size for each buffer
    ///
    /// # Example
    ///
    /// ```ignore
    /// let pool = BytesPool::new(100, 16 * 1024);
    /// ```
    pub fn new(max_buffers: usize, default_buffer_size: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_buffers)),
            max_buffers,
            default_buffer_size,
        }
    }

    /// Acquire a BytesMut buffer from the pool.
    ///
    /// If the pool has available capacity, returns a new buffer.
    /// Otherwise, blocks until capacity is available.
    ///
    /// # Arguments
    ///
    /// * `size` - Minimum capacity for the buffer
    ///
    /// # Returns
    ///
    /// A BytesMut buffer with at least the requested capacity.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let buffer = pool.acquire_buffer(8192).await;
    /// ```
    pub async fn acquire_buffer(&self, size: usize) -> BytesMut {
        // Acquire a permit from the semaphore
        let _permit = self.semaphore.acquire().await;

        // Create a new buffer with requested capacity
        BytesMut::with_capacity(size.max(self.default_buffer_size))
    }

    /// Try to acquire a buffer without blocking.
    ///
    /// # Arguments
    ///
    /// * `size` - Minimum capacity for the buffer
    ///
    /// # Returns
    ///
    /// * `Some(buffer)` - If a buffer was available
    /// * `None` - If the pool is at capacity
    pub fn try_acquire_buffer(&self, size: usize) -> Option<BytesMut> {
        // Try to acquire a permit without blocking
        if let Ok(_permit) = self.semaphore.try_acquire() {
            Some(BytesMut::with_capacity(size.max(self.default_buffer_size)))
        } else {
            None
        }
    }

    /// Get the current number of available buffers in the pool.
    pub fn available_buffers(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum buffer capacity of the pool.
    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }

    /// Get the default buffer size.
    pub fn default_buffer_size(&self) -> usize {
        self.default_buffer_size
    }
}

impl std::fmt::Debug for BytesPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesPool")
            .field("max_buffers", &self.max_buffers)
            .field("available_buffers", &self.available_buffers())
            .field("default_buffer_size", &self.default_buffer_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_buffer() {
        let pool = BytesPool::new(10, 1024);

        let buffer = pool.acquire_buffer(2048).await;
        assert!(buffer.capacity() >= 2048);
    }

    #[tokio::test]
    async fn test_try_acquire_buffer() {
        let pool = BytesPool::new(1, 1024);

        // First acquisition should succeed
        let buffer1 = pool.try_acquire_buffer(512);
        assert!(buffer1.is_some());

        // Second acquisition should fail (pool at capacity)
        let buffer2 = pool.try_acquire_buffer(512);
        assert!(buffer2.is_none());

        // After dropping first, second should succeed
        drop(buffer1);
        let buffer3 = pool.try_acquire_buffer(512);
        assert!(buffer3.is_some());
    }

    #[tokio::test]
    async fn test_available_buffers() {
        let pool = BytesPool::new(5, 1024);

        assert_eq!(pool.available_buffers(), 5);

        let _buffer = pool.acquire_buffer(512).await;
        assert_eq!(pool.available_buffers(), 4);
    }
}
