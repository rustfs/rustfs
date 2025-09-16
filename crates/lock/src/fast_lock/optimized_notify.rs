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

use once_cell::sync::Lazy;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Optimized notification pool to reduce memory overhead and thundering herd effects
/// Increased pool size for better performance under high concurrency
static NOTIFY_POOL: Lazy<Vec<Arc<Notify>>> = Lazy::new(|| (0..128).map(|_| Arc::new(Notify::new())).collect());

/// Optimized notification system for object locks
#[derive(Debug)]
pub struct OptimizedNotify {
    /// Number of readers waiting
    pub reader_waiters: AtomicU32,
    /// Number of writers waiting
    pub writer_waiters: AtomicU32,
    /// Index into the global notify pool
    pub notify_pool_index: AtomicUsize,
}

impl OptimizedNotify {
    pub fn new() -> Self {
        // Use random pool index to distribute load
        use std::time::{SystemTime, UNIX_EPOCH};
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let pool_index = (seed as usize) % NOTIFY_POOL.len();

        Self {
            reader_waiters: AtomicU32::new(0),
            writer_waiters: AtomicU32::new(0),
            notify_pool_index: AtomicUsize::new(pool_index),
        }
    }

    /// Notify waiting readers
    pub fn notify_readers(&self) {
        if self.reader_waiters.load(Ordering::Acquire) > 0 {
            let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
            NOTIFY_POOL[pool_index].notify_waiters();
        }
    }

    /// Notify one waiting writer
    pub fn notify_writer(&self) {
        if self.writer_waiters.load(Ordering::Acquire) > 0 {
            let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
            NOTIFY_POOL[pool_index].notify_one();
        }
    }

    /// Wait for reader notification
    pub async fn wait_for_read(&self) {
        self.reader_waiters.fetch_add(1, Ordering::AcqRel);
        let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
        NOTIFY_POOL[pool_index].notified().await;
        self.reader_waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Wait for writer notification
    pub async fn wait_for_write(&self) {
        self.writer_waiters.fetch_add(1, Ordering::AcqRel);
        let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
        NOTIFY_POOL[pool_index].notified().await;
        self.writer_waiters.fetch_sub(1, Ordering::AcqRel);
    }

    /// Check if anyone is waiting
    pub fn has_waiters(&self) -> bool {
        self.reader_waiters.load(Ordering::Acquire) > 0 || self.writer_waiters.load(Ordering::Acquire) > 0
    }
}

impl Default for OptimizedNotify {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_optimized_notify() {
        let notify = OptimizedNotify::new();

        // Test that notification works
        let notify_clone = Arc::new(notify);
        let notify_for_task = notify_clone.clone();

        let handle = tokio::spawn(async move {
            notify_for_task.wait_for_read().await;
        });

        // Give some time for the task to start waiting
        tokio::time::sleep(Duration::from_millis(10)).await;
        notify_clone.notify_readers();

        // Should complete quickly
        assert!(timeout(Duration::from_millis(100), handle).await.is_ok());
    }

    #[tokio::test]
    async fn test_writer_notification() {
        let notify = Arc::new(OptimizedNotify::new());
        let notify_for_task = notify.clone();

        let handle = tokio::spawn(async move {
            notify_for_task.wait_for_write().await;
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        notify.notify_writer();

        assert!(timeout(Duration::from_millis(100), handle).await.is_ok());
    }
}
