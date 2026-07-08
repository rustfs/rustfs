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

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use tokio::sync::Notify;

/// Optimized notification pool to reduce memory overhead and thundering herd effects
/// Increased pool size for better performance under high concurrency
static NOTIFY_POOL: LazyLock<Vec<Arc<Notify>>> = LazyLock::new(|| (0..128).map(|_| Arc::new(Notify::new())).collect());

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

/// Cancellation-safe waiter-count ticket for [`OptimizedNotify`].
///
/// Increments the referenced counter on construction and decrements it on
/// drop. The decrement therefore runs even when the awaiting future is
/// cancelled/dropped before the `notified()` await completes, so the counter
/// cannot leak on capped-wait timeouts.
struct WaiterCountGuard<'a> {
    counter: &'a AtomicU32,
}

impl<'a> WaiterCountGuard<'a> {
    fn new(counter: &'a AtomicU32) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self { counter }
    }
}

impl Drop for WaiterCountGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
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
        // RAII guard decrements the counter even if this future is dropped at
        // the await below (e.g. when the caller wraps it in a `timeout(...)`
        // that elapses), preventing the waiter counter from leaking upward.
        let _guard = WaiterCountGuard::new(&self.reader_waiters);
        let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
        NOTIFY_POOL[pool_index].notified().await;
    }

    /// Wait for writer notification
    pub async fn wait_for_write(&self) {
        // See `wait_for_read` for why the decrement must be RAII-guarded.
        let _guard = WaiterCountGuard::new(&self.writer_waiters);
        let pool_index = self.notify_pool_index.load(Ordering::Relaxed) % NOTIFY_POOL.len();
        NOTIFY_POOL[pool_index].notified().await;
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

    #[tokio::test]
    async fn test_reader_waiter_count_released_on_abort() {
        let notify = Arc::new(OptimizedNotify::new());
        let notify_for_task = notify.clone();

        let handle = tokio::spawn(async move {
            notify_for_task.wait_for_read().await;
        });

        // Wait until the task has registered itself as a waiting reader.
        timeout(Duration::from_secs(1), async {
            while notify.reader_waiters.load(Ordering::Acquire) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("reader waiter never registered");
        assert_eq!(notify.reader_waiters.load(Ordering::Acquire), 1);

        handle.abort();
        let _ = handle.await;

        assert_eq!(
            notify.reader_waiters.load(Ordering::Acquire),
            0,
            "reader waiter count must return to 0 after the waiting future is dropped"
        );
    }

    #[tokio::test]
    async fn test_writer_waiter_count_released_after_capped_timeouts() {
        // Reproduces the shard slow-path pattern: the notification is never
        // delivered, so each capped `timeout` elapses and drops the waiting
        // future. Without an RAII decrement the counter would climb by one per
        // iteration; with it, the counter must return to 0 every time.
        let notify = OptimizedNotify::new();

        for _ in 0..5 {
            let _ = timeout(Duration::from_millis(10), notify.wait_for_write()).await;
            assert_eq!(
                notify.writer_waiters.load(Ordering::Acquire),
                0,
                "writer waiter count must return to 0 after a capped-wait timeout"
            );
        }
    }
}
