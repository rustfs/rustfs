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

//! Caching layer for Prometheus metrics.
//!
//! This module provides a simple, thread-safe cache for metrics responses
//! to avoid re-collecting expensive metrics on every Prometheus scrape.
//!
//! # Configuration
//!
//! The cache TTL can be configured via environment variable:
//! - `RUSTFS_PROMETHEUS_CACHE_TTL`: Cache TTL in seconds (default: 10)
//! - Set to `0` to disable caching entirely
//!
//! # Design
//!
//! - Uses a read-write lock for concurrent access
//! - Configurable TTL (time-to-live) per cache entry
//! - Automatic expiration on read
//! - Minimal overhead when cache is warm

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, Instant};

/// Environment variable name for configuring cache TTL.
pub const CACHE_TTL_ENV_VAR: &str = "RUSTFS_PROMETHEUS_CACHE_TTL";

/// Default cache TTL of 10 seconds.
///
/// This is shorter than typical Prometheus scrape intervals (15-60s)
/// to ensure reasonably fresh data while avoiding redundant collection.
pub const DEFAULT_CACHE_TTL_SECS: u64 = 10;

/// Configured cache TTL, read from environment variable at startup.
///
/// Set `RUSTFS_PROMETHEUS_CACHE_TTL` to customize (in seconds).
/// Set to `0` to disable caching.
static CONFIGURED_TTL: LazyLock<Duration> = LazyLock::new(|| {
    let secs = std::env::var(CACHE_TTL_ENV_VAR)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CACHE_TTL_SECS);
    Duration::from_secs(secs)
});

/// Get the configured cache TTL.
///
/// Returns the value from `RUSTFS_PROMETHEUS_CACHE_TTL` environment variable,
/// or the default (10 seconds) if not set.
#[must_use]
pub fn get_cache_ttl() -> Duration {
    *CONFIGURED_TTL
}

/// Check if caching is enabled (TTL > 0).
#[must_use]
pub fn is_caching_enabled() -> bool {
    !CONFIGURED_TTL.is_zero()
}

/// A cached metrics response with expiration time.
#[derive(Debug, Clone)]
struct CacheEntry {
    /// The rendered Prometheus metrics output.
    data: String,
    /// When this entry expires.
    expires_at: Instant,
}

impl CacheEntry {
    fn new(data: String, ttl: Duration) -> Self {
        Self {
            data,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// Thread-safe cache for a single metrics endpoint.
///
/// This cache stores the rendered Prometheus output string and serves
/// it until the TTL expires. When expired, the next request will
/// trigger a refresh.
///
/// # Example
///
/// ```
/// use rustfs_obs::prometheus::cache::MetricsCache;
/// use std::time::Duration;
///
/// let cache = MetricsCache::new(Duration::from_secs(10));
///
/// // First call - cache miss, returns None
/// assert!(cache.get().is_none());
///
/// // Set the cached value
/// cache.set("# HELP metric Description\nmetric 42\n".to_string());
///
/// // Second call - cache hit
/// assert!(cache.get().is_some());
/// ```
/// Guard that resets the computing flag on drop (including panic unwind).
///
/// This ensures that if the compute function panics, the `computing` flag
/// is always reset, preventing the cache from being permanently locked.
struct ComputeGuard<'a>(&'a AtomicBool);

impl Drop for ComputeGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Thread-safe cache with stampede protection.
///
/// The `computing` flag prevents multiple threads from simultaneously
/// computing the value when the cache expires (cache stampede).
#[derive(Debug)]
pub struct MetricsCache {
    entry: RwLock<Option<CacheEntry>>,
    ttl: Duration,
    /// Atomic flag to prevent cache stampede - only one thread computes at a time.
    computing: AtomicBool,
}

impl MetricsCache {
    /// Creates a new cache with the specified TTL.
    pub const fn new(ttl: Duration) -> Self {
        Self {
            entry: RwLock::new(None),
            ttl,
            computing: AtomicBool::new(false),
        }
    }

    /// Creates a new cache with the configured TTL from environment.
    ///
    /// Reads `RUSTFS_PROMETHEUS_CACHE_TTL` environment variable.
    /// Defaults to 10 seconds if not set.
    pub fn with_configured_ttl() -> Self {
        Self::new(get_cache_ttl())
    }

    /// Gets the cached value if it exists and hasn't expired.
    ///
    /// Returns `None` if the cache is empty or expired.
    pub fn get(&self) -> Option<String> {
        let guard = self.entry.read().ok()?;
        match guard.as_ref() {
            Some(entry) if !entry.is_expired() => Some(entry.data.clone()),
            _ => None,
        }
    }

    /// Sets the cached value with the configured TTL.
    pub fn set(&self, data: String) {
        if let Ok(mut guard) = self.entry.write() {
            *guard = Some(CacheEntry::new(data, self.ttl));
        }
    }

    /// Clears the cache, forcing a refresh on the next get.
    pub fn clear(&self) {
        if let Ok(mut guard) = self.entry.write() {
            *guard = None;
        }
    }

    /// Gets the cached value or computes it using the provided closure.
    ///
    /// This is the recommended way to use the cache - it handles the
    /// cache miss case by computing and caching the new value.
    ///
    /// # Stampede Protection
    ///
    /// When multiple threads simultaneously request an expired cache entry,
    /// only one thread will compute the new value while others wait.
    /// This prevents the "thundering herd" problem where many threads
    /// would all try to compute the same expensive value at once.
    ///
    /// # Example
    ///
    /// ```
    /// use rustfs_obs::prometheus::cache::MetricsCache;
    /// use std::time::Duration;
    ///
    /// let cache = MetricsCache::new(Duration::from_secs(10));
    ///
    /// let value = cache.get_or_compute(|| {
    ///     // Expensive metrics collection here
    ///     "metric_name 42\n".to_string()
    /// });
    /// ```
    pub fn get_or_compute<F>(&self, compute: F) -> String
    where
        F: FnOnce() -> String,
    {
        // Fast path: check if cached value exists
        if let Some(cached) = self.get() {
            return cached;
        }

        // Try to acquire compute lock using CAS
        if self
            .computing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We won the race - compute the value.
            // Use a guard to ensure the flag is reset even if compute() panics.
            let _guard = ComputeGuard(&self.computing);
            let data = compute();
            self.set(data.clone());
            // _guard dropped here, releasing the flag (also on panic unwind)
            return data;
        }

        // Another thread is computing - wait and retry
        for _ in 0..10 {
            std::thread::sleep(Duration::from_millis(10));
            if let Some(cached) = self.get() {
                return cached;
            }
        }

        // Timeout - compute anyway as fallback
        compute()
    }

    /// Async version of get_or_compute for async collection functions.
    ///
    /// # Stampede Protection
    ///
    /// When multiple tasks simultaneously request an expired cache entry,
    /// only one task will compute the new value while others wait.
    /// This prevents the "thundering herd" problem where many tasks
    /// would all try to compute the same expensive value at once.
    pub async fn get_or_compute_async<F, Fut>(&self, compute: F) -> String
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = String>,
    {
        // Fast path: check if cached value exists
        if let Some(cached) = self.get() {
            return cached;
        }

        // Try to acquire compute lock using CAS
        if self
            .computing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            // We won the race - compute the value.
            // Use a guard to ensure the flag is reset even if compute() panics.
            let _guard = ComputeGuard(&self.computing);
            let data = compute().await;
            self.set(data.clone());
            // _guard dropped here, releasing the flag (also on panic unwind)
            return data;
        }

        // Another task is computing - wait and retry
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            if let Some(cached) = self.get() {
                return cached;
            }
        }

        // Timeout - compute anyway as fallback
        compute().await
    }
}

impl Default for MetricsCache {
    fn default() -> Self {
        Self::with_configured_ttl()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::thread;

    // Use a fixed TTL for tests to avoid env var interference
    const TEST_TTL: Duration = Duration::from_secs(10);

    #[test]
    fn test_cache_miss_on_empty() {
        let cache = MetricsCache::new(TEST_TTL);
        assert!(cache.get().is_none());
    }

    #[test]
    fn test_cache_hit_after_set() {
        let cache = MetricsCache::new(TEST_TTL);
        cache.set("test_data".to_string());
        assert_eq!(cache.get(), Some("test_data".to_string()));
    }

    #[test]
    fn test_cache_expiration() {
        let cache = MetricsCache::new(Duration::from_millis(50));
        cache.set("test_data".to_string());
        assert!(cache.get().is_some());

        thread::sleep(Duration::from_millis(100));
        assert!(cache.get().is_none());
    }

    #[test]
    fn test_get_or_compute_caches_value() {
        let cache = MetricsCache::new(TEST_TTL);
        let mut call_count = 0;

        let result1 = cache.get_or_compute(|| {
            call_count += 1;
            "computed".to_string()
        });
        assert_eq!(result1, "computed");
        assert_eq!(call_count, 1);

        // Second call should use cached value
        let result2 = cache.get_or_compute(|| {
            call_count += 1;
            "computed_again".to_string()
        });
        assert_eq!(result2, "computed");
        assert_eq!(call_count, 1); // Still 1, not called again
    }

    #[test]
    fn test_clear_forces_recompute() {
        let cache = MetricsCache::new(TEST_TTL);
        cache.set("old_value".to_string());
        cache.clear();
        assert!(cache.get().is_none());
    }

    #[test]
    fn test_zero_ttl_disables_caching() {
        let cache = MetricsCache::new(Duration::ZERO);
        cache.set("test_data".to_string());
        // With zero TTL, cache should immediately expire
        assert!(cache.get().is_none());
    }

    #[test]
    fn test_default_ttl_value() {
        assert_eq!(DEFAULT_CACHE_TTL_SECS, 10);
    }

    #[test]
    fn test_stampede_protection() {
        // Test that when multiple threads try to compute simultaneously,
        // only one actually runs the compute function (stampede protection).
        let cache = Arc::new(MetricsCache::new(TEST_TTL));
        let compute_count = Arc::new(AtomicUsize::new(0));
        let num_threads = 10;

        let mut handles = Vec::with_capacity(num_threads);

        for _ in 0..num_threads {
            let cache = Arc::clone(&cache);
            let compute_count = Arc::clone(&compute_count);

            handles.push(thread::spawn(move || {
                cache.get_or_compute(|| {
                    // Increment counter to track how many times compute is called
                    compute_count.fetch_add(1, Ordering::SeqCst);
                    // Simulate some compute work
                    thread::sleep(Duration::from_millis(50));
                    "computed_value".to_string()
                })
            }));
        }

        // Wait for all threads to complete
        let results: Vec<String> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All results should be the same computed value
        for result in &results {
            assert_eq!(result, "computed_value");
        }

        // The compute function should have been called at most 2 times:
        // - 1 winner thread that computes and caches
        // - Possibly 1 timeout thread if waiting took too long
        // In practice, with stampede protection, we expect exactly 1 call
        // but allow for some edge cases with timing
        let actual_count = compute_count.load(Ordering::SeqCst);
        assert!(
            actual_count <= 2,
            "Expected at most 2 compute calls, but got {actual_count}. \
             Stampede protection may not be working correctly."
        );
    }

    #[tokio::test]
    async fn test_stampede_protection_async() {
        use tokio::task::JoinSet;

        // Test that when multiple tasks try to compute simultaneously,
        // only one actually runs the compute function (stampede protection).
        let cache = Arc::new(MetricsCache::new(TEST_TTL));
        let compute_count = Arc::new(AtomicUsize::new(0));
        let num_tasks = 10;

        let mut join_set = JoinSet::new();

        for _ in 0..num_tasks {
            let cache = Arc::clone(&cache);
            let compute_count = Arc::clone(&compute_count);

            join_set.spawn(async move {
                cache
                    .get_or_compute_async(|| {
                        let compute_count = Arc::clone(&compute_count);
                        async move {
                            // Increment counter to track how many times compute is called
                            compute_count.fetch_add(1, Ordering::SeqCst);
                            // Simulate some compute work
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            "computed_value".to_string()
                        }
                    })
                    .await
            });
        }

        // Wait for all tasks to complete and collect results
        let mut results = Vec::with_capacity(num_tasks);
        while let Some(result) = join_set.join_next().await {
            results.push(result.unwrap());
        }

        // All results should be the same computed value
        for result in &results {
            assert_eq!(result, "computed_value");
        }

        // The compute function should have been called at most 2 times
        let actual_count = compute_count.load(Ordering::SeqCst);
        assert!(
            actual_count <= 2,
            "Expected at most 2 compute calls, but got {actual_count}. \
             Stampede protection may not be working correctly."
        );
    }

    #[test]
    fn test_panic_safety() {
        use std::panic;

        let cache = MetricsCache::new(TEST_TTL);

        // First call panics
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            cache.get_or_compute(|| {
                panic!("intentional panic");
            })
        }));
        assert!(result.is_err());

        // Second call should still work (computing flag was reset)
        let value = cache.get_or_compute(|| "recovered".to_string());
        assert_eq!(value, "recovered");
    }

    #[tokio::test]
    async fn test_panic_safety_async() {
        use tokio::task;

        let cache = Arc::new(MetricsCache::new(TEST_TTL));

        // First call panics - spawn a task that will panic
        let cache_clone = Arc::clone(&cache);
        let handle = task::spawn(async move {
            cache_clone
                .get_or_compute_async(|| async { panic!("intentional panic") })
                .await
        });

        // The spawned task should panic
        let result = handle.await;
        assert!(result.is_err());

        // Second call should still work (computing flag was reset)
        let value = cache.get_or_compute_async(|| async { "recovered".to_string() }).await;
        assert_eq!(value, "recovered");
    }
}
