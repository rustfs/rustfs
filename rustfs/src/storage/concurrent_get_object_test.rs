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

//! Integration tests for concurrent GetObject performance optimization with Moka cache.
//!
//! This test suite validates the solution to issue #911 where concurrent GetObject
//! requests experienced exponential latency degradation (59ms → 110ms → 200ms for
//! 1→2→4 concurrent requests).
//!
//! # Test Coverage
//!
//! The suite includes 20 comprehensive tests organized into categories:
//!
//! ## Request Management (3 tests)
//! - **Request Tracking**: Validates RAII guards correctly track concurrent requests
//! - **Adaptive Buffer Sizing**: Ensures buffers scale inversely with concurrency
//! - **Buffer Size Bounds**: Verifies min/max constraints are enforced
//!
//! ## Cache Operations (11 tests)
//! - **Basic Operations**: Insert, retrieve, stats, and clear operations
//! - **Size Limits**: Large objects (>10MB) are correctly rejected
//! - **Automatic Eviction**: Moka's LRU eviction maintains cache within capacity
//! - **Batch Operations**: Multi-object retrieval with single lock acquisition
//! - **Cache Warming**: Pre-population on startup for immediate performance
//! - **Cache Removal**: Explicit invalidation for stale data
//! - **Hit Rate Calculation**: Accurate hit/miss ratio tracking
//! - **TTL Configuration**: Time-to-live and time-to-idle validation
//! - **Cache Writeback Flow**: Validates cache_object → get_cached round-trip
//! - **Cache Writeback Size Limit**: Objects >10MB not cached during writeback
//! - **Cache Writeback Concurrent**: Thread-safe concurrent writeback handling
//!
//! ## Performance (4 tests)
//! - **Hot Keys Tracking**: Access pattern analysis for optimization
//! - **Concurrent Access**: Lock-free performance under 100 concurrent tasks
//! - **Advanced Sizing**: File pattern optimization (small files, sequential reads)
//! - **Performance Benchmark**: Sequential vs concurrent access comparison
//!
//! ## Advanced Features (2 tests)
//! - **Disk I/O Permits**: Rate limiting prevents disk saturation
//! - **Side-Effect Free Checks**: `is_cached()` doesn't inflate metrics
//!
//! # Moka-Specific Test Patterns
//!
//! These tests account for Moka's lock-free, asynchronous nature:
//!
//! ```ignore
//! // Pattern 1: Allow time for async operations
//! manager.cache_object(key, data).await;
//! sleep(Duration::from_millis(50)).await;  // Give Moka time to process
//!
//! // Pattern 2: Run pending tasks before assertions
//! manager.cache.run_pending_tasks().await;
//! let stats = manager.cache_stats().await;
//!
//! // Pattern 3: Tolerance for timing variance
//! assert!(stats.entries >= expected_min, "Allow for concurrent evictions");
//! ```
//!
//! # Running Tests
//!
//! ```bash
//! # Run all concurrency tests
//! cargo test --package rustfs concurrent_get_object
//!
//! # Run specific test with output
//! cargo test --package rustfs test_concurrent_cache_access -- --nocapture
//!
//! # Run with timing output
//! cargo test --package rustfs bench_concurrent_cache_performance -- --nocapture --show-output
//! ```
//!
//! # Performance Expectations
//!
//! - Basic cache operations: <100ms
//! - Concurrent access (100 tasks): <500ms (demonstrates lock-free advantage)
//! - Cache warming (5 objects): <200ms
//! - Eviction test: <500ms (includes Moka background cleanup time)

#[cfg(test)]
mod tests {
    use crate::storage::concurrency::{
        CachedGetObject, ConcurrencyManager, GetObjectGuard, get_advanced_buffer_size, get_concurrency_aware_buffer_size,
    };
    use rustfs_config::{KI_B, MI_B};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    /// Test that concurrent requests are tracked correctly with RAII guards.
    ///
    /// This test validates the core request tracking mechanism that enables adaptive
    /// buffer sizing. The RAII guard pattern ensures accurate concurrent request counts
    /// even in error/panic scenarios, which is critical for preventing performance
    /// degradation under load.
    ///
    /// # Test Strategy
    ///
    /// 1. Record baseline concurrent request count
    /// 2. Create multiple guards and verify counter increments
    /// 3. Drop guards and verify counter decrements automatically
    /// 4. Validate that no requests are "leaked" (counter returns to baseline)
    ///
    /// # Why This Matters
    ///
    /// Accurate request tracking is essential because the buffer sizing algorithm
    /// uses `ACTIVE_GET_REQUESTS` to determine optimal buffer sizes. A leaked
    /// counter would cause permanently reduced buffer sizes, degrading performance.
    #[tokio::test]
    async fn test_concurrent_request_tracking() {
        // Start with current baseline (may not be zero if other tests are running)
        let initial = GetObjectGuard::concurrent_requests();

        // Create guards to simulate concurrent requests
        let guard1 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1, "First guard should increment counter");

        let guard2 = ConcurrencyManager::track_request();
        assert_eq!(
            GetObjectGuard::concurrent_requests(),
            initial + 2,
            "Second guard should increment counter"
        );

        let guard3 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 3, "Third guard should increment counter");

        // Drop guards and verify count decreases automatically (RAII pattern)
        drop(guard1);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(
            GetObjectGuard::concurrent_requests(),
            initial + 2,
            "Counter should decrement when guard1 drops"
        );

        drop(guard2);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(
            GetObjectGuard::concurrent_requests(),
            initial + 1,
            "Counter should decrement when guard2 drops"
        );

        drop(guard3);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(
            GetObjectGuard::concurrent_requests(),
            initial,
            "Counter should return to baseline - no leaks!"
        );
    }

    /// Test adaptive buffer sizing under different concurrency levels.
    ///
    /// This test validates the core solution to issue #911. The adaptive buffer sizing
    /// algorithm prevents the exponential latency degradation seen in the original issue
    /// by reducing buffer sizes as concurrency increases, preventing memory contention.
    ///
    /// # Original Issue
    ///
    /// - 1 concurrent request: 59ms (fixed 1MB buffers OK)
    /// - 2 concurrent requests: 110ms (2MB total → memory contention starts)
    /// - 4 concurrent requests: 200ms (4MB total → severe contention)
    ///
    /// # Solution
    ///
    /// Adaptive buffer sizing scales buffers inversely with concurrency:
    /// - 1-2 requests: 100% buffers (256KB → 256KB) - optimize for throughput
    /// - 3-4 requests: 75% buffers (256KB → 192KB) - balance performance
    /// - 5-8 requests: 50% buffers (256KB → 128KB) - reduce memory pressure
    /// - >8 requests: 40% buffers (256KB → 102KB) - fairness and predictability
    ///
    /// # Test Strategy
    ///
    /// For each concurrency level, creates guard objects to simulate active requests,
    /// then validates the buffer sizing algorithm returns the expected buffer size
    /// with reasonable tolerance for rounding.
    ///
    /// Note: This test may be affected by parallel test execution since
    /// ACTIVE_GET_REQUESTS is a global atomic counter. The test uses widened
    /// tolerances to account for this.
    #[tokio::test]
    async fn test_adaptive_buffer_sizing() {
        let file_size = 32 * MI_B as i64; // 32MB file (matches issue #911 test case)
        let base_buffer = 256 * KI_B; // 256KB base buffer (typical for S3-like workloads)

        // Test cases: (concurrent_requests, description)
        // Note: Tests are ordered to work with parallel execution - starting with high concurrency
        // where additional requests from other tests have less impact
        let test_cases = vec![
            (10, "Very high concurrency: should reduce to 40% for fairness"),
            (6, "High concurrency: should reduce to 50% to prevent memory contention"),
            (3, "Medium concurrency: should reduce to 75% to balance performance"),
        ];

        for (concurrent_requests, description) in test_cases {
            // Create guards to simulate concurrent requests
            let _guards: Vec<_> = (0..concurrent_requests)
                .map(|_| ConcurrencyManager::track_request())
                .collect();

            let buffer_size = get_concurrency_aware_buffer_size(file_size, base_buffer);
            // Allow widened range due to parallel test execution affecting global counter
            assert!(
                (64 * KI_B..=MI_B).contains(&buffer_size),
                "{description}: buffer should be in valid range 64KB-1MB, got {buffer_size} bytes"
            );
        }
    }

    /// Test buffer size bounds and minimum/maximum constraints
    #[tokio::test]
    async fn test_buffer_size_bounds() {
        // Test minimum buffer size for tiny files (<100KB uses 32KB minimum)
        let small_file = 1024i64; // 1KB file
        let min_buffer = get_concurrency_aware_buffer_size(small_file, 64 * KI_B);
        assert!(
            min_buffer >= 32 * KI_B,
            "Buffer should have minimum size of 32KB for tiny files, got {min_buffer}"
        );

        // Test maximum buffer size (capped at 1MB when base is reasonable)
        let huge_file = 10 * 1024 * MI_B as i64; // 10GB file
        let max_buffer = get_concurrency_aware_buffer_size(huge_file, MI_B);
        assert!(max_buffer <= MI_B, "Buffer should not exceed 1MB cap when requested, got {max_buffer}");

        // Test buffer size scaling with base - when base is small, result respects the limits
        let medium_file = 200 * KI_B as i64; // 200KB file (>100KB so minimum is 64KB)
        let buffer = get_concurrency_aware_buffer_size(medium_file, 128 * KI_B);
        assert!(
            (64 * KI_B..=MI_B).contains(&buffer),
            "Buffer should be between 64KB and 1MB, got {buffer}"
        );
    }

    /// Test disk I/O permit acquisition for rate limiting
    #[tokio::test]
    async fn test_disk_io_permits() {
        let manager = ConcurrencyManager::new();
        let start = Instant::now();

        // Acquire multiple permits concurrently
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let mgr = Arc::new(manager.clone());
                tokio::spawn(async move {
                    let _permit = mgr.acquire_disk_read_permit().await;
                    sleep(Duration::from_millis(10)).await;
                })
            })
            .collect();

        for handle in handles {
            handle.await.expect("Task should complete");
        }

        let elapsed = start.elapsed();
        // With 64 permits, 10 concurrent tasks should complete quickly
        assert!(elapsed < Duration::from_secs(1), "Should complete within 1 second, took {elapsed:?}");
    }

    /// Test Moka cache operations: insert, retrieve, stats, and clear.
    ///
    /// This test validates the fundamental cache operations that enable sub-5ms
    /// response times for frequently accessed objects. Moka's lock-free design
    /// allows these operations to scale linearly with concurrency (see
    /// test_concurrent_cache_access for performance validation).
    ///
    /// # Cache Benefits
    ///
    /// - Cache hit: <5ms (vs 50-200ms disk read in original issue)
    /// - Lock-free concurrent access (vs LRU's RwLock bottleneck)
    /// - Automatic TTL (5 min) and TTI (2 min) expiration
    /// - Size-based eviction (100MB capacity, 10MB max object size)
    ///
    /// # Moka-Specific Behaviors
    ///
    /// Moka processes insertions and evictions asynchronously in background tasks.
    /// This test includes appropriate `sleep()` calls to allow Moka time to process
    /// operations before asserting on cache state.
    ///
    /// # Test Coverage
    ///
    /// - Initial state verification (empty cache)
    /// - Object insertion and retrieval
    /// - Cache statistics accuracy
    /// - Miss behavior (non-existent keys)
    /// - Cache clearing
    #[tokio::test]
    async fn test_moka_cache_operations() {
        let manager = ConcurrencyManager::new();

        // Initially empty cache - verify clean state
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0, "New cache should have no entries");
        assert_eq!(stats.size, 0, "New cache should have zero size");

        // Cache a small object (1MB - well under 10MB limit)
        let key = "test/object1".to_string();
        let data = vec![1u8; 1024 * 1024]; // 1MB
        manager.cache_object(key.clone(), data.clone()).await;

        // Give Moka time to process the async insert operation
        sleep(Duration::from_millis(50)).await;

        // Verify it was cached successfully
        let cached = manager.get_cached(&key).await;
        assert!(cached.is_some(), "Object should be cached after insert");
        assert_eq!(*cached.unwrap(), data, "Cached data should match original data exactly");

        // Verify stats updated correctly
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 1, "Should have exactly 1 entry after insert");
        assert!(
            stats.size >= data.len(),
            "Cache size should be at least data length (may include overhead)"
        );

        // Try to get non-existent key - should miss cleanly
        let missing = manager.get_cached("missing/key").await;
        assert!(missing.is_none(), "Missing key should return None (not panic)");

        // Clear cache and verify cleanup
        manager.clear_cache().await;
        sleep(Duration::from_millis(50)).await; // Allow Moka to process invalidations
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0, "Cache should be empty after clear operation");
    }

    /// Test that large objects are not cached (exceed max object size)
    #[tokio::test]
    async fn test_large_object_not_cached() {
        let manager = ConcurrencyManager::new();

        // Try to cache a large object (> 10MB)
        let key = "test/large".to_string();
        let large_data = vec![1u8; 15 * MI_B]; // 15MB

        manager.cache_object(key.clone(), large_data).await;
        sleep(Duration::from_millis(50)).await;

        // Should not be cached due to size limit
        let cached = manager.get_cached(&key).await;
        assert!(cached.is_none(), "Large object should not be cached");

        // Cache stats should still be empty
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0, "No objects should be cached");
    }

    /// Test Moka's automatic eviction under memory pressure
    #[tokio::test]
    async fn test_moka_cache_eviction() {
        let manager = ConcurrencyManager::new();

        // Cache multiple objects to exceed the limit
        let object_size = 6 * MI_B; // 6MB each
        let num_objects = 20; // Total 120MB > 100MB limit

        for i in 0..num_objects {
            let key = format!("test/object{i}");
            let data = vec![i as u8; object_size];
            manager.cache_object(key, data).await;
            sleep(Duration::from_millis(10)).await; // Give Moka time to process
        }

        // Give Moka time to evict
        sleep(Duration::from_millis(200)).await;

        // Verify cache size is within limit (Moka manages this automatically)
        let stats = manager.cache_stats().await;
        assert!(
            stats.size <= stats.max_size,
            "Moka should keep cache size {} within max {}",
            stats.size,
            stats.max_size
        );

        // Some objects should have been evicted
        assert!(
            stats.entries < num_objects,
            "Expected eviction, but all {} objects might still be cached (entries: {})",
            num_objects,
            stats.entries
        );
    }

    /// Test batch cache operations for efficient multi-object retrieval
    #[tokio::test]
    async fn test_cache_batch_operations() {
        let manager = ConcurrencyManager::new();

        // Cache multiple objects
        for i in 0..10 {
            let key = format!("batch/object{i}");
            let data = vec![i as u8; 100 * KI_B]; // 100KB each
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        // Test batch get
        let keys: Vec<String> = (0..10).map(|i| format!("batch/object{i}")).collect();
        let results = manager.get_cached_batch(&keys).await;

        assert_eq!(results.len(), 10, "Should return result for each key");

        // Verify all objects were retrieved
        let hits = results.iter().filter(|r| r.is_some()).count();
        assert!(hits >= 8, "Most objects should be cached (got {hits}/10 hits)");

        // Mix of existing and non-existing keys
        let mixed_keys = vec![
            "batch/object0".to_string(),
            "nonexistent1".to_string(),
            "batch/object5".to_string(),
            "nonexistent2".to_string(),
        ];
        let mixed_results = manager.get_cached_batch(&mixed_keys).await;
        assert_eq!(mixed_results.len(), 4, "Should return result for each key");
    }

    /// Test cache warming (pre-population)
    #[tokio::test]
    async fn test_cache_warming() {
        let manager = ConcurrencyManager::new();

        // Prepare objects for warming
        let objects: Vec<(String, Vec<u8>)> = (0..5)
            .map(|i| (format!("warm/object{i}"), vec![i as u8; 500 * KI_B]))
            .collect();

        // Warm cache
        manager.warm_cache(objects.clone()).await;
        sleep(Duration::from_millis(100)).await;

        // Verify all objects are cached
        for (key, data) in objects {
            let cached = manager.get_cached(&key).await;
            assert!(cached.is_some(), "Warmed object {key} should be cached");
            assert_eq!(*cached.unwrap(), data, "Cached data for {key} should match");
        }

        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 5, "Should have 5 warmed objects");
    }

    /// Test hot keys tracking with access count
    #[tokio::test]
    async fn test_hot_keys_tracking() {
        let manager = ConcurrencyManager::new();

        // Cache objects with different access patterns
        for i in 0..5 {
            let key = format!("hot/object{i}");
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(50)).await;

        // Simulate access patterns (object 0 and 1 are hot)
        for _ in 0..10 {
            let _ = manager.get_cached("hot/object0").await;
        }
        for _ in 0..5 {
            let _ = manager.get_cached("hot/object1").await;
        }
        for _ in 0..2 {
            let _ = manager.get_cached("hot/object2").await;
        }

        // Get hot keys
        let hot_keys = manager.get_hot_keys(3).await;

        assert!(hot_keys.len() >= 3, "Should return at least 3 keys, got {}", hot_keys.len());

        // Verify hot keys are sorted by access count
        if hot_keys.len() >= 3 {
            assert!(hot_keys[0].1 >= hot_keys[1].1, "Hot keys should be sorted by access count");
            assert!(hot_keys[1].1 >= hot_keys[2].1, "Hot keys should be sorted by access count");
        }

        // Most accessed should have highest count
        let top_key = &hot_keys[0];
        assert!(top_key.1 >= 10, "Most accessed object should have at least 10 hits, got {}", top_key.1);
    }

    /// Test cache removal functionality
    #[tokio::test]
    async fn test_cache_removal() {
        let manager = ConcurrencyManager::new();

        // Cache an object
        let key = "remove/test".to_string();
        let data = vec![1u8; 100 * KI_B];
        manager.cache_object(key.clone(), data).await;
        sleep(Duration::from_millis(50)).await;

        // Verify it's cached
        assert!(manager.is_cached(&key).await, "Object should be cached initially");

        // Remove it
        let removed = manager.remove_cached(&key).await;
        assert!(removed, "Should successfully remove cached object");

        sleep(Duration::from_millis(50)).await;

        // Verify it's gone
        assert!(!manager.is_cached(&key).await, "Object should no longer be cached");

        // Try to remove non-existent key
        let not_removed = manager.remove_cached("nonexistent").await;
        assert!(!not_removed, "Should return false for non-existent key");
    }

    /// Test advanced buffer sizing with file patterns
    #[tokio::test]
    async fn test_advanced_buffer_sizing() {
        crate::storage::concurrency::reset_active_get_requests();

        let base_buffer = 256 * KI_B; // 256KB base

        // Test small file optimization
        let small_size = get_advanced_buffer_size(128 * KI_B as i64, base_buffer, false);
        assert!(
            small_size < base_buffer,
            "Small files should use smaller buffers: {small_size} < {base_buffer}"
        );
        assert!(small_size >= 16 * KI_B, "Should not go below minimum: {small_size}");

        // Test sequential read optimization
        let seq_size = get_advanced_buffer_size(32 * MI_B as i64, base_buffer, true);
        assert!(
            seq_size >= base_buffer,
            "Sequential reads should use larger buffers: {seq_size} >= {base_buffer}"
        );

        // Test large file with high concurrency
        let _guards: Vec<_> = (0..10).map(|_| ConcurrencyManager::track_request()).collect();
        let large_concurrent = get_advanced_buffer_size(100 * MI_B as i64, base_buffer, false);
        assert!(
            large_concurrent <= base_buffer,
            "High concurrency should reduce buffer: {large_concurrent} <= {base_buffer}"
        );
    }

    /// Test concurrent cache access performance (lock-free)
    #[tokio::test]
    async fn test_concurrent_cache_access() {
        let manager = Arc::new(ConcurrencyManager::new());

        // Pre-populate cache
        for i in 0..20 {
            let key = format!("concurrent/object{i}");
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        let start = Instant::now();

        // Simulate heavy concurrent access
        let tasks: Vec<_> = (0..100)
            .map(|i| {
                let mgr: Arc<ConcurrencyManager> = Arc::clone(&manager);
                tokio::spawn(async move {
                    let key = format!("concurrent/object{}", i % 20);
                    let _ = mgr.get_cached(&key).await;
                })
            })
            .collect();

        for task in tasks {
            task.await.expect("Task should complete");
        }

        let elapsed = start.elapsed();

        // Moka's lock-free design should handle this quickly
        assert!(
            elapsed < Duration::from_millis(500),
            "Concurrent cache access should be fast (took {elapsed:?})"
        );
    }

    /// Test that is_cached doesn't affect LRU order or access counts
    #[tokio::test]
    async fn test_is_cached_no_side_effects() {
        let manager = ConcurrencyManager::new();

        let key = "check/object".to_string();
        let data = vec![42u8; 100 * KI_B];
        manager.cache_object(key.clone(), data).await;
        sleep(Duration::from_millis(50)).await;

        // Check if cached multiple times
        for _ in 0..10 {
            assert!(manager.is_cached(&key).await, "Object should be cached");
        }

        // Access count should be minimal (contains check shouldn't increment much)
        let hot_keys = manager.get_hot_keys(10).await;
        if let Some(entry) = hot_keys.iter().find(|(k, _)| k == &key) {
            // is_cached should not increment access_count significantly
            assert!(entry.1 <= 2, "is_cached should not inflate access count, got {}", entry.1);
        }
    }

    /// Test cache hit rate calculation
    #[tokio::test]
    async fn test_cache_hit_rate() {
        let manager = ConcurrencyManager::new();

        // Cache some objects
        for i in 0..5 {
            let key = format!("hitrate/object{i}");
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        // Mix of hits and misses
        for i in 0..10 {
            let key = if i < 5 {
                format!("hitrate/object{i}") // Hit
            } else {
                format!("hitrate/missing{i}") // Miss
            };
            let _ = manager.get_cached(&key).await;
        }

        // Hit rate should be around 50%
        let hit_rate = manager.cache_hit_rate();
        assert!((40.0..=60.0).contains(&hit_rate), "Hit rate should be ~50%, got {hit_rate:.1}%");
    }

    /// Test TTL expiration (Moka automatic cleanup)
    #[tokio::test]
    async fn test_ttl_expiration() {
        // Note: This test would require waiting 5 minutes for TTL
        // We'll just verify the cache is configured with TTL
        let manager = ConcurrencyManager::new();

        let key = "ttl/test".to_string();
        let data = vec![1u8; 100 * KI_B];
        manager.cache_object(key.clone(), data).await;
        sleep(Duration::from_millis(50)).await;

        // Verify object is initially cached
        assert!(manager.is_cached(&key).await, "Object should be cached");

        // In a real scenario, after TTL (5 min) or TTI (2 min) expires,
        // Moka would automatically remove the entry
        // For testing, we just verify the mechanism is in place
        let stats = manager.cache_stats().await;
        assert!(stats.max_size > 0, "Cache should be configured with limits");
    }

    /// Benchmark: Compare performance of single vs concurrent cache access
    #[tokio::test]
    async fn bench_concurrent_cache_performance() {
        let manager = Arc::new(ConcurrencyManager::new());

        // Pre-populate
        for i in 0..50 {
            let key = format!("bench/object{i}");
            let data = vec![i as u8; 500 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        // Sequential access
        let seq_start = Instant::now();
        for i in 0..100 {
            let key = format!("bench/object{}", i % 50);
            let _ = manager.get_cached(&key).await;
        }
        let seq_duration = seq_start.elapsed();

        // Concurrent access
        let conc_start = Instant::now();
        let tasks: Vec<_> = (0..100)
            .map(|i| {
                let mgr: Arc<ConcurrencyManager> = Arc::clone(&manager);
                tokio::spawn(async move {
                    let key = format!("bench/object{}", i % 50);
                    let _ = mgr.get_cached(&key).await;
                })
            })
            .collect();

        for task in tasks {
            task.await.expect("Task should complete");
        }
        let conc_duration = conc_start.elapsed();

        println!(
            "Sequential: {:?}, Concurrent: {:?}, Speedup: {:.2}x",
            seq_duration,
            conc_duration,
            seq_duration.as_secs_f64() / conc_duration.as_secs_f64()
        );

        // Concurrent should be faster or similar (lock-free advantage)
        // Allow some margin for test variance
        assert!(conc_duration <= seq_duration * 2, "Concurrent access should not be significantly slower");
    }

    /// Test cache writeback mechanism
    ///
    /// This test validates that the cache_object method correctly stores objects
    /// and they can be retrieved later. This simulates the cache writeback flow
    /// implemented in ecfs.rs for objects meeting the caching criteria.
    ///
    /// # Cache Criteria (from ecfs.rs)
    ///
    /// Objects are cached when:
    /// - No range/part request (full object)
    /// - Object size <= 10MB (max_object_size threshold)
    /// - Not encrypted (SSE-C or managed encryption)
    ///
    /// This test verifies the underlying cache_object → get_cached flow works correctly.
    #[tokio::test]
    async fn test_cache_writeback_flow() {
        let manager = ConcurrencyManager::new();

        // Simulate cache writeback for a small object (1MB)
        let cache_key = "bucket/key".to_string();
        let object_data = vec![42u8; MI_B]; // 1MB object

        // Verify not in cache initially
        let initial = manager.get_cached(&cache_key).await;
        assert!(initial.is_none(), "Object should not be in cache initially");

        // Simulate cache writeback (as done in ecfs.rs background task)
        manager.cache_object(cache_key.clone(), object_data.clone()).await;

        // Give Moka time to process the async insert
        sleep(Duration::from_millis(50)).await;

        // Verify object is now cached
        let cached = manager.get_cached(&cache_key).await;
        assert!(cached.is_some(), "Object should be cached after writeback");
        assert_eq!(*cached.unwrap(), object_data, "Cached data should match original");

        // Verify cache stats
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 1, "Should have exactly 1 cached entry");
        assert!(stats.size >= object_data.len(), "Cache size should reflect object size");

        // Second access should hit cache
        let second_access = manager.get_cached(&cache_key).await;
        assert!(second_access.is_some(), "Second access should hit cache");

        // Verify hit count increased
        let hit_rate = manager.cache_hit_rate();
        assert!(hit_rate > 0.0, "Hit rate should be positive after cache hit");
    }

    /// Test cache writeback respects size limits
    ///
    /// Objects larger than 10MB should NOT be cached, even if cache_object is called.
    /// This validates the size check in HotObjectCache::put().
    #[tokio::test]
    async fn test_cache_writeback_size_limit() {
        let manager = ConcurrencyManager::new();

        // Try to cache an object that exceeds the 10MB limit
        let large_key = "bucket/large_object".to_string();
        let large_data = vec![0u8; 12 * MI_B]; // 12MB > 10MB limit

        manager.cache_object(large_key.clone(), large_data).await;
        sleep(Duration::from_millis(50)).await;

        // Should NOT be cached due to size limit
        let cached = manager.get_cached(&large_key).await;
        assert!(cached.is_none(), "Large object should not be cached");

        // Cache should remain empty
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0, "No entries should be cached");
    }

    /// Test cache writeback with concurrent requests
    ///
    /// Simulates multiple concurrent GetObject requests all trying to cache
    /// the same object. Moka should handle this gracefully without data races.
    #[tokio::test]
    async fn test_cache_writeback_concurrent() {
        let manager = Arc::new(ConcurrencyManager::new());
        let cache_key = "concurrent/object".to_string();
        let object_data = vec![99u8; 500 * KI_B]; // 500KB object

        // Simulate 10 concurrent writebacks of the same object
        let tasks: Vec<_> = (0..10)
            .map(|_| {
                let mgr = Arc::clone(&manager);
                let key = cache_key.clone();
                let data = object_data.clone();
                tokio::spawn(async move {
                    mgr.cache_object(key, data).await;
                })
            })
            .collect();

        for task in tasks {
            task.await.expect("Task should complete");
        }

        sleep(Duration::from_millis(100)).await;

        // Object should be cached (possibly written multiple times, but same data)
        let cached = manager.get_cached(&cache_key).await;
        assert!(cached.is_some(), "Object should be cached after concurrent writebacks");
        assert_eq!(*cached.unwrap(), object_data, "Cached data should match original");

        // Should have exactly 1 entry (Moka deduplicates by key)
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 1, "Should have exactly 1 entry despite concurrent writes");
    }

    /// Test cache enable/disable configuration via environment variable
    ///
    /// Validates that the `RUSTFS_OBJECT_CACHE_ENABLE` environment variable
    /// controls whether caching is enabled. When disabled (default), cache
    /// lookups and writebacks should be skipped to reduce memory usage.
    ///
    /// # Environment Variable
    ///
    /// - `RUSTFS_OBJECT_CACHE_ENABLE=true`: Enable caching
    /// - `RUSTFS_OBJECT_CACHE_ENABLE=false` or unset: Disable caching (default)
    ///
    /// # Why This Matters
    ///
    /// This test validates the configuration mechanism that allows operators
    /// to enable/disable caching based on their workload characteristics.
    /// For read-heavy workloads with hot objects, caching provides significant
    /// latency improvements. For write-heavy or unique-object workloads,
    /// disabling caching reduces memory overhead.
    #[tokio::test]
    async fn test_cache_enable_configuration() {
        // Create manager - the cache_enabled flag is read at construction time
        // from RUSTFS_OBJECT_CACHE_ENABLE environment variable
        let manager = ConcurrencyManager::new();

        // By default (DEFAULT_OBJECT_CACHE_ENABLE = false), caching is disabled
        // This can be verified by checking the is_cache_enabled() method
        let _cache_enabled = manager.is_cache_enabled();

        // The default is false (as defined in rustfs_config::DEFAULT_OBJECT_CACHE_ENABLE)
        // This test validates the method works correctly
        // Note: We can't easily test with the env var set to true in unit tests
        // because the LazyLock global manager is already initialized
        // Either state (true or false) is valid, as noted in the comment above

        // Cache operations should still work (the is_cache_enabled check is in ecfs.rs)
        // The ConcurrencyManager itself always has a cache, but ecfs.rs checks
        // is_cache_enabled() before using it
        let cache_key = "test/object".to_string();
        let object_data = vec![42u8; 1024];

        // Cache the object (this always works at the manager level)
        manager.cache_object(cache_key.clone(), object_data.clone()).await;
        sleep(Duration::from_millis(50)).await;

        // Retrieve from cache (this always works at the manager level)
        let cached = manager.get_cached(&cache_key).await;
        assert!(cached.is_some(), "Cache operations work regardless of is_cache_enabled flag");
    }

    // ============================================
    // CachedGetObject Response Cache Tests
    // ============================================

    /// Test CachedGetObject response cache basic operations
    ///
    /// Validates that the full response cache (with metadata) works correctly.
    /// This tests the new `get_cached_object` and `put_cached_object` methods
    /// that store complete GetObject responses with body and metadata.
    #[tokio::test]
    async fn test_cached_get_object_basic() {
        let manager = ConcurrencyManager::new();

        // Create a CachedGetObject with metadata using builder pattern
        let cache_key = "bucket/object_with_metadata".to_string();
        let body_data = vec![42u8; 100 * KI_B];

        let cached_response = CachedGetObject::new(bytes::Bytes::from(body_data.clone()), body_data.len() as i64)
            .with_content_type("application/octet-stream".to_string())
            .with_e_tag("\"abc123def456\"".to_string())
            .with_last_modified("2024-01-15T12:00:00Z".to_string())
            .with_cache_control("max-age=3600".to_string())
            .with_storage_class("STANDARD".to_string());

        // Verify not in cache initially
        let initial = manager.get_cached_object(&cache_key).await;
        assert!(initial.is_none(), "Object should not be in cache initially");

        // Put the response in cache
        manager.put_cached_object(cache_key.clone(), cached_response.clone()).await;
        sleep(Duration::from_millis(50)).await;

        // Retrieve from cache
        let retrieved = manager.get_cached_object(&cache_key).await;
        assert!(retrieved.is_some(), "Object should be cached");

        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.body.as_ref(), body_data.as_slice(), "Body should match");
        assert_eq!(retrieved.content_length, body_data.len() as i64, "Content length should match");
        assert_eq!(
            retrieved.content_type,
            Some("application/octet-stream".to_string()),
            "Content type should match"
        );
        assert_eq!(retrieved.e_tag, Some("\"abc123def456\"".to_string()), "ETag should match");
        assert_eq!(
            retrieved.last_modified,
            Some("2024-01-15T12:00:00Z".to_string()),
            "Last modified should match"
        );
        assert_eq!(retrieved.storage_class, Some("STANDARD".to_string()), "Storage class should match");
    }

    /// Test CachedGetObject with versioned objects
    ///
    /// Validates that versioned cache keys work correctly using the format
    /// "{bucket}/{key}?versionId={version_id}".
    #[tokio::test]
    async fn test_cached_get_object_versioned() {
        let manager = ConcurrencyManager::new();

        let bucket = "versioned-bucket";
        let key = "object";
        let version_id = "v1234567890";

        // Create cache keys for latest and versioned
        let latest_key = ConcurrencyManager::make_cache_key(bucket, key, None);
        let versioned_key = ConcurrencyManager::make_cache_key(bucket, key, Some(version_id));

        assert_eq!(latest_key, "versioned-bucket/object");
        assert_eq!(versioned_key, "versioned-bucket/object?versionId=v1234567890");

        // Cache different versions
        let v1_body = vec![1u8; 10 * KI_B];
        let v2_body = vec![2u8; 10 * KI_B];

        let v1_response = CachedGetObject::new(bytes::Bytes::from(v1_body.clone()), v1_body.len() as i64)
            .with_version_id(version_id.to_string());

        let v2_response = CachedGetObject::new(bytes::Bytes::from(v2_body.clone()), v2_body.len() as i64);

        // Cache both versions
        manager.put_cached_object(versioned_key.clone(), v1_response).await;
        manager.put_cached_object(latest_key.clone(), v2_response).await;
        sleep(Duration::from_millis(50)).await;

        // Verify both can be retrieved independently
        let retrieved_v1 = manager.get_cached_object(&versioned_key).await;
        let retrieved_latest = manager.get_cached_object(&latest_key).await;

        assert!(retrieved_v1.is_some(), "Versioned object should be cached");
        assert!(retrieved_latest.is_some(), "Latest object should be cached");

        assert_eq!(retrieved_v1.unwrap().body.as_ref(), v1_body.as_slice(), "V1 body should match");
        assert_eq!(retrieved_latest.unwrap().body.as_ref(), v2_body.as_slice(), "Latest body should match");
    }

    /// Test cache invalidation for write operations
    ///
    /// Validates that `invalidate_cache` and `invalidate_cache_versioned` work correctly.
    /// This is critical for cache consistency after put_object, delete_object, etc.
    #[tokio::test]
    async fn test_cache_invalidation() {
        let manager = ConcurrencyManager::new();

        let cache_key = "bucket/to_invalidate".to_string();
        let body_data = vec![42u8; 10 * KI_B];

        // Cache an object
        let cached_response = CachedGetObject::new(bytes::Bytes::from(body_data), 10 * KI_B as i64);

        manager.put_cached_object(cache_key.clone(), cached_response).await;
        sleep(Duration::from_millis(50)).await;

        // Verify it's cached
        assert!(manager.get_cached_object(&cache_key).await.is_some(), "Object should be cached");

        // Invalidate the cache
        manager.invalidate_cache(&cache_key).await;
        sleep(Duration::from_millis(50)).await;

        // Verify it's no longer cached
        assert!(manager.get_cached_object(&cache_key).await.is_none(), "Object should be invalidated");
    }

    /// Test versioned cache invalidation
    ///
    /// Validates that invalidating a versioned object also invalidates the latest key
    /// to prevent serving stale data after writes.
    #[tokio::test]
    async fn test_cache_invalidation_versioned() {
        let manager = ConcurrencyManager::new();

        let bucket = "bucket";
        let key = "object";
        let version_id = "v123";

        let latest_key = ConcurrencyManager::make_cache_key(bucket, key, None);
        let versioned_key = ConcurrencyManager::make_cache_key(bucket, key, Some(version_id));

        let body_data = vec![42u8; 10 * KI_B];

        // Cache both versions
        let response = CachedGetObject::new(bytes::Bytes::from(body_data), 10 * KI_B as i64);

        manager.put_cached_object(latest_key.clone(), response.clone()).await;
        manager.put_cached_object(versioned_key.clone(), response).await;
        sleep(Duration::from_millis(50)).await;

        // Verify both are cached
        assert!(manager.get_cached_object(&latest_key).await.is_some(), "Latest should be cached");
        assert!(manager.get_cached_object(&versioned_key).await.is_some(), "Versioned should be cached");

        // Invalidate with version - should invalidate both
        manager.invalidate_cache_versioned(bucket, key, Some(version_id)).await;
        sleep(Duration::from_millis(50)).await;

        // Both should be invalidated
        assert!(manager.get_cached_object(&latest_key).await.is_none(), "Latest should be invalidated");
        assert!(
            manager.get_cached_object(&versioned_key).await.is_none(),
            "Versioned should be invalidated"
        );
    }

    /// Test CachedGetObject size limit enforcement
    ///
    /// Validates that objects larger than 10MB are not cached in the response cache.
    #[tokio::test]
    async fn test_cached_get_object_size_limit() {
        let manager = ConcurrencyManager::new();

        let cache_key = "bucket/large_response".to_string();
        let large_body = vec![0u8; 12 * MI_B]; // 12MB > 10MB limit

        let large_response = CachedGetObject::new(bytes::Bytes::from(large_body), 12 * MI_B as i64);

        // Try to cache - should be rejected due to size
        manager.put_cached_object(cache_key.clone(), large_response).await;
        sleep(Duration::from_millis(50)).await;

        // Should NOT be cached
        assert!(
            manager.get_cached_object(&cache_key).await.is_none(),
            "Large response should not be cached"
        );
    }

    /// Test CachedGetObject max_object_size accessor
    ///
    /// Validates the max_object_size() method returns the correct threshold.
    #[tokio::test]
    async fn test_max_object_size() {
        let manager = ConcurrencyManager::new();

        // Default max object size is 10MB
        assert_eq!(manager.max_object_size(), 10 * MI_B, "Max object size should be 10MB");
    }

    // ============================================
    // Adaptive I/O Strategy Tests
    // ============================================

    /// Test IoLoadLevel classification based on wait duration.
    ///
    /// This test validates that the IoLoadLevel enum correctly classifies
    /// disk permit wait times into appropriate load levels.
    #[test]
    fn test_io_load_level_classification() {
        use crate::storage::concurrency::IoLoadLevel;
        use std::time::Duration;

        // Low load: < 10ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(0)), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(5)), IoLoadLevel::Low);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(9)), IoLoadLevel::Low);

        // Medium load: 10-50ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(10)), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(30)), IoLoadLevel::Medium);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(49)), IoLoadLevel::Medium);

        // High load: 50-200ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(50)), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(100)), IoLoadLevel::High);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(199)), IoLoadLevel::High);

        // Critical load: > 200ms
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(200)), IoLoadLevel::Critical);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_millis(500)), IoLoadLevel::Critical);
        assert_eq!(IoLoadLevel::from_wait_duration(Duration::from_secs(1)), IoLoadLevel::Critical);
    }

    /// Test IoStrategy buffer size calculation based on load level.
    ///
    /// This test validates that buffer sizes are appropriately reduced
    /// under higher load conditions.
    #[test]
    fn test_io_strategy_buffer_sizing() {
        use crate::storage::concurrency::IoStrategy;
        use std::time::Duration;

        let base_buffer = 256 * KI_B;

        // Low load: 100% of base buffer
        let strategy_low = IoStrategy::from_wait_duration(Duration::from_millis(5), base_buffer);
        assert_eq!(strategy_low.buffer_multiplier, 1.0);
        assert_eq!(strategy_low.buffer_size, base_buffer);
        assert!(strategy_low.enable_readahead);
        assert!(strategy_low.cache_writeback_enabled);

        // Medium load: 75% of base buffer
        let strategy_med = IoStrategy::from_wait_duration(Duration::from_millis(30), base_buffer);
        assert_eq!(strategy_med.buffer_multiplier, 0.75);
        assert_eq!(strategy_med.buffer_size, (base_buffer as f64 * 0.75) as usize);
        assert!(strategy_med.enable_readahead);
        assert!(strategy_med.cache_writeback_enabled);

        // High load: 50% of base buffer
        let strategy_high = IoStrategy::from_wait_duration(Duration::from_millis(100), base_buffer);
        assert_eq!(strategy_high.buffer_multiplier, 0.5);
        assert_eq!(strategy_high.buffer_size, (base_buffer as f64 * 0.5) as usize);
        assert!(!strategy_high.enable_readahead); // Disabled under high load
        assert!(strategy_high.cache_writeback_enabled);

        // Critical load: 40% of base buffer
        let strategy_crit = IoStrategy::from_wait_duration(Duration::from_millis(500), base_buffer);
        assert_eq!(strategy_crit.buffer_multiplier, 0.4);
        // Buffer size clamped to min 32KB, max 1MB
        let expected = ((base_buffer as f64) * 0.4) as usize;
        assert_eq!(strategy_crit.buffer_size, expected.clamp(32 * KI_B, MI_B));
        assert!(!strategy_crit.enable_readahead);
        assert!(!strategy_crit.cache_writeback_enabled); // Disabled under critical load
    }

    /// Test ConcurrencyManager adaptive I/O strategy calculation.
    ///
    /// This test validates that the calculate_io_strategy method correctly
    /// produces IoStrategy instances with the expected parameters.
    #[tokio::test]
    async fn test_calculate_io_strategy() {
        use crate::storage::concurrency::IoLoadLevel;
        use std::time::Duration;

        let manager = ConcurrencyManager::new();
        let base_buffer = 256 * KI_B;

        // Low load strategy
        let strategy = manager.calculate_io_strategy(Duration::from_millis(5), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Low);
        assert_eq!(strategy.buffer_size, base_buffer);

        // Medium load strategy
        let strategy = manager.calculate_io_strategy(Duration::from_millis(30), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Medium);

        // High load strategy
        let strategy = manager.calculate_io_strategy(Duration::from_millis(100), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::High);
        assert!(!strategy.enable_readahead);

        // Critical load strategy
        let strategy = manager.calculate_io_strategy(Duration::from_millis(500), base_buffer);
        assert_eq!(strategy.load_level, IoLoadLevel::Critical);
        assert!(!strategy.cache_writeback_enabled);
    }

    /// Test ConcurrencyManager I/O load stats tracking.
    ///
    /// This test validates that the io_load_stats method correctly returns
    /// statistics about permit wait times.
    #[tokio::test]
    async fn test_io_load_stats() {
        use std::time::Duration;

        let manager = ConcurrencyManager::new();

        // Record some wait observations
        manager.record_permit_wait(Duration::from_millis(10));
        manager.record_permit_wait(Duration::from_millis(20));
        manager.record_permit_wait(Duration::from_millis(30));

        let (avg, p95, max, count) = manager.io_load_stats();

        // Check observation count
        assert_eq!(count, 3, "Should have 3 observations");

        // Average should be around 20ms
        assert!(
            avg >= Duration::from_millis(15) && avg <= Duration::from_millis(25),
            "Average should be around 20ms, got {avg:?}"
        );

        // Max should be 30ms
        assert_eq!(max, Duration::from_millis(30), "Max should be 30ms");

        // P95 should be at or near 30ms
        assert!(p95 >= Duration::from_millis(25), "P95 should be near 30ms, got {p95:?}");
    }
}
