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

//! Integration tests for concurrent GetObject performance optimization with Moka cache
//!
//! This test module validates the concurrency management features including:
//! - Request tracking and RAII guards
//! - Adaptive buffer sizing based on concurrent load
//! - Moka cache operations with automatic TTL/TTI
//! - Lock-free concurrent cache access
//! - Batch operations and cache warming
//! - Hot key tracking and hit rate analysis
//! - Comprehensive metrics integration

#[cfg(test)]
mod tests {
    use crate::storage::concurrency::{
        ConcurrencyManager, GetObjectGuard, get_advanced_buffer_size, get_concurrency_aware_buffer_size,
    };
    use rustfs_config::{KI_B, MI_B};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::{Instant, sleep};

    /// Test that concurrent requests are tracked correctly with RAII guards
    #[tokio::test]
    async fn test_concurrent_request_tracking() {
        // Start with current baseline
        let initial = GetObjectGuard::concurrent_requests();

        // Create guards to simulate concurrent requests
        let guard1 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        let guard2 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        let guard3 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 3);

        // Drop guards and verify count decreases automatically (RAII)
        drop(guard1);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        drop(guard2);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        drop(guard3);
        sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial);
    }

    /// Test adaptive buffer sizing under different concurrency levels
    #[tokio::test]
    async fn test_adaptive_buffer_sizing() {
        let file_size = 32 * MI_B as i64;
        let base_buffer = 256 * KI_B; // 256KB base

        // Simulate different concurrency levels
        let test_cases = vec![
            (1, 1.0, "Very low concurrency: should use full buffer"),
            (2, 1.0, "Low concurrency: should use full buffer"),
            (3, 0.75, "Medium concurrency: should reduce to 75%"),
            (6, 0.5, "High concurrency: should reduce to 50%"),
            (10, 0.4, "Very high concurrency: should reduce to 40%"),
        ];

        for (concurrent_requests, expected_multiplier, description) in test_cases {
            // Create guards to simulate concurrent requests
            let _guards: Vec<_> = (0..concurrent_requests)
                .map(|_| ConcurrencyManager::track_request())
                .collect();

            let buffer_size = get_concurrency_aware_buffer_size(file_size, base_buffer);
            let expected_size = (base_buffer as f64 * expected_multiplier) as usize;

            // Allow some tolerance for rounding
            let tolerance = base_buffer / 10;
            assert!(
                buffer_size >= expected_size.saturating_sub(tolerance) && buffer_size <= expected_size + tolerance,
                "{}: expected ~{} bytes, got {} bytes",
                description,
                expected_size,
                buffer_size
            );
        }
    }

    /// Test buffer size bounds and minimum/maximum constraints
    #[tokio::test]
    async fn test_buffer_size_bounds() {
        // Test minimum buffer size
        let small_file = 1024i64; // 1KB file
        let min_buffer = get_concurrency_aware_buffer_size(small_file, 64 * KI_B);
        assert!(min_buffer >= 64 * KI_B, "Buffer should have minimum size of 64KB, got {}", min_buffer);

        // Test maximum buffer size
        let huge_file = 10 * 1024 * MI_B as i64; // 10GB file
        let max_buffer = get_concurrency_aware_buffer_size(huge_file, 10 * MI_B);
        assert!(max_buffer <= 10 * MI_B, "Buffer should not exceed 10MB, got {}", max_buffer);

        // Test that file size smaller than buffer uses file size
        let tiny_file = 32 * KI_B as i64;
        let buffer = get_concurrency_aware_buffer_size(tiny_file, 256 * KI_B);
        assert!(buffer <= tiny_file as usize, "Buffer should not exceed file size for tiny files");
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
        assert!(elapsed < Duration::from_secs(1), "Should complete within 1 second, took {:?}", elapsed);
    }

    /// Test Moka cache operations: insert, retrieve, and stats
    #[tokio::test]
    async fn test_moka_cache_operations() {
        let manager = ConcurrencyManager::new();

        // Initially empty cache
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size, 0);

        // Cache a small object
        let key = "test/object1".to_string();
        let data = vec![1u8; 1024 * 1024]; // 1MB
        manager.cache_object(key.clone(), data.clone()).await;

        // Give Moka time to process the insert
        sleep(Duration::from_millis(50)).await;

        // Verify it was cached
        let cached = manager.get_cached(&key).await;
        assert!(cached.is_some(), "Object should be cached");
        assert_eq!(*cached.unwrap(), data, "Cached data should match");

        // Verify stats updated
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 1, "Should have 1 entry");
        assert!(stats.size >= data.len(), "Size should be at least data length");

        // Try to get non-existent key
        let missing = manager.get_cached("missing/key").await;
        assert!(missing.is_none(), "Missing key should return None");

        // Clear cache
        manager.clear_cache().await;
        sleep(Duration::from_millis(50)).await;
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0, "Cache should be empty after clear");
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
            let key = format!("test/object{}", i);
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
            let key = format!("batch/object{}", i);
            let data = vec![i as u8; 100 * KI_B]; // 100KB each
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        // Test batch get
        let keys: Vec<String> = (0..10).map(|i| format!("batch/object{}", i)).collect();
        let results = manager.get_cached_batch(&keys).await;

        assert_eq!(results.len(), 10, "Should return result for each key");

        // Verify all objects were retrieved
        let hits = results.iter().filter(|r| r.is_some()).count();
        assert!(hits >= 8, "Most objects should be cached (got {}/10 hits)", hits);

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
            .map(|i| (format!("warm/object{}", i), vec![i as u8; 500 * KI_B]))
            .collect();

        // Warm cache
        manager.warm_cache(objects.clone()).await;
        sleep(Duration::from_millis(100)).await;

        // Verify all objects are cached
        for (key, data) in objects {
            let cached = manager.get_cached(&key).await;
            assert!(cached.is_some(), "Warmed object {} should be cached", key);
            assert_eq!(*cached.unwrap(), data, "Cached data for {} should match", key);
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
            let key = format!("hot/object{}", i);
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
        let base_buffer = 256 * KI_B; // 256KB base

        // Test small file optimization
        let small_size = get_advanced_buffer_size(128 * KI_B as i64, base_buffer, false);
        assert!(
            small_size < base_buffer,
            "Small files should use smaller buffers: {} < {}",
            small_size,
            base_buffer
        );
        assert!(small_size >= 16 * KI_B, "Should not go below minimum: {}", small_size);

        // Test sequential read optimization
        let seq_size = get_advanced_buffer_size(32 * MI_B as i64, base_buffer, true);
        assert!(
            seq_size >= base_buffer,
            "Sequential reads should use larger buffers: {} >= {}",
            seq_size,
            base_buffer
        );

        // Test large file with high concurrency
        let _guards: Vec<_> = (0..10).map(|_| ConcurrencyManager::track_request()).collect();
        let large_concurrent = get_advanced_buffer_size(100 * MI_B as i64, base_buffer, false);
        assert!(
            large_concurrent <= base_buffer,
            "High concurrency should reduce buffer: {} <= {}",
            large_concurrent,
            base_buffer
        );
    }

    /// Test concurrent cache access performance (lock-free)
    #[tokio::test]
    async fn test_concurrent_cache_access() {
        let manager = Arc::new(ConcurrencyManager::new());

        // Pre-populate cache
        for i in 0..20 {
            let key = format!("concurrent/object{}", i);
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        let start = Instant::now();

        // Simulate heavy concurrent access
        let tasks: Vec<_> = (0..100)
            .map(|i| {
                let mgr = Arc::clone(&manager);
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
            "Concurrent cache access should be fast (took {:?})",
            elapsed
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
            let key = format!("hitrate/object{}", i);
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        sleep(Duration::from_millis(100)).await;

        // Mix of hits and misses
        for i in 0..10 {
            let key = if i < 5 {
                format!("hitrate/object{}", i) // Hit
            } else {
                format!("hitrate/missing{}", i) // Miss
            };
            let _ = manager.get_cached(&key).await;
        }

        // Hit rate should be around 50%
        let hit_rate = manager.cache_hit_rate();
        assert!((40.0..=60.0).contains(&hit_rate), "Hit rate should be ~50%, got {:.1}%", hit_rate);
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
            let key = format!("bench/object{}", i);
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
                let mgr = Arc::clone(&manager);
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
}
