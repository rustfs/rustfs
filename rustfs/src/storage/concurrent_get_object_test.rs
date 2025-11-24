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

//! Integration tests for concurrent GetObject performance optimization
//!
//! This test module validates the concurrency management features including:
//! - Request tracking and RAII guards
//! - Adaptive buffer sizing based on concurrent load
//! - LRU cache operations and eviction
//! - Batch operations and cache warming
//! - Hot key tracking and analysis

#[cfg(test)]
mod tests {
    use crate::storage::concurrency::{
        ConcurrencyManager, GetObjectGuard, get_advanced_buffer_size, get_concurrency_aware_buffer_size,
    };
    use rustfs_config::{KI_B, MI_B};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

    /// Test that concurrent requests are tracked correctly
    #[tokio::test]
    async fn test_concurrent_request_tracking() {
        // Start with no active requests
        let initial = GetObjectGuard::concurrent_requests();

        // Create guards to simulate concurrent requests
        let guard1 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        let guard2 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        let guard3 = ConcurrencyManager::track_request();
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 3);

        // Drop guards and verify count decreases
        drop(guard1);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 2);

        drop(guard2);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial + 1);

        drop(guard3);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(GetObjectGuard::concurrent_requests(), initial);
    }

    /// Test adaptive buffer sizing under different concurrency levels
    #[tokio::test]
    async fn test_adaptive_buffer_sizing() {
        let file_size = 32 * MI_B as i64;
        let base_buffer = 256 * 1024; // 256KB base

        // Simulate different concurrency levels
        let test_cases = vec![
            (1, 1.0, "Very low concurrency: should use full buffer"),
            (2, 1.0, "Low concurrency: should use full buffer"),
            (3, 0.75, "Medium concurrency: should reduce to 75%"),
            (6, 0.5, "High concurrency: should reduce to 50%"),
            (10, 0.4, "Very high concurrency: should reduce to 40%"),
        ];

        for (concurrent_requests, expected_multiplier, description) in test_cases {
            // Simulate concurrent requests
            let mut guards = Vec::new();
            for _ in 0..concurrent_requests {
                guards.push(ConcurrencyManager::track_request());
            }

            tokio::time::sleep(Duration::from_millis(10)).await;

            let buffer_size = get_concurrency_aware_buffer_size(file_size, base_buffer);
            let actual_multiplier = buffer_size as f64 / base_buffer as f64;

            println!(
                "{}: {} requests, buffer {} bytes, multiplier {:.2}",
                description, concurrent_requests, buffer_size, actual_multiplier
            );

            // Allow some tolerance for rounding
            assert!(
                (actual_multiplier - expected_multiplier).abs() < 0.15,
                "{} - Expected multiplier {:.2}, got {:.2}",
                description,
                expected_multiplier,
                actual_multiplier
            );

            // Cleanup
            drop(guards);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Test that buffer size stays within reasonable bounds
    #[tokio::test]
    async fn test_buffer_size_bounds() {
        let base_buffer = 512 * 1024; // 512KB

        // Test with extreme concurrency
        let mut guards = Vec::new();
        for _ in 0..100 {
            guards.push(ConcurrencyManager::track_request());
        }

        tokio::time::sleep(Duration::from_millis(10)).await;

        let buffer_size = get_concurrency_aware_buffer_size(10 * MI_B as i64, base_buffer);

        // Should not go below 64KB
        assert!(buffer_size >= 64 * 1024, "Buffer size too small: {}", buffer_size);

        // Should not exceed 1MB for high concurrency
        assert!(buffer_size <= MI_B, "Buffer size too large: {}", buffer_size);

        drop(guards);
    }

    /// Benchmark concurrent request handling
    #[tokio::test]
    async fn bench_concurrent_requests() {
        let concurrency_levels = vec![1, 2, 4, 8, 16];

        for concurrency in concurrency_levels {
            let start = Instant::now();
            let mut handles = Vec::new();

            for _ in 0..concurrency {
                let handle = tokio::spawn(async {
                    let _guard = ConcurrencyManager::track_request();

                    // Simulate some work (e.g., reading a file)
                    tokio::time::sleep(Duration::from_millis(10)).await;

                    _guard.elapsed()
                });

                handles.push(handle);
            }

            // Wait for all to complete
            let mut durations = Vec::new();
            for handle in handles {
                if let Ok(duration) = handle.await {
                    durations.push(duration);
                }
            }

            let total_elapsed = start.elapsed();
            let avg_duration = durations.iter().sum::<Duration>() / durations.len() as u32;

            println!(
                "Concurrency {}: total={}ms, avg={}ms, max={}ms",
                concurrency,
                total_elapsed.as_millis(),
                avg_duration.as_millis(),
                durations.iter().max().unwrap().as_millis()
            );
        }
    }

    /// Test disk I/O permit acquisition
    #[tokio::test]
    async fn test_disk_io_permits() {
        let manager = ConcurrencyManager::new();

        // Acquire multiple permits
        let permit1 = manager.acquire_disk_read_permit().await;
        let permit2 = manager.acquire_disk_read_permit().await;

        // Drop permits
        drop(permit1);
        drop(permit2);

        // Should be able to acquire again
        let _permit3 = manager.acquire_disk_read_permit().await;
    }

    /// Test cache behavior with manager
    #[tokio::test]
    async fn test_cache_operations() {
        let manager = ConcurrencyManager::new();

        // Initially empty cache
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size, 0);

        // Cache a small object
        let key = "test/object1".to_string();
        let data = vec![1u8; 1024 * 1024]; // 1MB
        manager.cache_object(key.clone(), data.clone()).await;

        // Verify it was cached
        let cached = manager.get_cached(&key).await;
        assert!(cached.is_some());
        assert_eq!(*cached.unwrap(), data);

        // Verify stats updated
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 1);
        assert!(stats.size >= data.len());

        // Try to get non-existent key
        let missing = manager.get_cached("missing/key").await;
        assert!(missing.is_none());

        // Clear cache
        manager.clear_cache().await;
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.size, 0);
    }

    /// Test that large objects are not cached
    #[tokio::test]
    async fn test_large_object_not_cached() {
        let manager = ConcurrencyManager::new();

        // Try to cache a large object (> 10MB)
        let key = "test/large".to_string();
        let large_data = vec![1u8; 15 * MI_B]; // 15MB

        manager.cache_object(key.clone(), large_data).await;

        // Should not be cached
        let cached = manager.get_cached(&key).await;
        assert!(cached.is_none());

        // Cache stats should still be empty
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 0);
    }

    /// Test cache eviction under memory pressure
    #[tokio::test]
    async fn test_cache_eviction() {
        let manager = ConcurrencyManager::new();

        // Cache multiple objects until we exceed the limit
        let object_size = 6 * MI_B; // 6MB each
        let num_objects = 20; // Total 120MB > 100MB limit

        for i in 0..num_objects {
            let key = format!("test/object{}", i);
            let data = vec![1u8; object_size];
            manager.cache_object(key, data).await;
        }

        // Verify cache size is within limit
        let stats = manager.cache_stats().await;
        assert!(stats.size <= stats.max_size, "Cache size {} exceeded max {}", stats.size, stats.max_size);

        // Some objects should have been evicted
        assert!(
            stats.entries < num_objects,
            "Expected eviction, but all {} objects are still cached",
            stats.entries
        );

        // First objects should be evicted (LRU)
        let first = manager.get_cached("test/object0").await;
        assert!(first.is_none(), "First object should have been evicted");

        // Recent objects should still be there
        let recent_key = format!("test/object{}", num_objects - 1);
        let recent = manager.get_cached(&recent_key).await;
        assert!(recent.is_some(), "Recent object should still be cached");
    }

    /// Test batch cache operations
    #[tokio::test]
    async fn test_cache_batch_operations() {
        let manager = ConcurrencyManager::new();

        // Cache multiple objects
        for i in 0..10 {
            let key = format!("batch/object{}", i);
            let data = vec![i as u8; 100 * KI_B]; // 100KB each
            manager.cache_object(key, data).await;
        }

        // Test batch get
        let keys: Vec<String> = (0..10).map(|i| format!("batch/object{}", i)).collect();
        let results = manager.get_cached_batch(&keys).await;

        assert_eq!(results.len(), 10, "Should return result for each key");
        for (i, result) in results.iter().enumerate() {
            assert!(result.is_some(), "Object {} should be in cache", i);
            assert_eq!(result.as_ref().unwrap().len(), 100 * KI_B);
        }

        // Test batch get with missing keys
        let mixed_keys = vec![
            "batch/object0".to_string(),
            "missing/key1".to_string(),
            "batch/object5".to_string(),
            "missing/key2".to_string(),
        ];
        let mixed_results = manager.get_cached_batch(&mixed_keys).await;

        assert_eq!(mixed_results.len(), 4);
        assert!(mixed_results[0].is_some(), "First key should exist");
        assert!(mixed_results[1].is_none(), "Second key should be missing");
        assert!(mixed_results[2].is_some(), "Third key should exist");
        assert!(mixed_results[3].is_none(), "Fourth key should be missing");
    }

    /// Test cache warming functionality
    #[tokio::test]
    async fn test_cache_warming() {
        let manager = ConcurrencyManager::new();

        // Prepare objects for warming
        let mut warm_objects = Vec::new();
        for i in 0..5 {
            let key = format!("warm/object{}", i);
            let data = vec![i as u8; 512 * KI_B]; // 512KB each
            warm_objects.push((key, data));
        }

        // Warm cache
        manager.warm_cache(warm_objects).await;

        // Verify all objects are cached
        let stats = manager.cache_stats().await;
        assert_eq!(stats.entries, 5, "All objects should be cached");

        for i in 0..5 {
            let key = format!("warm/object{}", i);
            assert!(manager.is_cached(&key).await, "Object {} should be cached", i);
        }
    }

    /// Test hot keys tracking
    #[tokio::test]
    async fn test_hot_keys_tracking() {
        let manager = ConcurrencyManager::new();

        // Cache objects with different access patterns
        for i in 0..5 {
            let key = format!("hot/object{}", i);
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

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

        assert_eq!(hot_keys.len(), 3, "Should return top 3 keys");
        assert_eq!(hot_keys[0].0, "hot/object0", "Most accessed should be first");
        assert!(hot_keys[0].1 >= 10, "Object 0 should have at least 10 hits");
        assert_eq!(hot_keys[1].0, "hot/object1", "Second most accessed should be second");
        assert!(hot_keys[1].1 >= 5, "Object 1 should have at least 5 hits");
    }

    /// Test cache removal
    #[tokio::test]
    async fn test_cache_removal() {
        let manager = ConcurrencyManager::new();

        // Cache an object
        let key = "remove/test".to_string();
        let data = vec![1u8; 100 * KI_B];
        manager.cache_object(key.clone(), data).await;

        // Verify it's cached
        assert!(manager.is_cached(&key).await, "Object should be cached");

        // Remove it
        let removed = manager.remove_cached(&key).await;
        assert!(removed, "Should successfully remove cached object");

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
        assert!(small_size < base_buffer, "Small files should use smaller buffers");
        assert!(small_size >= 16 * KI_B, "Should not go below minimum");

        // Test sequential read optimization
        let _guard = ConcurrencyManager::track_request();
        let sequential_size = get_advanced_buffer_size(10 * MI_B as i64, base_buffer, true);
        let random_size = get_advanced_buffer_size(10 * MI_B as i64, base_buffer, false);

        // Sequential reads should get larger buffers at low concurrency
        assert!(
            sequential_size >= random_size,
            "Sequential reads should have equal or larger buffers at low concurrency"
        );

        drop(_guard);

        // Test high concurrency with large files
        let mut guards = Vec::new();
        for _ in 0..10 {
            guards.push(ConcurrencyManager::track_request());
        }

        let high_concurrency_size = get_advanced_buffer_size(50 * MI_B as i64, base_buffer, false);
        assert!(high_concurrency_size <= base_buffer, "High concurrency should reduce buffer size");

        drop(guards);
    }

    /// Test cache performance under concurrent load
    #[tokio::test]
    async fn test_concurrent_cache_access() {
        let manager = Arc::new(ConcurrencyManager::new());

        // Pre-populate cache
        for i in 0..50 {
            let key = format!("concurrent/object{}", i);
            let data = vec![i as u8; 100 * KI_B];
            manager.cache_object(key, data).await;
        }

        // Spawn multiple concurrent readers
        let mut handles = Vec::new();
        for worker_id in 0..10 {
            let manager_clone = Arc::clone(&manager);
            let handle = tokio::spawn(async move {
                let mut hit_count = 0;
                for i in 0..50 {
                    let key = format!("concurrent/object{}", i);
                    if manager_clone.get_cached(&key).await.is_some() {
                        hit_count += 1;
                    }
                }
                (worker_id, hit_count)
            });
            handles.push(handle);
        }

        // Wait for all workers to complete
        let mut total_hits = 0;
        for handle in handles {
            let (worker_id, hits) = handle.await.unwrap();
            println!("Worker {} got {} cache hits", worker_id, hits);
            total_hits += hits;
        }

        // All workers should get hits for all cached objects
        assert_eq!(total_hits, 500, "Should have 500 total hits (10 workers * 50 objects)");
    }

    /// Test is_cached doesn't affect LRU order
    #[tokio::test]
    async fn test_is_cached_no_promotion() {
        let manager = ConcurrencyManager::new();

        // Cache two objects
        manager.cache_object("first".to_string(), vec![1u8; 100 * KI_B]).await;
        manager.cache_object("second".to_string(), vec![2u8; 100 * KI_B]).await;

        // Check first without accessing it
        assert!(manager.is_cached("first").await);

        // Access second multiple times
        for _ in 0..5 {
            let _ = manager.get_cached("second").await;
        }

        // Both should still be cached
        assert!(manager.is_cached("first").await);
        assert!(manager.is_cached("second").await);

        // Get hot keys - second should be hotter
        let hot_keys = manager.get_hot_keys(2).await;
        assert_eq!(hot_keys[0].0, "second", "Second should be hottest");
        assert!(hot_keys[0].1 >= 5, "Second should have at least 5 hits");
    }
}
