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

use rustfs::storage::concurrency::{ConcurrencyManager, GetObjectGuard, get_concurrency_aware_buffer_size};
use rustfs_config::MI_B;
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
        
        println!("{}: {} requests, buffer {} bytes, multiplier {:.2}",
                 description, concurrent_requests, buffer_size, actual_multiplier);
        
        // Allow some tolerance for rounding
        assert!(
            (actual_multiplier - expected_multiplier).abs() < 0.15,
            "{} - Expected multiplier {:.2}, got {:.2}",
            description, expected_multiplier, actual_multiplier
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
    assert!(stats.size <= stats.max_size, 
            "Cache size {} exceeded max {}", stats.size, stats.max_size);
    
    // Some objects should have been evicted
    assert!(stats.entries < num_objects,
            "Expected eviction, but all {} objects are still cached", stats.entries);
    
    // First objects should be evicted (LRU)
    let first = manager.get_cached("test/object0").await;
    assert!(first.is_none(), "First object should have been evicted");
    
    // Recent objects should still be there
    let recent_key = format!("test/object{}", num_objects - 1);
    let recent = manager.get_cached(&recent_key).await;
    assert!(recent.is_some(), "Recent object should still be cached");
}
