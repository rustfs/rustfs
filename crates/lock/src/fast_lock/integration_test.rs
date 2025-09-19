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

//! Integration tests for performance optimizations

#[cfg(test)]
mod tests {
    use crate::fast_lock::FastObjectLockManager;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_object_pool_integration() {
        let manager = FastObjectLockManager::new();

        // Create many locks to test pool efficiency
        let mut guards = Vec::new();
        for i in 0..100 {
            let bucket = format!("test-bucket-{}", i % 10); // Reuse some bucket names
            let object = format!("test-object-{i}");

            let guard = manager
                .acquire_write_lock(bucket.as_str(), object.as_str(), "test-owner")
                .await
                .expect("Failed to acquire lock");
            guards.push(guard);
        }

        // Drop all guards to return objects to pool
        drop(guards);

        // Wait a moment for cleanup
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Get pool statistics from all shards
        let pool_stats = manager.get_pool_stats();
        let (hits, misses, releases, pool_size) = pool_stats.iter().fold((0, 0, 0, 0), |acc, stats| {
            (acc.0 + stats.0, acc.1 + stats.1, acc.2 + stats.2, acc.3 + stats.3)
        });
        let hit_rate = if hits + misses > 0 {
            hits as f64 / (hits + misses) as f64
        } else {
            0.0
        };

        println!("Pool stats - Hits: {hits}, Misses: {misses}, Releases: {releases}, Pool size: {pool_size}");
        println!("Hit rate: {:.2}%", hit_rate * 100.0);

        // We should see some pool activity
        assert!(hits + misses > 0, "Pool should have been used");
    }

    #[tokio::test]
    async fn test_optimized_notification_system() {
        let manager = FastObjectLockManager::new();

        // Test that notifications work by measuring timing
        let start = std::time::Instant::now();

        // Acquire two read locks on different objects (should be fast)
        let guard1 = manager
            .acquire_read_lock("bucket", "object1", "reader1")
            .await
            .expect("Failed to acquire first read lock");

        let guard2 = manager
            .acquire_read_lock("bucket", "object2", "reader2")
            .await
            .expect("Failed to acquire second read lock");

        let duration = start.elapsed();
        println!("Two read locks on different objects took: {duration:?}");

        // Should be very fast since no contention
        assert!(duration < Duration::from_millis(10), "Read locks should be fast with no contention");

        drop(guard1);
        drop(guard2);

        // Test same object contention
        let start = std::time::Instant::now();
        let guard1 = manager
            .acquire_read_lock("bucket", "same-object", "reader1")
            .await
            .expect("Failed to acquire first read lock on same object");

        let guard2 = manager
            .acquire_read_lock("bucket", "same-object", "reader2")
            .await
            .expect("Failed to acquire second read lock on same object");

        let duration = start.elapsed();
        println!("Two read locks on same object took: {duration:?}");

        // Should still be fast since read locks are compatible
        assert!(duration < Duration::from_millis(10), "Compatible read locks should be fast");

        drop(guard1);
        drop(guard2);
    }

    #[tokio::test]
    async fn test_fast_path_optimization() {
        let manager = FastObjectLockManager::new();

        // First acquisition should be fast path
        let start = std::time::Instant::now();
        let guard1 = manager
            .acquire_read_lock("bucket", "object", "reader1")
            .await
            .expect("Failed to acquire first read lock");
        let first_duration = start.elapsed();

        // Second read lock should also be fast path
        let start = std::time::Instant::now();
        let guard2 = manager
            .acquire_read_lock("bucket", "object", "reader2")
            .await
            .expect("Failed to acquire second read lock");
        let second_duration = start.elapsed();

        println!("First lock: {first_duration:?}, Second lock: {second_duration:?}");

        // Both should be very fast (sub-millisecond typically)
        assert!(first_duration < Duration::from_millis(10));
        assert!(second_duration < Duration::from_millis(10));

        drop(guard1);
        drop(guard2);
    }

    #[tokio::test]
    async fn test_batch_operations_optimization() {
        let manager = FastObjectLockManager::new();

        // Test batch operation with sorted keys
        let batch = crate::fast_lock::BatchLockRequest::new("batch-owner")
            .add_read_lock("bucket", "obj1")
            .add_read_lock("bucket", "obj2")
            .add_write_lock("bucket", "obj3")
            .with_all_or_nothing(false);

        let start = std::time::Instant::now();
        let result = manager.acquire_locks_batch(batch).await;
        let duration = start.elapsed();

        println!("Batch operation took: {duration:?}");

        assert!(result.all_acquired, "All locks should be acquired");
        assert_eq!(result.successful_locks.len(), 3);
        assert!(result.failed_locks.is_empty());

        // Batch should be reasonably fast
        assert!(duration < Duration::from_millis(100));
    }
}
