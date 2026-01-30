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

#[cfg(test)]
mod fast_lock_tests {
    use crate::fast_lock::FastObjectLockManager;
    use crate::fast_lock::types::{LockConfig, LockMode, LockPriority, LockResult, ObjectKey, ObjectLockRequest};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::sleep;

    /// Helper function to create a test lock manager
    fn create_test_manager() -> FastObjectLockManager {
        let config = LockConfig {
            shard_count: 4, // Use smaller shard count for tests
            default_lock_timeout: Duration::from_secs(30),
            default_acquire_timeout: Duration::from_secs(5),
            ..LockConfig::default()
        };
        FastObjectLockManager::with_config(config)
    }

    #[tokio::test]
    async fn test_basic_write_lock_acquire_release() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner: Arc<str> = Arc::from("test-owner");

        // Acquire write lock
        let mut guard = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire write lock");

        // Verify guard properties
        assert_eq!(guard.key(), &key);
        assert_eq!(guard.mode(), LockMode::Exclusive);
        assert_eq!(guard.owner(), &owner);
        assert!(!guard.is_released());

        // Manually release lock
        assert!(guard.release(), "Should release lock successfully");
        assert!(guard.is_released(), "Guard should be marked as released");

        // Try to acquire again - should succeed
        let guard2 = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire write lock again after release");
        drop(guard2);
    }

    #[tokio::test]
    async fn test_basic_read_lock_acquire_release() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner: Arc<str> = Arc::from("test-owner");

        // Acquire read lock
        let mut guard = manager
            .acquire_read_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire read lock");

        // Verify guard properties
        assert_eq!(guard.key(), &key);
        assert_eq!(guard.mode(), LockMode::Shared);
        assert_eq!(guard.owner(), &owner);
        assert!(!guard.is_released());

        // Manually release lock
        assert!(guard.release(), "Should release lock successfully");
        assert!(guard.is_released(), "Guard should be marked as released");
    }

    #[tokio::test]
    async fn test_lock_auto_release_on_drop() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");

        // Acquire lock and drop guard
        {
            let guard = manager
                .acquire_write_lock(key.clone(), owner1.clone())
                .await
                .expect("Should acquire write lock");
            assert!(!guard.is_released());
            // Guard is dropped here, lock should be automatically released
        }

        // Wait a bit to ensure cleanup
        sleep(Duration::from_millis(10)).await;

        // Another owner should be able to acquire the lock
        let guard2 = manager
            .acquire_write_lock(key.clone(), owner2.clone())
            .await
            .expect("Should acquire write lock after previous guard dropped");
        drop(guard2);
    }

    #[tokio::test]
    async fn test_multiple_read_locks() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");
        let owner3: Arc<str> = Arc::from("owner3");

        // Multiple read locks should be allowed
        let mut guard1 = manager
            .acquire_read_lock(key.clone(), owner1.clone())
            .await
            .expect("Should acquire first read lock");

        let mut guard2 = manager
            .acquire_read_lock(key.clone(), owner2.clone())
            .await
            .expect("Should acquire second read lock");

        let mut guard3 = manager
            .acquire_read_lock(key.clone(), owner3.clone())
            .await
            .expect("Should acquire third read lock");

        // All guards should be valid
        assert_eq!(guard1.mode(), LockMode::Shared);
        assert_eq!(guard2.mode(), LockMode::Shared);
        assert_eq!(guard3.mode(), LockMode::Shared);

        // Release all
        assert!(guard1.release());
        assert!(guard2.release());
        assert!(guard3.release());
    }

    #[tokio::test]
    async fn test_write_lock_excludes_read_lock() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let writer: Arc<str> = Arc::from("writer");
        let reader: Arc<str> = Arc::from("reader");

        // Acquire write lock
        let mut write_guard = manager
            .acquire_write_lock(key.clone(), writer.clone())
            .await
            .expect("Should acquire write lock");

        // Try to acquire read lock - should timeout
        let read_request =
            ObjectLockRequest::new_read(key.clone(), reader.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(read_request).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "Read lock should timeout when write lock is held"
        );

        // Release write lock
        assert!(write_guard.release());

        // Now read lock should succeed
        let mut read_guard = manager
            .acquire_read_lock(key.clone(), reader.clone())
            .await
            .expect("Should acquire read lock after write lock released");
        assert!(read_guard.release());
    }

    #[tokio::test]
    async fn test_read_lock_excludes_write_lock() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let reader: Arc<str> = Arc::from("reader");
        let writer: Arc<str> = Arc::from("writer");

        // Acquire read lock
        let mut read_guard = manager
            .acquire_read_lock(key.clone(), reader.clone())
            .await
            .expect("Should acquire read lock");

        // Try to acquire write lock - should timeout
        let write_request =
            ObjectLockRequest::new_write(key.clone(), writer.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(write_request).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "Write lock should timeout when read lock is held"
        );

        // Release read lock
        assert!(read_guard.release());

        // Now write lock should succeed
        let mut write_guard = manager
            .acquire_write_lock(key.clone(), writer.clone())
            .await
            .expect("Should acquire write lock after read lock released");
        assert!(write_guard.release());
    }

    #[tokio::test]
    async fn test_write_lock_excludes_write_lock() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");

        // Acquire first write lock
        let mut guard1 = manager
            .acquire_write_lock(key.clone(), owner1.clone())
            .await
            .expect("Should acquire first write lock");

        // Try to acquire second write lock - should timeout
        let request2 = ObjectLockRequest::new_write(key.clone(), owner2.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(request2).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "Second write lock should timeout when first write lock is held"
        );

        // Release first lock
        assert!(guard1.release());

        // Now second write lock should succeed
        let mut guard2 = manager
            .acquire_write_lock(key.clone(), owner2.clone())
            .await
            .expect("Should acquire second write lock after first released");
        assert!(guard2.release());
    }

    #[tokio::test]
    async fn test_same_owner_reentrant_write_lock() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner: Arc<str> = Arc::from("owner");

        // Acquire first write lock
        let mut guard1 = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire first write lock");

        // Same owner trying to acquire again - should timeout (not reentrant)
        let request2 = ObjectLockRequest::new_write(key.clone(), owner.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(request2).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "Same owner should not be able to acquire lock again (not reentrant)"
        );

        assert!(guard1.release());
    }

    #[tokio::test]
    async fn test_different_keys_no_conflict() {
        let manager = create_test_manager();
        let key1 = ObjectKey::new("bucket1", "object1");
        let key2 = ObjectKey::new("bucket2", "object2");
        let owner: Arc<str> = Arc::from("owner");

        // Acquire locks on different keys simultaneously
        let mut guard1 = manager
            .acquire_write_lock(key1.clone(), owner.clone())
            .await
            .expect("Should acquire lock on key1");

        let mut guard2 = manager
            .acquire_write_lock(key2.clone(), owner.clone())
            .await
            .expect("Should acquire lock on key2");

        // Both should be valid
        assert_eq!(guard1.key(), &key1);
        assert_eq!(guard2.key(), &key2);

        assert!(guard1.release());
        assert!(guard2.release());
    }

    #[tokio::test]
    async fn test_versioned_keys() {
        let manager = create_test_manager();
        let base_key = ObjectKey::new("bucket", "object");
        let versioned_key = ObjectKey::with_version("bucket", "object", "v1");
        let owner: Arc<str> = Arc::from("owner");

        // Acquire lock on base key
        let mut guard1 = manager
            .acquire_write_lock(base_key.clone(), owner.clone())
            .await
            .expect("Should acquire lock on base key");

        // Should be able to acquire lock on versioned key (different keys)
        let mut guard2 = manager
            .acquire_write_lock(versioned_key.clone(), owner.clone())
            .await
            .expect("Should acquire lock on versioned key");

        assert_eq!(guard1.key(), &base_key);
        assert_eq!(guard2.key(), &versioned_key);

        assert!(guard1.release());
        assert!(guard2.release());
    }

    #[tokio::test]
    async fn test_concurrent_read_locks() {
        let manager = Arc::new(create_test_manager());
        let key = ObjectKey::new("test-bucket", "test-object");
        let num_readers = 10;

        let mut handles = Vec::new();

        // Spawn multiple readers
        for i in 0..num_readers {
            let manager = manager.clone();
            let key = key.clone();
            let owner: Arc<str> = Arc::from(format!("reader-{}", i));

            let handle = tokio::spawn(async move {
                let mut guard = manager.acquire_read_lock(key, owner).await.expect("Should acquire read lock");
                // Hold lock for a bit
                sleep(Duration::from_millis(10)).await;
                assert!(guard.release());
            });

            handles.push(handle);
        }

        // Wait for all readers
        for handle in handles {
            handle.await.expect("Reader task should complete");
        }
    }

    #[tokio::test]
    async fn test_concurrent_write_lock_contention() {
        let manager = Arc::new(create_test_manager());
        let key = ObjectKey::new("test-bucket", "test-object");
        let num_writers = 5;

        let mut handles = Vec::new();

        // Spawn multiple writers - they should serialize
        for i in 0..num_writers {
            let manager = manager.clone();
            let key = key.clone();
            let owner: Arc<str> = Arc::from(format!("writer-{}", i));

            let handle = tokio::spawn(async move {
                let mut guard = manager
                    .acquire_write_lock(key, owner)
                    .await
                    .expect("Should acquire write lock");
                // Hold lock for a bit
                sleep(Duration::from_millis(10)).await;
                assert!(guard.release());
            });

            handles.push(handle);
        }

        // Wait for all writers - they should complete sequentially
        for handle in handles {
            handle.await.expect("Writer task should complete");
        }
    }

    #[tokio::test]
    async fn test_lock_timeout() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner1: Arc<str> = Arc::from("owner1");
        let owner2: Arc<str> = Arc::from("owner2");

        // Acquire first lock
        let mut guard1 = manager
            .acquire_write_lock(key.clone(), owner1.clone())
            .await
            .expect("Should acquire first lock");

        // Try to acquire with short timeout - should timeout
        let request = ObjectLockRequest::new_write(key.clone(), owner2.clone()).with_acquire_timeout(Duration::from_millis(50));
        let result = manager.acquire_lock(request).await;
        assert!(matches!(result, Err(LockResult::Timeout)), "Should timeout when lock is held");

        assert!(guard1.release());
    }

    #[tokio::test]
    async fn test_lock_priority() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let normal_owner: Arc<str> = Arc::from("normal");
        let high_owner: Arc<str> = Arc::from("high");

        // Acquire normal priority lock
        let normal_request = ObjectLockRequest::new_write(key.clone(), normal_owner.clone())
            .with_priority(LockPriority::Normal)
            .with_acquire_timeout(Duration::from_secs(1));
        let mut normal_guard = manager
            .acquire_lock(normal_request)
            .await
            .expect("Should acquire normal priority lock");

        // Try high priority lock - should still timeout (write locks are exclusive)
        let high_request = ObjectLockRequest::new_write(key.clone(), high_owner.clone())
            .with_priority(LockPriority::High)
            .with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(high_request).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "High priority write lock should still timeout when normal write lock is held"
        );

        assert!(normal_guard.release());
    }

    #[tokio::test]
    async fn test_double_release() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner: Arc<str> = Arc::from("owner");

        let mut guard = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire lock");

        // First release should succeed
        assert!(guard.release(), "First release should succeed");
        assert!(guard.is_released(), "Guard should be marked as released");

        // Second release should fail
        assert!(!guard.release(), "Second release should fail");
    }

    #[tokio::test]
    async fn test_lock_info() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let owner: Arc<str> = Arc::from("owner");

        let mut guard = manager
            .acquire_write_lock(key.clone(), owner.clone())
            .await
            .expect("Should acquire lock");

        // Get lock info
        let lock_info = guard.lock_info();
        assert!(lock_info.is_some(), "Should have lock info");
        if let Some(info) = lock_info {
            assert_eq!(info.key, key);
            assert_eq!(info.mode, LockMode::Exclusive);
            assert_eq!(info.owner, owner);
        }

        // Release lock
        assert!(guard.release());

        // Lock info should be None after release
        let lock_info_after = guard.lock_info();
        assert!(lock_info_after.is_none(), "Lock info should be None after release");
    }

    #[tokio::test]
    async fn test_read_write_mixed_scenario() {
        let manager = create_test_manager();
        let key = ObjectKey::new("test-bucket", "test-object");
        let reader1: Arc<str> = Arc::from("reader1");
        let reader2: Arc<str> = Arc::from("reader2");
        let writer: Arc<str> = Arc::from("writer");

        // Acquire two read locks
        let mut read_guard1 = manager
            .acquire_read_lock(key.clone(), reader1.clone())
            .await
            .expect("Should acquire first read lock");
        let mut read_guard2 = manager
            .acquire_read_lock(key.clone(), reader2.clone())
            .await
            .expect("Should acquire second read lock");

        // Writer should timeout
        let write_request =
            ObjectLockRequest::new_write(key.clone(), writer.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result = manager.acquire_lock(write_request).await;
        assert!(
            matches!(result, Err(LockResult::Timeout)),
            "Write lock should timeout when read locks are held"
        );

        // Release one read lock
        assert!(read_guard1.release());

        // Writer should still timeout (other read lock still held)
        let write_request2 =
            ObjectLockRequest::new_write(key.clone(), writer.clone()).with_acquire_timeout(Duration::from_millis(100));
        let result2 = manager.acquire_lock(write_request2).await;
        assert!(
            matches!(result2, Err(LockResult::Timeout)),
            "Write lock should still timeout when read lock is held"
        );

        // Release second read lock
        assert!(read_guard2.release());

        // Now writer should succeed
        let mut write_guard = manager
            .acquire_write_lock(key.clone(), writer.clone())
            .await
            .expect("Should acquire write lock after all read locks released");
        assert!(write_guard.release());
    }
}
