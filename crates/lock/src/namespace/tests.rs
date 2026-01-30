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

use super::*;
use crate::GlobalLockManager;
use crate::client::{ClientFactory, local::LocalClient};
use crate::types::LockType;
use std::sync::Arc;
use std::time::Duration;

fn create_test_object_key(bucket: &str, object: &str) -> ObjectKey {
    ObjectKey {
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version: None,
    }
}

#[tokio::test]
async fn test_namespace_lock_new() {
    let client = ClientFactory::create_local();
    let lock = NamespaceLock::new("test-namespace".to_string(), client);
    assert_eq!(lock.namespace(), "test-namespace");
}

#[tokio::test]
async fn test_namespace_lock_with_client() {
    let client = ClientFactory::create_local();
    let lock = NamespaceLock::with_client(client);
    assert_eq!(lock.namespace(), "default");
}

#[tokio::test]
async fn test_namespace_lock_with_local_manager() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("local-ns".to_string(), manager);
    assert_eq!(lock.namespace(), "local-ns");
}

#[tokio::test]
async fn test_namespace_lock_with_clients() {
    let clients = vec![ClientFactory::create_local(), ClientFactory::create_local()];
    let lock = NamespaceLock::with_clients("multi-client".to_string(), clients);
    assert_eq!(lock.namespace(), "multi-client");
}

#[tokio::test]
async fn test_namespace_lock_get_resource_key() {
    let client = ClientFactory::create_local();
    let lock = NamespaceLock::new("test-ns".to_string(), client);
    let resource = create_test_object_key("bucket", "object");
    let key = lock.get_resource_key(&resource);
    assert!(key.contains("test-ns"));
    assert!(key.contains("bucket"));
    assert!(key.contains("object"));
}

#[tokio::test]
async fn test_namespace_lock_acquire_guard_local() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("test-local".to_string(), manager);
    let resource = create_test_object_key("bucket", "object");

    let request = LockRequest::new(resource.clone(), LockType::Exclusive, "owner1")
        .with_acquire_timeout(Duration::from_secs(5))
        .with_ttl(Duration::from_secs(30));

    let guard_opt = lock.acquire_guard(&request).await.unwrap();
    assert!(guard_opt.is_some());

    if let Some(NamespaceLockGuard::Fast(guard)) = guard_opt {
        assert_eq!(guard.key(), &resource);
        assert!(!guard.is_released());

        // Test release
        let mut guard = guard;
        assert!(guard.release());
        assert!(guard.is_released());
    } else {
        panic!("Expected Fast guard");
    }
}

#[tokio::test]
async fn test_namespace_lock_get_write_lock_local() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("test-write".to_string(), manager);
    let resource = create_test_object_key("bucket", "object");

    let guard = lock
        .get_write_lock(resource.clone(), "owner1", Duration::from_secs(5))
        .await
        .unwrap();

    match guard {
        NamespaceLockGuard::Fast(guard) => {
            assert_eq!(guard.key(), &resource);
            assert!(!guard.is_released());
        }
        NamespaceLockGuard::Standard(_) => {
            panic!("Expected Fast guard for local lock");
        }
    }
}

#[tokio::test]
async fn test_namespace_lock_get_read_lock_local() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("test-read".to_string(), manager);
    let resource = create_test_object_key("bucket", "object");

    let guard = lock
        .get_read_lock(resource.clone(), "owner1", Duration::from_secs(5))
        .await
        .unwrap();

    match guard {
        NamespaceLockGuard::Fast(guard) => {
            assert_eq!(guard.key(), &resource);
            assert!(!guard.is_released());
        }
        NamespaceLockGuard::Standard(_) => {
            panic!("Expected Fast guard for local lock");
        }
    }
}

#[tokio::test]
async fn test_namespace_lock_guard_release() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("test-release".to_string(), manager);
    let resource = create_test_object_key("bucket", "object");

    let mut guard = lock.get_write_lock(resource, "owner1", Duration::from_secs(5)).await.unwrap();

    assert!(!guard.is_released());
    assert!(guard.release());
    assert!(guard.is_released());
}

#[tokio::test]
async fn test_namespace_lock_wrapper() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("wrapper-test".to_string(), manager);
    let resource = create_test_object_key("bucket", "object");
    let wrapper = NamespaceLockWrapper::new(lock, resource.clone(), "owner1".to_string());

    let guard = wrapper.get_write_lock(Duration::from_secs(5)).await.unwrap();

    match guard {
        NamespaceLockGuard::Fast(guard) => {
            assert_eq!(guard.key(), &resource);
        }
        _ => panic!("Expected Fast guard"),
    }
}

#[tokio::test]
async fn test_namespace_lock_get_health_local() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("health-test".to_string(), manager);

    let health = lock.get_health().await;
    assert_eq!(health.node_id, "health-test");
    assert_eq!(health.status, crate::types::HealthStatus::Healthy);
    assert_eq!(health.connected_nodes, 1);
    assert_eq!(health.total_nodes, 1);
}

#[tokio::test]
async fn test_namespace_lock_get_stats_local() {
    let manager = Arc::new(GlobalLockManager::new());
    let lock = NamespaceLock::with_local_manager("stats-test".to_string(), manager);

    let stats = lock.get_stats().await;
    // Local locks don't expose detailed stats, so defaults should be 0
    assert_eq!(stats.successful_acquires, 0);
    assert_eq!(stats.failed_acquires, 0);
}

#[tokio::test]
async fn test_namespace_lock_default() {
    let lock = NamespaceLock::default();
    assert_eq!(lock.namespace(), "default");
}

#[tokio::test]
async fn test_namespace_lock_guard_lock_id() {
    let client = ClientFactory::create_local();
    let lock = NamespaceLock::new("test-id".to_string(), client);
    let resource = create_test_object_key("bucket", "object");

    let request = LockRequest::new(resource, LockType::Exclusive, "owner1")
        .with_acquire_timeout(Duration::from_secs(5))
        .with_ttl(Duration::from_secs(30));

    if let Some(NamespaceLockGuard::Standard(guard)) = lock.acquire_guard(&request).await.unwrap() {
        // lock_id() returns &LockId, not Option, so we just check it's not empty
        let lock_id = guard.lock_id();
        assert!(!lock_id.uuid.is_empty());
    }
}

#[tokio::test]
async fn test_namespace_lock_distributed_multi_node_simulation() {
    // Simulate a 3-node distributed environment where each node has its own lock backend
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());
    let manager3 = Arc::new(GlobalLockManager::new());

    // Create 3 clients, each bound to its own manager (simulating independent nodes)
    let client1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager1));
    let client2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager2));
    let client3: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager3));

    let clients = vec![client1, client2, client3];

    // Create NamespaceLock with 3 clients (quorum will be 2)
    let lock = NamespaceLock::with_clients("multi-node".to_string(), clients);
    assert_eq!(lock.namespace(), "multi-node");

    let resource = create_test_object_key("test-bucket", "test-object");

    // Test 1: Owner A acquires write lock successfully
    let mut guard_a = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(5))
        .await
        .expect("Owner A should acquire write lock");

    // Verify it's a Standard guard (DistributedLock path)
    match &guard_a {
        NamespaceLockGuard::Standard(_) => {
            // Expected for distributed lock
        }
        NamespaceLockGuard::Fast(_) => {
            panic!("Expected Standard guard for distributed lock");
        }
    }

    // Test 2: Owner B tries to acquire write lock while A holds it - should fail
    // Since all 3 backends are holding locks from owner-a, owner-b cannot acquire on any backend
    // This means 0 successes < quorum(2), so acquisition should fail
    let result_b = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await;

    assert!(result_b.is_err(), "Owner B should fail to acquire lock while owner A holds it");

    // Verify the error is a timeout or quorum failure (since quorum cannot be reached)
    if let Err(err) = result_b {
        // The error should indicate timeout or quorum failure
        let err_str = err.to_string().to_lowercase();
        assert!(
            err_str.contains("timeout") || err_str.contains("quorum") || err_str.contains("not reached"),
            "Error should be timeout or quorum related, got: {}",
            err
        );
    }

    // Test 3: Release owner A's lock
    assert!(guard_a.release(), "Should release guard_a successfully");
    assert!(guard_a.is_released(), "Guard A should be marked as released");

    // Test 4: Owner B should now be able to acquire the lock
    let guard_b = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_secs(5))
        .await
        .expect("Owner B should acquire write lock after A releases");

    match &guard_b {
        NamespaceLockGuard::Standard(_) => {
            // Expected for distributed lock
        }
        NamespaceLockGuard::Fast(_) => {
            panic!("Expected Standard guard for distributed lock");
        }
    }

    // Test 5: Verify health check shows 3 nodes
    let health = lock.get_health().await;
    assert_eq!(health.node_id, "multi-node");
    assert_eq!(health.total_nodes, 3);
    assert_eq!(health.connected_nodes, 3);
    assert_eq!(health.status, crate::types::HealthStatus::Healthy);

    // Cleanup
    drop(guard_b);
}

#[tokio::test]
async fn test_namespace_lock_distributed_with_clients_and_quorum() {
    // Same 3-node setup as multi-node simulation; use explicit quorum via with_clients_and_quorum
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());
    let manager3 = Arc::new(GlobalLockManager::new());

    let client1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager1));
    let client2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager2));
    let client3: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager3));

    let clients = vec![client1, client2, client3];

    // Create NamespaceLock with explicit quorum=2 (same as with_clients default for 3 nodes)
    let lock = NamespaceLock::with_clients_and_quorum("multi-node".to_string(), clients, 2);
    assert_eq!(lock.namespace(), "multi-node");

    let resource = create_test_object_key("test-bucket", "test-object");

    // Owner A acquires write lock successfully
    let mut guard_a = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(5))
        .await
        .expect("Owner A should acquire write lock");

    match &guard_a {
        NamespaceLockGuard::Standard(_) => {}
        NamespaceLockGuard::Fast(_) => panic!("Expected Standard guard for distributed lock"),
    }

    // Owner B tries to acquire while A holds it - should fail (quorum not reached)
    let result_b = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await;

    assert!(result_b.is_err(), "Owner B should fail to acquire lock while owner A holds it");

    if let Err(err) = result_b {
        let err_str = err.to_string().to_lowercase();
        assert!(
            err_str.contains("timeout") || err_str.contains("quorum") || err_str.contains("not reached"),
            "Error should be timeout or quorum related, got: {}",
            err
        );
    }

    // Release owner A's lock
    assert!(guard_a.release(), "Should release guard_a successfully");
    assert!(guard_a.is_released(), "Guard A should be marked as released");

    // Owner B should now acquire the lock
    let guard_b = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_secs(5))
        .await
        .expect("Owner B should acquire write lock after A releases");

    match &guard_b {
        NamespaceLockGuard::Standard(_) => {}
        NamespaceLockGuard::Fast(_) => panic!("Expected Standard guard for distributed lock"),
    }

    // Health check: 3 nodes, Healthy
    let health = lock.get_health().await;
    assert_eq!(health.node_id, "multi-node");
    assert_eq!(health.total_nodes, 3);
    assert_eq!(health.connected_nodes, 3);
    assert_eq!(health.status, crate::types::HealthStatus::Healthy);

    drop(guard_b);
}
