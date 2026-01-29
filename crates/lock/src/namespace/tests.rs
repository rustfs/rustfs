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
use crate::client::ClientFactory;
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
