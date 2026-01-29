#![cfg(test)]
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

use super::{grpc_lock_client::GrpcLockClient, grpc_lock_server::spawn_lock_server};
use rustfs_lock::{GlobalLockManager, NamespaceLock, ObjectKey, client::local::LocalClient};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_distributed_lock_4_nodes_grpc() {
    // Spawn 4 gRPC lock servers, each with its own GlobalLockManager
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());
    let manager3 = Arc::new(GlobalLockManager::new());
    let manager4 = Arc::new(GlobalLockManager::new());

    let client1: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager1));
    let client2: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager2));
    let client3: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager3));
    let client4: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager4));

    // Spawn 4 gRPC servers on random ports
    let (addr1, handle1) = spawn_lock_server(client1).await.expect("Failed to spawn server 1");
    let (addr2, handle2) = spawn_lock_server(client2).await.expect("Failed to spawn server 2");
    let (addr3, handle3) = spawn_lock_server(client3).await.expect("Failed to spawn server 3");
    let (addr4, handle4) = spawn_lock_server(client4).await.expect("Failed to spawn server 4");

    // Give servers a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create 4 gRPC clients (no auth)
    let grpc_client1: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr1));
    let grpc_client2: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr2));
    let grpc_client3: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr3));
    let grpc_client4: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr4));

    let clients = vec![grpc_client1, grpc_client2, grpc_client3, grpc_client4];

    // Create NamespaceLock with 4 clients and quorum=3
    let lock = NamespaceLock::with_clients_and_quorum("grpc-4-node".to_string(), clients, 3);
    assert_eq!(lock.namespace(), "grpc-4-node");

    let resource = ObjectKey {
        bucket: Arc::from("test-bucket"),
        object: Arc::from("test-object"),
        version: None,
    };

    // Test 1: Owner A acquires write lock successfully
    let mut guard_a = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(5))
        .await
        .expect("Owner A should acquire write lock");

    // Verify it's a Standard guard (DistributedLock path)
    match &guard_a {
        rustfs_lock::NamespaceLockGuard::Standard(_) => {
            // Expected for distributed lock
        }
        rustfs_lock::NamespaceLockGuard::Fast(_) => {
            panic!("Expected Standard guard for distributed lock");
        }
    }

    // Test 2: Owner B tries to acquire write lock while A holds it - should fail
    // Since all 4 backends are holding locks from owner-a, owner-b cannot acquire on any backend
    // This means 0 successes < quorum(3), so acquisition should fail
    let result_b = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await;

    assert!(result_b.is_err(), "Owner B should fail to acquire lock while owner A holds it");

    // Verify the error is a timeout or quorum failure
    if let Err(err) = result_b {
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
        rustfs_lock::NamespaceLockGuard::Standard(_) => {
            // Expected for distributed lock
        }
        rustfs_lock::NamespaceLockGuard::Fast(_) => {
            panic!("Expected Standard guard for distributed lock");
        }
    }

    // Test 5: Verify health check shows 4 nodes
    let health = lock.get_health().await;
    assert_eq!(health.node_id, "grpc-4-node");
    assert_eq!(health.total_nodes, 4);
    assert_eq!(health.connected_nodes, 4);
    assert_eq!(health.status, rustfs_lock::types::HealthStatus::Healthy);

    // Cleanup
    drop(guard_b);

    // Shutdown servers
    handle1.abort();
    handle2.abort();
    handle3.abort();
    handle4.abort();
}
