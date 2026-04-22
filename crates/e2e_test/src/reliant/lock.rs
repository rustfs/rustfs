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
use rustfs_lock::client::{LockClient, local::LocalClient};
use rustfs_lock::{
    GlobalLockManager, LockError, LockInfo, LockRequest, LockResponse, LockStats, LockType, NamespaceLock, ObjectKey,
};
use std::sync::Arc;
use std::time::Duration;

fn test_resource() -> ObjectKey {
    ObjectKey {
        bucket: Arc::from("test-bucket"),
        object: Arc::from("test-object"),
        version: None,
    }
}

#[derive(Debug, Default)]
struct FailingClient;

#[async_trait::async_trait]
impl rustfs_lock::LockClient for FailingClient {
    async fn acquire_lock(&self, _request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
        Err(LockError::internal("simulated gRPC node failure"))
    }

    async fn release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
        Ok(false)
    }

    async fn refresh(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
        Ok(false)
    }

    async fn force_release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
        Ok(false)
    }

    async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<LockInfo>> {
        Ok(None)
    }

    async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn close(&self) -> rustfs_lock::Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        false
    }

    async fn is_local(&self) -> bool {
        false
    }
}

async fn failing_grpc_client() -> (Arc<dyn rustfs_lock::LockClient>, tokio::task::JoinHandle<()>) {
    let failing_client: Arc<dyn rustfs_lock::LockClient> = Arc::new(FailingClient);
    let (addr, handle) = spawn_lock_server(failing_client)
        .await
        .expect("Failed to spawn failing gRPC lock server");
    (Arc::new(GrpcLockClient::new(addr)), handle)
}

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

    let resource = test_resource();

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

#[tokio::test]
async fn test_distributed_lock_2_nodes_grpc_read_survives_failed_node() {
    let manager = Arc::new(GlobalLockManager::new());
    let local_client: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager));

    let (addr, handle) = spawn_lock_server(local_client).await.expect("Failed to spawn server");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let grpc_client_ok: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr));
    let (grpc_client_bad, failing_handle) = failing_grpc_client().await;
    let lock = NamespaceLock::with_clients_and_quorum("grpc-2-node".to_string(), vec![grpc_client_ok, grpc_client_bad], 2);
    let resource = test_resource();

    let guard = lock
        .get_read_lock(resource.clone(), "owner-a", Duration::from_secs(2))
        .await
        .expect("Read lock should succeed with one healthy node in a two-node gRPC cluster");

    match guard {
        rustfs_lock::NamespaceLockGuard::Standard(_) => {}
        rustfs_lock::NamespaceLockGuard::Fast(_) => panic!("Expected Standard guard for distributed lock"),
    }

    let err = lock
        .get_write_lock(resource, "owner-a", Duration::from_secs(2))
        .await
        .expect_err("Write lock should fail with one healthy node in a two-node gRPC cluster");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "Error should be quorum related, got: {}",
        err
    );

    handle.abort();
    failing_handle.abort();
}

#[tokio::test]
async fn test_grpc_lock_client_batch_acquire_and_release() {
    let manager = Arc::new(GlobalLockManager::new());
    let local_client: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager));

    let (addr, handle) = spawn_lock_server(local_client).await.expect("Failed to spawn server");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let grpc_client = GrpcLockClient::new(addr);
    let requests = vec![
        LockRequest::new(test_resource(), LockType::Exclusive, "owner-a").with_acquire_timeout(Duration::from_secs(2)),
        LockRequest::new(
            ObjectKey {
                bucket: Arc::from("test-bucket"),
                object: Arc::from("test-object-2"),
                version: None,
            },
            LockType::Exclusive,
            "owner-a",
        )
        .with_acquire_timeout(Duration::from_secs(2)),
    ];

    let responses = grpc_client
        .acquire_locks_batch(&requests)
        .await
        .expect("batch acquire should succeed");
    assert_eq!(responses.len(), requests.len());
    assert!(responses.iter().all(|response| response.success));

    let lock_ids = responses
        .iter()
        .map(|response| {
            response
                .lock_info
                .as_ref()
                .expect("batch response should include lock info")
                .id
                .clone()
        })
        .collect::<Vec<_>>();
    let released = grpc_client
        .release_locks_batch(&lock_ids)
        .await
        .expect("batch release should succeed");
    assert_eq!(released, vec![true, true]);

    handle.abort();
}

#[tokio::test]
async fn test_grpc_lock_client_uses_request_lock_id_and_reports_missing_unlock() {
    let manager = Arc::new(GlobalLockManager::new());
    let local_client: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager));

    let (addr, handle) = spawn_lock_server(local_client).await.expect("Failed to spawn server");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let grpc_client = GrpcLockClient::new(addr);
    let request = LockRequest::new(test_resource(), LockType::Exclusive, "owner-a").with_acquire_timeout(Duration::from_secs(2));

    let response = grpc_client.acquire_lock(&request).await.expect("gRPC acquire should succeed");
    let lock_info = response.lock_info.expect("gRPC acquire should include lock info");
    assert_eq!(lock_info.id, request.lock_id);

    assert!(
        grpc_client
            .release(&request.lock_id)
            .await
            .expect("gRPC release should succeed"),
        "release should find the request lock id"
    );

    let missing_release = grpc_client
        .release(&request.lock_id)
        .await
        .expect_err("second release should report missing lock");
    assert!(
        missing_release.to_string().contains("lock not found for release"),
        "missing release should preserve server error, got: {missing_release}"
    );

    handle.abort();
}

#[tokio::test]
async fn test_distributed_lock_4_nodes_grpc_read_write_quorum_split_with_two_failed_nodes() {
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());
    let client1: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager1));
    let client2: Arc<dyn rustfs_lock::LockClient> = Arc::new(LocalClient::with_manager(manager2));

    let (addr1, handle1) = spawn_lock_server(client1).await.expect("Failed to spawn server 1");
    let (addr2, handle2) = spawn_lock_server(client2).await.expect("Failed to spawn server 2");
    tokio::time::sleep(Duration::from_millis(100)).await;

    let grpc_client1: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr1));
    let grpc_client2: Arc<dyn rustfs_lock::LockClient> = Arc::new(GrpcLockClient::new(addr2));
    let (grpc_client3, handle3) = failing_grpc_client().await;
    let (grpc_client4, handle4) = failing_grpc_client().await;

    let lock = NamespaceLock::with_clients(
        "grpc-4-node-partial".to_string(),
        vec![grpc_client1, grpc_client2, grpc_client3, grpc_client4],
    );
    let resource = test_resource();

    let mut read_guard = lock
        .get_read_lock(resource.clone(), "owner-a", Duration::from_secs(2))
        .await
        .expect("Read lock should succeed with two healthy nodes in a four-node gRPC cluster");
    assert!(read_guard.release(), "Read guard should release cleanly");

    let err = lock
        .get_write_lock(resource, "owner-b", Duration::from_secs(2))
        .await
        .expect_err("Write lock should fail when only two of four gRPC nodes are healthy");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "Error should be quorum related, got: {}",
        err
    );

    handle1.abort();
    handle2.abort();
    handle3.abort();
    handle4.abort();
}
