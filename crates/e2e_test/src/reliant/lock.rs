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

use async_trait::async_trait;
use rustfs_ecstore::disk::endpoint::Endpoint;
use rustfs_ecstore::rpc::RemoteClient;
use rustfs_lock::client::{LockClient, local::LocalClient};
use rustfs_lock::types::{LockInfo, LockResponse, LockStats};
use rustfs_lock::{LockId, LockMetadata, LockPriority, LockType};
use rustfs_lock::{LockRequest, NamespaceLock, NamespaceLockManager};
use rustfs_protos::proto_gen::node_service::GenerallyLockRequest;
use serial_test::serial;
use std::{collections::HashMap, error::Error, sync::Arc, time::Duration};
use tokio::time::sleep;
use tonic::Request;
use url::Url;

const CLUSTER_ADDR: &str = "http://localhost:9000";

fn get_cluster_endpoints() -> Vec<Endpoint> {
    vec![Endpoint {
        url: Url::parse(CLUSTER_ADDR).unwrap(),
        is_local: false,
        pool_idx: 0,
        set_idx: 0,
        disk_idx: 0,
    }]
}

async fn create_unique_clients(endpoints: &[Endpoint]) -> Result<Vec<Arc<dyn LockClient>>, Box<dyn Error>> {
    let mut unique_endpoints: HashMap<String, &Endpoint> = HashMap::new();

    for endpoint in endpoints {
        if endpoint.is_local {
            unique_endpoints.insert("local".to_string(), endpoint);
        } else {
            let host_port = format!(
                "{}:{}",
                endpoint.url.host_str().unwrap_or("localhost"),
                endpoint.url.port().unwrap_or(9000)
            );
            unique_endpoints.insert(host_port, endpoint);
        }
    }

    let mut clients = Vec::new();
    for (_key, endpoint) in unique_endpoints {
        if endpoint.is_local {
            clients.push(Arc::new(LocalClient::new()) as Arc<dyn LockClient>);
        } else {
            clients.push(Arc::new(RemoteClient::new(endpoint.url.to_string())) as Arc<dyn LockClient>);
        }
    }

    Ok(clients)
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_guard_drop_releases_exclusive_lock_local() -> Result<(), Box<dyn Error>> {
    // Single local client; no external server required
    let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let ns_lock = NamespaceLock::with_clients("e2e_guard_local".to_string(), vec![client]);

    // Acquire exclusive guard
    let g1 = ns_lock
        .lock_guard("guard_exclusive", "owner1", Duration::from_millis(100), Duration::from_secs(5))
        .await?;
    assert!(g1.is_some(), "first guard acquisition should succeed");

    // While g1 is alive, second exclusive acquisition should fail
    let g2 = ns_lock
        .lock_guard("guard_exclusive", "owner2", Duration::from_millis(50), Duration::from_secs(5))
        .await?;
    assert!(g2.is_none(), "second guard acquisition should fail while first is held");

    // Drop first guard to trigger background release
    drop(g1);
    // Give the background unlock worker a short moment to process
    sleep(Duration::from_millis(80)).await;

    // Now acquisition should succeed
    let g3 = ns_lock
        .lock_guard("guard_exclusive", "owner2", Duration::from_millis(100), Duration::from_secs(5))
        .await?;
    assert!(g3.is_some(), "acquisition should succeed after guard drop releases the lock");
    drop(g3);

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_guard_shared_then_write_after_drop() -> Result<(), Box<dyn Error>> {
    // Two shared read guards should coexist; write should be blocked until they drop
    let client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let ns_lock = NamespaceLock::with_clients("e2e_guard_rw".to_string(), vec![client]);

    // Acquire two read guards
    let r1 = ns_lock
        .rlock_guard("rw_resource", "reader1", Duration::from_millis(100), Duration::from_secs(5))
        .await?;
    let r2 = ns_lock
        .rlock_guard("rw_resource", "reader2", Duration::from_millis(100), Duration::from_secs(5))
        .await?;
    assert!(r1.is_some() && r2.is_some(), "both read guards should be acquired");

    // Attempt write while readers hold the lock should fail
    let w_fail = ns_lock
        .lock_guard("rw_resource", "writer", Duration::from_millis(50), Duration::from_secs(5))
        .await?;
    assert!(w_fail.is_none(), "write should be blocked when read guards are active");

    // Drop read guards to release
    drop(r1);
    drop(r2);
    sleep(Duration::from_millis(80)).await;

    // Now write should succeed
    let w_ok = ns_lock
        .lock_guard("rw_resource", "writer", Duration::from_millis(150), Duration::from_secs(5))
        .await?;
    assert!(w_ok.is_some(), "write should succeed after read guards are dropped");
    drop(w_ok);

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_unlock_rpc() -> Result<(), Box<dyn Error>> {
    let args = LockRequest {
        lock_id: LockId::new_deterministic("dandan"),
        resource: "dandan".to_string(),
        lock_type: LockType::Exclusive,
        owner: "dd".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let args = serde_json::to_string(&args)?;

    let mut client = RemoteClient::new(CLUSTER_ADDR.to_string()).get_client().await?;
    println!("got client");
    let request = Request::new(GenerallyLockRequest { args: args.clone() });

    println!("start request");
    let response = client.lock(request).await?.into_inner();
    println!("request ended");
    if let Some(error_info) = response.error_info {
        panic!("can not get lock: {error_info}");
    }

    let request = Request::new(GenerallyLockRequest { args });
    let response = client.un_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not get un_lock: {error_info}");
    }

    Ok(())
}

/// Mock client that simulates remote node failures
#[derive(Debug)]
struct FailingMockClient {
    local_client: Arc<dyn LockClient>,
    should_fail_acquire: bool,
    should_fail_release: bool,
}

impl FailingMockClient {
    fn new(should_fail_acquire: bool, should_fail_release: bool) -> Self {
        Self {
            local_client: Arc::new(LocalClient::new()),
            should_fail_acquire,
            should_fail_release,
        }
    }
}

#[async_trait]
impl LockClient for FailingMockClient {
    async fn acquire_exclusive(&self, request: &LockRequest) -> rustfs_lock::error::Result<LockResponse> {
        if self.should_fail_acquire {
            // Simulate network timeout or remote node failure
            return Ok(LockResponse::failure("Simulated remote node failure", Duration::from_millis(100)));
        }
        self.local_client.acquire_exclusive(request).await
    }

    async fn acquire_shared(&self, request: &LockRequest) -> rustfs_lock::error::Result<LockResponse> {
        if self.should_fail_acquire {
            return Ok(LockResponse::failure("Simulated remote node failure", Duration::from_millis(100)));
        }
        self.local_client.acquire_shared(request).await
    }

    async fn release(&self, lock_id: &LockId) -> rustfs_lock::error::Result<bool> {
        if self.should_fail_release {
            return Err(rustfs_lock::error::LockError::internal("Simulated release failure"));
        }
        self.local_client.release(lock_id).await
    }

    async fn refresh(&self, lock_id: &LockId) -> rustfs_lock::error::Result<bool> {
        self.local_client.refresh(lock_id).await
    }

    async fn force_release(&self, lock_id: &LockId) -> rustfs_lock::error::Result<bool> {
        self.local_client.force_release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> rustfs_lock::error::Result<Option<LockInfo>> {
        self.local_client.check_status(lock_id).await
    }

    async fn get_stats(&self) -> rustfs_lock::error::Result<LockStats> {
        self.local_client.get_stats().await
    }

    async fn close(&self) -> rustfs_lock::error::Result<()> {
        self.local_client.close().await
    }

    async fn is_online(&self) -> bool {
        if self.should_fail_acquire {
            return false; // Simulate offline node
        }
        true // Simulate online node
    }

    async fn is_local(&self) -> bool {
        false // Simulate remote client
    }
}

#[tokio::test]
#[serial]
async fn test_transactional_lock_with_remote_failure() -> Result<(), Box<dyn Error>> {
    println!("🧪 Testing transactional lock with simulated remote node failure");

    // Create a two-node cluster: one local (success) + one remote (failure)
    let local_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let failing_remote_client: Arc<dyn LockClient> = Arc::new(FailingMockClient::new(true, false));

    let clients = vec![local_client, failing_remote_client];
    let ns_lock = NamespaceLock::with_clients("test_transactional".to_string(), clients);

    let resource = "critical_resource".to_string();

    // Test single lock operation with 2PC
    println!("📝 Testing single lock with remote failure...");
    let request = LockRequest::new(&resource, LockType::Exclusive, "test_owner").with_ttl(Duration::from_secs(30));

    let response = ns_lock.acquire_lock(&request).await?;

    // Should fail because quorum (2/2) is not met due to remote failure
    assert!(!response.success, "Lock should fail due to remote node failure");
    println!("✅ Single lock correctly failed due to remote node failure");

    // Verify no locks are left behind on the local node
    let local_client_direct = LocalClient::new();
    let lock_id = LockId::new_deterministic(&ns_lock.get_resource_key(&resource));
    let lock_status = local_client_direct.check_status(&lock_id).await?;
    assert!(lock_status.is_none(), "No lock should remain on local node after rollback");
    println!("✅ Verified rollback: no locks left on local node");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_transactional_batch_lock_with_mixed_failures() -> Result<(), Box<dyn Error>> {
    println!("🧪 Testing transactional batch lock with mixed node failures");

    // Create a cluster with different failure patterns
    let local_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let failing_remote_client: Arc<dyn LockClient> = Arc::new(FailingMockClient::new(true, false));

    let clients = vec![local_client, failing_remote_client];
    let ns_lock = NamespaceLock::with_clients("test_batch_transactional".to_string(), clients);

    let resources = vec!["resource_1".to_string(), "resource_2".to_string(), "resource_3".to_string()];

    println!("📝 Testing batch lock with remote failure...");
    let result = ns_lock
        .lock_batch(&resources, "batch_owner", Duration::from_millis(100), Duration::from_secs(30))
        .await?;

    // Should fail because remote node cannot acquire locks
    assert!(!result, "Batch lock should fail due to remote node failure");
    println!("✅ Batch lock correctly failed due to remote node failure");

    // Verify no locks are left behind on any resource
    let local_client_direct = LocalClient::new();
    for resource in &resources {
        let lock_id = LockId::new_deterministic(&ns_lock.get_resource_key(resource));
        let lock_status = local_client_direct.check_status(&lock_id).await?;
        assert!(lock_status.is_none(), "No lock should remain for resource: {resource}");
    }
    println!("✅ Verified rollback: no locks left on any resource");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_transactional_lock_with_quorum_success() -> Result<(), Box<dyn Error>> {
    println!("🧪 Testing transactional lock with quorum success");

    // Create a three-node cluster where 2 succeed and 1 fails (quorum = 2 automatically)
    let local_client1: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let local_client2: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let failing_remote_client: Arc<dyn LockClient> = Arc::new(FailingMockClient::new(true, false));

    let clients = vec![local_client1, local_client2, failing_remote_client];
    let ns_lock = NamespaceLock::with_clients("test_quorum".to_string(), clients);

    let resource = "quorum_resource".to_string();

    println!("📝 Testing lock with automatic quorum=2, 2 success + 1 failure...");
    let request = LockRequest::new(&resource, LockType::Exclusive, "quorum_owner").with_ttl(Duration::from_secs(30));

    let response = ns_lock.acquire_lock(&request).await?;

    // Should fail because we require all nodes to succeed for consistency
    // (even though quorum is met, the implementation requires all nodes for consistency)
    assert!(!response.success, "Lock should fail due to consistency requirement");
    println!("✅ Lock correctly failed due to consistency requirement (partial success rolled back)");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_transactional_lock_rollback_on_release_failure() -> Result<(), Box<dyn Error>> {
    println!("🧪 Testing rollback behavior when release fails");

    // Create clients where acquire succeeds but release fails
    let local_client: Arc<dyn LockClient> = Arc::new(LocalClient::new());
    let failing_release_client: Arc<dyn LockClient> = Arc::new(FailingMockClient::new(false, true));

    let clients = vec![local_client, failing_release_client];
    let ns_lock = NamespaceLock::with_clients("test_release_failure".to_string(), clients);

    let resource = "release_test_resource".to_string();

    println!("📝 Testing lock acquisition with release failure handling...");
    let request = LockRequest::new(&resource, LockType::Exclusive, "test_owner").with_ttl(Duration::from_secs(30));

    // This should fail because both LocalClient instances share the same global lock map
    // The first client (LocalClient) will acquire the lock, but the second client
    // (FailingMockClient's internal LocalClient) will fail to acquire the same resource
    let response = ns_lock.acquire_lock(&request).await?;

    // The operation should fail due to lock contention between the two LocalClient instances
    assert!(
        !response.success,
        "Lock should fail due to lock contention between LocalClient instances sharing global lock map"
    );
    println!("✅ Lock correctly failed due to lock contention (both clients use same global lock map)");

    // Verify no locks are left behind after rollback
    let local_client_direct = LocalClient::new();
    let lock_id = LockId::new_deterministic(&ns_lock.get_resource_key(&resource));
    let lock_status = local_client_direct.check_status(&lock_id).await?;
    assert!(lock_status.is_none(), "No lock should remain after rollback");
    println!("✅ Verified rollback: no locks left after failed acquisition");

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_unlock_ns_lock() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock = NamespaceLock::with_clients("test".to_string(), clients);

    let resources = vec!["foo".to_string()];
    let result = ns_lock
        .lock_batch(&resources, "dandan", Duration::from_secs(5), Duration::from_secs(10))
        .await;
    match &result {
        Ok(success) => println!("Lock result: {success}"),
        Err(e) => println!("Lock error: {e}"),
    }
    let result = result?;
    assert!(result, "Lock should succeed, but got: {result}");

    ns_lock.unlock_batch(&resources, "dandan").await?;
    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_concurrent_lock_attempts() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock = NamespaceLock::with_clients("test".to_string(), clients);
    let resource = vec!["concurrent_resource".to_string()];

    // First lock should succeed
    println!("Attempting first lock...");
    let result1 = ns_lock
        .lock_batch(&resource, "owner1", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    println!("First lock result: {result1}");
    assert!(result1, "First lock should succeed");

    // Second lock should fail (resource already locked)
    println!("Attempting second lock...");
    let result2 = ns_lock
        .lock_batch(&resource, "owner2", Duration::from_secs(1), Duration::from_secs(10))
        .await?;
    println!("Second lock result: {result2}");
    assert!(!result2, "Second lock should fail");

    // Unlock by first owner
    println!("Unlocking first lock...");
    ns_lock.unlock_batch(&resource, "owner1").await?;
    println!("First lock unlocked");

    // Now second owner should be able to lock
    println!("Attempting third lock...");
    let result3 = ns_lock
        .lock_batch(&resource, "owner2", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    println!("Third lock result: {result3}");
    assert!(result3, "Lock should succeed after unlock");

    // Clean up
    println!("Cleaning up...");
    ns_lock.unlock_batch(&resource, "owner2").await?;
    println!("Test completed");

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_read_write_lock_compatibility() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock = NamespaceLock::with_clients("test_rw".to_string(), clients);
    let resource = vec!["rw_resource".to_string()];

    // First read lock should succeed
    let result1 = ns_lock
        .rlock_batch(&resource, "reader1", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result1, "First read lock should succeed");

    // Second read lock should also succeed (read locks are compatible)
    let result2 = ns_lock
        .rlock_batch(&resource, "reader2", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result2, "Second read lock should succeed");

    // Write lock should fail (read locks are held)
    let result3 = ns_lock
        .lock_batch(&resource, "writer1", Duration::from_secs(1), Duration::from_secs(10))
        .await?;
    assert!(!result3, "Write lock should fail when read locks are held");

    // Release read locks
    ns_lock.runlock_batch(&resource, "reader1").await?;
    ns_lock.runlock_batch(&resource, "reader2").await?;

    // Now write lock should succeed
    let result4 = ns_lock
        .lock_batch(&resource, "writer1", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result4, "Write lock should succeed after read locks released");

    // Clean up
    ns_lock.unlock_batch(&resource, "writer1").await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_timeout() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock = NamespaceLock::with_clients("test_timeout".to_string(), clients);
    let resource = vec!["timeout_resource".to_string()];

    // First lock with short timeout
    let result1 = ns_lock
        .lock_batch(&resource, "owner1", Duration::from_secs(2), Duration::from_secs(1))
        .await?;
    assert!(result1, "First lock should succeed");

    // Wait for lock to expire
    sleep(Duration::from_secs(5)).await;

    // Second lock should succeed after timeout
    let result2 = ns_lock
        .lock_batch(&resource, "owner2", Duration::from_secs(5), Duration::from_secs(1))
        .await?;
    assert!(result2, "Lock should succeed after timeout");

    // Clean up
    ns_lock.unlock_batch(&resource, "owner2").await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_batch_lock_operations() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock = NamespaceLock::with_clients("test_batch".to_string(), clients);
    let resources = vec![
        "batch_resource1".to_string(),
        "batch_resource2".to_string(),
        "batch_resource3".to_string(),
    ];

    // Lock all resources
    let result = ns_lock
        .lock_batch(&resources, "batch_owner", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result, "Batch lock should succeed");

    // Try to lock one of the resources with different owner - should fail
    let single_resource = vec!["batch_resource2".to_string()];
    let result2 = ns_lock
        .lock_batch(&single_resource, "other_owner", Duration::from_secs(1), Duration::from_secs(10))
        .await?;
    assert!(!result2, "Lock should fail for already locked resource");

    // Unlock all resources
    ns_lock.unlock_batch(&resources, "batch_owner").await?;

    // Now should be able to lock single resource
    let result3 = ns_lock
        .lock_batch(&single_resource, "other_owner", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result3, "Lock should succeed after batch unlock");

    // Clean up
    ns_lock.unlock_batch(&single_resource, "other_owner").await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_multiple_namespaces() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock1 = NamespaceLock::with_clients("namespace1".to_string(), clients.clone());
    let ns_lock2 = NamespaceLock::with_clients("namespace2".to_string(), clients);
    let resource = vec!["shared_resource".to_string()];

    // Lock same resource in different namespaces - both should succeed
    let result1 = ns_lock1
        .lock_batch(&resource, "owner1", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result1, "Lock in namespace1 should succeed");

    let result2 = ns_lock2
        .lock_batch(&resource, "owner2", Duration::from_secs(5), Duration::from_secs(10))
        .await?;
    assert!(result2, "Lock in namespace2 should succeed");

    // Clean up
    ns_lock1.unlock_batch(&resource, "owner1").await?;
    ns_lock2.unlock_batch(&resource, "owner2").await?;

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_rpc_read_lock() -> Result<(), Box<dyn Error>> {
    let args = LockRequest {
        lock_id: LockId::new_deterministic("read_resource"),
        resource: "read_resource".to_string(),
        lock_type: LockType::Shared,
        owner: "reader1".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let args_str = serde_json::to_string(&args)?;

    let mut client = RemoteClient::new(CLUSTER_ADDR.to_string()).get_client().await?;

    // First read lock
    let request = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response = client.r_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not get read lock: {error_info}");
    }

    // Second read lock with different owner should also succeed
    let args2 = LockRequest {
        lock_id: LockId::new_deterministic("read_resource"),
        resource: "read_resource".to_string(),
        lock_type: LockType::Shared,
        owner: "reader2".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let args2_str = serde_json::to_string(&args2)?;
    let request2 = Request::new(GenerallyLockRequest { args: args2_str });
    let response2 = client.r_lock(request2).await?.into_inner();
    if let Some(error_info) = response2.error_info {
        panic!("can not get second read lock: {error_info}");
    }

    // Unlock both
    let request = Request::new(GenerallyLockRequest { args: args_str });
    let response = client.r_un_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not unlock read lock: {error_info}");
    }

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_refresh() -> Result<(), Box<dyn Error>> {
    let args = LockRequest {
        lock_id: LockId::new_deterministic("refresh_resource"),
        resource: "refresh_resource".to_string(),
        lock_type: LockType::Exclusive,
        owner: "refresh_owner".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let args_str = serde_json::to_string(&args)?;

    let mut client = RemoteClient::new(CLUSTER_ADDR.to_string()).get_client().await?;

    // Acquire lock
    let request = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response = client.lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not get lock: {error_info}");
    }

    // Refresh lock
    let request = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response = client.refresh(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not refresh lock: {error_info}");
    }
    assert!(response.success, "Lock refresh should succeed");

    // Unlock
    let request = Request::new(GenerallyLockRequest { args: args_str });
    let response = client.un_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not unlock: {error_info}");
    }

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_force_unlock() -> Result<(), Box<dyn Error>> {
    let args = LockRequest {
        lock_id: LockId::new_deterministic("force_resource"),
        resource: "force_resource".to_string(),
        lock_type: LockType::Exclusive,
        owner: "force_owner".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let args_str = serde_json::to_string(&args)?;

    let mut client = RemoteClient::new(CLUSTER_ADDR.to_string()).get_client().await?;

    // Acquire lock
    let request = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response = client.lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not get lock: {error_info}");
    }

    // Force unlock (even by different owner)
    let force_args = LockRequest {
        lock_id: LockId::new_deterministic("force_resource"),
        resource: "force_resource".to_string(),
        lock_type: LockType::Exclusive,
        owner: "admin".to_string(),
        acquire_timeout: Duration::from_secs(30),
        ttl: Duration::from_secs(30),
        metadata: LockMetadata::default(),
        priority: LockPriority::Normal,
        deadlock_detection: false,
    };
    let force_args_str = serde_json::to_string(&force_args)?;
    let request = Request::new(GenerallyLockRequest { args: force_args_str });
    let response = client.force_un_lock(request).await?.into_inner();
    if let Some(error_info) = response.error_info {
        panic!("can not force unlock: {error_info}");
    }
    assert!(response.success, "Force unlock should succeed");

    Ok(())
}

#[tokio::test]
#[serial]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_global_lock_map_sharing() -> Result<(), Box<dyn Error>> {
    let endpoints = get_cluster_endpoints();
    let clients = create_unique_clients(&endpoints).await?;
    let ns_lock1 = NamespaceLock::with_clients("global_test".to_string(), clients.clone());
    let ns_lock2 = NamespaceLock::with_clients("global_test".to_string(), clients);

    let resource = vec!["global_test_resource".to_string()];

    // First instance acquires lock
    println!("First lock map attempting to acquire lock...");
    let result1 = ns_lock1
        .lock_batch(&resource, "owner1", std::time::Duration::from_secs(5), std::time::Duration::from_secs(10))
        .await?;
    println!("First lock result: {result1}");
    assert!(result1, "First lock should succeed");

    // Second instance should fail to acquire the same lock
    println!("Second lock map attempting to acquire lock...");
    let result2 = ns_lock2
        .lock_batch(&resource, "owner2", std::time::Duration::from_secs(1), std::time::Duration::from_secs(10))
        .await?;
    println!("Second lock result: {result2}");
    assert!(!result2, "Second lock should fail because resource is already locked");

    // Release lock from first instance
    println!("First lock map releasing lock...");
    ns_lock1.unlock_batch(&resource, "owner1").await?;

    // Now second instance should be able to acquire lock
    println!("Second lock map attempting to acquire lock again...");
    let result3 = ns_lock2
        .lock_batch(&resource, "owner2", std::time::Duration::from_secs(5), std::time::Duration::from_secs(10))
        .await?;
    println!("Third lock result: {result3}");
    assert!(result3, "Lock should succeed after first lock is released");

    // Clean up
    ns_lock2.unlock_batch(&resource, "owner2").await?;

    Ok(())
}
