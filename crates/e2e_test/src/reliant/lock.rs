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

use rustfs_ecstore::{disk::endpoint::Endpoint, lock_utils::create_unique_clients};
use rustfs_lock::{LockId, LockMetadata, LockPriority, LockType};
use rustfs_lock::{LockRequest, NamespaceLock, NamespaceLockManager};
use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
use serial_test::serial;
use std::{error::Error, time::Duration};
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

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
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

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;

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

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;

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

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;

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
