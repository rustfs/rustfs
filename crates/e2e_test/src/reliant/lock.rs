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

use rustfs_lock::{create_namespace_lock, LockArgs, NamespaceLockManager};
use rustfs_protos::{node_service_time_out_client, proto_gen::node_service::GenerallyLockRequest};
use std::{error::Error, time::Duration};
use tokio::time::sleep;
use tonic::Request;

const CLUSTER_ADDR: &str = "http://localhost:9000";

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_unlock_rpc() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "1111".to_string(),
        resources: vec!["dandan".to_string()],
        owner: "dd".to_string(),
        source: "".to_string(),
        quorum: 3,
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
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_unlock_ns_lock() -> Result<(), Box<dyn Error>> {
    let ns_lock = create_namespace_lock("test".to_string(), true);

    let resources = vec!["foo".to_string()];
    let result = ns_lock.lock_batch(&resources, "dandan", Duration::from_secs(5)).await;
    match &result {
        Ok(success) => println!("Lock result: {}", success),
        Err(e) => println!("Lock error: {}", e),
    }
    let result = result?;
    assert!(result, "Lock should succeed, but got: {}", result);

    ns_lock.unlock_batch(&resources, "dandan").await?;
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_concurrent_lock_attempts() -> Result<(), Box<dyn Error>> {
    let ns_lock = create_namespace_lock("test".to_string(), true);
    let resource = vec!["concurrent_resource".to_string()];

    // First lock should succeed
    println!("Attempting first lock...");
    let result1 = ns_lock.lock_batch(&resource, "owner1", Duration::from_secs(5)).await?;
    println!("First lock result: {}", result1);
    assert!(result1, "First lock should succeed");

    // Second lock should fail (resource already locked)
    println!("Attempting second lock...");
    let result2 = ns_lock.lock_batch(&resource, "owner2", Duration::from_secs(1)).await?;
    println!("Second lock result: {}", result2);
    assert!(!result2, "Second lock should fail");

    // Unlock by first owner
    println!("Unlocking first lock...");
    ns_lock.unlock_batch(&resource, "owner1").await?;
    println!("First lock unlocked");

    // Now second owner should be able to lock
    println!("Attempting third lock...");
    let result3 = ns_lock.lock_batch(&resource, "owner2", Duration::from_secs(5)).await?;
    println!("Third lock result: {}", result3);
    assert!(result3, "Lock should succeed after unlock");

    // Clean up
    println!("Cleaning up...");
    ns_lock.unlock_batch(&resource, "owner2").await?;
    println!("Test completed");

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_read_write_lock_compatibility() -> Result<(), Box<dyn Error>> {
    let ns_lock = create_namespace_lock("test_rw".to_string(), true);
    let resource = vec!["rw_resource".to_string()];

    // First read lock should succeed
    let result1 = ns_lock.rlock_batch(&resource, "reader1", Duration::from_secs(5)).await?;
    assert!(result1, "First read lock should succeed");

    // Second read lock should also succeed (read locks are compatible)
    let result2 = ns_lock.rlock_batch(&resource, "reader2", Duration::from_secs(5)).await?;
    assert!(result2, "Second read lock should succeed");

    // Write lock should fail (read locks are held)
    let result3 = ns_lock.lock_batch(&resource, "writer1", Duration::from_secs(1)).await?;
    assert!(!result3, "Write lock should fail when read locks are held");

    // Release read locks
    ns_lock.runlock_batch(&resource, "reader1").await?;
    ns_lock.runlock_batch(&resource, "reader2").await?;

    // Now write lock should succeed
    let result4 = ns_lock.lock_batch(&resource, "writer1", Duration::from_secs(5)).await?;
    assert!(result4, "Write lock should succeed after read locks released");

    // Clean up
    ns_lock.unlock_batch(&resource, "writer1").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_timeout() -> Result<(), Box<dyn Error>> {
    let ns_lock = create_namespace_lock("test_timeout".to_string(), true);
    let resource = vec!["timeout_resource".to_string()];

    // First lock with short timeout
    let result1 = ns_lock.lock_batch(&resource, "owner1", Duration::from_secs(2)).await?;
    assert!(result1, "First lock should succeed");

    // Wait for lock to expire
    sleep(Duration::from_secs(3)).await;

    // Second lock should succeed after timeout
    let result2 = ns_lock.lock_batch(&resource, "owner2", Duration::from_secs(5)).await?;
    assert!(result2, "Lock should succeed after timeout");

    // Clean up
    ns_lock.unlock_batch(&resource, "owner2").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_batch_lock_operations() -> Result<(), Box<dyn Error>> {
    let ns_lock = create_namespace_lock("test_batch".to_string(), true);
    let resources = vec![
        "batch_resource1".to_string(),
        "batch_resource2".to_string(),
        "batch_resource3".to_string(),
    ];

    // Lock all resources
    let result = ns_lock.lock_batch(&resources, "batch_owner", Duration::from_secs(5)).await?;
    assert!(result, "Batch lock should succeed");

    // Try to lock one of the resources with different owner - should fail
    let single_resource = vec!["batch_resource2".to_string()];
    let result2 = ns_lock.lock_batch(&single_resource, "other_owner", Duration::from_secs(1)).await?;
    assert!(!result2, "Lock should fail for already locked resource");

    // Unlock all resources
    ns_lock.unlock_batch(&resources, "batch_owner").await?;

    // Now should be able to lock single resource
    let result3 = ns_lock.lock_batch(&single_resource, "other_owner", Duration::from_secs(5)).await?;
    assert!(result3, "Lock should succeed after batch unlock");

    // Clean up
    ns_lock.unlock_batch(&single_resource, "other_owner").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_multiple_namespaces() -> Result<(), Box<dyn Error>> {
    let ns_lock1 = create_namespace_lock("namespace1".to_string(), true);
    let ns_lock2 = create_namespace_lock("namespace2".to_string(), true);
    let resource = vec!["shared_resource".to_string()];

    // Lock same resource in different namespaces - both should succeed
    let result1 = ns_lock1.lock_batch(&resource, "owner1", Duration::from_secs(5)).await?;
    assert!(result1, "Lock in namespace1 should succeed");

    let result2 = ns_lock2.lock_batch(&resource, "owner2", Duration::from_secs(5)).await?;
    assert!(result2, "Lock in namespace2 should succeed");

    // Clean up
    ns_lock1.unlock_batch(&resource, "owner1").await?;
    ns_lock2.unlock_batch(&resource, "owner2").await?;

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_rpc_read_lock() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "2222".to_string(),
        resources: vec!["read_resource".to_string()],
        owner: "reader1".to_string(),
        source: "".to_string(),
        quorum: 3,
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
    let args2 = LockArgs {
        uid: "3333".to_string(),
        resources: vec!["read_resource".to_string()],
        owner: "reader2".to_string(),
        source: "".to_string(),
        quorum: 3,
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
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_lock_refresh() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "4444".to_string(),
        resources: vec!["refresh_resource".to_string()],
        owner: "refresh_owner".to_string(),
        source: "".to_string(),
        quorum: 3,
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
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_force_unlock() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "5555".to_string(),
        resources: vec!["force_resource".to_string()],
        owner: "force_owner".to_string(),
        source: "".to_string(),
        quorum: 3,
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
    let force_args = LockArgs {
        uid: "5555".to_string(),
        resources: vec!["force_resource".to_string()],
        owner: "admin".to_string(),
        source: "".to_string(),
        quorum: 3,
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
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_concurrent_rpc_lock_attempts() -> Result<(), Box<dyn Error>> {
    let args1 = LockArgs {
        uid: "concurrent_test_1".to_string(),
        resources: vec!["concurrent_rpc_resource".to_string()],
        owner: "owner1".to_string(),
        source: "".to_string(),
        quorum: 3,
    };
    let args1_str = serde_json::to_string(&args1)?;

    let args2 = LockArgs {
        uid: "concurrent_test_2".to_string(),
        resources: vec!["concurrent_rpc_resource".to_string()],
        owner: "owner2".to_string(),
        source: "".to_string(),
        quorum: 3,
    };
    let args2_str = serde_json::to_string(&args2)?;

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    
    // First lock should succeed
    println!("Attempting first RPC lock...");
    let request1 = Request::new(GenerallyLockRequest { args: args1_str.clone() });
    let response1 = client.lock(request1).await?.into_inner();
    println!("First RPC lock response: success={}, error={:?}", response1.success, response1.error_info);
    assert!(response1.success && response1.error_info.is_none(), "First lock should succeed");

    // Second lock should fail (resource already locked)
    println!("Attempting second RPC lock...");
    let request2 = Request::new(GenerallyLockRequest { args: args2_str });
    let response2 = client.lock(request2).await?.into_inner();
    println!("Second RPC lock response: success={}, error={:?}", response2.success, response2.error_info);
    assert!(!response2.success, "Second lock should fail");

    // Unlock by first owner
    println!("Unlocking first RPC lock...");
    let unlock_request = Request::new(GenerallyLockRequest { args: args1_str });
    let unlock_response = client.un_lock(unlock_request).await?.into_inner();
    println!("Unlock response: success={}, error={:?}", unlock_response.success, unlock_response.error_info);
    assert!(unlock_response.success && unlock_response.error_info.is_none(), "Unlock should succeed");

    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_global_lock_map_sharing() -> Result<(), Box<dyn Error>> {
    // Create two separate NsLockMap instances
    let lock_map1 = rustfs_lock::NsLockMap::new(false, None);
    let lock_map2 = rustfs_lock::NsLockMap::new(false, None);
    
    let resource = vec!["global_test_resource".to_string()];
    
    // First instance acquires lock
    println!("First lock map attempting to acquire lock...");
    let result1 = lock_map1.lock_batch_with_ttl(&resource, "owner1", std::time::Duration::from_secs(5), Some(std::time::Duration::from_secs(30))).await?;
    println!("First lock result: {}", result1);
    assert!(result1, "First lock should succeed");
    
    // Second instance should fail to acquire the same lock
    println!("Second lock map attempting to acquire lock...");
    let result2 = lock_map2.lock_batch_with_ttl(&resource, "owner2", std::time::Duration::from_secs(1), Some(std::time::Duration::from_secs(30))).await?;
    println!("Second lock result: {}", result2);
    assert!(!result2, "Second lock should fail because resource is already locked");
    
    // Release lock from first instance
    println!("First lock map releasing lock...");
    lock_map1.unlock_batch(&resource, "owner1").await?;
    
    // Now second instance should be able to acquire lock
    println!("Second lock map attempting to acquire lock again...");
    let result3 = lock_map2.lock_batch_with_ttl(&resource, "owner2", std::time::Duration::from_secs(5), Some(std::time::Duration::from_secs(30))).await?;
    println!("Third lock result: {}", result3);
    assert!(result3, "Lock should succeed after first lock is released");
    
    // Clean up
    lock_map2.unlock_batch(&resource, "owner2").await?;
    
    Ok(())
}

#[tokio::test]
#[ignore = "requires running RustFS server at localhost:9000"]
async fn test_sequential_rpc_lock_calls() -> Result<(), Box<dyn Error>> {
    let args = LockArgs {
        uid: "sequential_test".to_string(),
        resources: vec!["sequential_resource".to_string()],
        owner: "sequential_owner".to_string(),
        source: "".to_string(),
        quorum: 3,
    };
    let args_str = serde_json::to_string(&args)?;

    let mut client = node_service_time_out_client(&CLUSTER_ADDR.to_string()).await?;
    
    // First lock should succeed
    println!("First lock attempt...");
    let request1 = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response1 = client.lock(request1).await?.into_inner();
    println!("First response: success={}, error={:?}", response1.success, response1.error_info);
    assert!(response1.success && response1.error_info.is_none(), "First lock should succeed");

    // Second lock with same owner should also succeed (re-entrant)
    println!("Second lock attempt with same owner...");
    let request2 = Request::new(GenerallyLockRequest { args: args_str.clone() });
    let response2 = client.lock(request2).await?.into_inner();
    println!("Second response: success={}, error={:?}", response2.success, response2.error_info);
    
    // Different owner should fail
    let args2 = LockArgs {
        uid: "sequential_test_2".to_string(),
        resources: vec!["sequential_resource".to_string()],
        owner: "different_owner".to_string(),
        source: "".to_string(),
        quorum: 3,
    };
    let args2_str = serde_json::to_string(&args2)?;
    
    println!("Third lock attempt with different owner...");
    let request3 = Request::new(GenerallyLockRequest { args: args2_str });
    let response3 = client.lock(request3).await?.into_inner();
    println!("Third response: success={}, error={:?}", response3.success, response3.error_info);
    assert!(!response3.success, "Lock with different owner should fail");

    // Unlock
    println!("Unlocking...");
    let unlock_request = Request::new(GenerallyLockRequest { args: args_str });
    let unlock_response = client.un_lock(unlock_request).await?.into_inner();
    println!("Unlock response: success={}, error={:?}", unlock_response.success, unlock_response.error_info);

    Ok(())
}
