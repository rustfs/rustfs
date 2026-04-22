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
use crate::client::{ClientFactory, local::LocalClient};
use crate::types::LockType;
use crate::{GlobalLockManager, LockError, LockInfo, LockResponse, LockStats};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[derive(Debug, Default)]
struct FailingClient;

#[async_trait::async_trait]
impl crate::client::LockClient for FailingClient {
    async fn acquire_lock(&self, _request: &LockRequest) -> crate::Result<LockResponse> {
        Err(LockError::internal("simulated offline client"))
    }

    async fn release(&self, _lock_id: &LockId) -> crate::Result<bool> {
        Ok(false)
    }

    async fn refresh(&self, _lock_id: &LockId) -> crate::Result<bool> {
        Ok(false)
    }

    async fn force_release(&self, _lock_id: &LockId) -> crate::Result<bool> {
        Ok(false)
    }

    async fn check_status(&self, _lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
        Ok(None)
    }

    async fn get_stats(&self) -> crate::Result<LockStats> {
        Ok(LockStats::default())
    }

    async fn close(&self) -> crate::Result<()> {
        Ok(())
    }

    async fn is_online(&self) -> bool {
        false
    }

    async fn is_local(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct DelayedClient {
    inner: Arc<dyn crate::client::LockClient>,
    delay: Duration,
}

#[async_trait::async_trait]
impl crate::client::LockClient for DelayedClient {
    async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
        tokio::time::sleep(self.delay).await;
        self.inner.acquire_lock(request).await
    }

    async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.inner.release(lock_id).await
    }

    async fn refresh(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.inner.refresh(lock_id).await
    }

    async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.inner.force_release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
        self.inner.check_status(lock_id).await
    }

    async fn get_stats(&self) -> crate::Result<LockStats> {
        self.inner.get_stats().await
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }

    async fn is_online(&self) -> bool {
        self.inner.is_online().await
    }

    async fn is_local(&self) -> bool {
        self.inner.is_local().await
    }
}

#[derive(Debug)]
struct FlakyReleaseClient {
    inner: LocalClient,
    failed_releases_remaining: AtomicUsize,
    release_attempts: AtomicUsize,
}

impl FlakyReleaseClient {
    fn new(manager: Arc<GlobalLockManager>, failed_releases: usize) -> Self {
        Self {
            inner: LocalClient::with_manager(manager),
            failed_releases_remaining: AtomicUsize::new(failed_releases),
            release_attempts: AtomicUsize::new(0),
        }
    }

    fn release_attempts(&self) -> usize {
        self.release_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl crate::client::LockClient for FlakyReleaseClient {
    async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
        self.inner.acquire_lock(request).await
    }

    async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.release_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .failed_releases_remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
            .is_ok()
        {
            return Ok(false);
        }

        self.inner.release(lock_id).await
    }

    async fn refresh(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.inner.refresh(lock_id).await
    }

    async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.inner.force_release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
        self.inner.check_status(lock_id).await
    }

    async fn get_stats(&self) -> crate::Result<LockStats> {
        self.inner.get_stats().await
    }

    async fn close(&self) -> crate::Result<()> {
        self.inner.close().await
    }

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        true
    }
}

fn create_test_object_key(bucket: &str, object: &str) -> ObjectKey {
    ObjectKey {
        bucket: Arc::from(bucket),
        object: Arc::from(object),
        version: None,
    }
}

async fn wait_until_all_managers_can_write(managers: &[Arc<GlobalLockManager>], resource: ObjectKey) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);

    loop {
        let mut guards = Vec::with_capacity(managers.len());
        let mut all_available = true;

        for (idx, manager) in managers.iter().enumerate() {
            let local_lock = NamespaceLock::with_local_manager(format!("probe-node-{idx}"), manager.clone());
            match local_lock
                .get_write_lock(resource.clone(), "probe-owner", Duration::from_millis(20))
                .await
            {
                Ok(guard) => guards.push(guard),
                Err(_) => {
                    all_available = false;
                    break;
                }
            }
        }

        drop(guards);

        if all_available {
            return;
        }

        assert!(
            tokio::time::Instant::now() < deadline,
            "distributed lock was not released on all simulated nodes"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
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
async fn test_lock_client_default_batch_acquire_and_release() {
    let manager = Arc::new(GlobalLockManager::new());
    let client = LocalClient::with_manager(manager);
    let requests = vec![
        LockRequest::new(create_test_object_key("bucket", "object-a"), LockType::Exclusive, "owner-a")
            .with_acquire_timeout(Duration::from_secs(1)),
        LockRequest::new(create_test_object_key("bucket", "object-b"), LockType::Exclusive, "owner-a")
            .with_acquire_timeout(Duration::from_secs(1)),
    ];

    let responses = client.acquire_locks_batch(&requests).await.unwrap();
    assert_eq!(responses.len(), requests.len());
    assert!(responses.iter().all(|response| response.success));

    let lock_ids = responses
        .iter()
        .map(|response| {
            response
                .lock_info
                .as_ref()
                .expect("successful batch acquire should return lock info")
                .id
                .clone()
        })
        .collect::<Vec<_>>();
    let released = client.release_locks_batch(&lock_ids).await.unwrap();

    assert_eq!(released, vec![true, true]);
}

#[tokio::test]
async fn test_local_client_uses_request_lock_id_for_release() {
    let manager = Arc::new(GlobalLockManager::new());
    let client = LocalClient::with_manager(manager);
    let resource = create_test_object_key("bucket", "object");
    let request = LockRequest::new(resource.clone(), LockType::Exclusive, "owner-a").with_acquire_timeout(Duration::from_secs(1));

    let response = client.acquire_lock(&request).await.unwrap();
    let lock_info = response.lock_info.expect("successful acquire should return lock info");
    assert_eq!(lock_info.id, request.lock_id);

    assert!(client.release(&request.lock_id).await.unwrap());

    let second_request = LockRequest::new(resource, LockType::Exclusive, "owner-b").with_acquire_timeout(Duration::from_secs(1));
    let second_response = client.acquire_lock(&second_request).await.unwrap();
    assert!(second_response.success);
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

#[tokio::test]
async fn test_namespace_lock_distributed_eight_node_write_releases_all_nodes() {
    let managers = (0..8).map(|_| Arc::new(GlobalLockManager::new())).collect::<Vec<_>>();
    let clients = managers
        .iter()
        .map(|manager| Arc::new(LocalClient::with_manager(manager.clone())) as Arc<dyn LockClient>)
        .collect::<Vec<_>>();

    let lock = NamespaceLock::with_clients_and_quorum("eight-node".to_string(), clients, 5);
    let resource = create_test_object_key("bucket", "object-eight-node");

    let mut guard = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(1))
        .await
        .expect("owner-a should acquire write lock across eight simulated nodes");

    let err = lock
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await
        .expect_err("owner-b should not acquire while owner-a holds all node locks");
    let err_str = err.to_string();
    assert!(
        err_str.contains("required 5") && err_str.contains("achieved"),
        "expected 8-node quorum failure below required write quorum, got: {err}"
    );

    assert!(guard.release(), "distributed guard should enqueue release");
    wait_until_all_managers_can_write(&managers, resource).await;
}

#[tokio::test]
async fn test_namespace_lock_distributed_unlock_retries_release_false() {
    let managers = (0..3).map(|_| Arc::new(GlobalLockManager::new())).collect::<Vec<_>>();
    let flaky_clients = managers
        .iter()
        .map(|manager| Arc::new(FlakyReleaseClient::new(manager.clone(), 1)))
        .collect::<Vec<_>>();
    let clients = flaky_clients
        .iter()
        .map(|client| client.clone() as Arc<dyn LockClient>)
        .collect::<Vec<_>>();

    let lock = NamespaceLock::with_clients("flaky-release".to_string(), clients);
    let resource = create_test_object_key("bucket", "object-flaky-release");

    let mut guard = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(1))
        .await
        .expect("owner-a should acquire write lock before flaky release");

    assert!(guard.release(), "distributed guard should enqueue release");
    wait_until_all_managers_can_write(&managers, resource).await;

    assert!(
        flaky_clients.iter().all(|client| client.release_attempts() >= 2),
        "each simulated node should be retried after an initial false release"
    );
}

#[test]
fn test_namespace_lock_distributed_drop_without_runtime_does_not_panic() {
    let (manager, resource, guard) = {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should be created");
        runtime.block_on(async {
            let manager = Arc::new(GlobalLockManager::new());
            let resource = create_test_object_key("bucket", "object-drop-no-runtime");
            let lock = NamespaceLock::with_clients(
                "drop-no-runtime".to_string(),
                vec![Arc::new(LocalClient::with_manager(manager.clone()))],
            );
            let guard = lock
                .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(1))
                .await
                .expect("lock should be acquired");
            (manager, resource, guard)
        })
    };

    let drop_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(guard)));
    assert!(drop_result.is_ok(), "dropping distributed guard without runtime should not panic");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime should be created");
    runtime.block_on(wait_until_all_managers_can_write(&[manager], resource));
}

#[tokio::test]
async fn test_namespace_lock_distributed_read_lock_succeeds_with_two_nodes_one_offline() {
    let manager = Arc::new(GlobalLockManager::new());
    let client_ok: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
    let client_offline: Arc<dyn LockClient> = Arc::new(FailingClient);

    let lock = NamespaceLock::with_clients_and_quorum("two-node".to_string(), vec![client_ok, client_offline], 2);
    let resource = create_test_object_key("bucket", "object");

    let guard = lock
        .get_read_lock(resource, "owner-a", Duration::from_millis(100))
        .await
        .expect("read lock should succeed with one healthy node in a two-node cluster");

    match guard {
        NamespaceLockGuard::Standard(_) => {}
        NamespaceLockGuard::Fast(_) => panic!("Expected Standard guard for distributed lock"),
    }
}

#[tokio::test]
async fn test_namespace_lock_distributed_write_lock_fails_with_two_nodes_one_offline() {
    let manager = Arc::new(GlobalLockManager::new());
    let client_ok: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager));
    let client_offline: Arc<dyn LockClient> = Arc::new(FailingClient);

    let lock = NamespaceLock::with_clients_and_quorum("two-node".to_string(), vec![client_ok, client_offline], 2);
    let resource = create_test_object_key("bucket", "object");

    let err = lock
        .get_write_lock(resource, "owner-a", Duration::from_millis(100))
        .await
        .expect_err("write lock should fail with one healthy node in a two-node cluster");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "expected quorum error, got: {err}"
    );
}

#[tokio::test]
async fn test_namespace_lock_distributed_quorum_failure_rolls_back_successful_nodes() {
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());

    let client1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager1.clone()));
    let client2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager2.clone()));
    let client3: Arc<dyn LockClient> = Arc::new(FailingClient);

    let resource = create_test_object_key("bucket", "object");

    let distributed_lock = NamespaceLock::with_clients_and_quorum("three-node".to_string(), vec![client1, client2, client3], 3);
    let err = distributed_lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_millis(100))
        .await
        .expect_err("write lock should fail when quorum requires all three nodes");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "expected quorum error, got: {err}"
    );

    let local_lock_1 = NamespaceLock::with_local_manager("node-1".to_string(), manager1);
    let local_lock_2 = NamespaceLock::with_local_manager("node-2".to_string(), manager2);

    let guard1 = local_lock_1
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await
        .expect("quorum rollback should release node 1");
    let guard2 = local_lock_2
        .get_write_lock(resource, "owner-b", Duration::from_millis(100))
        .await
        .expect("quorum rollback should release node 2");

    drop(guard1);
    drop(guard2);
}

#[tokio::test]
async fn test_namespace_lock_distributed_quorum_rollback_retries_release_false() {
    let managers = (0..2).map(|_| Arc::new(GlobalLockManager::new())).collect::<Vec<_>>();
    let flaky_clients = managers
        .iter()
        .map(|manager| Arc::new(FlakyReleaseClient::new(manager.clone(), 1)))
        .collect::<Vec<_>>();
    let clients = vec![
        flaky_clients[0].clone() as Arc<dyn LockClient>,
        flaky_clients[1].clone() as Arc<dyn LockClient>,
        Arc::new(FailingClient) as Arc<dyn LockClient>,
    ];
    let resource = create_test_object_key("bucket", "object-rollback-retry");
    let lock = NamespaceLock::with_clients_and_quorum("rollback-retry".to_string(), clients, 3);

    let err = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_millis(100))
        .await
        .expect_err("write lock should fail when quorum requires the offline node");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "expected quorum error, got: {err}"
    );
    wait_until_all_managers_can_write(&managers, resource).await;

    assert!(
        flaky_clients.iter().all(|client| client.release_attempts() >= 2),
        "rollback should retry node releases that initially returned false"
    );
}

#[tokio::test]
async fn test_namespace_lock_distributed_even_node_read_write_quorum_split() {
    let manager1 = Arc::new(GlobalLockManager::new());
    let manager2 = Arc::new(GlobalLockManager::new());

    let client1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager1));
    let client2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager2));
    let client3: Arc<dyn LockClient> = Arc::new(FailingClient);
    let client4: Arc<dyn LockClient> = Arc::new(FailingClient);

    let lock = NamespaceLock::with_clients("four-node".to_string(), vec![client1, client2, client3, client4]);
    let resource = create_test_object_key("bucket", "object");

    let mut read_guard = lock
        .get_read_lock(resource.clone(), "owner-a", Duration::from_millis(100))
        .await
        .expect("read lock should succeed with two healthy nodes in a four-node cluster");

    match &read_guard {
        NamespaceLockGuard::Standard(_) => {}
        NamespaceLockGuard::Fast(_) => panic!("Expected Standard guard for distributed lock"),
    }
    assert!(read_guard.release(), "read guard should release cleanly");

    let err = lock
        .get_write_lock(resource, "owner-a", Duration::from_millis(100))
        .await
        .expect_err("write lock should fail because four-node cluster requires quorum of 3");

    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "expected quorum error, got: {err}"
    );
}

#[tokio::test]
async fn test_namespace_lock_distributed_read_lock_returns_after_quorum_without_waiting_for_slow_clients() {
    let manager_fast_1 = Arc::new(GlobalLockManager::new());
    let manager_fast_2 = Arc::new(GlobalLockManager::new());
    let manager_slow_1 = Arc::new(GlobalLockManager::new());
    let manager_slow_2 = Arc::new(GlobalLockManager::new());

    let client_fast_1: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast_1));
    let client_fast_2: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast_2));
    let client_slow_1: Arc<dyn LockClient> = Arc::new(DelayedClient {
        inner: Arc::new(LocalClient::with_manager(manager_slow_1.clone())),
        delay: Duration::from_millis(250),
    });
    let client_slow_2: Arc<dyn LockClient> = Arc::new(DelayedClient {
        inner: Arc::new(LocalClient::with_manager(manager_slow_2.clone())),
        delay: Duration::from_millis(250),
    });

    let lock = NamespaceLock::with_clients(
        "four-node-read".to_string(),
        vec![client_fast_1, client_fast_2, client_slow_1, client_slow_2],
    );
    let resource = create_test_object_key("bucket", "object");

    let started = tokio::time::Instant::now();
    let mut guard = lock
        .get_read_lock(resource.clone(), "owner-a", Duration::from_secs(1))
        .await
        .expect("read lock should succeed after reaching quorum");

    assert!(
        started.elapsed() < Duration::from_millis(150),
        "read lock should return once quorum is satisfied instead of waiting for slow clients"
    );
    assert!(guard.release(), "distributed read guard should release successfully");

    tokio::time::sleep(Duration::from_millis(350)).await;

    let slow_lock_1 = NamespaceLock::with_local_manager("slow-node-1".to_string(), manager_slow_1);
    let slow_lock_2 = NamespaceLock::with_local_manager("slow-node-2".to_string(), manager_slow_2);

    let write_guard_1 = slow_lock_1
        .get_write_lock(resource.clone(), "owner-b", Duration::from_millis(100))
        .await
        .expect("late successful read lock should be cleaned up on slow node 1");
    let write_guard_2 = slow_lock_2
        .get_write_lock(resource, "owner-b", Duration::from_millis(100))
        .await
        .expect("late successful read lock should be cleaned up on slow node 2");

    drop(write_guard_1);
    drop(write_guard_2);
}

#[tokio::test]
async fn test_namespace_lock_distributed_failure_returns_early_and_cleans_up_late_successes() {
    let manager_fast = Arc::new(GlobalLockManager::new());
    let manager_slow = Arc::new(GlobalLockManager::new());

    let client_fast: Arc<dyn LockClient> = Arc::new(LocalClient::with_manager(manager_fast));
    let client_fail_1: Arc<dyn LockClient> = Arc::new(FailingClient);
    let client_fail_2: Arc<dyn LockClient> = Arc::new(FailingClient);
    let client_slow: Arc<dyn LockClient> = Arc::new(DelayedClient {
        inner: Arc::new(LocalClient::with_manager(manager_slow.clone())),
        delay: Duration::from_millis(250),
    });

    let lock = NamespaceLock::with_clients(
        "four-node-write".to_string(),
        vec![client_fast, client_fail_1, client_fail_2, client_slow],
    );
    let resource = create_test_object_key("bucket", "object");

    let started = tokio::time::Instant::now();
    let err = lock
        .get_write_lock(resource.clone(), "owner-a", Duration::from_secs(1))
        .await
        .expect_err("write lock should fail when quorum becomes impossible");

    assert!(
        started.elapsed() < Duration::from_millis(150),
        "write lock should fail as soon as quorum becomes impossible"
    );
    let err_str = err.to_string().to_lowercase();
    assert!(
        err_str.contains("quorum") || err_str.contains("not reached"),
        "expected quorum failure, got: {err}"
    );

    tokio::time::sleep(Duration::from_millis(350)).await;

    let slow_lock = NamespaceLock::with_local_manager("slow-node".to_string(), manager_slow);
    let write_guard = slow_lock
        .get_write_lock(resource, "owner-b", Duration::from_millis(100))
        .await
        .expect("late successful write lock should be cleaned up after early quorum failure");

    drop(write_guard);
}
