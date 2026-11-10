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

use super::{DistributedLock, LockBatchAcquireRound};
use crate::{
    GlobalLockManager, LocalClient, LockCleanupOutcome, LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats,
    LockType, ObjectKey, client::LockClient,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;

#[derive(Debug)]
struct ObservedBatchClient {
    backend: LocalClient,
    deny_acquire: bool,
    acquire_gate: Option<Arc<Semaphore>>,
    cleanup_gate: Option<Arc<Semaphore>>,
    granted: AtomicUsize,
    grant_changed: Notify,
    released: AtomicUsize,
    release_changed: Notify,
    cleanup_entered: Notify,
    cleanup_calls: AtomicUsize,
    cleanup_changed: Notify,
    cleanup_ids: Mutex<Vec<LockId>>,
    omit_lock_info: AtomicBool,
    lose_release_ack: AtomicBool,
    release_ack_gate: Semaphore,
}

impl ObservedBatchClient {
    fn new(deny_acquire: bool, acquire_gate: Option<Arc<Semaphore>>, cleanup_gate: Option<Arc<Semaphore>>) -> Arc<Self> {
        Arc::new(Self {
            backend: LocalClient::with_manager(Arc::new(GlobalLockManager::new())),
            deny_acquire,
            acquire_gate,
            cleanup_gate,
            granted: AtomicUsize::new(0),
            grant_changed: Notify::new(),
            released: AtomicUsize::new(0),
            release_changed: Notify::new(),
            cleanup_entered: Notify::new(),
            cleanup_calls: AtomicUsize::new(0),
            cleanup_changed: Notify::new(),
            cleanup_ids: Mutex::new(Vec::new()),
            omit_lock_info: AtomicBool::new(false),
            lose_release_ack: AtomicBool::new(false),
            release_ack_gate: Semaphore::new(0),
        })
    }

    async fn wait_for_count(counter: &AtomicUsize, changed: &Notify, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notified = changed.notified();
                if counter.load(Ordering::SeqCst) >= expected {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("expected real backend grant/release event before the test deadline");
    }

    async fn wait_for_grants(&self, expected: usize) {
        Self::wait_for_count(&self.granted, &self.grant_changed, expected).await;
    }

    async fn wait_for_releases(&self, expected: usize) {
        Self::wait_for_count(&self.released, &self.release_changed, expected).await;
    }
}

#[async_trait::async_trait]
impl LockClient for ObservedBatchClient {
    async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
        if self.deny_acquire {
            return Ok(LockResponse::failure("injected peer refusal", Duration::ZERO));
        }
        if let Some(gate) = &self.acquire_gate {
            gate.acquire().await.expect("test acquisition gate remains open").forget();
        }
        let mut response = self.backend.acquire_lock(request).await?;
        if response.success {
            self.granted.fetch_add(1, Ordering::SeqCst);
            self.grant_changed.notify_one();
            if self.omit_lock_info.load(Ordering::SeqCst) {
                response.lock_info = None;
            }
        }
        Ok(response)
    }

    async fn cleanup_grant(&self, lock_id: &LockId) -> crate::Result<LockCleanupOutcome> {
        self.cleanup_ids
            .lock()
            .expect("test cleanup ID log is available")
            .push(lock_id.clone());
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        self.cleanup_changed.notify_one();
        self.cleanup_entered.notify_one();
        if let Some(gate) = &self.cleanup_gate {
            gate.acquire().await.expect("test cleanup gate remains open").forget();
        }
        let outcome = self.backend.cleanup_grant(lock_id).await?;
        if outcome == LockCleanupOutcome::Released {
            self.released.fetch_add(1, Ordering::SeqCst);
            self.release_changed.notify_one();
            if self.lose_release_ack.swap(false, Ordering::SeqCst) {
                self.release_ack_gate
                    .acquire()
                    .await
                    .expect("test ACK gate remains open")
                    .forget();
                return Err(LockError::internal("injected lost release ACK"));
            }
        }
        Ok(outcome)
    }

    async fn release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.cleanup_grant(lock_id)
            .await
            .map(|outcome| outcome == LockCleanupOutcome::Released)
    }

    async fn refresh(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.backend.refresh(lock_id).await
    }

    async fn force_release(&self, lock_id: &LockId) -> crate::Result<bool> {
        self.backend.force_release(lock_id).await
    }

    async fn check_status(&self, lock_id: &LockId) -> crate::Result<Option<LockInfo>> {
        self.backend.check_status(lock_id).await
    }

    async fn get_stats(&self) -> crate::Result<LockStats> {
        self.backend.get_stats().await
    }

    async fn close(&self) -> crate::Result<()> {
        self.backend.close().await
    }

    async fn is_online(&self) -> bool {
        true
    }

    async fn is_local(&self) -> bool {
        true
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_batch_acquisition_cleans_completed_and_late_grants() {
    let late_gate = Arc::new(Semaphore::new(0));
    let completed = ObservedBatchClient::new(false, None, None);
    let late = ObservedBatchClient::new(false, Some(late_gate.clone()), None);
    let lock = DistributedLock::new("cancelled-batch".to_string(), vec![completed.clone(), late.clone()], 2);
    let resources = (0..1000)
        .map(|key| ObjectKey::new("cancelled-batch", format!("key-{key}")))
        .collect::<Vec<_>>();
    let operation = tokio::spawn(async move {
        lock.acquire_guard_batch(&resources, "owner", Duration::from_secs(5), Duration::from_secs(30))
            .await
    });

    completed.wait_for_grants(1000).await;
    assert_eq!(completed.backend.list_lock_leases().await.len(), 1000);
    assert!(!operation.is_finished(), "the other peer still cannot complete its batch");
    operation.abort();
    assert!(operation.await.expect_err("caller was cancelled").is_cancelled());
    late_gate.add_permits(1000);

    completed.wait_for_releases(1000).await;
    late.wait_for_releases(1000).await;
    assert!(completed.backend.list_lock_leases().await.is_empty());
    assert!(late.backend.list_lock_leases().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancelled_batch_rollback_keeps_its_cleanup_owner() {
    let cleanup_gate = Arc::new(Semaphore::new(0));
    let granted = ObservedBatchClient::new(false, None, Some(cleanup_gate.clone()));
    let refused = ObservedBatchClient::new(true, None, None);
    let lock = DistributedLock::new("cancelled-rollback".to_string(), vec![granted.clone(), refused], 2);
    let operation = tokio::spawn(async move {
        lock.acquire_guard_batch(
            &[ObjectKey::new("cancelled-rollback", "object")],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await
    });

    tokio::time::timeout(Duration::from_secs(1), granted.cleanup_entered.notified())
        .await
        .expect("below-quorum rollback entered the real grant cleanup");
    assert_eq!(granted.backend.list_lock_leases().await.len(), 1);
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("caller was cancelled during rollback")
            .is_cancelled()
    );
    cleanup_gate.add_permits(1);

    granted.wait_for_releases(1).await;
    assert!(granted.backend.list_lock_leases().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn cancelled_batch_round_drains_ready_unjoined_replies_with_original_ids() {
    for omit_lock_info in [false, true] {
        let first = ObservedBatchClient::new(false, None, None);
        let second = ObservedBatchClient::new(false, None, None);
        first.omit_lock_info.store(omit_lock_info, Ordering::SeqCst);
        second.omit_lock_info.store(omit_lock_info, Ordering::SeqCst);
        let clients: Arc<[Arc<dyn LockClient>]> = vec![first.clone() as Arc<dyn LockClient>, second.clone()].into();
        let requests: Arc<[LockRequest]> = vec![
            LockRequest::new(ObjectKey::new("ready-batch", "object"), LockType::Exclusive, "owner")
                .with_acquire_timeout(Duration::from_secs(5))
                .with_ttl(Duration::from_secs(30)),
        ]
        .into();
        let mut round = LockBatchAcquireRound {
            pending: JoinSet::new(),
            grants: vec![Vec::new()],
            clients,
            span: tracing::Span::current(),
        };
        let mut tasks = Vec::new();
        for (peer_index, client) in round.clients.iter().enumerate() {
            let client = client.clone();
            let requests = requests.clone();
            tasks.push(round.pending.spawn(async move {
                let started = tokio::time::Instant::now();
                let outcomes = client
                    .acquire_locks_batch_until(&requests, started + Duration::from_secs(5))
                    .await;
                super::LockBatchAcquireTaskResult {
                    peer_index,
                    key_indices: vec![0],
                    requests,
                    outcomes,
                    started,
                    elapsed: Duration::ZERO,
                }
            }));
        }
        first.wait_for_grants(1).await;
        second.wait_for_grants(1).await;
        assert!(tasks.iter().all(tokio::task::AbortHandle::is_finished));
        assert_eq!(round.pending.len(), 2, "both ready replies remain unjoined");
        assert!(round.grants.iter().all(Vec::is_empty), "no reply has been transferred yet");
        drop(round);

        first.wait_for_releases(1).await;
        second.wait_for_releases(1).await;
        assert!(first.backend.list_lock_leases().await.is_empty());
        assert!(second.backend.list_lock_leases().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_batch_preserves_a_locally_partially_completed_batch() {
    let client = ObservedBatchClient::new(false, None, None);
    let blocked = ObjectKey::new("local-partial-batch", "blocked");
    let blocker = LockRequest::new(blocked.clone(), LockType::Exclusive, "blocker")
        .with_acquire_timeout(Duration::from_secs(5))
        .with_ttl(Duration::from_secs(30));
    assert!(
        client
            .backend
            .acquire_lock(&blocker)
            .await
            .expect("hold one local key")
            .success
    );
    let lock = DistributedLock::new("local-partial-batch".to_string(), vec![client.clone()], 1);
    let operation = tokio::spawn(async move {
        lock.acquire_guard_batch(
            &[
                blocked,
                ObjectKey::new("local-partial-batch", "free-one"),
                ObjectKey::new("local-partial-batch", "free-two"),
            ],
            "batch-owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await
    });
    client.wait_for_grants(2).await;
    assert_eq!(client.backend.list_lock_leases().await.len(), 3, "blocker plus two partial grants");
    assert!(!operation.is_finished());
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("cancel the partially completed batch")
            .is_cancelled()
    );
    assert_eq!(
        client
            .backend
            .cleanup_grant(&blocker.lock_id)
            .await
            .expect("release the original holder"),
        LockCleanupOutcome::Released
    );

    client.wait_for_grants(3).await;
    client.wait_for_releases(3).await;
    assert!(client.backend.list_lock_leases().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn successful_batch_guards_retain_their_real_grants_until_drop() {
    let client = ObservedBatchClient::new(false, None, None);
    let lock = DistributedLock::new("successful-batch".to_string(), vec![client.clone()], 1);
    let results = lock
        .acquire_guard_batch(
            &[
                ObjectKey::new("successful-batch", "one"),
                ObjectKey::new("successful-batch", "two"),
            ],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(client.backend.list_lock_leases().await.len(), 2);
    assert_eq!(client.cleanup_calls.load(Ordering::SeqCst), 0, "round drop cannot clean live guards");
    drop(results);

    client.wait_for_releases(2).await;
    assert!(client.backend.list_lock_leases().await.is_empty());
}

#[tokio::test(start_paused = true)]
async fn batch_guard_and_rollback_use_the_request_id_when_lock_info_is_missing() {
    for deny_other_peer in [false, true] {
        let granted = ObservedBatchClient::new(false, None, None);
        granted.omit_lock_info.store(true, Ordering::SeqCst);
        let other = ObservedBatchClient::new(deny_other_peer, None, None);
        let lock = DistributedLock::new("missing-batch-info".to_string(), vec![granted.clone(), other.clone()], 2);
        let results = lock
            .acquire_guard_batch(
                &[ObjectKey::new("missing-batch-info", "object")],
                "owner",
                Duration::from_secs(5),
                Duration::from_secs(30),
            )
            .await;
        assert_eq!(results[0].is_err(), deny_other_peer);
        if !deny_other_peer {
            assert_eq!(granted.backend.list_lock_leases().await.len(), 1);
            assert_eq!(granted.cleanup_calls.load(Ordering::SeqCst), 0);
        }
        drop(results);
        granted.wait_for_releases(1).await;
        if !deny_other_peer {
            other.wait_for_releases(1).await;
        }
        assert!(granted.backend.list_lock_leases().await.is_empty());
        assert!(other.backend.list_lock_leases().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn cancelled_batch_rollback_retries_lost_ack_without_releasing_a_successor() {
    let granted = ObservedBatchClient::new(false, None, None);
    granted.lose_release_ack.store(true, Ordering::SeqCst);
    let refused = ObservedBatchClient::new(true, None, None);
    let resource = ObjectKey::new("batch-lost-ack", "object");
    let lock = DistributedLock::new("batch-lost-ack".to_string(), vec![granted.clone(), refused], 2);
    let acquire_resource = resource.clone();
    let operation = tokio::spawn(async move {
        lock.acquire_guard_batch(&[acquire_resource], "predecessor", Duration::from_secs(5), Duration::from_secs(30))
            .await
    });
    granted.wait_for_releases(1).await;
    let successor = LockRequest::new(resource, LockType::Exclusive, "successor")
        .with_acquire_timeout(Duration::from_secs(5))
        .with_ttl(Duration::from_secs(30));
    assert!(
        granted
            .backend
            .acquire_lock(&successor)
            .await
            .expect("successor acquires after physical release")
            .success
    );
    operation.abort();
    assert!(
        operation
            .await
            .expect_err("cancel caller while release ACK is blocked")
            .is_cancelled()
    );
    granted.release_ack_gate.add_permits(1);

    ObservedBatchClient::wait_for_count(&granted.cleanup_calls, &granted.cleanup_changed, 2).await;
    let cleanup_ids = granted.cleanup_ids.lock().expect("cleanup ID log").clone();
    assert!(cleanup_ids.len() >= 2, "the uncertain ACK must be retried");
    assert!(
        cleanup_ids.iter().all(|id| id == &cleanup_ids[0]),
        "cleanup retains the exact predecessor ID"
    );
    assert_ne!(cleanup_ids[0], successor.lock_id);
    assert!(
        granted
            .backend
            .check_status(&successor.lock_id)
            .await
            .expect("check successor")
            .is_some()
    );
    assert_eq!(granted.backend.list_lock_leases().await.len(), 1);
    assert_eq!(
        granted
            .backend
            .cleanup_grant(&successor.lock_id)
            .await
            .expect("clean successor test grant"),
        LockCleanupOutcome::Released
    );
}
