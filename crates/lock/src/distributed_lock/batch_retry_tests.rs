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

use super::{BatchLockPeerDiagnostics, DistributedLock};
use crate::{
    BatchAcquireOutcome, GlobalLockManager, LocalClient, LockCleanupOutcome, LockError, LockId, LockInfo, LockRequest,
    LockResponse, LockStats, ObjectKey, client::LockClient,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{Notify, Semaphore},
    time::Instant,
};

#[derive(Clone, Debug)]
enum Plan {
    Acquire,
    Retryable,
    Terminal,
    Ambiguous,
    ReplyGate(Arc<Semaphore>),
}

#[derive(Debug)]
struct TypedClient {
    backend: LocalClient,
    plans: Mutex<VecDeque<Plan>>,
    calls: Mutex<Vec<Vec<LockId>>>,
    deadlines: Mutex<Vec<Instant>>,
    changed: Notify,
    grants: AtomicUsize,
    cleanup_calls: AtomicUsize,
    released: AtomicUsize,
    refreshes: AtomicUsize,
    refresh_allowed: AtomicBool,
    cleanup_failures: AtomicUsize,
    cleanup_gate: Option<Arc<Semaphore>>,
    cleanup_key_gate: Mutex<Option<(ObjectKey, Arc<Semaphore>)>>,
    local: bool,
    local_gate: Mutex<Option<Arc<Semaphore>>>,
}

impl TypedClient {
    fn new(plans: Vec<Plan>, local: bool, cleanup_gate: Option<Arc<Semaphore>>) -> Arc<Self> {
        Arc::new(Self {
            backend: LocalClient::with_manager(Arc::new(GlobalLockManager::new())),
            plans: Mutex::new(plans.into()),
            calls: Mutex::new(Vec::new()),
            deadlines: Mutex::new(Vec::new()),
            changed: Notify::new(),
            grants: AtomicUsize::new(0),
            cleanup_calls: AtomicUsize::new(0),
            released: AtomicUsize::new(0),
            refreshes: AtomicUsize::new(0),
            refresh_allowed: AtomicBool::new(true),
            cleanup_failures: AtomicUsize::new(0),
            cleanup_gate,
            cleanup_key_gate: Mutex::new(None),
            local,
            local_gate: Mutex::new(None),
        })
    }

    async fn wait_until(&self, predicate: impl Fn() -> bool) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let notified = self.changed.notified();
                if predicate() {
                    return;
                }
                notified.await;
            }
        })
        .await
        .expect("expected a real acquisition or cleanup event");
    }

    async fn wait_calls(&self, count: usize) {
        self.wait_until(|| self.calls.lock().expect("call log").len() >= count).await;
    }

    async fn wait_grants(&self, count: usize) {
        self.wait_until(|| self.grants.load(Ordering::SeqCst) >= count).await;
    }

    async fn wait_releases(&self, count: usize) {
        self.wait_until(|| self.released.load(Ordering::SeqCst) >= count).await;
    }
}

#[async_trait::async_trait]
impl LockClient for TypedClient {
    async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
        self.backend.acquire_lock(request).await
    }
    async fn acquire_locks_batch_until(&self, requests: &[LockRequest], deadline: Instant) -> Vec<BatchAcquireOutcome> {
        self.calls
            .lock()
            .expect("call log")
            .push(requests.iter().map(|request| request.lock_id.clone()).collect());
        self.deadlines.lock().expect("deadline log").push(deadline);
        self.changed.notify_one();
        let plan = self.plans.lock().expect("plan queue").pop_front().unwrap_or(Plan::Acquire);
        match plan {
            Plan::Retryable => requests
                .iter()
                .map(|_| BatchAcquireOutcome::RetryableNoGrant {
                    error: LockError::internal("explicit no grant"),
                })
                .collect(),
            Plan::Terminal => requests
                .iter()
                .map(|_| BatchAcquireOutcome::TerminalNoGrant {
                    error: LockError::permission_denied("explicit refusal"),
                })
                .collect(),
            Plan::Ambiguous => requests
                .iter()
                .map(|_| BatchAcquireOutcome::Ambiguous {
                    error: LockError::internal("unknown outcome"),
                    cleanup_owned_by_client: true,
                })
                .collect(),
            Plan::Acquire | Plan::ReplyGate(_) => {
                let outcomes = self.backend.acquire_locks_batch_until(requests, deadline).await;
                self.grants.fetch_add(
                    outcomes
                        .iter()
                        .filter(|outcome| matches!(outcome, BatchAcquireOutcome::Granted { .. }))
                        .count(),
                    Ordering::SeqCst,
                );
                self.changed.notify_one();
                if let Plan::ReplyGate(gate) = plan {
                    gate.acquire().await.expect("response gate").forget();
                }
                outcomes
            }
        }
    }
    async fn cleanup_grant(&self, id: &LockId) -> crate::Result<LockCleanupOutcome> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        if let Some(gate) = &self.cleanup_gate {
            gate.acquire().await.expect("cleanup gate").forget();
        }
        let key_gate = self
            .cleanup_key_gate
            .lock()
            .expect("key cleanup gate")
            .as_ref()
            .filter(|(key, _)| *key == id.resource)
            .map(|(_, gate)| gate.clone());
        if let Some(gate) = key_gate {
            gate.acquire().await.expect("key cleanup gate").forget();
        }
        if self
            .cleanup_failures
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |count| count.checked_sub(1))
            .is_ok()
        {
            return Err(LockError::internal("injected cleanup failure"));
        }
        let result = self.backend.cleanup_grant(id).await?;
        if result == LockCleanupOutcome::Released {
            self.released.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_one();
        }
        Ok(result)
    }
    async fn release(&self, id: &LockId) -> crate::Result<bool> {
        self.cleanup_grant(id)
            .await
            .map(|outcome| outcome == LockCleanupOutcome::Released)
    }
    async fn refresh(&self, id: &LockId) -> crate::Result<bool> {
        self.refreshes.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        if self.refresh_allowed.load(Ordering::SeqCst) {
            self.backend.refresh(id).await
        } else {
            Ok(false)
        }
    }
    async fn force_release(&self, id: &LockId) -> crate::Result<bool> {
        self.backend.force_release(id).await
    }
    async fn check_status(&self, id: &LockId) -> crate::Result<Option<LockInfo>> {
        self.backend.check_status(id).await
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
        let gate = self.local_gate.lock().expect("local lookup gate").take();
        if let Some(gate) = gate {
            gate.acquire().await.expect("local lookup gate open").forget();
        }
        self.local
    }
}

fn lock_for(clients: &[Arc<TypedClient>], quorum: usize) -> DistributedLock {
    DistributedLock::new(
        "batch-retry".to_owned(),
        clients.iter().cloned().map(|client| client as Arc<dyn LockClient>).collect(),
        quorum,
    )
}

#[tokio::test(start_paused = true)]
async fn healthy_thousand_key_batch_keeps_one_call_per_peer() {
    let peers: Vec<_> = (0..4).map(|_| TypedClient::new(Vec::new(), true, None)).collect();
    let resources: Vec<_> = (0..1000)
        .map(|index| ObjectKey::new("healthy-batch", format!("{index}")))
        .collect();
    let results = lock_for(&peers, 3)
        .acquire_guard_batch(&resources, "owner", Duration::from_secs(5), Duration::from_secs(30))
        .await;
    assert!(results.iter().all(Result::is_ok));
    for peer in &peers {
        peer.wait_calls(1).await;
        let calls = peer.calls.lock().expect("call log");
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].len(), 1000);
    }
    drop(results);
}

#[tokio::test(start_paused = true)]
async fn third_pending_vote_is_waited_for_even_with_an_ambiguous_peer() {
    for before_deadline in [true, false] {
        let gate = Arc::new(Semaphore::new(0));
        let peers = vec![
            TypedClient::new(Vec::new(), true, None),
            TypedClient::new(Vec::new(), true, None),
            TypedClient::new(vec![Plan::ReplyGate(gate.clone())], false, None),
            TypedClient::new(vec![Plan::Ambiguous], false, None),
        ];
        let lock = lock_for(&peers, 3);
        let start = Instant::now();
        let operation = tokio::spawn(async move {
            lock.acquire_guard_batch(
                &[ObjectKey::new("pending-third", "key")],
                "owner",
                Duration::from_secs(5),
                Duration::from_secs(30),
            )
            .await
        });
        for peer in &peers[..3] {
            peer.wait_grants(1).await;
        }
        assert!(!operation.is_finished(), "two votes do not discard a pending third vote");
        if before_deadline {
            gate.add_permits(1);
            let result = operation.await.expect("batch task");
            assert!(result[0].is_ok());
            assert!(start.elapsed() < Duration::from_secs(5));
            drop(result);
        } else {
            tokio::time::advance(Duration::from_secs(5)).await;
            let result = operation.await.expect("deadline returns without the delayed reply");
            assert!(result[0].is_err());
            gate.add_permits(1);
        }
        for peer in &peers[..3] {
            peer.wait_releases(1).await;
            assert!(peer.backend.list_lock_leases().await.is_empty());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn ambiguous_peer_is_excluded_and_confirmed_subset_retries_with_fresh_ids() {
    for terminal in [false, true] {
        let peers = vec![
            TypedClient::new(vec![Plan::Ambiguous], false, None),
            TypedClient::new(Vec::new(), true, None),
            TypedClient::new(Vec::new(), true, None),
            TypedClient::new(vec![if terminal { Plan::Terminal } else { Plan::Retryable }], true, None),
        ];
        peers[1].cleanup_failures.store(1, Ordering::SeqCst);
        let result = lock_for(&peers, 3)
            .acquire_guard_batch(
                &[ObjectKey::new("subset-retry", "key")],
                "owner",
                Duration::from_secs(5),
                Duration::from_secs(30),
            )
            .await;
        assert_eq!(result[0].is_err(), terminal);
        assert_eq!(
            peers[0].calls.lock().expect("ambiguous call log").len(),
            1,
            "uncertain peer never receives another attempt"
        );
        for peer in &peers[1..] {
            let calls = peer.calls.lock().expect("healthy call log");
            assert_eq!(calls.len(), if terminal { 1 } else { 2 });
            if !terminal {
                assert_ne!(calls[0][0], calls[1][0], "each retry uses a fresh exact ID");
            }
        }
        assert!(
            peers[1].cleanup_calls.load(Ordering::SeqCst) >= 2,
            "first failed cleanup is retried before a new acquire"
        );
        assert_eq!(peers[1].released.load(Ordering::SeqCst), 1);
        assert_eq!(peers[2].released.load(Ordering::SeqCst), 1);
        drop(result);
    }
}

#[tokio::test(start_paused = true)]
async fn hot_key_retries_healthy_subset_without_waiting_for_silent_peer() {
    let peers: Vec<_> = (0..3).map(|_| TypedClient::new(Vec::new(), true, None)).collect();
    let holder_lock = lock_for(&peers, 3);
    let hot = ObjectKey::new("hot-subset", "hot");
    let holder = holder_lock
        .lock_guard(hot.clone(), "holder", Duration::from_secs(5), Duration::from_secs(30))
        .await
        .expect("legitimate distributed holder")
        .expect("holder reaches q3");
    let late_gate = Arc::new(Semaphore::new(0));
    let late = TypedClient::new(vec![Plan::ReplyGate(late_gate.clone())], false, None);
    let mut batch_peers = peers.clone();
    batch_peers.push(late.clone());
    let lock = lock_for(&batch_peers, 3);
    let start = Instant::now();
    let resources: Vec<_> = std::iter::once(hot.clone())
        .chain((0..999).map(|index| ObjectKey::new("hot-subset", format!("free-{index}"))))
        .collect();
    let release_holder = tokio::spawn(async move {
        tokio::time::sleep_until(start + Duration::from_millis(1500)).await;
        drop(holder);
    });
    let results = lock
        .acquire_guard_batch(&resources, "owner", Duration::from_secs(5), Duration::from_secs(30))
        .await;
    release_holder.await.expect("holder release");
    assert!(
        results.iter().all(Result::is_ok),
        "hot key recovers after the first local window, while the fourth peer is still silent"
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    assert_eq!(late.calls.lock().expect("silent peer call log").len(), 1);
    for peer in &peers {
        let calls = peer.calls.lock().expect("call log");
        assert_eq!(calls[0].len(), 1000);
        assert!(calls.len() >= 2);
        assert!(
            calls[1..].iter().all(|batch| batch.len() == 1 && batch[0].resource == hot),
            "only the unresolved hot key is retried"
        );
    }
    late_gate.add_permits(1);
    late.wait_releases(1000).await;
    drop(results);
}

#[tokio::test(start_paused = true)]
async fn completed_eligible_quorum_retries_while_a_possible_vote_is_silent() {
    let peers: Vec<_> = (0..3).map(|_| TypedClient::new(Vec::new(), true, None)).collect();
    let resource = ObjectKey::new("completed-quorum", "hot");
    let holder = LockRequest::new(resource.clone(), crate::LockType::Exclusive, "holder").with_ttl(Duration::from_secs(30));
    assert!(
        peers[2]
            .backend
            .acquire_lock(&holder)
            .await
            .expect("contended peer holder")
            .success
    );
    let gate = Arc::new(Semaphore::new(0));
    let late = TypedClient::new(vec![Plan::ReplyGate(gate.clone())], false, None);
    let mut batch_peers = peers.clone();
    batch_peers.push(late.clone());
    let lock = lock_for(&batch_peers, 3);
    let start = Instant::now();
    let contended = peers[2].clone();
    let release_holder = tokio::spawn(async move {
        tokio::time::sleep_until(start + Duration::from_millis(1500)).await;
        contended
            .backend
            .cleanup_grant(&holder.lock_id)
            .await
            .expect("release contention");
    });
    let results = lock
        .acquire_guard_batch(&[resource], "owner", Duration::from_secs(5), Duration::from_secs(30))
        .await;
    release_holder.await.expect("holder release task");
    assert!(
        results[0].is_ok(),
        "the three completed eligible peers retry before the silent fourth responds"
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    for peer in &peers {
        let calls = peer.calls.lock().expect("reachable peer attempts");
        assert!(calls.len() >= 2);
        assert_ne!(calls[0][0], calls[1][0], "rollback precedes a fresh-ID attempt");
    }
    assert_eq!(late.calls.lock().expect("silent peer attempts").len(), 1);
    gate.add_permits(1);
    late.wait_releases(1).await;
    drop(results);
    for peer in &peers {
        peer.wait_releases(if Arc::ptr_eq(peer, &peers[2]) { 1 } else { 2 }).await;
        assert!(peer.backend.list_lock_leases().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn cleaned_late_peer_rejoins_keys_with_distinct_peer_contention() {
    let peers: Vec<_> = (0..3).map(|_| TypedClient::new(Vec::new(), true, None)).collect();
    let resources = vec![
        ObjectKey::new("late-rejoins", "first"),
        ObjectKey::new("late-rejoins", "second"),
    ];
    let mut holders = Vec::new();
    for (peer, key) in [(1, 0), (2, 1)] {
        let holder = LockRequest::new(resources[key].clone(), crate::LockType::Exclusive, "distinct-holder")
            .with_ttl(Duration::from_secs(30));
        assert!(
            peers[peer]
                .backend
                .acquire_lock(&holder)
                .await
                .expect("distinct holder")
                .success
        );
        holders.push((peer, holder));
    }
    let gate = Arc::new(Semaphore::new(0));
    let late = TypedClient::new(vec![Plan::ReplyGate(gate.clone())], false, None);
    let mut batch_peers = peers.clone();
    batch_peers.push(late.clone());
    let start = Instant::now();
    let response = tokio::spawn(async move {
        tokio::time::sleep_until(start + Duration::from_millis(1500)).await;
        gate.add_permits(1);
    });
    let results = lock_for(&batch_peers, 3)
        .acquire_guard_batch(&resources, "owner", Duration::from_secs(5), Duration::from_secs(30))
        .await;
    response.await.expect("late response task");
    assert!(
        results.iter().all(Result::is_ok),
        "confirmed late cleanup restores a useful peer for each key"
    );
    assert!(start.elapsed() < Duration::from_secs(5));
    assert!(late.calls.lock().expect("late attempts").len() >= 2);
    for (peer, holder) in holders {
        assert!(
            peers[peer]
                .backend
                .check_status(&holder.lock_id)
                .await
                .expect("holder status")
                .is_some()
        );
        peers[peer]
            .backend
            .cleanup_grant(&holder.lock_id)
            .await
            .expect("release holder");
    }
    drop(results);
    for peer in batch_peers {
        peer.wait_until(|| peer.released.load(Ordering::SeqCst) >= peer.grants.load(Ordering::SeqCst))
            .await;
        assert!(peer.backend.list_lock_leases().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn cleaned_key_retries_while_another_keys_rollback_is_stalled() {
    let peers = vec![
        TypedClient::new(Vec::new(), true, None),
        TypedClient::new(Vec::new(), true, None),
        TypedClient::new(Vec::new(), true, None),
        TypedClient::new(vec![Plan::Ambiguous], false, None),
    ];
    let stalled = ObjectKey::new("independent-rollback", "stalled");
    let ready = ObjectKey::new("independent-rollback", "ready");
    let gate = Arc::new(Semaphore::new(0));
    *peers[0].cleanup_key_gate.lock().expect("install per-key gate") = Some((stalled.clone(), gate.clone()));
    let holders: Vec<_> = [stalled.clone(), ready.clone()]
        .into_iter()
        .map(|key| LockRequest::new(key, crate::LockType::Exclusive, "holder").with_ttl(Duration::from_secs(30)))
        .collect();
    for holder in &holders {
        assert!(
            peers[2]
                .backend
                .acquire_lock(holder)
                .await
                .expect("contention holder")
                .success
        );
    }
    let start = Instant::now();
    let contended = peers[2].clone();
    let release_holder = tokio::spawn(async move {
        tokio::time::sleep_until(start + Duration::from_millis(1500)).await;
        for holder in holders {
            contended
                .backend
                .cleanup_grant(&holder.lock_id)
                .await
                .expect("release held key");
        }
    });
    let results = lock_for(&peers, 3)
        .acquire_guard_batch(
            &[stalled.clone(), ready.clone()],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await;
    release_holder.await.expect("holder release task");
    assert!(results[0].is_err(), "unconfirmed cleanup keeps its own key blocked");
    assert!(
        results[1].is_ok(),
        "a fully cleaned key retries without waiting for another key's rollback"
    );
    assert_eq!(start.elapsed(), Duration::from_secs(5));
    for peer in &peers[..3] {
        let calls = peer.calls.lock().expect("per-key retry attempts");
        assert!(calls.len() >= 2);
        assert!(calls[1..].iter().all(|batch| batch.len() == 1 && batch[0].resource == ready));
    }
    assert_eq!(peers[3].calls.lock().expect("ambiguous attempts").len(), 1);
    assert!(
        peers[0]
            .backend
            .list_lock_leases()
            .await
            .iter()
            .any(|lease| lease.resource == stalled)
    );
    gate.add_permits(1);
    peers[0].wait_releases(2).await;
    assert!(
        peers[0]
            .backend
            .list_lock_leases()
            .await
            .iter()
            .all(|lease| lease.resource != stalled)
    );
    drop(results);
    for (index, peer) in peers[..3].iter().enumerate() {
        peer.wait_releases(if index == 2 { 1 } else { 3 }).await;
        assert!(peer.backend.list_lock_leases().await.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn successful_keys_are_renewed_while_hot_key_uses_the_total_budget() {
    for refresh_allowed in [true, false] {
        let peers: Vec<_> = (0..3).map(|_| TypedClient::new(Vec::new(), true, None)).collect();
        let hot = ObjectKey::new("retained-guard", "hot");
        let holder = lock_for(&peers, 3)
            .lock_guard(hot.clone(), "holder", Duration::from_secs(5), Duration::from_secs(30))
            .await
            .expect("distributed holder")
            .expect("q3 holder");
        for peer in &peers {
            peer.refresh_allowed.store(refresh_allowed, Ordering::SeqCst);
        }
        let start = Instant::now();
        let results = lock_for(&peers, 3)
            .acquire_guard_batch(
                &[hot, ObjectKey::new("retained-guard", "free")],
                "owner",
                Duration::from_secs(5),
                Duration::from_secs(2),
            )
            .await;
        assert!(results[0].is_err(), "holder is never released inside the acquisition budget");
        assert!(
            peers.iter().any(|peer| peer.refreshes.load(Ordering::SeqCst) > 0),
            "cold key must form an early guard before D_total: calls={:?}, grants={:?}, refreshes={:?}",
            peers
                .iter()
                .map(|peer| peer.calls.lock().expect("call log").len())
                .collect::<Vec<_>>(),
            peers
                .iter()
                .map(|peer| peer.grants.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
            peers
                .iter()
                .map(|peer| peer.refreshes.load(Ordering::SeqCst))
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            results[1].is_ok(),
            refresh_allowed,
            "the final result rejects a previously successful guard after quorum is lost"
        );
        assert_eq!(start.elapsed(), Duration::from_secs(5));
        if refresh_allowed {
            assert!(
                peers.iter().all(|peer| peer.refreshes.load(Ordering::SeqCst) > 1),
                "healthy early guards repeatedly renew every quorum peer during other keys' retries"
            );
        }
        drop(results);
        drop(holder);
    }
}

#[tokio::test(start_paused = true)]
async fn rollback_deadline_detaches_cleanup_without_reacquiring() {
    let gate = Arc::new(Semaphore::new(0));
    let peers = vec![
        TypedClient::new(vec![Plan::Ambiguous], false, None),
        TypedClient::new(Vec::new(), true, Some(gate.clone())),
        TypedClient::new(Vec::new(), true, None),
        TypedClient::new(vec![Plan::Retryable], true, None),
    ];
    let start = Instant::now();
    let result = lock_for(&peers, 3)
        .acquire_guard_batch(
            &[ObjectKey::new("rollback-deadline", "key")],
            "owner",
            Duration::from_millis(500),
            Duration::from_secs(30),
        )
        .await;
    assert!(result[0].is_err());
    assert_eq!(start.elapsed(), Duration::from_millis(500));
    assert!(peers.iter().all(|peer| peer.calls.lock().expect("call log").len() == 1));
    assert_eq!(peers[1].backend.list_lock_leases().await.len(), 1);
    gate.add_permits(1);
    peers[1].wait_releases(1).await;
    assert!(peers[1].backend.list_lock_leases().await.is_empty());
}

#[test]
fn batch_diagnostics_preserve_typed_no_grant_and_ambiguity() {
    let outcomes = vec![
        BatchAcquireOutcome::Granted {
            lock_info: None,
            wait_time: Duration::ZERO,
        },
        BatchAcquireOutcome::RetryableNoGrant {
            error: LockError::internal("explicit"),
        },
        BatchAcquireOutcome::TerminalNoGrant {
            error: LockError::internal("terminal"),
        },
        BatchAcquireOutcome::Ambiguous {
            error: LockError::internal("unknown"),
            cleanup_owned_by_client: false,
        },
    ];
    let diagnostics = BatchLockPeerDiagnostics::from_outcomes(1, 5, &outcomes, Duration::ZERO);
    assert_eq!(diagnostics.granted_keys, 1);
    assert_eq!(diagnostics.retryable_no_grant_keys, 1);
    assert_eq!(diagnostics.terminal_no_grant_keys, 1);
    assert_eq!(diagnostics.ambiguous_keys, 1);
    assert_eq!(diagnostics.missing_response_keys, 1);
}

#[derive(Debug)]
struct LegacyHiddenClient {
    backend: LocalClient,
    requested: Mutex<Vec<LockId>>,
    release_attempts: Mutex<std::collections::HashMap<LockId, usize>>,
    cleanup_calls: AtomicUsize,
    changed: Notify,
}

#[async_trait::async_trait]
impl LockClient for LegacyHiddenClient {
    async fn acquire_lock(&self, request: &LockRequest) -> crate::Result<LockResponse> {
        self.requested.lock().expect("request IDs").push(request.lock_id.clone());
        if request.resource.object.as_ref() == "error" {
            return Err(LockError::internal("one legacy item failed"));
        }
        self.backend.acquire_lock(request).await
    }
    async fn release(&self, id: &LockId) -> crate::Result<bool> {
        let attempt = {
            let mut attempts = self.release_attempts.lock().expect("release attempts");
            let attempt = attempts.entry(id.clone()).or_default();
            *attempt += 1;
            *attempt
        };
        self.changed.notify_one();
        // An absence-looking first response cannot terminate uncertainty.
        if attempt == 1 {
            return Ok(false);
        }
        let result = self.backend.release(id).await;
        self.changed.notify_one();
        result
    }
    async fn cleanup_grant(&self, _id: &LockId) -> crate::Result<LockCleanupOutcome> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        Ok(LockCleanupOutcome::AlreadyAbsent)
    }
    async fn refresh(&self, id: &LockId) -> crate::Result<bool> {
        self.backend.refresh(id).await
    }
    async fn force_release(&self, id: &LockId) -> crate::Result<bool> {
        self.backend.force_release(id).await
    }
    async fn check_status(&self, id: &LockId) -> crate::Result<Option<LockInfo>> {
        self.backend.check_status(id).await
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
async fn legacy_collapsed_error_cleans_hidden_grant_without_accepting_absence() {
    let peer = Arc::new(LegacyHiddenClient {
        backend: LocalClient::with_manager(Arc::new(GlobalLockManager::new())),
        requested: Mutex::new(Vec::new()),
        release_attempts: Mutex::new(std::collections::HashMap::new()),
        cleanup_calls: AtomicUsize::new(0),
        changed: Notify::new(),
    });
    let lock = DistributedLock::new("legacy-hidden".to_owned(), vec![peer.clone()], 1);
    let results = lock
        .acquire_guard_batch(
            &[
                ObjectKey::new("legacy-hidden", "granted"),
                ObjectKey::new("legacy-hidden", "error"),
            ],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await;
    assert!(
        results.iter().all(Result::is_err),
        "the default collect error obscures the successful sibling"
    );
    tokio::time::timeout(Duration::from_millis(50), async {
        loop {
            let notified = peer.changed.notified();
            if peer.release_attempts.lock().expect("release attempts").len() == 2 {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("both uncertain IDs receive the first release pass");
    assert_eq!(
        peer.backend.list_lock_leases().await.len(),
        1,
        "false release does not prove this hidden grant absent"
    );
    assert_eq!(
        peer.cleanup_calls.load(Ordering::SeqCst),
        0,
        "uncertainty must never use authoritative-absence cleanup"
    );
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let notified = peer.changed.notified();
            if peer.backend.list_lock_leases().await.is_empty() {
                break;
            }
            notified.await;
        }
    })
    .await
    .expect("exact-ID release is retried and clears the hidden grant before its TTL");
    let requests = peer.requested.lock().expect("request IDs");
    let releases = peer.release_attempts.lock().expect("release attempts");
    assert_eq!(requests.len(), 2, "ambiguous requests are never reacquired");
    assert!(
        releases.keys().all(|id| requests.contains(id)),
        "cleanup only uses the original request IDs"
    );
    assert!(releases.values().all(|&attempts| attempts >= 2));
}

#[tokio::test(start_paused = true)]
async fn local_wait_deadline_is_fixed_before_scheduling_and_single_peer_keeps_total() {
    let gate = Arc::new(Semaphore::new(0));
    let local = TypedClient::new(Vec::new(), true, None);
    *local.local_gate.lock().expect("set lookup gate") = Some(gate.clone());
    let peers = vec![
        local.clone(),
        TypedClient::new(vec![Plan::Retryable], true, None),
        TypedClient::new(vec![Plan::Retryable], true, None),
        TypedClient::new(vec![Plan::Ambiguous], false, None),
    ];
    let lock = lock_for(&peers, 3);
    let operation = tokio::spawn(async move {
        lock.acquire_guard_batch(
            &[ObjectKey::new("late-local", "key")],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await
    });
    peers[1].wait_calls(1).await;
    peers[2].wait_calls(1).await;
    tokio::time::advance(Duration::from_millis(1250)).await;
    gate.add_permits(1);
    let result = operation.await.expect("retry after the delayed local lookup");
    assert!(result[0].is_ok());
    assert_eq!(
        local.calls.lock().expect("local calls").len(),
        1,
        "the expired first local round never calls acquire again with a fresh budget"
    );
    assert_eq!(peers[1].calls.lock().expect("peer calls").len(), 2);
    assert_eq!(peers[2].calls.lock().expect("peer calls").len(), 2);
    drop(result);

    let single = TypedClient::new(Vec::new(), true, None);
    let start = Instant::now();
    let results = lock_for(std::slice::from_ref(&single), 1)
        .acquire_guard_batch(
            &[ObjectKey::new("single-local", "key")],
            "owner",
            Duration::from_secs(5),
            Duration::from_secs(30),
        )
        .await;
    assert!(results[0].is_ok());
    assert_eq!(single.deadlines.lock().expect("single deadline")[0], start + Duration::from_secs(5));
    drop(results);
}

#[tokio::test(start_paused = true)]
async fn total_deadline_never_dispatches_after_a_delayed_peer_lookup() {
    let gate = Arc::new(Semaphore::new(0));
    let peer = TypedClient::new(Vec::new(), false, None);
    *peer.local_gate.lock().expect("set lookup gate") = Some(gate.clone());
    let result = lock_for(std::slice::from_ref(&peer), 1)
        .acquire_guard_batch(
            &[ObjectKey::new("no-late-dispatch", "key")],
            "owner",
            Duration::from_secs(1),
            Duration::from_secs(30),
        )
        .await;
    assert!(result[0].is_err());
    gate.add_permits(1);
    // The lookup task is now runnable; yield until its gate permit was consumed.
    tokio::time::timeout(Duration::from_secs(1), async {
        while gate.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("late lookup task resumes under its retained owner");
    assert!(peer.calls.lock().expect("peer calls").is_empty(), "no acquire call starts after D_total");
    assert!(peer.backend.list_lock_leases().await.is_empty());
}
