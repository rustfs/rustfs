// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use std::sync::Arc;
use tokio::{task::JoinHandle, time::timeout};

fn manager() -> Arc<FastObjectLockManager> {
    Arc::new(FastObjectLockManager::with_config(LockConfig {
        shard_count: 1,
        max_idle_time: Duration::ZERO,
        ..Default::default()
    }))
}

fn key() -> ObjectKey {
    ObjectKey::new("publication-bucket", rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH)
}

fn queued(manager: &Arc<FastObjectLockManager>, owner: &str, mode: LockMode) -> JoinHandle<FastLockGuard> {
    let manager = manager.clone();
    let owner = owner.to_owned();
    tokio::spawn(async move {
        let mut request = ObjectLockRequest::new_read(key(), owner);
        request.mode = mode;
        manager
            .acquire_lock(request.with_acquire_timeout(Duration::from_secs(5)))
            .await
            .expect("queued publication lock")
    })
}

async fn waiters(manager: &FastObjectLockManager, count: u64) {
    timeout(Duration::from_secs(3), async {
        while manager.current_lock_counts().1 != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter admission must be observable");
}

async fn acquired(task: JoinHandle<FastLockGuard>) -> FastLockGuard {
    timeout(Duration::from_secs(3), task)
        .await
        .expect("queue must progress")
        .expect("acquire task")
}

#[tokio::test]
async fn publication_queue_serves_read_groups_and_writers_in_arrival_order() {
    let manager = manager();
    let holder = manager.acquire_write_lock(key(), "holder").await.expect("initial holder");
    let w1 = queued(&manager, "w1", LockMode::Exclusive);
    waiters(&manager, 1).await;
    let r1 = queued(&manager, "r1", LockMode::Shared);
    waiters(&manager, 2).await;
    let r2 = queued(&manager, "r2", LockMode::Shared);
    waiters(&manager, 3).await;
    let w2 = queued(&manager, "w2", LockMode::Exclusive);
    waiters(&manager, 4).await;
    let r3 = queued(&manager, "r3", LockMode::Shared);
    waiters(&manager, 5).await;
    assert_eq!(manager.cleanup_expired_traditional().await, 0, "queued state must stay mapped");
    drop(holder);
    assert_eq!(
        manager.cleanup_expired_traditional().await,
        0,
        "an unlocked state with queued tickets must stay mapped"
    );
    let w1 = acquired(w1).await;
    assert!(!r1.is_finished() && !r2.is_finished() && !w2.is_finished() && !r3.is_finished());
    drop(w1);
    let r1 = acquired(r1).await;
    let r2 = acquired(r2).await;
    assert!(!w2.is_finished() && !r3.is_finished(), "later requests cannot pass the reader group");
    drop(r1);
    assert!(!w2.is_finished(), "the second reader still holds the fence");
    drop(r2);
    let w2 = acquired(w2).await;
    assert!(!r3.is_finished(), "a later reader cannot starve the queued writer");
    drop(w2);
    drop(acquired(r3).await);
    assert_eq!(manager.current_lock_counts(), (0, 0));
}

#[tokio::test]
async fn publication_queue_cancellation_and_timeout_remove_their_turns() {
    let manager = manager();
    let holder = manager.acquire_write_lock(key(), "holder").await.expect("initial holder");
    let cancelled = queued(&manager, "cancelled", LockMode::Exclusive);
    waiters(&manager, 1).await;
    let expiring = tokio::spawn({
        let manager = manager.clone();
        async move {
            manager
                .acquire_lock(ObjectLockRequest::new_write(key(), "expires").with_acquire_timeout(Duration::from_millis(500)))
                .await
        }
    });
    waiters(&manager, 2).await;
    let reader = queued(&manager, "survivor", LockMode::Shared);
    waiters(&manager, 3).await;
    cancelled.abort();
    assert!(cancelled.await.expect_err("cancel waiter").is_cancelled());
    assert!(matches!(expiring.await.expect("timeout task"), Err(LockResult::Timeout)));
    waiters(&manager, 1).await;
    assert_eq!(manager.cleanup_expired_traditional().await, 0);
    drop(holder);
    drop(acquired(reader).await);
    assert_eq!(manager.current_lock_counts(), (0, 0));
    timeout(Duration::from_secs(3), async {
        while manager.cleanup_expired_traditional().await == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an empty queue must allow map cleanup");
    // An empty queue neither keeps a stale turn nor prevents subsequent writers.
    drop(manager.acquire_write_lock(key(), "next").await.expect("queue recovered"));
}

#[tokio::test]
async fn publication_queue_batch_fast_path_cannot_bypass_a_reader() {
    for all_or_nothing in [false, true] {
        let manager = manager();
        let holder = manager.acquire_write_lock(key(), "holder").await.expect("initial holder");
        let reader = queued(&manager, "queued-reader", LockMode::Shared);
        waiters(&manager, 1).await;
        drop(holder);
        // This future can try the batch fast path before the reader runs again.
        let batch = manager.acquire_locks_batch(
            BatchLockRequest::new("batch")
                .add_write_lock(key())
                .with_all_or_nothing(all_or_nothing),
        );
        tokio::pin!(batch);
        let reader = tokio::select! {
            biased;
            _ = &mut batch => panic!("batch overtook an already queued reader"),
            reader = acquired(reader) => reader,
        };
        drop(reader);
        let batch = timeout(Duration::from_secs(3), batch)
            .await
            .expect("batch progresses after reader");
        assert!(batch.all_acquired);
        drop(batch);
        assert_eq!(manager.current_lock_counts(), (0, 0));
    }
}

#[test]
fn publication_queue_is_scoped_to_the_unversioned_bucket_fence() {
    assert!(publication::applies(&key()));
    assert!(!publication::applies(&ObjectKey::new("publication-bucket", "plain-object")));
    assert!(!publication::applies(&ObjectKey::with_version(
        "publication-bucket",
        rustfs_common::table_catalog::TABLE_BUCKET_PUBLICATION_LOCK_PATH,
        "version"
    )));
}

async fn cluster_waiters(managers: &[Arc<FastObjectLockManager>], minimum: u64) {
    timeout(Duration::from_secs(3), async {
        while !managers.iter().all(|manager| manager.current_lock_counts().1 >= minimum) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all real local peers registered the waiter");
}

fn cluster_queued(lock: &Arc<crate::NamespaceLock>, owner: &str, kind: crate::LockType) -> JoinHandle<crate::NamespaceLockGuard> {
    let lock = lock.clone();
    let owner = owner.to_owned();
    tokio::spawn(async move {
        lock.acquire_guard(
            &crate::LockRequest::new(key(), kind, owner)
                .with_acquire_timeout(Duration::from_secs(5))
                .with_ttl(Duration::from_secs(30)),
        )
        .await
        .expect("distributed acquire")
        .expect("distributed quorum")
    })
}

async fn cluster_acquired(task: JoinHandle<crate::NamespaceLockGuard>) -> crate::NamespaceLockGuard {
    timeout(Duration::from_secs(3), task)
        .await
        .expect("quorum must progress")
        .expect("cluster task")
}

#[tokio::test]
async fn publication_queue_distributed_quorum_preserves_bidirectional_progress() {
    let managers = (0..3).map(|_| manager()).collect::<Vec<_>>();
    let clients = managers
        .iter()
        .map(|manager| {
            Arc::new(crate::LocalClient::with_manager(Arc::new(crate::GlobalLockManager::Enabled(
                manager.clone(),
            )))) as Arc<dyn crate::LockClient>
        })
        .collect::<Vec<_>>();
    let lock = Arc::new(crate::NamespaceLock::with_clients("publication-fairness".to_owned(), clients));
    // Hold every voter while establishing a deterministic arrival order.
    // A distributed holder may return at quorum and release its spare vote.
    let mut holders = Vec::new();
    for manager in &managers {
        holders.push(manager.acquire_write_lock(key(), "holder").await.expect("hold voter"));
    }
    let w1 = cluster_queued(&lock, "w1", crate::LockType::Exclusive);
    cluster_waiters(&managers, 1).await;
    let r1 = cluster_queued(&lock, "r1", crate::LockType::Shared);
    cluster_waiters(&managers, 2).await;
    let w2 = cluster_queued(&lock, "w2", crate::LockType::Exclusive);
    cluster_waiters(&managers, 3).await;
    let r2 = cluster_queued(&lock, "r2", crate::LockType::Shared);
    cluster_waiters(&managers, 4).await;
    drop(holders);
    let w1 = cluster_acquired(w1).await;
    assert!(!r1.is_finished() && !w2.is_finished() && !r2.is_finished());
    drop(w1);
    let r1 = cluster_acquired(r1).await;
    assert!(!w2.is_finished() && !r2.is_finished());
    drop(r1);
    let w2 = cluster_acquired(w2).await;
    assert!(!r2.is_finished());
    drop(w2);
    drop(cluster_acquired(r2).await);
    timeout(Duration::from_secs(3), async {
        while managers.iter().any(|manager| manager.current_lock_counts() != (0, 0)) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all quorum guards and pending cleanup must release");
}

#[tokio::test]
async fn publication_queue_distributed_cancel_and_timeout_leave_no_orphan_turns() {
    let managers = (0..3).map(|_| manager()).collect::<Vec<_>>();
    let clients = managers
        .iter()
        .map(|manager| {
            Arc::new(crate::LocalClient::with_manager(Arc::new(crate::GlobalLockManager::Enabled(
                manager.clone(),
            )))) as Arc<dyn crate::LockClient>
        })
        .collect::<Vec<_>>();
    let lock = Arc::new(crate::NamespaceLock::with_clients("publication-cancellation".to_owned(), clients));
    let mut holders = Vec::new();
    for manager in &managers {
        holders.push(manager.acquire_write_lock(key(), "holder").await.expect("hold voter"));
    }
    let cancelled = cluster_queued(&lock, "cancelled", crate::LockType::Exclusive);
    cluster_waiters(&managers, 1).await;
    cancelled.abort();
    assert!(cancelled.await.expect_err("cancel distributed waiter").is_cancelled());
    timeout(Duration::from_secs(3), async {
        while managers.iter().any(|manager| manager.current_lock_counts().1 != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted peer attempts must remove their tickets");
    let timed_out = lock
        .acquire_guard(
            &crate::LockRequest::new(key(), crate::LockType::Exclusive, "timeout")
                .with_acquire_timeout(Duration::from_millis(100)),
        )
        .await
        .expect("contention is a failed acquire");
    assert!(timed_out.is_none());
    timeout(Duration::from_secs(3), async {
        while managers.iter().any(|manager| manager.current_lock_counts().1 != 0) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("timed out peer attempts must remove their tickets");
    let reader = cluster_queued(&lock, "next-reader", crate::LockType::Shared);
    cluster_waiters(&managers, 1).await;
    let writer = cluster_queued(&lock, "next-writer", crate::LockType::Exclusive);
    cluster_waiters(&managers, 2).await;
    drop(holders);
    let reader = cluster_acquired(reader).await;
    assert!(!writer.is_finished());
    drop(reader);
    drop(cluster_acquired(writer).await);
    timeout(Duration::from_secs(3), async {
        while managers.iter().any(|manager| manager.current_lock_counts() != (0, 0)) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("distributed holders and queues must drain");
}

#[tokio::test]
async fn publication_queue_does_not_change_ordinary_key_writer_preference() {
    let manager = manager();
    let ordinary = ObjectKey::new("publication-bucket", "ordinary-key");
    let holder = manager
        .acquire_write_lock(ordinary.clone(), "holder")
        .await
        .expect("ordinary holder");
    let reader = tokio::spawn({
        let manager = manager.clone();
        let ordinary = ordinary.clone();
        async move { manager.acquire_read_lock(ordinary, "reader").await.expect("ordinary reader") }
    });
    waiters(&manager, 1).await;
    let writer = tokio::spawn({
        let manager = manager.clone();
        async move { manager.acquire_write_lock(ordinary, "writer").await.expect("ordinary writer") }
    });
    waiters(&manager, 2).await;
    drop(holder);
    let writer = acquired(writer).await;
    assert!(!reader.is_finished(), "ordinary keys retain writer preference");
    drop(writer);
    drop(acquired(reader).await);
    assert_eq!(manager.current_lock_counts(), (0, 0));
}
