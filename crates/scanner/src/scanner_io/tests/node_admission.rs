// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::workload_admission::ScannerNodeRequest;
use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use std::num::NonZeroU16;
use tokio::sync::oneshot;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ResumeCase {
    Unchanged,
    Updated,
    Cancelled,
}

async fn check_cache_lease_resume(case: ResumeCase) {
    let (_directory, store) = setup_local_scanner_store(1, 4).await;
    let set = store.pools[0].disk_set[0].clone();
    let source = DataUsageCacheSource::new(0, 0);
    let cache_name = "node-resume/.usage-cache.bin";
    let cache = DataUsageCache::default();
    cache.save(set.clone(), cache_name).await.expect("seed cache");
    let revisions = DataUsageCache::read_revisions(set.clone(), cache_name)
        .await
        .expect("initial revisions");
    let gate = NodeAdmission::new(NonZeroU16::new(1).expect("one shared unit"));
    gate.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    let mut held = Some(gate.acquire().await);
    let mut guard = acquire_scanner_cache_locks(set.as_ref(), cache_name, source)
        .await
        .expect("initial cache lease");
    let ctx = CancellationToken::new();
    let (respond, response) = oneshot::channel();
    let request = ScannerNodeRequest {
        owner: gate.clone(),
        respond,
        cancel: ctx.clone(),
    };
    let mut expected = revisions.clone();
    let result = {
        let resume = guard.resume_node_operation(set.clone(), cache_name, source, &revisions, request);
        tokio::pin!(resume);
        assert!(futures::poll!(&mut resume).is_pending(), "shared node is occupied");
        let other = tokio::time::timeout(Duration::from_secs(1), acquire_scanner_cache_locks(set.as_ref(), cache_name, source))
            .await
            .expect("cache lock must be released before joining the node queue")
            .expect("another cache worker can acquire the lease");
        if case == ResumeCase::Updated {
            let mut newer = cache;
            newer.info.next_cycle = 1;
            newer
                .save(set.clone(), cache_name)
                .await
                .expect("another worker updates the cache");
            expected = DataUsageCache::read_revisions(set.clone(), cache_name)
                .await
                .expect("new cache revisions");
            assert_ne!(expected, revisions);
        }
        drop(other);
        if case == ResumeCase::Cancelled {
            ctx.cancel();
        } else {
            held.take().expect("held node unit").complete();
        }
        tokio::time::timeout(Duration::from_secs(2), resume)
            .await
            .expect("node resumption must finish or observe cancellation")
    };
    if case == ResumeCase::Unchanged {
        result.expect("unchanged cache resumes the same walk");
        let permit = response.await.expect("coordinator returns the node permit");
        assert_eq!(gate.snapshot().active, 1);
        assert_eq!(gate.completed(), Some(1), "admitting scanner work must not count it as completed");
        assert!(!guard.is_lock_lost());
        let lock = set
            .new_ns_lock(RUSTFS_META_BUCKET, &scanner_cache_lock_resource(cache_name, source))
            .await
            .expect("competing cache lock");
        assert!(
            lock.get_write_lock_quiet(Duration::from_millis(20)).await.is_err(),
            "cache lease must be restored before the operation resumes"
        );
        permit.complete();
    } else {
        result.expect_err("cancelled or stale work cannot resume");
        assert!(response.await.is_err(), "failed resumption must not authorize storage work");
        if case == ResumeCase::Cancelled {
            assert!(guard.is_lock_lost());
            assert_eq!(gate.snapshot().active, 1, "the running heal keeps its unit");
            drop(held.take());
            assert_eq!(gate.completed(), Some(0));
        } else {
            assert_eq!(gate.completed(), Some(1));
        }
    }
    drop(guard);
    assert_eq!(gate.snapshot().active, 0);
    assert!(gate.try_acquire().is_some(), "dropped requests must leave no queue or capacity leak");
    assert_eq!(
        DataUsageCache::read_revisions(set.clone(), cache_name)
            .await
            .expect("final revisions"),
        expected
    );
    acquire_scanner_cache_locks(set.as_ref(), cache_name, source)
        .await
        .expect("final cache lease is available");
}

#[tokio::test]
#[serial]
async fn scanner_node_wait_yields_cache_lease_before_fifo_admission() {
    check_cache_lease_resume(ResumeCase::Unchanged).await;
}

#[tokio::test]
#[serial]
async fn scanner_node_wait_rejects_cache_changed_while_unlocked() {
    check_cache_lease_resume(ResumeCase::Updated).await;
}

#[tokio::test]
#[serial]
async fn scanner_node_wait_cancellation_releases_queue_without_stealing_running_unit() {
    check_cache_lease_resume(ResumeCase::Cancelled).await;
}
