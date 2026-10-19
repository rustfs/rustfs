// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A bucket whose walk stops early without exhausting the cycle budget (here
//! a sweep resumed under a newer plan, the `mixed_coverage` case of
//! rustfs/rustfs#8494) no longer makes the whole cycle `Incomplete`. The set
//! publishes the bucket's last certified value, the cycle completes with the
//! bucket pending, and the next cycle walks it again. A bucket that never
//! certified keeps the cycle incomplete.

use super::*;

const BUCKETS: [&str; 3] = ["cold-bucket", "hot-bucket", "idle-bucket"];
const LEADER_EPOCH: u64 = 11;

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

struct CycleOptions {
    baseline: Option<Bytes>,
    requires_full_scan: bool,
    max_objects: Option<u64>,
    resolved_scope_observer: Option<tokio::sync::oneshot::Sender<ScannerBucketScanScope>>,
}

/// Run one cycle; an incomplete cycle may publish nothing or an observation.
async fn run_cycle(store: &Arc<ECStore>, cycle: u64, options: CycleOptions) -> (ScannerCycleResult, Option<DataUsageInfo>) {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects: options.max_objects,
            ..Default::default()
        },
    );
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx: budget.token(),
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: LEADER_EPOCH,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: options.baseline,
                observed_usage_candidate: None,
                requires_full_scan: options.requires_full_scan,
                service_cohort: None,
                resolved_scope_observer: options.resolved_scope_observer,
                post_walk_gate: None,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    let snapshot = receiver.recv().await;
    assert!(receiver.recv().await.is_none(), "a cycle publishes at most one terminal snapshot");
    (result, snapshot)
}

fn baseline_bytes(snapshot: &DataUsageInfo) -> Bytes {
    Bytes::from(serde_json::to_vec(snapshot).expect("snapshot should encode"))
}

fn complete_cycle(requires_full_scan: bool, baseline: Option<&DataUsageInfo>) -> CycleOptions {
    CycleOptions {
        baseline: baseline.map(baseline_bytes),
        requires_full_scan,
        max_objects: None,
        resolved_scope_observer: None,
    }
}

#[tokio::test]
#[serial]
async fn partial_bucket_with_a_certified_value_completes_the_cycle_as_pending() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "p0/initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: a quiet full walk certifies every bucket.
    let (result, first) = run_cycle(&store, 1, complete_cycle(true, None)).await;
    let first = first.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());

    // Cycle 2: a scoped walk of the written bucket leaves the set cache
    // under the scoped plan, which the full walk below must still seed from.
    for object in ["p1/second", "p2/second", "p3/second"] {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    record_dirty_usage_bucket("hot-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (result, second) = run_cycle(&store, 2, complete_cycle(false, Some(&first))).await;
    let second = second.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(second.usage_snapshot_converged, Some(true));
    assert_eq!(second.buckets_usage["hot-bucket"].objects_count, 4);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());

    // Cycle 3: the object budget stops the hot bucket's walk and leaves a
    // checkpoint under this cycle's plan; the cycle itself is incomplete.
    for object in ["p4/third", "p5/third"] {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    record_dirty_usage_bucket("hot-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (result, _) = run_cycle(
        &store,
        3,
        CycleOptions {
            baseline: Some(baseline_bytes(&second)),
            requires_full_scan: false,
            max_objects: Some(2),
            resolved_scope_observer: None,
        },
    )
    .await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete, "an exhausted budget stays incomplete");
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let mut interrupted = DataUsageCache::default();
    interrupted
        .load(set, &path_join_buf(&["hot-bucket", DATA_USAGE_CACHE_NAME]))
        .await
        .expect("the interrupted sweep should leave a bucket cache");
    assert!(!interrupted.info.snapshot_complete);
    assert!(interrupted.has_scan_position(), "the interrupted sweep keeps a durable position");

    // Cycle 4: the checkpoint resumes under a different plan, so the sweep
    // is mixed coverage and returns Partial without exhausting any budget.
    // The set stands in with the last certified value and the cycle
    // completes with the bucket pending instead of being incomplete.
    let (result, fourth) = run_cycle(&store, 4, complete_cycle(true, Some(&second))).await;
    let fourth = fourth.expect("a complete cycle publishes");
    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "a partial walk behind a certified value no longer makes the cycle incomplete"
    );
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert!(!fourth.usage_snapshot_observational, "the publication stays authoritative");
    assert_eq!(fourth.usage_snapshot_converged, Some(false));
    assert_eq!(fourth.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert_eq!(fourth.usage_snapshot_pending_bucket_count, 1);
    assert!(fourth.is_complete_bucket_usage_snapshot());
    assert_eq!(
        fourth.buckets_usage["hot-bucket"].objects_count, 4,
        "the pending bucket carries its last certified value, not the partial walk"
    );
    assert_eq!(fourth.buckets_usage["cold-bucket"].objects_count, 1);
    assert_eq!(fourth.buckets_usage["idle-bucket"].objects_count, 1);
    assert_eq!(fourth.objects_total_count, 6);
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests().keys().cloned().collect::<Vec<_>>(),
        vec!["hot-bucket".to_string()],
        "a partial walk certifies nothing: the bucket stays dirty"
    );

    // Cycle 5: even with the dirty map gone, the baseline's pending list
    // scopes the walk to the bucket, whose fresh sweep certifies it.
    clear_dirty_usage_buckets_for_tests();
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (result, fifth) = run_cycle(
        &store,
        5,
        CycleOptions {
            baseline: Some(baseline_bytes(&fourth)),
            requires_full_scan: false,
            max_objects: None,
            resolved_scope_observer: Some(observer),
        },
    )
    .await;
    let fifth = fifth.expect("a complete cycle publishes");
    let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["hot-bucket".to_string()])),
        "the next cycle walks the pending bucket"
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(fifth.usage_snapshot_converged, Some(true));
    assert!(fifth.usage_snapshot_pending_buckets.is_empty());
    assert_eq!(fifth.buckets_usage["hot-bucket"].objects_count, 6);
    assert_eq!(fifth.buckets_usage["cold-bucket"].objects_count, 1);
    assert_eq!(fifth.objects_total_count, 8);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn partial_bucket_without_a_certified_value_keeps_the_cycle_incomplete() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket("fresh-bucket", &MakeBucketOptions::default())
        .await
        .expect("bucket should be created");
    for object in ["p0/a", "p1/b", "p2/c", "p3/d"] {
        put_fixture_object(&store, "fresh-bucket", object).await;
    }
    record_dirty_usage_bucket("fresh-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: the budget stops the only bucket's first walk; no set cache
    // was ever published, so nothing can stand in for the bucket.
    let (result, _) = run_cycle(
        &store,
        1,
        CycleOptions {
            baseline: None,
            requires_full_scan: true,
            max_objects: Some(2),
            resolved_scope_observer: None,
        },
    )
    .await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);

    // Cycle 2: the checkpoint resumes under another plan and returns
    // Partial. The bucket has no certified value, so the cycle stays
    // incomplete and publishes no authoritative snapshot.
    let (result, second) = run_cycle(&store, 2, complete_cycle(false, None)).await;
    assert_eq!(
        result.status,
        ScannerCycleStatus::Incomplete,
        "a bucket that never certified cannot be carried"
    );
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert!(
        second.is_none_or(|snapshot| snapshot.usage_snapshot_observational || !snapshot.usage_snapshot_complete),
        "an incomplete cycle publishes at most an observation"
    );
    assert_eq!(dirty_usage_buckets_for_tests().len(), 1, "the bucket stays dirty");

    // Cycle 3: the fresh sweep under one plan certifies the bucket.
    let (result, third) = run_cycle(&store, 3, complete_cycle(false, None)).await;
    let third = third.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(third.usage_snapshot_converged, Some(true));
    assert_eq!(third.buckets_usage["fresh-bucket"].objects_count, 4);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}
