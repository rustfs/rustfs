// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! A bucket sweep interrupted by the cycle budget resumes from its checkpoint
//! in a later cycle even after an event that changes the reuse identity but
//! not the namespace. The event here is a maintenance publication; a
//! coordinator restart is the same case for the coverage identity, but a
//! process cannot change its own instance id in a test. The other buckets
//! are walked again; the sweep in progress is not restarted as mixed
//! coverage (rustfs/rustfs#8494).

use super::*;

const HOT_OBJECTS: [&str; 6] = ["p0/a", "p1/b", "p2/c", "p3/d", "p4/e", "p5/f"];
const LEADER_EPOCH: u64 = 11;

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

async fn run_full_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    max_objects: Option<u64>,
) -> (ScannerCycleResult, Option<DataUsageInfo>, Arc<ScannerCycleBudget>) {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects,
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
                budget: Arc::clone(&budget),
                updates,
                want_cycle: cycle,
                leader_epoch: LEADER_EPOCH,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: None,
                observed_usage_candidate: None,
                requires_full_scan: true,
                service_cohort: None,
                resolved_scope_observer: None,
                post_walk_gate: None,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    let snapshot = receiver.recv().await;
    assert!(receiver.recv().await.is_none(), "a cycle publishes at most one terminal snapshot");
    (result, snapshot, budget)
}

#[tokio::test]
#[serial]
async fn checkpoint_resumes_across_a_maintenance_publication() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in ["hot-bucket", "idle-bucket"] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
    }
    for object in HOT_OBJECTS {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    put_fixture_object(&store, "idle-bucket", "p0/a").await;
    record_dirty_usage_bucket("hot-bucket");
    record_dirty_usage_bucket("idle-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: a quiet full walk certifies both buckets.
    let (result, first, _) = run_full_cycle(&store, 1, None).await;
    let first = first.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert_eq!(first.buckets_usage["hot-bucket"].objects_count, 6);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());

    // Cycle 2: the written bucket is the only dirty one, so it is walked
    // first and the object budget stops its sweep with a checkpoint; the
    // cycle itself is incomplete.
    for object in ["p6/g", "p7/h"] {
        put_fixture_object(&store, "hot-bucket", object).await;
    }
    record_dirty_usage_bucket("hot-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (result, _, budget) = run_full_cycle(&store, 2, Some(2)).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(budget.budget_elapsed());
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let mut checkpoint = DataUsageCache::default();
    checkpoint
        .load(Arc::clone(&set), &path_join_buf(&["hot-bucket", DATA_USAGE_CACHE_NAME]))
        .await
        .expect("the interrupted sweep should leave a bucket cache");
    assert!(!checkpoint.info.snapshot_complete);
    assert!(checkpoint.has_scan_position(), "the interrupted sweep must keep a durable position");
    let started_plan = checkpoint
        .info
        .scan_progress
        .expect("the interrupted sweep records its coverage plan")
        .started_plan;

    // A maintenance publication between the cycles changes the reuse
    // identity (finished results are walked again) but not the coverage
    // identity (a sweep in progress keeps its plan).
    let before = crate::scanner::probe_scanner_activity(store.as_ref(), false)
        .await
        .expect("the fixture activity should be observable");
    record_scanner_maintenance_change("idle-bucket");
    let after = crate::scanner::probe_scanner_activity(store.as_ref(), false)
        .await
        .expect("the fixture activity should be observable");
    assert_ne!(
        crate::scanner::scanner_activity_structural_digest(&before),
        crate::scanner::scanner_activity_structural_digest(&after),
        "a maintenance publication invalidates reusable results"
    );
    assert_eq!(
        crate::scanner::scanner_activity_coverage_digest(&before),
        crate::scanner::scanner_activity_coverage_digest(&after),
        "a maintenance publication keeps the coverage plan"
    );

    // Cycle 3: the hot bucket resumes from its checkpoint and certifies;
    // a fresh walk would have read all nine objects.
    let (result, third, budget) = run_full_cycle(&store, 3, None).await;
    let third = third.expect("a complete cycle publishes");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(third.usage_snapshot_converged, Some(true));
    assert_eq!(third.buckets_usage["hot-bucket"].objects_count, 8);
    assert_eq!(third.buckets_usage["idle-bucket"].objects_count, 1);
    assert!(
        budget.progress().0 < 9,
        "the resumed sweep must not restart from the first object: {} objects read",
        budget.progress().0
    );
    let mut completed = DataUsageCache::default();
    completed
        .load(set, &path_join_buf(&["hot-bucket", DATA_USAGE_CACHE_NAME]))
        .await
        .expect("the certified bucket cache should load");
    assert!(completed.info.snapshot_complete);
    assert!(completed.info.scan_progress.is_none());
    assert_ne!(
        completed.info.scan_plan_digest,
        Some(started_plan),
        "a complete cache is current under the reuse plan, not the coverage plan"
    );
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}
