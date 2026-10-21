// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Scope admission across the bucket-listing window on the single-disk
//! fixture, without lifecycle or replication rules. A write that lands in a
//! listed bucket between the listing cutoff and the dirty-usage snapshot is
//! part of the start snapshot and must not force a full scan; a dirty entry
//! removed in the same window still does.

use super::*;

const BUCKETS: [&str; 3] = ["cold-bucket", "hot-bucket", "idle-bucket"];

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

async fn run_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    baseline: Option<Bytes>,
    requires_full_scan: bool,
) -> (ScannerCycleResult, DataUsageInfo, ScannerBucketScanScope) {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let result = tokio::time::timeout(
        Duration::from_secs(60),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx,
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: baseline,
                observed_usage_candidate: None,
                requires_full_scan,
                service_cohort: None,
                resolved_scope_observer: Some(observer),
                post_walk_gate: None,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    let snapshot = receiver.recv().await.expect("cycle should publish a snapshot");
    let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
    (result, snapshot, scope)
}

fn baseline_bytes(snapshot: &DataUsageInfo) -> Bytes {
    Bytes::from(serde_json::to_vec(snapshot).expect("snapshot should encode"))
}

/// Creates the fixture buckets and certifies them with one quiet full walk,
/// returning the converged snapshot as the next cycle's baseline.
async fn converged_baseline(store: &Arc<ECStore>) -> Bytes {
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (result, first, _) = run_cycle(store, 1, None, true).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    baseline_bytes(&first)
}

#[tokio::test]
#[serial]
async fn write_to_a_listed_bucket_during_listing_keeps_the_cycle_scoped() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let baseline = converged_baseline(&store).await;

    put_fixture_object(&store, "cold-bucket", "second").await;
    record_dirty_usage_bucket("cold-bucket");
    put_fixture_object(&store, "hot-bucket", "second").await;
    wait_for_namespace_commit_tails(store.as_ref()).await;
    // The hot bucket's dirty mark lands after the listing cutoff but before
    // the snapshot: under sustained writes some bucket is always in this
    // window, so the listing must not be required to be globally quiet.
    super::io_cycle::set_bucket_listing_window_hook_for_tests(|| record_dirty_usage_bucket("hot-bucket"));

    let (result, second, scope) = run_cycle(&store, 2, Some(baseline), false).await;

    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["cold-bucket".to_string(), "hot-bucket".to_string()])),
        "a listed bucket written during listing joins the start snapshot instead of forcing a full scan"
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.scope_fallback(), None);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(second.usage_snapshot_converged, Some(true));
    assert_eq!(second.buckets_usage["cold-bucket"].objects_count, 2);
    assert_eq!(second.buckets_usage["hot-bucket"].objects_count, 2);
    assert_eq!(second.buckets_usage["idle-bucket"].objects_count, 1);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn bucket_created_during_listing_stays_pending_without_forcing_a_full_scan() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let baseline = converged_baseline(&store).await;

    put_fixture_object(&store, "cold-bucket", "second").await;
    record_dirty_usage_bucket("cold-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    // A bucket the listing did not return cannot be certified by this cycle;
    // it must stay grey and be reported pending, not silently cleared.
    super::io_cycle::set_bucket_listing_window_hook_for_tests(|| record_dirty_usage_bucket("created-after-listing"));

    let (result, second, scope) = run_cycle(&store, 2, Some(baseline), false).await;

    assert_eq!(scope.selected_buckets_for_tests(), Some(&HashSet::from(["cold-bucket".to_string()])));
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.scope_fallback(), None);
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert_eq!(second.usage_snapshot_pending_buckets, vec!["created-after-listing".to_string()]);
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests().keys().cloned().collect::<Vec<_>>(),
        vec!["created-after-listing".to_string()],
        "the unlisted bucket stays dirty for the next cycle"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn dirty_entry_removed_during_listing_still_forces_a_full_scan() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let baseline = converged_baseline(&store).await;

    put_fixture_object(&store, "cold-bucket", "second").await;
    record_dirty_usage_bucket("cold-bucket");
    put_fixture_object(&store, "hot-bucket", "second").await;
    record_dirty_usage_bucket("hot-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    // A removal in the window may be a certification against a publication
    // newer than the baseline this cycle loaded; scoping would then reuse a
    // stale baseline value for that bucket, so the cycle stays a full scan.
    // The unlisted bucket dirtied in the same window stays pending, which is
    // what the leader loop paces instead of waking immediately.
    super::io_cycle::set_bucket_listing_window_hook_for_tests(|| {
        clear_dirty_usage_bucket("hot-bucket");
        record_dirty_usage_bucket("created-after-listing");
    });
    let fallbacks_before = global_metrics().report().await.usage_freshness.scope_fallback_total;

    let (result, second, scope) = run_cycle(&store, 2, Some(baseline), false).await;

    assert!(scope.is_default(), "a removal during listing must not admit a scoped scan");
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.scope_fallback(), Some(ScannerScopeFallbackReason::DirtySnapshotIncomplete));
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert_eq!(
        result.completed_scope_fallback(),
        Some(ScannerScopeFallbackReason::DirtySnapshotIncomplete)
    );
    assert_eq!(second.buckets_usage["hot-bucket"].objects_count, 2);
    assert_eq!(second.buckets_usage["cold-bucket"].objects_count, 2);
    let freshness = global_metrics().report().await.usage_freshness;
    assert_eq!(freshness.scope_fallback_total, fallbacks_before + 1);
    assert_eq!(freshness.last_scope_fallback_reason, "dirty_snapshot_incomplete");
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn required_full_scan_is_not_reported_as_a_scope_fallback() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let baseline = converged_baseline(&store).await;
    put_fixture_object(&store, "cold-bucket", "second").await;
    record_dirty_usage_bucket("cold-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    super::io_cycle::set_bucket_listing_window_hook_for_tests(|| record_dirty_usage_bucket("created-after-listing"));

    let (result, _, scope) = run_cycle(&store, 2, Some(baseline), true).await;

    assert!(scope.is_default());
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert_eq!(result.scope_fallback(), None, "scheduled maintenance walks are not degradations");
    assert_eq!(result.completed_scope_fallback(), None);
    clear_dirty_usage_buckets_for_tests();
}
