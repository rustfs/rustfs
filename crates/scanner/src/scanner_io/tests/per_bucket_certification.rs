// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Per-bucket certification on the single-disk fixture: a bucket written
//! during the walk is reported pending while the other buckets are certified,
//! the publication stays authoritative, and the next cycle is scoped to the
//! pending bucket. An in-flight namespace commit that names its bucket only
//! shades that bucket; a guardrail veto, including a commit that cannot be
//! attributed, still supersedes the whole cycle.

use super::*;

const BUCKETS: [&str; 3] = ["cold-bucket", "hot-bucket", "idle-bucket"];

async fn put_fixture_object(store: &Arc<ECStore>, bucket: &str, object: &str) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture object should persist");
}

struct CycleHooks {
    resolved_scope_observer: Option<tokio::sync::oneshot::Sender<ScannerBucketScanScope>>,
    post_walk_gate: Option<tokio::sync::oneshot::Receiver<()>>,
}

async fn run_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    baseline: Option<Bytes>,
    requires_full_scan: bool,
    hooks: CycleHooks,
) -> (ScannerCycleResult, DataUsageInfo) {
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
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
                resolved_scope_observer: hooks.resolved_scope_observer,
                post_walk_gate: hooks.post_walk_gate,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should succeed");
    let snapshot = receiver.recv().await.expect("cycle should publish a snapshot");
    assert!(receiver.recv().await.is_none(), "a cycle publishes one terminal snapshot");
    (result, snapshot)
}

fn baseline_bytes(snapshot: &DataUsageInfo) -> Bytes {
    Bytes::from(serde_json::to_vec(snapshot).expect("snapshot should encode"))
}

#[tokio::test]
#[serial]
async fn write_during_walk_leaves_only_that_bucket_pending_and_scopes_the_next_cycle() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // Cycle 1: a quiet full walk certifies every bucket.
    let (result, first) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
        },
    )
    .await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(first.usage_snapshot_converged, Some(true));
    assert!(!first.usage_snapshot_observational);
    assert!(first.usage_snapshot_pending_buckets.is_empty());
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty(), "a converged cycle clears the dirty map");

    // Cycle 2: cold and idle are dirty at the start snapshot; hot is written
    // after the scope resolved and before the post-walk proof runs.
    for bucket in ["cold-bucket", "idle-bucket"] {
        put_fixture_object(&store, bucket, "second").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let writer_store = store.clone();
    let writer = tokio::spawn(async move {
        let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
        put_fixture_object(&writer_store, "hot-bucket", "during-walk").await;
        record_dirty_usage_bucket("hot-bucket");
        wait_for_namespace_commit_tails(writer_store.as_ref()).await;
        release.send(()).expect("the cycle should still be waiting on the gate");
        scope
    });
    let (result, second) = run_cycle(
        &store,
        2,
        Some(baseline_bytes(&first)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: Some(gate),
        },
    )
    .await;
    let scope = writer.await.expect("writer task should finish");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["cold-bucket".to_string(), "idle-bucket".to_string()])),
        "the converged baseline scopes cycle 2 to the buckets dirty at its start"
    );

    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "a write during the walk no longer supersedes"
    );
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert!(!second.usage_snapshot_observational, "pending buckets publish on the authoritative route");
    assert_eq!(second.usage_snapshot_converged, Some(false));
    assert_eq!(second.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert_eq!(second.usage_snapshot_pending_bucket_count, 1);
    assert!(second.is_complete_bucket_usage_snapshot());
    assert_eq!(second.buckets_usage["cold-bucket"].objects_count, 2);
    assert_eq!(second.buckets_usage["idle-bucket"].objects_count, 2);
    assert_eq!(
        second.buckets_usage["hot-bucket"].objects_count, 1,
        "the pending bucket carries its last walked value"
    );
    assert!(result.has_dirty_usage_to_acknowledge());
    assert!(result.clear_verified_usage().is_empty());
    let dirty = dirty_usage_buckets_for_tests();
    assert_eq!(
        dirty.keys().cloned().collect::<Vec<_>>(),
        vec!["hot-bucket".to_string()],
        "certification clears the unchanged buckets and keeps the written one"
    );

    // Cycle 3: the unconverged authoritative snapshot is still a structural
    // baseline, so the walk is scoped to the pending bucket.
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (result, third) = run_cycle(
        &store,
        3,
        Some(baseline_bytes(&second)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: None,
        },
    )
    .await;
    let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["hot-bucket".to_string()])),
        "the next cycle walks only the pending bucket"
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(third.usage_snapshot_converged, Some(true));
    assert!(third.usage_snapshot_pending_buckets.is_empty());
    assert_eq!(third.buckets_usage["hot-bucket"].objects_count, 2);
    assert_eq!(third.buckets_usage["cold-bucket"].objects_count, 2);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn held_put_tail_leaves_only_its_bucket_pending_instead_of_superseding() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let vetoes_before = global_metrics().report().await.usage_freshness.guardrail_veto_total;

    // A continuously written single node always has some rename tail in
    // flight. Hold one across the whole cycle, publication included: it
    // names its bucket, so it shades that bucket instead of vetoing.
    let pending_commit = ecstore_hold_bucket_namespace_commit(store.as_ref(), "hot-bucket", "tail/object");
    assert!(store.scanner_data_usage_publication_blocked().await);
    let (result, first) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
        },
    )
    .await;

    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "an attributed in-flight commit no longer supersedes the cycle"
    );
    assert_eq!(result.pending_dirty_usage_buckets(), 1);
    assert!(!first.usage_snapshot_observational, "pending buckets publish on the authoritative route");
    assert_eq!(first.usage_snapshot_converged, Some(false));
    assert_eq!(first.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert!(first.is_complete_bucket_usage_snapshot());
    for bucket in BUCKETS {
        assert_eq!(first.buckets_usage[bucket].objects_count, 1, "{bucket} carries its walked value");
    }
    assert!(result.has_dirty_usage_to_acknowledge());
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests().keys().cloned().collect::<Vec<_>>(),
        vec!["hot-bucket".to_string()],
        "certification clears the quiet buckets and keeps the bucket with the in-flight commit"
    );
    assert_eq!(
        global_metrics().report().await.usage_freshness.guardrail_veto_total,
        vetoes_before,
        "an attributed commit is not a guardrail veto"
    );

    // The tail drains between cycles, before the next walk records its
    // floor, so that cycle walks only the hot bucket and converges.
    drop(pending_commit);
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (result, second) = run_cycle(
        &store,
        2,
        Some(baseline_bytes(&first)),
        false,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: None,
        },
    )
    .await;
    let scope = resolved_scope.await.expect("the cycle should report its resolved scope");
    assert_eq!(
        scope.selected_buckets_for_tests(),
        Some(&HashSet::from(["hot-bucket".to_string()])),
        "the next cycle walks only the bucket the tail shaded"
    );
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert_eq!(second.usage_snapshot_converged, Some(true));
    assert!(second.usage_snapshot_pending_buckets.is_empty());
    assert_eq!(second.buckets_usage["hot-bucket"].objects_count, 1);
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty());

    // A commit with no S3 dirty mark (a heal rewrite) on a bucket that is not
    // dirty: holding it pending is not enough, so the collector shades it and
    // the following cycle walks it.
    let heal_commit = ecstore_hold_bucket_namespace_commit(store.as_ref(), "idle-bucket", "healed/object");
    let (result, third) = run_cycle(
        &store,
        3,
        Some(baseline_bytes(&second)),
        false,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
        },
    )
    .await;
    drop(heal_commit);
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(third.usage_snapshot_pending_buckets, vec!["idle-bucket".to_string()]);
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests().keys().cloned().collect::<Vec<_>>(),
        vec!["idle-bucket".to_string()],
        "an unmarked commit's bucket is shaded into the dirty map"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn put_tail_that_ends_during_the_walk_leaves_its_bucket_pending() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;

    // The S3 layer marks a PUT dirty when the request returns, which for an
    // early-ACK PUT is before its rename tail ends. A tail that ends during
    // the walk therefore never moves the generation certification compares;
    // its ended card is what keeps the bucket grey.
    let pending_commit = ecstore_hold_bucket_namespace_commit(store.as_ref(), "hot-bucket", "tail/object");
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let ender = tokio::spawn(async move {
        resolved_scope.await.expect("the cycle should report its resolved scope");
        drop(pending_commit);
        release.send(()).expect("the cycle should still be waiting on the gate");
    });
    let (result, snapshot) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: Some(observer),
            post_walk_gate: Some(gate),
        },
    )
    .await;
    ender.await.expect("the tail owner task should finish");

    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(snapshot.usage_snapshot_pending_buckets, vec!["hot-bucket".to_string()]);
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests().keys().cloned().collect::<Vec<_>>(),
        vec!["hot-bucket".to_string()],
        "the tail's end shades its bucket even though the S3 mark predates the walk"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn unattributed_namespace_commit_still_supersedes_the_cycle_onto_the_observed_route() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in BUCKETS {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        put_fixture_object(&store, bucket, "initial").await;
        record_dirty_usage_bucket(bucket);
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
    let vetoes_before = global_metrics().report().await.usage_freshness.guardrail_veto_total;

    // Every production commit names its bucket, so a held PUT tail now only
    // shades that bucket (see the held-tail test above). A commit that cannot
    // be attributed (an owner without a bucket name, an exhausted commit
    // counter, or an end that did not fit the card table) still flips the
    // local publication barrier, which is a guardrail veto regardless of the
    // dirty map.
    let pending_commit = ecstore_hold_namespace_commit(store.as_ref());
    assert!(store.scanner_data_usage_publication_blocked().await);
    let (result, snapshot) = run_cycle(
        &store,
        1,
        None,
        true,
        CycleHooks {
            resolved_scope_observer: None,
            post_walk_gate: None,
        },
    )
    .await;
    drop(pending_commit);

    assert_eq!(result.status, ScannerCycleStatus::Superseded);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert!(snapshot.usage_snapshot_observational, "a vetoed cycle routes to the observed object");
    assert_eq!(snapshot.usage_snapshot_converged, Some(false));
    assert!(snapshot.usage_snapshot_pending_buckets.is_empty());
    assert!(snapshot.is_complete_bucket_usage_snapshot());
    assert!(!result.has_dirty_usage_to_acknowledge(), "nothing is certified under a veto");
    assert_eq!(dirty_usage_buckets_for_tests().len(), BUCKETS.len(), "the dirty map is untouched");
    let freshness = global_metrics().report().await.usage_freshness;
    assert_eq!(freshness.guardrail_veto_total, vetoes_before + 1);
    assert_eq!(
        freshness.last_guardrail_veto_reason,
        format!("publication_blocked@{}", crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE)
    );
    clear_dirty_usage_buckets_for_tests();
}
