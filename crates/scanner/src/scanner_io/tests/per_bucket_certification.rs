// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Per-bucket certification on the single-disk fixture: a bucket written
//! during the walk is reported pending while the other buckets are certified,
//! the publication stays authoritative, and the next cycle is scoped to the
//! pending bucket. A PUT tail pending across the walk is ordinary namespace
//! activity; a guardrail veto still supersedes the whole cycle.

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
async fn pending_put_tail_across_the_walk_no_longer_vetoes_the_cycle() {
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

    // A PUT whose rename fanout is still pending when the post-walk probe
    // runs used to flip the local publication barrier and supersede the
    // whole cycle (rustfs/rustfs#8494). It is ordinary namespace activity:
    // the walk certifies every bucket and publishes authority.
    let pending_commit = ecstore_hold_namespace_commit(store.as_ref());
    assert!(ecstore_namespace_commits_pending(store.as_ref()));
    assert!(!store.scanner_data_usage_publication_blocked().await);
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

    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(result.pending_dirty_usage_buckets(), 0);
    assert!(!snapshot.usage_snapshot_observational, "the publication takes the authoritative route");
    assert_eq!(snapshot.usage_snapshot_converged, Some(true));
    assert!(snapshot.usage_snapshot_pending_buckets.is_empty());
    assert!(snapshot.is_complete_bucket_usage_snapshot());
    for bucket in BUCKETS {
        assert_eq!(snapshot.buckets_usage[bucket].objects_count, 1);
    }
    assert!(result.clear_verified_usage().is_empty());
    assert!(dirty_usage_buckets_for_tests().is_empty(), "a converged cycle clears the dirty map");
    let freshness = global_metrics().report().await.usage_freshness;
    assert_eq!(
        freshness.guardrail_veto_total, vetoes_before,
        "a pending PUT tail is not a guardrail veto"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn guardrail_veto_still_supersedes_the_cycle_onto_the_observed_route() {
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

    // A maintenance publication during the walk moves a guardrail field. It
    // is not part of the coverage plan, so bucket checkpoints keep their
    // plan, but the cycle it moved in is still superseded regardless of the
    // dirty map: no cycle records whether it published that work.
    let (observer, resolved_scope) = tokio::sync::oneshot::channel();
    let (release, gate) = tokio::sync::oneshot::channel();
    let maintainer = tokio::spawn(async move {
        resolved_scope.await.expect("the cycle should report its resolved scope");
        record_scanner_maintenance_change("hot-bucket");
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
    maintainer.await.expect("the maintenance task should finish");

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
        format!("maintenance_generation@{}", crate::scanner::LOCAL_SCANNER_ACTIVITY_NODE)
    );
    clear_dirty_usage_buckets_for_tests();
}
