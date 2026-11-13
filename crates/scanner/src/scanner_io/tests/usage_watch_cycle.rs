// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.
use super::*;

const BUCKET: &str = "a-watch-long";

async fn put(store: &ECStore, bucket: &str, object: &str, bytes: usize) {
    let mut reader = ScannerPutObjReader::from_vec(vec![7; bytes]);
    store
        .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
        .await
        .expect("fixture PUT");
    wait_for_namespace_commit_tails(store).await;
}

async fn run(
    store: &Arc<ECStore>,
    cohort: Arc<StdMutex<ScannerServiceCohort>>,
    cycle: u64,
    max_objects: Option<u64>,
) -> (ScannerCycleResult, Option<DataUsageInfo>, DataUsageCache) {
    let owner = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &owner,
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
                owner_ctx: owner,
                ctx: budget.token(),
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: None,
                observed_usage_candidate: None,
                requires_full_scan: true,
                service_cohort: Some(cohort),
                resolved_scope_observer: None,
                post_walk_hook: None,
                post_walk_gate: None,
                peer_observations: None,
            },
        ),
    )
    .await
    .expect("bounded cycle")
    .expect("scanner cycle");
    let usage = receiver.recv().await;
    assert!(receiver.recv().await.is_none());
    let mut cache = DataUsageCache::default();
    cache
        .load(store.pools[0].disk_set[0].clone(), &path_join_buf(&[BUCKET, DATA_USAGE_CACHE_NAME]))
        .await
        .expect("durable bucket checkpoint");
    (result, usage, cache)
}

#[tokio::test]
#[serial]
async fn durable_watch_start_survives_three_cycles_and_unrelated_bucket_writes() {
    let (dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("long bucket");
    store
        .make_bucket("z-unrelated", &MakeBucketOptions::default())
        .await
        .expect("unrelated bucket");
    for index in 0..9 {
        put(&store, BUCKET, &format!("prefix-{index:02}/object"), 16).await;
    }
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let (_, _, first) = run(&store, cohort.clone(), 1, Some(2)).await;
    assert!(first.has_scan_position());
    let first_watch = first.info.scan_usage_watch.clone().expect("real storage watch in checkpoint");
    let first_plan = first.info.scan_progress.expect("durable starting plan").started_plan;
    for cycle in 2..=3 {
        put(&store, "z-unrelated", &format!("change-{cycle}"), 32).await;
        let (_, _, continued) = run(&store, cohort.clone(), cycle, Some(2)).await;
        assert!(!continued.info.snapshot_complete, "the long bucket must really span three cycles");
        let progress = continued.info.scan_progress.expect("continued plan");
        assert_eq!(progress.started_plan, first_plan);
        assert_eq!(progress.requested_plan, first_plan, "unrelated writes cannot invalidate coverage");
        assert_eq!(
            continued.info.scan_usage_watch.as_ref().map(|proof| proof.digest()),
            Some(first_watch.digest())
        );
        assert!(first_watch.verify_with_store(store.as_ref()).await);
    }
    let (result, usage, completed) = run(&store, cohort, 4, None).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert!(completed.info.snapshot_complete);
    assert!(completed.completed_bucket_observation().is_none());
    let root = completed.checked_flatten_complete_scope(BUCKET).expect("full bucket");
    assert_eq!(root.objects, 9);
    assert_eq!(root.size, 9 * 16);
    assert!(result.usage_watch_proofs().iter().any(|proof| proof.bucket == BUCKET));
    let usage = usage.expect("complete certified usage");
    assert!(usage.buckets_usage[BUCKET].usage_certification.is_some());
    assert!(!usage.usage_snapshot_pending_buckets.iter().any(|bucket| bucket == BUCKET));
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-02", "assertion_id": "USAGE-02-A02", "status": "PASS",
            "evidence": {"expected_producer_owners": first_watch.peers.len(), "physical_sources": 1, "continued_cycles": [1, 2, 3], "certified_cycle": 4, "objects": root.objects, "bytes": root.size, "watch_digest": first_watch.digest().0, "certification": usage.buckets_usage[BUCKET].usage_certification, "scope": "single-ECStore; no claim for another-node assertion A01"}
        })
    );
    drop(store);
    let path = dir.path().to_path_buf();
    dir.close().expect("remove usage-watch fixture");
    assert!(!path.exists());
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-02", "status": "PASS", "fixture_removed": true})
    );
}

#[tokio::test]
#[serial]
async fn changed_watch_during_resume_preserves_an_observation_then_requires_a_fresh_sweep() {
    let (dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("long bucket");
    for index in 0..6 {
        put(&store, BUCKET, &format!("prefix-{index:02}/object"), 16).await;
    }
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let (_, _, first) = run(&store, cohort.clone(), 1, Some(2)).await;
    assert!(first.has_scan_position());
    let scanned = first
        .checked_flatten(&format!("{BUCKET}/prefix-00"))
        .expect("the changed range was actually scanned before the checkpoint");
    assert_eq!(scanned.objects, 1);
    assert_eq!(scanned.size, 16);
    let watch = first.info.scan_usage_watch.clone().expect("starting watch");
    let before_mutation = store.scanner_namespace_mutation_generation();
    put(&store, BUCKET, "prefix-00/object", 64).await;
    let later_start = store.scanner_namespace_mutation_generation();
    assert!(
        later_start > before_mutation,
        "the real overwrite changed the physical producer generation"
    );
    record_dirty_usage_bucket(BUCKET);
    let dirty_generation = dirty_usage_buckets_for_tests()[BUCKET];
    assert!(!watch.verify_with_store(store.as_ref()).await, "actual PUT invalidates original revision");
    let (result, _, mixed) = run(&store, cohort.clone(), 2, None).await;
    let later_post = store.scanner_namespace_mutation_generation();
    assert_eq!(
        later_start, later_post,
        "later-cycle start/post equality cannot hide the earlier inter-cycle overwrite"
    );
    assert!(!mixed.info.snapshot_complete);
    assert!(!mixed.info.lkg_snapshot_complete);
    assert!(!mixed.has_scan_position());
    assert!(mixed.completed_bucket_observation().is_some());
    assert!(result.usage_watch_proofs().iter().all(|proof| proof.bucket != BUCKET));
    assert!(!result.has_dirty_usage_to_acknowledge());
    assert!(result.clear_verified_usage().is_empty());
    assert_eq!(
        dirty_usage_buckets_for_tests()[BUCKET],
        dirty_generation,
        "mixed EOF cannot clear pending work"
    );
    let (_, _, fresh) = run(&store, cohort, 3, None).await;
    assert!(fresh.info.snapshot_complete);
    assert!(fresh.completed_bucket_observation().is_none());
    let root = fresh.checked_flatten_complete_scope(BUCKET).expect("fresh full bucket");
    assert_eq!(root.objects, 6);
    assert_eq!(root.size, 5 * 16 + 64);
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-03", "assertion_id": "USAGE-03-A02", "status": "PASS",
            "evidence": {"scanned_object_bytes": scanned.size, "overwritten_bytes": 64, "before_mutation_generation": before_mutation, "later_start_generation": later_start, "later_post_generation": later_post}
        })
    );
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-03", "assertion_id": "USAGE-03-A03", "status": "PASS",
            "evidence": {"mixed_primary_complete": mixed.info.snapshot_complete, "mixed_lkg_complete": mixed.info.lkg_snapshot_complete, "mixed_ack_candidates": 0, "retained_dirty_generation": dirty_generation, "fresh_objects": root.objects, "fresh_bytes": root.size}
        })
    );
    clear_dirty_usage_buckets_for_tests();
    drop(store);
    let path = dir.path().to_path_buf();
    dir.close().expect("remove inter-cycle overwrite fixture");
    assert!(!path.exists());
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-03", "status": "PASS", "fixture_removed": true})
    );
}
