// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

const CYCLE: u64 = 31;
const EPOCH: u64 = 7;
const RETIRED: &str = "retirement-generation";
const LIVE: &str = "retirement-survivor";

async fn run_turn(
    set: &Arc<SetDisks>,
    buckets: &[BucketInfo],
    continuation: Option<ScannerSourceTurnContinuation>,
    failures: ScannerBucketFailureState,
) -> Result<Option<DataUsageCache>> {
    let owner = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&owner, ScannerCycleBudgetConfig::default());
    budget.use_production_drain_grace_for_tests();
    let epoch = scanner_publication_epoch(set.clone())
        .await
        .expect("fixture publication epoch");
    let tier_generation = crate::runtime_tier_registry_for_cycle(CYCLE, EPOCH).await.generation;
    let (updates, mut receiver) = mpsc::channel(2);
    set.clone()
        .nsscanner_cache_turn(
            budget.token(),
            budget,
            ScannerSourceTurnRequest {
                scan_plan: ScannerBucketScanPlan {
                    owner_ctx: owner,
                    usage_watch_store: None,
                    service_cohort: None,
                    buckets: buckets.to_vec(),
                    all_buckets: Arc::new(buckets.to_vec()),
                    scope: ScannerBucketScanScope::default(),
                    digest: DataUsageScanPlanDigest([31; 32]),
                    bucket_coverage_digest: DataUsageScanPlanDigest([32; 32]),
                    checkpoint_coverage_digest: DataUsageScanPlanDigest([33; 32]),
                    requires_full_scan: true,
                    execution_digest: DataUsageScanPlanDigest([34; 32]),
                    leader_epoch: EPOCH,
                    tier_registry_generation: tier_generation,
                    publication_epoch: Some(epoch),
                    dirty_usage_buckets: Arc::new(HashMap::new()),
                    bucket_failures: failures,
                    bucket_completions: None,
                    pending_maintenance_work: Arc::new(AtomicBool::new(false)),
                    cache_cycle_floor: Arc::new(AtomicU64::new(CYCLE)),
                    cold_zero_walk_reuse_observed: Arc::new(AtomicBool::new(false)),
                    segment_invalidation_proof: None,
                },
                continuation,
            },
            updates,
            CYCLE,
            HealScanMode::Normal,
        )
        .await?;
    let cache = receiver.recv().await;
    assert!(receiver.recv().await.is_none(), "one source turn emits at most one cache");
    Ok(cache)
}

async fn completed_source(store: &Arc<ECStore>) -> (Vec<BucketInfo>, DataUsageCache) {
    for bucket in [RETIRED, LIVE] {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create source bucket");
    }
    store
        .put_object(
            LIVE,
            "surviving-object",
            &mut ScannerPutObjReader::from_vec(vec![7; 13]),
            &ScannerObjectOptions::default(),
        )
        .await
        .expect("persist unrelated source data");
    wait_for_namespace_commit_tails(store).await;
    let buckets = vec![bucket_info(RETIRED), bucket_info(LIVE)];
    let cache = run_turn(&store.pools[0].disk_set[0], &buckets, None, ScannerBucketFailureState::default())
        .await
        .expect("first real source turn")
        .expect("complete source snapshot");
    assert!(cache.info.snapshot_complete, "the continuation starts from actual completed worker roots");
    assert_eq!(cache.find(LIVE).expect("live bucket root").size, 13);
    (buckets, cache)
}

fn continuation(cache: DataUsageCache) -> ScannerSourceTurnContinuation {
    let completed_buckets = cache
        .find(DATA_USAGE_ROOT)
        .expect("actual source root")
        .children
        .iter()
        .cloned()
        .collect();
    ScannerSourceTurnContinuation {
        cache,
        completed_buckets,
    }
}

async fn assert_retired_source_turn_discards_only_deleted_generation_and_retains_pending_work() {
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    let (buckets, before) = completed_source(&store).await;
    let set = &store.pools[0].disk_set[0];
    let identity = before.info.scan_bucket_incarnations[RETIRED];
    assert!(
        !set.is_bucket_generation_retired(RETIRED, identity)
            .await
            .expect("live generation proof")
    );
    store
        .delete_bucket(RETIRED, &DeleteBucketOptions::default())
        .await
        .expect("delete empty generation");
    wait_for_namespace_commit_tails(&store).await;
    assert!(
        set.is_bucket_generation_retired(RETIRED, identity)
            .await
            .expect("durable retirement proof")
    );
    let failures = ScannerBucketFailureState::default();
    let after = run_turn(set, &buckets, Some(continuation(before)), failures.clone())
        .await
        .expect("retirement must not fail the unrelated source turn")
        .expect("pending source snapshot");
    assert!(!after.info.snapshot_complete, "an old plan cannot certify a deleted name as empty");
    assert!(after.find(RETIRED).is_none(), "retired values must leave the cache");
    assert!(!after.info.scan_bucket_incarnations.contains_key(RETIRED));
    assert_eq!(
        (
            after.find(LIVE).expect("surviving bucket").objects,
            after.find(LIVE).expect("surviving bucket").size
        ),
        (1, 13)
    );
    assert!(failures.hard.lock().await.is_empty(), "confirmed retirement is not a worker failure");
    assert!(
        failures.partial.lock().await.contains(RETIRED),
        "responsibility remains until namespace replanning"
    );
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove source retirement fixture");
    assert!(!path.exists());
}

async fn assert_recreated_source_turn_rescans_successor_without_old_value_or_identity() {
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    let (buckets, before) = completed_source(&store).await;
    let old_identity = before.info.scan_bucket_incarnations[RETIRED];
    store
        .delete_bucket(RETIRED, &DeleteBucketOptions::default())
        .await
        .expect("retire predecessor");
    store
        .make_bucket(RETIRED, &MakeBucketOptions::default())
        .await
        .expect("create successor");
    for (key, size) in [("new-first", 11), ("new-second", 17)] {
        store
            .put_object(
                RETIRED,
                key,
                &mut ScannerPutObjReader::from_vec(vec![8; size]),
                &ScannerObjectOptions::default(),
            )
            .await
            .expect("successor object");
    }
    wait_for_namespace_commit_tails(&store).await;
    let set = &store.pools[0].disk_set[0];
    let new_identity = set
        .bucket_incarnation_id_from_disk(RETIRED)
        .await
        .expect("successor identity");
    assert_ne!(old_identity, new_identity);
    let after = run_turn(set, &buckets, Some(continuation(before)), ScannerBucketFailureState::default())
        .await
        .expect("successor must be independently rescanned")
        .expect("successor snapshot");
    assert!(after.info.snapshot_complete);
    let successor = after.find(RETIRED).expect("new bucket value");
    assert_eq!((successor.objects, successor.size), (2, 28));
    assert_eq!(after.info.scan_bucket_incarnations[RETIRED], new_identity);
    assert_eq!(after.find(LIVE).expect("unrelated bucket").size, 13);
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove source retirement fixture");
    assert!(!path.exists());
}

async fn assert_missing_retirement_proof_and_foreign_source_fences_cannot_clear_completion() {
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    let (buckets, before) = completed_source(&store).await;
    let set = &store.pools[0].disk_set[0];
    assert!(!scanner_bucket_generation_is_retired(set, RETIRED, None).await);
    assert!(!scanner_bucket_generation_is_retired(set, RETIRED, Some(Uuid::nil())).await);
    for field in ["bucket", "source", "cycle", "epoch", "plan", "tier"] {
        let mut old = before.clone();
        match field {
            "bucket" => {
                old.info.scan_bucket_incarnations.insert(RETIRED.to_string(), Uuid::new_v4());
            }
            "source" => old.info.source = Some(DataUsageCacheSource::new(9, 9)),
            "cycle" => old.info.next_cycle += 1,
            "epoch" => old.info.leader_epoch += 1,
            "plan" => old.info.scan_plan_digest = Some(DataUsageScanPlanDigest([99; 32])),
            "tier" => old.info.tier_registry_generation = Some(u64::MAX),
            _ => unreachable!("enumerated source fence"),
        }
        let error = run_turn(set, &buckets, Some(continuation(old)), ScannerBucketFailureState::default())
            .await
            .expect_err("unconfirmed identity changes cannot retire completed work");
        assert!(error.to_string().contains("identity"), "{field}: {error}");
    }
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove source retirement fixture");
    assert!(!path.exists());
}

async fn assert_missing_directories_and_unreadable_retirement_proof_remain_unconfirmed() {
    let (directory, store) = setup_local_scanner_store(1, 4).await;
    let (buckets, before) = completed_source(&store).await;
    let set = &store.pools[0].disk_set[0];
    let identity = before.info.scan_bucket_incarnations[RETIRED];
    // A single lost volume is recoverable and cannot prove bucket deletion.
    tokio::fs::remove_dir_all(directory.path().join("pool0-disk0").join(RETIRED))
        .await
        .expect("remove only this fixture's empty volume");
    assert!(!scanner_bucket_generation_is_retired(set, RETIRED, Some(identity)).await);
    store
        .delete_bucket(RETIRED, &DeleteBucketOptions::default())
        .await
        .expect("retire empty generation");
    save_config(
        store.clone(),
        &format!("bucket-retirements/{RETIRED}/{identity}.json"),
        b"invalid-retirement-record".to_vec(),
    )
    .await
    .expect("write malformed task-owned proof");
    assert!(set.is_bucket_generation_retired(RETIRED, identity).await.is_err());
    assert!(!scanner_bucket_generation_is_retired(set, RETIRED, Some(identity)).await);
    let error = run_turn(set, &buckets, Some(continuation(before)), ScannerBucketFailureState::default())
        .await
        .expect_err("an unreadable retirement must preserve failed completion validation");
    assert!(error.to_string().contains("identity"));
    drop(store);
    let path = directory.path().to_path_buf();
    directory.close().expect("remove source retirement fixture");
    assert!(!path.exists());
}

#[tokio::test]
#[serial]
async fn retired_source_turn_discards_only_deleted_generation_and_retains_pending_work() {
    assert_retired_source_turn_discards_only_deleted_generation_and_retains_pending_work().await;
}

#[tokio::test]
#[serial]
async fn recreated_source_turn_rescans_successor_without_old_value_or_identity() {
    assert_recreated_source_turn_rescans_successor_without_old_value_or_identity().await;
}

#[tokio::test]
#[serial]
async fn missing_retirement_proof_and_foreign_source_fences_cannot_clear_completion() {
    assert_missing_retirement_proof_and_foreign_source_fences_cannot_clear_completion().await;
}

#[tokio::test]
#[serial]
async fn missing_directories_and_unreadable_retirement_proof_remain_unconfirmed() {
    assert_missing_directories_and_unreadable_retirement_proof_remain_unconfirmed().await;
}

pub(crate) async fn assert_source_turn_retirement_contract() -> serde_json::Value {
    assert_retired_source_turn_discards_only_deleted_generation_and_retains_pending_work().await;
    assert_recreated_source_turn_rescans_successor_without_old_value_or_identity().await;
    assert_missing_retirement_proof_and_foreign_source_fences_cannot_clear_completion().await;
    assert_missing_directories_and_unreadable_retirement_proof_remain_unconfirmed().await;
    clear_dirty_usage_buckets_for_tests();
    serde_json::json!({
        "completed_source_turn_retired_generation_removed": true,
        "unrelated_source_value_retained": true,
        "deleted_name_responsibility_retained_until_replanning": true,
        "successor_independently_scanned": {"objects": 2, "bytes": 28},
        "missing_directories_and_unreadable_retirement_fail_closed": true,
        "source_cycle_epoch_plan_tier_fences_preserved": true,
        "fixtures_removed": true,
    })
}
