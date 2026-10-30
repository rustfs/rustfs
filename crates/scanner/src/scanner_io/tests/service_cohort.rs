// Copyright 2026 RustFS Team
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

use super::*;
use crate::data_usage_define::{DATA_USAGE_OBJ_NAME_PATH, read_config_with_revision};

async fn create_cohort_bucket(store: &ECStore, bucket: &str) {
    create_cohort_bucket_objects(store, bucket, 1).await;
}

async fn create_cohort_bucket_objects(store: &ECStore, bucket: &str, objects: usize) {
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("fixture bucket");
    for set in store.all_set_disks() {
        for index in 0..objects {
            let object = format!("object-{index:04}");
            let mut reader = ScannerPutObjReader::from_vec(b"cohort".to_vec());
            let lock = set.new_ns_lock(bucket, &object).await.expect("fixture namespace lock");
            let guard = WriteCommitGuard::acquire(&lock, Duration::from_secs(30))
                .await
                .expect("fixture write owner");
            let mut opts = ScannerObjectOptions {
                write_completion: WriteCompletion::TailDrained,
                ..Default::default()
            };
            opts.add_write_commit_guard(&guard);
            set.put_object(bucket, &object, &mut reader, &opts)
                .await
                .expect("fixture object and all rename tails should persist");
        }
    }
}

async fn run_cohort_cycle(
    store: &Arc<ECStore>,
    cohort: Arc<StdMutex<ScannerServiceCohort>>,
    cycle: u64,
    budget: Arc<ScannerCycleBudget>,
) -> (ScannerCycleResult, Option<DataUsageInfo>) {
    let root_before = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("root before candidate");
    let dirty_before = dirty_usage_buckets_for_tests();
    let generation_before = dirty_usage_generation();
    let (updates, mut receiver) = mpsc::channel(1);
    let result = tokio::time::timeout(
        Duration::from_secs(30),
        nsscanner_with_storage_status_scoped(
            store.as_ref(),
            ScannerCycleRequest {
                ctx: budget.token(),
                budget,
                updates,
                want_cycle: cycle,
                leader_epoch: 11,
                scan_mode: HealScanMode::Normal,
                scan_scope: ScannerBucketScanScope::default(),
                persisted_usage_baseline: None,
                observed_usage_candidate: None,
                requires_full_scan: false,
                service_cohort: Some(cohort),
                resolved_scope_observer: None,
                post_walk_gate: None,
                peer_observations: None,
            },
        ),
    )
    .await
    .expect("cohort cycle should finish")
    .expect("cohort cycle should return its status");
    let usage = receiver.recv().await;
    assert!(receiver.recv().await.is_none());
    assert_eq!(
        read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("root after candidate"),
        root_before
    );
    assert_eq!(
        dirty_usage_buckets_for_tests(),
        dirty_before,
        "candidate production must not ACK pending work"
    );
    assert_eq!(dirty_usage_generation(), generation_before);
    (result, usage)
}

#[tokio::test]
#[serial]
async fn service_cohort_production_dispatch_services_waiters_across_sources() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let hot = format!("a-hot-{}", Uuid::new_v4().simple());
    let bootstrap = format!("z-bootstrap-{}", Uuid::new_v4().simple());
    create_cohort_bucket(&store, &hot).await;
    create_cohort_bucket(&store, &bootstrap).await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let expected = store
        .all_set_disks()
        .iter()
        .flat_map(|set| {
            let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
            [(source, hot.clone()), (source, bootstrap.clone())]
        })
        .collect::<HashSet<_>>();
    let mut seen = HashSet::new();
    for cycle in 1..=4 {
        // Both newly bootstrapped names and repeated dirty work sort ahead of
        // the original bootstrap in the old dirty-first policy.
        if cycle > 1 {
            create_cohort_bucket(&store, &format!("b-new-{cycle}")).await;
        }
        record_dirty_usage_bucket(&hot);
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(1),
                ..Default::default()
            },
        );
        run_cohort_cycle(&store, cohort.clone(), cycle, budget.clone()).await;
        assert!(budget.budget_elapsed());
        assert_eq!(
            budget.progress().0,
            1,
            "each round must reach one real object, not just mark an admission"
        );
        let admitted = cohort
            .lock()
            .expect("cohort lock")
            .admitted_members()
            .into_iter()
            .collect::<HashSet<_>>();
        let newly_admitted = admitted.difference(&seen).cloned().collect::<Vec<_>>();
        assert_eq!(
            newly_admitted.len(),
            1,
            "serial parent object budget must stop before another bucket admission"
        );
        seen.extend(newly_admitted);
    }
    assert!(
        expected.is_subset(&seen),
        "ongoing dirty/new bootstrap must not displace the original cohort"
    );

    // Admission fairness is not completed coverage: prior budgeted prefixes
    // were observed under changing plans. A clean tail must not certify them.
    let ctx = CancellationToken::new();
    let (result, usage) =
        run_cohort_cycle(&store, cohort, 5, ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default())).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(usage.is_none(), "neither source has a complete mixed-plan baseline to publish");
    for set in store.all_set_disks() {
        let mut cache = DataUsageCache::default();
        cache
            .load(set, &path_join_buf(&[&hot, DATA_USAGE_CACHE_NAME]))
            .await
            .expect("retained hot prefix");
        assert!(!cache.info.snapshot_complete);
        assert!(cache.info.scan_progress.is_some());
        assert!(cache.info.scan_plan_digest.is_none(), "mixed coverage must remain non-authoritative");
    }
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn service_cohort_flat_bucket_budget_does_not_publish_unscanned_small_bucket() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let flat = format!("a-flat-{}", Uuid::new_v4().simple());
    let small = format!("z-small-{}", Uuid::new_v4().simple());
    create_cohort_bucket_objects(&store, &flat, 6).await;
    create_cohort_bucket(&store, &small).await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));

    let expected_flat = store
        .all_set_disks()
        .iter()
        .map(|set| (DataUsageCacheSource::new(set.pool_index, set.set_index), flat.clone()))
        .collect::<HashSet<_>>();
    let expected_small = store
        .all_set_disks()
        .iter()
        .map(|set| (DataUsageCacheSource::new(set.pool_index, set.set_index), small.clone()))
        .collect::<HashSet<_>>();

    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &ctx,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    let (result, usage) = run_cohort_cycle(&store, cohort.clone(), 1, budget.clone()).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(usage.is_none(), "wide-bucket budget exhaustion must not publish a partial aggregate");
    assert!(budget.budget_elapsed());
    let first_round_admitted = cohort
        .lock()
        .expect("cohort lock")
        .admitted_members()
        .into_iter()
        .collect::<HashSet<_>>();
    assert!(
        !first_round_admitted.is_disjoint(&expected_flat),
        "the first fixed budget round should exercise the wide flat bucket"
    );
    assert!(
        first_round_admitted.is_disjoint(&expected_small),
        "a small bucket not yet reached by the real scanner must not be marked admitted"
    );

    for cycle in 2..=4 {
        let ctx = CancellationToken::new();
        let (result, usage) = run_cohort_cycle(
            &store,
            cohort.clone(),
            cycle,
            ScannerCycleBudget::new_with_progress_tracking(
                &ctx,
                ScannerCycleBudgetConfig {
                    max_objects: Some(1),
                    ..Default::default()
                },
            ),
        )
        .await;
        assert_eq!(result.status, ScannerCycleStatus::Incomplete);
        assert!(usage.is_none(), "mixed partial coverage still cannot publish the set root");
    }
    let admitted_after_budgeted_rounds = cohort
        .lock()
        .expect("cohort lock")
        .admitted_members()
        .into_iter()
        .collect::<HashSet<_>>();
    assert!(
        expected_small.is_subset(&admitted_after_budgeted_rounds),
        "tracked small buckets must receive real execution opportunities within their fixed service-round bound"
    );

    let ctx = CancellationToken::new();
    let (result, usage) =
        run_cohort_cycle(&store, cohort, 5, ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default())).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    assert_eq!(usage.expect("final complete aggregate").objects_total_count, 14);
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn service_cohort_fresh_complete_aggregate_preserves_reordered_sources() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    for bucket in ["cohort-first", "cohort-second"] {
        create_cohort_bucket(&store, bucket).await;
    }
    let sets = store.all_set_disks();
    let listing = store
        .list_bucket_for_scanner(&BucketOptions::default())
        .await
        .expect("fresh inventory");
    let inventory = listing
        .set_buckets
        .into_iter()
        .map(|set| (DataUsageCacheSource::new(set.pool_index, set.set_index), set.buckets))
        .collect::<HashMap<_, _>>();
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    {
        let mut cohort = cohort.lock().expect("cohort lock");
        cohort.refresh(&inventory);
        for bucket in &inventory[&DataUsageCacheSource::new(0, 0)] {
            cohort.record_admitted(DataUsageCacheSource::new(0, 0), &bucket.name);
        }
        assert_eq!(cohort.order_set_indices(&sets), vec![1, 0]);
    }
    let ctx = CancellationToken::new();
    let (result, usage) =
        run_cohort_cycle(&store, cohort, 1, ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default())).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let usage = usage.expect("fresh complete aggregate");
    assert_eq!(usage.objects_total_count, 4);
    assert!(usage.buckets_usage.values().all(|bucket| bucket.objects_count == 2));
    assert_eq!(usage.usage_snapshot_set_states.len(), 2);
    assert_eq!(
        usage
            .usage_snapshot_set_states
            .iter()
            .map(|set| (set.pool_index, set.set_index))
            .collect::<HashSet<_>>(),
        HashSet::from([(0, 0), (1, 0)])
    );
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn service_cohort_cancelled_dispatch_does_not_consume_waiters_or_leak_permits() {
    let (_dir, store) = setup_two_pool_scanner_store().await;
    clear_dirty_usage_buckets_for_tests();
    let bucket = format!("cancel-{}", Uuid::new_v4().simple());
    create_cohort_bucket(&store, &bucket).await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
    ctx.cancel();
    run_cohort_cycle(&store, cohort.clone(), 1, budget).await;
    assert!(cohort.lock().expect("cohort lock").admitted_members().is_empty());
    let report = rustfs_scanner_metrics::metrics::global_metrics().scanner_runtime_details_report();
    assert!(report.active_bucket_drive_scans.is_empty());

    let ctx = CancellationToken::new();
    let (result, usage) =
        run_cohort_cycle(&store, cohort, 1, ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default())).await;
    assert_eq!(
        result.status,
        ScannerCycleStatus::Complete,
        "cancellation must not block a subsequent scan"
    );
    assert_eq!(usage.expect("retry aggregate").objects_total_count, 2);
    clear_dirty_usage_buckets_for_tests();
}

/// N=17, q=4: every bucket reaches a worker within ceil(17/4)=5 rounds while
/// one bucket stays hot and another fails forever, with leader-local state
/// discarded after each round as a restart would.
#[tokio::test]
#[serial]
async fn cold_bucket_service_bound_survives_restart_with_hot_and_failing_bucket() {
    const BUCKETS: usize = 17;
    const QUANTA: u64 = 4;
    const ROUND_BOUND: usize = 5;
    let (_dir, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    let names = (0..BUCKETS).map(|index| format!("cold-{index:02}")).collect::<Vec<_>>();
    for name in &names {
        create_cohort_bucket(&store, name).await;
    }
    let hot = names[0].clone();
    let _failing = fail_coordinator_bucket_scan(&names[1]);
    let mut waited = [0_usize; BUCKETS];
    for round in 1..=(ROUND_BOUND * 3) {
        record_dirty_usage_bucket(&hot);
        let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &ctx,
            ScannerCycleBudgetConfig {
                max_objects: Some(QUANTA),
                ..Default::default()
            },
        );
        run_cohort_cycle(&store, cohort.clone(), u64::try_from(round).expect("round"), budget).await;
        let admitted = cohort
            .lock()
            .expect("cohort lock")
            .admitted_members()
            .into_iter()
            .map(|(_, bucket)| bucket)
            .collect::<HashSet<_>>();
        for (index, name) in names.iter().enumerate() {
            waited[index] = if admitted.contains(name) { 0 } else { waited[index] + 1 };
            assert!(
                waited[index] < ROUND_BOUND,
                "{name} waited {} rounds at round {round}; admitted {admitted:?}",
                waited[index]
            );
        }
    }
    let saved = load_set_cache(&store).await;
    let ledger = saved
        .info
        .scan_bucket_service
        .expect("incomplete cycles must save the service order");
    assert!(ledger.buckets[&names[1]].failure_debt > 0, "the failing bucket must keep visible debt");
    assert_eq!(ledger.buckets[&names[1]].last_certified, None);
    assert_eq!(ledger.failure_debt().0, 1, "only the failing bucket owes a sweep");
    assert!(!saved.info.snapshot_complete, "the saved order must not certify the set");
    clear_dirty_usage_buckets_for_tests();
}

async fn load_set_cache(store: &Arc<ECStore>) -> DataUsageCache {
    let mut cache = DataUsageCache::default();
    cache
        .load(store.all_set_disks()[0].clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("load set cache");
    cache
}

fn quantum_budget(quantum: crate::scanner_budget::ScannerBucketQuantum) -> Arc<ScannerCycleBudget> {
    ScannerCycleBudget::new_with_progress_tracking(
        &CancellationToken::new(),
        ScannerCycleBudgetConfig {
            bucket_quantum: Some(quantum),
            ..Default::default()
        },
    )
}

#[tokio::test]
#[serial]
async fn fair_prefix_quantum_spent_bucket_returns_behind_waiting_buckets() {
    let (_dir, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    create_cohort_bucket_objects(&store, "a-flat", 6).await;
    create_cohort_bucket(&store, "z-cold-one").await;
    create_cohort_bucket(&store, "z-cold-two").await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let budget = quantum_budget(crate::scanner_budget::ScannerBucketQuantum {
        max_objects: Some(2),
        ..Default::default()
    });
    let (result, usage) = run_cohort_cycle(&store, cohort, 1, budget.clone()).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete, "shares must add up to one whole sweep");
    assert_eq!(usage.expect("complete aggregate").objects_total_count, 8);
    assert!(!budget.budget_elapsed(), "only the bucket shares ended");

    let ledger = load_set_cache(&store).await.info.scan_bucket_service.expect("saved order");
    assert!(ledger.buckets["a-flat"].service_quanta >= 3, "{ledger:?}");
    assert_eq!(ledger.buckets["z-cold-one"].service_quanta, 1, "{ledger:?}");
    assert_eq!(ledger.buckets["z-cold-two"].service_quanta, 1, "{ledger:?}");
    assert!(
        ledger.buckets["a-flat"].waiting_since > ledger.buckets["z-cold-two"].waiting_since,
        "the wide bucket must finish behind the buckets it yielded to: {ledger:?}"
    );
    assert_eq!(ledger.failure_debt(), (0, None), "a spent share is not a failure");
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn fair_prefix_quantum_without_durable_motion_ends_the_bucket_turn() {
    let (_dir, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    create_cohort_bucket_objects(&store, "a-flat", 6).await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let budget = quantum_budget(crate::scanner_budget::ScannerBucketQuantum {
        max_duration: Some(Duration::ZERO),
        ..Default::default()
    });
    let (result, usage) = run_cohort_cycle(&store, cohort, 1, budget).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(usage.is_none());
    let ledger = load_set_cache(&store).await.info.scan_bucket_service.expect("saved order");
    assert_eq!(
        ledger.buckets["a-flat"].service_quanta, 1,
        "a share that saved nothing must not requeue itself: {ledger:?}"
    );
    assert_eq!(ledger.failure_debt(), (0, None));
    clear_dirty_usage_buckets_for_tests();
}

/// A one-object share stops inside its first child, so the frontier never
/// moves. Requeueing that bucket would spin until the cycle deadline.
#[tokio::test]
#[serial]
async fn fair_prefix_quantum_repeated_share_without_a_new_frontier_stops_requeueing() {
    let (_dir, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    create_cohort_bucket_objects(&store, "a-flat", 3).await;
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let budget = quantum_budget(crate::scanner_budget::ScannerBucketQuantum {
        max_objects: Some(1),
        ..Default::default()
    });
    let (result, usage) = run_cohort_cycle(&store, cohort, 1, budget).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(usage.is_none());
    let ledger = load_set_cache(&store).await.info.scan_bucket_service.expect("saved order");
    assert!(
        ledger.buckets["a-flat"].service_quanta <= 3,
        "turns without durable motion must end: {ledger:?}"
    );
    assert_eq!(ledger.failure_debt(), (0, None));
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn cold_bucket_service_saved_order_keeps_coverage_and_rejects_older_owners() {
    let (_dir, store) = setup_local_scanner_store(1, 4).await;
    clear_dirty_usage_buckets_for_tests();
    create_cohort_bucket(&store, "cold-only").await;
    let ctx = CancellationToken::new();
    let cohort = Arc::new(StdMutex::new(ScannerServiceCohort::default()));
    let (result, _) = run_cohort_cycle(&store, cohort, 3, ScannerCycleBudget::new(&ctx, Default::default())).await;
    assert_eq!(result.status, ScannerCycleStatus::Complete);
    let certified = load_set_cache(&store).await;
    assert!(certified.info.snapshot_complete);
    let set = store.all_set_disks()[0].clone();
    let epoch = scanner_publication_epoch(set.clone()).await.expect("publication epoch");
    let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
    let mut ledger = certified.info.scan_bucket_service.clone().expect("saved order");
    ledger.next_sequence += 1;

    assert!(persist_scanner_service_ledger(set.clone(), source, ledger.clone(), 3, 11, epoch).await);
    let saved = load_set_cache(&store).await;
    assert_eq!(saved.info.scan_bucket_service.as_ref(), Some(&ledger));
    assert_eq!(saved.info.snapshot_complete, certified.info.snapshot_complete);
    assert_eq!(saved.info.scan_plan_digest, certified.info.scan_plan_digest);
    assert_eq!(saved.info.scan_execution_digest, certified.info.scan_execution_digest);
    assert_eq!(saved.info.next_cycle, certified.info.next_cycle);
    assert_eq!(saved.info.leader_epoch, certified.info.leader_epoch);
    assert_eq!(saved.cache.len(), certified.cache.len());

    let mut stale = ledger.clone();
    stale.next_sequence += 1;
    assert!(
        !persist_scanner_service_ledger(set.clone(), source, stale.clone(), 3, 10, epoch).await,
        "a late write from an older leader must not replace the order"
    );
    assert!(
        !persist_scanner_service_ledger(set.clone(), source, stale, 2, 11, epoch).await,
        "a late write from an older cycle must not replace the order"
    );
    assert_eq!(load_set_cache(&store).await.info.scan_bucket_service.as_ref(), Some(&ledger));
    clear_dirty_usage_buckets_for_tests();
}
