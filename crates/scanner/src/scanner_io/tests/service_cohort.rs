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
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("fixture bucket");
    for set in store.all_set_disks() {
        let mut reader = ScannerPutObjReader::from_vec(b"cohort".to_vec());
        set.put_object(
            bucket,
            "initial",
            &mut reader,
            &ScannerObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .expect("fixture object and all rename tails should persist");
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
                requires_full_scan: false,
                service_cohort: Some(cohort),
                resolved_scope_observer: None,
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
