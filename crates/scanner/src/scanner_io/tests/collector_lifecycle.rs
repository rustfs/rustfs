// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::data_usage_define::{DATA_USAGE_OBJ_NAME_PATH, read_config_with_revision};

const HEALTHY_BUCKET: &str = "collector-b-healthy";
const FAULT_BUCKET: &str = "collector-a-fault";
const PARTIAL_BUCKET: &str = "collector-z-partial";

#[derive(Clone, Copy)]
enum Stop {
    WorkerPanic,
    ParentCancellation,
}

struct Probe {
    stop: Stop,
    fault_entered: CancellationToken,
    partial_entered: CancellationToken,
    release_fault: CancellationToken,
    fault_exited: CancellationToken,
    release_partial: CancellationToken,
    partial_exited: CancellationToken,
}

static PROBE: StdMutex<Option<Arc<Probe>>> = StdMutex::new(None);

struct ProbeGuard(Arc<Probe>);

impl ProbeGuard {
    fn install(stop: Stop) -> Self {
        let probe = Arc::new(Probe {
            stop,
            fault_entered: CancellationToken::new(),
            partial_entered: CancellationToken::new(),
            release_fault: CancellationToken::new(),
            fault_exited: CancellationToken::new(),
            release_partial: CancellationToken::new(),
            partial_exited: CancellationToken::new(),
        });
        let mut slot = PROBE.lock().expect("collector lifecycle probe lock");
        assert!(slot.is_none(), "collector lifecycle fixtures must run serially");
        *slot = Some(probe.clone());
        Self(probe)
    }
}

impl Drop for ProbeGuard {
    fn drop(&mut self) {
        self.0.release_fault.cancel();
        self.0.release_partial.cancel();
        *PROBE.lock().expect("collector lifecycle probe cleanup") = None;
        clear_dirty_usage_buckets_for_tests();
    }
}

struct AdmissionExit(CancellationToken);

impl Drop for AdmissionExit {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) async fn observe_worker_start(worker_index: usize) {
    let probe = PROBE.lock().expect("collector lifecycle probe lock").clone();
    let Some(probe) = probe else { return };
    if worker_index != 0 {
        probe.fault_entered.cancelled().await;
    }
}

pub(super) async fn observe_bucket_admission(worker_index: usize, bucket: &str) {
    let probe = PROBE.lock().expect("collector lifecycle probe lock").clone();
    let Some(probe) = probe else { return };
    match bucket {
        FAULT_BUCKET => {
            assert_eq!(worker_index, 0, "the first waited handle must own the injected failure");
            let _exit = AdmissionExit(probe.fault_exited.clone());
            probe.fault_entered.cancel();
            probe.release_fault.cancelled().await;
            if matches!(probe.stop, Stop::WorkerPanic) {
                panic!("injected collector lifecycle worker panic");
            }
        }
        PARTIAL_BUCKET => {
            assert_eq!(worker_index, 1, "the second worker must retain the admitted resume");
            let _exit = AdmissionExit(probe.partial_exited.clone());
            probe.partial_entered.cancel();
            probe.release_partial.cancelled().await;
        }
        HEALTHY_BUCKET => assert_eq!(worker_index, 1, "the second worker must send the healthy root first"),
        _ => {}
    }
}

async fn create_bucket(store: &Arc<ECStore>, name: &str, objects: usize) {
    store
        .make_bucket(name, &MakeBucketOptions::default())
        .await
        .expect("create collector fixture bucket");
    for index in 0..objects {
        store
            .put_object(
                name,
                &format!("prefix-{index}/object"),
                &mut ScannerPutObjReader::from_vec(b"collector".to_vec()),
                &ScannerObjectOptions::default(),
            )
            .await
            .expect("persist collector fixture object");
    }
    wait_for_namespace_commit_tails(store.as_ref()).await;
}

async fn scan_plan(set: &Arc<SetDisks>, names: &[&str], cycle: u64) -> ScannerBucketScanPlan {
    let buckets = names.iter().map(|name| bucket_info(name)).collect::<Vec<_>>();
    let source = DataUsageCacheSource::new(set.pool_index, set.set_index);
    let mut cohort = ScannerServiceCohort::default();
    cohort.refresh(&HashMap::from([(source, buckets.clone())]));
    let digest = scanner_bucket_plan_digest(&buckets, [7; 32]);
    let tier_registry_generation = runtime_tier_registry_for_cycle(cycle, 11).await.generation;
    ScannerBucketScanPlan {
        all_buckets: Arc::new(buckets.clone()),
        buckets,
        scope: ScannerBucketScanScope::default(),
        digest,
        bucket_coverage_digest: digest,
        checkpoint_coverage_digest: digest,
        requires_full_scan: true,
        service_cohort: Some(Arc::new(StdMutex::new(cohort))),
        execution_digest: digest,
        leader_epoch: 11,
        tier_registry_generation,
        publication_epoch: Some(
            scanner_publication_epoch(set.clone())
                .await
                .expect("fixture publication admission"),
        ),
        dirty_usage_buckets: Arc::new(dirty_usage_buckets_for_tests()),
        bucket_failures: ScannerBucketFailureState::default(),
        bucket_completions: None,
        pending_maintenance_work: Arc::new(AtomicBool::new(false)),
        cache_cycle_floor: Arc::new(AtomicU64::new(cycle)),
        cold_zero_walk_reuse_observed: Arc::new(AtomicBool::new(false)),
        segment_invalidation_proof: None,
    }
}

async fn assert_collector_lifecycle(stop: Stop) {
    let (_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    create_bucket(&store, PARTIAL_BUCKET, 4).await;
    let set = store.pools[0].disk_set[0].clone();
    let seed_parent = CancellationToken::new();
    let seed_budget = ScannerCycleBudget::new_with_progress_tracking(
        &seed_parent,
        ScannerCycleBudgetConfig {
            max_objects: Some(1),
            ..Default::default()
        },
    );
    let (updates, mut seed_results) = mpsc::channel(1);
    set.clone()
        .nsscanner_cache(
            seed_budget.token(),
            seed_budget.clone(),
            scan_plan(&set, &[PARTIAL_BUCKET], 1).await,
            updates,
            1,
            HealScanMode::Normal,
        )
        .await
        .expect("budgeted production walk must save partial progress");
    assert_eq!(seed_budget.progress().0, 1, "the checkpoint must come from actual object work");
    assert!(!seed_results.recv().await.expect("partial set result").info.snapshot_complete);
    assert!(seed_results.recv().await.is_none());
    let partial_name = path_join_buf(&[PARTIAL_BUCKET, DATA_USAGE_CACHE_NAME]);
    let mut partial = DataUsageCache::default();
    let partial_revision = partial
        .load_with_revisions(set.clone(), &partial_name)
        .await
        .expect("read the checkpoint saved by the production worker");
    assert!(!partial.info.snapshot_complete);
    assert!(partial.info.scan_identity.is_some());
    assert!(partial.has_scan_position(), "the fixture must retain a real interrupted position");
    let saved_partial = serde_json::to_value(&partial).expect("checkpoint comparison value");

    create_bucket(&store, HEALTHY_BUCKET, 1).await;
    create_bucket(&store, FAULT_BUCKET, 1).await;
    for bucket in [HEALTHY_BUCKET, FAULT_BUCKET, PARTIAL_BUCKET] {
        record_dirty_usage_bucket(bucket);
    }
    let mut set_caches_before = Vec::new();
    for name in [DATA_USAGE_CACHE_NAME.to_string(), format!("{DATA_USAGE_CACHE_NAME}.bkp")] {
        let path = path_join_buf(&[crate::BUCKET_META_PREFIX, &name]);
        let before = read_config_with_revision(set.clone(), &path)
            .await
            .expect("set cache before interrupted cycle");
        set_caches_before.push((path, before));
    }
    let dirty_before = dirty_usage_buckets_for_tests();
    let root_before = read_config_with_revision(store.clone(), DATA_USAGE_OBJ_NAME_PATH.as_str())
        .await
        .expect("usage root before interrupted cycle");
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&parent, ScannerCycleBudgetConfig::default());
    let (updates, mut results) = mpsc::channel(1);
    let guard = ProbeGuard::install(stop);
    let probe = &guard.0;
    let scan = set.clone().nsscanner_cache(
        budget.token(),
        budget.clone(),
        scan_plan(&set, &[HEALTHY_BUCKET, FAULT_BUCKET, PARTIAL_BUCKET], 2).await,
        updates,
        2,
        HealScanMode::Normal,
    );
    tokio::pin!(scan);
    tokio::select! {
        result = &mut scan => panic!("set finished before both workers reached their gates: {result:?}"),
        _ = async {
            probe.fault_entered.cancelled().await;
            probe.partial_entered.cancelled().await;
        } => {}
    }
    // Worker zero owns the first waited handle and is held at FAULT_BUCKET.
    // Worker one starts only after that admission, then reaches PARTIAL_BUCKET
    // after HEALTHY_BUCKET's root send completed in the production loop.
    let mut healthy = DataUsageCache::default();
    healthy
        .load(set.clone(), &path_join_buf(&[HEALTHY_BUCKET, DATA_USAGE_CACHE_NAME]))
        .await
        .expect("healthy root must be durable before the injected stop");
    assert!(healthy.info.snapshot_complete);
    assert_eq!(healthy.checked_flatten(HEALTHY_BUCKET).expect("healthy root").objects, 1);
    assert_eq!(budget.progress().0, 1);
    assert!(!parent.is_cancelled(), "cancellation must occur during an active scan");
    assert!(matches!(results.try_recv(), Err(mpsc::error::TryRecvError::Empty)));

    if matches!(stop, Stop::ParentCancellation) {
        parent.cancel();
    }
    probe.release_fault.cancel();
    tokio::select! {
        result = &mut scan => panic!("set returned before the other admitted worker exited: {result:?}"),
        _ = probe.fault_exited.cancelled() => {}
    }
    // These current-thread Tokio tests observe the unwind after the panic
    // task has yielded back to the executor. The set must still join the
    // other worker, whose gate has not been released.
    assert!(futures::poll!(&mut scan).is_pending(), "set must await its still-admitted worker");
    assert!(!probe.partial_exited.is_cancelled());
    parent.cancel();
    probe.release_partial.cancel();
    let result = scan.await;
    assert!(
        probe.partial_exited.is_cancelled(),
        "the remaining worker must leave before the set returns"
    );
    assert!(budget.reason().is_none(), "this stop must come from the parent, not a budget deadline");
    match stop {
        Stop::WorkerPanic => {
            let error = result.expect_err("worker panic must remain a set join failure");
            assert!(error.to_string().contains("scanner disk bucket task join failed"));
            assert!(error.to_string().contains("injected collector lifecycle worker panic"));
            assert!(results.recv().await.is_none(), "failed set must not emit complete or partial authority");
        }
        Stop::ParentCancellation => {
            result.expect("cancelled workers must still drain the accepted roots");
            let drained = results
                .recv()
                .await
                .expect("cancelled set must retain its incomplete observation");
            assert!(!drained.info.snapshot_complete);
            assert!(drained.info.last_update.is_none());
            assert_eq!(
                drained
                    .find(HEALTHY_BUCKET)
                    .expect("collector retained the accepted root")
                    .objects,
                1
            );
            assert!(results.recv().await.is_none(), "set output must close after its collector is joined");
        }
    }
    for (path, before) in set_caches_before {
        assert_eq!(
            read_config_with_revision(set.clone(), &path)
                .await
                .expect("set cache after worker shutdown"),
            before,
            "interrupted workers must not publish a complete set snapshot or advance its revision: {path}"
        );
    }
    assert_eq!(
        partial
            .load_with_revisions(set, &partial_name)
            .await
            .expect("reload partial after worker shutdown"),
        partial_revision,
        "stopping an admitted resume must not replace the durable checkpoint"
    );
    assert_eq!(serde_json::to_value(&partial).expect("checkpoint comparison value"), saved_partial);
    // These global checks guard side effects of the set entry point only.
    // cycle_budget_publication covers the outer publication and ACK proof.
    assert_eq!(
        dirty_usage_buckets_for_tests(),
        dirty_before,
        "set scan must not mutate global dirty buckets"
    );
    assert_eq!(
        read_config_with_revision(store, DATA_USAGE_OBJ_NAME_PATH.as_str())
            .await
            .expect("usage root after interrupted cycle"),
        root_before,
        "set scan must not replace global usage as a side effect"
    );
}

struct RuntimeConfigReset;

impl Drop for RuntimeConfigReset {
    fn drop(&mut self) {
        crate::runtime_config::refresh_scanner_runtime_config_for_tests();
    }
}

async fn run_collector_lifecycle(stop: Stop) {
    let _reset = RuntimeConfigReset;
    temp_env::async_with_vars(
        [
            (ENV_SCANNER_WALKERS_PER_DISK, Some("2")),
            (ENV_SCANNER_MAX_CONCURRENT_DISK_SCANS, Some("2")),
        ],
        async {
            crate::runtime_config::refresh_scanner_runtime_config_for_tests();
            tokio::time::timeout(Duration::from_secs(60), assert_collector_lifecycle(stop))
                .await
                .expect("collector lifecycle fixture must finish within its existing bounded deadline");
        },
    )
    .await;
}

#[tokio::test]
#[serial]
async fn collector_worker_panic_waits_for_admitted_resume_and_preserves_checkpoint() {
    run_collector_lifecycle(Stop::WorkerPanic).await;
}

#[tokio::test]
#[serial]
async fn collector_parent_cancel_drains_accepted_root_and_preserves_checkpoint() {
    run_collector_lifecycle(Stop::ParentCancellation).await;
}
