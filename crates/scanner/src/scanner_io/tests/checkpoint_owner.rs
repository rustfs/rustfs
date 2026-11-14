// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::owner::{EcstoreLocalPublicationPause, EcstoreLocalPublicationStage};

const WORKER_ENV: &str = "RUSTFS_CHECKPOINT_OWNER_TEST_WORKER";
const REPORT_ENV: &str = "RUSTFS_CHECKPOINT_OWNER_TEST_REPORT";
const BUCKET: &str = "checkpoint-owner";
const CYCLE: u64 = 43;
const LEADER_EPOCH: u64 = 11;
// Exercise the production periodic trigger, including its minimum wall-clock
// interval. The extra object keeps the periodic save distinct from final drain.
const CHECKPOINT_OBJECTS: u64 = 1024;
const CHECKPOINT_MIN_INTERVAL: Duration = Duration::from_secs(5);
const PHASE_TIMEOUT: Duration = Duration::from_secs(60);

async fn checkpoint_plan(set: &Arc<SetDisks>, owner: &CancellationToken) -> ScannerBucketScanPlan {
    let buckets = vec![bucket_info(BUCKET)];
    let digest = scanner_bucket_plan_digest(&buckets, [7; 32]);
    ScannerBucketScanPlan {
        owner_ctx: owner.clone(),
        usage_watch_store: None,
        all_buckets: Arc::new(buckets.clone()),
        buckets,
        scope: ScannerBucketScanScope::default(),
        digest,
        bucket_coverage_digest: digest,
        checkpoint_coverage_digest: digest,
        completed_checkpoint_cycle_floor: None,
        requires_full_scan: true,
        service_cohort: None,
        execution_digest: digest,
        leader_epoch: LEADER_EPOCH,
        tier_registry_generation: runtime_tier_registry_for_cycle(CYCLE, LEADER_EPOCH).await.generation,
        publication_epoch: Some(scanner_publication_epoch(set.clone()).await.expect("checkpoint owner epoch")),
        dirty_usage_buckets: Arc::new(dirty_usage_buckets_for_tests()),
        bucket_failures: ScannerBucketFailureState::default(),
        bucket_completions: Some(Arc::new(Mutex::new(HashMap::new()))),
        pending_maintenance_work: Arc::new(AtomicBool::new(false)),
        cache_cycle_floor: Arc::new(AtomicU64::new(CYCLE)),
        cold_zero_walk_reuse_observed: Arc::new(AtomicBool::new(false)),
        segment_invalidation_proof: None,
    }
}

async fn run_checkpoint_owner_case(lose_owner: bool) -> serde_json::Value {
    let (directory, store) = setup_local_scanner_store(1, 1).await;
    let weak_store = Arc::downgrade(&store);
    assert!(crate::storage_api::owner::ecstore_set_object_store_resolver(Arc::new(
        move || weak_store.upgrade()
    )));
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket(BUCKET, &MakeBucketOptions::default())
        .await
        .expect("create checkpoint owner bucket");
    for object in 0..=CHECKPOINT_OBJECTS {
        store
            .put_object(
                BUCKET,
                &format!("object-{object:04}"),
                &mut ScannerPutObjReader::from_vec(vec![1]),
                &ScannerObjectOptions::default(),
            )
            .await
            .expect("write real object for the periodic checkpoint threshold");
    }
    save_config(
        store.clone(),
        DATA_USAGE_BLOOM_NAME_PATH.as_str(),
        crate::scanner::encode_scanner_cycle_fence_for_test(CYCLE, LEADER_EPOCH),
    )
    .await
    .expect("persist the real global cycle fence");
    wait_for_namespace_commit_tails(store.as_ref()).await;
    record_dirty_usage_bucket(BUCKET);
    let dirty_before = dirty_usage_buckets_for_tests();
    let set = store.pools[0].disk_set[0].clone();
    let disk = scanner_set_disk_inventory(&set).await.pop().expect("checkpoint owner disk");
    let cache_name = path_join_buf(&[BUCKET, DATA_USAGE_CACHE_NAME]);
    let revisions_before = DataUsageCache::read_revisions(set.clone(), &cache_name)
        .await
        .expect("read absent checkpoint revisions");
    let owner = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&owner, ScannerCycleBudgetConfig::default());
    budget.use_production_drain_grace_for_tests();
    let plan = checkpoint_plan(&set, &owner).await;
    let failures = plan.bucket_failures.clone();
    let completions = plan.bucket_completions.clone().expect("checkpoint completion collector");
    let metrics_before = global_metrics().report().await;

    let mut first_object = EcstoreLocalPublicationPause::install(
        &disk,
        BUCKET,
        "object-0000/xl.meta",
        EcstoreLocalPublicationStage::MetadataReadAfterOpen,
    )
    .expect("pause the first real object read");
    let checkpoint_path = path_join_buf(&[crate::BUCKET_META_PREFIX, &cache_name, STORAGE_FORMAT_FILE]);
    let mut checkpoint_write = EcstoreLocalPublicationPause::install(
        &disk,
        RUSTFS_META_BUCKET,
        &checkpoint_path,
        EcstoreLocalPublicationStage::PreparedRename,
    )
    .expect("pause the accepted physical checkpoint publication");
    let (updates, mut results) = mpsc::channel(1);
    let scanner = AbortOnDropHandle::new(tokio::spawn(set.clone().nsscanner_cache(
        budget.token(),
        budget.clone(),
        plan,
        updates,
        CYCLE,
        HealScanMode::Normal,
    )));
    tokio::time::timeout(PHASE_TIMEOUT, first_object.entered())
        .await
        .expect("scanner must reach the first real metadata read")
        .expect("first object barrier must remain installed");
    assert_eq!(budget.progress().0, 0);
    // FolderScanner uses std::time::Instant here. This wait satisfies its real
    // checkpoint interval; the native barriers determine all race ordering.
    tokio::time::sleep(CHECKPOINT_MIN_INTERVAL).await;
    drop(first_object);
    tokio::time::timeout(PHASE_TIMEOUT, checkpoint_write.entered())
        .await
        .expect("periodic checkpoint must reach its physical rename before EOF")
        .expect("checkpoint write barrier must remain installed");
    assert_eq!(
        budget.progress().0,
        CHECKPOINT_OBJECTS,
        "this must be a periodic save, not the final partial save"
    );
    assert!(!budget.token().is_cancelled());
    assert!(!owner.is_cancelled());
    if lose_owner {
        owner.cancel();
    } else {
        budget.cancel_for_runtime();
        assert!(!owner.is_cancelled(), "a work stop must retain publication ownership");
    }
    drop(checkpoint_write);
    tokio::time::timeout(PHASE_TIMEOUT, scanner)
        .await
        .expect("checkpoint handling must drain within its original bounded lifetime")
        .expect("checkpoint worker must not panic")
        .expect("the set must report incomplete coverage after cancellation");
    let result = results.recv().await.expect("incomplete source result");
    assert!(!result.info.snapshot_complete);
    assert!(results.recv().await.is_none());
    assert!(completions.lock().await.values().all(HashMap::is_empty));
    assert_eq!(
        dirty_usage_buckets_for_tests(),
        dirty_before,
        "partial work cannot acknowledge dirty responsibility"
    );
    let metrics_after = global_metrics().report().await;
    if lose_owner {
        assert!(
            failures.hard.lock().await.contains(BUCKET),
            "owner loss must reject further checkpoint handling"
        );
        assert!(
            !failures.partial.lock().await.contains(BUCKET),
            "owner loss is not an ordinary work-budget stop"
        );
        // The accepted native write may already publish before owner loss is
        // observed. Only authorization and subsequent work must be rejected.
    } else {
        assert!(
            failures.hard.lock().await.is_empty(),
            "a budget stop must not become a leader-fence failure"
        );
        assert!(failures.partial.lock().await.contains(BUCKET));
        assert_eq!(metrics_after.scan_checkpoint_save_failures, metrics_before.scan_checkpoint_save_failures);
        assert!(
            metrics_after
                .scan_checkpoints_persisted
                .iter()
                .any(|saved| saved.bucket == BUCKET && saved.pool_index == 0 && saved.set_index == 0),
            "the periodic handler must finish its real save and CAS revision refresh after a work stop"
        );
        let mut checkpoint = DataUsageCache::default();
        let revisions_after = checkpoint
            .load_with_revisions(set.clone(), &cache_name)
            .await
            .expect("reload the checkpoint saved under the live owner");
        assert_ne!(revisions_after, revisions_before);
        assert!(!checkpoint.info.snapshot_complete);
        assert!(checkpoint.info.scan_identity.is_some());
        assert!(checkpoint.has_scan_position(), "restart must retain a real interrupted cursor");
        let root = checkpoint.root().expect("saved partial graph root");
        assert_eq!(
            checkpoint.flatten(&root).objects,
            usize::try_from(CHECKPOINT_OBJECTS).expect("fixture object count")
        );
    }
    clear_dirty_usage_buckets_for_tests();
    drop(disk);
    drop(set);
    drop(store);
    let fixture_path = directory.path().to_path_buf();
    directory.close().expect("remove checkpoint owner disk fixture");
    assert!(!fixture_path.exists());
    serde_json::json!({
        "owner_lost": lose_owner,
        "objects_at_periodic_save": CHECKPOINT_OBJECTS,
        "snapshot_complete": result.info.snapshot_complete,
        "dirty_responsibility_retained": dirty_before.len(),
        "fixture_removed": true,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn periodic_checkpoint_drains_work_stop_but_rejects_owner_loss() {
    if let Some(mode) = std::env::var_os(WORKER_ENV) {
        let lose_owner = match mode.to_str().expect("checkpoint worker mode") {
            "work" => false,
            "owner" => true,
            mode => panic!("unexpected checkpoint worker mode: {mode}"),
        };
        let report = run_checkpoint_owner_case(lose_owner).await;
        std::fs::write(
            std::env::var_os(REPORT_ENV).expect("checkpoint worker report path"),
            serde_json::to_vec(&report).expect("checkpoint worker report JSON"),
        )
        .expect("write the completed checkpoint assertions");
        return;
    }

    // The real checkpoint handler resolves a process-global OnceLock owner.
    // Isolate both cases from other libtest fixtures and from one another.
    let directory = tempfile::tempdir().expect("checkpoint subprocess reports");
    let test = format!(
        "{}::periodic_checkpoint_drains_work_stop_but_rejects_owner_loss",
        module_path!().split_once("::").expect("checkpoint test module path").1
    );
    for mode in ["work", "owner"] {
        let report_path = directory.path().join(format!("{mode}.json"));
        let log_path = directory.path().join(format!("{mode}.log"));
        let log = std::fs::File::create(&log_path).expect("checkpoint worker log");
        let mut child = std::process::Command::new(std::env::current_exe().expect("scanner test binary"))
            .args([test.as_str(), "--exact", "--test-threads=1", "--nocapture"])
            .env(WORKER_ENV, mode)
            .env(REPORT_ENV, &report_path)
            .env(rustfs_config::ENV_SCANNER_SPEED, "fastest")
            // Return the walk's poll after each object. At object 1024 the
            // checkpoint is already queued, so its handler can take over
            // before another metadata visit completes. A paused next-object
            // read would retain the disk's exclusive background I/O permit.
            .env(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, "1")
            .stdout(log.try_clone().expect("checkpoint stdout log"))
            .stderr(log)
            .spawn()
            .expect("start isolated checkpoint owner case");
        let deadline = std::time::Instant::now() + Duration::from_secs(180);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok(None) => {
                    let killed = child.kill();
                    let reaped = child.wait();
                    killed.expect("kill timed-out checkpoint owner worker");
                    reaped.expect("reap timed-out checkpoint owner worker");
                    panic!(
                        "checkpoint owner case {mode} timed out: {}",
                        std::fs::read_to_string(&log_path).expect("timed-out worker log")
                    );
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("checkpoint owner case {mode} could not be observed: {error}");
                }
            }
        };
        assert!(
            status.success(),
            "checkpoint owner case {mode} failed: {}",
            std::fs::read_to_string(&log_path).expect("failed worker log")
        );
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&report_path).expect("the exact worker must run and finish all assertions"))
                .expect("completed checkpoint worker report");
        assert_eq!(report["owner_lost"], mode == "owner");
        assert_eq!(report["objects_at_periodic_save"], CHECKPOINT_OBJECTS);
        assert_eq!(report["fixture_removed"], true);
        println!("checkpoint_owner_assertion: {report}");
    }
}
