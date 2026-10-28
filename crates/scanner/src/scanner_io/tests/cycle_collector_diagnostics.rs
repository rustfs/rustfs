// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use std::num::NonZeroU16;

const LEADER_EPOCH: u64 = 11;

#[derive(Clone)]
struct LogWriter(Arc<StdMutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("collector log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn capture_cycle<F: std::future::Future>(future: F) -> (F::Output, Vec<serde_json::Value>) {
    use tracing::instrument::WithSubscriber as _;

    let buffer = Arc::new(StdMutex::new(Vec::new()));
    let writer = Arc::clone(&buffer);
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(move || LogWriter(Arc::clone(&writer)))
        .finish();
    let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let result = future.with_subscriber(subscriber).await;
    let logs = buffer.lock().expect("collector log buffer");
    let events = std::str::from_utf8(&logs)
        .expect("collector logs should be UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("collector event should be JSON"))
        .collect();
    (result, events)
}

async fn run_cycle(
    store: &Arc<ECStore>,
    cycle: u64,
    max_objects: Option<u64>,
    requires_full_scan: bool,
) -> (ScannerCycleResult, Option<DataUsageInfo>, bool) {
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
                requires_full_scan,
                service_cohort: None,
                resolved_scope_observer: None,
                post_walk_gate: None,
            },
        ),
    )
    .await
    .expect("cycle should finish within the test deadline")
    .expect("cycle should return a status");
    let snapshot = receiver.recv().await;
    assert!(receiver.recv().await.is_none(), "a cycle emits at most one snapshot");
    (result, snapshot, budget.budget_elapsed())
}

fn collector_events(events: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    events
        .iter()
        .filter(|event| event["fields"]["state"] == "incomplete_cycle_collector_summary")
        .map(|event| &event["fields"])
        .collect()
}

#[tokio::test]
#[serial]
async fn collector_reports_an_unobserved_slot_after_a_stale_set_snapshot() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket("diagnostic-bucket", &MakeBucketOptions::default())
        .await
        .expect("fixture bucket should be created");
    wait_for_namespace_commit_tails(store.as_ref()).await;

    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let mut newer = complete_set_usage_cache(&[("diagnostic-bucket", 1)], DataUsageScanPlanDigest([1; 32]));
    newer.info.source = Some(DataUsageCacheSource::new(0, 0));
    newer.info.next_cycle = 7;
    newer.info.leader_epoch = LEADER_EPOCH + 1;
    newer
        .save(set, DATA_USAGE_CACHE_NAME)
        .await
        .expect("newer set cache should persist");

    let ((result, snapshot, budget_elapsed), events) = capture_cycle(run_cycle(&store, 7, None, true)).await;
    assert_eq!(result.status, ScannerCycleStatus::Incomplete);
    assert!(!budget_elapsed);
    assert!(snapshot.is_none_or(|snapshot| !snapshot.usage_snapshot_complete));
    let events = collector_events(&events);
    assert_eq!(events.len(), 1, "a non-budget incomplete cycle needs one collector observation");
    let fields = events[0];
    assert_eq!(fields["cycle"], 7);
    assert_eq!(fields["leader_epoch"], LEADER_EPOCH);
    assert_eq!(fields["expected_sources"], 1);
    assert_eq!(fields["observed_slots"], 0);
    assert_eq!(fields["unobserved_slots"], 1);
    assert_eq!(fields["sample_expected_pool"], 0);
    assert_eq!(fields["sample_expected_set"], 0);
    assert_eq!(fields["sample_observed"], false);
    assert!(fields.get("sample_snapshot_source_pool").is_none());
    assert!(fields.get("sample_snapshot_complete").is_none());
    clear_dirty_usage_buckets_for_tests();
}

#[tokio::test]
#[serial]
async fn collector_reports_received_incomplete_cache_only_for_non_budget_partial() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    clear_dirty_usage_buckets_for_tests();
    store
        .make_bucket("fresh-bucket", &MakeBucketOptions::default())
        .await
        .expect("fixture bucket should be created");
    for object in ["p0/a", "p1/b", "p2/c", "p3/d"] {
        let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
        store
            .put_object("fresh-bucket", object, &mut reader, &ScannerObjectOptions::default())
            .await
            .expect("fixture object should persist");
    }
    record_dirty_usage_bucket("fresh-bucket");
    wait_for_namespace_commit_tails(store.as_ref()).await;

    let ((first, _, budget_elapsed), events) = capture_cycle(run_cycle(&store, 1, Some(2), true)).await;
    assert_eq!(first.status, ScannerCycleStatus::Incomplete);
    assert!(budget_elapsed);
    assert!(collector_events(&events).is_empty(), "budget partials do not use this event");

    let ((second, snapshot, budget_elapsed), events) = capture_cycle(run_cycle(&store, 2, None, false)).await;
    assert_eq!(second.status, ScannerCycleStatus::Incomplete);
    assert!(!budget_elapsed);
    assert!(snapshot.is_none_or(|snapshot| !snapshot.usage_snapshot_complete));
    let events = collector_events(&events);
    assert_eq!(events.len(), 1, "a non-budget incomplete cycle needs one collector observation");
    let fields = events[0];
    assert_eq!(fields["observed_slots"], 1);
    assert_eq!(fields["unobserved_slots"], 0);
    assert_eq!(fields["sample_observed"], true);
    assert_eq!(fields["sample_expected_pool"], 0);
    assert_eq!(fields["sample_expected_set"], 0);
    assert_eq!(fields["sample_snapshot_source_pool"], 0);
    assert_eq!(fields["sample_snapshot_source_set"], 0);
    assert_eq!(fields["sample_snapshot_complete"], false);
    assert_eq!(fields["sample_snapshot_has_last_update"], false);
    assert_eq!(fields["sample_snapshot_cycle"], 2);
    assert_eq!(fields["sample_snapshot_leader_epoch"], LEADER_EPOCH);
    assert_eq!(fields["sample_snapshot_has_plan"], true);
    assert_eq!(fields["sample_snapshot_plan_matches"], true);

    let ((third, snapshot, _), events) = capture_cycle(run_cycle(&store, 3, None, false)).await;
    assert_eq!(third.status, ScannerCycleStatus::Complete);
    assert!(snapshot.is_some_and(|snapshot| snapshot.usage_snapshot_complete));
    assert!(
        collector_events(&events).is_empty(),
        "a complete cycle must not report incomplete collector slots"
    );
    clear_dirty_usage_buckets_for_tests();
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AdmittedCycle {
    Complete,
    Cancelled,
    CacheChanged,
}

fn recorded_events(buffer: &StdMutex<Vec<u8>>) -> Vec<serde_json::Value> {
    let buffer = buffer.lock().expect("collector log buffer");
    buffer
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|line| line.ends_with(b"\n"))
        .map(|line| serde_json::from_slice(line).expect("complete collector events should be JSON"))
        .collect()
}

async fn admitted_collector_cycle(
    store: &Arc<ECStore>,
    owner: &Arc<NodeAdmission>,
    logs: &Arc<StdMutex<Vec<u8>>>,
    cycle: u64,
    case: AdmittedCycle,
) {
    const BUCKET: &str = "admitted-collector";
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let source = DataUsageCacheSource::new(0, 0);
    let cache_name = format!("{BUCKET}/{DATA_USAGE_CACHE_NAME}");
    record_dirty_usage_bucket(BUCKET);
    let dirty_generation = dirty_usage_buckets_for_tests().get(BUCKET).copied();
    assert!(dirty_generation.is_some(), "the fixture must start with dirty usage");
    let original_bucket_revision = DataUsageCache::read_revisions(set.clone(), &cache_name)
        .await
        .expect("read bucket cache revisions before the cycle");
    let original_set_revision = DataUsageCache::read_revisions(set.clone(), DATA_USAGE_CACHE_NAME)
        .await
        .expect("read set cache revisions before the cycle");
    logs.lock().expect("collector log buffer").clear();

    // The worker can acquire its cache lease but cannot start a node operation.
    let mut blocker = Some(
        tokio::time::timeout(Duration::from_secs(10), owner.acquire())
            .await
            .expect("fixture should acquire the node unit"),
    );
    assert_eq!(owner.snapshot().active, 1);
    let ctx = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, ScannerCycleBudgetConfig::default());
    let (updates, mut receiver) = mpsc::channel(1);
    let scan = nsscanner_with_storage_status_scoped(
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
    );
    tokio::pin!(scan);
    let cache_ready = async {
        loop {
            if recorded_events(logs)
                .iter()
                .any(|event| event["fields"]["state"] == "cache_ready" && event["fields"]["bucket"] == BUCKET)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    tokio::select! {
        biased;
        result = &mut scan => panic!("cycle ended before bucket cache admission: {result:?}"),
        result = tokio::time::timeout(Duration::from_secs(60), cache_ready) => {
            result.expect("worker should load its cache while the node is occupied");
        }
    }

    let mut competing_lease = Some(tokio::select! {
        biased;
        result = &mut scan => panic!("cycle ended before yielding its cache lease: {result:?}"),
        result = tokio::time::timeout(
            Duration::from_secs(60), acquire_scanner_cache_locks(set.as_ref(), &cache_name, source),
        ) => result.expect("scanner should yield its cache lease").expect("competing cache lease"),
    });
    let expected_bucket_revision = if case == AdmittedCycle::CacheChanged {
        let mut newer = DataUsageCache::default();
        newer
            .load(set.clone(), &cache_name)
            .await
            .expect("load the protected bucket cache");
        assert!(newer.info.snapshot_complete, "the competing writer starts from the certified cache");
        newer.info.next_cycle = cycle - 1;
        newer
            .save(set.clone(), &cache_name)
            .await
            .expect("persist the competing cache revision");
        let updated = DataUsageCache::read_revisions(set.clone(), &cache_name)
            .await
            .expect("read the competing cache revision");
        assert_ne!(updated, original_bucket_revision, "the competing write must change the revision");
        Some(updated)
    } else {
        None
    };

    if case == AdmittedCycle::Cancelled {
        ctx.cancel();
    } else {
        blocker.take().expect("held node unit").complete();
        let admitted = async {
            while owner.snapshot().active != 1 {
                tokio::task::yield_now().await;
            }
        };
        tokio::select! {
            biased;
            result = &mut scan => panic!("cycle ended before reacquiring node admission: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(60), admitted) => {
                result.expect("scanner should acquire the released node unit");
            }
        }
        assert!(
            !recorded_events(logs)
                .iter()
                .any(|event| event["fields"]["state"] == "scan_completed" && event["fields"]["bucket"] == BUCKET),
            "the worker cannot complete the walk while the other lease is held"
        );
    }

    if case != AdmittedCycle::Cancelled {
        drop(competing_lease.take());
    }
    let result = tokio::time::timeout(Duration::from_secs(60), &mut scan)
        .await
        .expect("scanner cycle should finish")
        .expect("scanner cycle should return a status");
    let snapshot = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await
        .expect("cycle update channel should finish");
    assert!(
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("cycle update channel should close")
            .is_none(),
        "a cycle emits at most one snapshot"
    );
    if case == AdmittedCycle::Cancelled {
        assert_eq!(owner.snapshot().active, 1, "cancellation must not take the occupied node unit");
        drop(blocker.take());
        drop(competing_lease.take());
    }
    assert_eq!(owner.snapshot().active, 0, "the cycle must release its node capacity");
    assert!(owner.try_acquire().is_some(), "the node queue must not retain the finished request");
    let events = recorded_events(logs);
    let root_summaries: Vec<_> = events
        .iter()
        .filter(|event| event["fields"]["state"] == "bucket_root_collector_drained")
        .map(|event| &event["fields"])
        .collect();
    assert_eq!(root_summaries.len(), 1, "each real cycle drains one set collector");
    let roots = root_summaries[0];
    assert_eq!(roots["collector_cache_cycle"], cycle);
    assert_eq!(roots["rejected_roots"], 0);
    assert_eq!(dirty_usage_buckets_for_tests().get(BUCKET).copied(), dirty_generation);

    match case {
        AdmittedCycle::Complete => {
            assert_eq!(result.status, ScannerCycleStatus::Complete);
            let snapshot = snapshot.expect("complete cycle must produce a usage snapshot");
            assert!(snapshot.usage_snapshot_complete);
            assert_eq!(snapshot.objects_total_count, 4);
            assert_eq!(roots["received_roots"], 1);
            assert_eq!(roots["distinct_applied_roots"], 1);
            assert_eq!(roots["cancelled_at_drain"], false);
            assert!(collector_events(&events).is_empty());
        }
        AdmittedCycle::Cancelled | AdmittedCycle::CacheChanged => {
            assert_eq!(result.status, ScannerCycleStatus::Incomplete);
            assert!(!result.has_dirty_usage_to_acknowledge());
            assert!(snapshot.is_none_or(|snapshot| !snapshot.usage_snapshot_complete));
            assert_eq!(roots["received_roots"], 0);
            assert_eq!(roots["distinct_applied_roots"], 0);
            assert_eq!(roots["cancelled_at_drain"], case == AdmittedCycle::Cancelled);
            assert_eq!(
                DataUsageCache::read_revisions(set.clone(), DATA_USAGE_CACHE_NAME)
                    .await
                    .expect("read the retained set cache revisions"),
                original_set_revision
            );
            let expected = expected_bucket_revision.unwrap_or(original_bucket_revision);
            assert_eq!(
                DataUsageCache::read_revisions(set.clone(), &cache_name)
                    .await
                    .expect("read the retained bucket cache revisions"),
                expected
            );
            let summaries = collector_events(&events);
            if case == AdmittedCycle::Cancelled {
                assert!(summaries.is_empty(), "a cancelled cycle must not report an ordinary incomplete summary");
            } else {
                assert!(events.iter().any(|event| {
                    event["fields"]["state"] == "scan_failed"
                        && event["fields"]["error"]
                            .as_str()
                            .is_some_and(|error| error.contains("scanner cache changed while waiting for node admission"))
                }));
                assert_eq!(summaries.len(), 1);
                let fields = summaries[0];
                assert_eq!(fields["cycle"], cycle);
                assert_eq!(fields["leader_epoch"], LEADER_EPOCH);
                assert_eq!(fields["expected_sources"], 1);
                assert_eq!(fields["observed_slots"], 1);
                assert_eq!(fields["unobserved_slots"], 0);
                assert_eq!(fields["sample_observed"], true);
                assert_eq!(fields["sample_snapshot_complete"], false);
                assert_eq!(fields["sample_snapshot_cycle"], cycle);
            }
        }
    }
    let mut retained = DataUsageCache::default();
    retained
        .load(set, &cache_name)
        .await
        .expect("load the retained certified bucket cache");
    assert!(retained.info.snapshot_complete);
    assert_eq!(
        retained.info.next_cycle,
        match case {
            AdmittedCycle::Complete | AdmittedCycle::Cancelled => 1,
            AdmittedCycle::CacheChanged => 2,
        }
    );
    assert_eq!(retained.checked_flatten(BUCKET).expect("retained bucket tree").objects, 4);
}

#[test]
fn collector_cycle_with_node_admission_preserves_cache_and_reports_outcomes() {
    const TEST: &str = "scanner_io::tests::cycle_collector_diagnostics::collector_cycle_with_node_admission_preserves_cache_and_reports_outcomes";
    const CHILD: &str = "RUSTFS_TEST_ADMITTED_COLLECTOR_CHILD";
    if std::env::var(CHILD).ok().as_deref() != Some(TEST) {
        // The production admission owner is process-global and cannot be reset.
        let output = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--nocapture", "--color", "never"])
            .env(CHILD, TEST)
            .env("RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES", "1")
            .env("RUSTFS_SCANNER_IDLE_MODE", "false")
            .env("RUSTFS_SCANNER_DISK_IO_BUDGET_IOPS", "off")
            .env("RUSTFS_SCANNER_DISK_IO_BUDGET_BYTES_PER_SEC", "off")
            .env("RUSTFS_LOCK_ACQUIRE_TIMEOUT", "30")
            .output()
            .expect("start the isolated collector cycle");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success()
                && stdout
                    .lines()
                    .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
            "the isolated collector cycle must run exactly once: {stdout}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let logs = Arc::new(StdMutex::new(Vec::new()));
    let writer = Arc::clone(&logs);
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || LogWriter(Arc::clone(&writer)))
            .finish(),
    )
    .expect("install the isolated collector observer");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("create the isolated scanner runtime");
    runtime.block_on(async {
        let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
        clear_dirty_usage_buckets_for_tests();
        store
            .make_bucket("admitted-collector", &MakeBucketOptions::default())
            .await
            .expect("create the fixture bucket");
        for object in ["p0/a", "p1/b", "p2/c", "p3/d"] {
            let mut reader = ScannerPutObjReader::from_vec(vec![7; 16]);
            store
                .put_object("admitted-collector", object, &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("persist the fixture object");
        }
        wait_for_namespace_commit_tails(store.as_ref()).await;
        assert!(!crate::scanner_is_erasure().await, "this fixture must not start scanner healing");

        let owner = NodeAdmission::new(NonZeroU16::new(1).expect("one node unit"));
        let observation = FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        };
        let mut controller = FeedbackController::new();
        owner.update(&mut controller, Some(observation));
        let sampler_stop = CancellationToken::new();
        let sampler_cancel = sampler_stop.clone();
        let sampler_owner = owner.clone();
        let sampler = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = sampler_cancel.cancelled() => break,
                    _ = tick.tick() => { sampler_owner.update(&mut controller, Some(observation)); }
                }
            }
        });
        assert!(crate::set_scanner_node_admission(owner.clone()), "install the real node owner once");
        admitted_collector_cycle(&store, &owner, &logs, 1, AdmittedCycle::Complete).await;
        admitted_collector_cycle(&store, &owner, &logs, 2, AdmittedCycle::Cancelled).await;
        admitted_collector_cycle(&store, &owner, &logs, 3, AdmittedCycle::CacheChanged).await;
        sampler_stop.cancel();
        sampler.await.expect("stop the admission feedback sampler");
    });
}
