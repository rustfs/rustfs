// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;

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
