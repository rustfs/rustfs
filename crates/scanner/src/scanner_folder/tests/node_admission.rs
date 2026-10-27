// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
#[cfg(unix)]
use crate::storage_api::owner::{
    EcstoreLocalPublicationPause as LocalPublicationPause, EcstoreLocalPublicationStage as LocalPublicationStage,
    ecstore_with_background_disk_io, ecstore_with_background_resource_unit,
};
use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
use std::num::NonZeroU16;
use std::sync::atomic::AtomicU64;

fn owner(open: bool) -> Arc<NodeAdmission> {
    let owner = NodeAdmission::new(NonZeroU16::new(1).expect("one node unit"));
    if open {
        owner.update(
            &mut FeedbackController::new(),
            Some(FeedbackObservation {
                generation: 1,
                max_concurrency: 1,
                pressure: ResourcePressure::Unknown,
                completed_units: None,
            }),
        );
    }
    owner
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_blocks_real_walk_before_open_or_metadata() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata(&root, "bucket", "object").await;
    let visits = super::metadata_visits::observe(&mut scanner);
    let gate = owner(false);
    scanner.node_admission = Some(gate.clone());
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    for name in ["bucket", "does-not-exist"] {
        let result = scanner
            .scan_folder(
                CancellationToken::new(),
                CachedFolder {
                    name: name.to_string(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut DataUsageEntry::default(),
            )
            .await;
        assert!(
            matches!(result, Err(ScannerError::ResourceDeferred)),
            "node gate must precede both real reads and missing-path lookup: {result:?}"
        );
    }
    assert_eq!(visits.load(Ordering::Relaxed), 0);
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0));
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_shares_capacity_with_heal_owner() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata(&root, "bucket", "object").await;
    let gate = owner(true);
    let heal_unit = gate.acquire().await;
    scanner.node_admission = Some(gate.clone());
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let result = scanner
        .scan_folder(CancellationToken::new(), folder.clone(), &mut DataUsageEntry::default())
        .await;
    assert!(
        matches!(result, Err(ScannerError::ResourceDeferred)),
        "scanner must not create a separate node budget: {result:?}"
    );
    assert_eq!(gate.snapshot().active, 1);
    heal_unit.complete();
    let mut entry = DataUsageEntry::default();
    scanner
        .scan_folder(CancellationToken::new(), folder, &mut entry)
        .await
        .expect("released heal capacity admits the real scan");
    assert_eq!(
        scanner
            .new_cache
            .size_recursive("bucket")
            .expect("complete bucket tree")
            .objects,
        1
    );
    assert_eq!(gate.snapshot().active, 0);
    assert!(
        gate.completed().is_some_and(|count| count > 1),
        "successful scanner work must reach shared feedback"
    );
}

#[tokio::test(start_paused = true)]
async fn scanner_node_admission_precancellation_never_polls_storage() {
    let gate = owner(false);
    let cancel = CancellationToken::new();
    let polled = std::cell::Cell::new(false);
    cancel.cancel();
    let operation = run_scanner_node_operation(Some(&gate), &cancel, async {
        polled.set(true);
        Ok::<_, std::io::Error>(())
    });
    assert!(matches!(operation.await, Err(ScannerError::Other(message)) if message == "Operation cancelled"));
    assert!(!polled.get());
    assert_eq!(gate.completed(), Some(0));
    assert_eq!(gate.snapshot().active, 0);
}

#[tokio::test]
async fn scanner_without_node_admission_preserves_existing_cancellation_scope() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = run_scanner_node_operation(None, &cancel, async {
        assert!(crate::workload_admission::scanner_node_operation_cancellation().is_none());
        Ok::<_, std::io::Error>(())
    })
    .await;
    assert!(matches!(result, Ok(Ok(()))));
}

#[tokio::test]
async fn scanner_node_cancellation_after_acquire_does_not_start_operation() {
    let gate = owner(true);
    let cancel = CancellationToken::new();
    let polled = std::cell::Cell::new(false);
    let hook_cancel = cancel.clone();
    let hook: Arc<dyn Fn() + Send + Sync> = Arc::new(move || hook_cancel.cancel());
    let result = crate::workload_admission::TEST_SCANNER_AFTER_NODE_ACQUIRE
        .scope(
            hook,
            run_scanner_node_operation(Some(&gate), &cancel, async {
                polled.set(true);
                Ok::<_, std::io::Error>(())
            }),
        )
        .await;
    assert!(matches!(result, Err(ScannerError::Other(message)) if message == "Operation cancelled"));
    assert!(!polled.get());
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0));
}

#[tokio::test]
async fn scanner_node_cancellation_returns_capacity_without_abandoning_operation() {
    let gate = owner(true);
    let cancel = CancellationToken::new();
    let (finish, completed) = tokio::sync::oneshot::channel();
    let mut running = Box::pin(run_scanner_node_operation(Some(&gate), &cancel, async move {
        completed.await.expect("the original operation remains owned");
        Ok::<_, std::io::Error>(())
    }));

    assert!(futures::poll!(&mut running).is_pending());
    assert_eq!(gate.snapshot().active, 1);
    cancel.cancel();
    assert!(futures::poll!(&mut running).is_pending());
    assert_eq!(gate.snapshot().active, 0, "cancellation returns the shared node slot");
    assert_eq!(gate.completed(), Some(0));

    let other = gate.try_acquire().expect("healthy work can use the released slot");
    drop(other);
    finish.send(()).expect("the original operation was not dropped");
    running
        .await
        .expect("admission succeeded")
        .expect("the original operation completed");
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0), "cancelled work is not reported as progress");
}

#[tokio::test]
async fn scanner_node_cancellation_wins_when_started_operation_becomes_ready() {
    let gate = owner(true);
    let cancel = CancellationToken::new();
    let (finish, completed) = tokio::sync::oneshot::channel();
    let mut running = Box::pin(run_scanner_node_operation(Some(&gate), &cancel, async move {
        completed.await.expect("the original operation remains owned");
        Ok::<_, std::io::Error>(())
    }));
    assert!(futures::poll!(&mut running).is_pending());
    finish.send(()).expect("the operation is still waiting");
    cancel.cancel();
    running
        .await
        .expect("admission succeeded")
        .expect("the original operation completed");
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0), "cancellation wins over a simultaneous result");
}

#[tokio::test]
async fn scanner_node_cancellation_during_ready_poll_is_not_progress() {
    let gate = owner(true);
    let cancel = CancellationToken::new();
    let operation_cancel = cancel.clone();
    run_scanner_node_operation(Some(&gate), &cancel, async move {
        assert!(!crate::workload_admission::scanner_node_operation_cancellation().is_some_and(|token| token.is_cancelled()));
        operation_cancel.cancel();
        assert!(crate::workload_admission::scanner_node_operation_cancellation().is_some_and(|token| token.is_cancelled()));
        Ok::<_, std::io::Error>(())
    })
    .await
    .expect("admission succeeded")
    .expect("the original operation completed");
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0), "cancellation during the ready poll is not progress");
}

#[tokio::test(start_paused = true)]
async fn scanner_node_admission_shrink_keeps_running_unit_and_preserves_errors() {
    let gate = owner(true);
    let cancel = CancellationToken::new();
    let mut running = Box::pin(run_scanner_node_operation(
        Some(&gate),
        &cancel,
        std::future::pending::<Result<(), std::io::Error>>(),
    ));
    assert!(futures::poll!(&mut running).is_pending());
    assert_eq!(gate.snapshot().active, 1);
    gate.update(&mut FeedbackController::new(), None);
    let polled = std::cell::Cell::new(false);
    let deferred = run_scanner_node_operation(Some(&gate), &cancel, async {
        polled.set(true);
        Ok::<_, std::io::Error>(())
    })
    .await;
    assert!(matches!(deferred, Err(ScannerError::ResourceDeferred)));
    assert!(!polled.get());
    assert_eq!(gate.snapshot().active, 1, "shrink cannot revoke admitted work");
    drop(running);
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0), "abandoned work is not progress");
    gate.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 2,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    let error = run_scanner_node_operation(Some(&gate), &cancel, async {
        Err::<(), _>(std::io::Error::from(std::io::ErrorKind::PermissionDenied))
    })
    .await
    .expect("resource admission succeeds")
    .expect_err("storage error survives");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert_eq!(gate.completed(), Some(0));
    run_scanner_node_operation(Some(&gate), &cancel, async { Ok::<_, std::io::Error>(()) })
        .await
        .expect("admitted")
        .expect("successful work");
    assert_eq!(gate.completed(), Some(1));
    assert_eq!(gate.snapshot().active, 0);
}

struct ShrinkOnMetadata {
    owner: Arc<NodeAdmission>,
    visited: AtomicU64,
}
impl metrics::CounterFn for ShrinkOnMetadata {
    fn increment(&self, value: u64) {
        if self.visited.fetch_add(value, Ordering::SeqCst) == 0 {
            self.owner.update(&mut FeedbackController::new(), None);
            assert_eq!(self.owner.snapshot().active, 1, "shrink occurs during the admitted object");
        }
    }
    fn absolute(&self, _: u64) {
        panic!("visit counter must be cumulative");
    }
}
struct ShrinkRecorder(Arc<ShrinkOnMetadata>);
impl metrics::Recorder for ShrinkRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() == "rustfs_scanner_object_metadata_visits_total" {
            metrics::Counter::from_arc(self.0.clone())
        } else {
            metrics::Counter::noop()
        }
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_mid_walk_shrink_preserves_partial_and_resumes_all_objects() {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for path in ["prefix/a/first", "prefix/b/second"] {
        write_test_object_metadata(&root, "bucket", path).await;
    }
    let gate = owner(true);
    let recorder = ShrinkRecorder(Arc::new(ShrinkOnMetadata {
        owner: gate.clone(),
        visited: AtomicU64::new(0),
    }));
    let scan = |cache| {
        let cancel = CancellationToken::new();
        scan_data_folder(
            cancel.clone(),
            ScannerCycleBudget::new(&cancel, Default::default()),
            vec![scanner.local_disk.clone()],
            scanner.local_disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest),
        )
    };
    let cache = DataUsageCache {
        info: crate::DataUsageCacheInfo {
            name: "bucket".to_string(),
            skip_healing: true,
            ..Default::default()
        },
        ..Default::default()
    };
    let baseline = scan(cache.clone()).await.expect("independent full walk");
    let partial = crate::workload_admission::TEST_SCANNER_NODE_ADMISSION
        .scope(gate.clone(), async {
            let mut scanning = std::pin::pin!(scan(cache));
            std::future::poll_fn(|cx| metrics::with_local_recorder(&recorder, || scanning.as_mut().poll(cx))).await
        })
        .await;
    let Err(ScannerError::PartialCache(partial)) = partial else {
        panic!("deferral below root cannot certify completion: {partial:?}");
    };
    assert!(!partial.info.snapshot_complete);
    assert_eq!(
        partial
            .size_recursive("bucket")
            .expect("partial keeps accounted work")
            .objects,
        1
    );
    assert_eq!(recorder.0.visited.load(Ordering::SeqCst), 1);
    assert_eq!(gate.snapshot().active, 0);
    gate.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 2,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    let resumed = crate::workload_admission::TEST_SCANNER_NODE_ADMISSION
        .scope(gate.clone(), scan(*partial))
        .await
        .expect("resume completes with restored node capacity");
    assert!(resumed.info.snapshot_complete);
    let expected = baseline.size_recursive("bucket").expect("baseline bucket");
    let actual = resumed.size_recursive("bucket").expect("resumed bucket");
    assert_eq!(
        (actual.objects, actual.size, actual.versions),
        (expected.objects, expected.size, expected.versions)
    );
    assert_eq!(actual.objects, 2);
    assert_eq!(gate.snapshot().active, 0);
}

#[tokio::test(start_paused = true)]
async fn scanner_node_admission_does_not_park_while_cache_lease_may_be_held() {
    let gate = owner(false);
    let cancel = CancellationToken::new();
    let polled = std::cell::Cell::new(false);
    let mut operation = Box::pin(run_scanner_node_operation(Some(&gate), &cancel, async {
        polled.set(true);
        Ok::<_, std::io::Error>(())
    }));
    assert!(matches!(
        futures::poll!(&mut operation),
        std::task::Poll::Ready(Err(ScannerError::ResourceDeferred))
    ));
    assert!(!polled.get());
    assert_eq!(gate.snapshot().active, 0);
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_early_deferral_keeps_old_tree_incomplete() {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard { temp_dir: Some(root) };
    let mut cache = DataUsageCache {
        info: crate::DataUsageCacheInfo {
            name: "bucket".to_string(),
            skip_healing: true,
            snapshot_complete: true,
            ..Default::default()
        },
        ..Default::default()
    };
    cache.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 7,
            size: 19,
            ..Default::default()
        },
    );
    let gate = owner(false);
    let cancel = CancellationToken::new();
    let result = crate::workload_admission::TEST_SCANNER_NODE_ADMISSION
        .scope(
            gate.clone(),
            scan_data_folder(
                cancel.clone(),
                ScannerCycleBudget::new(&cancel, Default::default()),
                vec![scanner.local_disk.clone()],
                scanner.local_disk,
                cache,
                None,
                HealScanMode::Normal,
                DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest),
            ),
        )
        .await;
    let Err(ScannerError::PartialCache(partial)) = result else {
        panic!("resource pressure is a partial census, not a disk error: {result:?}");
    };
    assert!(!partial.info.snapshot_complete);
    let totals = partial.size_recursive("bucket").expect("old tree retained");
    assert_eq!((totals.objects, totals.size), (7, 19));
    assert_eq!(gate.completed(), Some(0));
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_compacted_heal_child_preserves_deferral() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let bucket = "bucket";
    let object = "prefix/abandoned";
    for index in 0..DATA_SCANNER_COMPACT_AT_FOLDERS {
        tokio::fs::create_dir_all(root.join(bucket).join("prefix").join(format!("child-{index:04}")))
            .await
            .expect("create real compaction threshold");
    }
    let mut disks = vec![scanner.local_disk.clone()];
    for (index, version) in [(2, 0x1935), (3, 0x1935), (4, 0x1934)] {
        let disk_root = root.join(format!("disk-{index}"));
        // Two partial frames exceed the coordinator channel's capacity, so
        // discovery must consume the first before the finished frame can race it.
        for child in ["first", "second"] {
            let child_object = format!("{object}/{child}");
            let metadata = metadata_for_object_version(bucket, &child_object, Some(Uuid::from_u128(version)));
            write_test_object_metadata_bytes(&disk_root, bucket, &child_object, &metadata).await;
        }
        let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("remote fixture endpoint");
        disks.push(
            new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("remote fixture disk"),
        );
    }
    scanner.old_cache.info.name = bucket.to_string();
    scanner.new_cache.info.name = bucket.to_string();
    scanner.old_cache.replace(
        &format!("{bucket}/{object}"),
        "bucket/prefix",
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    scanner.heal_object_select = 1;
    scanner.disks = disks;
    scanner.disks_quorum = 2;
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    let gate = owner(true);
    scanner.node_admission = Some(gate.clone());
    let shrunk = Arc::new(AtomicBool::new(false));
    let response_gate = gate.clone();
    let response_shrunk = shrunk.clone();
    let (_commands, mut heal_rx) = rustfs_heal_contracts::heal_channel::init_heal_channels().expect("isolated heal channel");
    let responder = tokio::spawn(async move {
        while let Some(command) = heal_rx.recv().await {
            let rustfs_heal_contracts::heal_channel::HealReceiptCommand { request, response_tx } = command;
            if request.object_version_id.is_some() {
                response_shrunk.store(true, Ordering::SeqCst);
                response_gate.update(&mut FeedbackController::new(), None);
            }
            let _ = response_tx.send(Ok(rustfs_heal_contracts::heal_channel::HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: request.id,
            }));
        }
    });
    let mut into = DataUsageEntry::default();
    let folder = CachedFolder {
        name: "bucket/prefix".to_string(),
        parent: Some(hash_path(bucket)),
        object_heal_prob_div: 1,
    };
    let result = {
        let mut walk = std::pin::pin!(scanner.scan_folder(CancellationToken::new(), folder, &mut into));
        let mut controller = FeedbackController::new();
        let mut generation = 0;
        std::future::poll_fn(|cx| {
            // Keep telemetry fresh until the real heal-discovery receipt shrinks it.
            if !shrunk.load(Ordering::SeqCst) {
                generation += 1;
                gate.update(
                    &mut controller,
                    Some(FeedbackObservation {
                        generation,
                        max_concurrency: 1,
                        pressure: ResourcePressure::Unknown,
                        completed_units: None,
                    }),
                );
            }
            walk.as_mut().poll(cx)
        })
        .await
    };
    responder.abort();
    assert!(into.compacted, "real directory count must enter the compacted recursion branch");
    assert!(shrunk.load(Ordering::SeqCst), "remote orphan discovery must submit an exact-version heal");
    assert!(
        matches!(result, Err(ScannerError::ResourceDeferred)),
        "compacted recursion must retain typed resource pressure for partial-cache handling: {result:?}"
    );
    assert_eq!(gate.snapshot().active, 0);
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_progresses_during_continuous_heal_queue() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata(&root, "bucket", "object").await;
    for index in 0..65 {
        tokio::fs::write(root.join("bucket/object").join(format!("part.{index}.integrity.orphan")), b"")
            .await
            .expect("sidecars require more than one directory refill");
    }
    scanner.is_erasure_mode = true;
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&CancellationToken::new(), Default::default());
    let visits = super::metadata_visits::observe(&mut scanner);
    let gate = owner(true);
    scanner.node_admission = Some(gate.clone());
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    let heal_stop = CancellationToken::new();
    let heal_stop_worker = heal_stop.clone();
    let heal_gate = gate.clone();
    let heal_completions = Arc::new(AtomicU64::new(0));
    let completed = heal_completions.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let heals = tokio::spawn(async move {
        let mut held = heal_gate.acquire().await;
        started.send(()).expect("scanner waits for the first heal");
        loop {
            let next = heal_gate.acquire();
            tokio::pin!(next);
            assert!(futures::poll!(&mut next).is_pending());
            held.complete();
            completed.fetch_add(1, Ordering::Relaxed);
            held = tokio::select! {
                biased;
                _ = heal_stop_worker.cancelled() => return,
                permit = &mut next => permit,
            };
            tokio::task::yield_now().await;
        }
    });
    ready.await.expect("heal starts before scanner");
    let (node_tx, mut node_rx) = mpsc::channel::<crate::workload_admission::ScannerNodeRequest>(1);
    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    let mut handoffs = 0;
    let result = tokio::time::timeout(Duration::from_secs(2), async {
        let mut totals = DataUsageEntry::default();
        let scan = crate::workload_admission::with_scanner_node_requests(
            Some(node_tx),
            scanner.scan_folder(CancellationToken::new(), folder, &mut totals),
        );
        tokio::pin!(scan);
        loop {
            tokio::select! {
                result = &mut scan => break result,
                Some(request) = node_rx.recv() => {
                    handoffs += 1;
                    // This direct folder fixture owns no cache lock. Production
                    // coordinators release and revalidate their lease here.
                    let permit = request.owner.acquire().await;
                    assert_eq!(gate.snapshot().active, 1);
                    request.respond.send(permit).unwrap_or_else(|_| panic!("real walk still waits"));
                }
            }
        }
    })
    .await;
    heal_stop.cancel();
    heals.await.expect("continuous heal worker exits");
    result
        .expect("scanner must finish while Heal keeps queueing")
        .expect("real directory scan");
    assert_eq!(scanner.new_cache.size_recursive("bucket").expect("complete tree").objects, 1);
    assert!(handoffs > 1, "the walk must yield through multiple shared-node turns");
    assert_eq!(scanner.budget.entries_visited(), 67, "resumption must not replay directory entries");
    assert_eq!(
        visits.load(Ordering::Relaxed),
        1,
        "classification must account for the object exactly once"
    );
    assert!(heal_completions.load(Ordering::Relaxed) > 0, "Heal must also advance during the walk");
    assert_eq!(gate.snapshot().active, 0);
}

#[tokio::test]
#[serial]
async fn scanner_node_admission_usage_updates_preserve_totals_across_mid_walk_handoff() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    scanner.old_cache.info.name = "bucket".to_string();
    scanner.new_cache.info.name = "bucket".to_string();
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    scanner.skip_heal.store(true, Ordering::SeqCst);
    for (object, version_id) in [("existing/0000", Uuid::from_u128(1)), ("new/0000", Uuid::from_u128(2))] {
        write_test_object_metadata_bytes(
            &root,
            "bucket",
            object,
            &metadata_for_object_version("bucket", object, Some(version_id)),
        )
        .await;
    }
    scanner.old_cache.replace("bucket", "", DataUsageEntry::default());
    scanner
        .old_cache
        .replace("bucket/existing", "bucket", DataUsageEntry::default());
    scanner.old_cache.replace(
        "bucket/existing/0000",
        "bucket/existing",
        DataUsageEntry {
            objects: 1,
            size: 99,
            ..Default::default()
        },
    );
    let gate = owner(true);
    let visits = Arc::new(ShrinkOnMetadata {
        owner: gate.clone(),
        visited: AtomicU64::new(0),
    });
    scanner.node_admission = Some(gate.clone());
    scanner.object_metadata_visits = metrics::Counter::from_arc(visits.clone());
    let (sender, mut receiver) = mpsc::channel(1);
    scanner.usage_updates = Some(ScannerUsageUpdates::new(sender, "bucket".to_string()));
    let (node_tx, mut node_rx) = mpsc::channel::<crate::workload_admission::ScannerNodeRequest>(1);
    let cancel = CancellationToken::new();
    let mut progress = Vec::new();
    let mut handoffs = 0;
    let result = {
        let mut entry = DataUsageEntry::default();
        let scan = crate::workload_admission::with_scanner_node_requests(
            Some(node_tx),
            scanner.scan_folder(
                cancel.clone(),
                CachedFolder {
                    name: "bucket".to_string(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut entry,
            ),
        );
        tokio::pin!(scan);
        let coordinate = async {
            loop {
                tokio::select! {
                    result = &mut scan => break result,
                    Some(update) = receiver.recv() => progress.push(update),
                    Some(request) = node_rx.recv() => {
                        handoffs += 1;
                        assert!(Arc::ptr_eq(&request.owner, &gate), "handoff must use the real shared owner");
                        assert_eq!(visits.visited.load(Ordering::SeqCst), 1, "the next object cannot start before admission");
                        assert_eq!(gate.snapshot().active, 0, "the prior object returned its node unit");
                        let permit = request.owner.acquire();
                        tokio::pin!(permit);
                        assert!(futures::poll!(&mut permit).is_pending(), "the actual closed gate must defer the walk");
                        // This direct fixture owns no cache lease. The production
                        // coordinator releases/revalidates its lease at this handoff.
                        gate.update(
                            &mut FeedbackController::new(),
                            Some(FeedbackObservation {
                                generation: 2,
                                max_concurrency: 1,
                                pressure: ResourcePressure::Unknown,
                                completed_units: None,
                            }),
                        );
                        request.respond.send(permit.await).unwrap_or_else(|_| panic!("original walk still waits"));
                    }
                }
            }
        };
        tokio::pin!(coordinate);
        let bounded = tokio::time::timeout(Duration::from_secs(2), &mut coordinate).await;
        if bounded.is_err() {
            cancel.cancel();
            let _ = coordinate.await;
        }
        bounded
    };
    result
        .expect("the same admitted walk must finish within its original bound")
        .expect("real directory and metadata scan succeeds");
    assert_eq!(handoffs, 1, "the first metadata visit must exercise a real mid-walk deferral");
    assert_eq!(
        visits.visited.load(Ordering::SeqCst),
        2,
        "resumption cannot replay the first metadata visit"
    );
    assert_eq!(gate.snapshot().active, 0);
    assert!(gate.try_acquire().is_some(), "the handoff must not leak a queue entry or node unit");
    let totals = scanner
        .new_cache
        .checked_flatten_complete_scope("bucket")
        .expect("complete root after the original walk resumes");
    assert_eq!((totals.objects, totals.size, totals.versions), (2, 2, 2));
    assert_eq!(
        scanner
            .old_cache
            .find("bucket/existing/0000")
            .expect("retained baseline")
            .size,
        99,
        "progress and resumption cannot mutate the old cache"
    );
    while let Ok(update) = receiver.try_recv() {
        progress.push(update);
    }
    let first = progress.first().expect("the bounded sender emits actual partial progress");
    assert_eq!((first.objects, first.size, first.versions), (1, 1, 1));
    for update in &progress {
        assert!(update.objects <= totals.objects && update.size <= totals.size && update.versions <= totals.versions);
    }
    let updates = scanner
        .usage_updates
        .as_mut()
        .expect("requested progress owner survives resumption");
    let observed = updates.cache.checked_flatten("bucket").expect("connected progress cache");
    assert_eq!(
        (observed.objects, observed.size, observed.versions),
        (totals.objects, totals.size, totals.versions)
    );
    // Use the existing usage-update fixture's due-time mechanism; totals still
    // come solely from the real walk and the production send_update method.
    updates.last_update = SystemTime::UNIX_EPOCH;
    scanner.send_update().await;
    let update = receiver
        .try_recv()
        .expect("resumed aggregate reaches the actual bounded receiver");
    assert_eq!(
        (update.objects, update.size, update.versions),
        (totals.objects, totals.size, totals.versions)
    );
    scanner.send_update().await;
    assert!(
        receiver.try_recv().is_err(),
        "resumption must retain the original one-minute update throttle"
    );
}

#[cfg(unix)]
fn held_read_metadata_fixture() -> Vec<u8> {
    let old_id = Uuid::new_v4();
    let mut meta = FileMeta::new();
    meta.add_version(FileInfo {
        volume: "bucket".to_string(),
        name: "object".to_string(),
        version_id: Some(old_id),
        transition_status: crate::TRANSITION_COMPLETE.to_string(),
        transitioned_objname: "remote/object".to_string(),
        transition_version_id: Some(Uuid::new_v4()),
        transition_tier: "WARM".to_string(),
        mod_time: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    })
    .expect("add transitioned version");
    let mut deleted = FileInfo {
        volume: "bucket".to_string(),
        name: "object".to_string(),
        version_id: Some(old_id),
        mod_time: Some(OffsetDateTime::now_utc()),
        ..Default::default()
    };
    deleted.set_tier_free_version_id(&Uuid::new_v4().to_string());
    meta.delete_version(&deleted).expect("create free version");
    let mut live = FileInfo::new("object", 4, 2);
    live.volume = "bucket".to_string();
    live.name = "object".to_string();
    live.version_id = Some(Uuid::new_v4());
    live.versioned = true;
    live.mod_time = Some(OffsetDateTime::now_utc());
    live.size = 1;
    meta.add_version(live).expect("add live version");
    let versions = meta
        .get_file_info_versions("bucket", "object", false)
        .expect("fixture versions");
    assert_eq!(versions.versions.len(), 1);
    assert_eq!(versions.free_versions.len(), 1);
    meta.marshal_msg().expect("marshal real metadata fixture")
}

#[cfg(unix)]
fn enable_held_read_test(scanner: &mut FolderScanner, node: &Arc<NodeAdmission>, cancel: &CancellationToken) {
    scanner.node_admission = Some(node.clone());
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(cancel, Default::default());
    scanner.tier_registry.names = Arc::from(["WARM".to_string()]);
}

#[cfg(unix)]
async fn held_read_positive_dispatch_control() {
    let (mut scanner, root) = build_test_scanner().await;
    let _root = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata_bytes(&root, "bucket", "object", &held_read_metadata_fixture()).await;
    let cancel = CancellationToken::new();
    let node = owner(true);
    enable_held_read_test(&mut scanner, &node, &cancel);
    let (tx, mut stages) = tokio::sync::mpsc::unbounded_channel();
    let mut entry = DataUsageEntry::default();
    TEST_SIZE_DISPATCH_BOUNDARIES
        .scope(
            tx,
            scanner.scan_folder(
                cancel,
                CachedFolder {
                    name: "bucket".to_string(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut entry,
            ),
        )
        .await
        .expect("uncancelled real metadata scan");
    assert_eq!(stages.try_recv().expect("actual action boundary"), "apply_actions");
    assert_eq!(stages.try_recv().expect("actual free-version boundary"), "free_version_dispatch");
    assert!(stages.try_recv().is_err());
}

#[cfg(unix)]
async fn held_read_cancelled_scan(corrupt: bool) {
    use rustfs_concurrency::adaptive::AdaptiveAdmission;
    use std::task::Poll;

    let (mut scanner, root) = build_test_scanner().await;
    let _root = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let bytes = if corrupt {
        b"invalid xl metadata".to_vec()
    } else {
        held_read_metadata_fixture()
    };
    write_test_object_metadata_bytes(&root, "bucket", "object", &bytes).await;
    let healthy_path = root.join("healthy-read-probe");
    tokio::fs::write(&healthy_path, b"ready")
        .await
        .expect("write healthy fixture");
    let node = owner(true);
    let physical = AdaptiveAdmission::new(NonZeroU16::new(1).expect("one physical unit"), 1);
    let physical_unit = physical.acquire().await;
    let cancel = CancellationToken::new();
    enable_held_read_test(&mut scanner, &node, &cancel);
    let mut paused = LocalPublicationPause::install(
        scanner.local_disk.as_ref(),
        "bucket",
        "object/xl.meta",
        LocalPublicationStage::MetadataReadAfterOpen,
    )
    .expect("pause opened original metadata in its native worker");
    let (tx, mut stages) = tokio::sync::mpsc::unbounded_channel();
    let child_cancel = cancel.clone();
    let mut original = Box::pin(TEST_SIZE_DISPATCH_BOUNDARIES.scope(
        tx,
        ecstore_with_background_resource_unit(physical_unit, async move {
            ecstore_with_background_disk_io(child_cancel.clone(), async move {
                let mut entry = DataUsageEntry::default();
                let result = scanner
                    .scan_folder(
                        child_cancel,
                        CachedFolder {
                            name: "bucket".to_string(),
                            parent: None,
                            object_heal_prob_div: 1,
                        },
                        &mut entry,
                    )
                    .await;
                (result, scanner, entry)
            })
            .await
        }),
    ));
    let observation = Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS);
    let mut early_result = None;
    let entered = tokio::time::timeout(observation, async {
        tokio::select! {
            result = paused.entered() => result.is_ok(),
            result = original.as_mut() => { early_result = Some(result); false }
        }
    })
    .await
    .unwrap_or(false);
    let node_before = node.snapshot().active;
    let physical_before = physical.snapshot().active;
    let completed_before = node.completed();
    cancel.cancel();
    let after_cancel = if entered {
        Some(futures::poll!(original.as_mut()))
    } else {
        None
    };
    let original_pending = matches!(&after_cancel, Some(Poll::Pending));
    let node_after = node.snapshot().active;
    let physical_after = physical.snapshot().active;
    let healthy_cancel = CancellationToken::new();
    let mut healthy = if entered && original_pending && node_after == 0 {
        node.update(
            &mut FeedbackController::new(),
            Some(FeedbackObservation {
                generation: 2,
                max_concurrency: 1,
                pressure: ResourcePressure::Unknown,
                completed_units: None,
            }),
        );
        Some(Box::pin(run_scanner_node_operation(
            Some(&node),
            &healthy_cancel,
            tokio::fs::read(&healthy_path),
        )))
    } else {
        None
    };
    let mut healthy_result = None;
    let mut original_during_healthy = None;
    let healthy_in_time = if let Some(healthy_future) = healthy.as_mut() {
        tokio::time::timeout(observation, async {
            tokio::select! {
                biased;
                result = original.as_mut() => { original_during_healthy = Some(result); false }
                result = healthy_future.as_mut() => { healthy_result = Some(result); true }
            }
        })
        .await
        .unwrap_or(false)
    } else {
        false
    };
    let completed_after_healthy = node.completed();
    drop(paused);
    let captured = match after_cancel {
        Some(Poll::Ready(result)) => Some(result),
        _ => early_result.or(original_during_healthy),
    };
    let (joined_in_time, (joined, scanner, entry)) = match captured {
        Some(result) => (true, result),
        None => match tokio::time::timeout(observation, original.as_mut()).await {
            Ok(result) => (true, result),
            Err(_) => (false, original.as_mut().await),
        },
    };
    // A failed observation stays failed, but the same healthy read is still joined.
    if healthy_result.is_none() {
        if let Some(healthy_future) = healthy.as_mut() {
            healthy_result = Some(healthy_future.as_mut().await);
        }
    }
    drop(healthy);
    // Drop the completed scope before waiting for the native unit's final return.
    drop(original);
    let released_in_time = tokio::time::timeout(observation, async {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !released_in_time {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    }
    let mut observed_stages = Vec::new();
    while let Ok(stage) = stages.try_recv() {
        observed_stages.push(stage);
    }
    assert!(entered, "real xl.meta was opened before cancellation");
    assert!(completed_before.is_some());
    assert_eq!(node_before, 1);
    assert_eq!(physical_before, 1);
    assert!(original_pending, "the same operation stays pending until release");
    assert_eq!(node_after, 0, "cancellation immediately returns the node unit");
    assert_eq!(physical_after, 1, "the original physical scope remains held");
    assert!(healthy_in_time, "the healthy read completed before the original observation deadline");
    assert_eq!(
        healthy_result
            .expect("node released")
            .expect("healthy node admitted")
            .expect("healthy file read"),
        b"ready"
    );
    let expected = completed_before.map(|count| count.saturating_add(1));
    assert_eq!(completed_after_healthy, expected, "only healthy work completed");
    assert!(joined_in_time, "the original scan joined inside the original observation bound");
    assert!(joined.is_err(), "the original scan reports cancellation");
    assert_eq!(node.completed(), expected, "cancelled work cannot claim node progress");
    assert_eq!(node.snapshot().active, 0);
    assert!(released_in_time);
    assert!(
        observed_stages.is_empty(),
        "cancelled metadata cannot enter action or free-version dispatch: {observed_stages:?}"
    );
    assert_eq!(entry.failed_objects, 0, "cancellation is not corrupt metadata");
    assert!(
        scanner.new_cache.info.failed_objects.is_empty(),
        "cancellation creates no failed-object record"
    );
    assert!(scanner.new_cache.info.pending_heals.is_empty(), "cancellation creates no pending heal");
    assert!(!scanner.pending_heals_changed, "cancellation cannot change the heal ledger");
    assert_eq!(
        scanner.checkpoint_objects, 0,
        "cancelled read is not published as completed object progress"
    );
    assert_eq!(scanner.budget.progress().0, 0, "cancelled object must not enter budget progress");
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn cancelled_held_metadata_returns_node_without_dispatch_or_failed_record() {
    held_read_positive_dispatch_control().await;
    held_read_cancelled_scan(false).await;
    held_read_cancelled_scan(true).await;
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn held_metadata_native_worker_retains_physical_unit_after_parent_future_drop() {
    use rustfs_concurrency::adaptive::AdaptiveAdmission;

    let (scanner, root) = build_test_scanner().await;
    let _root = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata(&root, "bucket", "object").await;
    let disk = scanner.local_disk.clone();
    let mut opened =
        LocalPublicationPause::install(disk.as_ref(), "bucket", "object/xl.meta", LocalPublicationStage::MetadataReadAfterOpen)
            .expect("pause the actual opened file");
    let mut read_finished =
        LocalPublicationPause::install(disk.as_ref(), "bucket", "object/xl.meta", LocalPublicationStage::MetadataReadAfterRead)
            .expect("observe the actual native read finishing");
    let physical = AdaptiveAdmission::new(NonZeroU16::new(1).expect("one physical unit"), 1);
    let physical_unit = physical.acquire().await;
    let mut original = Box::pin(ecstore_with_background_resource_unit(physical_unit, async move {
        ecstore_with_background_disk_io(CancellationToken::new(), async move {
            crate::storage_api::EcstoreDiskAPI::read_metadata(disk.as_ref(), "bucket", "object/xl.meta").await
        })
        .await
    }));
    let observation = Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS);
    let entered = tokio::time::timeout(observation, async {
        tokio::select! {
            result = opened.entered() => result.is_ok(),
            _ = original.as_mut() => false,
        }
    })
    .await
    .unwrap_or(false);
    let physical_before_drop = physical.snapshot().active;
    // No scanner cache lock is acquired: only the actual disk read and its physical owner exist.
    drop(original);
    let physical_after_drop = physical.snapshot().active;
    drop(opened);
    let finished = tokio::time::timeout(observation, read_finished.entered())
        .await
        .is_ok_and(|result| result.is_ok());
    let physical_at_native_read_completion = physical.snapshot().active;
    drop(read_finished);
    let released_in_time = tokio::time::timeout(observation, async {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !released_in_time {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    }
    assert!(entered, "the original native worker opened xl.meta");
    assert_eq!(physical_before_drop, 1);
    assert_eq!(
        physical_after_drop, 1,
        "the native worker retains the unit after its parent future is gone"
    );
    assert!(finished, "the same native worker completes its actual read after release");
    assert_eq!(
        physical_at_native_read_completion, 1,
        "the unit stays held through native read completion"
    );
    assert!(released_in_time, "native worker exit returns the physical unit");
}

#[cfg(unix)]
async fn join_real_heal_scan<F>(
    mut scan: std::pin::Pin<&mut F>,
    receiver: &mut rustfs_heal_contracts::heal_channel::HealReceiptReceiver,
    received: &mut Vec<(String, Option<String>, Option<String>)>,
) -> Result<(), ScannerError>
where
    F: std::future::Future<Output = Result<(), ScannerError>>,
{
    loop {
        tokio::select! {
            result = scan.as_mut() => return result,
            Some(command) = receiver.recv() => {
                let rustfs_heal_contracts::heal_channel::HealReceiptCommand { request, response_tx } = command;
                received.push((request.bucket.clone(), request.object_prefix.clone(), request.object_version_id.clone()));
                let _ = response_tx.send(Ok(rustfs_heal_contracts::heal_channel::HealAdmissionReceipt {
                    result: HealAdmissionResult::Accepted,
                    task_id: request.id,
                }));
            }
        }
    }
}

#[cfg(unix)]
async fn real_heal_channel_case(
    cancel_held_read: bool,
    receiver: &mut rustfs_heal_contracts::heal_channel::HealReceiptReceiver,
) -> (bool, bool, Result<(), ScannerError>, Vec<(String, Option<String>, Option<String>)>, Uuid) {
    use std::task::Poll;

    let (mut scanner, root) = build_test_scanner().await;
    let _root = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let version = Uuid::new_v4();
    write_test_object_metadata_bytes(&root, "bucket", "object", &metadata_for_object_version("bucket", "object", Some(version)))
        .await;
    let cancel = CancellationToken::new();
    let node = owner(true);
    enable_held_read_test(&mut scanner, &node, &cancel);
    scanner.heal_object_select = 1;
    let mut paused = if cancel_held_read {
        Some(
            LocalPublicationPause::install(
                scanner.local_disk.as_ref(),
                "bucket",
                "object/xl.meta",
                LocalPublicationStage::MetadataReadAfterOpen,
            )
            .expect("pause actual metadata open"),
        )
    } else {
        None
    };
    let physical = rustfs_concurrency::adaptive::AdaptiveAdmission::new(NonZeroU16::new(1).expect("one unit"), 1);
    let physical_unit = physical.acquire().await;
    let mut entry = DataUsageEntry::default();
    let scan = scanner.scan_folder(
        cancel.clone(),
        CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        &mut entry,
    );
    let mut original = Box::pin(ecstore_with_background_resource_unit(
        physical_unit,
        ecstore_with_background_disk_io(cancel.clone(), scan),
    ));
    let observation = Duration::from_secs(rustfs_config::DEFAULT_DRIVE_METADATA_TIMEOUT_SECS);
    let mut early_result = None;
    let (entered, pending) = if let Some(pause) = paused.as_mut() {
        let entered = tokio::time::timeout(observation, async {
            tokio::select! {
                result = pause.entered() => result.is_ok(),
                result = original.as_mut() => { early_result = Some(result); false }
            }
        })
        .await
        .unwrap_or(false);
        cancel.cancel();
        let observed = if entered {
            Some(futures::poll!(original.as_mut()))
        } else {
            None
        };
        let pending = matches!(&observed, Some(Poll::Pending));
        if let Some(Poll::Ready(result)) = observed {
            early_result = Some(result);
        }
        (entered, pending)
    } else {
        (true, true)
    };
    drop(paused);
    let mut received = Vec::new();
    let (joined_in_time, result) = match early_result {
        Some(result) => (false, result),
        None => {
            let mut joined = Box::pin(join_real_heal_scan(original.as_mut(), receiver, &mut received));
            match tokio::time::timeout(observation, joined.as_mut()).await {
                Ok(result) => (true, result),
                Err(_) => (false, joined.as_mut().await),
            }
        }
    };
    drop(original);
    let physical_in_time = tokio::time::timeout(observation, async {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    if !physical_in_time {
        while physical.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    }
    while let Ok(command) = receiver.try_recv() {
        let rustfs_heal_contracts::heal_channel::HealReceiptCommand { request, response_tx } = command;
        received.push((request.bucket.clone(), request.object_prefix.clone(), request.object_version_id.clone()));
        let _ = response_tx.send(Ok(rustfs_heal_contracts::heal_channel::HealAdmissionReceipt {
            result: HealAdmissionResult::Accepted,
            task_id: request.id,
        }));
    }
    (entered && pending, joined_in_time && physical_in_time, result, received, version)
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn held_read_cancellation_prevents_real_heal_receipt_command() {
    let (_commands, mut receiver) = rustfs_heal_contracts::heal_channel::init_heal_channels().expect("isolated heal channel");
    let (positive_observed, positive_joined, positive, positive_requests, version) =
        real_heal_channel_case(false, &mut receiver).await;
    let (cancel_observed, cancel_joined, cancelled, cancelled_requests, _) = real_heal_channel_case(true, &mut receiver).await;
    assert!(positive_observed && positive_joined);
    assert!(positive.is_ok(), "uncancelled scan completes");
    assert_eq!(
        positive_requests,
        vec![("bucket".to_string(), Some("object".to_string()), Some(version.to_string()))]
    );
    assert!(cancel_observed && cancel_joined, "same held original must be cancelled and joined");
    assert!(cancelled.is_err(), "cancelled scan reports cancellation");
    assert!(cancelled_requests.is_empty(), "cancelled metadata must not submit an actual heal request");
}
