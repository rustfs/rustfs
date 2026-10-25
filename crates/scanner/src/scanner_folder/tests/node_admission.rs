// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
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
async fn scanner_physical_native_tail_releases_node_and_allows_unrelated_disk() {
    use crate::storage_api::owner::{PhysicalDiskBinding, ecstore_spawn_blocking_with_resource_ownership};
    use crate::workload_admission::with_scanner_physical_disk;
    use rustfs_concurrency::adaptive::AdaptiveAdmission;

    let node = owner(true);
    let disk = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
    let binding = Arc::new(PhysicalDiskBinding::default());
    binding.set_test_capacity(vec![disk.clone()]).expect("physical fixture");
    let (entered, started) = tokio::sync::oneshot::channel();
    let (finish, blocked) = std::sync::mpsc::channel();
    let cancel = CancellationToken::new();
    let mut operation = Box::pin(with_scanner_physical_disk(
        Some(binding),
        run_scanner_node_operation(Some(&node), &cancel, async {
            ecstore_spawn_blocking_with_resource_ownership(move || {
                let _ = entered.send(());
                blocked.recv_timeout(Duration::from_secs(2)).expect("native worker released");
            })
            .await
            .map_err(std::io::Error::other)
        }),
    ));
    assert!(futures::poll!(&mut operation).is_pending());
    tokio::time::timeout(Duration::from_secs(1), started)
        .await
        .expect("native worker starts")
        .expect("start signal");
    drop(operation);
    assert_eq!(node.snapshot().active, 0, "cancelled waiter returns scheduling capacity");
    assert_eq!(node.completed(), Some(0));
    assert_eq!(disk.snapshot().active, 1, "native tail still owns its physical capacity");
    let other = Arc::new(PhysicalDiskBinding::default());
    other
        .set_test_capacity(vec![AdaptiveAdmission::new(NonZeroU16::MIN, 1)])
        .expect("independent disk");
    with_scanner_physical_disk(
        Some(other),
        run_scanner_node_operation(Some(&node), &cancel, async { Ok::<_, std::io::Error>(()) }),
    )
    .await
    .expect("independent disk admission")
    .expect("independent operation");
    finish.send(()).expect("finish native work");
    tokio::time::timeout(Duration::from_secs(1), async {
        while disk.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native completion returns capacity");
    assert_eq!(disk.completed(), Some(0), "cancelled native work is not successful scanner progress");
}

#[tokio::test]
async fn scanner_physical_unknown_binding_never_falls_back_to_node_only() {
    let node = owner(true);
    let polled = std::cell::Cell::new(false);
    let cancel = CancellationToken::new();
    let result = crate::workload_admission::with_scanner_physical_disk(
        Some(Arc::new(crate::storage_api::owner::PhysicalDiskBinding::default())),
        run_scanner_node_operation(Some(&node), &cancel, async {
            polled.set(true);
            Ok::<_, std::io::Error>(())
        }),
    )
    .await;
    assert!(matches!(result, Err(ScannerError::ResourceDeferred)));
    assert!(!polled.get());
    assert_eq!(node.snapshot().active, 0);
}

#[tokio::test]
async fn scanner_physical_metadata_scope_retains_only_the_local_read() {
    use crate::storage_api::owner::{PhysicalDiskBinding, ecstore_spawn_blocking_with_resource_ownership};
    use crate::workload_admission::{run_scanner_metadata_operation, with_scanner_metadata_io, with_scanner_physical_disk};
    use rustfs_concurrency::adaptive::AdaptiveAdmission;

    for local_read in [true, false] {
        let node = owner(true);
        let disk = AdaptiveAdmission::new(NonZeroU16::MIN, 1);
        let binding = Arc::new(PhysicalDiskBinding::default());
        binding.set_test_capacity(vec![disk.clone()]).expect("physical fixture");
        let (entered, started) = tokio::sync::oneshot::channel();
        let (finish, blocked) = std::sync::mpsc::channel();
        let cancel = CancellationToken::new();
        let mut operation = Box::pin(with_scanner_physical_disk(
            Some(binding),
            run_scanner_metadata_operation(Some(&node), &cancel, async {
                let io = async {
                    ecstore_spawn_blocking_with_resource_ownership(move || {
                        let _ = entered.send(());
                        blocked.recv_timeout(Duration::from_secs(2)).expect("finish native job");
                    })
                    .await
                    .map_err(std::io::Error::other)
                };
                if local_read {
                    with_scanner_metadata_io(io).await
                } else {
                    io.await
                }
            }),
        ));
        assert!(futures::poll!(&mut operation).is_pending());
        tokio::time::timeout(Duration::from_secs(1), started)
            .await
            .expect("native start")
            .expect("start signal");
        drop(operation);
        assert_eq!(node.snapshot().active, 0);
        assert_eq!(
            disk.snapshot().active,
            u32::from(local_read),
            "later lifecycle work cannot retain the metadata disk's unit"
        );
        finish.send(()).expect("release native job");
        tokio::time::timeout(Duration::from_secs(1), async {
            while disk.snapshot().active != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("physical ownership eventually returned");
    }
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
    scanner
        .local_disk
        .physical_admission()
        .expect("local physical binding")
        .set_test_capacity(vec![rustfs_concurrency::adaptive::AdaptiveAdmission::new(
            std::num::NonZeroU16::MIN,
            1,
        )])
        .expect("synthetic physical device");
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
    check_early_admission_deferral(false, 1).await;
}

#[tokio::test]
#[serial]
async fn scanner_physical_early_deferral_keeps_real_walk_incomplete() {
    check_early_admission_deferral(true, 0).await;
}

async fn check_early_admission_deferral(node_open: bool, physical_limit: u32) {
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
    scanner
        .local_disk
        .physical_admission()
        .expect("local binding")
        .set_test_capacity(vec![rustfs_concurrency::adaptive::AdaptiveAdmission::new(
            NonZeroU16::MIN,
            physical_limit,
        )])
        .expect("independent physical capacity");
    let gate = owner(node_open);
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
    scanner.update_cache.info.name = bucket.to_string();
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
                    request.respond.send(Ok(crate::workload_admission::ScannerNodePermit::Node(permit)))
                        .unwrap_or_else(|_| panic!("real walk still waits"));
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
