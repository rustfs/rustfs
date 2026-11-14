// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

const PREFIX: &str = "bucket/prefix";
const OBJECTS: u64 = 4;

struct CompactedFixture {
    disk: Arc<Disk>,
    root: std::path::PathBuf,
    cache: DataUsageCache,
    identity: crate::DataUsageScanIdentity,
    store: Arc<FixtureStore>,
    _cleanup: TestGuard,
}

async fn scan(
    disk: &Arc<Disk>,
    cache: DataUsageCache,
    mode: HealScanMode,
    max_objects: u64,
) -> (ScannerDiskScanOutcome, Arc<ScannerCycleBudget>) {
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &CancellationToken::new(),
        ScannerCycleBudgetConfig {
            max_objects: Some(max_objects),
            ..Default::default()
        },
    );
    let outcome = disk
        .clone()
        .nsscanner_disk(budget.token(), budget.clone(), vec![disk.clone()], cache, None, super::scan_options(mode))
        .await
        .expect("bounded real disk scan");
    (outcome, budget)
}

async fn save_reload(store: &Arc<FixtureStore>, cache: &DataUsageCache) -> DataUsageCache {
    let revisions = DataUsageCache::default()
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("fixture save revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("save compacted checkpoint through real codec and revision checks");
    let loaded = store.strict_load().await;
    assert_eq!(loaded.info.snapshot_complete, cache.info.snapshot_complete);
    assert_eq!(loaded.info.scan_checkpoint, cache.info.scan_checkpoint);
    assert_eq!(loaded.info.scan_identity, cache.info.scan_identity);
    assert_eq!(loaded.info.scan_plan_digest, cache.info.scan_plan_digest);
    assert_eq!(
        loaded.checked_flatten("bucket").expect("reloaded root").size,
        cache.checked_flatten("bucket").expect("returned root").size
    );
    loaded
}

impl CompactedFixture {
    async fn new(mode: HealScanMode) -> Self {
        let (scanner, root) = build_test_scanner().await;
        let cleanup = TestGuard {
            temp_dir: Some(root.clone()),
        };
        for index in 0..OBJECTS {
            write_checkpoint_object(&root, &format!("prefix/{index:04}"), &[(None, 1)]).await;
        }
        let identity = crate::DataUsageScanIdentity {
            scan_mode: mode,
            tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
            ..bound_checkpoint().1
        };
        let mut cache = DataUsageCache::default();
        // Build and compact coverage in the first real sweep. No synthetic
        // compacted cache is injected.
        cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
        cache.info.skip_healing = true;
        let (outcome, budget) = scan(&scanner.local_disk, cache, mode, OBJECTS + 1).await;
        let ScannerDiskScanOutcome::Complete(completed) = outcome else {
            panic!("fixture baseline must complete")
        };
        assert_eq!(budget.progress().0, OBJECTS, "baseline must read every metadata object");
        cache = completed;
        let store = FixtureStore::new();
        cache = save_reload(&store, &cache).await;
        assert!(cache.find(PREFIX).expect("baseline prefix").compacted);
        assert_eq!(cache.checked_flatten("bucket").expect("complete baseline").size, 4);
        assert!(cache.info.scan_progress.is_none());
        Self {
            disk: scanner.local_disk,
            root,
            cache,
            identity,
            store,
            _cleanup: cleanup,
        }
    }

    fn next_cycle(&self, sampled: bool) -> u64 {
        let first = self.cache.info.next_cycle + 1;
        (first..first + 16)
            .find(|cycle| hash_path(PREFIX).mod_(u32::try_from(*cycle).expect("bounded cycle"), 16) == sampled)
            .expect("one selected cycle and non-selected cycles exist within the fixed rotation")
    }

    fn prepare(&mut self, cycle: u64) {
        let state = crate::scanner_io::current_cache_root_or_prepare_with_generation(
            &mut self.cache,
            "bucket",
            SOURCE,
            cycle,
            7,
            PLAN,
            crate::scanner_io::DataUsageCacheReuseOptions {
                checkpoint_identity: Some(self.identity),
                ..Default::default()
            },
        );
        assert!(matches!(state, crate::scanner_io::DataUsageCacheScanState::Prepared { .. }));
        assert_eq!(self.cache.info.scan_identity, Some(self.identity));
        assert_eq!(self.cache.info.scan_plan_digest, Some(PLAN));
        assert!(
            self.cache.info.scan_progress.is_none(),
            "same-strength complete baseline uses the existing tree"
        );
        assert!(self.cache.find(PREFIX).expect("prepared prefix").compacted);
    }

    async fn change_metadata_without_activity_event(&self) {
        // This models a local metadata change not announced by a segment
        // producer. Deep traversal must not depend on a usage-clean signal.
        // Healing is disabled: the oracle proves metadata re-entry, not repair.
        write_checkpoint_object(&self.root, "prefix/0000", &[(None, 7)]).await;
    }
}

#[tokio::test]
#[serial]
async fn deep_compacted_same_plan_rechecks_unsampled_prefix() {
    temp_env::async_with_vars([(ENV_DATA_USAGE_UPDATE_DIR_CYCLES, Some("16"))], async {
        let mut fixture = CompactedFixture::new(HealScanMode::Deep).await;
        fixture.change_metadata_without_activity_event().await;
        fixture.prepare(fixture.next_cycle(false));
        let (outcome, budget) = scan(&fixture.disk, fixture.cache.clone(), HealScanMode::Deep, OBJECTS + 1).await;
        assert_eq!(
            budget.progress().0,
            OBJECTS,
            "Deep must inspect compacted children even outside the usage sample cycle"
        );
        let ScannerDiskScanOutcome::Complete(cache) = outcome else {
            panic!("bounded Deep scan must complete")
        };
        let loaded = save_reload(&fixture.store, &cache).await;
        let root = loaded.checked_flatten("bucket").expect("Deep scan root");
        assert_eq!((root.objects, root.size), (4, 10), "Deep must observe the changed metadata");
    })
    .await;
}

#[tokio::test]
#[serial]
async fn deep_compacted_normal_scan_preserves_periodic_sampling() {
    temp_env::async_with_vars([(ENV_DATA_USAGE_UPDATE_DIR_CYCLES, Some("16"))], async {
        let mut fixture = CompactedFixture::new(HealScanMode::Normal).await;
        fixture.change_metadata_without_activity_event().await;
        fixture.prepare(fixture.next_cycle(false));
        let (outcome, budget) = scan(&fixture.disk, fixture.cache.clone(), HealScanMode::Normal, OBJECTS + 1).await;
        assert_eq!(budget.progress().0, 0, "Normal retains its existing unsampled-subtree policy");
        let ScannerDiskScanOutcome::Complete(cache) = outcome else { panic!("normal sampling completes") };
        assert_eq!(cache.checked_flatten("bucket").expect("sampled root").size, 4);
        fixture.cache = save_reload(&fixture.store, &cache).await;
        fixture.prepare(fixture.next_cycle(true));
        let (outcome, budget) = scan(&fixture.disk, fixture.cache.clone(), HealScanMode::Normal, OBJECTS + 1).await;
        assert_eq!(budget.progress().0, OBJECTS);
        let ScannerDiskScanOutcome::Complete(cache) = outcome else {
            panic!("selected Normal rotation completes")
        };
        assert_eq!(cache.checked_flatten("bucket").expect("refreshed root").size, 10);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn deep_compacted_budget_preserves_partial_checkpoint() {
    temp_env::async_with_vars([(ENV_DATA_USAGE_UPDATE_DIR_CYCLES, Some("16"))], async {
        let mut fixture = CompactedFixture::new(HealScanMode::Deep).await;
        fixture.prepare(fixture.next_cycle(false));
        let (outcome, budget) = scan(&fixture.disk, fixture.cache.clone(), HealScanMode::Deep, 2).await;
        assert_eq!(budget.progress().0, 2);
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        let ScannerDiskScanOutcome::Partial(cache) = outcome else {
            panic!("Deep must retain budget interruption as partial")
        };
        assert!(!cache.info.snapshot_complete);
        let loaded = save_reload(&fixture.store, &cache).await;
        assert!(!loaded.info.snapshot_complete);
        assert!(loaded.checked_flatten("bucket").expect("partial root").objects > 0);
    })
    .await;
}

#[tokio::test]
#[serial]
async fn large_prefix_resumes_after_budget_across_cycles_and_leaders() {
    check_large_prefix_resumption(HealScanMode::Normal).await;
}

#[tokio::test]
#[serial]
async fn large_prefix_deep_resumes_after_budget_across_cycles_and_leaders() {
    check_large_prefix_resumption(HealScanMode::Deep).await;
}

async fn check_large_prefix_resumption(mode: HealScanMode) {
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..DATA_SCANNER_COMPACT_AT_FOLDERS {
        write_checkpoint_object(&root, &format!("prefix/{index:04}"), &[(None, 1)]).await;
    }
    write_checkpoint_object(&root, "z-last", &[(None, 1)]).await;
    let identity = crate::DataUsageScanIdentity {
        scan_mode: mode,
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    let store = FixtureStore::new();
    let mut cache = DataUsageCache::default();
    let mut previous = 0;
    let mut scanned_objects = 0;
    for round in 0..8 {
        cache.prepare_bucket_checkpoint("bucket", 11 + round, 7 + round, SOURCE, PLAN, identity);
        cache.info.skip_healing = true;
        let (outcome, budget) = scan(&scanner.local_disk, cache, mode, 1000).await;
        scanned_objects += budget.progress().0;
        let (scanned, complete) = match outcome {
            ScannerDiskScanOutcome::Complete(cache) => (cache, true),
            ScannerDiskScanOutcome::Partial(cache) => (cache, false),
            ScannerDiskScanOutcome::NamespaceNotFound(_) => panic!("fixture namespace exists"),
        };
        cache = save_reload(&store, &scanned).await;
        let total = cache.checked_flatten("bucket").expect("saved bucket coverage");
        assert!(total.objects > previous, "round {round}: large-prefix coverage must advance after reload");
        if complete {
            let expected = DATA_SCANNER_COMPACT_AT_FOLDERS + 1;
            assert_eq!(total.objects, expected);
            assert_eq!(total.size, expected);
            assert_eq!(cache.find("bucket/z-last").map(|entry| entry.objects), Some(1));
            assert!(cache.info.snapshot_complete);
            let expected_reads = u64::try_from(expected).expect("fixture object count fits u64");
            // The budget cancels after accounting the last object, before its
            // child traversal completes. Only that boundary object may replay.
            assert!(
                (expected_reads..=expected_reads + round).contains(&scanned_objects),
                "resumption must not reread objects before the completed frontier"
            );
            assert!(cache.info.scan_progress.is_none());
            assert!(cache.info.scan_checkpoint.is_none());
            assert!(cache.info.scan_coverage_receipt.is_none());

            // A completed prefix releases its children before the first sweep
            // returns; a later same-plan scan keeps that compact representation.
            let retained_entries = cache.cache.len();
            assert!(cache.find(PREFIX).expect("completed prefix").compacted);
            assert_eq!(retained_entries, 3, "only the bucket, compacted prefix and tail remain");
            cache.prepare_bucket_checkpoint("bucket", 12 + round, 7 + round, SOURCE, PLAN, identity);
            assert!(cache.info.scan_progress.is_none(), "a complete same-plan cache needs no forward sweep");
            let (outcome, _) = scan(&scanner.local_disk, cache, mode, expected_reads + 1).await;
            let ScannerDiskScanOutcome::Complete(compacted) = outcome else {
                panic!("same-plan scan must complete after resuming the large prefix")
            };
            let reloaded = save_reload(&store, &compacted).await;
            assert!(reloaded.find(PREFIX).expect("compacted prefix").compacted);
            assert!(
                reloaded.cache.len() <= retained_entries,
                "later sweeps must not retain the compacted prefix's children"
            );
            let total = reloaded.checked_flatten("bucket").expect("compacted bucket coverage");
            assert_eq!((total.objects, total.size), (expected, expected));
            assert!(reloaded.info.snapshot_complete);
            return;
        }
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(!cache.info.snapshot_complete);
        let frontier = cache
            .validated_scan_frontier()
            .expect("saved partial coverage must have a valid frontier");
        assert!(cache.find(frontier).is_some(), "the persisted frontier must retain its child record");
        previous = total.objects;
    }
    panic!("large prefix must finish within repeated bounded scans");
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn wide_root_legacy_permission_failure_isolated_to_child() {
    use std::os::unix::fs::PermissionsExt;

    assert!(!crate::scanner_is_erasure().await, "single-drive Namespace scan");
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_checkpoint_object(&root, "healthy/object", &[(None, 7)]).await;
    let bucket = root.join("bucket");
    let denied = bucket.join("denied");
    std::fs::create_dir(&denied).expect("create denied root child");
    for idx in 0..DATA_SCANNER_COMPACT_AT_FOLDERS - 2 {
        std::fs::create_dir(bucket.join(format!("empty-{idx:05}"))).expect("create wide directory");
    }
    assert_eq!(std::fs::read_dir(&bucket).expect("list root").count(), DATA_SCANNER_COMPACT_AT_FOLDERS);
    std::fs::set_permissions(&denied, std::fs::Permissions::from_mode(0o000)).expect("remove child read access");

    match std::fs::read_dir(&denied) {
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied),
        Ok(_) => {
            std::fs::set_permissions(denied, std::fs::Permissions::from_mode(0o700)).expect("restore child access");
            eprintln!("skipping permission-denied fixture because this process can read mode-000 directories");
            return;
        }
    }
    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    assert!(cache.info.scan_progress.is_none());
    cache.info.skip_healing = true;
    let budget = ScannerCycleBudget::new(&CancellationToken::new(), Default::default());
    let wide = scanner
        .local_disk
        .clone()
        .nsscanner_disk(
            budget.token(),
            budget,
            vec![scanner.local_disk.clone()],
            cache,
            None,
            super::scan_options(HealScanMode::Normal),
        )
        .await;
    std::fs::set_permissions(denied, std::fs::Permissions::from_mode(0o700)).expect("restore child access");
    let ScannerDiskScanOutcome::Partial(wide) = wide.expect("wide legacy scanner must keep the same child-error isolation")
    else {
        panic!("an unreadable prefix must prevent complete snapshot publication");
    };
    assert!(!wide.info.snapshot_complete);
    let summary = wide.checked_flatten("bucket").expect("aggregate healthy child");
    assert!(summary.compacted);
    assert_eq!((summary.objects, summary.size), (1, 7));
    assert_eq!(wide.cache.len(), 1);
}

// Select one fixture disk so unrelated concurrent scans cannot trigger this
// cancellation. The hook runs at the root flush boundary, after compaction.
static CANCEL_ROOT_FLUSH: Mutex<Option<std::path::PathBuf>> = Mutex::new(None);

pub(in crate::scanner_folder) fn cancel_at_root_flush(root: &str, cache: &DataUsageCache, ctx: &CancellationToken) {
    let mut selected = CANCEL_ROOT_FLUSH.lock().expect("root flush cancellation lock");
    if selected.as_deref() == Some(std::path::Path::new(root)) {
        let cached = cache.root().expect("root cached before flush");
        assert!(cached.compacted, "fixture must reach post-reduction compaction");
        assert!(cached.children.is_empty());
        *selected = None;
        ctx.cancel();
    }
}

struct RootFlushCancellationGuard;

impl Drop for RootFlushCancellationGuard {
    fn drop(&mut self) {
        *CANCEL_ROOT_FLUSH.lock().expect("clear root flush cancellation") = None;
    }
}

async fn wide_legacy_root() -> (FolderScanner, std::path::PathBuf, TestGuard, DataUsageCache) {
    let (scanner, root) = build_test_scanner().await;
    let cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_checkpoint_object(&root, "a-healthy/object", &[(Some(Uuid::from_u128(101)), 7)]).await;
    write_checkpoint_object(&root, "z-healthy/object", &[(Some(Uuid::from_u128(102)), 11)]).await;
    for index in 0..DATA_SCANNER_COMPACT_AT_FOLDERS - 2 {
        std::fs::create_dir(root.join("bucket").join(format!("empty-{index:05}"))).expect("create wide root child");
    }
    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    assert!(cache.info.scan_progress.is_none());
    (scanner, root, cleanup, cache)
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn wide_root_legacy_namespace_error_isolated_to_child() {
    assert!(!crate::scanner_is_erasure().await, "single-drive Namespace scan");
    let (scanner, root, _cleanup, cache) = wide_legacy_root().await;
    // The error occurs while resolving a symlink inside the child, after its
    // directory opened successfully. An open-only permission check misses it.
    let regular = root.join("regular-file");
    std::fs::write(&regular, b"payload").expect("write non-directory symlink target");
    for broken in ["m-broken", "nested/broken"] {
        let directory = root.join("bucket").join(broken);
        std::fs::create_dir_all(&directory).expect("create failing namespace child");
        let link = directory.join("bad-link");
        symlink(regular.join("child"), &link).expect("create NotADirectory symlink");
        assert_eq!(
            std::fs::metadata(link).expect_err("invalid symlink target").kind(),
            ErrorKind::NotADirectory
        );
    }
    write_checkpoint_object(&root, "nested/healthy/object", &[(Some(Uuid::from_u128(103)), 13)]).await;
    write_checkpoint_object(&root, "m-partial", &[(Some(Uuid::from_u128(104)), 101)]).await;
    // Fail enumeration after this child's metadata contributed usage. Simply
    // catching the error with a shared accumulator would retain its 101 bytes.
    let _partial =
        erasure_probe::OrderedDirectory::failing(&scanner.local_disk.path().join("bucket/m-partial"), 1, ErrorKind::Other);
    let (outcome, budget) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 5).await;
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("namespace errors must prevent complete snapshot publication without aborting healthy siblings")
    };
    assert!(!cache.info.snapshot_complete);
    assert_eq!(budget.progress().0, 4, "healthy siblings and the failing child must read metadata");
    let total = cache.checked_flatten("bucket").expect("valid compacted root");
    assert!(total.compacted);
    assert_eq!((total.objects, total.versions, total.size), (3, 3, 31));
    assert_eq!(cache.cache.len(), 1, "child isolation must retain aggregate-only storage");
    let mut reloaded = save_reload(&FixtureStore::new(), &cache).await;
    assert_eq!(reloaded.checked_flatten("bucket").expect("persisted aggregate").objects, 3);
    drop(_partial);
    for broken in ["m-broken", "nested/broken"] {
        std::fs::remove_dir_all(root.join("bucket").join(broken)).expect("remove failed namespace fixture");
    }
    assert_eq!(
        reloaded.prepare_for_scan("bucket", 12, 7, SOURCE, PLAN, false),
        crate::DataUsageCachePrepareOutcome::Reset
    );
    reloaded.info.skip_healing = true;
    let (outcome, budget) = scan(&scanner.local_disk, reloaded, HealScanMode::Normal, 5).await;
    let ScannerDiskScanOutcome::Complete(verified) = outcome else {
        panic!("a clean retry must rebuild complete coverage")
    };
    assert!(verified.info.snapshot_complete);
    assert_eq!(
        budget.progress().0,
        4,
        "retry must revisit every object including the previously failed child"
    );
    let total = verified.checked_flatten("bucket").expect("verified complete aggregate");
    assert_eq!((total.objects, total.versions, total.size), (4, 4, 132));
}

#[tokio::test]
#[serial]
async fn wide_root_legacy_child_cancellation_preserves_partial_accounting() {
    let (scanner, _root, _cleanup, cache) = wide_legacy_root().await;
    let (outcome, budget) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 1).await;
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
    assert_eq!(budget.progress().0, 1);
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("cancellation inside a compacted namespace child must propagate")
    };
    let total = cache.checked_flatten("bucket").expect("partial compacted accounting");
    assert!(total.compacted);
    assert_eq!((total.objects, total.versions), (1, 1));
    assert!([7, 11].contains(&total.size), "completed child-local bytes must survive cancellation");
    let reloaded = save_reload(&FixtureStore::new(), &cache).await;
    assert_eq!(reloaded.checked_flatten("bucket").expect("persisted partial accounting").size, total.size);
}

#[tokio::test]
#[serial]
async fn wide_root_reused_children_cancellation_preserves_compacted_accounting() {
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    cache.replace("bucket", "", DataUsageEntry::default());
    let children = DATA_SCANNER_COMPACT_AT_CHILDREN + 1;
    for index in 0..children {
        let name = format!("bucket/clean-{index:05}");
        std::fs::create_dir_all(root.join(&name)).expect("create reusable root child");
        cache.replace(
            &name,
            "bucket",
            DataUsageEntry {
                compacted: true,
                objects: 1,
                versions: 2,
                size: 7,
                ..Default::default()
            },
        );
    }
    assert_eq!(cache.root().expect("wide baseline root").children.len(), children);
    *CANCEL_ROOT_FLUSH.lock().expect("install root flush cancellation") = Some(scanner.local_disk.path());
    let _cancel_guard = RootFlushCancellationGuard;
    let budget = ScannerCycleBudget::new(&CancellationToken::new(), Default::default());
    let mut options = super::scan_options(HealScanMode::Normal);
    options.prefix_scan_scope = ScannerBucketPrefixScanScope::from_dirty_top_level_entries(HashSet::from(["dirty".into()]));
    let outcome = scanner
        .local_disk
        .clone()
        .nsscanner_disk(budget.token(), budget.clone(), vec![scanner.local_disk.clone()], cache, None, options)
        .await
        .expect("cancelled root flush must return a partial cache");
    assert!(CANCEL_ROOT_FLUSH.lock().expect("flush hook observed").is_none());
    assert_eq!(budget.progress().0, 0, "clean children must be reused without metadata reads");
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("flush cancellation must propagate")
    };
    assert!(!cache.info.snapshot_complete);
    let total = cache
        .checked_flatten("bucket")
        .expect("cancellation must not leave dangling root links");
    assert!(total.compacted);
    assert!(total.children.is_empty());
    assert_eq!((total.objects, total.versions, total.size), (children, children * 2, children * 7));
    assert_eq!(cache.cache.len(), 1);
    let reloaded = save_reload(&FixtureStore::new(), &cache).await;
    assert_eq!(reloaded.checked_flatten("bucket").expect("persisted root accounting").objects, children);
}

#[tokio::test]
#[serial]
async fn wide_root_legacy_child_resource_deferral_preserves_partial_accounting() {
    use rustfs_concurrency::adaptive::{FeedbackController, FeedbackObservation, NodeAdmission, ResourcePressure};
    use std::num::NonZeroU16;

    let (mut scanner, _root, _cleanup, cache) = wide_legacy_root().await;
    scanner.old_cache = cache.clone();
    scanner.new_cache.info = cache.info;
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    let admission = NodeAdmission::new(NonZeroU16::MIN);
    admission.update(
        &mut FeedbackController::new(),
        Some(FeedbackObservation {
            generation: 1,
            max_concurrency: 1,
            pressure: ResourcePressure::Unknown,
            completed_units: None,
        }),
    );
    scanner.node_admission = Some(admission.clone());
    let (sender, mut receiver) = mpsc::channel(16);
    scanner.usage_updates = Some(ScannerUsageUpdates::new(sender, "bucket".into()));
    let deferred = Arc::new(AtomicBool::new(false));
    let observed = deferred.clone();
    scanner.update_current_path = Arc::new(move |path| {
        if path == "bucket/a-healthy/object" {
            // Defer the next directory after this object's metadata completed.
            observed.store(true, Ordering::SeqCst);
        } else if observed.load(Ordering::SeqCst) {
            admission.update(&mut FeedbackController::new(), None);
        }
        Box::pin(async {})
    });
    let mut total = DataUsageEntry::default();
    let result = scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".into(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut total,
        )
        .await;
    assert!(deferred.load(Ordering::SeqCst), "first metadata object must be reached");
    assert!(
        matches!(result, Err(ScannerError::ResourceDeferred)),
        "resource pressure must propagate: {result:?}"
    );
    assert!(total.compacted);
    assert_eq!((total.objects, total.versions, total.size), (1, 1, 7));
    assert!(scanner.usage_updates.is_some(), "resource deferral must restore the progress owner");
    let committed = receiver.try_recv().expect("completed sibling progress");
    assert_eq!((committed.objects, committed.versions, committed.size), (1, 1, 7));
}

#[tokio::test]
#[serial]
async fn wide_root_legacy_usage_updates_exclude_uncommitted_child_accounting() {
    temp_env::async_with_vars([(rustfs_config::ENV_SCANNER_YIELD_EVERY_N_OBJECTS, Some("1"))], async {
        let (mut scanner, root, _cleanup, cache) = wide_legacy_root().await;
        write_checkpoint_object(&root, "000-partial", &[(Some(Uuid::from_u128(105)), 101)]).await;
        let _partial = erasure_probe::OrderedDirectory::failing(&root.join("bucket/000-partial"), 1, ErrorKind::Other);
        scanner.old_cache = cache.clone();
        scanner.new_cache.info = cache.info;
        scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
        let (sender, mut receiver) = mpsc::channel(16);
        scanner.usage_updates = Some(ScannerUsageUpdates::new(sender, "bucket".into()));
        scanner
            .scan_folder(
                CancellationToken::new(),
                CachedFolder {
                    name: "bucket".into(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut DataUsageEntry::default(),
            )
            .await
            .expect("failed namespace must not abort healthy siblings");
        let committed = scanner
            .new_cache
            .checked_flatten("bucket")
            .expect("canonical committed aggregate");
        assert_eq!((committed.objects, committed.versions, committed.size), (2, 2, 18));
        let first = receiver.try_recv().expect("committed usage progress");
        assert_eq!(
            (first.objects, first.versions, first.size),
            (1, 1, 7),
            "provisional 101-byte child must never be published"
        );
        assert!(receiver.try_recv().is_err(), "minute interval still bounds updates");
        let updates = scanner
            .usage_updates
            .as_mut()
            .expect("requested usage owner must be restored");
        let total = updates.cache.checked_flatten("bucket").expect("connected committed progress");
        assert_eq!((total.objects, total.versions, total.size), (2, 2, 18));
        assert_eq!(
            updates.cache.cache.len(),
            1,
            "compacted progress must not retain provisional child entries"
        );
        updates.last_update = SystemTime::UNIX_EPOCH;
        scanner.send_update().await;
        let last = receiver.try_recv().expect("final committed aggregate");
        assert_eq!((last.objects, last.versions, last.size), (2, 2, 18));
    })
    .await;
}

#[tokio::test]
#[serial]
async fn legacy_namespace_gap_requires_fresh_verification_before_sampling() {
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for (object, version, size) in [("prefix/000-partial", 111, 101), ("prefix/healthy", 112, 7)] {
        write_checkpoint_object(&root, object, &[(Some(Uuid::from_u128(version)), size)]).await;
    }
    let partial = erasure_probe::OrderedDirectory::failing(
        &scanner.local_disk.path().join("bucket/prefix/000-partial"),
        1,
        ErrorKind::Other,
    );
    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    let (outcome, budget) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 3).await;
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("a narrow root must also refuse to certify skipped namespace coverage")
    };
    assert_eq!(budget.progress().0, 2, "failed child must reach provisional metadata accounting");
    assert!(!cache.info.snapshot_complete);
    assert!(cache.find(PREFIX).expect("incomplete aggregate").compacted);
    assert_eq!(
        cache.info.scan_plan_digest, None,
        "incomplete coverage must invalidate sampled baseline reuse"
    );
    let mut reloaded = save_reload(&FixtureStore::new(), &cache).await;
    drop(partial);
    assert_eq!(
        reloaded.prepare_for_scan("bucket", 12, 7, SOURCE, PLAN, false),
        crate::DataUsageCachePrepareOutcome::Reset,
        "a legacy retry must not sample an incomplete compacted subtree"
    );
    reloaded.info.skip_healing = true;
    let (outcome, budget) = scan(&scanner.local_disk, reloaded, HealScanMode::Normal, 3).await;
    let ScannerDiskScanOutcome::Complete(cache) = outcome else {
        panic!("clean retry must restore complete coverage")
    };
    assert!(cache.info.snapshot_complete);
    assert_eq!(budget.progress().0, 2, "both metadata objects must be rechecked");
    let total = cache.checked_flatten("bucket").expect("verified retry accounting");
    assert_eq!((total.objects, total.versions, total.size), (2, 2, 108));
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn wide_root_legacy_symlink_lookup_failure_isolated_to_child() {
    use std::os::unix::fs::symlink;

    assert!(!crate::scanner_is_erasure().await, "single-drive Namespace scan");
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_checkpoint_object(&root, "healthy/object", &[(None, 7)]).await;
    let bucket = root.join("bucket");
    let bad = bucket.join("bad");
    std::fs::create_dir(&bad).expect("create bad root child");
    let regular_file = root.join("regular-file");
    std::fs::write(&regular_file, b"not a directory").expect("create regular symlink ancestor");
    let broken_link = bad.join("broken-link");
    symlink(regular_file.join("child"), &broken_link).expect("create symlink through non-directory");
    assert_eq!(
        std::fs::metadata(broken_link).expect_err("target ancestor is a file").kind(),
        std::io::ErrorKind::NotADirectory
    );
    for idx in 0..DATA_SCANNER_COMPACT_AT_FOLDERS - 2 {
        std::fs::create_dir(bucket.join(format!("empty-{idx:05}"))).expect("create wide directory");
    }

    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    let (outcome, _) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 100).await;
    let ScannerDiskScanOutcome::Partial(wide) = outcome else {
        panic!("a failed child must preserve sibling progress without certifying complete coverage");
    };
    assert!(!wide.info.snapshot_complete);
    let summary = wide.checked_flatten("bucket").expect("aggregate healthy child");
    assert!(summary.compacted);
    assert_eq!((summary.objects, summary.size), (1, 7));
    assert_eq!(wide.cache.len(), 1);
}

#[tokio::test]
#[serial]
async fn legacy_failed_object_keeps_retry_history_after_partial_scan() {
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    write_test_object_metadata_bytes(&root, "bucket", "broken/object", &[]).await;

    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    let (outcome, _) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 100).await;
    let ScannerDiskScanOutcome::Partial(partial) = outcome else {
        panic!("a failed metadata read must keep the sweep partial");
    };
    assert_eq!(partial.info.failed_objects.len(), 1, "failed metadata should enter the retry cache");
    let mut reloaded = save_reload(&FixtureStore::new(), &partial).await;
    reloaded.prepare_for_scan("bucket", 12, 7, SOURCE, PLAN, false);
    assert_eq!(
        reloaded.info.failed_objects, partial.info.failed_objects,
        "same-plan retry must retain the timestamp that suppresses repeated repair attempts"
    );
}

#[cfg(unix)]
#[tokio::test]
#[serial]
async fn interrupted_legacy_gap_requires_fresh_verification_before_sampling() {
    use std::os::unix::fs::symlink;

    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for (object, size) in [("prefix/a-bad/object", 101), ("prefix/z-good", 7), ("z-tail/object", 11)] {
        write_checkpoint_object(&root, object, &[(None, size)]).await;
    }
    let regular_file = root.join("regular-file");
    std::fs::write(&regular_file, b"not a directory").expect("create regular symlink ancestor");
    let broken_link = root.join("bucket/prefix/a-bad/bad-link");
    symlink(regular_file.join("child"), &broken_link).expect("create symlink through non-directory");

    let mut cache = DataUsageCache::default();
    cache.prepare_for_scan("bucket", 11, 7, SOURCE, PLAN, false);
    cache.info.skip_healing = true;
    let (outcome, budget) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 2).await;
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
    let ScannerDiskScanOutcome::Partial(partial) = outcome else {
        panic!("the tail must interrupt the scan after the prefix was compacted");
    };
    let prefix = partial.find(PREFIX).expect("incomplete compacted prefix");
    assert!(prefix.compacted);
    assert_eq!((prefix.objects, prefix.size), (1, 7));

    let mut reloaded = save_reload(&FixtureStore::new(), &partial).await;
    std::fs::remove_file(broken_link).expect("restore namespace traversal");
    let next_cycle = (12_u32..28)
        .find(|cycle| !hash_path(PREFIX).mod_(*cycle, data_usage_update_dir_cycles()))
        .expect("choose a cycle that would otherwise sample the incomplete prefix");
    reloaded.prepare_for_scan("bucket", u64::from(next_cycle), 7, SOURCE, PLAN, false);
    reloaded.info.skip_healing = true;
    let (outcome, _) = scan(&scanner.local_disk, reloaded, HealScanMode::Normal, 4).await;
    let ScannerDiskScanOutcome::Complete(verified) = outcome else {
        panic!("retry must complete after the namespace is repaired");
    };
    let total = verified.checked_flatten("bucket").expect("verified retry accounting");
    assert_eq!((total.objects, total.size), (3, 119));
}
