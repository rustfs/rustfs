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
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
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
        // The first real scan builds coverage; the second same-plan scan takes
        // the normal compaction path. No synthetic compacted cache is injected.
        for cycle in [11, 12] {
            cache.prepare_bucket_checkpoint("bucket", cycle, 7, SOURCE, PLAN, identity);
            cache.info.skip_healing = true;
            let (outcome, budget) = scan(&scanner.local_disk, cache, mode, OBJECTS + 1).await;
            let ScannerDiskScanOutcome::Complete(completed) = outcome else {
                panic!("fixture baseline must complete")
            };
            assert_eq!(budget.progress().0, OBJECTS, "baseline must read every metadata object");
            cache = completed;
        }
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
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for index in 0..DATA_SCANNER_COMPACT_AT_FOLDERS {
        write_checkpoint_object(&root, &format!("prefix/{index:04}"), &[(None, 1)]).await;
    }
    write_checkpoint_object(&root, "z-last", &[(None, 1)]).await;
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    let store = FixtureStore::new();
    let mut cache = DataUsageCache::default();
    let mut previous = 0;
    for round in 0..8 {
        cache.prepare_bucket_checkpoint("bucket", 11 + round, 7 + round, SOURCE, PLAN, identity);
        cache.info.skip_healing = true;
        let (outcome, budget) = scan(&scanner.local_disk, cache, HealScanMode::Normal, 1000).await;
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
            return;
        }
        assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
        assert!(!cache.info.snapshot_complete);
        previous = total.objects;
    }
    panic!("large prefix must finish within repeated bounded scans");
}
