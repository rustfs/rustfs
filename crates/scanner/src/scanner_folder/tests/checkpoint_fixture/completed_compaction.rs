// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

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
        .nsscanner_disk(budget.token(), budget.clone(), vec![disk.clone()], cache, None, scan_options(mode))
        .await
        .expect("scan checkpoint fixture through the real disk path");
    (outcome, budget)
}

async fn assert_completed_compaction_resumes(mode: HealScanMode) {
    let (scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for (prefix, size) in [("a", 2), ("b", 3), ("c", 5)] {
        for object in 0..4 {
            write_checkpoint_object(&root, &format!("nested/{prefix}/{object:04}"), &[(None, size)]).await;
        }
    }
    let identity = crate::DataUsageScanIdentity {
        scan_mode: mode,
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    cache.info.skip_healing = true;
    let (outcome, budget) = scan(&scanner.local_disk, cache, mode, 6).await;
    assert_eq!(budget.progress().0, 6);
    assert_eq!(budget.reason(), Some(ScannerCycleBudgetReason::Objects));
    let ScannerDiskScanOutcome::Partial(cache) = outcome else {
        panic!("the object budget must interrupt the second prefix")
    };
    let completed = cache.find("bucket/nested/a").expect("completed prefix remains addressable");
    assert!(completed.compacted, "a completed prefix must release leaves during the first sweep");
    assert!(completed.children.is_empty());
    assert_eq!((completed.objects, completed.size), (4, 8));
    assert!(
        cache.find("bucket/nested/a/0000").is_none(),
        "completed leaves must not remain in the cache"
    );
    for prefix in ["bucket", "bucket/nested", "bucket/nested/b"] {
        assert!(!cache.find(prefix).expect("unfinished ancestor retained").compacted);
    }
    assert!(
        cache.find("bucket/nested/b/0000").is_some(),
        "unfinished prefix retains its resume boundary"
    );
    let partial = cache
        .checked_flatten_complete_scope("bucket")
        .expect("partial graph has no orphan entries");
    assert_eq!((partial.objects, partial.versions, partial.size), (6, 0, 14));
    assert_eq!(cache.validated_scan_frontier(), Some("bucket/nested/b/0000"));

    let store = FixtureStore::new();
    let revisions = DataUsageCache::default()
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("initial fixture revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("persist compacted partial cache through the production codec and CAS path");
    let mut reloaded = store.strict_load().await;
    assert_eq!(reloaded.info.scan_coverage_receipt, cache.info.scan_coverage_receipt);
    assert_eq!(reloaded.validated_scan_frontier(), cache.validated_scan_frontier());
    assert_eq!(
        reloaded.prepare_bucket_checkpoint("bucket", 12, 8, SOURCE, PLAN, identity),
        crate::DataUsageCachePrepareOutcome::Reused,
        "a later cycle and leader must adopt compacted completed coverage"
    );
    let (outcome, budget) = scan(&scanner.local_disk, reloaded, mode, 13).await;
    assert_eq!(
        budget.progress().0,
        7,
        "only the interrupted boundary object and six unseen objects may be read on resume"
    );
    let ScannerDiskScanOutcome::Complete(completed) = outcome else {
        panic!("resumed sweep must complete")
    };
    let totals = completed
        .checked_flatten_complete_scope("bucket")
        .expect("completed cache has no orphan entries");
    assert_eq!((totals.objects, totals.versions, totals.size), (12, 0, 40));
    assert!(completed.info.snapshot_complete);
    assert!(completed.info.scan_progress.is_none());
    assert!(!completed.has_scan_position());
    assert!(completed.find("bucket/nested").expect("completed nested prefix").compacted);
    assert_eq!(completed.cache.len(), 2, "only the bucket root and completed prefix aggregate remain");
}

#[tokio::test]
#[serial]
async fn completed_compaction_normal_resumes_without_revisiting_completed_prefix() {
    assert_completed_compaction_resumes(HealScanMode::Normal).await;
}

#[tokio::test]
#[serial]
async fn completed_compaction_deep_resumes_without_revisiting_completed_prefix() {
    assert_completed_compaction_resumes(HealScanMode::Deep).await;
}

#[tokio::test]
#[serial]
async fn completed_compaction_preserves_frontier_inside_coverage_gap() {
    assert_compaction_frontier(true).await;
}

#[tokio::test]
#[serial]
async fn completed_compaction_promotes_frontier_without_parent_bookkeeping() {
    assert_compaction_frontier(false).await;
}

async fn assert_compaction_frontier(coverage_gap: bool) {
    let (mut scanner, root) = build_test_scanner().await;
    let _cleanup = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for object in 0..3 {
        write_checkpoint_object(&root, &format!("prefix/{object:04}"), &[(None, 2)]).await;
    }
    let identity = crate::DataUsageScanIdentity {
        tier_registry_generation: crate::runtime_tier_registry_for_cycle(11, 7).await.generation,
        ..bound_checkpoint().1
    };
    scanner
        .old_cache
        .prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    scanner.new_cache.info = scanner.old_cache.info.clone();
    scanner.update_cache.info = scanner.old_cache.info.clone();
    scanner.is_erasure_mode = true;
    scanner.skip_heal.store(true, Ordering::SeqCst);
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    // A failed child freezes the frontier. Without a gap, completing this
    // subtree must preserve coverage even when the caller is a heal rewalk
    // that does not advance the parent frontier after scan_folder returns.
    scanner.coverage_gap = coverage_gap;
    scanner.coverage_frontier = Some("bucket/prefix/0000".to_string());
    let mut entry = DataUsageEntry::default();
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket/prefix".to_string(),
                parent: Some(hash_path("bucket")),
                object_heal_prob_div: 1,
            },
            &mut entry,
        )
        .await
        .expect("walk the ancestor of a frozen coverage frontier");
    let expected_frontier = if coverage_gap { "bucket/prefix/0000" } else { "bucket/prefix" };
    assert_eq!(scanner.coverage_frontier.as_deref(), Some(expected_frontier));
    assert_eq!(
        scanner.new_cache.find("bucket/prefix").expect("completed ancestor").compacted,
        !coverage_gap
    );
    assert_eq!(scanner.new_cache.find("bucket/prefix/0000").is_some(), coverage_gap);
    scanner
        .new_cache
        .seal_scan_frontier(scanner.coverage_frontier.as_deref())
        .expect("seal the retained frontier");
    assert_eq!(scanner.new_cache.validated_scan_frontier(), Some(expected_frontier));
    let totals = scanner
        .new_cache
        .checked_flatten_complete_scope("bucket")
        .expect("coverage-gap cache remains connected");
    assert_eq!((totals.objects, totals.size), (3, 6));
}
