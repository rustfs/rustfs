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

//! Periodic snapshots captured while nested traversal frames are still active.

use super::*;
use tokio::sync::Notify;

const OBJECTS: u64 = 8;

fn prepare_scanner(scanner: &mut FolderScanner, cache: DataUsageCache) {
    scanner.resume_frontier = cache.validated_scan_frontier().map(str::to_owned);
    scanner.coverage_frontier = scanner.resume_frontier.clone();
    scanner.new_cache = DataUsageCache {
        info: cache.info.clone(),
        ..Default::default()
    };
    scanner.update_cache = DataUsageCache {
        info: cache.info.clone(),
        ..Default::default()
    };
    scanner.old_cache = cache;
    scanner.is_erasure_mode = true;
    scanner.skip_heal.store(true, Ordering::SeqCst);
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
}

async fn nested_periodic_snapshot(
    corrupt_first: bool,
    root_scalars: (usize, usize),
) -> (DataUsageCache, crate::DataUsageScanIdentity, FolderScanner, TestGuard) {
    let (mut scanner, root) = build_test_scanner().await;
    let guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for branch in ["a", "b"] {
        for object in 0..4 {
            write_checkpoint_object(&root, &format!("prefix/{branch}/{object:04}"), &[(None, 1)]).await;
        }
    }
    if corrupt_first {
        write_test_object_metadata_bytes(&root, "bucket", "prefix/a/0000", b"not-valid-filemeta").await;
    }
    let (_, identity) = bound_checkpoint();
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    prepare_scanner(&mut scanner, cache);
    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel(1);
    scanner.checkpoint_tx = Some(checkpoint_tx);
    scanner.checkpoint_objects = SCANNER_CHECKPOINT_OBJECT_INTERVAL - 2;
    scanner.last_checkpoint_at = Instant::now()
        .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
        .expect("seed elapsed checkpoint interval");
    let paused = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let pause_path = if corrupt_first {
        "bucket/prefix/a/0003"
    } else {
        "bucket/prefix/a/0002"
    };
    scanner.update_current_path = Arc::new({
        let paused = paused.clone();
        let release = release.clone();
        move |path: &str| {
            let pause_here = path == pause_path;
            let paused = paused.clone();
            let release = release.clone();
            Box::pin(async move {
                if pause_here {
                    paused.notify_one();
                    release.notified().await;
                }
            })
        }
    });
    let parent = CancellationToken::new();
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&parent, Default::default());
    let budget = scanner.budget.clone();
    let mut entry = DataUsageEntry {
        objects: root_scalars.0,
        size: root_scalars.1,
        ..Default::default()
    };
    let snapshot = {
        let scan = scanner.scan_folder(
            parent.clone(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut entry,
        );
        tokio::pin!(scan);
        let snapshot = tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                snapshot = checkpoint_rx.recv() => snapshot.expect("real walk emits periodic snapshot"),
                result = &mut scan => panic!("walk finished before periodic snapshot: {result:?}"),
            }
        })
        .await
        .expect("bounded nested checkpoint emission");
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                () = paused.notified() => {},
                result = &mut scan => panic!("nested ancestors unwound before capture: {result:?}"),
            }
        })
        .await
        .expect("walk pauses before third healthy object");
        assert_eq!(budget.progress().0, 2, "capture must precede ancestor unwind and further object reads");
        assert!(!parent.is_cancelled(), "periodic capture must not use cancellation linking");
        snapshot
    };
    assert!(!snapshot.info.snapshot_complete);
    assert!(
        snapshot.validated_raw_enumeration_cursor().is_some() || snapshot.validated_raw_enumeration_page_index().is_some(),
        "enumeration progress must coexist with completed subtree coverage"
    );
    let totals = snapshot
        .checked_flatten_complete_scope("bucket")
        .expect("periodic snapshot must connect every active ancestor to the bucket root");
    assert_eq!(
        totals.objects,
        2 + root_scalars.0,
        "snapshot must retain suspended ancestor and current frame object accounting"
    );
    assert_eq!(
        totals.size,
        2 + root_scalars.1,
        "suspended ancestor and current frame scalar bytes must survive snapshot construction"
    );
    let store = FixtureStore::new();
    let revisions = DataUsageCache::default()
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("initial periodic fixture revisions");
    snapshot
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("persist real mid-walk periodic snapshot");
    let mut loaded = DataUsageCache::default();
    loaded
        .load(store.clone(), CACHE_NAME)
        .await
        .expect("reload real periodic snapshot");
    assert_eq!(loaded.info.scan_coverage_receipt, snapshot.info.scan_coverage_receipt);
    assert_eq!(loaded.info.scan_raw_enumeration_cursor, snapshot.info.scan_raw_enumeration_cursor);
    assert_eq!(
        loaded
            .checked_flatten_complete_scope("bucket")
            .expect("reloaded complete scope")
            .objects,
        2 + root_scalars.0
    );
    assert_eq!(
        loaded.prepare_bucket_checkpoint("bucket", 11, 8, SOURCE, PLAN, identity),
        crate::DataUsageCachePrepareOutcome::Reused,
        "a higher leader must adopt the persisted periodic snapshot"
    );
    (loaded, identity, scanner, guard)
}

async fn finish_resumed_walk(cache: DataUsageCache, original: &FolderScanner) -> (DataUsageCache, u64, TestGuard) {
    let (mut scanner, unused_root) = build_test_scanner().await;
    let guard = TestGuard {
        temp_dir: Some(unused_root),
    };
    scanner.root = original.root.clone();
    scanner.local_disk = original.local_disk.clone();
    prepare_scanner(&mut scanner, cache);
    let parent = CancellationToken::new();
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&parent, Default::default());
    let mut entry = DataUsageEntry::default();
    scanner
        .scan_folder(
            parent,
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut entry,
        )
        .await
        .expect("finish resumed nested periodic walk");
    let visits = scanner.budget.progress().0;
    (scanner.new_cache, visits, guard)
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_resumes_after_leader_handoff_with_exact_totals() {
    let (loaded, _, original, _guard) = nested_periodic_snapshot(false, (0, 0)).await;
    assert_eq!(loaded.validated_scan_frontier(), Some("bucket/prefix/a/0000"));
    let (finished, visits, _unused_guard) = finish_resumed_walk(loaded, &original).await;
    assert_eq!(visits, OBJECTS - 1, "the completed nested sibling must skip its metadata read");
    let totals = finished
        .checked_flatten_complete_scope("bucket")
        .expect("finished connected root");
    assert_eq!(u64::try_from(totals.objects).expect("object count"), OBJECTS);
    assert_eq!(u64::try_from(totals.size).expect("byte count"), OBJECTS);
    assert_eq!(totals.failed_objects, 0);
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_keeps_failed_prefix_outside_resume_frontier() {
    let (loaded, _, original, _guard) = nested_periodic_snapshot(true, (0, 0)).await;
    assert!(
        loaded.validated_scan_frontier().is_none(),
        "an earlier corrupt object must block subtree skipping"
    );
    assert!(!loaded.info.failed_objects.is_empty(), "persisted failure debt must survive handoff");
    let (finished, visits, _unused_guard) = finish_resumed_walk(loaded, &original).await;
    assert_eq!(visits, OBJECTS - 1, "every healthy object must be re-read after the coverage gap");
    let totals = finished
        .checked_flatten_complete_scope("bucket")
        .expect("failed sweep still has connected accounting");
    assert_eq!(u64::try_from(totals.objects).expect("healthy count"), OBJECTS - 1);
    assert_eq!(u64::try_from(totals.size).expect("healthy bytes"), OBJECTS - 1);
    assert!(!finished.info.failed_objects.is_empty(), "unrepaired metadata must remain pending");
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_preserves_suspended_ancestor_scalars_through_reload() {
    let (loaded, _, _, _guard) = nested_periodic_snapshot(false, (3, 21)).await;
    let root = loaded.root().expect("reloaded root accumulator");
    assert_eq!(root.objects, 3, "suspended root object accounting must not become an empty topology node");
    assert_eq!(root.size, 21, "suspended root bytes must remain independent of descendant totals");
    let totals = loaded
        .checked_flatten_complete_scope("bucket")
        .expect("reloaded ancestor accounting");
    assert_eq!(totals.objects, 5);
    assert_eq!(totals.size, 23);
    assert_eq!(loaded.validated_scan_frontier(), Some("bucket/prefix/a/0000"));
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_keeps_durable_frontier_across_early_repeated_handoffs() {
    let (cache, identity, original, _guard) = nested_periodic_snapshot(false, (0, 0)).await;
    let store = FixtureStore::new();
    let revisions = DataUsageCache::default()
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("initial repeated-handoff revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("save the inherited valid subtree frontier");
    let (mut scanner, unused_root) = build_test_scanner().await;
    let _unused_guard = TestGuard {
        temp_dir: Some(unused_root),
    };
    scanner.root = original.root.clone();
    scanner.local_disk = original.local_disk.clone();
    prepare_scanner(&mut scanner, cache);
    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel(1);
    scanner.checkpoint_tx = Some(checkpoint_tx);
    scanner.checkpoint_objects = SCANNER_CHECKPOINT_OBJECT_INTERVAL;
    scanner.last_checkpoint_at = Instant::now()
        .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
        .expect("early resumed emitter cadence");
    let folder = CachedFolder {
        name: "bucket".to_string(),
        parent: None,
        object_heal_prob_div: 1,
    };
    scanner.record_raw_enumeration_entry("bucket", "prefix");
    scanner.maybe_send_checkpoint(&folder, &hash_path("bucket"), &DataUsageEntry::default());
    assert!(
        matches!(checkpoint_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "an early raw-only snapshot must not overwrite inherited completed coverage"
    );
    assert_eq!(scanner.last_checkpoint_objects, SCANNER_CHECKPOINT_OBJECT_INTERVAL);
    let mut inherited = DataUsageCache::default();
    let revisions = inherited
        .load_with_revisions(store.clone(), CACHE_NAME)
        .await
        .expect("reload unchanged durable frontier after deferred early emission");
    assert_eq!(inherited.validated_scan_frontier(), Some("bucket/prefix/a/0000"));
    assert_eq!(
        inherited.prepare_bucket_checkpoint("bucket", 11, 9, SOURCE, PLAN, identity),
        crate::DataUsageCachePrepareOutcome::Reused
    );
    assert_eq!(inherited.validated_scan_frontier(), Some("bucket/prefix/a/0000"));
    prepare_scanner(&mut scanner, inherited);
    scanner.checkpoint_objects = SCANNER_CHECKPOINT_OBJECT_INTERVAL - 2;
    scanner.last_checkpoint_objects = 0;
    scanner.last_checkpoint_at = Instant::now()
        .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
        .expect("later useful resumed emitter cadence");
    let parent = CancellationToken::new();
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(&parent, Default::default());
    let mut entry = DataUsageEntry::default();
    let candidate = {
        let scan = scanner.scan_folder(parent, folder, &mut entry);
        tokio::pin!(scan);
        tokio::time::timeout(Duration::from_secs(10), async {
            tokio::select! {
                checkpoint = checkpoint_rx.recv() => checkpoint.expect("resumed walk can later emit useful coverage"),
                result = &mut scan => {
                    result.expect("later resumed walk completes");
                    checkpoint_rx.try_recv().expect("completed walk emitted a useful periodic checkpoint")
                }
            }
        })
        .await
        .expect("bounded later useful emission")
    };
    assert_eq!(candidate.validated_scan_frontier(), Some("bucket/prefix/a/0001"));
    assert_eq!(
        candidate
            .checked_flatten_complete_scope("bucket")
            .expect("later snapshot connected accounting")
            .objects,
        3,
        "inherited completed object and both newly read objects must survive"
    );
    candidate
        .save_with_revisions_for_epoch(store.clone(), CACHE_NAME, &revisions, 0)
        .await
        .expect("persist later coverage without losing the inherited prefix");
    let mut latest = DataUsageCache::default();
    latest
        .load(store, CACHE_NAME)
        .await
        .expect("reload later periodic checkpoint");
    assert_eq!(
        latest.prepare_bucket_checkpoint("bucket", 11, 10, SOURCE, PLAN, identity),
        crate::DataUsageCachePrepareOutcome::Reused
    );
    assert_eq!(latest.validated_scan_frontier(), Some("bucket/prefix/a/0001"));
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_defers_unfinished_paths_at_or_before_completed_frontier() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard { temp_dir: Some(root) };
    let (_, identity) = bound_checkpoint();
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    cache.replace("bucket", "", DataUsageEntry::default());
    cache.replace(
        "bucket/z",
        "bucket",
        DataUsageEntry {
            objects: 3,
            size: 9,
            ..Default::default()
        },
    );
    cache
        .seal_scan_frontier(Some("bucket/z"))
        .expect("completed later sibling coverage");
    prepare_scanner(&mut scanner, cache.clone());
    scanner.new_cache = cache;
    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel(1);
    scanner.checkpoint_tx = Some(checkpoint_tx);
    let partial = DataUsageEntry {
        objects: 2,
        size: 5,
        ..Default::default()
    };
    for active in ["bucket/a", "bucket/z"] {
        scanner.record_raw_enumeration_entry(active, STORAGE_FORMAT_FILE);
        scanner.checkpoint_objects += SCANNER_CHECKPOINT_OBJECT_INTERVAL;
        scanner.last_checkpoint_at = Instant::now()
            .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
            .expect("unordered active path cadence");
        scanner.maybe_send_checkpoint(
            &CachedFolder {
                name: active.to_string(),
                parent: Some(hash_path("bucket")),
                object_heal_prob_div: 1,
            },
            &hash_path(active),
            &partial,
        );
        assert!(
            matches!(checkpoint_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
            "unfinished path {active} must not become certified completed coverage"
        );
        assert_eq!(
            scanner.last_checkpoint_objects, scanner.checkpoint_objects,
            "deferred paths still consume cadence"
        );
    }
    scanner.new_cache.replace("bucket/a", "bucket", partial);
    let root_entry = scanner.new_cache.root().expect("completed subtree owner");
    scanner.checkpoint_objects += SCANNER_CHECKPOINT_OBJECT_INTERVAL;
    scanner.last_checkpoint_at = Instant::now()
        .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
        .expect("completed owner cadence");
    scanner.maybe_send_checkpoint(
        &CachedFolder {
            name: "bucket".to_string(),
            parent: None,
            object_heal_prob_div: 1,
        },
        &hash_path("bucket"),
        &root_entry,
    );
    let checkpoint = checkpoint_rx.try_recv().expect("completed subtree owner may safely emit");
    assert_eq!(checkpoint.validated_scan_frontier(), Some("bucket/z"));
    let totals = checkpoint
        .checked_flatten_complete_scope("bucket")
        .expect("both completed sibling links remain reachable");
    assert_eq!(totals.objects, 5);
    assert_eq!(totals.size, 14);
    assert_eq!(checkpoint.cache.len(), 3);
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_partial_preservation_invalidates_covered_child_but_keeps_inner_frontier() {
    for (child, frontier, frontier_inside_child, expected_objects) in [
        ("bucket/a", "bucket/z", false, 5),
        ("bucket/z", "bucket/z", false, 2),
        ("bucket/parent", "bucket/parent/done", true, 5),
    ] {
        let (mut scanner, root) = build_test_scanner().await;
        let _guard = TestGuard { temp_dir: Some(root) };
        let (_, identity) = bound_checkpoint();
        let mut cache = DataUsageCache::default();
        cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
        cache.replace("bucket", "", DataUsageEntry::default());
        if frontier_inside_child {
            cache.replace(child, "bucket", DataUsageEntry::default());
        }
        cache.replace(
            frontier,
            if frontier_inside_child { child } else { "bucket" },
            DataUsageEntry {
                objects: 3,
                size: 9,
                ..Default::default()
            },
        );
        cache
            .seal_scan_frontier(Some(frontier))
            .expect("initial completed child coverage");
        prepare_scanner(&mut scanner, cache.clone());
        scanner.new_cache = cache;
        let mut parent_entry = scanner.new_cache.root().expect("partial child owner");
        let child_entry = DataUsageEntry {
            objects: 2,
            size: 5,
            ..Default::default()
        };
        scanner
            .preserve_partial_child_progress(&Some(hash_path("bucket")), &hash_path(child), &mut parent_entry, &child_entry)
            .await;
        let retained_frontier = scanner.coverage_frontier.clone();
        scanner.new_cache.replace_hashed(&hash_path("bucket"), &None, &parent_entry);
        scanner
            .new_cache
            .seal_scan_frontier(retained_frontier.as_deref())
            .expect("seal the final partial cache after child preservation");
        let totals = scanner
            .new_cache
            .checked_flatten_complete_scope("bucket")
            .expect("preserved partial child and completed siblings remain connected");
        assert_eq!(totals.objects, expected_objects, "partial child {child} must be counted once");
        assert_eq!(totals.size, if expected_objects == 2 { 5 } else { 14 });
        if frontier_inside_child {
            assert!(!scanner.coverage_gap);
            assert_eq!(scanner.new_cache.validated_scan_frontier(), Some(frontier));
        } else {
            assert!(
                scanner.coverage_gap,
                "unfinished covered child {child} must block further frontier advancement"
            );
            assert!(retained_frontier.is_none());
            assert!(scanner.new_cache.info.scan_coverage_receipt.is_none());
            assert!(scanner.new_cache.validated_scan_frontier().is_none());
        }
    }
}

async fn assert_nested_frame_restoration(max_objects: Option<u64>) {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for branch in ["a", "b"] {
        for object in 0..4 {
            write_checkpoint_object(&root, &format!("prefix/{branch}/{object:04}"), &[(None, 1)]).await;
        }
    }
    let (_, identity) = bound_checkpoint();
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    prepare_scanner(&mut scanner, cache);
    let (checkpoint_tx, _checkpoint_rx) = mpsc::channel(1);
    scanner.checkpoint_tx = Some(checkpoint_tx);
    let parent = CancellationToken::new();
    scanner.budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_objects,
            ..Default::default()
        },
    );
    let mut entry = DataUsageEntry {
        objects: usize::from(max_objects.is_some()) * 3,
        size: usize::from(max_objects.is_some()) * 21,
        ..Default::default()
    };
    let result = scanner
        .scan_folder(
            scanner.budget.token(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut entry,
        )
        .await;
    match max_objects {
        Some(_) => {
            assert!(result.is_err(), "object budget must interrupt the actual nested walk");
            assert_eq!(scanner.budget.reason(), Some(crate::scanner_budget::ScannerCycleBudgetReason::Objects));
            assert!(
                scanner.budget.token().is_cancelled(),
                "partial preservation must follow actual cancellation"
            );
            assert_eq!(
                entry.objects, 3,
                "suspended root object accounting must be restored before returning the error"
            );
            assert_eq!(
                entry.size, 21,
                "suspended root byte accounting must be restored before cancellation linking"
            );
        }
        None => result.expect("complete real nested walk with periodic sender enabled"),
    }
    let totals = scanner
        .new_cache
        .checked_flatten_complete_scope("bucket")
        .expect("complete nested accounting");
    let expected = max_objects.unwrap_or(OBJECTS);
    assert_eq!(u64::try_from(totals.objects).expect("object count"), expected);
    assert_eq!(u64::try_from(totals.size).expect("byte count"), expected);
    assert_eq!(scanner.checkpoint_depth, 0, "every completed child must restore its parent frame");
    assert_eq!(
        scanner.checkpoint_ancestors.len(),
        3,
        "sibling count must not increase depth-bounded spare storage"
    );
    for spare in &scanner.checkpoint_ancestors {
        assert!(spare.hash.0.is_empty(), "reusable frame must release its suspended path");
        assert!(
            !data_usage_root_has_progress(&spare.entry),
            "completed frame must not retain ancestor accounting"
        );
        assert!(!spare.entry.compacted);
        assert!(spare.entry.obj_sizes.is_empty());
        assert!(spare.entry.obj_versions.is_empty());
        assert!(spare.entry.tier_accounting_proof.is_none());
    }
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_reuses_empty_ancestor_frames_after_complete_walk() {
    assert_nested_frame_restoration(None).await;
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_restores_ancestor_frames_before_cancellation_linking() {
    assert_nested_frame_restoration(Some(2)).await;
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_adoption_records_checkpoint_used_on_real_scan_startup() {
    let (scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    tokio::fs::create_dir_all(root.join("bucket/static"))
        .await
        .expect("create the real cached subtree directory");
    let (mut cache, _) = bound_checkpoint();
    cache.info.skip_healing = true;
    let parent = CancellationToken::new();
    let budget = ScannerCycleBudget::new_with_progress_tracking(
        &parent,
        ScannerCycleBudgetConfig {
            max_directories: Some(1),
            ..Default::default()
        },
    );
    let before = global_metrics().report().await.scan_checkpoint_used;
    let completed = scan_data_folder(
        budget.token(),
        budget.clone(),
        vec![scanner.local_disk.clone()],
        scanner.local_disk,
        cache,
        None,
        HealScanMode::Normal,
        DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest),
    )
    .await
    .expect("validated frontier must skip the child within a root-only directory budget");
    let after = global_metrics().report().await.scan_checkpoint_used;
    assert_eq!(after, before + 1, "adopting a verified forward-sweep frontier must be observable");
    assert!(completed.info.snapshot_complete);
    assert_eq!(
        completed
            .checked_flatten_complete_scope("bucket")
            .expect("completed adopted scope")
            .objects,
        3
    );
    assert_eq!(budget.progress().1, 1, "adoption must avoid entering the completed subtree");
    assert_eq!(budget.reason(), None);
}

#[tokio::test]
#[serial]
async fn periodic_nested_snapshot_does_not_publish_shared_compacted_accumulator_under_child_path() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    for object in 0..4 {
        write_checkpoint_object(&root, &format!("prefix/nested/{object:04}"), &[(None, 1)]).await;
    }
    let (_, identity) = bound_checkpoint();
    let mut cache = DataUsageCache::default();
    cache.prepare_bucket_checkpoint("bucket", 11, 7, SOURCE, PLAN, identity);
    prepare_scanner(&mut scanner, cache);
    let (checkpoint_tx, mut checkpoint_rx) = mpsc::channel(1);
    scanner.checkpoint_tx = Some(checkpoint_tx);
    scanner.checkpoint_objects = SCANNER_CHECKPOINT_OBJECT_INTERVAL - 2;
    scanner.last_checkpoint_at = Instant::now()
        .checked_sub(SCANNER_CHECKPOINT_MIN_INTERVAL)
        .expect("seed compacted checkpoint interval");
    let mut entry = DataUsageEntry {
        compacted: true,
        ..Default::default()
    };
    scanner
        .scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: "bucket".to_string(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut entry,
        )
        .await
        .expect("walk with shared compacted owner accumulator");
    assert_eq!(entry.objects, 4);
    assert_eq!(entry.size, 4);
    assert!(
        entry.children.is_empty(),
        "compacted totals must not acquire independently accounted child links"
    );
    assert!(
        matches!(checkpoint_rx.try_recv(), Err(mpsc::error::TryRecvError::Empty)),
        "a shared compacted parent accumulator must not be snapshotted as the current child"
    );
}
