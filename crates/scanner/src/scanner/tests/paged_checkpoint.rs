// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::BUCKET_META_PREFIX;

const NAME: &str = "bucket/.usage-cache.bin";

fn seeded() -> DataUsageCache {
    let mut cache = DataUsageCache::default();
    cache.info.name = "bucket".into();
    cache.info.source = Some(DataUsageCacheSource::new(0, 0));
    cache.info.scan_identity = Some(crate::data_usage_define::DataUsageScanIdentity {
        version: crate::data_usage_define::DATA_USAGE_SCAN_CHECKPOINT_VERSION,
        bucket_incarnation: uuid::Uuid::from_u128(42),
        set_layout: DataUsageScanPlanDigest([7; 32]),
        publication_epoch: 0,
        tier_registry_generation: 1,
        scan_mode: HealScanMode::Normal,
    });
    cache.replace("bucket", "", DataUsageEntry::default());
    for index in 0..4096 {
        cache.replace(
            &format!("bucket/prefix-{index:05}"),
            "bucket",
            DataUsageEntry {
                size: index + 1,
                objects: 1,
                versions: 1,
                ..Default::default()
            },
        );
    }
    cache
}

async fn layout(store: &MemoryConfigStore) -> HashMap<String, (usize, usize)> {
    let puts = store.put_counts.lock().await.clone();
    store
        .objects
        .lock()
        .await
        .iter()
        .map(|(k, v)| (k.clone(), (puts[k], v.len())))
        .collect()
}

#[tokio::test]
async fn paged_checkpoint_writes_only_changed_pages_and_commits_root_last() {
    let store = Arc::new(MemoryConfigStore::default());
    let cache = seeded();
    let first = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("empty revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &first, 0)
        .await
        .expect("initial checkpoint");
    let before = layout(&store).await;
    let encoded = cache.marshal_msg().expect("encode baseline").len();
    let main = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}"));
    assert!(before[&main].1 < 8192, "only a compact manifest belongs in the root");
    assert!(before.keys().any(|key| key.contains(".pages/")), "large cache must be paged");

    let mut restarted = DataUsageCache::default();
    let revisions = restarted
        .load_with_revisions(store.clone(), NAME)
        .await
        .expect("restart from durable pages");
    let prior = restarted.checked_flatten("bucket").expect("all old entries");
    restarted.replace(
        "bucket/prefix-tail",
        "bucket",
        DataUsageEntry {
            objects: 1,
            versions: 1,
            size: 4711,
            ..Default::default()
        },
    );
    store.fail_puts_from.lock().await.insert(main.clone(), before[&main].0 + 1);
    restarted
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .expect_err("commit interrupted after writing pages");
    let interrupted = layout(&store).await;
    let mut observer = DataUsageCache::default();
    observer.load(store.clone(), NAME).await.expect("last root still readable");
    assert_eq!(observer.checked_flatten("bucket").expect("previous coverage").size, prior.size);
    store.fail_puts_from.lock().await.remove(&main);
    restarted
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .expect("retry interrupted commit");
    observer.load(store.clone(), NAME).await.expect("load committed result");
    let actual = observer.checked_flatten("bucket").expect("new root");
    assert_eq!((actual.objects, actual.size), (4097, prior.size + 4711));
    let after = layout(&store).await;
    for (key, (writes, _)) in &interrupted {
        if key.contains(".bin.pages/") {
            assert_eq!(*writes, after[key].0, "retry restaged a previously successful immutable page");
        }
    }
    let new_bytes: usize = after
        .iter()
        .filter(|(key, _)| !before.contains_key(*key))
        .map(|(_, (_, size))| *size)
        .sum();
    assert!(
        new_bytes < encoded / 3,
        "one append staged {new_bytes} new bytes for a {encoded}-byte cache"
    );
    for (key, (writes, _)) in &before {
        if key.contains(".pages/") {
            assert_eq!(*writes, after[key].0, "a committed page was resubmitted");
        }
    }
    restarted.info.next_cycle += 1;
    let stale = restarted.save_with_revisions_for_epoch(store.clone(), NAME, &first, 0).await;
    assert!(
        matches!(stale, Err(EcstoreError::PreconditionFailed)),
        "stale writer cannot replace the new root"
    );
}

#[tokio::test]
async fn paged_checkpoint_cold_restore_spans_fixed_duration_and_repairs_corrupt_primary() {
    let store = Arc::new(MemoryConfigStore::default());
    let cache = seeded();
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("read revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .expect("write both copies");
    store.checkpoint_page_delay_ms.store(16, Ordering::Relaxed);
    let mut loaded = DataUsageCache::default();
    let mut completed_at = None;
    for round in 1..=128 {
        if let Ok(result) = tokio::time::timeout(Duration::from_millis(26), loaded.load(store.clone(), NAME)).await {
            result.expect("resume exact committed manifest");
            completed_at = Some(round);
            break;
        }
    }
    assert!(
        completed_at.is_some_and(|round| round > 1),
        "cold recovery must advance across fixed deadlines"
    );
    store.checkpoint_page_delay_ms.store(0, Ordering::Relaxed);
    let prior = loaded.checked_flatten("bucket").expect("cold cache totals");
    assert_eq!((prior.objects, prior.size), (4096, 4096 * 4097 / 2));

    {
        let mut objects = store.objects.lock().await;
        let (_, block) = objects
            .iter_mut()
            .find(|(key, block)| key.contains(".bin.pages/") && block.len() > 8192)
            .expect("primary data page");
        block[0] ^= 0xff;
    }
    let revisions = loaded
        .load_with_revisions(store.clone(), NAME)
        .await
        .expect("read unaffected backup");
    assert_eq!(
        revisions,
        DataUsageCache::read_revisions(store.clone(), NAME)
            .await
            .expect("re-enter admission on same heads")
    );
    loaded
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .expect("replace damaged primary generation");
    store.objects.lock().await.retain(|key, _| !key.contains(".bkp"));
    let mut primary = DataUsageCache::default();
    primary.load(store, NAME).await.expect("repaired independent primary");
    assert_eq!(primary.checked_flatten("bucket").expect("primary totals").size, prior.size);
}

#[tokio::test(start_paused = true)]
async fn paged_checkpoint_bucket_admission_uses_the_scan_budget_during_cold_load() {
    let store = Arc::new(MemoryConfigStore::default());
    let cache = seeded();
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME).await.unwrap();
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .unwrap();
    store.checkpoint_page_delay_ms.store(16, Ordering::Relaxed);

    let owner = CancellationToken::new();
    let mut loaded = DataUsageCache::default();
    for round in 0..128 {
        let budget = ScannerCycleBudget::new_with_progress_tracking(
            &owner,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_millis(45)),
                ..Default::default()
            },
        );
        // Exercise the same admission used by local and remote workers. The
        // worker may drain after the deadline; only the scan token interrupts
        // recovery here, as it does in the daemon.
        let revisions = loaded
            .load_for_scan(store.clone(), NAME, &budget.token(), &budget)
            .await
            .unwrap();
        if revisions.is_some() {
            assert!(round > 0, "the first slice cannot finish this physical restore");
            assert!(!budget.token().is_cancelled(), "return work only to an active scan");
            let totals = loaded.checked_flatten("bucket").expect("restored full bucket");
            assert_eq!((totals.objects, totals.size), (4096, 4096 * 4097 / 2));
            return;
        }
    }
    panic!("bucket admission repeatedly consumed the entire budget restoring the same root");
}

#[tokio::test]
async fn paged_checkpoint_retirement_preserves_independent_backup_and_legacy_projection() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = seeded();
    cache.info.snapshot_complete = true;
    let first = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("initial revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &first, 0)
        .await
        .expect("persist first generation");
    let original = layout(&store).await;
    let main = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}"));
    let backup = format!("{main}.bkp");
    let legacy =
        DataUsageCache::unmarshal(&store.objects.lock().await[&main]).expect("old reader can read compact complete root");
    let totals = legacy
        .checked_flatten_complete_scope("bucket")
        .expect("legacy compact totals");
    assert_eq!((totals.objects, totals.size), (4096, 4096 * 4097 / 2));
    let initial = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("first generation revisions");
    cache.info.next_cycle += 1;
    store
        .fail_puts_from
        .lock()
        .await
        .insert(backup.clone(), original[&backup].0 + 1);
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &initial, 0)
        .await
        .expect("commit primary despite failed backup");
    let updated = layout(&store).await;
    for key in original.keys().filter(|key| key.contains(".pages/")) {
        assert_eq!(
            updated.contains_key(key),
            key.contains(".bkp.pages/"),
            "unexpected reclamation scope: {key}"
        );
    }
    let mut latest = DataUsageCache::default();
    latest.load(store.clone(), NAME).await.expect("new main remains readable");
    assert_eq!(latest.info.next_cycle, cache.info.next_cycle);
    store.objects.lock().await.remove(&main);
    latest
        .load(store, NAME)
        .await
        .expect("previous independent backup remains readable");
    assert_eq!(latest.info.next_cycle + 1, cache.info.next_cycle);
}

#[tokio::test(start_paused = true)]
async fn paged_checkpoint_scan_handoff_checks_the_latest_root_before_using_its_totals() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = seeded();
    cache.cache.clear();
    let replace = |cache: &mut DataUsageCache, size| {
        cache.replace(
            "bucket",
            "",
            DataUsageEntry {
                objects: 1,
                size,
                ..Default::default()
            },
        );
    };
    replace(&mut cache, 17);
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME).await.unwrap();
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .unwrap();
    let main = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}"));
    store.delayed_gets.lock().await.insert(main, Duration::from_secs(6));

    let parent = CancellationToken::new();
    let budget = || {
        ScannerCycleBudget::new_with_progress_tracking(
            &parent,
            ScannerCycleBudgetConfig {
                max_duration: Some(Duration::from_secs(10)),
                ..Default::default()
            },
        )
    };
    let first = budget();
    let mut loaded = DataUsageCache::default();
    assert!(
        loaded
            .load_for_scan(store.clone(), NAME, &first.token(), &first)
            .await
            .unwrap()
            .is_none()
    );
    assert!(!first.token().is_cancelled(), "the slow load moved its result to the next admission");

    let revisions = DataUsageCache::read_revisions(store.clone(), NAME).await.unwrap();
    replace(&mut cache, 99);
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0)
        .await
        .unwrap();
    let next = budget();
    assert!(
        loaded
            .load_for_scan(store, NAME, &next.token(), &next)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(loaded.checked_flatten("bucket").expect("new root").size, 99);
}
