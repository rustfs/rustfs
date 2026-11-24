// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test(start_paused = true)]
async fn paged_backup_preparation_stays_inside_the_backup_budget() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = seeded();
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("initial heads");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("seed primary and backup");
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("committed heads");
    cache.cache.get_mut("bucket/prefix-04095").expect("last leaf").size += 17;
    let backup = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}.bkp"));
    store.delayed_gets.lock().await.insert(backup, Duration::from_secs(60));
    let started = tokio::time::Instant::now();
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("primary commit survives a slow best-effort backup");
    assert!(
        started.elapsed() <= Duration::from_secs(5),
        "backup preparation exceeded its entire budget"
    );
    let mut loaded = DataUsageCache::default();
    loaded
        .load(store, NAME)
        .await
        .expect("primary remains readable after backup cancellation");
    assert_eq!(loaded.checked_flatten("bucket").expect("primary graph").size, 4096 * 4097 / 2 + 17);
}

#[tokio::test]
async fn display_progress_retained_compacted_handoff_preserves_measurement_time() {
    let store = Arc::new(MemoryConfigStore {
        display_progress_generation: Some(0),
        ..Default::default()
    });
    let mut cache = empty_bucket_checkpoint();
    let plan = DataUsageScanPlanDigest([1; 32]);
    cache.info.scan_progress = Some(crate::DataUsageScanProgress {
        started_plan: plan,
        requested_plan: plan,
    });
    let first_source = format!("{}|disk-a", uuid::Uuid::from_u128(1));
    cache.info.scan_directory_source = Some(first_source);
    cache.info.last_update = Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10));
    let [receipt, _] = crate::scanner_folder::DirectoryCheckpoint::storage_fixture(&cache.info, 4, [2, 3]).await;
    cache.info.scan_directory_work = vec![receipt];
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("initial heads");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("save compacted measurement");
    let progress_path = memory_config_key(RUSTFS_META_BUCKET, &rustfs_data_usage::bucket_scan_progress_path("bucket"));
    let original: rustfs_data_usage::BucketScanProgressSource =
        serde_json::from_slice(&store.objects.lock().await[&progress_path]).expect("original compacted sample");
    assert_eq!(original.objects, Some(2));
    for turn in 1..=2 {
        let mut cold = DataUsageCache::default();
        let revisions = cold
            .load_with_revisions(store.clone(), NAME)
            .await
            .expect("cold shared handoff");
        cold.activate_directory_source(
            format!("{}|disk-b", uuid::Uuid::from_u128(2)),
            &HashSet::from(["disk-a".into(), "disk-b".into()]),
        );
        cold.replace("bucket", "", DataUsageEntry::default());
        cold.info.leader_epoch += 1;
        cold.info.last_update = Some(std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(10 + turn * 60));
        assert!(
            !cold.info.scan_directory_work.is_empty(),
            "handoff must retain the previous compacted receipt"
        );
        cold.save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
            .await
            .expect("save enumerating executor");
        let sample: rustfs_data_usage::BucketScanProgressSource =
            serde_json::from_slice(&store.objects.lock().await[&progress_path]).expect("retained compacted sample");
        assert_eq!((sample.objects, sample.size), (original.objects, original.size));
        assert_eq!(
            sample.updated_at, original.updated_at,
            "checkpoint saves cannot refresh retained measurements"
        );
    }
}

#[tokio::test]
async fn indefinite_partial_checkpoints_keep_unreachable_bytes_bounded() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = seeded();
    let mut initial_bytes = 0;
    for round in 0..10 {
        for (name, entry) in &mut cache.cache {
            if name != "bucket" {
                entry.size += 17;
            }
        }
        // A single long-running cycle can checkpoint many times without changing cycle number.
        assert!(!cache.info.snapshot_complete);
        let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
            .await
            .expect("current heads");
        cache
            .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
            .await
            .expect("commit partial");
        let current: usize = layout(&store)
            .await
            .iter()
            .filter(|(path, _)| path.contains(".pages/"))
            .map(|(_, (_, size))| size)
            .sum();
        if round == 0 {
            initial_bytes = current;
            assert!(initial_bytes > 100_000);
        }
        assert!(
            current <= initial_bytes * 4,
            "after {} partials, {current} page bytes for {initial_bytes} initially",
            round + 1
        );
    }
    let loaded = &mut DataUsageCache::default();
    loaded.load(store, NAME).await.expect("restore bounded checkpoint");
    assert_eq!(
        loaded.checked_flatten("bucket").expect("valid restored tree").size,
        cache.checked_flatten("bucket").expect("fixture tree").size
    );
}

async fn reopen_checkpoint_store(store: &MemoryConfigStore) -> Arc<MemoryConfigStore> {
    let reopened = Arc::new(MemoryConfigStore::default());
    *reopened.objects.lock().await = store.objects.lock().await.clone();
    *reopened.revisions.lock().await = store.revisions.lock().await.clone();
    *reopened.put_counts.lock().await = store.put_counts.lock().await.clone();
    reopened
}

#[tokio::test]
async fn abandoned_checkpoint_generation_is_reclaimed_after_an_inline_commit() {
    let store = Arc::new(MemoryConfigStore::default());
    let main = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}"));
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("empty store");
    let mut committed = empty_bucket_checkpoint();
    committed
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("commit small checkpoint");

    let before = layout(&store).await;
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("inline revision");
    store.fail_puts_from.lock().await.insert(main.clone(), before[&main].0 + 1);
    seeded()
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect_err("interrupt the root commit after staging the large checkpoint");

    let staged: Vec<_> = layout(&store)
        .await
        .into_keys()
        .filter(|path| path.contains(".bin.pages/") && !before.contains_key(path))
        .collect();
    assert!(staged.len() > 1, "the interrupted commit must have written pages");
    assert_eq!(
        DataUsageCache::read_revisions(store.clone(), NAME)
            .await
            .expect("roots remain readable"),
        revisions
    );

    // Preserve durable objects while dropping the interrupted writer's receipts.
    let store = reopen_checkpoint_store(&store).await;
    store.fail_prefix_deletes.store(true, Ordering::Relaxed);
    committed.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 1,
            size: 17,
            ..Default::default()
        },
    );
    committed
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("publish the different inline checkpoint despite interrupted cleanup");
    let interrupted_cleanup = layout(&store).await;
    assert!(staged.iter().all(|path| interrupted_cleanup.contains_key(path)));

    // A second restart leaves only the new root's durable cleanup record.
    let restarted = reopen_checkpoint_store(&store).await;
    let mut restored = DataUsageCache::default();
    let revisions = restored
        .load_with_revisions(restarted.clone(), NAME)
        .await
        .expect("restore the committed inline checkpoint");
    assert_eq!(restored.checked_flatten("bucket").expect("inline totals").size, 17);
    restored
        .save_with_revisions_for_epoch(restarted.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("reclaim the abandoned generation after restart");
    let after = layout(&restarted).await;
    assert!(staged.iter().all(|path| !after.contains_key(path)), "abandoned pages leaked");
    restored.load(restarted, NAME).await.expect("load after reclaim");
    assert_eq!(restored.checked_flatten("bucket").expect("retained inline totals").size, 17);
}

enum BackupDamage {
    Missing,
    Corrupt,
    UnreadableMetadata,
}

async fn survives_lost_primary_after_backup_page_damage(damage: BackupDamage) {
    let store = Arc::new(MemoryConfigStore::default());
    let cache = seeded();
    let first = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("empty heads");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &first, 0, CancellationToken::new())
        .await
        .expect("initial checkpoint");
    let modify_tail = |cache: &mut DataUsageCache| {
        cache.cache.get_mut("bucket/prefix-04095").expect("last leaf").size += 99;
    };
    let control = Arc::new(MemoryConfigStore::default());
    let mut target = cache.clone();
    modify_tail(&mut target);
    let empty = DataUsageCache::read_revisions(control.clone(), NAME)
        .await
        .expect("empty control store");
    target
        .save_with_revisions_for_epoch(control.clone(), NAME, &empty, 0, CancellationToken::new())
        .await
        .expect("encode target in an independent fresh bundle");
    {
        let control_objects = control.objects.lock().await;
        let mut objects = store.objects.lock().await;
        let victim = objects
            .iter()
            .find_map(|(path, data)| {
                // A fresh target bundle only contains currently reachable objects.
                (path.contains(".bkp.pages/") && data.len() > 8192 && control_objects.get(path) == Some(data))
                    .then(|| path.clone())
            })
            .expect("a large unchanged leaf is reachable in both manifests");
        match damage {
            BackupDamage::Corrupt => objects.get_mut(&victim).expect("existing backup leaf")[0] ^= 1,
            BackupDamage::Missing => {
                objects.remove(&victim);
            }
            BackupDamage::UnreadableMetadata => {
                drop(objects);
                store.read_errors.lock().await.insert(victim, EcstoreError::FileCorrupt);
            }
        }
    }
    let mut from_main = DataUsageCache::default();
    let revisions = from_main
        .load_with_revisions(store.clone(), NAME)
        .await
        .expect("primary remains healthy");
    modify_tail(&mut from_main);
    from_main
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("incremental commit while primary is healthy");
    let main = memory_config_key(RUSTFS_META_BUCKET, &format!("{BUCKET_META_PREFIX}/{NAME}"));
    store.objects.lock().await.remove(&main);
    let mut recovered = DataUsageCache::default();
    recovered.load(store, NAME).await.expect("standalone cold backup recovery");
    let actual = recovered
        .checked_flatten("bucket")
        .expect("a connected backup after losing its primary");
    assert_eq!((actual.objects, actual.size), (4096, 4096 * 4097 / 2 + 99));
}

#[tokio::test]
async fn incremental_save_repairs_missing_backup_leaf() {
    survives_lost_primary_after_backup_page_damage(BackupDamage::Missing).await;
}

#[tokio::test]
async fn incremental_save_repairs_corrupt_backup_leaf() {
    survives_lost_primary_after_backup_page_damage(BackupDamage::Corrupt).await;
}

#[tokio::test]
async fn incremental_save_rebuilds_backup_with_unreadable_page_metadata() {
    survives_lost_primary_after_backup_page_damage(BackupDamage::UnreadableMetadata).await;
}

#[tokio::test]
async fn unreclaimed_partial_generation_bounds_staging_and_can_retry() {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = seeded();
    cache.info.snapshot_complete = true;
    let empty = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("empty store");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &empty, 0, CancellationToken::new())
        .await
        .expect("committed predecessor");
    store.fail_prefix_deletes.store(true, Ordering::Relaxed);
    cache.info.snapshot_complete = false;
    cache.info.next_cycle += 1;
    for _ in 0..8 {
        for (name, entry) in &mut cache.cache {
            if name != "bucket" {
                entry.size += 17;
            }
        }
        let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
            .await
            .expect("current heads");
        let before = layout(&store).await;
        if let Err(error) = cache
            .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
            .await
        {
            assert!(error.to_string().contains("reclaim"), "unexpected save failure: {error}");
            assert_eq!(before, layout(&store).await, "a blocked retirement must not stage further objects");
            assert!(
                cache
                    .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
                    .await
                    .is_err()
            );
            assert_eq!(
                before,
                layout(&store).await,
                "retry while cleanup is unavailable must leave both generations readable"
            );
            let mut previous = DataUsageCache::default();
            previous.load(store.clone(), NAME).await.expect("last committed checkpoint");
            assert!(previous.checked_flatten("bucket").is_some());
            store.fail_prefix_deletes.store(false, Ordering::Relaxed);
            cache
                .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
                .await
                .expect("save after previous retirement is reclaimed");
            previous.load(store.clone(), NAME).await.expect("read latest checkpoint");
            assert_eq!(
                cache.checked_flatten("bucket").expect("fixture").size,
                previous.checked_flatten("bucket").expect("recovered root").size
            );
            return;
        }
    }
    panic!("persistent retirement failure allowed unbounded staging");
}
