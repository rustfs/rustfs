// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

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
