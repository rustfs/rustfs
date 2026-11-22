// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::owner::ObjectOperations as _;
use crate::storage_api::scanner_io::{
    BUCKET_LIFECYCLE_CONFIG, apply_bucket_usage_memory_overlay, get_bucket_usage_memory, init_background_expiry,
    load_admin_data_usage_from_backend_cached, load_data_usage_from_backend, record_bucket_object_delete_memory,
    record_bucket_object_write_memory, replace_bucket_usage_memory_from_info, seed_persisted_usage, update_bucket_metadata,
};

#[tokio::test]
#[serial]
async fn get_size_preserves_expired_versions_until_both_policy_authorities_are_readable() {
    use crate::storage_api::scanner_io::{get_config_from_disk, set_bucket_metadata};
    use rustfs_common::trace_bus::{TraceFunc, subscribe_trace_events};
    let (temp, store) = setup_local_scanner_store(1, 1).await;
    let bucket = "expired-authority";
    let control = "ordinary-expiry";
    let object = "old";
    let xml = br#"<LifecycleConfiguration><Rule><ID>old</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>2</Days></Expiration></Rule></LifecycleConfiguration>"#;
    let make_item = async |bucket: &str, versioned| {
        store
            .make_bucket(
                bucket,
                &MakeBucketOptions {
                    versioning_enabled: versioned,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut reader = ScannerPutObjReader::from_vec(vec![1; 4096]);
        store
            .put_object(
                bucket,
                object,
                &mut reader,
                &ScannerObjectOptions {
                    versioned,
                    mod_time: Some(OffsetDateTime::now_utc() - time::Duration::days(4)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        wait_for_namespace_commit_tails(&store).await;
        update_bucket_metadata(bucket, BUCKET_LIFECYCLE_CONFIG, xml.to_vec())
            .await
            .unwrap();
        let (policy, _) = get_lifecycle_config(bucket).await.unwrap();
        let path = temp
            .path()
            .join("pool0-disk0")
            .join(bucket)
            .join(object)
            .join(STORAGE_FORMAT_FILE);
        ScannerItem {
            path: path.to_string_lossy().into_owned(),
            bucket: bucket.into(),
            prefix: object.into(),
            object_name: STORAGE_FORMAT_FILE.into(),
            file_type: std::fs::metadata(&path).unwrap().file_type(),
            lifecycle: Some(Arc::new(policy)),
            object_lock: None,
            replication: None,
            heal_scope: None,
            heal_enabled: false,
            heal_bitrot: false,
            debug: false,
        }
    };
    let item = make_item(bucket, true).await;
    let control_item = make_item(control, false).await;
    let original = get_config_from_disk(bucket).await.unwrap();
    let disk = store.pools[0].disk_set[0].disks.read().await[0].as_ref().unwrap().clone();
    init_background_expiry(store.clone()).await;
    let mut trace = subscribe_trace_events();
    let mut actions = |bucket: &str| {
        let mut count = 0;
        while let Ok(event) = trace.try_recv() {
            if event.func == TraceFunc::ScannerIlmAction && event.bucket.as_deref() == Some(bucket) {
                count += 1;
            }
        }
        count
    };
    for unreadable_versioning in [true, false] {
        let mut config = original.clone();
        if unreadable_versioning {
            config.versioning_config_xml = b"broken versioning".to_vec();
            config.versioning_config = None;
        } else {
            config.object_lock_config_xml = b"broken object lock".to_vec();
            config.object_lock_config = None;
        }
        set_bucket_metadata(bucket.into(), config).await.unwrap();
        assert_eq!(BucketVersioningSys::get(bucket).await.is_err(), unreadable_versioning);
        assert_eq!(object_lock_config_for_scanner_item(&item).await.is_err(), !unreadable_versioning);
        let summary = disk.get_size(item.clone()).await.unwrap();
        assert_eq!((summary.total_size, summary.versions), (4096, 1));
        assert_eq!(actions(bucket), 0);
        store
            .get_object_info(bucket, object, &ScannerObjectOptions::default())
            .await
            .expect("uncertain policy must retain object");
    }
    set_bucket_metadata(bucket.into(), original).await.unwrap();
    assert!(object_lock_config_for_scanner_item(&control_item).await.unwrap().is_none());
    disk.get_size(control_item).await.unwrap();
    assert!(actions(control) > 0, "confirmed absence must permit the expiry action");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match store.get_object_info(control, object, &ScannerObjectOptions::default()).await {
                Err(Error::ObjectNotFound(_, _)) => break,
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => panic!("read expired object: {error}"),
            }
        }
    })
    .await
    .expect("worker must delete the expired object once both authorities are known");
}

#[tokio::test]
#[serial]
async fn disk_scan_withdraws_persisted_lifecycle_policy() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    let bucket = "withdrawn-lifecycle";
    store.make_bucket(bucket, &MakeBucketOptions::default()).await.unwrap();
    let mut reader = ScannerPutObjReader::from_vec(b"retained".to_vec());
    store
        .put_object(bucket, "object", &mut reader, &ScannerObjectOptions::default())
        .await
        .unwrap();
    let xml = br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix></Prefix></Filter><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>"#;
    update_bucket_metadata(bucket, BUCKET_LIFECYCLE_CONFIG, xml.to_vec())
        .await
        .unwrap();
    let (policy, _) = get_lifecycle_config(bucket).await.unwrap();
    let set = Arc::clone(&store.pools[0].disk_set[0]);
    let disks = scanner_set_disk_inventory(&set).await;
    let cache_name = format!("{bucket}/{DATA_USAGE_CACHE_NAME}");
    for config in [
        Vec::new(),
        String::from_utf8(xml.to_vec())
            .unwrap()
            .replace("Enabled", "Disabled")
            .into_bytes(),
    ] {
        let mut old_cache = DataUsageCache::default();
        old_cache.info.name = bucket.to_string();
        old_cache.info.lifecycle = Some(Arc::new(policy.clone()));
        old_cache.info.replication = Some(Arc::new(ReplicationConfig::new(None, None)));
        old_cache.info.object_lock = Some(Arc::new(ObjectLockConfiguration::default()));
        old_cache.replace(bucket, "", DataUsageEntry::default());
        old_cache.save(Arc::clone(&set), &cache_name).await.unwrap();
        update_bucket_metadata(bucket, BUCKET_LIFECYCLE_CONFIG, config).await.unwrap();

        // A fresh cache instance models restart: the obsolete policy survives
        // serialization and must be withdrawn by the actual disk scan.
        let mut loaded = DataUsageCache::default();
        loaded.load(Arc::clone(&set), &cache_name).await.unwrap();
        assert!(loaded.info.lifecycle.is_some());
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let outcome = disks[0]
            .clone()
            .nsscanner_disk(
                ctx,
                budget,
                disks.clone(),
                loaded,
                None,
                ScannerDiskScanOptions {
                    checkpoint_endpoints: None,
                    scan_mode: HealScanMode::Normal,
                    prefix_scan_scope: None,
                    checkpoint_tx: None,
                    local_stop: None,
                },
            )
            .await
            .unwrap();
        let ScannerDiskScanOutcome::Complete(refreshed) = outcome else {
            panic!("the single-drive scan must complete");
        };
        assert!(refreshed.info.lifecycle.is_none());
        assert!(refreshed.info.replication.is_none());
        assert!(refreshed.info.object_lock.is_none());
        refreshed.save(Arc::clone(&set), &cache_name).await.unwrap();
        let mut restarted = DataUsageCache::default();
        restarted.load(Arc::clone(&set), &cache_name).await.unwrap();
        assert!(restarted.info.lifecycle.is_none(), "another restart must not resurrect policy");
        store
            .get_object_info(bucket, "object", &ScannerObjectOptions::default())
            .await
            .unwrap();
    }
}

#[tokio::test]
#[serial]
async fn compacted_prefix_expiration_between_usage_samples() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 4).await;
    let bucket = "compacted-expiration";
    let expired = "photos/vehicle/old.jpg";
    let fresh = "photos/vehicle/new.jpg";
    let unrelated = "archive/old.jpg";
    let now = OffsetDateTime::now_utc();
    let old = now - time::Duration::days(4);
    store
        .make_bucket(bucket, &MakeBucketOptions::default())
        .await
        .expect("create bucket");
    for (key, mod_time) in [(expired, old), (fresh, now), (unrelated, old)] {
        let mut reader = ScannerPutObjReader::from_vec(vec![1]);
        store
            .put_object(
                bucket,
                key,
                &mut reader,
                &ScannerObjectOptions {
                    mod_time: Some(mod_time),
                    ..Default::default()
                },
            )
            .await
            .expect("write dated object");
    }
    let disks = scanner_set_disk_inventory(&store.pools[0].disk_set[0]).await;
    let scan = async |cache| {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new_with_progress_tracking(&ctx, Default::default());
        let outcome = disks[0]
            .clone()
            .nsscanner_disk(
                ctx,
                budget.clone(),
                disks.clone(),
                cache,
                None,
                ScannerDiskScanOptions {
                    checkpoint_endpoints: None,
                    local_stop: None,
                    scan_mode: HealScanMode::Normal,
                    prefix_scan_scope: None,
                    checkpoint_tx: None,
                },
            )
            .await
            .expect("scan bucket");
        let ScannerDiskScanOutcome::Complete(cache) = outcome else { panic!("expected complete scan") };
        (cache, budget.progress().0)
    };
    let mut cache = DataUsageCache::default();
    cache.info.name = bucket.into();
    let (mut cache, scanned) = scan(cache).await;
    assert_eq!(scanned, 3);
    let prefixes = ["photos", "archive"].map(|prefix| rustfs_data_usage::hash_path(&format!("{bucket}/{prefix}")));
    assert!(prefixes.iter().all(|prefix| cache.is_compacted(prefix)));
    let xml = br#"<LifecycleConfiguration><Rule><ID>photos</ID><Status>Enabled</Status><Filter><Prefix>photos/vehicle/</Prefix></Filter><Expiration><Days>2</Days></Expiration></Rule></LifecycleConfiguration>"#;
    update_bucket_metadata(bucket, BUCKET_LIFECYCLE_CONFIG, xml.to_vec())
        .await
        .expect("configure expiration");
    init_background_expiry(store.clone()).await;
    let (_, scanned) = temp_env::async_with_vars([("RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES", None::<&str>)], async {
        let interval = crate::scanner_folder::data_usage_update_dir_cycles();
        let off_cycle = (1..=interval)
            .find(|cycle| prefixes.iter().all(|prefix| !prefix.mod_(*cycle, interval)))
            .expect("cycle that samples neither compacted prefix");
        cache.info.next_cycle = u64::from(off_cycle);
        scan(cache).await
    })
    .await;
    assert_eq!(scanned, 2, "visit the matching subtree and reuse the unrelated prefix");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match store.get_object_info(bucket, expired, &ScannerObjectOptions::default()).await {
                Err(Error::ObjectNotFound(_, _)) => break,
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(err) => panic!("read expired object: {err}"),
            }
        }
    })
    .await
    .expect("expiry worker must delete the old matching object");
    for key in [fresh, unrelated] {
        store
            .get_object_info(bucket, key, &ScannerObjectOptions::default())
            .await
            .expect("retain non-expiring object");
    }
}

// Lifecycle expiry goes through the process-global store, which the first
// store created in the process claims. Nextest runs this test in its own
// process; the lifecycle scenarios share the store that owns expiry processing.
#[tokio::test]
#[serial]
async fn lifecycle_expiration_usage_converges_across_complete_scans() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let next_cycle = check_persisted_stale_usage_recovery(&store, false, 1).await;
    let next_cycle = check_persisted_stale_usage_recovery(&store, true, next_cycle + 1).await;
    let next_cycle = check_usage_after_lifecycle_expiration(&store, "idle", false, next_cycle).await;
    check_usage_after_lifecycle_expiration(&store, "busy", true, next_cycle).await;
}

#[tokio::test]
#[serial]
async fn historical_usage_converges_with_writes_on_one_drive() {
    let (_temp_dir, store) = setup_local_scanner_store(1, 1).await;
    check_persisted_stale_usage_recovery(&store, true, 1).await;
}

async fn check_usage_after_lifecycle_expiration(
    store: &Arc<ECStore>,
    scenario: &str,
    write_between_scans: bool,
    first_cycle: u64,
) -> u64 {
    clear_dirty_usage_buckets_for_tests();
    let cases = [
        ("ilm-empty", true, false),
        ("ilm-partial", true, true),
        ("delete-empty", false, false),
        ("delete-partial", false, true),
    ]
    .map(|(name, lifecycle, keep_one)| (format!("{scenario}-{name}"), lifecycle, keep_one));
    let usage_matches = |snapshot: &DataUsageInfo, retained_writes: u64| {
        cases.iter().all(|(bucket, _, keep_one)| {
            snapshot.buckets_usage.get(bucket).is_some_and(|usage| {
                usage.objects_count == u64::from(*keep_one) + retained_writes
                    && usage.size == (u64::from(*keep_one) + retained_writes) * 42
            })
        })
    };
    let mut baseline = DataUsageInfo {
        last_update: Some(SystemTime::now()),
        usage_snapshot_complete: true,
        ..Default::default()
    };
    for (bucket, _, _) in &cases {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create usage bucket");
        baseline.buckets_usage.insert(bucket.clone(), BucketUsageInfo::default());
        baseline.bucket_sizes.insert(bucket.clone(), 0);
    }
    baseline.buckets_count = 4;
    replace_bucket_usage_memory_from_info(&baseline).await;

    for (bucket, lifecycle, keep_one) in &cases {
        for object in ["expire/first", if *keep_one { "keep/second" } else { "expire/second" }] {
            let mut reader = ScannerPutObjReader::from_vec(vec![0; 42]);
            store
                .put_object(bucket, object, &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("write object before expiration");
            record_bucket_object_write_memory(bucket, None, 42).await;
        }
        if *lifecycle {
            let config = br#"<LifecycleConfiguration><Rule><ID>expire</ID><Status>Enabled</Status><Filter><Prefix>expire/</Prefix></Filter><Expiration><Days>0</Days></Expiration></Rule></LifecycleConfiguration>"#;
            update_bucket_metadata(bucket, BUCKET_LIFECYCLE_CONFIG, config.to_vec())
                .await
                .expect("configure immediate expiration after writes");
        } else {
            for object in ["expire/first", "expire/second"]
                .into_iter()
                .take(if *keep_one { 1 } else { 2 })
            {
                store
                    .delete_object(bucket, object, ScannerObjectOptions::default())
                    .await
                    .expect("ordinary deletion should succeed");
                record_bucket_object_delete_memory(bucket, 42, true).await;
            }
        }
    }
    init_background_expiry(store.clone()).await;
    wait_for_namespace_commit_tails(store.as_ref()).await;

    let mut reconciled = false;
    let mut observed_expiration = false;
    let mut retained_writes = 0;
    let mut reconciled_cycles = 0;
    let mut cycle = first_cycle;
    while cycle < first_cycle + 8 {
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, mut receiver) = mpsc::channel(4);
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, cycle, 11, HealScanMode::Normal),
        )
        .await
        .expect("scan should finish")
        .expect("scan should succeed");
        while let Some(mut snapshot) = receiver.recv().await {
            if result.status != ScannerCycleStatus::Complete || !snapshot.is_complete_bucket_usage_snapshot() {
                continue;
            }
            if usage_matches(&snapshot, retained_writes) {
                observed_expiration = true;
                if write_between_scans {
                    for (bucket, _, _) in &cases {
                        let mut reader = ScannerPutObjReader::from_vec(vec![0; 42]);
                        store
                            .put_object(bucket, &format!("keep/write-{cycle}"), &mut reader, &ScannerObjectOptions::default())
                            .await
                            .expect("write between scan observation and publication");
                        record_bucket_object_write_memory(bucket, None, 42).await;
                    }
                    retained_writes += 1;
                    wait_for_namespace_commit_tails(store.as_ref()).await;
                }
                replace_bucket_usage_memory_from_info(&snapshot).await;
                apply_bucket_usage_memory_overlay(&mut snapshot).await;
                if usage_matches(&snapshot, retained_writes) {
                    reconciled_cycles += 1;
                    reconciled = !write_between_scans || reconciled_cycles >= 3;
                    break;
                }
            }
        }
        cycle += 1;
        if reconciled {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        observed_expiration,
        "{scenario}: complete scans must observe successful lifecycle deletions"
    );
    assert!(
        reconciled,
        "{scenario}: usage overlay must converge to the complete scanner counts and bytes"
    );
    cycle
}

async fn check_persisted_stale_usage_recovery(store: &Arc<ECStore>, continuing_writes: bool, first_cycle: u64) -> u64 {
    let cases = if continuing_writes {
        [("historical-busy-empty", 0_u64), ("historical-busy-retained", 1_u64)]
    } else {
        [("historical-empty", 0_u64), ("historical-retained", 1_u64)]
    };
    let mut stale = DataUsageInfo {
        last_update: Some(SystemTime::now()),
        usage_snapshot_complete: true,
        usage_snapshot_converged: Some(true),
        buckets_count: 2,
        scanner_epoch: Some(11),
        scanner_cycle: Some(first_cycle - 1),
        ..Default::default()
    };
    for (bucket, retained) in cases {
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create historical usage bucket");
        if retained > 0 {
            let mut reader = ScannerPutObjReader::from_vec(vec![0; 42]);
            store
                .put_object(bucket, "retained", &mut reader, &ScannerObjectOptions::default())
                .await
                .expect("create the only live object before seeding stale usage");
        }
        stale.buckets_usage.insert(
            bucket.to_owned(),
            BucketUsageInfo {
                objects_count: 436 + retained,
                size: 4_587_688_512 + retained * 42,
                ..Default::default()
            },
        );
        stale.bucket_sizes.insert(bucket.to_owned(), 4_587_688_512 + retained * 42);
    }
    stale.calculate_totals();
    wait_for_namespace_commit_tails(store.as_ref()).await;
    stale.last_update = Some(SystemTime::now());
    seed_persisted_usage(stale, store.clone())
        .await
        .expect("persist an incorrect complete aggregate");
    let loaded = load_data_usage_from_backend(store.clone())
        .await
        .expect("load the incorrect durable baseline");
    assert!(loaded.is_complete_bucket_usage_snapshot());
    for (bucket, retained) in cases {
        assert_eq!(loaded.buckets_usage[bucket].objects_count, 436 + retained);
        assert_eq!(loaded.buckets_usage[bucket].size, 4_587_688_512 + retained * 42);
    }
    // Start from the incorrect durable aggregate; only the production scanner
    // may publish a corrected snapshot. Per-set cache corruption is separate.
    replace_bucket_usage_memory_from_info(&loaded).await;
    let cycles = if continuing_writes { 6 } else { 2 };
    for cycle in first_cycle..first_cycle + cycles {
        let added_objects = if continuing_writes { cycle - first_cycle + 1 } else { 0 };
        if continuing_writes {
            for (bucket, _) in cases {
                let mut reader = ScannerPutObjReader::from_vec(vec![0; 42]);
                store
                    .put_object(bucket, &format!("keep/write-{cycle}"), &mut reader, &ScannerObjectOptions::default())
                    .await
                    .expect("write before each historical recovery scan");
                record_bucket_object_write_memory(bucket, None, 42).await;
            }
            wait_for_namespace_commit_tails(store.as_ref()).await;
        }
        let ctx = CancellationToken::new();
        let budget = ScannerCycleBudget::new(&ctx, ScannerCycleBudgetConfig::default());
        let (updates, receiver) = mpsc::channel(4);
        let persist = tokio::spawn(crate::scanner::store_data_usage_in_backend(ctx.clone(), store.clone(), receiver));
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            ScannerIOCycle::nsscanner_with_status(store.as_ref(), ctx, budget, updates, cycle, 11, HealScanMode::Normal),
        )
        .await
        .expect("historical recovery scan should finish")
        .expect("historical recovery scan should succeed");
        assert_eq!(result.status, ScannerCycleStatus::Complete);
        tokio::time::timeout(Duration::from_secs(30), persist)
            .await
            .expect("scanner publication should finish")
            .expect("scanner publication task should not panic");

        let durable = load_data_usage_from_backend(store.clone())
            .await
            .expect("reload scanner-produced durable usage");
        assert!(durable.is_complete_bucket_usage_snapshot());
        assert_eq!(durable.usage_snapshot_converged, Some(true));
        assert_eq!(durable.scanner_cycle, Some(cycle));
        let mut displayed = load_admin_data_usage_from_backend_cached(store.clone())
            .await
            .expect("load usage through the admin snapshot path");
        apply_bucket_usage_memory_overlay(&mut displayed).await;
        for (bucket, retained) in cases {
            assert_eq!(get_bucket_usage_memory(bucket).await, Some((retained + added_objects) * 42));
            for snapshot in [&durable, &displayed] {
                assert_eq!(snapshot.buckets_usage[bucket].objects_count, retained + added_objects);
                assert_eq!(snapshot.buckets_usage[bucket].size, (retained + added_objects) * 42);
            }
        }
    }
    first_cycle + cycles
}
