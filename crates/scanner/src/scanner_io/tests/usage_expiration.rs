// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::owner::ObjectOperations as _;
use crate::storage_api::scanner_io::{
    BUCKET_LIFECYCLE_CONFIG, apply_bucket_usage_memory_overlay, get_bucket_usage_memory, init_background_expiry,
    load_admin_data_usage_from_backend_cached, load_data_usage_from_backend, record_bucket_object_delete_memory,
    record_bucket_object_write_memory, replace_bucket_usage_memory_from_info, seed_persisted_usage, update_bucket_metadata,
};

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
