// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage_api::owner::ObjectOperations as _;
use crate::storage_api::scanner_io::{
    BUCKET_LIFECYCLE_CONFIG, apply_bucket_usage_memory_overlay, init_background_expiry, record_bucket_object_delete_memory,
    record_bucket_object_write_memory, replace_bucket_usage_memory_from_info, update_bucket_metadata,
};

// Lifecycle expiry goes through the process-global store, which the first
// store created in the process claims. Nextest runs this test in its own
// process; sharing one store also lets both scenarios run in one filtered
// `cargo test` process.
#[tokio::test]
#[serial]
async fn lifecycle_expiration_usage_converges_across_complete_scans() {
    let (_temp_dir, store) = setup_two_pool_scanner_store().await;
    let next_cycle = check_usage_after_lifecycle_expiration(&store, "idle", false, 1).await;
    check_usage_after_lifecycle_expiration(&store, "busy", true, next_cycle).await;
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
