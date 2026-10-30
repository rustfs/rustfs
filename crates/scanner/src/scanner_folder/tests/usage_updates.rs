// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[serial]
async fn usage_updates_preserve_totals_without_allocating_an_unrequested_cache() {
    for observed in [false, true] {
        let (mut scanner, root) = build_test_scanner().await;
        let _guard = TestGuard {
            temp_dir: Some(root.clone()),
        };
        scanner.old_cache.info.name = "bucket".to_string();
        scanner.new_cache.info.name = "bucket".to_string();
        scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
        scanner.skip_heal.store(true, Ordering::SeqCst);
        for prefix in ["existing", "new"] {
            for index in 0..8 {
                let object = format!("{prefix}/{index:04}");
                write_test_object_metadata_bytes(&root, "bucket", &object, &metadata_for_object_version("bucket", &object, None))
                    .await;
            }
        }
        scanner.old_cache.replace("bucket", "", DataUsageEntry::default());
        scanner
            .old_cache
            .replace("bucket/existing", "bucket", DataUsageEntry::default());
        scanner.old_cache.replace(
            "bucket/existing/0000",
            "bucket/existing",
            DataUsageEntry {
                objects: 1,
                size: 99,
                ..Default::default()
            },
        );
        let (sender, mut receiver) = mpsc::channel(2);
        if observed {
            scanner.usage_updates = Some(ScannerUsageUpdates::new(sender, "bucket".to_string()));
        }
        scanner
            .scan_folder(
                CancellationToken::new(),
                CachedFolder {
                    name: "bucket".to_string(),
                    parent: None,
                    object_heal_prob_div: 1,
                },
                &mut DataUsageEntry::default(),
            )
            .await
            .expect("scan existing and new prefixes");
        let totals = scanner
            .new_cache
            .checked_flatten_complete_scope("bucket")
            .expect("complete accounting");
        assert_eq!((totals.objects, totals.size), (16, 16));
        assert_eq!(
            scanner
                .old_cache
                .find("bucket/existing/0000")
                .expect("retained baseline")
                .size,
            99
        );
        if observed {
            while receiver.try_recv().is_ok() {}
            let updates = scanner.usage_updates.as_mut().expect("requested progress owner");
            let progress = updates.cache.checked_flatten("bucket").expect("progress cache is connected");
            assert_eq!((progress.objects, progress.size), (16, 16));
            updates.last_update = SystemTime::UNIX_EPOCH;
            scanner.send_update().await;
            let update = receiver.try_recv().expect("requested aggregate update");
            assert_eq!((update.objects, update.size), (16, 16));
            scanner.send_update().await;
            assert!(receiver.try_recv().is_err(), "unchanged minute interval must not emit another aggregate");
        } else {
            assert!(scanner.usage_updates.is_none(), "unobserved scans must never allocate a progress cache");
        }
    }
}

#[tokio::test]
async fn usage_updates_do_not_mirror_durable_ledgers_or_checkpoint_metadata() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard { temp_dir: Some(root) };
    scanner.new_cache.info.name = "bucket".to_string();
    let (sender, _receiver) = mpsc::channel(1);
    scanner.usage_updates = Some(ScannerUsageUpdates::new(sender, "bucket".to_string()));
    scanner.record_pending_scanner_heal(
        PendingScannerHealKind::Object,
        "bucket",
        Some("object"),
        None,
        HealScanMode::Normal,
        HealAdmissionResult::Full,
    );
    scanner.record_scan_resume_hint("bucket/object");
    let mut summary = SizeSummary::default();
    summary.record_size_reconciliation(SizeReconciliationEntry {
        key: "unresolved-size".to_string(),
        bucket: "bucket".to_string(),
        object: "object".to_string(),
        reason: "invalid_declared_size".to_string(),
        ..Default::default()
    });
    scanner.apply_size_reconciliation(&summary);
    scanner.finish_size_reconciliation_batch();
    let restored = DataUsageCache::unmarshal(&scanner.new_cache.marshal_msg().expect("encode canonical state"))
        .expect("reload canonical state");
    assert_eq!(restored.info.pending_heals.len(), 1);
    assert_eq!(restored.info.size_reconciliation.len(), 1);
    assert_eq!(restored.info.scan_checkpoint, scanner.new_cache.info.scan_checkpoint);
    let progress = &scanner.usage_updates.as_ref().expect("requested progress owner").cache;
    assert_eq!(progress.info.name, "bucket");
    assert!(progress.info.pending_heals.is_empty());
    assert!(progress.info.size_reconciliation.is_empty());
    assert!(progress.info.scan_checkpoint.is_none());
    assert!(progress.info.scan_resume_after.is_none());
}
