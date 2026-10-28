// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;

#[tokio::test]
#[serial]
async fn abandoned_listing_drains_candidate_buffered_before_finished() {
    let (mut scanner, root) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(root.clone()),
    };
    let bucket = "bucket";
    let object = "abandoned/object";
    tokio::fs::create_dir_all(root.join(bucket)).await.expect("local bucket");
    let mut disks = vec![scanner.local_disk.clone()];
    let mut original_metadata = Vec::new();
    for (index, version) in [(2, 0x1935), (3, 0x1935), (4, 0x1934)] {
        let disk_root = root.join(format!("disk-{index}"));
        let metadata = metadata_for_object_version(bucket, object, Some(Uuid::from_u128(version)));
        write_test_object_metadata_bytes(&disk_root, bucket, object, &metadata).await;
        original_metadata.push((disk_root.join(bucket).join(object).join(STORAGE_FORMAT_FILE), metadata));
        let endpoint = Endpoint::try_from(disk_root.to_string_lossy().as_ref()).expect("listing fixture endpoint");
        disks.push(
            new_disk(
                &endpoint,
                &DiskOption {
                    cleanup: false,
                    health_check: false,
                },
            )
            .await
            .expect("listing fixture disk"),
        );
    }
    for cache in [&mut scanner.old_cache, &mut scanner.new_cache] {
        cache.info.name = bucket.to_owned();
    }
    scanner.old_cache.replace(
        "bucket/abandoned/object",
        bucket,
        DataUsageEntry {
            objects: 1,
            ..Default::default()
        },
    );
    scanner.heal_object_select = 1;
    scanner.disks = disks;
    scanner.disks_quorum = 2;
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    // Force the valid select ordering that receives terminal errors before the
    // final candidate. One candidate frame fits in the bounded partial channel.
    scanner.defer_abandoned_entries_until_finished = true;
    let (_commands, mut heal_rx) = rustfs_heal_contracts::heal_channel::init_heal_channels().expect("isolated heal channel");
    let (captured_tx, mut captured_rx) = mpsc::unbounded_channel();
    let responder = tokio::spawn(async move {
        while let Some(command) = heal_rx.recv().await {
            let rustfs_heal_contracts::heal_channel::HealReceiptCommand { request, response_tx } = command;
            captured_tx
                .send((request.object_prefix.clone(), request.object_version_id.clone()))
                .expect("candidate capture remains open");
            let _ = response_tx.send(Ok(rustfs_heal_contracts::heal_channel::HealAdmissionReceipt {
                result: HealAdmissionResult::Accepted,
                task_id: request.id,
            }));
        }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        scanner.scan_folder(
            CancellationToken::new(),
            CachedFolder {
                name: bucket.to_owned(),
                parent: None,
                object_heal_prob_div: 1,
            },
            &mut DataUsageEntry::default(),
        ),
    )
    .await;
    responder.abort();
    result
        .expect("listing completion must close every callback channel")
        .expect("complete folder scan");
    let mut candidates = HashSet::new();
    while let Ok((object, version)) = captured_rx.try_recv() {
        if let (Some(object), Some(version)) = (object, version) {
            candidates.insert((object, version));
        }
    }
    let expected = [0x1934, 0x1935]
        .into_iter()
        // Raw base-object listings include the trailing separator. Preserve
        // the producer's candidate identity when draining its callback frame.
        .map(|version| (format!("{object}/"), Uuid::from_u128(version).to_string()))
        .collect();
    assert_eq!(
        candidates, expected,
        "terminal disk errors must not discard buffered exact-version candidates"
    );
    for (path, metadata) in original_metadata {
        assert_eq!(tokio::fs::read(path).await.expect("retained metadata"), metadata);
    }
}
