// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::runtime::usage_watch::{ScannerUsageWatchRequest, UsageWatchRegistry};

fn watch_request(bucket: &str) -> ScannerUsageWatchRequest {
    ScannerUsageWatchRequest {
        bucket: bucket.into(),
        bucket_incarnation: Uuid::from_u128(2),
        watch_id: None,
        release: false,
        session_id: None,
    }
}

async fn fixture() -> (tempfile::TempDir, Arc<LocalDisk>, Arc<UsageWatchRegistry>) {
    let root = tempfile::tempdir().expect("watch physical fixture directory");
    let endpoint = Endpoint::try_from(root.path().to_string_lossy().as_ref()).expect("watch physical endpoint");
    let disk = Arc::new(LocalDisk::new(&endpoint, false).await.expect("watch physical disk"));
    disk.make_volume("bucket").await.expect("watch test bucket");
    let registry = Arc::new(UsageWatchRegistry::default());
    disk.usage_watch_binding.bind(&registry);
    (root, disk, registry)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_watch_stays_inflight_after_metadata_is_visible_and_waiter_is_canceled() {
    let (_root, disk, registry) = fixture().await;
    let target = disk
        .io_get_object_path("bucket", "object")
        .expect("target metadata directory");
    let (entered, wait_for_enter) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let _hook = os::prepared_publication_test_hooks::install_at(
        os::prepared_publication_test_hooks::Stage::DirFsync,
        &target,
        move || {
            let _ = entered.send(());
            let _ = hold.recv();
        },
    );
    let writer_disk = Arc::clone(&disk);
    let mut writer = tokio::spawn(async move {
        let info =
            super::test::test_file_info("object", Uuid::new_v4(), Some(Uuid::new_v4()), Some(Bytes::from_static(b"payload")));
        writer_disk.write_metadata("", "bucket", "object", info).await
    });
    tokio::time::timeout(Duration::from_secs(10), wait_for_enter)
        .await
        .expect("reach physical fsync barrier")
        .expect("barrier entered");
    assert!(
        target.join(STORAGE_FORMAT_FILE).exists(),
        "metadata is already visible at the blocked fsync"
    );
    let during = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("install during active physical mutation");
    assert!(during.inflight > 0, "installation must include pre-existing writes");
    writer.abort();
    assert!((&mut writer).await.expect_err("cancel logical waiter").is_cancelled());
    let canceled = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("watch survives logical cancellation");
    assert!(canceled.inflight > 0, "physical tail retains its guard");
    drop(release);
    let settled = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let observation = registry
                .observe(Uuid::from_u128(1), &watch_request("bucket"))
                .expect("settlement proof");
            if observation.inflight == 0 {
                break observation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("physical tail settles");
    assert!(settled.revision > during.revision);
    assert!(!settled.unchanged_since(&during));
    println!(
        "usage_assertion: {}",
        serde_json::json!({
            "case_id": "USAGE-12", "assertion_id": "USAGE-12-A03", "status": "PASS",
            "evidence": {"inflight_at_install": during.inflight, "inflight_after_waiter_cancel": canceled.inflight, "inflight_after_physical_end": settled.inflight, "revision_at_install": during.revision, "revision_after_physical_end": settled.revision}
        })
    );
    drop(disk);
    drop(registry);
    let root_path = _root.path().to_path_buf();
    drop(_root);
    assert!(!root_path.exists());
    println!(
        "usage_cleanup: {}",
        serde_json::json!({"case_id": "USAGE-12", "status": "PASS", "fixture_removed": !root_path.exists()})
    );
}

#[tokio::test]
async fn usage_watch_status_only_metadata_keeps_maintenance_semantics() {
    let (_root, disk, registry) = fixture().await;
    let info = super::test::test_file_info("object", Uuid::new_v4(), Some(Uuid::new_v4()), Some(Bytes::from_static(b"payload")));
    disk.write_metadata("", "bucket", "object", info.clone())
        .await
        .expect("initial counted version");
    let before = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("idle watch");
    let mut status = info.clone();
    status.metadata.clear();
    rustfs_utils::http::metadata_compat::insert_str(
        &mut status.metadata,
        rustfs_utils::http::SUFFIX_REPLICATION_STATUS,
        "arn=COMPLETED;".into(),
    );
    disk.update_metadata("bucket", "object", status, &UpdateMetadataOpts::default())
        .await
        .expect("status-class metadata update");
    let after = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("status result watch");
    assert!(after.unchanged_since(&before), "replication counters retain maintenance cadence");
    let mut next = info;
    next.version_id = Some(Uuid::new_v4());
    disk.write_metadata("", "bucket", "object", next).await.expect("new version");
    let changed = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("new version watch");
    assert!(changed.revision > after.revision);
    assert_eq!(changed.inflight, 0);
}

#[tokio::test]
async fn usage_watch_raw_live_stream_revokes_coverage_and_internal_staging_does_not() {
    let (_root, disk, registry) = fixture().await;
    let before = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("idle watch");
    disk.usage_watch_binding.mark_uncovered_user_stream(RUSTFS_META_TMP_BUCKET);
    assert!(
        registry
            .observe(Uuid::from_u128(1), &watch_request("bucket"))
            .expect("internal staging stays covered")
            .unchanged_since(&before)
    );
    let _stream = disk.create_file("bucket", "bucket", "raw-file", 0).await;
    assert_eq!(registry.observe(Uuid::from_u128(1), &watch_request("bucket")), None);
    assert!(!disk.usage_watch_binding.matches(&registry));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn usage_watch_delete_paths_retains_ownership_through_canceled_trash_rename() {
    let (_root, disk, registry) = fixture().await;
    let target = disk.io_get_object_path("bucket", "delete-me").expect("delete source");
    std::fs::write(&target, b"payload").expect("initial source");
    let trash = disk.io_get_bucket_path(RUSTFS_META_TMP_DELETED_BUCKET).expect("trash volume");
    std::fs::create_dir_all(&trash).expect("trash directory");
    let before = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("initial proof");
    let (entered, wait_for_enter) = tokio::sync::oneshot::channel();
    let (release, hold) = std::sync::mpsc::channel::<()>();
    let _hook =
        os::prepared_publication_test_hooks::install_at(os::prepared_publication_test_hooks::Stage::Rename, &target, move || {
            let _ = entered.send(());
            let _ = hold.recv();
        });
    let deleting_disk = Arc::clone(&disk);
    let mut deleting = tokio::spawn(async move { deleting_disk.delete_paths("bucket", &["delete-me".into()]).await });
    tokio::time::timeout(Duration::from_secs(10), wait_for_enter)
        .await
        .expect("reach raw trash rename")
        .expect("rename entered");
    deleting.abort();
    assert!((&mut deleting).await.expect_err("logical waiter canceled").is_cancelled());
    let during = registry
        .observe(Uuid::from_u128(1), &watch_request("bucket"))
        .expect("active proof");
    assert!(during.inflight > 0, "trash syscall owns the watch after waiter cancellation");
    assert!(during.revision > before.revision);
    drop(release);
    let after = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let observation = registry
                .observe(Uuid::from_u128(1), &watch_request("bucket"))
                .expect("draining proof");
            if observation.inflight == 0 {
                break observation;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("physical rename settles");
    assert!(!target.exists());
    assert!(after.revision > during.revision);
    assert!(!after.unchanged_since(&before));
}

#[tokio::test]
async fn owned_stale_destination_removal_preserves_file_directory_and_symlink_semantics() {
    let (_root, disk, registry) = fixture().await;
    let target = disk.io_get_object_path("bucket", "stale").expect("stale destination");
    std::fs::write(&target, b"stale-file").expect("stale file");
    os::remove_with_owner(&target, disk.usage_watch_binding.owner("bucket", None))
        .await
        .expect("remove stale file");
    assert!(!target.exists());
    std::fs::create_dir(&target).expect("stale directory");
    os::remove_with_owner(&target, disk.usage_watch_binding.owner("bucket", None))
        .await
        .expect("remove empty directory");
    assert!(!target.exists());
    #[cfg(unix)]
    {
        let referent = disk.io_get_object_path("bucket", "retained").expect("symlink referent");
        std::fs::write(&referent, b"retained").expect("retained file");
        std::os::unix::fs::symlink(&referent, &target).expect("stale symlink");
        os::remove_with_owner(&target, disk.usage_watch_binding.owner("bucket", None))
            .await
            .expect("remove symlink itself");
        assert!(!target.exists());
        assert_eq!(std::fs::read(referent).expect("referent remains"), b"retained");
    }
    assert_eq!(
        registry
            .observe(Uuid::from_u128(1), &watch_request("bucket"))
            .expect("all work drained")
            .inflight,
        0
    );
}
