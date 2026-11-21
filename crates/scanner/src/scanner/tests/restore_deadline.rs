// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::data_usage_define::{DATA_USAGE_SCAN_CHECKPOINT_VERSION, DataUsageCacheRevisions, DataUsageScanIdentity};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

const NAME: &str = "bucket/.usage-cache.bin";
const COUNT: usize = 1536;

struct ReadWindow {
    pages: usize,
    device: Arc<Semaphore>,
    completed: std::sync::Mutex<Vec<String>>,
    deadline: Notify,
}

tokio::task_local! {
    static READ_WINDOW: Arc<ReadWindow>;
}

// Model a serial device with one page of I/O time per admission. Completing
// a second read would exceed the deadline, so the caller drops that waiter.
pub(super) async fn admit_page(object: &str) -> Option<OwnedSemaphorePermit> {
    let window = READ_WINDOW.try_with(Arc::clone).ok()?;
    let permit = window.device.clone().acquire_owned().await.expect("device stays open");
    {
        let mut completed = window.completed.lock().expect("device history");
        if completed.len() < window.pages {
            completed.push(object.into());
            return Some(permit);
        }
    }
    drop(permit);
    window.deadline.notify_one();
    std::future::pending().await
}

async fn window(store: &Arc<MemoryConfigStore>, loaded: &mut DataUsageCache) -> (Option<DataUsageCacheRevisions>, Vec<String>) {
    let window = Arc::new(ReadWindow {
        pages: 1,
        device: Arc::new(Semaphore::new(1)),
        completed: Default::default(),
        deadline: Notify::new(),
    });
    let result = READ_WINDOW
        .scope(window.clone(), async {
            tokio::select! {
                biased;
                value = loaded.load_with_revisions(store.clone(), NAME) => Some(value.expect("checkpoint load")),
                () = window.deadline.notified() => None,
            }
        })
        .await;
    let pages = std::mem::take(&mut *window.completed.lock().expect("device history"));
    (result, pages)
}

async fn fixture() -> (Arc<MemoryConfigStore>, DataUsageCache) {
    let store = Arc::new(MemoryConfigStore::default());
    let mut cache = DataUsageCache::default();
    cache.info.name = "bucket".into();
    cache.info.source = Some(DataUsageCacheSource::new(0, 0));
    cache.info.scan_identity = Some(DataUsageScanIdentity {
        version: DATA_USAGE_SCAN_CHECKPOINT_VERSION,
        bucket_incarnation: uuid::Uuid::from_u128(9),
        set_layout: DataUsageScanPlanDigest([7; 32]),
        publication_epoch: 0,
        tier_registry_generation: 1,
        scan_mode: HealScanMode::Normal,
    });
    cache.replace("bucket", "", DataUsageEntry::default());
    for key in 0..COUNT {
        cache.replace(
            &format!("bucket/key-{key:08}"),
            "bucket",
            DataUsageEntry {
                objects: 1,
                size: key + 1,
                ..Default::default()
            },
        );
    }
    let revisions = DataUsageCache::read_revisions(store.clone(), NAME)
        .await
        .expect("initial revisions");
    cache
        .save_with_revisions_for_epoch(store.clone(), NAME, &revisions, 0, CancellationToken::new())
        .await
        .expect("seed both copies");
    (store, cache)
}

async fn corrupt_primary(store: &MemoryConfigStore) {
    let mut objects = store.objects.lock().await;
    let mut pages = 0;
    for (_, data) in objects.iter_mut().filter(|(path, _)| path.contains(".bin.pages/")) {
        data[0] ^= 1;
        pages += 1;
    }
    assert!(pages > 4, "fixture must span more than one physical read batch");
}

async fn finish(store: &Arc<MemoryConfigStore>, backup: bool) {
    finish_across_windows(store, backup, None).await;
}

async fn finish_across_windows(store: &Arc<MemoryConfigStore>, backup: bool, competing: Option<&Arc<MemoryConfigStore>>) {
    let mut loaded = DataUsageCache::default();
    let mut main_reads = 0;
    let mut backup_reads = 0;
    for _ in 0..128 {
        let (result, pages) = window(store, &mut loaded).await;
        for page in pages {
            if page.contains(".bkp.pages/") {
                backup_reads += 1;
            } else {
                main_reads += 1;
            }
        }
        if result.is_some() {
            let total = loaded.checked_flatten("bucket").expect("restored bucket");
            assert_eq!(total.objects, COUNT);
            assert_eq!(total.size, COUNT * (COUNT + 1) / 2);
            if backup {
                assert_eq!(main_reads, 1, "read the corrupt primary once for this revision");
                assert!(backup_reads > 4, "the independent backup must be restored across admissions");
            } else {
                assert!(main_reads > 4);
                assert_eq!(backup_reads, 0);
            }
            return;
        }
        if let Some(other) = competing {
            tokio::time::advance(Duration::from_secs(2 * 60 * 60)).await;
            let _ = window(other, &mut DataUsageCache::default()).await;
        }
    }
    panic!("checkpoint did not converge: {main_reads} primary and {backup_reads} backup page reads");
}

#[tokio::test]
async fn serial_backend_restores_one_verified_page_per_deadline() {
    let (store, _) = fixture().await;
    finish(&store, false).await;
}

#[tokio::test]
async fn concurrent_cancellations_keep_the_longer_verified_prefix() {
    let (store, _) = fixture().await;
    let longer = Arc::new(ReadWindow {
        pages: 3,
        device: Arc::new(Semaphore::new(1)),
        completed: Default::default(),
        deadline: Notify::new(),
    });
    let mut first = DataUsageCache::default();
    let mut read = Box::pin(READ_WINDOW.scope(longer.clone(), first.load(store.clone(), NAME)));
    tokio::select! {
        result = &mut read => panic!("checkpoint unexpectedly finished within three reads: {result:?}"),
        () = longer.deadline.notified() => {}
    }
    let mut concurrent = DataUsageCache::default();
    assert!(window(&store, &mut concurrent).await.0.is_none());
    drop(read);

    let (_, next) = window(&store, &mut concurrent).await;
    let previous = longer.completed.lock().expect("first reader history");
    assert_eq!(previous.len(), 3);
    assert!(
        !previous.contains(&next[0]),
        "a shorter concurrent cancellation displaced a verified prefix"
    );
}

#[tokio::test]
async fn corrupt_primary_yields_future_admissions_to_the_healthy_backup() {
    let (store, _) = fixture().await;
    corrupt_primary(&store).await;
    finish(&store, true).await;
}

#[tokio::test]
async fn new_primary_revision_supersedes_an_incomplete_backup_restore() {
    let (store, _) = fixture().await;
    corrupt_primary(&store).await;
    let mut loaded = DataUsageCache::default();
    assert!(window(&store, &mut loaded).await.0.is_none());
    let (result, pages) = window(&store, &mut loaded).await;
    assert!(result.is_none());
    assert!(pages[0].contains(".bkp.pages/"), "second admission should advance backup");

    let mut new_primary = DataUsageCache::default();
    new_primary.info.name = "bucket".into();
    new_primary.replace(
        "bucket",
        "",
        DataUsageEntry {
            objects: 1,
            size: 19,
            ..Default::default()
        },
    );
    let root = memory_config_key(RUSTFS_META_BUCKET, &format!("{}/{NAME}", crate::BUCKET_META_PREFIX));
    let bytes = rmp_serde::to_vec_named(&new_primary).expect("replacement main root");
    store.objects.lock().await.insert(root.clone(), bytes);
    *store.revisions.lock().await.get_mut(&root).expect("primary revision") += 1;
    let (result, pages) = window(&store, &mut loaded).await;
    assert!(result.is_some(), "must prefer the replacement main head immediately");
    assert!(pages.is_empty(), "must discard work on the older backup");
    let total = loaded.checked_flatten("bucket").expect("new primary root");
    assert_eq!((total.objects, total.size), (1, 19));
}

async fn inject_primary_page_error(store: &MemoryConfigStore, error: EcstoreError) {
    let errors = store
        .objects
        .lock()
        .await
        .keys()
        .filter(|key| key.contains(".bin.pages/"))
        .map(|key| (key.clone(), error.clone()))
        .collect();
    *store.read_errors.lock().await = errors;
}

#[tokio::test(start_paused = true)]
async fn a_transient_primary_page_error_does_not_schedule_the_backup() {
    let (store, _) = fixture().await;
    inject_primary_page_error(&store, EcstoreError::SlowDown).await;
    let mut loaded = DataUsageCache::default();
    assert!(window(&store, &mut loaded).await.0.is_none());
    store.read_errors.lock().await.clear();
    finish(&store, false).await;
}

#[tokio::test]
async fn backup_phase_is_bound_to_its_storage_instance() {
    let (damaged, _) = fixture().await;
    corrupt_primary(&damaged).await;
    assert!(window(&damaged, &mut DataUsageCache::default()).await.0.is_none());

    let (healthy, _) = fixture().await;
    let first = DataUsageCache::read_revisions(damaged.clone(), NAME)
        .await
        .expect("failed primary revision");
    let second = DataUsageCache::read_revisions(healthy.clone(), NAME)
        .await
        .expect("other storage revision");
    assert_eq!(first, second, "both stores must expose the same ETags");
    finish(&healthy, false).await;
}

#[tokio::test(start_paused = true)]
async fn idle_restore_keeps_verified_pages_until_another_bucket_needs_its_slot() {
    let (primary, _) = fixture().await;
    let (other, _) = fixture().await;
    finish_across_windows(&primary, false, Some(&other)).await;
}

#[tokio::test(start_paused = true)]
async fn idle_restore_keeps_the_failed_primary_revision_across_scheduled_rounds() {
    let (primary, _) = fixture().await;
    let (other, _) = fixture().await;
    corrupt_primary(&primary).await;
    corrupt_primary(&other).await;
    finish_across_windows(&primary, true, Some(&other)).await;
}

#[tokio::test(start_paused = true)]
async fn physical_primary_page_corruption_falls_back_to_the_healthy_copy() {
    let (store, _) = fixture().await;
    inject_primary_page_error(&store, EcstoreError::FileCorrupt).await;
    finish(&store, true).await;
}
