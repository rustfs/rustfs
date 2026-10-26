// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;

const PAYLOAD_BYTES: &str = "rustfs_scanner_cache_save_payload_bytes_total";
const ENCODED_BYTES: &str = "rustfs_scanner_cache_serialized_bytes_total";

#[derive(Default)]
struct SaveRecorder(StdMutex<HashMap<metrics::Key, Arc<AtomicU64>>>);

impl metrics::Recorder for SaveRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}

    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if ![PAYLOAD_BYTES, ENCODED_BYTES].contains(&key.name()) {
            return metrics::Counter::noop();
        }
        metrics::Counter::from_arc(self.0.lock().expect("save recorder").entry(key.clone()).or_default().clone())
    }

    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }

    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

async fn check_save(store: BackupFallbackStore, revisions: Option<DataUsageCacheRevisions>, attempts: [usize; 2], success: bool) {
    let store = Arc::new(store);
    let cache = DataUsageCache::default();
    let encoded = rmp_serde::to_vec(&cache).expect("independent encoded body");
    let recorder = SaveRecorder::default();
    let save = async {
        match revisions {
            Some(revisions) => {
                cache
                    .save_with_revisions(store.clone(), DATA_USAGE_CACHE_NAME, &revisions)
                    .await
            }
            None => cache.save(store.clone(), DATA_USAGE_CACHE_NAME).await,
        }
    };
    let mut save = std::pin::pin!(save);
    let result = std::future::poll_fn(|cx| metrics::with_local_recorder(&recorder, || save.as_mut().poll(cx))).await;
    assert_eq!(result.is_ok(), success, "save result: {result:?}");

    let payloads = store.payloads.lock().await;
    for (path, payload) in payloads.iter() {
        assert!(*path < 2);
        assert_eq!(payload, &encoded, "every retry and copy must retain the encoded body");
    }
    let rows = recorder.0.lock().expect("save recorder");
    assert_eq!(
        rows[&metrics::Key::from_name(ENCODED_BYTES)].load(Ordering::Relaxed),
        u64::try_from(encoded.len()).expect("encoded length"),
        "one encoding per save, regardless of copy/retry count"
    );
    for (index, path) in ["main", "backup"].into_iter().enumerate() {
        assert_eq!(store.put_attempts[index].load(Ordering::SeqCst), attempts[index]);
        assert_eq!(payloads.iter().filter(|(slot, _)| *slot == index).count(), attempts[index]);
        let key = metrics::Key::from_parts(PAYLOAD_BYTES, vec![metrics::Label::new("cache", path)]);
        let actual = rows.get(&key).map(|value| value.load(Ordering::Relaxed));
        let consumed: usize = payloads
            .iter()
            .filter(|(slot, _)| *slot == index)
            .map(|(_, body)| body.len())
            .sum();
        assert_eq!(
            actual,
            (attempts[index] > 0).then(|| u64::try_from(consumed).expect("consumed payload bytes")),
            "missing or incorrect payload observation for {path}"
        );
    }
    assert_eq!(
        rows.len(),
        1 + attempts.into_iter().filter(|count| *count > 0).count(),
        "fixed labels only"
    );
}

fn revisions(backup: bool) -> DataUsageCacheRevisions {
    DataUsageCacheRevisions {
        main: DataUsageCacheRevision::Missing,
        backup: backup.then_some(DataUsageCacheRevision::Missing),
    }
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_counts_main_and_backup_without_revisions() {
    check_save(BackupFallbackStore::new(Vec::new(), false), None, [1, 1], true).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_counts_each_retry_with_revisions() {
    let mut store = BackupFallbackStore::new(Vec::new(), false);
    store.main_put_failures = 2;
    check_save(store, Some(revisions(true)), [3, 1], true).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_excludes_skipped_backup() {
    check_save(BackupFallbackStore::new(Vec::new(), false), Some(revisions(false)), [1, 0], true).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_excludes_rejected_publication() {
    let mut store = BackupFallbackStore::new(Vec::new(), false);
    store.publication_admitted = false;
    check_save(store, Some(revisions(true)), [0, 0], false).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_retains_failed_primary_attempts() {
    let mut store = BackupFallbackStore::new(Vec::new(), false);
    store.main_put_failures = usize::MAX;
    check_save(store, Some(revisions(true)), [3, 0], false).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_does_not_count_reconciliation_reads() {
    let encoded = rmp_serde::to_vec(&DataUsageCache::default()).expect("committed body");
    let mut store = BackupFallbackStore::with_recovered_main(Vec::new(), encoded);
    store.main_put_failures = usize::MAX;
    check_save(store, Some(revisions(true)), [3, 1], true).await;
}

#[tokio::test(start_paused = true)]
async fn save_payload_bytes_retains_timed_out_backup_attempt() {
    let mut store = BackupFallbackStore::new(Vec::new(), false);
    store.put_delays[1] = Duration::from_secs(86400);
    check_save(store, Some(revisions(true)), [1, 1], true).await;
}
