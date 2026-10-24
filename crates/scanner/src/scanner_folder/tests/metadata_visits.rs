// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::sync::atomic::AtomicU64;

const VISITS: &str = "rustfs_scanner_object_metadata_visits_total";

pub(super) fn observe(scanner: &mut FolderScanner) -> Arc<AtomicU64> {
    let count = Arc::new(AtomicU64::new(0));
    scanner.object_metadata_visits = metrics::Counter::from_arc(count.clone());
    count
}

#[derive(Default)]
struct VisitRecorder(Mutex<HashMap<metrics::Key, Arc<AtomicU64>>>);

impl metrics::Recorder for VisitRecorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if key.name() != VISITS {
            return metrics::Counter::noop();
        }
        metrics::Counter::from_arc(self.0.lock().expect("visit recorder").entry(key.clone()).or_default().clone())
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

#[tokio::test]
#[serial]
async fn real_scan_registers_bucket_disk_counter_and_accumulates_visits() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "first").await;
    write_test_object_metadata(&temp_dir, "bucket", "second").await;
    let recorder = VisitRecorder::default();
    let disk = scanner.local_disk.clone();
    for (cancelled, expected) in [(true, 0), (false, 2), (false, 4)] {
        let parent = CancellationToken::new();
        if cancelled {
            parent.cancel();
        }
        let budget = ScannerCycleBudget::new(&parent, Default::default());
        let cache = DataUsageCache {
            info: crate::data_usage_define::DataUsageCacheInfo {
                name: "bucket".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let scan = scan_data_folder(
            parent,
            budget,
            vec![disk.clone()],
            disk.clone(),
            cache,
            None,
            HealScanMode::Normal,
            DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest),
        );
        let mut scan = std::pin::pin!(scan);
        let result = std::future::poll_fn(|cx| metrics::with_local_recorder(&recorder, || scan.as_mut().poll(cx))).await;
        if cancelled {
            assert!(result.is_err(), "cancelled scan must still fail");
        } else {
            result.expect("real bucket scanner should finish");
        }
        let rows = recorder.0.lock().expect("visit recorder");
        let key = metrics::Key::from_parts(
            VISITS,
            vec![
                metrics::Label::new("bucket", "bucket"),
                metrics::Label::new("disk", disk.path().to_string_lossy().into_owned()),
            ],
        );
        assert_eq!(rows.len(), 1, "only bucket/disk identity, no object labels");
        assert_eq!(rows[&key].load(Ordering::Relaxed), expected);
    }
}
