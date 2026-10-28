// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::super::prefix_attribution::{TEST_SCOPE, test_scope};
use super::*;
use std::sync::atomic::AtomicU64;

const TOTAL: &str = "rustfs_scanner_object_metadata_visits_total";
const ATTRIBUTED: &str = "rustfs_scanner_object_metadata_prefix_visits_total";
const SUCCESSES: &str = "rustfs_scanner_object_metadata_prefix_successes_total";
const PLAN: &str = r#"{"bucket":"bucket","hot_prefix":"hot/","cold_prefix":"cold/"}"#;

#[derive(Default)]
struct Recorder(Mutex<HashMap<metrics::Key, Arc<AtomicU64>>>);

impl metrics::Recorder for Recorder {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        if !matches!(key.name(), TOTAL | ATTRIBUTED | SUCCESSES) {
            return metrics::Counter::noop();
        }
        metrics::Counter::from_arc(self.0.lock().expect("recorder").entry(key.clone()).or_default().clone())
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

fn attributed(name: &'static str, disk: &str, id: &str, class: &'static str) -> metrics::Key {
    metrics::Key::from_parts(
        name,
        vec![
            metrics::Label::new("disk", disk.to_owned()),
            metrics::Label::new("class", class),
            metrics::Label::new("scope", id.to_owned()),
        ],
    )
}

async fn run_scan(
    disk: Arc<Disk>,
    bucket: &str,
    cancelled: bool,
    configured: Option<Arc<super::super::prefix_attribution::Scope>>,
    recorder: &Recorder,
) -> Result<DataUsageCache, ScannerError> {
    let parent = CancellationToken::new();
    if cancelled {
        parent.cancel();
    }
    let budget = ScannerCycleBudget::new(&parent, Default::default());
    let cache = DataUsageCache {
        info: crate::data_usage_define::DataUsageCacheInfo {
            name: bucket.to_string(),
            ..Default::default()
        },
        ..Default::default()
    };
    TEST_SCOPE
        .scope(configured, async {
            let scan = scan_data_folder(
                parent,
                budget,
                vec![disk.clone()],
                disk,
                cache,
                None,
                HealScanMode::Normal,
                DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest),
            );
            let mut scan = std::pin::pin!(scan);
            std::future::poll_fn(|cx| metrics::with_local_recorder(recorder, || scan.as_mut().poll(cx))).await
        })
        .await
}

#[tokio::test]
#[serial]
async fn actual_metadata_attempts_split_same_bucket_without_prefix_labels() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    for name in ["hot/object", "cold/object", "hotter/object"] {
        write_test_object_metadata(&temp_dir, "bucket", name).await;
    }
    let disk = scanner.local_disk.clone();
    let disk_label = disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    let recorder = Recorder::default();
    for expected in [1, 2] {
        run_scan(disk.clone(), "bucket", false, Some(plan.clone()), &recorder)
            .await
            .expect("actual bucket scan");
        let rows = recorder.0.lock().expect("recorder");
        assert_eq!(rows.len(), 9, "one existing total plus four classes for attempts and successes");
        let total = metrics::Key::from_parts(
            TOTAL,
            vec![
                metrics::Label::new("bucket", "bucket"),
                metrics::Label::new("disk", disk_label.clone()),
            ],
        );
        assert_eq!(rows[&total].load(Ordering::Relaxed), expected * 3);
        for class in ["hot", "cold", "other"] {
            assert_eq!(
                rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), class)].load(Ordering::Relaxed),
                expected
            );
            assert_eq!(
                rows[&attributed(SUCCESSES, &disk_label, plan.id(), class)].load(Ordering::Relaxed),
                expected
            );
        }
        assert_eq!(
            rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "ambiguous")].load(Ordering::Relaxed),
            0
        );
    }
    assert!(
        run_scan(disk.clone(), "bucket", true, Some(plan.clone()), &recorder)
            .await
            .is_err()
    );
    let rows = recorder.0.lock().expect("recorder");
    for class in ["hot", "cold", "other"] {
        assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), class)].load(Ordering::Relaxed), 2);
    }
}

#[tokio::test]
#[serial]
async fn disabled_or_wrong_bucket_produces_no_attributed_series() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "hot/object").await;
    write_test_object_metadata(&temp_dir, "other-bucket", "hot/object").await;
    let disk = scanner.local_disk.clone();
    let recorder = Recorder::default();
    run_scan(disk.clone(), "bucket", false, None, &recorder)
        .await
        .expect("disabled scan");
    run_scan(disk, "other-bucket", false, test_scope(PLAN), &recorder)
        .await
        .expect("unselected bucket scan");
    assert!(
        recorder
            .0
            .lock()
            .expect("recorder")
            .keys()
            .all(|key| !matches!(key.name(), ATTRIBUTED | SUCCESSES))
    );
}

#[tokio::test]
#[serial]
async fn configured_absent_class_is_a_registered_zero() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "hot/object").await;
    let disk = scanner.local_disk.clone();
    let disk_label = disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    let recorder = Recorder::default();
    run_scan(disk, "bucket", false, Some(plan.clone()), &recorder)
        .await
        .expect("actual bucket scan");
    let rows = recorder.0.lock().expect("recorder");
    assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "hot")].load(Ordering::Relaxed), 1);
    assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "cold")].load(Ordering::Relaxed), 0);
    assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "other")].load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[serial]
async fn failed_metadata_attempt_is_not_reported_as_success() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "hot/object").await;
    write_test_object_metadata_bytes(&temp_dir, "bucket", "cold/object", b"invalid-metadata").await;
    let disk = scanner.local_disk.clone();
    let disk_label = disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    let recorder = Recorder::default();
    assert!(matches!(
        run_scan(disk, "bucket", false, Some(plan.clone()), &recorder).await,
        Err(ScannerError::PartialCache(_))
    ));
    let rows = recorder.0.lock().expect("recorder");
    assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "hot")].load(Ordering::Relaxed), 1);
    assert_eq!(rows[&attributed(SUCCESSES, &disk_label, plan.id(), "hot")].load(Ordering::Relaxed), 1);
    assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "cold")].load(Ordering::Relaxed), 1);
    assert_eq!(rows[&attributed(SUCCESSES, &disk_label, plan.id(), "cold")].load(Ordering::Relaxed), 0);
}

#[tokio::test]
#[serial]
async fn closed_node_admission_does_not_record_prefix_attempts() {
    use rustfs_concurrency::adaptive::NodeAdmission;
    use std::num::NonZeroU16;

    let (mut scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    write_test_object_metadata(&temp_dir, "bucket", "cold/object").await;
    let disk_label = scanner.local_disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    let recorder = Recorder::default();
    let gate = NodeAdmission::new(NonZeroU16::new(1).expect("one unit"));
    scanner.node_admission = Some(gate.clone());
    scanner.sleeper = DynamicSleeper::new(rustfs_config::ScannerSpeed::Fastest);
    TEST_SCOPE
        .scope(Some(plan.clone()), async {
            scanner.prefix_attribution = metrics::with_local_recorder(&recorder, || {
                super::super::prefix_attribution::configured_for("bucket", &disk_label)
            });
            let result = scanner
                .scan_folder(
                    CancellationToken::new(),
                    CachedFolder {
                        name: "bucket".to_string(),
                        parent: None,
                        object_heal_prob_div: 1,
                    },
                    &mut DataUsageEntry::default(),
                )
                .await;
            assert!(matches!(result, Err(ScannerError::ResourceDeferred)));
        })
        .await;
    let rows = recorder.0.lock().expect("recorder");
    for name in [ATTRIBUTED, SUCCESSES] {
        for class in ["hot", "cold", "other", "ambiguous"] {
            assert_eq!(rows[&attributed(name, &disk_label, plan.id(), class)].load(Ordering::Relaxed), 0);
        }
    }
    assert_eq!(gate.snapshot().active, 0);
    assert_eq!(gate.completed(), Some(0));
}

#[tokio::test]
#[serial]
async fn encoded_directory_paths_are_not_attributed_to_a_raw_prefix() {
    let (scanner, temp_dir) = build_test_scanner().await;
    let _guard = TestGuard {
        temp_dir: Some(temp_dir.clone()),
    };
    for (stored, logical) in [
        ("hot__XLDIR__", "hot/"),
        ("cold__XLDIR__", "cold/"),
        ("hot/inner__XLDIR__", "hot/inner/"),
        ("hot__XLDIR____XLDIR__", "hot__XLDIR__/"),
    ] {
        write_test_object_metadata_bytes(&temp_dir, "bucket", stored, &metadata_for_object("bucket", logical)).await;
    }
    let disk = scanner.local_disk.clone();
    let disk_label = disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    let recorder = Recorder::default();
    run_scan(disk, "bucket", false, Some(plan.clone()), &recorder)
        .await
        .expect("scan encoded metadata paths");
    let rows = recorder.0.lock().expect("recorder");
    assert_eq!(
        rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), "ambiguous")].load(Ordering::Relaxed),
        4
    );
    for class in ["hot", "cold", "other"] {
        assert_eq!(rows[&attributed(ATTRIBUTED, &disk_label, plan.id(), class)].load(Ordering::Relaxed), 0);
    }
}
