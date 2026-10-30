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
    recorder: &impl metrics::Recorder,
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

#[derive(Clone, Debug)]
struct ExportedCounter {
    name: String,
    labels: Vec<(String, String)>,
    value: u64,
}

#[derive(Clone, Debug, Default)]
struct CounterExporter(Arc<Mutex<Vec<ExportedCounter>>>);

impl opentelemetry_sdk::metrics::exporter::PushMetricExporter for CounterExporter {
    async fn export(
        &self,
        resource: &opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        use opentelemetry_proto::tonic::{
            collector::metrics::v1::ExportMetricsServiceRequest,
            common::v1::any_value,
            metrics::v1::{AggregationTemporality, metric, number_data_point},
        };
        use prost::Message;

        // Use the same SDK-to-OTLP conversion as the production HTTP exporter.
        let encoded = ExportMetricsServiceRequest::from(resource).encode_to_vec();
        let decoded = ExportMetricsServiceRequest::decode(encoded.as_slice()).expect("decode exported OTLP protobuf");
        let mut observed = self.0.lock().expect("lock exported counters");
        observed.clear();
        for resource in decoded.resource_metrics {
            for scope in resource.scope_metrics {
                for metric in scope.metrics {
                    if !matches!(
                        metric.name.as_str(),
                        TOTAL | ATTRIBUTED | SUCCESSES | "rustfs_scanner_zero_export_test_beacon_total"
                    ) {
                        continue;
                    }
                    let Some(metric::Data::Sum(sum)) = metric.data else {
                        panic!("counter exported with non-sum type");
                    };
                    assert!(sum.is_monotonic);
                    assert_eq!(
                        AggregationTemporality::try_from(sum.aggregation_temporality).expect("known counter temporality"),
                        AggregationTemporality::Cumulative
                    );
                    for point in sum.data_points {
                        assert!(point.start_time_unix_nano > 0);
                        assert!(point.time_unix_nano >= point.start_time_unix_nano);
                        assert_eq!(point.flags, 0);
                        let Some(number_data_point::Value::AsInt(value)) = point.value else {
                            panic!("counter point has no integer value");
                        };
                        let labels = point
                            .attributes
                            .into_iter()
                            .map(|attr| {
                                let Some(any_value::Value::StringValue(value)) = attr.value.and_then(|value| value.value) else {
                                    panic!("counter label is not a string");
                                };
                                (attr.key, value)
                            })
                            .collect();
                        observed.push(ExportedCounter {
                            name: metric.name.clone(),
                            labels,
                            value: u64::try_from(value).expect("counter value must be nonnegative"),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn shutdown_with_timeout(&self, _timeout: std::time::Duration) -> opentelemetry_sdk::error::OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> opentelemetry_sdk::metrics::Temporality {
        opentelemetry_sdk::metrics::Temporality::Cumulative
    }
}

#[tokio::test]
#[serial]
async fn configured_zero_total_is_exported_without_manufacturing_unconfigured_points() {
    use opentelemetry_sdk::metrics::PeriodicReader;

    let (scanner, _retained_temp_dir) = build_test_scanner().await;
    let disk = scanner.local_disk.clone();
    let disk_label = disk.path().to_string_lossy().into_owned();
    let plan = test_scope(PLAN).expect("valid scope");
    for (bucket, scope, configured) in [
        ("bucket", Some(plan.clone()), true),
        ("bucket", None, false),
        ("other-bucket", Some(plan.clone()), false),
    ] {
        let exporter = CounterExporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(std::time::Duration::from_secs(3600))
            .build();
        let (provider, recorder) = rustfs_obs::Recorder::builder("scanner-zero-export-test")
            .with_meter_provider(|builder| builder.with_reader(reader))
            .build();
        assert!(matches!(
            run_scan(disk.clone(), bucket, true, scope, &recorder).await,
            Err(ScannerError::Other(reason)) if reason == "Operation cancelled"
        ));
        metrics::with_local_recorder(&recorder, || {
            metrics::counter!("rustfs_scanner_zero_export_test_beacon_total").increment(1);
        });
        provider.force_flush().expect("export scanner counters");
        let rows = exporter.0.lock().expect("lock exported counters").clone();
        assert!(
            rows.iter()
                .any(|row| row.name == "rustfs_scanner_zero_export_test_beacon_total" && row.value == 1)
        );
        let has_label = |row: &ExportedCounter, key: &str, value: &str| {
            row.labels.iter().any(|(label, actual)| label == key && actual == value)
        };
        let totals: Vec<_> = rows
            .iter()
            .filter(|row| row.name == TOTAL && has_label(row, "bucket", bucket) && has_label(row, "disk", &disk_label))
            .collect();
        assert_eq!(totals.len(), usize::from(configured), "total presence requires matching diagnostic scope");
        if configured {
            assert_eq!(totals[0].value, 0);
            assert_eq!(totals[0].labels.len(), 2);
        }
        for name in [ATTRIBUTED, SUCCESSES] {
            for class in ["hot", "cold", "other", "ambiguous"] {
                let points: Vec<_> = rows
                    .iter()
                    .filter(|row| {
                        row.name == name
                            && has_label(row, "disk", &disk_label)
                            && has_label(row, "scope", plan.id())
                            && has_label(row, "class", class)
                    })
                    .collect();
                assert_eq!(points.len(), usize::from(configured), "attributed series presence");
                if configured {
                    assert_eq!(points[0].value, 0);
                    assert_eq!(points[0].labels.len(), 3);
                }
            }
        }
        provider.shutdown().expect("stop test exporter");
    }
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
    for name in [ATTRIBUTED, SUCCESSES] {
        assert_eq!(rows[&attributed(name, &disk_label, plan.id(), "ambiguous")].load(Ordering::Relaxed), 4);
        for class in ["hot", "cold", "other"] {
            assert_eq!(rows[&attributed(name, &disk_label, plan.id(), class)].load(Ordering::Relaxed), 0);
        }
    }
}
