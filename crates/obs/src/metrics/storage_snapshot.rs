// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Current storage snapshots, without cumulative SDK retention of removed series.

use super::report::{PrometheusMetric, counter_value_from_f64};
use super::schema::MetricType;
use opentelemetry::{KeyValue, metrics::Meter};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub(crate) const COLLECTION_SCOPE: &str = "collection_scope";
pub(crate) const OBSERVER: &str = "observer";
pub(crate) const LAST_SUCCESS: &str = "rustfs_storage_snapshot_last_success_timestamp_seconds";
pub(crate) const MAX_AGE: &str = "rustfs_storage_snapshot_max_age_seconds";

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    #[error("storage snapshots only support finite gauges and nonnegative counters")]
    InvalidValue,
    #[error("storage snapshot metric changed its instrument type")]
    ChangedType,
}

#[derive(Clone)]
struct Point {
    value: f64,
    counter: Option<u64>,
    attributes: Vec<KeyValue>,
}

#[derive(Default)]
struct Snapshot {
    points: HashMap<String, Vec<Point>>,
    collected_at: Option<Instant>,
    timestamp_seconds: f64,
    remaining_age: Duration,
}

/// One collector owns each instance. Export callbacks only read its published memory.
pub(crate) struct StorageSnapshotMetrics {
    meter: Meter,
    snapshot: Arc<RwLock<Snapshot>>,
    instruments: HashMap<String, MetricType>,
    labels: Vec<KeyValue>,
    max_age: Duration,
}

impl StorageSnapshotMetrics {
    pub(crate) fn new(meter: Meter, scope: &'static str, observer: String, max_age: Duration) -> Self {
        let snapshot = Arc::new(RwLock::new(Snapshot::default()));
        let labels = vec![KeyValue::new(COLLECTION_SCOPE, scope), KeyValue::new(OBSERVER, observer)];
        let weak = Arc::downgrade(&snapshot);
        let timestamp_labels = labels.clone();
        meter
            .f64_observable_gauge(LAST_SUCCESS)
            .with_description("Unix timestamp of the last successful storage snapshot collection")
            .with_callback(move |observer| {
                let Some(snapshot) = weak.upgrade() else { return };
                let Ok(snapshot) = snapshot.read() else { return };
                if snapshot.collected_at.is_some() {
                    observer.observe(snapshot.timestamp_seconds, &timestamp_labels);
                }
            })
            .build();
        let weak = Arc::downgrade(&snapshot);
        let age_labels = labels.clone();
        meter
            .f64_observable_gauge(MAX_AGE)
            .with_description("Validity budget in seconds remaining after storage snapshot collection completed")
            .with_callback(move |observer| {
                let Some(snapshot) = weak.upgrade() else { return };
                let Ok(snapshot) = snapshot.read() else { return };
                if snapshot.collected_at.is_some() {
                    observer.observe(snapshot.remaining_age.as_secs_f64(), &age_labels);
                }
            })
            .build();
        Self {
            meter,
            snapshot,
            instruments: HashMap::new(),
            labels,
            max_age,
        }
    }

    #[cfg(test)]
    fn replace(&mut self, metrics: Vec<PrometheusMetric>) -> Result<(), SnapshotError> {
        self.replace_collected(metrics, Instant::now())
    }

    pub(crate) fn replace_collected(
        &mut self,
        metrics: Vec<PrometheusMetric>,
        collection_started: Instant,
    ) -> Result<(), SnapshotError> {
        // Validate the entire update before changing membership or publishing any values.
        let mut types = self.instruments.clone();
        for metric in &metrics {
            match metric.metric_type {
                MetricType::Gauge if metric.value.is_finite() => (),
                MetricType::Counter if counter_value_from_f64(metric.value).is_some() => (),
                _ => return Err(SnapshotError::InvalidValue),
            }
            if types
                .insert(metric.name.to_string(), metric.metric_type)
                .is_some_and(|kind| kind != metric.metric_type)
            {
                return Err(SnapshotError::ChangedType);
            }
        }

        let mut points: HashMap<String, Vec<Point>> = HashMap::new();
        for metric in metrics {
            let name = metric.name.to_string();
            if !self.instruments.contains_key(&name) {
                self.register(&name, metric.metric_type, metric.help.as_ref())?;
                self.instruments.insert(name.clone(), metric.metric_type);
            }
            let mut attributes = self.labels.clone();
            attributes.extend(
                metric
                    .labels
                    .iter()
                    .map(|(key, value)| KeyValue::new(*key, value.to_string())),
            );
            let point = Point {
                value: metric.value,
                counter: (metric.metric_type == MetricType::Counter)
                    .then(|| counter_value_from_f64(metric.value))
                    .flatten(),
                attributes,
            };
            points.entry(name).or_default().push(point);
        }
        let timestamp_seconds = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs_f64();
        let mut snapshot = self.snapshot.write().unwrap_or_else(|poisoned| poisoned.into_inner());
        *snapshot = Snapshot {
            points,
            collected_at: Some(collection_started),
            timestamp_seconds,
            // The publication timestamp withdraws cached fields from the previous
            // snapshot. Deduct collection time separately so a slow RPC or usage
            // read cannot make an old observation fresh merely by completing.
            remaining_age: self.max_age.saturating_sub(collection_started.elapsed()),
        };
        Ok(())
    }

    fn register(&self, name: &str, kind: MetricType, description: &str) -> Result<(), SnapshotError> {
        let weak = Arc::downgrade(&self.snapshot);
        let name_owned = name.to_string();
        let max_age = self.max_age;
        match kind {
            MetricType::Counter => {
                self.meter
                    .u64_observable_counter(name.to_string())
                    .with_description(description.to_string())
                    .with_callback(move |observer| {
                        let Some(snapshot) = weak.upgrade() else { return };
                        let Ok(snapshot) = snapshot.read() else { return };
                        if snapshot.collected_at.is_none_or(|when| when.elapsed() > max_age) {
                            return;
                        }
                        if let Some(points) = snapshot.points.get(&name_owned) {
                            for point in points {
                                if let Some(value) = point.counter {
                                    observer.observe(value, &point.attributes);
                                }
                            }
                        }
                    })
                    .build();
            }
            MetricType::Gauge => {
                self.meter
                    .f64_observable_gauge(name.to_string())
                    .with_description(description.to_string())
                    .with_callback(move |observer| {
                        let Some(snapshot) = weak.upgrade() else { return };
                        let Ok(snapshot) = snapshot.read() else { return };
                        if snapshot.collected_at.is_none_or(|when| when.elapsed() > max_age) {
                            return;
                        }
                        if let Some(points) = snapshot.points.get(&name_owned) {
                            for point in points {
                                observer.observe(point.value, &point.attributes);
                            }
                        }
                    })
                    .build();
            }
            MetricType::Histogram => return Err(SnapshotError::InvalidValue),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::error::OTelSdkResult;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
    use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
    use std::sync::Mutex;

    #[derive(Clone, Debug)]
    struct ExportedPoint {
        name: String,
        value: f64,
        attributes: Vec<KeyValue>,
    }

    #[derive(Clone, Debug, Default)]
    struct Exporter(Arc<Mutex<Vec<ExportedPoint>>>);

    impl PushMetricExporter for Exporter {
        async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
            let mut exported = self.0.lock().unwrap();
            exported.clear();
            for metric in metrics.scope_metrics().flat_map(|scope| scope.metrics()) {
                match metric.data() {
                    AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            exported.push(ExportedPoint {
                                name: metric.name().to_string(),
                                value: point.value(),
                                attributes: point.attributes().cloned().collect(),
                            });
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        assert!(sum.is_monotonic());
                        for point in sum.data_points() {
                            exported.push(ExportedPoint {
                                name: metric.name().to_string(),
                                value: point.value() as f64,
                                attributes: point.attributes().cloned().collect(),
                            });
                        }
                    }
                    data => panic!("unexpected storage aggregation: {data:?}"),
                }
            }
            Ok(())
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }
        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }
        fn temporality(&self) -> Temporality {
            Temporality::Cumulative
        }
    }

    fn setup() -> (SdkMeterProvider, Exporter, StorageSnapshotMetrics) {
        let exporter = Exporter::default();
        let reader = PeriodicReader::builder(exporter.clone())
            .with_interval(Duration::from_secs(3600))
            .build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let snapshot =
            StorageSnapshotMetrics::new(provider.meter("storage-test"), "local", "node1:9000".into(), Duration::from_secs(30));
        (provider, exporter, snapshot)
    }

    fn drive(name: &'static str, kind: MetricType, id: &'static str, value: f64) -> PrometheusMetric {
        PrometheusMetric::new(name, kind, "test", value)
            .with_label("server", "node1:9000")
            .with_label("drive", "/data")
            .with_label("disk_id", id)
    }

    fn flush(provider: &SdkMeterProvider, exporter: &Exporter) -> Vec<ExportedPoint> {
        // The SDK skips export entirely when no callback observed any point.
        exporter.0.lock().unwrap().clear();
        provider.force_flush().unwrap();
        exporter.0.lock().unwrap().clone()
    }

    #[test]
    fn actual_sdk_drops_removed_gauges_counters_and_replaced_disk_identity() {
        let (provider, exporter, mut snapshot) = setup();
        snapshot
            .replace(vec![
                drive("test_info", MetricType::Gauge, "old", 1.0),
                drive("test_calls", MetricType::Counter, "old", 7.0),
            ])
            .unwrap();
        let first = flush(&provider, &exporter);
        assert!(first.iter().any(|point| point.name == "test_calls" && point.value == 7.0));
        snapshot
            .replace(vec![drive("test_info", MetricType::Gauge, "new", 1.0)])
            .unwrap();
        let second = flush(&provider, &exporter);
        assert!(!second.iter().any(|point| point.name == "test_calls"));
        let info = second.iter().filter(|point| point.name == "test_info").collect::<Vec<_>>();
        assert_eq!(info.len(), 1);
        assert!(info[0].attributes.contains(&KeyValue::new("disk_id", "new")));
        assert!(info[0].attributes.contains(&KeyValue::new(COLLECTION_SCOPE, "local")));
        assert!(info[0].attributes.contains(&KeyValue::new(OBSERVER, "node1:9000")));
        snapshot.replace(Vec::new()).unwrap();
        assert!(
            flush(&provider, &exporter)
                .iter()
                .all(|point| point.name == LAST_SUCCESS || point.name == MAX_AGE)
        );
        provider.shutdown().unwrap();
    }

    #[test]
    fn actual_sdk_preserves_counter_resets_instead_of_accumulating_old_absolute_values() {
        let (provider, exporter, mut snapshot) = setup();
        for value in [7.0, 2.0, 5.0] {
            snapshot
                .replace(vec![drive("test_calls", MetricType::Counter, "disk", value)])
                .unwrap();
            assert_eq!(
                flush(&provider, &exporter)
                    .iter()
                    .find(|point| point.name == "test_calls")
                    .unwrap()
                    .value,
                value
            );
        }
        provider.shutdown().unwrap();
    }

    #[test]
    fn failed_or_stalled_collection_cannot_refresh_last_success_or_export_old_points() {
        let (provider, exporter, mut snapshot) = setup();
        snapshot
            .replace(vec![drive("test_health", MetricType::Gauge, "disk", 1.0)])
            .unwrap();
        let first = flush(&provider, &exporter);
        let timestamp = first.iter().find(|point| point.name == LAST_SUCCESS).unwrap().value;
        assert!(
            snapshot
                .replace(vec![drive("test_health", MetricType::Gauge, "disk", f64::NAN)])
                .is_err()
        );
        assert!(
            snapshot
                .replace(vec![drive("test_health", MetricType::Counter, "disk", 3.0)])
                .is_err()
        );
        assert!(
            snapshot
                .replace(vec![
                    drive("new_name", MetricType::Gauge, "disk", 1.0),
                    drive("new_name", MetricType::Counter, "disk", 1.0),
                ])
                .is_err()
        );
        let second = flush(&provider, &exporter);
        assert_eq!(second.iter().find(|point| point.name == LAST_SUCCESS).unwrap().value, timestamp);
        assert!(second.iter().any(|point| point.name == "test_health" && point.value == 1.0));
        snapshot.snapshot.write().unwrap().collected_at = Some(Instant::now() - Duration::from_secs(31));
        let expired = flush(&provider, &exporter);
        assert!(!expired.iter().any(|point| point.name == "test_health"));
        assert_eq!(expired.iter().find(|point| point.name == LAST_SUCCESS).unwrap().value, timestamp);
        drop(snapshot);
        assert!(flush(&provider, &exporter).is_empty());
        provider.shutdown().unwrap();
    }

    #[test]
    fn a_slow_successful_collection_cannot_publish_an_already_expired_observation() {
        let (provider, exporter, mut snapshot) = setup();
        snapshot
            .replace_collected(
                vec![drive("test_health", MetricType::Gauge, "disk", 1.0)],
                Instant::now() - Duration::from_secs(31),
            )
            .expect("publish a completed but slow snapshot");
        let points = flush(&provider, &exporter);
        assert!(!points.iter().any(|point| point.name == "test_health"));
        assert_eq!(
            points
                .iter()
                .find(|point| point.name == MAX_AGE)
                .expect("validity budget")
                .value,
            0.0
        );
        provider.shutdown().expect("shutdown test provider");
    }
}
