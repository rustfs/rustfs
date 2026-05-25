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

use crate::GlobalError;
use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};
use opentelemetry::{
    InstrumentationScope, InstrumentationScopeBuilder, KeyValue, global,
    metrics::{Meter, MeterProvider},
};
use opentelemetry_sdk::metrics::{MeterProviderBuilder, SdkMeterProvider};
use std::{
    borrow::Cow,
    collections::HashMap,
    ops::Deref,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};
use tracing::error;

macro_rules! configure_builder {
    ($builder:expr, $metadata:expr) => {{
        let mut builder = $builder;
        if let Some(metadata) = $metadata {
            if let Some(unit) = metadata.unit {
                builder = builder.with_unit(unit.as_canonical_label());
            }
            builder = builder.with_description(metadata.description.to_string());
        }
        builder
    }};
}

/// A builder for constructing a [`Recorder`].
#[derive(Debug)]
pub struct Builder {
    builder: MeterProviderBuilder,
    scope: InstrumentationScopeBuilder,
}

impl Builder {
    /// Runs the closure (`f`) to modify the [`MeterProviderBuilder`] to build a
    /// [`MeterProvider`](MeterProvider).
    pub fn with_meter_provider(mut self, f: impl FnOnce(MeterProviderBuilder) -> MeterProviderBuilder) -> Self {
        self.builder = f(self.builder);
        self
    }

    /// Modify the [`InstrumentationScope`] to provide additional metadata from the
    /// closure (`f`).
    pub fn with_instrumentation_scope(
        mut self,
        f: impl FnOnce(InstrumentationScopeBuilder) -> InstrumentationScopeBuilder,
    ) -> Self {
        self.scope = f(self.scope);
        self
    }

    /// Consumes the builder and builds a new [`Recorder`] and returns
    /// a [`SdkMeterProvider`].
    ///
    /// A [`SdkMeterProvider`] is provided so you have the responsibility to
    /// do whatever you need to do with it.
    ///
    /// This will not install the recorder as the global recorder for
    /// the [`metrics`] crate, use [`Builder::install`]. This will not install a meter
    /// provider to [`global`], use [`Builder::install_global`].
    pub fn build(self) -> (SdkMeterProvider, Recorder) {
        let provider = self.builder.build();
        let meter = provider.meter_with_scope(self.scope.build());

        (provider, Recorder::with_meter(meter))
    }

    /// Builds a [`Recorder`] and sets it as the global recorder for the [`metrics`]
    /// crate.
    ///
    /// This method will not call [`global::set_meter_provider`] for OpenTelemetry and
    /// will be returned as the first element in the return's type tuple.
    pub fn install(self) -> Result<(SdkMeterProvider, Recorder), GlobalError> {
        let (provider, recorder) = self.build();
        metrics::set_global_recorder(recorder.clone())?;

        Ok((provider, recorder))
    }

    /// Builds the [`Recorder`] to record metrics to OpenTelemetry, set the global
    /// recorder for the [`metrics`] crate, and calls [`global::set_meter_provider`]
    /// to set the constructed [`SdkMeterProvider`].
    pub fn install_global(self) -> Result<Recorder, GlobalError> {
        let (provider, recorder) = self.install()?;
        global::set_meter_provider(provider);

        Ok(recorder)
    }
}

#[derive(Debug)]
struct MetricMetadata {
    unit: Option<Unit>,
    description: SharedString,
}

/// A standard recorder that implements [`metrics::Recorder`].
///
/// This instance implements <code>[`Deref`]\<Target = [`Meter`]\></code>, so
/// you can still interact with the SDK's initialized [`Meter`] instance.
#[derive(Debug, Clone)]
pub struct Recorder {
    meter: Meter,
    metrics_metadata: Arc<Mutex<HashMap<KeyName, MetricMetadata>>>,
    // cache metric handlers as to not reregister on each call
    cached_counters: Arc<RwLock<HashMap<Key, Counter>>>,
    cached_gauges: Arc<RwLock<HashMap<Key, Gauge>>>,
    cached_histograms: Arc<RwLock<HashMap<Key, Histogram>>>,
    metric_snapshots: Arc<RwLock<HashMap<Key, MetricSnapshot>>>,
}

impl Recorder {
    /// Creates a new [`Builder`] with a given name for instrumentation.
    pub fn builder<S: Into<Cow<'static, str>>>(name: S) -> Builder {
        Builder {
            builder: MeterProviderBuilder::default(),
            scope: InstrumentationScope::builder(name.into()),
        }
    }

    /// Creates a [`Recorder`] with an already established [`Meter`].
    pub fn with_meter(meter: Meter) -> Self {
        Recorder {
            meter,
            metrics_metadata: Default::default(),
            cached_counters: Default::default(),
            cached_gauges: Default::default(),
            cached_histograms: Default::default(),
            metric_snapshots: Default::default(),
        }
    }

    fn get_cached_metric<T: Clone>(lock: &RwLock<HashMap<Key, T>>, key: &Key, metric_type: &str) -> Option<T> {
        let cache = match lock.read() {
            Ok(g) => g,
            Err(e) => {
                error!("{} cache read lock poisoned: {}", metric_type, e);
                e.into_inner()
            }
        };
        cache.get(key).cloned()
    }

    fn insert_cached_metric<T: Clone>(lock: &RwLock<HashMap<Key, T>>, key: Key, value: T, metric_type: &str) -> T {
        let mut cache = match lock.write() {
            Ok(g) => g,
            Err(e) => {
                error!("{} cache write lock poisoned: {}", metric_type, e);
                e.into_inner()
            }
        };

        if let Some(v) = cache.get(&key) {
            return v.clone();
        }
        cache.insert(key, value.clone());
        value
    }

    fn with_metadata_lock<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut HashMap<KeyName, MetricMetadata>) -> R,
    {
        let mut guard = self.metrics_metadata.lock().unwrap_or_else(|e| {
            error!("metrics_metadata lock poisoned: {}", e);
            e.into_inner()
        });
        f(&mut guard)
    }

    fn describe_metric(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.with_metadata_lock(|metadata| {
            metadata.insert(key, MetricMetadata { unit, description });
        });
    }

    fn get_metadata_for_builder(&self, key_name: &str) -> Option<MetricMetadata> {
        self.with_metadata_lock(|metadata| metadata.remove(key_name))
    }

    fn snapshot_write_lock(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<Key, MetricSnapshot>> {
        self.metric_snapshots.write().unwrap_or_else(|e| {
            error!("metric_snapshots write lock poisoned: {}", e);
            e.into_inner()
        })
    }

    fn snapshot_read_lock(&self) -> std::sync::RwLockReadGuard<'_, HashMap<Key, MetricSnapshot>> {
        self.metric_snapshots.read().unwrap_or_else(|e| {
            error!("metric_snapshots read lock poisoned: {}", e);
            e.into_inner()
        })
    }

    fn ensure_counter_snapshot(
        &self,
        key: &Key,
        value: Arc<AtomicU64>,
        description: Option<SharedString>,
        _unit: Option<Unit>,
    ) {
        let mut snapshots = self.snapshot_write_lock();
        snapshots.entry(key.clone()).or_insert_with(|| {
            MetricSnapshot::Counter(CounterSnapshot {
                value,
                description: description.as_deref().unwrap_or_default().to_string(),
            })
        });
    }

    fn ensure_gauge_snapshot(
        &self,
        key: &Key,
        value: Arc<AtomicU64>,
        description: Option<SharedString>,
        _unit: Option<Unit>,
    ) {
        let mut snapshots = self.snapshot_write_lock();
        snapshots.entry(key.clone()).or_insert_with(|| {
            MetricSnapshot::Gauge(GaugeSnapshot {
                value,
                description: description.as_deref().unwrap_or_default().to_string(),
            })
        });
    }

    fn ensure_histogram_snapshot(
        &self,
        key: &Key,
        count: Arc<AtomicU64>,
        sum_bits: Arc<AtomicU64>,
        description: Option<SharedString>,
        _unit: Option<Unit>,
    ) {
        let mut snapshots = self.snapshot_write_lock();
        snapshots.entry(key.clone()).or_insert_with(|| {
            MetricSnapshot::Histogram(HistogramSnapshot {
                count,
                sum_bits,
                description: description.as_deref().unwrap_or_default().to_string(),
            })
        });
    }

    pub fn render_prometheus_text(&self) -> String {
        let snapshots = self.snapshot_read_lock();
        let mut families: std::collections::BTreeMap<String, MetricFamily> = std::collections::BTreeMap::new();

        for (key, snapshot) in snapshots.iter() {
            let metric_name = sanitize_prometheus_metric_name(key.name());
            let labels = format_prometheus_labels(key);
            match snapshot {
                MetricSnapshot::Counter(counter) => {
                    let help = counter.description_or_name(&metric_name);
                    let value = counter.value.load(Ordering::Relaxed);
                    append_family_sample(
                        &mut families,
                        &metric_name,
                        "counter",
                        help.to_string(),
                        format!("{metric_name}{labels} {value}"),
                    );
                }
                MetricSnapshot::Gauge(gauge) => {
                    let help = gauge.description_or_name(&metric_name);
                    let value = f64::from_bits(gauge.value.load(Ordering::Relaxed));
                    append_family_sample(
                        &mut families,
                        &metric_name,
                        "gauge",
                        help.to_string(),
                        format!("{metric_name}{labels} {}", format_prometheus_float(value)),
                    );
                }
                MetricSnapshot::Histogram(histogram) => {
                    let help = histogram.description_or_name(&metric_name);
                    let count = histogram.count.load(Ordering::Relaxed);
                    let sum = f64::from_bits(histogram.sum_bits.load(Ordering::Relaxed));
                    append_family_sample(
                        &mut families,
                        &metric_name,
                        "histogram",
                        help.to_string(),
                        format!(
                            "{metric_name}_bucket{} {}",
                            labels_with_extra(&labels, "le", "+Inf"),
                            count
                        ),
                    );
                    append_family_sample(
                        &mut families,
                        &metric_name,
                        "histogram",
                        help.to_string(),
                        format!("{metric_name}_sum{labels} {}", format_prometheus_float(sum)),
                    );
                    append_family_sample(
                        &mut families,
                        &metric_name,
                        "histogram",
                        help.to_string(),
                        format!("{metric_name}_count{labels} {count}"),
                    );
                }
            }
        }

        if families.is_empty() {
            return String::new();
        }

        let mut lines = Vec::new();
        for (metric_name, family) in families {
            lines.push(format!("# HELP {metric_name} {}", escape_prometheus_help(&family.help)));
            lines.push(format!("# TYPE {metric_name} {}", family.metric_type));
            lines.extend(family.samples);
        }

        lines.push(String::new());
        lines.join("\n")
    }

    pub fn metric_count(&self) -> usize {
        self.snapshot_read_lock().len()
    }
}

impl Deref for Recorder {
    type Target = Meter;

    fn deref(&self) -> &Self::Target {
        &self.meter
    }
}

impl metrics::Recorder for Recorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe_metric(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe_metric(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.describe_metric(key, unit, description);
    }

    fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
        if let Some(counter) = Self::get_cached_metric(&self.cached_counters, key, "counter") {
            return counter;
        }

        let builder = self.meter.u64_counter(key.name().to_owned());
        let metadata = self.get_metadata_for_builder(key.name());
        let meta_description = metadata.as_ref().map(|m| m.description.clone());
        let meta_unit = metadata.as_ref().and_then(|m| m.unit.clone());
        let builder = configure_builder!(builder, metadata);

        let counter = builder.build();
        let labels = key
            .labels()
            .map(|label| KeyValue::new(label.key().to_owned(), label.value().to_owned()))
            .collect();

        let value = Arc::new(AtomicU64::new(0));
        let handle = Counter::from_arc(Arc::new(WrappedCounter {
            counter,
            labels,
            value: Arc::clone(&value),
        }));

        self.ensure_counter_snapshot(key, value, meta_description, meta_unit);
        Self::insert_cached_metric(&self.cached_counters, key.clone(), handle, "counter")
    }

    fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
        if let Some(gauge) = Self::get_cached_metric(&self.cached_gauges, key, "gauge") {
            return gauge;
        }

        let builder = self.meter.f64_gauge(key.name().to_owned());
        let metadata = self.get_metadata_for_builder(key.name());
        let meta_description = metadata.as_ref().map(|m| m.description.clone());
        let meta_unit = metadata.as_ref().and_then(|m| m.unit.clone());
        let builder = configure_builder!(builder, metadata);

        let gauge = builder.build();
        let labels = key
            .labels()
            .map(|label| KeyValue::new(label.key().to_owned(), label.value().to_owned()))
            .collect();

        let value = Arc::new(AtomicU64::new(0));
        let handle = Gauge::from_arc(Arc::new(WrappedGauge {
            gauge,
            labels,
            value: Arc::clone(&value),
        }));

        self.ensure_gauge_snapshot(key, value, meta_description, meta_unit);
        Self::insert_cached_metric(&self.cached_gauges, key.clone(), handle, "gauge")
    }

    fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
        if let Some(histogram) = Self::get_cached_metric(&self.cached_histograms, key, "histogram") {
            return histogram;
        }

        let builder = self.meter.f64_histogram(key.name().to_owned());
        let metadata = self.get_metadata_for_builder(key.name());
        let meta_description = metadata.as_ref().map(|m| m.description.clone());
        let meta_unit = metadata.as_ref().and_then(|m| m.unit.clone());
        let builder = configure_builder!(builder, metadata);

        let histogram = builder.build();
        let labels = key
            .labels()
            .map(|label| KeyValue::new(label.key().to_owned(), label.value().to_owned()))
            .collect();

        let count = Arc::new(AtomicU64::new(0));
        let sum_bits = Arc::new(AtomicU64::new(0f64.to_bits()));
        let handle = Histogram::from_arc(Arc::new(WrappedHistogram {
            histogram,
            labels,
            count: Arc::clone(&count),
            sum_bits: Arc::clone(&sum_bits),
        }));

        self.ensure_histogram_snapshot(key, count, sum_bits, meta_description, meta_unit);
        Self::insert_cached_metric(&self.cached_histograms, key.clone(), handle, "histogram")
    }
}

struct WrappedCounter {
    counter: opentelemetry::metrics::Counter<u64>,
    labels: Vec<KeyValue>,
    value: Arc<AtomicU64>,
}

impl CounterFn for WrappedCounter {
    fn increment(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
        self.counter.add(value, &self.labels);
    }

    fn absolute(&self, value: u64) {
        let prev = self.value.swap(value, Ordering::Relaxed);
        let diff = value.saturating_sub(prev);
        self.counter.add(diff, &self.labels);
    }
}

struct WrappedGauge {
    gauge: opentelemetry::metrics::Gauge<f64>,
    labels: Vec<KeyValue>,
    value: Arc<AtomicU64>,
}

impl GaugeFn for WrappedGauge {
    fn increment(&self, value: f64) {
        let mut current = self.value.load(Ordering::Relaxed);
        let mut new = f64::from_bits(current) + value;
        while let Err(val) = self
            .value
            .compare_exchange(current, new.to_bits(), Ordering::AcqRel, Ordering::Relaxed)
        {
            current = val;
            new = f64::from_bits(current) + value;
        }

        self.gauge.record(new, &self.labels);
    }

    fn decrement(&self, value: f64) {
        let mut current = self.value.load(Ordering::Relaxed);
        let mut new = f64::from_bits(current) - value;
        while let Err(val) = self
            .value
            .compare_exchange(current, new.to_bits(), Ordering::AcqRel, Ordering::Relaxed)
        {
            current = val;
            new = f64::from_bits(current) - value;
        }

        self.gauge.record(new, &self.labels);
    }

    fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
        self.gauge.record(value, &self.labels);
    }
}

struct WrappedHistogram {
    histogram: opentelemetry::metrics::Histogram<f64>,
    labels: Vec<KeyValue>,
    count: Arc<AtomicU64>,
    sum_bits: Arc<AtomicU64>,
}

impl HistogramFn for WrappedHistogram {
    fn record(&self, value: f64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(current) + value;
            match self
                .sum_bits
                .compare_exchange(current, next.to_bits(), Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.histogram.record(value, &self.labels);
    }

    fn record_many(&self, value: f64, count: usize) {
        let count = count as u64;
        self.count.fetch_add(count, Ordering::Relaxed);
        let increment = value * count as f64;
        let mut current = self.sum_bits.load(Ordering::Relaxed);
        loop {
            let next = f64::from_bits(current) + increment;
            match self
                .sum_bits
                .compare_exchange(current, next.to_bits(), Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }

        for _ in 0..count {
            self.histogram.record(value, &self.labels);
        }
    }
}

#[derive(Debug, Clone)]
enum MetricSnapshot {
    Counter(CounterSnapshot),
    Gauge(GaugeSnapshot),
    Histogram(HistogramSnapshot),
}

#[derive(Debug, Clone)]
struct MetricFamily {
    metric_type: &'static str,
    help: String,
    samples: Vec<String>,
}

#[derive(Debug, Clone)]
struct CounterSnapshot {
    value: Arc<AtomicU64>,
    description: String,
}

impl CounterSnapshot {
    fn description_or_name<'a>(&'a self, metric_name: &'a str) -> &'a str {
        if self.description.is_empty() { metric_name } else { &self.description }
    }
}

#[derive(Debug, Clone)]
struct GaugeSnapshot {
    value: Arc<AtomicU64>,
    description: String,
}

impl GaugeSnapshot {
    fn description_or_name<'a>(&'a self, metric_name: &'a str) -> &'a str {
        if self.description.is_empty() { metric_name } else { &self.description }
    }
}

#[derive(Debug, Clone)]
struct HistogramSnapshot {
    count: Arc<AtomicU64>,
    sum_bits: Arc<AtomicU64>,
    description: String,
}

impl HistogramSnapshot {
    fn description_or_name<'a>(&'a self, metric_name: &'a str) -> &'a str {
        if self.description.is_empty() { metric_name } else { &self.description }
    }
}

fn append_family_sample(
    families: &mut std::collections::BTreeMap<String, MetricFamily>,
    metric_name: &str,
    metric_type: &'static str,
    help: String,
    sample: String,
) {
    use std::collections::btree_map::Entry;

    match families.entry(metric_name.to_string()) {
        Entry::Vacant(v) => {
            v.insert(MetricFamily {
                metric_type,
                help,
                samples: vec![sample],
            });
        }
        Entry::Occupied(mut o) => {
            let family = o.get_mut();
            if family.metric_type != metric_type {
                error!(
                    metric_name,
                    existing_type = family.metric_type,
                    requested_type = metric_type,
                    "conflicting metric type for prometheus family; dropping sample"
                );
                return;
            }

            if family.help.is_empty() && !help.is_empty() {
                family.help = help;
            }
            family.samples.push(sample);
        }
    }
}

fn sanitize_prometheus_metric_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (idx, ch) in name.chars().enumerate() {
        let valid = ch.is_ascii_alphanumeric() || ch == '_';
        let first_valid = ch.is_ascii_alphabetic() || ch == '_';
        if (idx == 0 && !first_valid) || !valid {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

fn format_prometheus_labels(key: &Key) -> String {
    let mut labels = Vec::new();
    for label in key.labels() {
        labels.push(format!(
            "{}=\"{}\"",
            sanitize_prometheus_label_name(label.key()),
            escape_prometheus_label_value(label.value())
        ));
    }

    if labels.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", labels.join(","))
    }
}

fn labels_with_extra(labels: &str, key: &str, value: &str) -> String {
    if labels.is_empty() {
        format!("{{{}=\"{}\"}}", sanitize_prometheus_label_name(key), escape_prometheus_label_value(value))
    } else {
        let inner = labels.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(labels);
        format!(
            "{{{},{}=\"{}\"}}",
            inner,
            sanitize_prometheus_label_name(key),
            escape_prometheus_label_value(value)
        )
    }
}

fn sanitize_prometheus_label_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for (idx, ch) in name.chars().enumerate() {
        let valid = ch.is_ascii_alphanumeric() || ch == '_';
        let first_valid = ch.is_ascii_alphabetic() || ch == '_';
        if (idx == 0 && !first_valid) || !valid {
            out.push('_');
        } else {
            out.push(ch);
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

fn escape_prometheus_help(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\n', "\\n")
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

fn format_prometheus_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_negative() {
            "-Inf".to_string()
        } else {
            "+Inf".to_string()
        }
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::Recorder as _;
    use opentelemetry_sdk::metrics::Temporality;

    fn test_recorder() -> Recorder {
        let exporter = opentelemetry_stdout::MetricExporterBuilder::default()
            .with_temporality(Temporality::Cumulative)
            .build();

        let (_provider, recorder) = Recorder::builder("test")
            .with_meter_provider(|b| b.with_periodic_exporter(exporter))
            .build();

        recorder
    }

    fn test_metadata() -> Metadata<'static> {
        Metadata::new(module_path!(), metrics::Level::INFO, None)
    }

    #[test]
    fn standard_usage() {
        let exporter = opentelemetry_stdout::MetricExporterBuilder::default()
            .with_temporality(Temporality::Cumulative)
            .build();

        let (provider, recorder) = Recorder::builder("my-app")
            .with_meter_provider(|builder| builder.with_periodic_exporter(exporter))
            .build();

        global::set_meter_provider(provider.clone());
        metrics::set_global_recorder(recorder).unwrap();

        let counter = metrics::counter!("my-counter");
        counter.increment(1);

        provider.force_flush().unwrap();
    }

    #[test]
    fn counter_cached_on_repeated_registration() {
        let recorder = test_recorder();
        let key = Key::from_name("requests_total");
        let meta = test_metadata();

        let _first = recorder.register_counter(&key, &meta);
        let _second = recorder.register_counter(&key, &meta);

        let cache = recorder.cached_counters.read().unwrap();
        assert_eq!(cache.len(), 1, "counter should be cached and inserted only once");
    }

    #[test]
    fn gauge_cached_on_repeated_registration() {
        let recorder = test_recorder();
        let key = Key::from_name("active_connections");
        let meta = test_metadata();

        let _first = recorder.register_gauge(&key, &meta);
        let _second = recorder.register_gauge(&key, &meta);

        let cache = recorder.cached_gauges.read().unwrap();
        assert_eq!(cache.len(), 1, "gauge should be cached and inserted only once");
    }

    #[test]
    fn histogram_cached_on_repeated_registration() {
        let recorder = test_recorder();
        let key = Key::from_name("request_duration");
        let meta = test_metadata();

        let _first = recorder.register_histogram(&key, &meta);
        let _second = recorder.register_histogram(&key, &meta);

        let cache = recorder.cached_histograms.read().unwrap();
        assert_eq!(cache.len(), 1, "histogram should be cached and inserted only once");
    }

    #[test]
    fn concurrent_register_counter_inserts_once() {
        let recorder = test_recorder();
        let key = Key::from_name("concurrent_counter");
        let shared = Arc::new(recorder);
        let barrier = Arc::new(std::sync::Barrier::new(10));

        let handles: Vec<_> = (0..10)
            .map(|_| {
                let r = Arc::clone(&shared);
                let k = key.clone();
                let b = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    b.wait();
                    let _ = r.register_counter(&k, &test_metadata());
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let cache = shared.cached_counters.read().unwrap();
        assert_eq!(cache.len(), 1, "concurrent registrations should produce exactly one cache entry");
    }

    #[test]
    fn render_prometheus_text_includes_counter_gauge_and_histogram_samples() {
        let recorder = test_recorder();
        let meta = test_metadata();

        let counter_key = Key::from_name("requests_total");
        let counter = recorder.register_counter(&counter_key, &meta);
        counter.increment(3);

        let gauge_key = Key::from_name("queue_depth");
        let gauge = recorder.register_gauge(&gauge_key, &meta);
        gauge.set(7.5);

        let histogram_key = Key::from_name("request_latency_seconds");
        let histogram = recorder.register_histogram(&histogram_key, &meta);
        histogram.record(0.5);
        histogram.record_many(1.0, 2);

        let text = recorder.render_prometheus_text();

        assert!(text.contains("# TYPE requests_total counter"));
        assert!(text.contains("requests_total 3"));

        assert!(text.contains("# TYPE queue_depth gauge"));
        assert!(text.contains("queue_depth 7.5"));

        assert!(text.contains("# TYPE request_latency_seconds histogram"));
        assert!(text.contains("request_latency_seconds_bucket{le=\"+Inf\"} 3"));
        assert!(text.contains("request_latency_seconds_sum 2.5"));
        assert!(text.contains("request_latency_seconds_count 3"));
        assert_eq!(text.matches("# TYPE request_latency_seconds histogram").count(), 1);
    }
}
