//! Test-only value-capturing `metrics::Recorder`.
//!
//! Install with [`metrics::with_local_recorder`] to assert exact counter values
//! and histogram sample counts emitted by metric helpers. The recorder is
//! thread-local for the closure's duration, so it is safe under parallel
//! `cargo test` and never touches the global recorder. Helpers under test must
//! record from the calling thread — metrics emitted from spawned tasks or
//! `spawn_blocking` are invisible to a local recorder.

use metrics::{Counter, Gauge, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct CapturingRecorder {
    counters: Arc<Mutex<HashMap<Key, Arc<AtomicU64>>>>,
    histograms: Arc<Mutex<HashMap<Key, Arc<VecHistogram>>>>,
}

#[derive(Default)]
struct VecHistogram(Mutex<Vec<f64>>);

impl HistogramFn for VecHistogram {
    fn record(&self, value: f64) {
        self.0.lock().expect("histogram samples should be lockable").push(value);
    }
}

impl metrics::Recorder for CapturingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        let cell = self
            .counters
            .lock()
            .expect("counter map should be lockable")
            .entry(key.clone())
            .or_default()
            .clone();
        Counter::from_arc(cell)
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        let cell = self
            .histograms
            .lock()
            .expect("histogram map should be lockable")
            .entry(key.clone())
            .or_default()
            .clone();
        Histogram::from_arc(cell)
    }
}

impl CapturingRecorder {
    /// Total value of the counter with `name` whose labels include every `(key, value)` pair.
    pub(crate) fn counter_value(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
        self.counters
            .lock()
            .expect("counter map should be lockable")
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && labels
                        .iter()
                        .all(|(lk, lv)| key.labels().any(|label| label.key() == *lk && label.value() == *lv))
            })
            .map(|(_, value)| value.load(Ordering::Relaxed))
            .sum()
    }

    /// Number of samples recorded across all histograms with `name`.
    pub(crate) fn histogram_sample_count(&self, name: &str) -> usize {
        self.histograms
            .lock()
            .expect("histogram map should be lockable")
            .iter()
            .filter(|(key, _)| key.name() == name)
            .map(|(_, samples)| samples.0.lock().expect("samples should be lockable").len())
            .sum()
    }

    pub(crate) fn histogram_values(&self, name: &str, labels: &[(&str, &str)]) -> Vec<f64> {
        self.histograms
            .lock()
            .expect("histogram map should be lockable")
            .iter()
            .filter(|(key, _)| {
                key.name() == name
                    && labels
                        .iter()
                        .all(|(lk, lv)| key.labels().any(|label| label.key() == *lk && label.value() == *lv))
            })
            .flat_map(|(_, samples)| samples.0.lock().expect("samples should be lockable").clone())
            .collect()
    }
}
