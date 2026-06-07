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

//! Lock optimization metrics recording functions.

use std::time::Duration;

/// Record lock optimization enabled.
#[inline(always)]
pub fn record_lock_optimization_enabled(enabled: bool) {
    use metrics::gauge;
    gauge!("rustfs_lock_optimization_enabled").set(if enabled { 1.0 } else { 0.0 });
}

/// Record spin attempt.
#[inline(always)]
pub fn record_spin_attempt(success: bool) {
    use metrics::counter;
    if success {
        counter!("rustfs_lock_spin_successes").increment(1);
    } else {
        counter!("rustfs_lock_spin_failures").increment(1);
    }
}

/// Record adaptive spin count change.
#[inline(always)]
pub fn record_spin_count_change(new_count: usize) {
    use metrics::gauge;
    gauge!("rustfs_lock_spin_count").set(new_count as f64);
}

/// Record lock hold time.
#[inline(always)]
pub fn record_lock_hold_time(hold_time: Duration) {
    use metrics::histogram;
    histogram!("rustfs_lock_hold_time_secs").record(hold_time.as_secs_f64());
}

/// Record early release.
#[inline(always)]
pub fn record_early_release() {
    use metrics::counter;
    counter!("rustfs_lock_early_releases").increment(1);
}

/// Record contention event.
#[inline(always)]
pub fn record_contention_event() {
    use metrics::counter;
    counter!("rustfs_lock_contentions").increment(1);
}

/// Record object namespace lock diagnostics being enabled.
#[inline(always)]
pub fn record_object_lock_diag_enabled(enabled: bool) {
    use metrics::gauge;
    gauge!("rustfs_object_lock_diag_enabled").set(if enabled { 1.0 } else { 0.0 });
}

/// Record object namespace lock acquire duration.
#[inline(always)]
pub fn record_object_lock_diag_acquire_duration(op: &'static str, mode: &'static str, duration: Duration) {
    use metrics::histogram;
    histogram!(
        "rustfs_object_lock_diag_acquire_duration_seconds",
        "op" => op,
        "mode" => mode
    )
    .record(duration.as_secs_f64());
}

/// Record object namespace lock hold duration.
#[inline(always)]
pub fn record_object_lock_diag_hold_duration(op: &'static str, mode: &'static str, duration: Duration) {
    use metrics::histogram;
    histogram!(
        "rustfs_object_lock_diag_hold_duration_seconds",
        "op" => op,
        "mode" => mode
    )
    .record(duration.as_secs_f64());
}

/// Record an object namespace lock slow-acquire event.
#[inline(always)]
pub fn record_object_lock_diag_slow_acquire(op: &'static str, mode: &'static str) {
    use metrics::counter;
    counter!(
        "rustfs_object_lock_diag_slow_acquire_total",
        "op" => op,
        "mode" => mode
    )
    .increment(1);
}

/// Record an object namespace lock slow-hold event.
#[inline(always)]
pub fn record_object_lock_diag_slow_hold(op: &'static str, mode: &'static str) {
    use metrics::counter;
    counter!(
        "rustfs_object_lock_diag_slow_hold_total",
        "op" => op,
        "mode" => mode
    )
    .increment(1);
}

/// Lock statistics summary.
#[derive(Debug, Clone, Default)]
pub struct LockMetricsSummary {
    /// Total acquisitions.
    pub acquisitions: u64,
    /// Total releases.
    pub releases: u64,
    /// Spin successes.
    pub spin_successes: u64,
    /// Spin failures.
    pub spin_failures: u64,
    /// Early releases.
    pub early_releases: u64,
    /// Contentions.
    pub contentions: u64,
}

impl LockMetricsSummary {
    /// Create new summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get spin success rate.
    pub fn spin_success_rate(&self) -> f64 {
        let total = self.spin_successes + self.spin_failures;
        if total == 0 {
            0.0
        } else {
            self.spin_successes as f64 / total as f64
        }
    }

    /// Get contention rate.
    pub fn contention_rate(&self) -> f64 {
        if self.acquisitions == 0 {
            0.0
        } else {
            self.contentions as f64 / self.acquisitions as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::{Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, SharedString, Unit};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Default)]
    struct SeenMetricsRecorder {
        counters: Arc<Mutex<Vec<Key>>>,
        gauges: Arc<Mutex<Vec<Key>>>,
        histograms: Arc<Mutex<Vec<Key>>>,
    }

    impl SeenMetricsRecorder {
        fn saw_counter_named(&self, name: &str) -> bool {
            self.counters
                .lock()
                .expect("counter key collection should be lockable")
                .iter()
                .any(|key| key.name() == name)
        }

        fn saw_gauge_named(&self, name: &str) -> bool {
            self.gauges
                .lock()
                .expect("gauge key collection should be lockable")
                .iter()
                .any(|key| key.name() == name)
        }

        fn saw_histogram_named(&self, name: &str) -> bool {
            self.histograms
                .lock()
                .expect("histogram key collection should be lockable")
                .iter()
                .any(|key| key.name() == name)
        }
    }

    impl metrics::Recorder for SeenMetricsRecorder {
        fn describe_counter(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_gauge(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn describe_histogram(&self, _key: KeyName, _unit: Option<Unit>, _description: SharedString) {}

        fn register_counter(&self, key: &Key, _metadata: &Metadata<'_>) -> Counter {
            self.counters
                .lock()
                .expect("counter key collection should be lockable")
                .push(key.clone());
            Counter::from_arc(Arc::new(NoopCounter))
        }

        fn register_gauge(&self, key: &Key, _metadata: &Metadata<'_>) -> Gauge {
            self.gauges
                .lock()
                .expect("gauge key collection should be lockable")
                .push(key.clone());
            Gauge::from_arc(Arc::new(NoopGauge))
        }

        fn register_histogram(&self, key: &Key, _metadata: &Metadata<'_>) -> Histogram {
            self.histograms
                .lock()
                .expect("histogram key collection should be lockable")
                .push(key.clone());
            Histogram::from_arc(Arc::new(NoopHistogram))
        }
    }

    struct NoopCounter;

    impl CounterFn for NoopCounter {
        fn increment(&self, _value: u64) {}

        fn absolute(&self, _value: u64) {}
    }

    struct NoopGauge;

    impl GaugeFn for NoopGauge {
        fn increment(&self, _value: f64) {}

        fn decrement(&self, _value: f64) {}

        fn set(&self, _value: f64) {}
    }

    struct NoopHistogram;

    impl HistogramFn for NoopHistogram {
        fn record(&self, _value: f64) {}
    }

    #[test]
    fn test_record_lock_optimization_enabled() {
        record_lock_optimization_enabled(true);
        record_lock_optimization_enabled(false);
    }

    #[test]
    fn test_record_spin_attempt() {
        record_spin_attempt(true);
        record_spin_attempt(false);
    }

    #[test]
    fn test_record_spin_count_change() {
        record_spin_count_change(100);
        record_spin_count_change(200);
    }

    #[test]
    fn test_record_lock_hold_time() {
        record_lock_hold_time(Duration::from_millis(10));
        record_lock_hold_time(Duration::from_millis(100));
    }

    #[test]
    fn test_record_early_release() {
        record_early_release();
    }

    #[test]
    fn test_record_contention_event() {
        record_contention_event();
    }

    #[test]
    fn test_record_object_lock_diag_enabled() {
        let recorder = SeenMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_object_lock_diag_enabled(true);
            record_object_lock_diag_enabled(false);
        });
        assert!(
            recorder.saw_gauge_named("rustfs_object_lock_diag_enabled"),
            "expected object lock diagnostics enabled gauge to be emitted"
        );
    }

    #[test]
    fn test_record_object_lock_diag_acquire_duration() {
        let recorder = SeenMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_object_lock_diag_acquire_duration("get_object", "read", Duration::from_millis(10));
        });
        assert!(
            recorder.saw_histogram_named("rustfs_object_lock_diag_acquire_duration_seconds"),
            "expected object lock diagnostics acquire histogram to be emitted"
        );
    }

    #[test]
    fn test_record_object_lock_diag_hold_duration() {
        let recorder = SeenMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_object_lock_diag_hold_duration("put_object_commit", "write", Duration::from_millis(20));
        });
        assert!(
            recorder.saw_histogram_named("rustfs_object_lock_diag_hold_duration_seconds"),
            "expected object lock diagnostics hold histogram to be emitted"
        );
    }

    #[test]
    fn test_record_object_lock_diag_slow_events() {
        let recorder = SeenMetricsRecorder::default();
        metrics::with_local_recorder(&recorder, || {
            record_object_lock_diag_slow_acquire("get_object_info", "read");
            record_object_lock_diag_slow_hold("complete_multipart_upload_commit", "write");
        });
        assert!(
            recorder.saw_counter_named("rustfs_object_lock_diag_slow_acquire_total"),
            "expected object lock diagnostics slow-acquire counter to be emitted"
        );
        assert!(
            recorder.saw_counter_named("rustfs_object_lock_diag_slow_hold_total"),
            "expected object lock diagnostics slow-hold counter to be emitted"
        );
    }

    #[test]
    fn test_lock_metrics_summary() {
        let mut summary = LockMetricsSummary::new();
        summary.acquisitions = 100;
        summary.spin_successes = 80;
        summary.spin_failures = 20;
        summary.contentions = 10;

        assert!((summary.spin_success_rate() - 0.8).abs() < 0.01);
        assert!((summary.contention_rate() - 0.1).abs() < 0.01);
    }
}
