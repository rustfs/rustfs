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

use arc_swap::ArcSwap;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SelectInputMetricsSnapshot {
    pub bytes_scanned: u64,
    pub bytes_processed: u64,
}

#[derive(Debug)]
pub struct SelectInputMetrics {
    active: ArcSwap<SelectInputMetricBank>,
}

#[derive(Debug, Default)]
struct SelectInputMetricBank {
    uncompressed_bytes: AtomicU64,
    compressed_bytes_scanned: AtomicU64,
    compressed_bytes_processed: AtomicU64,
}

#[derive(Clone, Debug)]
pub(crate) struct SelectInputMetricsRecorder {
    bank: Arc<SelectInputMetricBank>,
}

impl Default for SelectInputMetrics {
    fn default() -> Self {
        Self {
            active: ArcSwap::from_pointee(SelectInputMetricBank::default()),
        }
    }
}

impl SelectInputMetrics {
    pub fn snapshot(&self) -> SelectInputMetricsSnapshot {
        let bank = self.active.load();
        let uncompressed_bytes = bank.uncompressed_bytes.load(Ordering::Relaxed);
        SelectInputMetricsSnapshot {
            bytes_scanned: uncompressed_bytes.saturating_add(bank.compressed_bytes_scanned.load(Ordering::Relaxed)),
            bytes_processed: uncompressed_bytes.saturating_add(bank.compressed_bytes_processed.load(Ordering::Relaxed)),
        }
    }

    pub(crate) fn recorder(&self) -> SelectInputMetricsRecorder {
        SelectInputMetricsRecorder {
            bank: self.active.load_full(),
        }
    }

    /// Publishes a fresh bank so late planner writes remain isolated.
    pub fn reset(&self) {
        self.active.store(Arc::new(SelectInputMetricBank::default()));
    }
}

impl SelectInputMetricsRecorder {
    pub(crate) fn record_uncompressed(&self, bytes: usize) {
        saturating_add(&self.bank.uncompressed_bytes, bytes);
    }

    pub(crate) fn record_scanned(&self, bytes: usize) {
        saturating_add(&self.bank.compressed_bytes_scanned, bytes);
    }

    pub(crate) fn record_processed(&self, bytes: usize) {
        saturating_add(&self.bank.compressed_bytes_processed, bytes);
    }
}

fn saturating_add(counter: &AtomicU64, bytes: usize) {
    let increment = u64::try_from(bytes).unwrap_or(u64::MAX);
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(increment)));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_uncompressed_input_at_both_boundaries() {
        let metrics = SelectInputMetrics::default();
        metrics.recorder().record_uncompressed(7);

        assert_eq!(
            metrics.snapshot(),
            SelectInputMetricsSnapshot {
                bytes_scanned: 7,
                bytes_processed: 7,
            }
        );
    }

    #[test]
    fn counters_saturate_instead_of_wrapping() {
        let metrics = SelectInputMetrics::default();
        metrics
            .active
            .load()
            .uncompressed_bytes
            .store(u64::MAX - 1, Ordering::Relaxed);

        metrics.recorder().record_uncompressed(2);

        assert_eq!(metrics.snapshot().bytes_scanned, u64::MAX);
        assert_eq!(metrics.snapshot().bytes_processed, u64::MAX);
    }

    #[test]
    fn compressed_boundaries_are_counted_independently() {
        let metrics = SelectInputMetrics::default();
        let recorder = metrics.recorder();
        recorder.record_scanned(39);
        recorder.record_processed(19);

        assert_eq!(
            metrics.snapshot(),
            SelectInputMetricsSnapshot {
                bytes_scanned: 39,
                bytes_processed: 19,
            }
        );
    }

    #[test]
    fn reset_clears_schema_inference_bytes() {
        let metrics = SelectInputMetrics::default();
        let planning = metrics.recorder();
        planning.record_uncompressed(9);

        metrics.reset();
        planning.record_uncompressed(5);
        let execution = metrics.recorder();
        execution.record_uncompressed(3);

        assert_eq!(
            metrics.snapshot(),
            SelectInputMetricsSnapshot {
                bytes_scanned: 3,
                bytes_processed: 3,
            }
        );
    }
}
