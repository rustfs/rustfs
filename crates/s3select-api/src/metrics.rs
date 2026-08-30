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

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SelectInputMetricsSnapshot {
    pub bytes_scanned: u64,
    pub bytes_processed: u64,
}

#[derive(Debug, Default)]
pub struct SelectInputMetrics {
    uncompressed_bytes: AtomicU64,
}

impl SelectInputMetrics {
    pub fn snapshot(&self) -> SelectInputMetricsSnapshot {
        let uncompressed_bytes = self.uncompressed_bytes.load(Ordering::Relaxed);
        SelectInputMetricsSnapshot {
            bytes_scanned: uncompressed_bytes,
            bytes_processed: uncompressed_bytes,
        }
    }

    pub(crate) fn record_uncompressed(&self, bytes: usize) {
        let increment = u64::try_from(bytes).unwrap_or(u64::MAX);
        let _ = self
            .uncompressed_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| Some(current.saturating_add(increment)));
    }

    /// Clears planner-only reads before query execution begins.
    pub fn reset(&self) {
        self.uncompressed_bytes.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_uncompressed_input_at_both_boundaries() {
        let metrics = SelectInputMetrics::default();
        metrics.record_uncompressed(7);

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
        metrics.uncompressed_bytes.store(u64::MAX - 1, Ordering::Relaxed);

        metrics.record_uncompressed(2);

        assert_eq!(metrics.snapshot().bytes_scanned, u64::MAX);
        assert_eq!(metrics.snapshot().bytes_processed, u64::MAX);
    }

    #[test]
    fn reset_clears_schema_inference_bytes() {
        let metrics = SelectInputMetrics::default();
        metrics.record_uncompressed(9);

        metrics.reset();

        assert_eq!(metrics.snapshot(), SelectInputMetricsSnapshot::default());
    }
}
