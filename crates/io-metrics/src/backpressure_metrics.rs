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

//! Backpressure metrics recording functions.

/// Record backpressure state change.
#[inline(always)]
pub fn record_backpressure_state_change(from: &str, to: &str) {
    use metrics::counter;
    counter!("rustfs_backpressure_state_changes", "from" => from.to_string(), "to" => to.to_string()).increment(1);
}

/// Record backpressure rejection.
#[inline(always)]
pub fn record_backpressure_rejection() {
    use metrics::counter;
    counter!("rustfs_backpressure_rejections").increment(1);
}

/// Record concurrent operations count.
#[inline(always)]
pub fn record_concurrent_operations(count: usize) {
    use metrics::gauge;
    gauge!("rustfs_backpressure_concurrent").set(count as f64);
}

/// Record backpressure activation.
#[inline(always)]
pub fn record_backpressure_activation() {
    use metrics::counter;
    counter!("rustfs_backpressure_activations").increment(1);
}

/// Record backpressure deactivation.
#[inline(always)]
pub fn record_backpressure_deactivation() {
    use metrics::counter;
    counter!("rustfs_backpressure_deactivations").increment(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Replaces the per-helper smoke tests that called the record_* helpers
    /// and asserted nothing: the calls (same literals) now run against a local
    /// DebuggingRecorder and every metric name the helpers own must actually
    /// be emitted (rustfs/backlog#1836 PR3).
    #[test]
    fn record_helpers_emit_their_metrics() {
        let recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            record_backpressure_state_change("normal", "warning");
            record_backpressure_state_change("warning", "critical");
            record_backpressure_rejection();
            record_concurrent_operations(10);
            record_concurrent_operations(32);
            record_backpressure_activation();
            record_backpressure_deactivation();
        });

        let emitted: std::collections::HashSet<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _, _, _)| composite.key().name().to_string())
            .collect();
        for expected in [
            "rustfs_backpressure_state_changes",
            "rustfs_backpressure_rejections",
            "rustfs_backpressure_concurrent",
            "rustfs_backpressure_activations",
            "rustfs_backpressure_deactivations",
        ] {
            assert!(emitted.contains(expected), "{expected} must be emitted by its record helper");
        }
    }
}
