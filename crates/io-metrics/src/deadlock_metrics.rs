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

//! Deadlock detection metrics recording functions.

use std::time::Duration;

/// Record potential deadlock detected.
#[inline(always)]
pub fn record_deadlock_detected(cycle_length: usize) {
    use metrics::{counter, histogram};
    counter!("rustfs_deadlock_detected_total").increment(1);
    histogram!("rustfs_deadlock_cycle_length").record(cycle_length as f64);
}

/// Record long-held lock.
#[inline(always)]
pub fn record_long_held_lock(_lock_id: u64, hold_time: Duration) {
    use metrics::{counter, histogram};
    counter!("rustfs_deadlock_long_held").increment(1);
    histogram!("rustfs_deadlock_hold_time_secs").record(hold_time.as_secs_f64());
}

/// Record lock acquisition.
#[inline(always)]
pub fn record_lock_acquisition(lock_type: &str) {
    use metrics::counter;
    counter!("rustfs_lock_acquisitions", "type" => lock_type.to_string()).increment(1);
}

/// Record lock release.
#[inline(always)]
pub fn record_lock_release(lock_type: &str, hold_time: Duration) {
    use metrics::{counter, histogram};
    counter!("rustfs_lock_releases", "type" => lock_type.to_string()).increment(1);
    histogram!("rustfs_lock_hold_time_secs", "type" => lock_type.to_string()).record(hold_time.as_secs_f64());
}

/// Record lock contention.
#[inline(always)]
pub fn record_lock_contention(lock_type: &str) {
    use metrics::counter;
    counter!("rustfs_lock_contentions", "type" => lock_type.to_string()).increment(1);
}

/// Record wait graph edge added.
#[inline(always)]
pub fn record_wait_edge_added() {
    use metrics::counter;
    counter!("rustfs_deadlock_wait_edges_added").increment(1);
}

/// Record wait graph edge removed.
#[inline(always)]
pub fn record_wait_edge_removed() {
    use metrics::counter;
    counter!("rustfs_deadlock_wait_edges_removed").increment(1);
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
            record_deadlock_detected(3);
            record_deadlock_detected(5);
            record_long_held_lock(1, Duration::from_secs(30));
            record_long_held_lock(2, Duration::from_secs(60));
            record_lock_acquisition("mutex");
            record_lock_acquisition("rwlock");
            record_lock_release("mutex", Duration::from_millis(10));
            record_lock_release("rwlock", Duration::from_millis(5));
            record_lock_contention("mutex");
            record_lock_contention("rwlock");
            record_wait_edge_added();
            record_wait_edge_removed();
        });

        let emitted: std::collections::HashSet<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .map(|(composite, _, _, _)| composite.key().name().to_string())
            .collect();
        for expected in [
            "rustfs_deadlock_detected_total",
            "rustfs_deadlock_cycle_length",
            "rustfs_deadlock_long_held",
            "rustfs_deadlock_hold_time_secs",
            "rustfs_lock_acquisitions",
            "rustfs_lock_releases",
            "rustfs_lock_hold_time_secs",
            "rustfs_lock_contentions",
            "rustfs_deadlock_wait_edges_added",
            "rustfs_deadlock_wait_edges_removed",
        ] {
            assert!(emitted.contains(expected), "{expected} must be emitted by its record helper");
        }
    }
}
