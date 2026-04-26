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

//! Timeout metrics recording functions.

use std::time::Duration;

/// Record timeout event.
#[inline(always)]
pub fn record_timeout_event(operation: &str) {
    use metrics::counter;
    counter!("rustfs_io_timeout_events_total", "operation" => operation.to_string()).increment(1);
}

/// Record operation duration.
#[inline(always)]
pub fn record_operation_duration(operation: &str, duration: Duration) {
    use metrics::histogram;
    histogram!("rustfs_io_operation_duration_seconds", "operation" => operation.to_string()).record(duration.as_secs_f64());
}

/// Record dynamic timeout calculation.
#[inline(always)]
pub fn record_dynamic_timeout(size_bytes: u64, timeout: Duration) {
    use metrics::{gauge, histogram};
    gauge!("rustfs_timeout_dynamic_size").set(size_bytes as f64);
    gauge!("rustfs_timeout_dynamic_secs").set(timeout.as_secs_f64());
    histogram!("rustfs_timeout_dynamic_size_histogram").record(size_bytes as f64);
}

/// Record operation progress.
#[inline(always)]
pub fn record_operation_progress(operation: &str, percent: f64) {
    use metrics::gauge;
    gauge!("rustfs_operation_progress", "operation" => operation.to_string()).set(percent);
}

/// Record stalled operation.
#[inline(always)]
pub fn record_stalled_operation(operation: &str) {
    use metrics::counter;
    counter!("rustfs_operation_stalled", "operation" => operation.to_string()).increment(1);
}

/// Record operation completion.
#[inline(always)]
pub fn record_operation_completion(operation: &str, success: bool) {
    use metrics::counter;
    let status = if success { "success" } else { "failure" };
    counter!("rustfs_operation_completions", "operation" => operation.to_string(), "status" => status).increment(1);
}

/// Timeout statistics summary.
#[derive(Debug, Clone, Default)]
pub struct TimeoutMetricsSummary {
    /// Total operations.
    pub total_operations: u64,
    /// Timed out operations.
    pub timed_out: u64,
    /// Stalled operations.
    pub stalled: u64,
    /// Successful operations.
    pub successful: u64,
    /// Failed operations.
    pub failed: u64,
}

impl TimeoutMetricsSummary {
    /// Create new summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get timeout rate.
    pub fn timeout_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.timed_out as f64 / self.total_operations as f64
        }
    }

    /// Get stall rate.
    pub fn stall_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.stalled as f64 / self.total_operations as f64
        }
    }

    /// Get success rate.
    pub fn success_rate(&self) -> f64 {
        if self.total_operations == 0 {
            0.0
        } else {
            self.successful as f64 / self.total_operations as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_timeout_event() {
        record_timeout_event("get_object");
        record_timeout_event("put_object");
    }

    #[test]
    fn test_record_operation_duration() {
        record_operation_duration("get_object", Duration::from_millis(100));
        record_operation_duration("put_object", Duration::from_millis(500));
    }

    #[test]
    fn test_record_dynamic_timeout() {
        record_dynamic_timeout(1024 * 1024, Duration::from_secs(10));
        record_dynamic_timeout(100 * 1024 * 1024, Duration::from_secs(30));
    }

    #[test]
    fn test_record_operation_progress() {
        record_operation_progress("get_object", 50.0);
        record_operation_progress("get_object", 100.0);
    }

    #[test]
    fn test_record_stalled_operation() {
        record_stalled_operation("get_object");
    }

    #[test]
    fn test_record_operation_completion() {
        record_operation_completion("get_object", true);
        record_operation_completion("get_object", false);
    }

    #[test]
    fn test_timeout_metrics_summary() {
        let mut summary = TimeoutMetricsSummary::new();
        summary.total_operations = 100;
        summary.timed_out = 5;
        summary.stalled = 2;
        summary.successful = 90;
        summary.failed = 5;

        assert!((summary.timeout_rate() - 0.05).abs() < 0.01);
        assert!((summary.stall_rate() - 0.02).abs() < 0.01);
        assert!((summary.success_rate() - 0.9).abs() < 0.01);
    }
}
