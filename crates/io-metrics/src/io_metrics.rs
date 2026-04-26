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

//! I/O scheduler metrics recording functions.
//!
//! This module provides metrics recording for I/O scheduler operations.

/// Record I/O scheduler decision.
///
/// # Arguments
///
/// * `buffer_size` - Buffer size in bytes
/// * `load_level` - Load level string
/// * `strategy` - Strategy type string
#[inline(always)]
pub fn record_io_scheduler_decision(buffer_size: usize, load_level: &str, strategy: &str) {
    use metrics::{counter, gauge, histogram};

    counter!("rustfs_io_scheduler_decisions").increment(1);
    gauge!("rustfs_io_scheduler_buffer_size").set(buffer_size as f64);
    counter!("rustfs_io_scheduler_load", "level" => load_level.to_string()).increment(1);
    counter!("rustfs_io_scheduler_strategy", "type" => strategy.to_string()).increment(1);
    histogram!("rustfs_io_scheduler_buffer_size_histogram").record(buffer_size as f64);
}

/// Record I/O priority decision.
///
/// # Arguments
///
/// * `priority` - Priority level string
/// * `size` - Request size in bytes
#[inline(always)]
pub fn record_io_priority_decision(priority: &str, size: usize) {
    use metrics::{counter, histogram};

    counter!("rustfs_io_priority_decisions").increment(1);
    counter!("rustfs_io_priority_by_level", "priority" => priority.to_string()).increment(1);
    histogram!("rustfs_io_priority_request_size").record(size as f64);
}

/// Record load level change.
///
/// # Arguments
///
/// * `from` - Previous load level
/// * `to` - New load level
#[inline(always)]
pub fn record_load_level_change(from: &str, to: &str) {
    use metrics::counter;
    counter!("rustfs_io_load_changes", "from" => from.to_string(), "to" => to.to_string()).increment(1);
}

/// Record bandwidth observation.
///
/// # Arguments
///
/// * `bps` - Bytes per second
#[inline(always)]
pub fn record_bandwidth_observation(bps: u64) {
    use metrics::{gauge, histogram};
    gauge!("rustfs_io_bandwidth_bps").set(bps as f64);
    histogram!("rustfs_io_bandwidth_histogram").record(bps as f64);
}

/// Record buffer size adjustment.
///
/// # Arguments
///
/// * `original` - Original buffer size
/// * `adjusted` - Adjusted buffer size
/// * `reason` - Reason for adjustment
#[inline(always)]
pub fn record_buffer_size_adjustment(original: usize, adjusted: usize, reason: &str) {
    use metrics::{counter, gauge};
    counter!("rustfs_io_buffer_adjustments", "reason" => reason.to_string()).increment(1);
    gauge!("rustfs_io_buffer_original").set(original as f64);
    gauge!("rustfs_io_buffer_adjusted").set(adjusted as f64);
}

/// Record queue operation.
///
/// # Arguments
///
/// * `operation` - Operation type ("enqueue" or "dequeue")
/// * `priority` - Priority level
/// * `queue_size` - Current queue size
#[inline(always)]
pub fn record_queue_operation(operation: &str, priority: &str, queue_size: usize) {
    use metrics::{counter, gauge};
    counter!("rustfs_io_queue_operations", "operation" => operation.to_string(), "priority" => priority.to_string()).increment(1);
    gauge!("rustfs_io_queue_size", "priority" => priority.to_string()).set(queue_size as f64);
}

/// Record starvation event.
///
/// # Arguments
///
/// * `priority` - Starved priority level
#[inline(always)]
pub fn record_starvation_event(priority: &str) {
    use metrics::counter;
    counter!("rustfs_io_starvation_events", "priority" => priority.to_string()).increment(1);
}

/// I/O scheduler statistics.
#[derive(Debug, Clone, Default)]
pub struct IoSchedulerStats {
    /// Number of scheduler decisions.
    pub decisions: u64,
    /// Number of priority decisions.
    pub priority_decisions: u64,
    /// Number of load level changes.
    pub load_changes: u64,
    /// Number of buffer adjustments.
    pub buffer_adjustments: u64,
    /// Number of starvation events.
    pub starvation_events: u64,
}

impl IoSchedulerStats {
    /// Create new statistics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a scheduler decision.
    pub fn record_decision(&mut self) {
        self.decisions += 1;
    }

    /// Record a priority decision.
    pub fn record_priority_decision(&mut self) {
        self.priority_decisions += 1;
    }

    /// Record a load change.
    pub fn record_load_change(&mut self) {
        self.load_changes += 1;
    }

    /// Record a buffer adjustment.
    pub fn record_buffer_adjustment(&mut self) {
        self.buffer_adjustments += 1;
    }

    /// Record a starvation event.
    pub fn record_starvation(&mut self) {
        self.starvation_events += 1;
    }

    /// Reset statistics.
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_io_scheduler_decision() {
        record_io_scheduler_decision(128 * 1024, "low", "sequential");
        record_io_scheduler_decision(64 * 1024, "high", "random");
    }

    #[test]
    fn test_record_io_priority_decision() {
        record_io_priority_decision("high", 1024);
        record_io_priority_decision("normal", 1024 * 1024);
        record_io_priority_decision("low", 10 * 1024 * 1024);
    }

    #[test]
    fn test_record_load_level_change() {
        record_load_level_change("low", "medium");
        record_load_level_change("medium", "high");
    }

    #[test]
    fn test_record_bandwidth_observation() {
        record_bandwidth_observation(100 * 1024 * 1024);
        record_bandwidth_observation(500 * 1024 * 1024);
    }

    #[test]
    fn test_record_buffer_size_adjustment() {
        record_buffer_size_adjustment(128 * 1024, 64 * 1024, "concurrency");
        record_buffer_size_adjustment(128 * 1024, 256 * 1024, "sequential");
    }

    #[test]
    fn test_record_queue_operation() {
        record_queue_operation("enqueue", "high", 10);
        record_queue_operation("dequeue", "high", 9);
    }

    #[test]
    fn test_record_starvation_event() {
        record_starvation_event("low");
    }

    #[test]
    fn test_io_scheduler_stats() {
        let mut stats = IoSchedulerStats::new();

        stats.record_decision();
        stats.record_priority_decision();
        stats.record_load_change();
        stats.record_buffer_adjustment();
        stats.record_starvation();

        assert_eq!(stats.decisions, 1);
        assert_eq!(stats.priority_decisions, 1);
        assert_eq!(stats.load_changes, 1);
        assert_eq!(stats.buffer_adjustments, 1);
        assert_eq!(stats.starvation_events, 1);
    }
}
