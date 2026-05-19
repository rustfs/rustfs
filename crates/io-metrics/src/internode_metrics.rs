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

use metrics::{counter, gauge};
use std::sync::{
    Arc, LazyLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InternodeMetricsSnapshot {
    pub sent_bytes_total: u64,
    pub recv_bytes_total: u64,
    pub outgoing_requests_total: u64,
    pub incoming_requests_total: u64,
    pub errors_total: u64,
    pub dial_errors_total: u64,
    pub dial_avg_time_nanos: u64,
    pub last_dial_unix_millis: u64,
}

#[derive(Debug, Default)]
pub struct InternodeMetrics {
    sent_bytes_total: AtomicU64,
    recv_bytes_total: AtomicU64,
    outgoing_requests_total: AtomicU64,
    incoming_requests_total: AtomicU64,
    errors_total: AtomicU64,
    dial_errors_total: AtomicU64,
    dial_total_time_nanos: AtomicU64,
    dial_samples_total: AtomicU64,
    last_dial_unix_millis: AtomicU64,
}

impl InternodeMetrics {
    pub fn record_sent_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        self.sent_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_sent_bytes_total").increment(bytes);
    }

    pub fn record_recv_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        self.recv_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_recv_bytes_total").increment(bytes);
    }

    pub fn record_outgoing_request(&self) {
        self.outgoing_requests_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_requests_outgoing_total").increment(1);
    }

    pub fn record_incoming_request(&self) {
        self.incoming_requests_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_requests_incoming_total").increment(1);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_errors_total").increment(1);
    }

    pub fn record_dial_result(&self, duration: Duration, success: bool) {
        let elapsed_nanos = duration.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.dial_total_time_nanos.fetch_add(elapsed_nanos, Ordering::Relaxed);
        let samples = self.dial_samples_total.fetch_add(1, Ordering::Relaxed) + 1;
        let total = self.dial_total_time_nanos.load(Ordering::Relaxed);
        gauge!("rustfs_system_network_internode_dial_avg_time_nanos").set(total as f64 / samples as f64);

        if !success {
            self.dial_errors_total.fetch_add(1, Ordering::Relaxed);
            counter!("rustfs_system_network_internode_dial_errors_total").increment(1);
        }

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64;
        self.last_dial_unix_millis.store(now_ms, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> InternodeMetricsSnapshot {
        let dial_samples_total = self.dial_samples_total.load(Ordering::Relaxed);
        let dial_total_time_nanos = self.dial_total_time_nanos.load(Ordering::Relaxed);
        let dial_avg_time_nanos = dial_total_time_nanos.checked_div(dial_samples_total).unwrap_or(0);

        InternodeMetricsSnapshot {
            sent_bytes_total: self.sent_bytes_total.load(Ordering::Relaxed),
            recv_bytes_total: self.recv_bytes_total.load(Ordering::Relaxed),
            outgoing_requests_total: self.outgoing_requests_total.load(Ordering::Relaxed),
            incoming_requests_total: self.incoming_requests_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            dial_errors_total: self.dial_errors_total.load(Ordering::Relaxed),
            dial_avg_time_nanos,
            last_dial_unix_millis: self.last_dial_unix_millis.load(Ordering::Relaxed),
        }
    }

    #[doc(hidden)]
    pub fn reset_for_test(&self) {
        self.sent_bytes_total.store(0, Ordering::Relaxed);
        self.recv_bytes_total.store(0, Ordering::Relaxed);
        self.outgoing_requests_total.store(0, Ordering::Relaxed);
        self.incoming_requests_total.store(0, Ordering::Relaxed);
        self.errors_total.store(0, Ordering::Relaxed);
        self.dial_errors_total.store(0, Ordering::Relaxed);
        self.dial_total_time_nanos.store(0, Ordering::Relaxed);
        self.dial_samples_total.store(0, Ordering::Relaxed);
        self.last_dial_unix_millis.store(0, Ordering::Relaxed);
    }
}

pub fn global_internode_metrics() -> &'static Arc<InternodeMetrics> {
    static GLOBAL_INTERNODE_METRICS: LazyLock<Arc<InternodeMetrics>> = LazyLock::new(|| Arc::new(InternodeMetrics::default()));
    &GLOBAL_INTERNODE_METRICS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reports_recorded_values() {
        let metrics = global_internode_metrics();
        metrics.reset_for_test();

        metrics.record_sent_bytes(64);
        metrics.record_recv_bytes(32);
        metrics.record_outgoing_request();
        metrics.record_incoming_request();
        metrics.record_error();
        metrics.record_dial_result(Duration::from_millis(9), true);
        metrics.record_dial_result(Duration::from_millis(3), false);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent_bytes_total, 64);
        assert_eq!(snapshot.recv_bytes_total, 32);
        assert_eq!(snapshot.outgoing_requests_total, 1);
        assert_eq!(snapshot.incoming_requests_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.dial_errors_total, 1);
        assert_eq!(snapshot.dial_avg_time_nanos, 6_000_000);
        assert!(snapshot.last_dial_unix_millis > 0);

        metrics.reset_for_test();
    }
}
