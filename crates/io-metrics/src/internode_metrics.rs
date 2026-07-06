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
use std::collections::HashMap;
use std::sync::{
    Arc, LazyLock, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const INTERNODE_OPERATION_READ_FILE_STREAM: &str = "read_file_stream";
pub const INTERNODE_OPERATION_PUT_FILE_STREAM: &str = "put_file_stream";
pub const INTERNODE_OPERATION_WALK_DIR: &str = "walk_dir";
pub const INTERNODE_OPERATION_GRPC_READ_ALL: &str = "grpc_read_all";
pub const INTERNODE_OPERATION_GRPC_WRITE_ALL: &str = "grpc_write_all";
pub const INTERNODE_OPERATION_GRPC_READ_MULTIPLE: &str = "grpc_read_multiple";
pub const INTERNODE_TRANSPORT_BACKEND_TCP_HTTP: &str = "tcp-http";
pub const INTERNODE_TRANSPORT_BACKEND_GRPC: &str = "grpc";
pub const INTERNODE_TRANSPORT_BACKEND_UNKNOWN: &str = "unknown";

/// Direction of a msgpack/JSON codec decode, for the JSON-fallback counter: a server decoding a
/// peer's request vs a client decoding a peer's response (grpc-optimization P2).
pub const INTERNODE_MSGPACK_DIRECTION_REQUEST: &str = "request";
pub const INTERNODE_MSGPACK_DIRECTION_RESPONSE: &str = "response";

const OPERATION_LABEL: &str = "operation";
const BACKEND_LABEL: &str = "backend";
const CLASSIFICATION_LABEL: &str = "classification";
const STAGE_LABEL: &str = "stage";
const DOMINANT_ERROR_LABEL: &str = "dominant_error";
const HTTP_VERSION_LABEL: &str = "http_version";
const DIRECTION_LABEL: &str = "direction";
const MESSAGE_LABEL: &str = "message";
const INTERNODE_OPERATION_SENT_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_sent_bytes_total";
const INTERNODE_OPERATION_RECV_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_recv_bytes_total";
const INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_outgoing_total";
const INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_incoming_total";
const INTERNODE_OPERATION_ERRORS_TOTAL: &str = "rustfs_system_network_internode_operation_errors_total";
const INTERNODE_OPERATION_DURATION_MS: &str = "rustfs_system_network_internode_operation_duration_ms";
const INTERNODE_OPERATION_CLASSIFIED_ERRORS_TOTAL: &str = "rustfs_system_network_internode_operation_classified_errors_total";
const INTERNODE_OPERATION_RETRIES_TOTAL: &str = "rustfs_system_network_internode_operation_retries_total";
const INTERNODE_OPERATION_RETRY_SUCCESSES_TOTAL: &str = "rustfs_system_network_internode_operation_retry_successes_total";
const INTERNODE_OPERATION_HTTP_VERSIONS_TOTAL: &str = "rustfs_system_network_internode_operation_http_versions_total";
const INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL: &str = "rustfs_system_network_internode_operation_stall_timeouts_total";
const INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL: &str =
    "rustfs_system_network_internode_operation_write_shutdown_errors_total";
const INTERNODE_OPERATION_PAYLOAD_BYTES: &str = "rustfs_system_network_internode_operation_payload_bytes";
const INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL: &str = "rustfs_system_network_internode_operation_large_payloads_total";
const INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL: &str = "rustfs_system_network_internode_msgpack_json_fallback_total";
const ERASURE_WRITE_QUORUM_FAILURES_TOTAL: &str = "rustfs_system_storage_erasure_write_quorum_failures_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternodeOperationMetricDescriptor {
    pub name: &'static str,
    pub labels: &'static [&'static str],
}

const OPERATION_BACKEND_LABELS: &[&str] = &[OPERATION_LABEL, BACKEND_LABEL];
const OPERATION_BACKEND_CLASSIFICATION_LABELS: &[&str] = &[OPERATION_LABEL, BACKEND_LABEL, CLASSIFICATION_LABEL];
const OPERATION_BACKEND_HTTP_VERSION_LABELS: &[&str] = &[OPERATION_LABEL, BACKEND_LABEL, HTTP_VERSION_LABEL];
const QUORUM_FAILURE_LABELS: &[&str] = &[STAGE_LABEL, DOMINANT_ERROR_LABEL];

pub const INTERNODE_OPERATION_METRICS: &[InternodeOperationMetricDescriptor] = &[
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_SENT_BYTES_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RECV_BYTES_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_ERRORS_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_DURATION_MS,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_CLASSIFIED_ERRORS_TOTAL,
        labels: OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RETRIES_TOTAL,
        labels: OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RETRY_SUCCESSES_TOTAL,
        labels: OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_HTTP_VERSIONS_TOTAL,
        labels: OPERATION_BACKEND_HTTP_VERSION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: ERASURE_WRITE_QUORUM_FAILURES_TOTAL,
        labels: QUORUM_FAILURE_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_PAYLOAD_BYTES,
        labels: OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL,
        labels: OPERATION_BACKEND_LABELS,
    },
];

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
    pub operation_http_versions_total: u64,
    pub operation_stall_timeouts_total: u64,
    pub operation_write_shutdown_errors_total: u64,
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
    operation_http_versions_total: AtomicU64,
    operation_stall_timeouts_total: AtomicU64,
    operation_write_shutdown_errors_total: AtomicU64,
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

    pub fn record_sent_bytes_for_operation(&self, operation: &'static str, bytes: usize) {
        self.record_sent_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN, bytes);
    }

    pub fn record_sent_bytes_for_operation_and_backend(&self, operation: &'static str, backend: &'static str, bytes: usize) {
        self.record_sent_bytes(bytes);

        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        counter!(INTERNODE_OPERATION_SENT_BYTES_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend).increment(bytes);
    }

    pub fn record_recv_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        self.recv_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_recv_bytes_total").increment(bytes);
    }

    pub fn record_recv_bytes_for_operation(&self, operation: &'static str, bytes: usize) {
        self.record_recv_bytes_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN, bytes);
    }

    pub fn record_recv_bytes_for_operation_and_backend(&self, operation: &'static str, backend: &'static str, bytes: usize) {
        self.record_recv_bytes(bytes);

        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        counter!(INTERNODE_OPERATION_RECV_BYTES_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend).increment(bytes);
    }

    pub fn record_outgoing_request(&self) {
        self.outgoing_requests_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_requests_outgoing_total").increment(1);
    }

    pub fn record_outgoing_request_for_operation(&self, operation: &'static str) {
        self.record_outgoing_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_outgoing_request_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_outgoing_request();
        counter!(INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend)
            .increment(1);
    }

    pub fn record_incoming_request(&self) {
        self.incoming_requests_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_requests_incoming_total").increment(1);
    }

    pub fn record_incoming_request_for_operation(&self, operation: &'static str) {
        self.record_incoming_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_incoming_request_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_incoming_request();
        counter!(INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend)
            .increment(1);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        counter!("rustfs_system_network_internode_errors_total").increment(1);
    }

    pub fn record_error_for_operation(&self, operation: &'static str) {
        self.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_error_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_error();
        counter!(INTERNODE_OPERATION_ERRORS_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend).increment(1);
    }

    pub fn record_duration_for_operation_and_backend(&self, operation: &'static str, backend: &'static str, duration: Duration) {
        let duration_ms = duration.as_secs_f64() * 1000.0;
        metrics::histogram!(INTERNODE_OPERATION_DURATION_MS, OPERATION_LABEL => operation, BACKEND_LABEL => backend)
            .record(duration_ms);
    }

    pub fn record_classified_error_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        classification: &'static str,
    ) {
        counter!(
            INTERNODE_OPERATION_CLASSIFIED_ERRORS_TOTAL,
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            CLASSIFICATION_LABEL => classification
        )
        .increment(1);
    }

    pub fn record_retry_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        classification: &'static str,
    ) {
        counter!(
            INTERNODE_OPERATION_RETRIES_TOTAL,
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            CLASSIFICATION_LABEL => classification
        )
        .increment(1);
    }

    pub fn record_retry_success_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        classification: &'static str,
    ) {
        counter!(
            INTERNODE_OPERATION_RETRY_SUCCESSES_TOTAL,
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            CLASSIFICATION_LABEL => classification
        )
        .increment(1);
    }

    pub fn record_http_version_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        http_version: &'static str,
    ) {
        self.operation_http_versions_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_OPERATION_HTTP_VERSIONS_TOTAL,
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            HTTP_VERSION_LABEL => http_version
        )
        .increment(1);
    }

    pub fn record_stall_timeout_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.operation_stall_timeouts_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend).increment(1);
    }

    pub fn record_write_shutdown_error_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.operation_write_shutdown_errors_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend)
            .increment(1);
    }

    /// Record the payload size (bytes) of a completed internode operation into a histogram
    /// keyed by operation+backend. Used to size which unary `bytes`-carrying RPCs
    /// (`ReadAll`/`ReadMultiple`/`WriteAll`) would benefit from being moved off the shared
    /// control-plane channel (see docs/grpc-optimization P1).
    pub fn record_operation_payload_bytes(&self, operation: &'static str, backend: &'static str, bytes: usize) {
        metrics::histogram!(INTERNODE_OPERATION_PAYLOAD_BYTES, OPERATION_LABEL => operation, BACKEND_LABEL => backend)
            .record(bytes as f64);
    }

    /// Increment the large-payload counter for an operation+backend whose payload exceeded the
    /// caller-configured warning threshold. Feeds alerting on large unary RPCs that contend with
    /// latency-sensitive control-plane traffic on the shared connection.
    pub fn record_large_operation_payload(&self, operation: &'static str, backend: &'static str) {
        counter!(INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL, OPERATION_LABEL => operation, BACKEND_LABEL => backend).increment(1);
    }

    /// Count a decode that fell back to the JSON compatibility field because the msgpack `_bin`
    /// payload was absent. Both internode RPC directions dual-encode msgpack + JSON today; this
    /// counter must read zero across a release window before the redundant JSON fields can be
    /// dropped (grpc-optimization P2). `direction` is [`INTERNODE_MSGPACK_DIRECTION_REQUEST`] or
    /// [`INTERNODE_MSGPACK_DIRECTION_RESPONSE`]; `message` is the low-cardinality value name.
    pub fn record_msgpack_json_fallback(&self, direction: &'static str, message: &'static str) {
        counter!(INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL, DIRECTION_LABEL => direction, MESSAGE_LABEL => message).increment(1);
    }

    pub fn record_erasure_write_quorum_failure(&self, stage: &'static str, dominant_error: &'static str) {
        counter!(
            ERASURE_WRITE_QUORUM_FAILURES_TOTAL,
            STAGE_LABEL => stage,
            DOMINANT_ERROR_LABEL => dominant_error
        )
        .increment(1);
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
            operation_http_versions_total: self.operation_http_versions_total.load(Ordering::Relaxed),
            operation_stall_timeouts_total: self.operation_stall_timeouts_total.load(Ordering::Relaxed),
            operation_write_shutdown_errors_total: self.operation_write_shutdown_errors_total.load(Ordering::Relaxed),
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
        self.operation_http_versions_total.store(0, Ordering::Relaxed);
        self.operation_stall_timeouts_total.store(0, Ordering::Relaxed);
        self.operation_write_shutdown_errors_total.store(0, Ordering::Relaxed);
    }
}

pub fn global_internode_metrics() -> &'static Arc<InternodeMetrics> {
    static GLOBAL_INTERNODE_METRICS: LazyLock<Arc<InternodeMetrics>> = LazyLock::new(|| Arc::new(InternodeMetrics::default()));
    &GLOBAL_INTERNODE_METRICS
}

// ── Cluster peer online/offline health (grpc-optimization P3) ──
// Tracks reachability of each internode peer and exposes the count of offline peers as a gauge,
// for parity with MinIO's `minio_cluster_servers_offline_total`. This is pure observability: it
// does not change peer selection or quorum. A peer flips offline after a configured number of
// consecutive failures and back online on the next successful dial.

/// Gauge: number of internode peers currently considered offline.
const CLUSTER_SERVERS_OFFLINE_TOTAL: &str = "rustfs_cluster_servers_offline_total";

#[derive(Debug)]
struct PeerHealthState {
    online: bool,
    consecutive_failures: u32,
}

impl Default for PeerHealthState {
    fn default() -> Self {
        // A newly observed peer is assumed online until it accrues failures.
        Self {
            online: true,
            consecutive_failures: 0,
        }
    }
}

static CLUSTER_PEER_HEALTH: LazyLock<Mutex<HashMap<String, PeerHealthState>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

fn publish_offline_gauge(peers: &HashMap<String, PeerHealthState>) {
    let offline = peers.values().filter(|peer| !peer.online).count();
    gauge!(CLUSTER_SERVERS_OFFLINE_TOTAL).set(offline as f64);
}

/// Record that a cluster peer is reachable: mark it online and reset its consecutive-failure
/// counter. Called on a successful dial to `addr`.
pub fn record_peer_reachable(addr: &str) {
    let Ok(mut peers) = CLUSTER_PEER_HEALTH.lock() else {
        return;
    };
    let entry = peers.entry(addr.to_string()).or_default();
    entry.online = true;
    entry.consecutive_failures = 0;
    publish_offline_gauge(&peers);
}

/// Record a failed interaction with a cluster peer (dial failure or RPC-triggered eviction). After
/// `failure_threshold` (>= 1) consecutive failures the peer flips offline.
pub fn record_peer_unreachable(addr: &str, failure_threshold: u32) {
    let Ok(mut peers) = CLUSTER_PEER_HEALTH.lock() else {
        return;
    };
    let entry = peers.entry(addr.to_string()).or_default();
    entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
    if entry.consecutive_failures >= failure_threshold.max(1) {
        entry.online = false;
    }
    publish_offline_gauge(&peers);
}

#[cfg(test)]
fn cluster_peer_online(addr: &str) -> Option<bool> {
    CLUSTER_PEER_HEALTH.lock().ok()?.get(addr).map(|peer| peer.online)
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

    #[test]
    fn operation_metrics_also_update_aggregate_snapshot() {
        let metrics = InternodeMetrics::default();

        metrics.record_sent_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_READ_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            128,
        );
        metrics.record_recv_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            256,
        );
        metrics.record_outgoing_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_WRITE_ALL,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_incoming_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_ALL,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_error_for_operation_and_backend(INTERNODE_OPERATION_WALK_DIR, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent_bytes_total, 128);
        assert_eq!(snapshot.recv_bytes_total, 256);
        assert_eq!(snapshot.outgoing_requests_total, 1);
        assert_eq!(snapshot.incoming_requests_total, 1);
        assert_eq!(snapshot.errors_total, 1);
    }

    #[test]
    fn operation_metric_descriptors_include_backend_and_operation_labels() {
        assert_eq!(INTERNODE_OPERATION_METRICS.len(), 15);
        for metric in &INTERNODE_OPERATION_METRICS[..6] {
            assert_eq!(metric.labels, &[OPERATION_LABEL, BACKEND_LABEL]);
        }
        for metric in &INTERNODE_OPERATION_METRICS[6..9] {
            assert_eq!(metric.labels, &[OPERATION_LABEL, BACKEND_LABEL, CLASSIFICATION_LABEL]);
        }
        assert_eq!(
            INTERNODE_OPERATION_METRICS[9].labels,
            &[OPERATION_LABEL, BACKEND_LABEL, HTTP_VERSION_LABEL]
        );
        for metric in &INTERNODE_OPERATION_METRICS[10..12] {
            assert_eq!(metric.labels, &[OPERATION_LABEL, BACKEND_LABEL]);
        }
        assert_eq!(INTERNODE_OPERATION_METRICS[12].labels, &[STAGE_LABEL, DOMINANT_ERROR_LABEL]);
        // Payload histogram + large-payload counter carry operation+backend labels.
        assert_eq!(INTERNODE_OPERATION_METRICS[13].labels, &[OPERATION_LABEL, BACKEND_LABEL]);
        assert_eq!(INTERNODE_OPERATION_METRICS[14].labels, &[OPERATION_LABEL, BACKEND_LABEL]);
    }

    #[test]
    fn operation_metric_names_and_low_cardinality_values_are_stable() {
        assert_eq!(INTERNODE_OPERATION_READ_FILE_STREAM, "read_file_stream");
        assert_eq!(INTERNODE_OPERATION_PUT_FILE_STREAM, "put_file_stream");
        assert_eq!(INTERNODE_OPERATION_WALK_DIR, "walk_dir");
        assert_eq!(INTERNODE_OPERATION_GRPC_READ_ALL, "grpc_read_all");
        assert_eq!(INTERNODE_OPERATION_GRPC_WRITE_ALL, "grpc_write_all");

        assert_eq!(INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, "tcp-http");
        assert_eq!(INTERNODE_TRANSPORT_BACKEND_GRPC, "grpc");
        assert_eq!(INTERNODE_TRANSPORT_BACKEND_UNKNOWN, "unknown");

        assert_eq!(
            INTERNODE_OPERATION_METRICS[5].name,
            "rustfs_system_network_internode_operation_duration_ms"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[6].name,
            "rustfs_system_network_internode_operation_classified_errors_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[7].name,
            "rustfs_system_network_internode_operation_retries_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[8].name,
            "rustfs_system_network_internode_operation_retry_successes_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[9].name,
            "rustfs_system_network_internode_operation_http_versions_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[10].name,
            "rustfs_system_network_internode_operation_stall_timeouts_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[11].name,
            "rustfs_system_network_internode_operation_write_shutdown_errors_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[12].name,
            "rustfs_system_storage_erasure_write_quorum_failures_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[13].name,
            "rustfs_system_network_internode_operation_payload_bytes"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[14].name,
            "rustfs_system_network_internode_operation_large_payloads_total"
        );
        assert_eq!(INTERNODE_OPERATION_GRPC_READ_MULTIPLE, "grpc_read_multiple");
        assert_eq!(
            INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL,
            "rustfs_system_network_internode_msgpack_json_fallback_total"
        );
        assert_eq!(INTERNODE_MSGPACK_DIRECTION_REQUEST, "request");
        assert_eq!(INTERNODE_MSGPACK_DIRECTION_RESPONSE, "response");
    }

    #[test]
    fn msgpack_json_fallback_counter_records_without_panicking() {
        // Smoke test: the counter accepts both directions and a static message label.
        let metrics = InternodeMetrics::default();
        metrics.record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_REQUEST, "FileInfo");
        metrics.record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_RESPONSE, "RawFileInfo");
    }

    #[test]
    fn cluster_peer_flips_offline_after_threshold_and_back_online() {
        // Unique addr keeps this independent of the process-global registry / other tests.
        let addr = "http://cluster-peer-health-unit-test:9000";
        assert_eq!(cluster_peer_online(addr), None);

        record_peer_unreachable(addr, 3);
        record_peer_unreachable(addr, 3);
        assert_eq!(cluster_peer_online(addr), Some(true), "still online below threshold");

        record_peer_unreachable(addr, 3);
        assert_eq!(cluster_peer_online(addr), Some(false), "offline at threshold");

        record_peer_reachable(addr);
        assert_eq!(cluster_peer_online(addr), Some(true), "back online after a reachable dial");
    }

    #[test]
    fn cluster_peer_threshold_is_clamped_to_at_least_one() {
        let addr = "http://cluster-peer-health-clamp-test:9000";
        // A zero threshold must not mean "never offline"; one failure suffices.
        record_peer_unreachable(addr, 0);
        assert_eq!(cluster_peer_online(addr), Some(false));
        record_peer_reachable(addr);
        assert_eq!(cluster_peer_online(addr), Some(true));
    }

    #[test]
    fn cluster_servers_offline_total_name_is_stable() {
        assert_eq!(CLUSTER_SERVERS_OFFLINE_TOTAL, "rustfs_cluster_servers_offline_total");
    }

    #[test]
    fn classified_and_retry_metrics_update_counters() {
        let metrics = InternodeMetrics::default();

        metrics.record_classified_error_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            "connection_reset",
        );
        metrics.record_retry_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            "connection_reset",
        );
        metrics.record_retry_success_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            "connection_reset",
        );
        metrics.record_http_version_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            "http/1.1",
        );
        metrics.record_stall_timeout_for_operation_and_backend(
            INTERNODE_OPERATION_READ_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        );
        metrics.record_write_shutdown_error_for_operation_and_backend(
            INTERNODE_OPERATION_PUT_FILE_STREAM,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        );
        metrics.record_erasure_write_quorum_failure("write", "connection_reset");

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent_bytes_total, 0);
        assert_eq!(snapshot.recv_bytes_total, 0);
        assert_eq!(snapshot.outgoing_requests_total, 0);
        assert_eq!(snapshot.incoming_requests_total, 0);
        assert_eq!(snapshot.operation_http_versions_total, 1);
        assert_eq!(snapshot.operation_stall_timeouts_total, 1);
        assert_eq!(snapshot.operation_write_shutdown_errors_total, 1);
    }
}
