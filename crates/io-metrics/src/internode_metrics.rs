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
    Arc, LazyLock, OnceLock, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const INTERNODE_OPERATION_READ_FILE_STREAM: &str = "read_file_stream";
pub const INTERNODE_OPERATION_PUT_FILE_STREAM: &str = "put_file_stream";
pub const INTERNODE_OPERATION_PUT_FILE_CAPABILITY: &str = "put_file_capability";
pub const INTERNODE_OPERATION_WALK_DIR: &str = "walk_dir";
pub const INTERNODE_OPERATION_NS_SCANNER: &str = "ns_scanner";
pub const INTERNODE_OPERATION_GRPC_READ_ALL: &str = "grpc_read_all";
pub const INTERNODE_OPERATION_GRPC_WRITE_ALL: &str = "grpc_write_all";
pub const INTERNODE_OPERATION_GRPC_READ_MULTIPLE: &str = "grpc_read_multiple";
pub const INTERNODE_OPERATION_GRPC_READ_VERSION: &str = "grpc_read_version";
pub const INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION: &str = "grpc_batch_read_version";
pub const INTERNODE_OPERATION_GRPC_LOCK: &str = "grpc_lock";
pub const INTERNODE_OPERATION_GRPC_UNLOCK: &str = "grpc_unlock";
pub const INTERNODE_OPERATION_GRPC_LOCK_BATCH: &str = "grpc_lock_batch";
pub const INTERNODE_OPERATION_GRPC_UNLOCK_BATCH: &str = "grpc_unlock_batch";
pub const INTERNODE_OPERATION_GRPC_REFRESH: &str = "grpc_refresh";
pub const INTERNODE_OPERATION_GRPC_FORCE_UNLOCK: &str = "grpc_force_unlock";
pub const INTERNODE_OPERATION_GRPC_OTHER: &str = "grpc_other";
pub const INTERNODE_TRANSPORT_BACKEND_TCP_HTTP: &str = "tcp-http";
pub const INTERNODE_TRANSPORT_BACKEND_GRPC: &str = "grpc";
pub const INTERNODE_TRANSPORT_BACKEND_UNKNOWN: &str = "unknown";

/// Direction of a msgpack/JSON codec decode, for the JSON-fallback counter: a server decoding a
/// peer's request vs a client decoding a peer's response (grpc-optimization P2).
pub const INTERNODE_MSGPACK_DIRECTION_REQUEST: &str = "request";
pub const INTERNODE_MSGPACK_DIRECTION_RESPONSE: &str = "response";
pub const INTERNODE_MSGPACK_CODEC_MSGPACK: &str = "msgpack";
pub const INTERNODE_MSGPACK_CODEC_JSON: &str = "json";
pub const INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE: &str = "read_version_request_encode";
pub const INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE: &str = "read_version_request_decode";
pub const INTERNODE_STAGE_READ_VERSION_DISK_READ: &str = "read_version_disk_read";
pub const INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE: &str = "read_version_response_json_encode";
pub const INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE: &str = "read_version_response_msgpack_encode";
pub const INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP: &str = "read_version_rpc_roundtrip";
pub const INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE: &str = "read_version_response_decode";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_ENCODE: &str = "batch_read_version_request_encode";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_REQUEST_DECODE: &str = "batch_read_version_request_decode";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_DISK_READ: &str = "batch_read_version_disk_read";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_JSON_ENCODE: &str = "batch_read_version_response_json_encode";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_MSGPACK_ENCODE: &str = "batch_read_version_response_msgpack_encode";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_RPC_ROUNDTRIP: &str = "batch_read_version_rpc_roundtrip";
pub const INTERNODE_STAGE_BATCH_READ_VERSION_RESPONSE_DECODE: &str = "batch_read_version_response_decode";

const OPERATION_LABEL: &str = "operation";
const BACKEND_LABEL: &str = "backend";
const SERVER_LABEL: &str = "server";
const CLASSIFICATION_LABEL: &str = "classification";
const STAGE_LABEL: &str = "stage";
const DOMINANT_ERROR_LABEL: &str = "dominant_error";
const HTTP_VERSION_LABEL: &str = "http_version";
const FAILURE_REASON_LABEL: &str = "failure_reason";
const RPC_PATH_LABEL: &str = "rpc_path";
const REASON_LABEL: &str = "reason";
const DIRECTION_LABEL: &str = "direction";
const MESSAGE_LABEL: &str = "message";
const CODEC_LABEL: &str = "codec";
const INTERNODE_OPERATION_SENT_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_sent_bytes_total";
const INTERNODE_OPERATION_RECV_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_recv_bytes_total";
const INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_outgoing_total";
const INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_incoming_total";
const INTERNODE_OPERATION_ERRORS_TOTAL: &str = "rustfs_system_network_internode_operation_errors_total";
const INTERNODE_OPERATION_DURATION_MS: &str = "rustfs_system_network_internode_operation_duration_ms";
const INTERNODE_OPERATION_STAGE_DURATION_MS: &str = "rustfs_system_network_internode_operation_stage_duration_ms";
const INTERNODE_OPERATION_CLASSIFIED_ERRORS_TOTAL: &str = "rustfs_system_network_internode_operation_classified_errors_total";
const INTERNODE_OPERATION_RETRIES_TOTAL: &str = "rustfs_system_network_internode_operation_retries_total";
const INTERNODE_OPERATION_RETRY_SUCCESSES_TOTAL: &str = "rustfs_system_network_internode_operation_retry_successes_total";
const INTERNODE_OPERATION_HTTP_VERSIONS_TOTAL: &str = "rustfs_system_network_internode_operation_http_versions_total";
const INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL: &str = "rustfs_system_network_internode_operation_stall_timeouts_total";
const INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL: &str =
    "rustfs_system_network_internode_operation_write_shutdown_errors_total";
const INTERNODE_RPC_AUTH_FAILURES_TOTAL: &str = "rustfs_system_network_internode_rpc_auth_failures_total";
const INTERNODE_OPERATION_PAYLOAD_BYTES: &str = "rustfs_system_network_internode_operation_payload_bytes";
const INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL: &str = "rustfs_system_network_internode_operation_large_payloads_total";
const INTERNODE_MSGPACK_JSON_DECODE_TOTAL: &str = "rustfs_system_network_internode_msgpack_json_decode_total";
const INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL: &str = "rustfs_system_network_internode_msgpack_json_fallback_total";
const INTERNODE_MSGPACK_JSON_DECODE_ERROR_TOTAL: &str = "rustfs_system_network_internode_msgpack_json_decode_error_total";
const INTERNODE_SIGNATURE_V1_FALLBACK_TOTAL: &str = "rustfs_system_network_internode_signature_v1_fallback_total";
const INTERNODE_BODY_DIGEST_FALLBACK_TOTAL: &str = "rustfs_system_network_internode_body_digest_fallback_total";
const INTERNODE_REPLAY_SCOPE_FALLBACK_TOTAL: &str = "rustfs_system_network_internode_replay_scope_fallback_total";
const INTERNODE_REPLAY_CACHE_OVERFLOW_TOTAL: &str = "rustfs_system_network_internode_replay_cache_overflow_total";
const INTERNODE_REPLAY_CACHE_OVERFLOW_BY_OPERATION_TOTAL: &str =
    "rustfs_system_network_internode_replay_cache_overflow_by_operation_total";
const INTERNODE_REPLAY_CACHE_RECORDS_TOTAL: &str = "rustfs_system_network_internode_replay_cache_records_total";
const INTERNODE_REPLAY_CACHE_ENTRIES: &str = "rustfs_system_network_internode_replay_cache_entries";
const INTERNODE_REPLAY_CACHE_CAPACITY: &str = "rustfs_system_network_internode_replay_cache_capacity";
const INTERNODE_REPLAY_CACHE_EVICTIONS_TOTAL: &str = "rustfs_system_network_internode_replay_cache_evictions_total";
const ERASURE_WRITE_QUORUM_FAILURES_TOTAL: &str = "rustfs_system_storage_erasure_write_quorum_failures_total";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternodeOperationMetricDescriptor {
    pub name: &'static str,
    pub labels: &'static [&'static str],
}

const SERVER_OPERATION_BACKEND_LABELS: &[&str] = &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL];
const SERVER_OPERATION_BACKEND_CLASSIFICATION_LABELS: &[&str] =
    &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, CLASSIFICATION_LABEL];
const SERVER_OPERATION_BACKEND_HTTP_VERSION_LABELS: &[&str] = &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, HTTP_VERSION_LABEL];
const SERVER_OPERATION_BACKEND_FAILURE_REASON_LABELS: &[&str] =
    &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, FAILURE_REASON_LABEL];
const SERVER_OPERATION_BACKEND_RPC_PATH_LABELS: &[&str] = &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, RPC_PATH_LABEL];
const SERVER_OPERATION_BACKEND_STAGE_LABELS: &[&str] = &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, STAGE_LABEL];
const SERVER_LABELS: &[&str] = &[SERVER_LABEL];
const SERVER_REASON_LABELS: &[&str] = &[SERVER_LABEL, REASON_LABEL];
const SERVER_QUORUM_FAILURE_LABELS: &[&str] = &[SERVER_LABEL, STAGE_LABEL, DOMINANT_ERROR_LABEL];

pub const INTERNODE_OPERATION_METRICS: &[InternodeOperationMetricDescriptor] = &[
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_SENT_BYTES_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RECV_BYTES_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_ERRORS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_DURATION_MS,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_STAGE_DURATION_MS,
        labels: SERVER_OPERATION_BACKEND_STAGE_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_CLASSIFIED_ERRORS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RETRIES_TOTAL,
        labels: SERVER_OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_RETRY_SUCCESSES_TOTAL,
        labels: SERVER_OPERATION_BACKEND_CLASSIFICATION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_HTTP_VERSIONS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_HTTP_VERSION_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_RPC_AUTH_FAILURES_TOTAL,
        labels: SERVER_OPERATION_BACKEND_FAILURE_REASON_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_REPLAY_CACHE_OVERFLOW_BY_OPERATION_TOTAL,
        labels: SERVER_OPERATION_BACKEND_RPC_PATH_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_REPLAY_CACHE_RECORDS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_RPC_PATH_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_REPLAY_CACHE_ENTRIES,
        labels: SERVER_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_REPLAY_CACHE_CAPACITY,
        labels: SERVER_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_REPLAY_CACHE_EVICTIONS_TOTAL,
        labels: SERVER_REASON_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: ERASURE_WRITE_QUORUM_FAILURES_TOTAL,
        labels: SERVER_QUORUM_FAILURE_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_PAYLOAD_BYTES,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
    InternodeOperationMetricDescriptor {
        name: INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL,
        labels: SERVER_OPERATION_BACKEND_LABELS,
    },
];

static STABLE_SERVER_LABEL: OnceLock<String> = OnceLock::new();

#[cfg(not(test))]
struct InternodeServerMetricHandles {
    sent_bytes: metrics::Counter,
    recv_bytes: metrics::Counter,
    outgoing_requests: metrics::Counter,
    incoming_requests: metrics::Counter,
    errors: metrics::Counter,
}

#[cfg(not(test))]
impl InternodeServerMetricHandles {
    fn new(server: &'static str) -> Self {
        Self {
            sent_bytes: counter!("rustfs_system_network_internode_sent_bytes_total", SERVER_LABEL => server),
            recv_bytes: counter!("rustfs_system_network_internode_recv_bytes_total", SERVER_LABEL => server),
            outgoing_requests: counter!("rustfs_system_network_internode_requests_outgoing_total", SERVER_LABEL => server),
            incoming_requests: counter!("rustfs_system_network_internode_requests_incoming_total", SERVER_LABEL => server),
            errors: counter!("rustfs_system_network_internode_errors_total", SERVER_LABEL => server),
        }
    }
}

#[cfg(not(test))]
static INTERNODE_SERVER_METRIC_HANDLES: LazyLock<InternodeServerMetricHandles> =
    LazyLock::new(|| InternodeServerMetricHandles::new(current_server_label()));

#[cfg(not(test))]
struct GrpcReadVersionMetricHandles {
    sent_bytes: metrics::Counter,
    recv_bytes: metrics::Counter,
    outgoing_requests: metrics::Counter,
    incoming_requests: metrics::Counter,
    errors: metrics::Counter,
    duration: metrics::Histogram,
    request_encode: metrics::Histogram,
    request_decode: metrics::Histogram,
    disk_read: metrics::Histogram,
    response_json_encode: metrics::Histogram,
    response_msgpack_encode: metrics::Histogram,
    rpc_roundtrip: metrics::Histogram,
    response_decode: metrics::Histogram,
}

#[cfg(not(test))]
impl GrpcReadVersionMetricHandles {
    fn new(server: &'static str) -> Self {
        Self {
            sent_bytes: counter!(
                INTERNODE_OPERATION_SENT_BYTES_TOTAL,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            recv_bytes: counter!(
                INTERNODE_OPERATION_RECV_BYTES_TOTAL,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            outgoing_requests: counter!(
                INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            incoming_requests: counter!(
                INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            errors: counter!(
                INTERNODE_OPERATION_ERRORS_TOTAL,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            duration: metrics::histogram!(
                INTERNODE_OPERATION_DURATION_MS,
                SERVER_LABEL => server,
                OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
                BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC
            ),
            request_encode: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE),
            request_decode: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE),
            disk_read: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_DISK_READ),
            response_json_encode: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE),
            response_msgpack_encode: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE),
            rpc_roundtrip: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP),
            response_decode: Self::stage_duration(server, INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE),
        }
    }

    fn stage_duration(server: &'static str, stage: &'static str) -> metrics::Histogram {
        metrics::histogram!(
            INTERNODE_OPERATION_STAGE_DURATION_MS,
            SERVER_LABEL => server,
            OPERATION_LABEL => INTERNODE_OPERATION_GRPC_READ_VERSION,
            BACKEND_LABEL => INTERNODE_TRANSPORT_BACKEND_GRPC,
            STAGE_LABEL => stage
        )
    }

    fn stage_duration_for(&self, stage: &'static str) -> Option<&metrics::Histogram> {
        match stage {
            INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE => Some(&self.request_encode),
            INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE => Some(&self.request_decode),
            INTERNODE_STAGE_READ_VERSION_DISK_READ => Some(&self.disk_read),
            INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE => Some(&self.response_json_encode),
            INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE => Some(&self.response_msgpack_encode),
            INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP => Some(&self.rpc_roundtrip),
            INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE => Some(&self.response_decode),
            _ => None,
        }
    }
}

#[cfg(not(test))]
static GRPC_READ_VERSION_METRIC_HANDLES: LazyLock<GrpcReadVersionMetricHandles> =
    LazyLock::new(|| GrpcReadVersionMetricHandles::new(current_server_label()));

#[cfg(not(test))]
fn server_metric_handles_if_ready() -> Option<&'static InternodeServerMetricHandles> {
    STABLE_SERVER_LABEL.get()?;
    Some(&INTERNODE_SERVER_METRIC_HANDLES)
}

#[cfg(not(test))]
fn grpc_read_version_metric_handles_if_ready(
    operation: &'static str,
    backend: &'static str,
) -> Option<&'static GrpcReadVersionMetricHandles> {
    STABLE_SERVER_LABEL.get()?;
    if operation == INTERNODE_OPERATION_GRPC_READ_VERSION && backend == INTERNODE_TRANSPORT_BACKEND_GRPC {
        Some(&GRPC_READ_VERSION_METRIC_HANDLES)
    } else {
        None
    }
}

/// Injects the stable server label (node name or address) stamped on
/// internode metrics. The runtime calls this when the local node name is
/// published (see ecstore's `set_local_node_name`); the first write wins.
/// io-metrics is a leaf crate and no longer resolves node identity itself
/// (backlog#1834) — before injection the label reads "unset".
pub fn set_internode_server_label(label: impl Into<String>) {
    let _ = STABLE_SERVER_LABEL.set(label.into());
}

fn current_server_label() -> &'static str {
    STABLE_SERVER_LABEL.get().map(String::as_str).unwrap_or("unset")
}

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
    pub rpc_auth_failures_total: u64,
    pub signature_v1_fallback_total: u64,
    pub body_digest_fallback_total: u64,
    pub replay_scope_fallback_total: u64,
    pub replay_cache_overflow_total: u64,
    pub replay_cache_entries: u64,
    pub replay_cache_capacity: u64,
    pub replay_cache_evictions_total: u64,
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
    rpc_auth_failures_total: AtomicU64,
    msgpack_json_decode_total: AtomicU64,
    msgpack_json_decode_error_total: AtomicU64,
    signature_v1_fallback_total: AtomicU64,
    body_digest_fallback_total: AtomicU64,
    replay_scope_fallback_total: AtomicU64,
    replay_cache_overflow_total: AtomicU64,
    replay_cache_entries: AtomicU64,
    replay_cache_capacity: AtomicU64,
    replay_cache_evictions_total: AtomicU64,
}

fn usize_to_u64_saturating(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

impl InternodeMetrics {
    pub fn record_sent_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        self.sent_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        #[cfg(not(test))]
        if let Some(handles) = server_metric_handles_if_ready() {
            handles.sent_bytes.increment(bytes);
            return;
        }
        counter!("rustfs_system_network_internode_sent_bytes_total", SERVER_LABEL => current_server_label()).increment(bytes);
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
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.sent_bytes.increment(bytes);
            return;
        }
        counter!(
            INTERNODE_OPERATION_SENT_BYTES_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(bytes);
    }

    pub fn record_recv_bytes(&self, bytes: usize) {
        let bytes = bytes as u64;
        if bytes == 0 {
            return;
        }
        self.recv_bytes_total.fetch_add(bytes, Ordering::Relaxed);
        #[cfg(not(test))]
        if let Some(handles) = server_metric_handles_if_ready() {
            handles.recv_bytes.increment(bytes);
            return;
        }
        counter!("rustfs_system_network_internode_recv_bytes_total", SERVER_LABEL => current_server_label()).increment(bytes);
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
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.recv_bytes.increment(bytes);
            return;
        }
        counter!(
            INTERNODE_OPERATION_RECV_BYTES_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(bytes);
    }

    pub fn record_outgoing_request(&self) {
        self.outgoing_requests_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(test))]
        if let Some(handles) = server_metric_handles_if_ready() {
            handles.outgoing_requests.increment(1);
            return;
        }
        counter!("rustfs_system_network_internode_requests_outgoing_total", SERVER_LABEL => current_server_label()).increment(1);
    }

    pub fn record_outgoing_request_for_operation(&self, operation: &'static str) {
        self.record_outgoing_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_outgoing_request_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_outgoing_request();
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.outgoing_requests.increment(1);
            return;
        }
        counter!(
            INTERNODE_OPERATION_REQUESTS_OUTGOING_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    pub fn record_incoming_request(&self) {
        self.incoming_requests_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(test))]
        if let Some(handles) = server_metric_handles_if_ready() {
            handles.incoming_requests.increment(1);
            return;
        }
        counter!("rustfs_system_network_internode_requests_incoming_total", SERVER_LABEL => current_server_label()).increment(1);
    }

    pub fn record_incoming_request_for_operation(&self, operation: &'static str) {
        self.record_incoming_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_incoming_request_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_incoming_request();
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.incoming_requests.increment(1);
            return;
        }
        counter!(
            INTERNODE_OPERATION_REQUESTS_INCOMING_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    pub fn record_error(&self) {
        self.errors_total.fetch_add(1, Ordering::Relaxed);
        #[cfg(not(test))]
        if let Some(handles) = server_metric_handles_if_ready() {
            handles.errors.increment(1);
            return;
        }
        counter!("rustfs_system_network_internode_errors_total", SERVER_LABEL => current_server_label()).increment(1);
    }

    pub fn record_error_for_operation(&self, operation: &'static str) {
        self.record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN);
    }

    pub fn record_error_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.record_error();
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.errors.increment(1);
            return;
        }
        counter!(
            INTERNODE_OPERATION_ERRORS_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    pub fn record_duration_for_operation_and_backend(&self, operation: &'static str, backend: &'static str, duration: Duration) {
        let duration_ms = duration.as_secs_f64() * 1000.0;
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend) {
            handles.duration.record(duration_ms);
            return;
        }
        metrics::histogram!(
            INTERNODE_OPERATION_DURATION_MS,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .record(duration_ms);
    }

    pub fn record_stage_duration_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        stage: &'static str,
        duration: Duration,
    ) {
        let duration_ms = duration.as_secs_f64() * 1000.0;
        #[cfg(not(test))]
        if let Some(handles) = grpc_read_version_metric_handles_if_ready(operation, backend)
            && let Some(histogram) = handles.stage_duration_for(stage)
        {
            histogram.record(duration_ms);
            return;
        }
        metrics::histogram!(
            INTERNODE_OPERATION_STAGE_DURATION_MS,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            STAGE_LABEL => stage
        )
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
            SERVER_LABEL => current_server_label(),
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
            SERVER_LABEL => current_server_label(),
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
            SERVER_LABEL => current_server_label(),
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
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            HTTP_VERSION_LABEL => http_version
        )
        .increment(1);
    }

    pub fn record_stall_timeout_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.operation_stall_timeouts_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_OPERATION_STALL_TIMEOUTS_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    pub fn record_write_shutdown_error_for_operation_and_backend(&self, operation: &'static str, backend: &'static str) {
        self.operation_write_shutdown_errors_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_OPERATION_WRITE_SHUTDOWN_ERRORS_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    pub fn record_rpc_auth_failure_for_operation_and_backend(
        &self,
        operation: &'static str,
        backend: &'static str,
        failure_reason: &'static str,
    ) {
        self.rpc_auth_failures_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_RPC_AUTH_FAILURES_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            FAILURE_REASON_LABEL => failure_reason
        )
        .increment(1);
    }

    /// Record the payload size (bytes) of a completed internode operation into a histogram
    /// keyed by operation+backend. Used to size which unary `bytes`-carrying RPCs
    /// (`ReadAll`/`ReadMultiple`/`WriteAll`) would benefit from being moved off the shared
    /// control-plane channel (see docs/grpc-optimization P1).
    pub fn record_operation_payload_bytes(&self, operation: &'static str, backend: &'static str, bytes: usize) {
        metrics::histogram!(
            INTERNODE_OPERATION_PAYLOAD_BYTES,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .record(bytes as f64);
    }

    /// Increment the large-payload counter for an operation+backend whose payload exceeded the
    /// caller-configured warning threshold. Feeds alerting on large unary RPCs that contend with
    /// latency-sensitive control-plane traffic on the shared connection.
    pub fn record_large_operation_payload(&self, operation: &'static str, backend: &'static str) {
        counter!(
            INTERNODE_OPERATION_LARGE_PAYLOADS_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend
        )
        .increment(1);
    }

    /// Count a decode that fell back to the JSON compatibility field because the msgpack `_bin`
    /// payload was absent. Both internode RPC directions dual-encode msgpack + JSON today; this
    /// counter must read zero across a release window before the redundant JSON fields can be
    /// dropped (grpc-optimization P2). `direction` is [`INTERNODE_MSGPACK_DIRECTION_REQUEST`] or
    /// [`INTERNODE_MSGPACK_DIRECTION_RESPONSE`]; `message` is the low-cardinality value name.
    pub fn record_msgpack_json_fallback(&self, direction: &'static str, message: &'static str) {
        counter!(
            INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL,
            SERVER_LABEL => current_server_label(),
            DIRECTION_LABEL => direction,
            MESSAGE_LABEL => message
        )
        .increment(1);
    }

    pub fn record_msgpack_json_decode(&self, direction: &'static str, message: &'static str, codec: &'static str) {
        self.msgpack_json_decode_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_MSGPACK_JSON_DECODE_TOTAL,
            SERVER_LABEL => current_server_label(),
            DIRECTION_LABEL => direction,
            MESSAGE_LABEL => message,
            CODEC_LABEL => codec
        )
        .increment(1);
    }

    pub fn record_msgpack_json_decode_error(&self, direction: &'static str, message: &'static str, codec: &'static str) {
        self.msgpack_json_decode_error_total.fetch_add(1, Ordering::Relaxed);
        counter!(
            INTERNODE_MSGPACK_JSON_DECODE_ERROR_TOTAL,
            SERVER_LABEL => current_server_label(),
            DIRECTION_LABEL => direction,
            MESSAGE_LABEL => message,
            CODEC_LABEL => codec
        )
        .increment(1);
    }

    #[doc(hidden)]
    pub fn msgpack_json_decode_error_total_for_test(&self) -> u64 {
        self.msgpack_json_decode_error_total.load(Ordering::Relaxed)
    }

    #[doc(hidden)]
    pub fn msgpack_json_decode_total_for_test(&self) -> u64 {
        self.msgpack_json_decode_total.load(Ordering::Relaxed)
    }

    /// Count an internode gRPC request that was accepted through the legacy constant-target
    /// signature because it carried no v2 auth headers (rolling-upgrade fallback, see
    /// <https://github.com/rustfs/backlog/issues/1327>). Only accepted requests count: rejected
    /// requests never authenticated, so they are not a rollout signal. This counter must read zero
    /// across a release window fleet-wide before `RUSTFS_INTERNODE_RPC_SIGNATURE_STRICT` may be
    /// enabled; after the strict flip the legacy fallback path is closed and the counter stays flat.
    pub fn record_signature_v1_fallback(&self) {
        self.signature_v1_fallback_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_SIGNATURE_V1_FALLBACK_TOTAL, SERVER_LABEL => current_server_label()).increment(1);
    }

    /// Count a mutating internode disk RPC that was accepted without a signature-bound canonical
    /// body digest (rolling-upgrade fallback, see
    /// <https://github.com/rustfs/backlog/issues/1327>). Only accepted requests count. This counter
    /// must read zero across a release window fleet-wide before
    /// `RUSTFS_INTERNODE_RPC_BODY_DIGEST_STRICT` may be enabled; after the strict flip digestless
    /// mutations are rejected and the counter stays flat.
    pub fn record_body_digest_fallback(&self) {
        self.body_digest_fallback_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_BODY_DIGEST_FALLBACK_TOTAL, SERVER_LABEL => current_server_label()).increment(1);
    }

    /// Count an accepted v1/v2 request that does not carry the replay-scoped signature. This is
    /// the convergence signal for `RUSTFS_INTERNODE_RPC_REPLAY_SCOPE_STRICT`.
    pub fn record_replay_scope_fallback(&self) {
        self.replay_scope_fallback_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_REPLAY_SCOPE_FALLBACK_TOTAL, SERVER_LABEL => current_server_label()).increment(1);
    }

    /// Count a body-bound internode RPC rejected because the replay-protection nonce cache was
    /// full. Overflow fails closed, so a sustained non-zero rate means
    /// `RUSTFS_INTERNODE_RPC_REPLAY_CACHE_CAPACITY` is undersized for this node's peak legitimate
    /// mutation rate and writes are being refused — alert on this counter.
    pub fn record_replay_cache_overflow(&self) {
        self.replay_cache_overflow_total.fetch_add(1, Ordering::Relaxed);
        counter!(INTERNODE_REPLAY_CACHE_OVERFLOW_TOTAL, SERVER_LABEL => current_server_label()).increment(1);
    }

    pub fn record_replay_cache_overflow_for_operation_and_backend_path(
        &self,
        operation: &'static str,
        backend: &'static str,
        rpc_path: &str,
    ) {
        self.record_replay_cache_overflow();
        counter!(
            INTERNODE_REPLAY_CACHE_OVERFLOW_BY_OPERATION_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            RPC_PATH_LABEL => rpc_path.to_owned()
        )
        .increment(1);
    }

    pub fn record_replay_cache_record_for_operation_and_backend_path(
        &self,
        operation: &'static str,
        backend: &'static str,
        rpc_path: &str,
    ) {
        counter!(
            INTERNODE_REPLAY_CACHE_RECORDS_TOTAL,
            SERVER_LABEL => current_server_label(),
            OPERATION_LABEL => operation,
            BACKEND_LABEL => backend,
            RPC_PATH_LABEL => rpc_path.to_owned()
        )
        .increment(1);
    }

    pub fn record_replay_cache_state(&self, entries: usize, capacity: usize) {
        let entries = usize_to_u64_saturating(entries);
        let capacity = usize_to_u64_saturating(capacity);
        self.replay_cache_entries.store(entries, Ordering::Relaxed);
        self.replay_cache_capacity.store(capacity, Ordering::Relaxed);
        gauge!(INTERNODE_REPLAY_CACHE_ENTRIES, SERVER_LABEL => current_server_label()).set(entries as f64);
        gauge!(INTERNODE_REPLAY_CACHE_CAPACITY, SERVER_LABEL => current_server_label()).set(capacity as f64);
    }

    pub fn record_replay_cache_evictions(&self, reason: &'static str, count: usize) {
        if count == 0 {
            return;
        }
        let count = usize_to_u64_saturating(count);
        self.replay_cache_evictions_total.fetch_add(count, Ordering::Relaxed);
        counter!(
            INTERNODE_REPLAY_CACHE_EVICTIONS_TOTAL,
            SERVER_LABEL => current_server_label(),
            REASON_LABEL => reason
        )
        .increment(count);
    }

    pub fn record_erasure_write_quorum_failure(&self, stage: &'static str, dominant_error: &'static str) {
        counter!(
            ERASURE_WRITE_QUORUM_FAILURES_TOTAL,
            SERVER_LABEL => current_server_label(),
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
        gauge!("rustfs_system_network_internode_dial_avg_time_nanos", SERVER_LABEL => current_server_label())
            .set(total as f64 / samples as f64);

        if !success {
            self.dial_errors_total.fetch_add(1, Ordering::Relaxed);
            counter!("rustfs_system_network_internode_dial_errors_total", SERVER_LABEL => current_server_label()).increment(1);
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
            rpc_auth_failures_total: self.rpc_auth_failures_total.load(Ordering::Relaxed),
            signature_v1_fallback_total: self.signature_v1_fallback_total.load(Ordering::Relaxed),
            body_digest_fallback_total: self.body_digest_fallback_total.load(Ordering::Relaxed),
            replay_scope_fallback_total: self.replay_scope_fallback_total.load(Ordering::Relaxed),
            replay_cache_overflow_total: self.replay_cache_overflow_total.load(Ordering::Relaxed),
            replay_cache_entries: self.replay_cache_entries.load(Ordering::Relaxed),
            replay_cache_capacity: self.replay_cache_capacity.load(Ordering::Relaxed),
            replay_cache_evictions_total: self.replay_cache_evictions_total.load(Ordering::Relaxed),
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
        self.rpc_auth_failures_total.store(0, Ordering::Relaxed);
        self.msgpack_json_decode_total.store(0, Ordering::Relaxed);
        self.msgpack_json_decode_error_total.store(0, Ordering::Relaxed);
        self.signature_v1_fallback_total.store(0, Ordering::Relaxed);
        self.body_digest_fallback_total.store(0, Ordering::Relaxed);
        self.replay_scope_fallback_total.store(0, Ordering::Relaxed);
        self.replay_cache_overflow_total.store(0, Ordering::Relaxed);
        self.replay_cache_entries.store(0, Ordering::Relaxed);
        self.replay_cache_capacity.store(0, Ordering::Relaxed);
        self.replay_cache_evictions_total.store(0, Ordering::Relaxed);
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
    /// Last time a request was let through to re-probe an offline peer (grpc-optimization P3
    /// offline bypass). `None` means "not yet re-probed since going offline".
    last_reprobe: Option<Instant>,
}

impl Default for PeerHealthState {
    fn default() -> Self {
        // A newly observed peer is assumed online until it accrues failures.
        Self {
            online: true,
            consecutive_failures: 0,
            last_reprobe: None,
        }
    }
}

/// Read-mostly: the hot `cluster_peer_should_bypass` check per internode RPC only
/// reads the map for the common (unknown/online) peer; a `RwLock` lets those run
/// concurrently instead of serializing every internode RPC on one mutex. Writes
/// (dial reachable/unreachable, and recording an offline peer's re-probe) are rare.
static CLUSTER_PEER_HEALTH: LazyLock<RwLock<HashMap<String, PeerHealthState>>> = LazyLock::new(|| RwLock::new(HashMap::new()));

fn publish_offline_gauge(peers: &HashMap<String, PeerHealthState>) {
    let offline = peers.values().filter(|peer| !peer.online).count();
    gauge!(CLUSTER_SERVERS_OFFLINE_TOTAL).set(offline as f64);
}

/// Canonicalize a peer address before using it as the `CLUSTER_PEER_HEALTH` key.
///
/// The same physical peer is referenced by different subsystems in slightly different string forms
/// — the data path keys by `endpoint.grid_host()` (`scheme://host:port`, no trailing slash) while
/// the lock path keys by `url::Url::to_string()` (`scheme://host:port/`, trailing slash). Without
/// normalization each form becomes its own health entry, so one downed node counts as 2 in the
/// `rustfs_cluster_servers_offline_total` gauge (and 2N for N nodes). Trimming trailing slashes
/// collapses them to a single canonical key.
fn normalize_peer_key(addr: &str) -> &str {
    addr.trim_end_matches('/')
}

/// Record that a cluster peer is reachable: mark it online and reset its consecutive-failure
/// counter. Called on a successful dial to `addr`.
pub fn record_peer_reachable(addr: &str) {
    // Recover from a poisoned lock so peer-health tracking and the offline gauge never stall permanently.
    let mut peers = CLUSTER_PEER_HEALTH.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = peers.entry(normalize_peer_key(addr).to_string()).or_default();
    entry.online = true;
    entry.consecutive_failures = 0;
    entry.last_reprobe = None;
    publish_offline_gauge(&peers);
}

/// Record a failed interaction with a cluster peer (dial failure or RPC-triggered eviction). After
/// `failure_threshold` (>= 1) consecutive failures the peer flips offline.
pub fn record_peer_unreachable(addr: &str, failure_threshold: u32) {
    // Recover from a poisoned lock so peer-health tracking and the offline gauge never stall permanently.
    let mut peers = CLUSTER_PEER_HEALTH.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    let entry = peers.entry(normalize_peer_key(addr).to_string()).or_default();
    entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
    if entry.consecutive_failures >= failure_threshold.max(1) {
        entry.online = false;
    }
    publish_offline_gauge(&peers);
}

/// Whether a cluster peer is currently considered offline (known and marked offline).
pub fn cluster_peer_is_offline(addr: &str) -> bool {
    let peers = CLUSTER_PEER_HEALTH.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    peers.get(normalize_peer_key(addr)).map(|peer| !peer.online).unwrap_or(false)
}

/// Return the last observed online/offline state for a peer, if this process has observed one.
pub fn cluster_peer_observed_online_status(addr: &str) -> Option<bool> {
    let peers = CLUSTER_PEER_HEALTH.read().unwrap_or_else(|poisoned| poisoned.into_inner());
    peers.get(normalize_peer_key(addr)).map(|peer| peer.online)
}

/// Decide whether to fast-fail (bypass) an offline peer instead of attempting to reach it
/// (grpc-optimization P3 offline bypass). Returns `true` to bypass.
///
/// Self-healing: for an offline peer this returns `true` most of the time, but lets one request
/// through every `reprobe_interval` (returning `false` and recording the re-probe time) so the peer
/// can recover via a normal dial even if no background monitor is running. Online peers are never
/// bypassed.
pub fn cluster_peer_should_bypass(addr: &str, reprobe_interval: Duration) -> bool {
    let key = normalize_peer_key(addr);

    // Fast path: the overwhelmingly common cases — an unknown peer or an online
    // one — are read-only, so take a shared read lock and let concurrent internode
    // RPCs check peer health without serializing on a single lock.
    {
        let peers = CLUSTER_PEER_HEALTH.read().unwrap_or_else(|poisoned| poisoned.into_inner());
        match peers.get(key) {
            None => return false,
            Some(entry) if entry.online => return false,
            Some(_) => {} // offline: fall through to the write path below
        }
    }

    // Slow path: the peer is offline, which may require recording a re-probe, so
    // take the write lock. Re-fetch and re-check because the state can change
    // between releasing the read lock and acquiring the write lock (e.g. a
    // successful dial flipped it back online).
    let mut peers = CLUSTER_PEER_HEALTH.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    let Some(entry) = peers.get_mut(key) else {
        return false;
    };
    if entry.online {
        return false;
    }
    let now = Instant::now();
    let due = match entry.last_reprobe {
        None => true,
        Some(last) => now.duration_since(last) >= reprobe_interval,
    };
    if due {
        // Let this request through to re-probe; do not bypass.
        entry.last_reprobe = Some(now);
        false
    } else {
        true
    }
}

#[cfg(test)]
fn cluster_peer_online(addr: &str) -> Option<bool> {
    CLUSTER_PEER_HEALTH
        .read()
        .ok()?
        .get(normalize_peer_key(addr))
        .map(|peer| peer.online)
}

#[cfg(test)]
fn cluster_peer_health_keys() -> Vec<String> {
    CLUSTER_PEER_HEALTH
        .read()
        .map(|peers| peers.keys().cloned().collect())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use std::collections::{HashMap, HashSet};

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
        metrics.record_rpc_auth_failure_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_OTHER,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            "missing_v2_signature",
        );
        metrics.record_replay_cache_state(64, 1024);
        metrics.record_replay_cache_evictions("expired", 3);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.sent_bytes_total, 128);
        assert_eq!(snapshot.recv_bytes_total, 256);
        assert_eq!(snapshot.outgoing_requests_total, 1);
        assert_eq!(snapshot.incoming_requests_total, 1);
        assert_eq!(snapshot.errors_total, 1);
        assert_eq!(snapshot.rpc_auth_failures_total, 1);
        assert_eq!(snapshot.replay_cache_entries, 64);
        assert_eq!(snapshot.replay_cache_capacity, 1024);
        assert_eq!(snapshot.replay_cache_evictions_total, 3);
    }

    #[test]
    fn operation_stage_duration_records_low_cardinality_stage_labels() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = InternodeMetrics::default();

        with_local_recorder(&recorder, || {
            metrics.record_stage_duration_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP,
                Duration::from_micros(125),
            );
        });

        let entries: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == INTERNODE_OPERATION_STAGE_DURATION_MS)
            .collect();
        assert_eq!(entries.len(), 1);
        let labels: HashMap<_, _> = entries[0]
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        assert_eq!(
            labels.get(OPERATION_LABEL).map(String::as_str),
            Some(INTERNODE_OPERATION_GRPC_READ_VERSION)
        );
        assert_eq!(labels.get(BACKEND_LABEL).map(String::as_str), Some(INTERNODE_TRANSPORT_BACKEND_GRPC));
        assert_eq!(
            labels.get(STAGE_LABEL).map(String::as_str),
            Some(INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP)
        );
        assert!(labels.get(SERVER_LABEL).is_some_and(|value| !value.is_empty()));
        match &entries[0].3 {
            DebugValue::Histogram(samples) => assert_eq!(samples.iter().map(|sample| sample.0).collect::<Vec<_>>(), vec![0.125]),
            other => panic!("{INTERNODE_OPERATION_STAGE_DURATION_MS} must be a histogram, got {other:?}"),
        }
    }

    #[test]
    fn operation_metric_descriptors_include_backend_and_operation_labels() {
        assert_eq!(INTERNODE_OPERATION_METRICS.len(), 22);
        for metric in &INTERNODE_OPERATION_METRICS[..6] {
            assert_eq!(metric.labels, &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL]);
        }
        assert_eq!(
            INTERNODE_OPERATION_METRICS[6].labels,
            &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, STAGE_LABEL]
        );
        for metric in &INTERNODE_OPERATION_METRICS[7..10] {
            assert_eq!(metric.labels, &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, CLASSIFICATION_LABEL]);
        }
        assert_eq!(
            INTERNODE_OPERATION_METRICS[10].labels,
            &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, HTTP_VERSION_LABEL]
        );
        for metric in &INTERNODE_OPERATION_METRICS[11..13] {
            assert_eq!(metric.labels, &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL]);
        }
        assert_eq!(
            INTERNODE_OPERATION_METRICS[13].labels,
            &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, FAILURE_REASON_LABEL]
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[14].labels,
            &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, RPC_PATH_LABEL]
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[15].labels,
            &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL, RPC_PATH_LABEL]
        );
        for metric in &INTERNODE_OPERATION_METRICS[16..18] {
            assert_eq!(metric.labels, &[SERVER_LABEL]);
        }
        assert_eq!(INTERNODE_OPERATION_METRICS[18].labels, &[SERVER_LABEL, REASON_LABEL]);
        assert_eq!(INTERNODE_OPERATION_METRICS[19].labels, &[SERVER_LABEL, STAGE_LABEL, DOMINANT_ERROR_LABEL]);
        // Payload histogram + large-payload counter carry operation+backend labels.
        assert_eq!(INTERNODE_OPERATION_METRICS[20].labels, &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL]);
        assert_eq!(INTERNODE_OPERATION_METRICS[21].labels, &[SERVER_LABEL, OPERATION_LABEL, BACKEND_LABEL]);
    }

    #[test]
    fn operation_metric_names_and_low_cardinality_values_are_stable() {
        assert_eq!(INTERNODE_OPERATION_READ_FILE_STREAM, "read_file_stream");
        assert_eq!(INTERNODE_OPERATION_PUT_FILE_STREAM, "put_file_stream");
        assert_eq!(INTERNODE_OPERATION_PUT_FILE_CAPABILITY, "put_file_capability");
        assert_eq!(INTERNODE_OPERATION_WALK_DIR, "walk_dir");
        assert_eq!(INTERNODE_OPERATION_GRPC_READ_ALL, "grpc_read_all");
        assert_eq!(INTERNODE_OPERATION_GRPC_WRITE_ALL, "grpc_write_all");
        assert_eq!(INTERNODE_OPERATION_GRPC_READ_VERSION, "grpc_read_version");
        assert_eq!(INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, "grpc_batch_read_version");
        assert_eq!(INTERNODE_OPERATION_GRPC_LOCK, "grpc_lock");
        assert_eq!(INTERNODE_OPERATION_GRPC_UNLOCK, "grpc_unlock");
        assert_eq!(INTERNODE_OPERATION_GRPC_LOCK_BATCH, "grpc_lock_batch");
        assert_eq!(INTERNODE_OPERATION_GRPC_UNLOCK_BATCH, "grpc_unlock_batch");
        assert_eq!(INTERNODE_OPERATION_GRPC_REFRESH, "grpc_refresh");
        assert_eq!(INTERNODE_OPERATION_GRPC_FORCE_UNLOCK, "grpc_force_unlock");
        assert_eq!(INTERNODE_OPERATION_GRPC_OTHER, "grpc_other");

        assert_eq!(INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, "tcp-http");
        assert_eq!(INTERNODE_TRANSPORT_BACKEND_GRPC, "grpc");
        assert_eq!(INTERNODE_TRANSPORT_BACKEND_UNKNOWN, "unknown");

        assert_eq!(
            INTERNODE_OPERATION_METRICS[5].name,
            "rustfs_system_network_internode_operation_duration_ms"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[6].name,
            "rustfs_system_network_internode_operation_stage_duration_ms"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[7].name,
            "rustfs_system_network_internode_operation_classified_errors_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[8].name,
            "rustfs_system_network_internode_operation_retries_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[9].name,
            "rustfs_system_network_internode_operation_retry_successes_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[10].name,
            "rustfs_system_network_internode_operation_http_versions_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[11].name,
            "rustfs_system_network_internode_operation_stall_timeouts_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[12].name,
            "rustfs_system_network_internode_operation_write_shutdown_errors_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[13].name,
            "rustfs_system_network_internode_rpc_auth_failures_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[14].name,
            "rustfs_system_network_internode_replay_cache_overflow_by_operation_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[15].name,
            "rustfs_system_network_internode_replay_cache_records_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[16].name,
            "rustfs_system_network_internode_replay_cache_entries"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[17].name,
            "rustfs_system_network_internode_replay_cache_capacity"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[18].name,
            "rustfs_system_network_internode_replay_cache_evictions_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[19].name,
            "rustfs_system_storage_erasure_write_quorum_failures_total"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[20].name,
            "rustfs_system_network_internode_operation_payload_bytes"
        );
        assert_eq!(
            INTERNODE_OPERATION_METRICS[21].name,
            "rustfs_system_network_internode_operation_large_payloads_total"
        );
        assert_eq!(INTERNODE_OPERATION_GRPC_READ_MULTIPLE, "grpc_read_multiple");
        assert_eq!(
            INTERNODE_MSGPACK_JSON_DECODE_TOTAL,
            "rustfs_system_network_internode_msgpack_json_decode_total"
        );
        assert_eq!(
            INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL,
            "rustfs_system_network_internode_msgpack_json_fallback_total"
        );
        assert_eq!(
            INTERNODE_MSGPACK_JSON_DECODE_ERROR_TOTAL,
            "rustfs_system_network_internode_msgpack_json_decode_error_total"
        );
        assert_eq!(INTERNODE_MSGPACK_DIRECTION_REQUEST, "request");
        assert_eq!(INTERNODE_MSGPACK_DIRECTION_RESPONSE, "response");
        assert_eq!(INTERNODE_MSGPACK_CODEC_MSGPACK, "msgpack");
        assert_eq!(INTERNODE_MSGPACK_CODEC_JSON, "json");
        assert_eq!(INTERNODE_STAGE_READ_VERSION_REQUEST_ENCODE, "read_version_request_encode");
        assert_eq!(INTERNODE_STAGE_READ_VERSION_REQUEST_DECODE, "read_version_request_decode");
        assert_eq!(INTERNODE_STAGE_READ_VERSION_DISK_READ, "read_version_disk_read");
        assert_eq!(INTERNODE_STAGE_READ_VERSION_RESPONSE_JSON_ENCODE, "read_version_response_json_encode");
        assert_eq!(
            INTERNODE_STAGE_READ_VERSION_RESPONSE_MSGPACK_ENCODE,
            "read_version_response_msgpack_encode"
        );
        assert_eq!(INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP, "read_version_rpc_roundtrip");
        assert_eq!(INTERNODE_STAGE_READ_VERSION_RESPONSE_DECODE, "read_version_response_decode");
        assert_eq!(
            INTERNODE_SIGNATURE_V1_FALLBACK_TOTAL,
            "rustfs_system_network_internode_signature_v1_fallback_total"
        );
        assert_eq!(
            INTERNODE_REPLAY_CACHE_RECORDS_TOTAL,
            "rustfs_system_network_internode_replay_cache_records_total"
        );
        assert_eq!(FAILURE_REASON_LABEL, "failure_reason");
        assert_eq!(RPC_PATH_LABEL, "rpc_path");
        assert_eq!(REASON_LABEL, "reason");
    }

    #[test]
    fn rpc_auth_failure_counter_records_low_cardinality_labels() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = InternodeMetrics::default();

        with_local_recorder(&recorder, || {
            metrics.record_rpc_auth_failure_for_operation_and_backend(
                INTERNODE_OPERATION_GRPC_READ_ALL,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                "invalid_v2_signature",
            );
        });

        assert_eq!(metrics.snapshot().rpc_auth_failures_total, 1);
        let entries: Vec<_> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == INTERNODE_RPC_AUTH_FAILURES_TOTAL)
            .collect();
        assert_eq!(entries.len(), 1);
        let labels: HashMap<_, _> = entries[0]
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        assert_eq!(labels.get(OPERATION_LABEL).map(String::as_str), Some(INTERNODE_OPERATION_GRPC_READ_ALL));
        assert_eq!(labels.get(BACKEND_LABEL).map(String::as_str), Some(INTERNODE_TRANSPORT_BACKEND_GRPC));
        assert_eq!(labels.get(FAILURE_REASON_LABEL).map(String::as_str), Some("invalid_v2_signature"));
        assert!(labels.get(SERVER_LABEL).is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn replay_cache_metrics_record_state_eviction_and_overflow_scope() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = InternodeMetrics::default();

        with_local_recorder(&recorder, || {
            metrics.record_replay_cache_state(7, 11);
            metrics.record_replay_cache_evictions("expired", 5);
            metrics.record_replay_cache_overflow_for_operation_and_backend_path(
                INTERNODE_OPERATION_GRPC_READ_ALL,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                "/node_service.NodeService/ReadAll",
            );
            metrics.record_replay_cache_record_for_operation_and_backend_path(
                INTERNODE_OPERATION_GRPC_READ_VERSION,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                "/node_service.NodeService/ReadVersion",
            );
        });

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.replay_cache_entries, 7);
        assert_eq!(snapshot.replay_cache_capacity, 11);
        assert_eq!(snapshot.replay_cache_evictions_total, 5);
        assert_eq!(snapshot.replay_cache_overflow_total, 1);

        let entries: Vec<_> = snapshotter.snapshot().into_vec();
        assert!(
            entries
                .iter()
                .any(|(composite, _, _, _)| composite.key().name() == INTERNODE_REPLAY_CACHE_ENTRIES)
        );
        assert!(
            entries
                .iter()
                .any(|(composite, _, _, _)| composite.key().name() == INTERNODE_REPLAY_CACHE_CAPACITY)
        );

        let overflow: Vec<_> = entries
            .iter()
            .filter(|(composite, _, _, _)| composite.key().name() == INTERNODE_REPLAY_CACHE_OVERFLOW_BY_OPERATION_TOTAL)
            .collect();
        assert_eq!(overflow.len(), 1);
        let labels: HashMap<_, _> = overflow[0]
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        assert_eq!(labels.get(OPERATION_LABEL).map(String::as_str), Some(INTERNODE_OPERATION_GRPC_READ_ALL));
        assert_eq!(labels.get(BACKEND_LABEL).map(String::as_str), Some(INTERNODE_TRANSPORT_BACKEND_GRPC));
        assert_eq!(labels.get(RPC_PATH_LABEL).map(String::as_str), Some("/node_service.NodeService/ReadAll"));
        assert!(labels.get(SERVER_LABEL).is_some_and(|value| !value.is_empty()));

        let records: Vec<_> = entries
            .iter()
            .filter(|(composite, _, _, _)| composite.key().name() == INTERNODE_REPLAY_CACHE_RECORDS_TOTAL)
            .collect();
        assert_eq!(records.len(), 1);
        let labels: HashMap<_, _> = records[0]
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect();
        assert_eq!(
            labels.get(OPERATION_LABEL).map(String::as_str),
            Some(INTERNODE_OPERATION_GRPC_READ_VERSION)
        );
        assert_eq!(labels.get(BACKEND_LABEL).map(String::as_str), Some(INTERNODE_TRANSPORT_BACKEND_GRPC));
        assert_eq!(
            labels.get(RPC_PATH_LABEL).map(String::as_str),
            Some("/node_service.NodeService/ReadVersion")
        );
        assert!(labels.get(SERVER_LABEL).is_some_and(|value| !value.is_empty()));
    }

    #[test]
    fn direct_internode_metrics_emit_stable_server_label() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = InternodeMetrics::default();

        with_local_recorder(&recorder, || {
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
            metrics.record_dial_result(Duration::from_millis(3), false);
        });

        let observed: Vec<(String, HashSet<String>, Option<String>)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| {
                matches!(
                    composite.key().name(),
                    "rustfs_system_network_internode_sent_bytes_total"
                        | "rustfs_system_network_internode_recv_bytes_total"
                        | INTERNODE_OPERATION_SENT_BYTES_TOTAL
                        | INTERNODE_OPERATION_RECV_BYTES_TOTAL
                        | "rustfs_system_network_internode_dial_avg_time_nanos"
                        | "rustfs_system_network_internode_dial_errors_total"
                )
            })
            .map(|(composite, _, _, _)| {
                let labels = composite.key().labels();
                let keys = labels.clone().map(|label| label.key().to_string()).collect();
                let server = labels
                    .filter(|label| label.key() == SERVER_LABEL)
                    .map(|label| label.value().to_string())
                    .next();
                (composite.key().name().to_string(), keys, server)
            })
            .collect();

        assert_eq!(observed.len(), 6);
        for (name, keys, server) in observed {
            assert!(keys.contains(SERVER_LABEL), "{name} must carry the server label");
            assert!(server.is_some_and(|value| !value.is_empty()), "{name} server label must not be empty");
        }
    }

    #[test]
    fn msgpack_json_fallback_counter_separates_the_two_directions() {
        // Previously a smoke test that asserted nothing; the counter carries no
        // in-struct total, so the emission itself is what has to be checked
        // (rustfs/backlog#1836).
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let metrics = InternodeMetrics::default();

        metrics::with_local_recorder(&recorder, || {
            metrics.record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_REQUEST, "FileInfo");
            metrics.record_msgpack_json_fallback(INTERNODE_MSGPACK_DIRECTION_RESPONSE, "RawFileInfo");
        });

        let observed: Vec<(String, String, u64)> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == INTERNODE_MSGPACK_JSON_FALLBACK_TOTAL)
            .map(|(composite, _, _, value)| {
                let labels: HashMap<String, String> = composite
                    .key()
                    .labels()
                    .map(|label| (label.key().to_string(), label.value().to_string()))
                    .collect();
                let count = match value {
                    DebugValue::Counter(count) => count,
                    other => panic!("fallback total must be a counter, got {other:?}"),
                };
                (
                    labels.get(DIRECTION_LABEL).cloned().unwrap_or_default(),
                    labels.get(MESSAGE_LABEL).cloned().unwrap_or_default(),
                    count,
                )
            })
            .collect();

        // Each direction/message pair is its own series, so a regression that
        // dropped a label would collapse these into one row.
        assert_eq!(observed.len(), 2, "each direction must land in its own series: {observed:?}");
        assert!(observed.contains(&(INTERNODE_MSGPACK_DIRECTION_REQUEST.to_string(), "FileInfo".to_string(), 1)));
        assert!(observed.contains(&(INTERNODE_MSGPACK_DIRECTION_RESPONSE.to_string(), "RawFileInfo".to_string(), 1)));
    }

    #[test]
    fn msgpack_json_decode_counter_tracks_codec_direction_and_message() {
        let metrics = InternodeMetrics::default();
        assert_eq!(metrics.msgpack_json_decode_total_for_test(), 0);

        metrics.record_msgpack_json_decode(INTERNODE_MSGPACK_DIRECTION_REQUEST, "FileInfo", INTERNODE_MSGPACK_CODEC_MSGPACK);
        metrics.record_msgpack_json_decode(INTERNODE_MSGPACK_DIRECTION_RESPONSE, "RawFileInfo", INTERNODE_MSGPACK_CODEC_JSON);

        assert_eq!(metrics.msgpack_json_decode_total_for_test(), 2);
        metrics.reset_for_test();
        assert_eq!(metrics.msgpack_json_decode_total_for_test(), 0);
    }

    #[test]
    fn msgpack_json_decode_error_counter_tracks_codec_direction_and_message() {
        let metrics = InternodeMetrics::default();
        assert_eq!(metrics.msgpack_json_decode_error_total_for_test(), 0);

        metrics.record_msgpack_json_decode_error(
            INTERNODE_MSGPACK_DIRECTION_REQUEST,
            "FileInfo",
            INTERNODE_MSGPACK_CODEC_MSGPACK,
        );
        metrics.record_msgpack_json_decode_error(
            INTERNODE_MSGPACK_DIRECTION_RESPONSE,
            "RawFileInfo",
            INTERNODE_MSGPACK_CODEC_JSON,
        );

        assert_eq!(metrics.msgpack_json_decode_error_total_for_test(), 2);
        metrics.reset_for_test();
        assert_eq!(metrics.msgpack_json_decode_error_total_for_test(), 0);
    }

    #[test]
    fn signature_v1_fallback_counter_updates_snapshot_and_resets() {
        // Instance-local metrics keep this independent of the process-global registry.
        let metrics = InternodeMetrics::default();
        assert_eq!(metrics.snapshot().signature_v1_fallback_total, 0);

        metrics.record_signature_v1_fallback();
        metrics.record_signature_v1_fallback();
        assert_eq!(metrics.snapshot().signature_v1_fallback_total, 2);

        metrics.reset_for_test();
        assert_eq!(metrics.snapshot().signature_v1_fallback_total, 0);
    }

    #[test]
    fn replay_scope_fallback_counter_updates_snapshot_and_resets() {
        let metrics = InternodeMetrics::default();
        assert_eq!(metrics.snapshot().replay_scope_fallback_total, 0);

        metrics.record_replay_scope_fallback();
        metrics.record_replay_scope_fallback();
        assert_eq!(metrics.snapshot().replay_scope_fallback_total, 2);

        metrics.reset_for_test();
        assert_eq!(metrics.snapshot().replay_scope_fallback_total, 0);
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
    fn peer_health_key_is_normalized_across_address_forms() {
        // Same physical peer, two address forms the codebase actually uses: the data path's
        // grid_host() (no trailing slash) and the lock path's url::Url::to_string() (trailing slash).
        // They MUST collapse to one health entry, else one downed node counts as 2 in the gauge.
        let bare = "http://cluster-peer-health-normalize-test:9000";
        let slashed = "http://cluster-peer-health-normalize-test:9000/";

        record_peer_unreachable(bare, 1);
        record_peer_unreachable(slashed, 1);

        let keys: Vec<String> = cluster_peer_health_keys()
            .into_iter()
            .filter(|k| k.contains("cluster-peer-health-normalize-test"))
            .collect();
        assert_eq!(keys.len(), 1, "two address forms of one peer must be a single health entry, got {keys:?}");

        // Either form observes the peer offline, and a reachable dial via one form clears the other.
        assert!(cluster_peer_is_offline(bare));
        assert!(cluster_peer_is_offline(slashed));
        record_peer_reachable(slashed);
        assert!(!cluster_peer_is_offline(bare), "reachable via one form must mark the shared entry online");
    }

    #[test]
    fn cluster_peer_observed_online_status_reports_known_and_unknown_peers() {
        let addr = "http://cluster-peer-snapshot-status-test:9000";
        assert_eq!(cluster_peer_observed_online_status(addr), None);

        record_peer_reachable(addr);
        assert_eq!(cluster_peer_observed_online_status(addr), Some(true));

        record_peer_unreachable(addr, 1);
        assert_eq!(cluster_peer_observed_online_status(addr), Some(false));
    }

    #[test]
    fn cluster_servers_offline_total_name_is_stable() {
        assert_eq!(CLUSTER_SERVERS_OFFLINE_TOTAL, "rustfs_cluster_servers_offline_total");
    }

    #[test]
    fn cluster_peer_should_bypass_is_self_healing() {
        let addr = "http://cluster-peer-bypass-selfheal-test:9000";
        let interval = Duration::from_secs(30);

        // Online peer: never bypassed.
        record_peer_reachable(addr);
        assert!(!cluster_peer_should_bypass(addr, interval));

        // Take it offline (threshold 1 for the test).
        record_peer_unreachable(addr, 1);
        assert!(cluster_peer_is_offline(addr));

        // First decision after going offline is a re-probe (not bypassed)...
        assert!(!cluster_peer_should_bypass(addr, interval));
        // ...and subsequent ones within the interval are bypassed.
        assert!(cluster_peer_should_bypass(addr, interval));
        assert!(cluster_peer_should_bypass(addr, interval));

        // A zero interval always allows a re-probe (never strands the peer).
        assert!(!cluster_peer_should_bypass(addr, Duration::ZERO));

        // Recovery clears bypass entirely.
        record_peer_reachable(addr);
        assert!(!cluster_peer_should_bypass(addr, interval));
    }

    #[test]
    fn cluster_peer_should_bypass_ignores_unknown_and_online_peers() {
        let addr = "http://cluster-peer-bypass-unknown-test:9000";
        // Unknown peer: not bypassed.
        assert!(!cluster_peer_should_bypass(addr, Duration::from_secs(5)));
        // Known-online peer: not bypassed.
        record_peer_reachable(addr);
        assert!(!cluster_peer_should_bypass(addr, Duration::from_secs(5)));
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
