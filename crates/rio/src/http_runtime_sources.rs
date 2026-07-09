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

use rustfs_io_metrics::internode_metrics::{INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, global_internode_metrics};
use rustfs_tls_runtime::{
    GlobalPublishedOutboundTlsState, load_global_outbound_tls_generation, load_global_outbound_tls_state,
    record_tls_consumer_stale_generation,
};
use std::time::Duration;

pub(crate) fn outbound_tls_generation() -> u64 {
    load_global_outbound_tls_generation().0
}

pub(crate) async fn outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    load_global_outbound_tls_state().await
}

pub(crate) fn record_stale_outbound_tls_generation(consumer: &'static str) {
    record_tls_consumer_stale_generation(consumer);
}

pub(crate) fn record_outgoing_request(operation: Option<&'static str>) {
    match operation {
        Some(operation) => global_internode_metrics()
            .record_outgoing_request_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP),
        None => global_internode_metrics().record_outgoing_request(),
    }
}

pub(crate) fn record_sent_bytes(operation: Option<&'static str>, bytes: usize) {
    match operation {
        Some(operation) => global_internode_metrics().record_sent_bytes_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            bytes,
        ),
        None => global_internode_metrics().record_sent_bytes(bytes),
    }
}

pub(crate) fn record_recv_bytes(operation: Option<&'static str>, bytes: usize) {
    match operation {
        Some(operation) => global_internode_metrics().record_recv_bytes_for_operation_and_backend(
            operation,
            INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
            bytes,
        ),
        None => global_internode_metrics().record_recv_bytes(bytes),
    }
}

pub(crate) fn record_error(operation: Option<&'static str>) {
    match operation {
        Some(operation) => {
            global_internode_metrics().record_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP)
        }
        None => global_internode_metrics().record_error(),
    }
}

pub(crate) fn record_classified_error(operation: &'static str, classification: &'static str) {
    global_internode_metrics().record_classified_error_for_operation_and_backend(
        operation,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_duration(operation: &'static str, duration: Duration) {
    global_internode_metrics().record_duration_for_operation_and_backend(
        operation,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        duration,
    );
}

pub(crate) fn record_http_version(operation: &'static str, http_version: &'static str) {
    global_internode_metrics().record_http_version_for_operation_and_backend(
        operation,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        http_version,
    );
}

pub(crate) fn record_stall_timeout(operation: &'static str) {
    global_internode_metrics().record_stall_timeout_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP);
}

pub(crate) fn record_write_shutdown_error(operation: &'static str) {
    global_internode_metrics()
        .record_write_shutdown_error_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP);
}
