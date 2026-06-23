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

use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_OPERATION_PUT_FILE_STREAM,
    INTERNODE_TRANSPORT_BACKEND_GRPC, INTERNODE_TRANSPORT_BACKEND_TCP_HTTP, global_internode_metrics,
};

#[cfg(test)]
use rustfs_io_metrics::internode_metrics::InternodeMetricsSnapshot;

pub(crate) fn record_remote_disk_open_write_retry(classification: &'static str) {
    global_internode_metrics().record_retry_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_open_write_retry_success(classification: &'static str) {
    global_internode_metrics().record_retry_success_for_operation_and_backend(
        INTERNODE_OPERATION_PUT_FILE_STREAM,
        INTERNODE_TRANSPORT_BACKEND_TCP_HTTP,
        classification,
    );
}

pub(crate) fn record_remote_disk_grpc_write_all_error() {
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_write_all_request() {
    global_internode_metrics()
        .record_outgoing_request_for_operation_and_backend(INTERNODE_OPERATION_GRPC_WRITE_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_write_all_sent_bytes(bytes: usize) {
    global_internode_metrics().record_sent_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_WRITE_ALL,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

pub(crate) fn record_remote_disk_grpc_read_all_error() {
    global_internode_metrics()
        .record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_read_all_request() {
    global_internode_metrics()
        .record_outgoing_request_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC);
}

pub(crate) fn record_remote_disk_grpc_read_all_recv_bytes(bytes: usize) {
    global_internode_metrics().record_recv_bytes_for_operation_and_backend(
        INTERNODE_OPERATION_GRPC_READ_ALL,
        INTERNODE_TRANSPORT_BACKEND_GRPC,
        bytes,
    );
}

#[cfg(test)]
pub(crate) fn reset_internode_metrics_for_test() {
    global_internode_metrics().reset_for_test();
}

#[cfg(test)]
pub(crate) fn internode_metrics_snapshot_for_test() -> InternodeMetricsSnapshot {
    global_internode_metrics().snapshot()
}
