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

use metrics::with_local_recorder;
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_STAGE_READ_VERSION_DISK_READ, INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP,
    INTERNODE_TRANSPORT_BACKEND_GRPC, InternodeMetrics, set_internode_server_label,
};
use std::time::Duration;

type MetricRow = (
    metrics_util::CompositeKey,
    Option<metrics::Unit>,
    Option<metrics::SharedString>,
    DebugValue,
);

const SERVER_LABEL: &str = "server";
const OPERATION_LABEL: &str = "operation";
const BACKEND_LABEL: &str = "backend";
const STAGE_LABEL: &str = "stage";
const SENT_BYTES_TOTAL: &str = "rustfs_system_network_internode_sent_bytes_total";
const RECV_BYTES_TOTAL: &str = "rustfs_system_network_internode_recv_bytes_total";
const REQUESTS_OUTGOING_TOTAL: &str = "rustfs_system_network_internode_requests_outgoing_total";
const REQUESTS_INCOMING_TOTAL: &str = "rustfs_system_network_internode_requests_incoming_total";
const ERRORS_TOTAL: &str = "rustfs_system_network_internode_errors_total";
const OPERATION_SENT_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_sent_bytes_total";
const OPERATION_RECV_BYTES_TOTAL: &str = "rustfs_system_network_internode_operation_recv_bytes_total";
const OPERATION_REQUESTS_OUTGOING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_outgoing_total";
const OPERATION_REQUESTS_INCOMING_TOTAL: &str = "rustfs_system_network_internode_operation_requests_incoming_total";
const OPERATION_ERRORS_TOTAL: &str = "rustfs_system_network_internode_operation_errors_total";
const OPERATION_DURATION_MS: &str = "rustfs_system_network_internode_operation_duration_ms";
const OPERATION_STAGE_DURATION_MS: &str = "rustfs_system_network_internode_operation_stage_duration_ms";

#[test]
fn cached_grpc_read_version_metric_handles_preserve_labels_and_values() {
    set_internode_server_label("cached-grpc-read-version-test");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    let metrics = InternodeMetrics::default();

    with_local_recorder(&recorder, || {
        metrics.record_sent_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            17,
        );
        metrics.record_recv_bytes_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            23,
        );
        metrics.record_outgoing_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_incoming_request_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
        );
        metrics.record_error_for_operation_and_backend(INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_TRANSPORT_BACKEND_GRPC);
        metrics.record_duration_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            Duration::from_micros(250),
        );
        metrics.record_stage_duration_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP,
            Duration::from_micros(125),
        );
        metrics.record_stage_duration_for_operation_and_backend(
            INTERNODE_OPERATION_GRPC_READ_VERSION,
            INTERNODE_TRANSPORT_BACKEND_GRPC,
            INTERNODE_STAGE_READ_VERSION_DISK_READ,
            Duration::from_micros(75),
        );
    });

    let rows = snapshotter.snapshot().into_vec();
    assert_counter(&rows, SENT_BYTES_TOTAL, &[(SERVER_LABEL, "cached-grpc-read-version-test")], 17);
    assert_counter(&rows, RECV_BYTES_TOTAL, &[(SERVER_LABEL, "cached-grpc-read-version-test")], 23);
    assert_counter(&rows, REQUESTS_OUTGOING_TOTAL, &[(SERVER_LABEL, "cached-grpc-read-version-test")], 1);
    assert_counter(&rows, REQUESTS_INCOMING_TOTAL, &[(SERVER_LABEL, "cached-grpc-read-version-test")], 1);
    assert_counter(&rows, ERRORS_TOTAL, &[(SERVER_LABEL, "cached-grpc-read-version-test")], 1);
    assert_counter(
        &rows,
        OPERATION_SENT_BYTES_TOTAL,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        17,
    );
    assert_counter(
        &rows,
        OPERATION_RECV_BYTES_TOTAL,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        23,
    );
    assert_counter(
        &rows,
        OPERATION_REQUESTS_OUTGOING_TOTAL,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        1,
    );
    assert_counter(
        &rows,
        OPERATION_REQUESTS_INCOMING_TOTAL,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        1,
    );
    assert_counter(
        &rows,
        OPERATION_ERRORS_TOTAL,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        1,
    );
    assert_histogram(
        &rows,
        OPERATION_DURATION_MS,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
        ],
        &[0.25],
    );
    assert_histogram(
        &rows,
        OPERATION_STAGE_DURATION_MS,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
            (STAGE_LABEL, INTERNODE_STAGE_READ_VERSION_RPC_ROUNDTRIP),
        ],
        &[0.125],
    );
    assert_histogram(
        &rows,
        OPERATION_STAGE_DURATION_MS,
        &[
            (SERVER_LABEL, "cached-grpc-read-version-test"),
            (OPERATION_LABEL, INTERNODE_OPERATION_GRPC_READ_VERSION),
            (BACKEND_LABEL, INTERNODE_TRANSPORT_BACKEND_GRPC),
            (STAGE_LABEL, INTERNODE_STAGE_READ_VERSION_DISK_READ),
        ],
        &[0.075],
    );
}

fn assert_counter(rows: &[MetricRow], name: &str, labels: &[(&str, &str)], expected: u64) {
    match metric_value(rows, name, labels) {
        DebugValue::Counter(value) => assert_eq!(*value, expected),
        other => panic!("{name} should be a counter, got {other:?}"),
    }
}

fn assert_histogram(rows: &[MetricRow], name: &str, labels: &[(&str, &str)], expected: &[f64]) {
    match metric_value(rows, name, labels) {
        DebugValue::Histogram(samples) => {
            let actual: Vec<_> = samples.iter().map(|sample| sample.0).collect();
            assert_eq!(actual, expected);
        }
        other => panic!("{name} should be a histogram, got {other:?}"),
    }
}

fn metric_value<'a>(rows: &'a [MetricRow], name: &str, labels: &[(&str, &str)]) -> &'a DebugValue {
    let mut matches = rows.iter().filter(|(composite, _, _, _)| {
        composite.key().name() == name
            && labels.iter().all(|(key, value)| {
                composite
                    .key()
                    .labels()
                    .any(|label| label.key() == *key && label.value() == *value)
            })
    });
    let Some((_, _, _, value)) = matches.next() else {
        panic!("{name} with labels {labels:?} was not recorded; rows={rows:?}");
    };
    assert!(matches.next().is_none(), "{name} with labels {labels:?} must be unique; rows={rows:?}");
    value
}
