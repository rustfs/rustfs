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

use metrics::{Counter, Gauge, Histogram, Key, KeyName, Metadata, Recorder, SharedString, Unit, with_local_recorder};
use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rustfs_io_metrics::internode_metrics::{
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION, INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_OPERATION_GRPC_READ_MULTIPLE,
    INTERNODE_OPERATION_GRPC_READ_VERSION, INTERNODE_TRANSPORT_BACKEND_GRPC, INTERNODE_TRANSPORT_BACKEND_UNKNOWN,
    InternodeMetrics, set_internode_server_label,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const DURATION_METRIC: &str = "rustfs_system_network_internode_operation_duration_ms";
const READ_OPERATIONS: [&str; 3] = [
    INTERNODE_OPERATION_GRPC_READ_VERSION,
    INTERNODE_OPERATION_GRPC_READ_MULTIPLE,
    INTERNODE_OPERATION_GRPC_BATCH_READ_VERSION,
];

#[derive(Default)]
struct RegistrationCountingRecorder {
    inner: DebuggingRecorder,
    duration_registrations: AtomicUsize,
}

impl Recorder for RegistrationCountingRecorder {
    fn describe_counter(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_counter(key, unit, description);
    }

    fn describe_gauge(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_gauge(key, unit, description);
    }

    fn describe_histogram(&self, key: KeyName, unit: Option<Unit>, description: SharedString) {
        self.inner.describe_histogram(key, unit, description);
    }

    fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
        self.inner.register_counter(key, metadata)
    }

    fn register_gauge(&self, key: &Key, metadata: &Metadata<'_>) -> Gauge {
        self.inner.register_gauge(key, metadata)
    }

    fn register_histogram(&self, key: &Key, metadata: &Metadata<'_>) -> Histogram {
        if key.name() == DURATION_METRIC {
            self.duration_registrations.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.register_histogram(key, metadata)
    }
}

// Integration tests exercise the production cache, which is disabled in unit tests
// so individual unit tests can install independent local recorders.
#[test]
fn unary_read_duration_handles_are_cached_only_after_server_label_is_ready() {
    let recorder = RegistrationCountingRecorder::default();
    let snapshotter = recorder.inner.snapshotter();
    let metrics = InternodeMetrics::default();

    with_local_recorder(&recorder, || {
        for operation in READ_OPERATIONS {
            metrics.record_duration_for_operation_and_backend(operation, INTERNODE_TRANSPORT_BACKEND_GRPC, Duration::ZERO);
        }
        set_internode_server_label("cached-duration-test");

        for operation in READ_OPERATIONS {
            metrics.record_duration_for_operation_and_backend(
                operation,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                Duration::from_micros(125),
            );
            let registrations = recorder.duration_registrations.load(Ordering::Relaxed);
            metrics.record_duration_for_operation_and_backend(
                operation,
                INTERNODE_TRANSPORT_BACKEND_GRPC,
                Duration::from_micros(250),
            );
            assert_eq!(
                recorder.duration_registrations.load(Ordering::Relaxed),
                registrations,
                "{operation} should reuse its duration handle after warm-up"
            );
        }

        // Other backends and operations must retain their own labels and fallback.
        for (operation, backend) in READ_OPERATIONS
            .map(|operation| (operation, INTERNODE_TRANSPORT_BACKEND_UNKNOWN))
            .into_iter()
            .chain([(INTERNODE_OPERATION_GRPC_READ_ALL, INTERNODE_TRANSPORT_BACKEND_GRPC)])
        {
            let registrations = recorder.duration_registrations.load(Ordering::Relaxed);
            metrics.record_duration_for_operation_and_backend(operation, backend, Duration::from_millis(1));
            assert_eq!(recorder.duration_registrations.load(Ordering::Relaxed), registrations + 1);
        }
    });

    let rows = snapshotter.snapshot().into_vec();
    let durations: Vec<_> = rows
        .iter()
        .filter(|(key, _, _, _)| key.key().name() == DURATION_METRIC)
        .collect();
    assert_eq!(durations.len(), 10);
    for (key, _, _, value) in durations {
        let labels: Vec<_> = key.key().labels().map(|label| (label.key(), label.value())).collect();
        assert_eq!(labels.len(), 3);
        let server = labels.iter().find(|(name, _)| *name == "server").expect("server label").1;
        let operation = labels
            .iter()
            .find(|(name, _)| *name == "operation")
            .expect("operation label")
            .1;
        let backend = labels.iter().find(|(name, _)| *name == "backend").expect("backend label").1;
        let expected: &[f64] = if server == "unset" {
            assert!(READ_OPERATIONS.contains(&operation));
            assert_eq!(backend, INTERNODE_TRANSPORT_BACKEND_GRPC);
            &[0.0]
        } else {
            assert_eq!(server, "cached-duration-test");
            if backend == INTERNODE_TRANSPORT_BACKEND_GRPC && READ_OPERATIONS.contains(&operation) {
                &[0.125, 0.25]
            } else {
                assert!(
                    (backend == INTERNODE_TRANSPORT_BACKEND_UNKNOWN && READ_OPERATIONS.contains(&operation))
                        || (backend == INTERNODE_TRANSPORT_BACKEND_GRPC && operation == INTERNODE_OPERATION_GRPC_READ_ALL)
                );
                &[1.0]
            }
        };
        match value {
            DebugValue::Histogram(samples) => {
                assert_eq!(samples.iter().map(|sample| sample.0).collect::<Vec<_>>(), expected);
            }
            other => panic!("duration metric must be a histogram, got {other:?}"),
        }
    }
}
