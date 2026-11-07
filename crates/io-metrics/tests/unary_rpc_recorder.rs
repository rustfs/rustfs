// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use rustfs_io_metrics::internode_metrics::{INTERNODE_OPERATION_GRPC_READ_ALL, measure_client_rpc, set_internode_server_label};

#[tokio::test]
async fn unary_rpc_handles_wait_for_recorder_installation() {
    set_internode_server_label("unary-recorder-test");
    // Exercise the real non-test library path before a process recorder exists.
    measure_client_rpc(INTERNODE_OPERATION_GRPC_READ_ALL, async { Ok::<_, ()>(()) })
        .await
        .expect("startup call");

    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::set_global_recorder(recorder).expect("install test recorder once");
    measure_client_rpc(INTERNODE_OPERATION_GRPC_READ_ALL, async { Ok::<_, ()>(()) })
        .await
        .expect("legacy duration before detailed metrics enablement");
    let before_enable = snapshotter.snapshot().into_vec();
    assert_eq!(
        before_enable.len(),
        1,
        "disabled detailed metrics register only the existing duration instrument"
    );
    assert_eq!(before_enable[0].0.key().name(), "rustfs_system_network_internode_operation_duration_ms");
    assert!(matches!(&before_enable[0].3, DebugValue::Histogram(samples) if samples.len() == 1));
    rustfs_io_metrics::set_metrics_enabled(true);
    for _ in 0..3 {
        measure_client_rpc(INTERNODE_OPERATION_GRPC_READ_ALL, async { Ok::<_, ()>(()) })
            .await
            .expect("recorded call");
    }
    let entries = snapshotter.snapshot().into_vec();
    let duration = entries
        .iter()
        .find(|(key, _, _, _)| {
            key.key().name() == "rustfs_system_network_internode_operation_duration_ms"
                && key
                    .key()
                    .labels()
                    .any(|label| label.key() == "operation" && label.value() == INTERNODE_OPERATION_GRPC_READ_ALL)
        })
        .expect("installed recorder receives the cached RPC duration handle");
    // Snapshotter drains histogram observations, so this snapshot contains the three later calls.
    assert!(matches!(&duration.3, DebugValue::Histogram(samples) if samples.len() == 3));
}
