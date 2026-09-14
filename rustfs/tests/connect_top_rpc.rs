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

use std::time::Duration;

use rustfs::connect::diagnostics::{
    LocalTopConsent, TopCaptureLimits, TopCaptureRequest, TopCaptureScope, TopOutcome, TopReasonCode, capture_top_rpc,
};
use rustfs_common::trace_bus::{
    TelemetryTraceEvent, TelemetryTraceOperation, TelemetryTraceStatus, telemetry_trace_emit, telemetry_trace_subscriber_count,
};
use serial_test::serial;
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

fn request(window: Duration) -> TopCaptureRequest {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: "organizations/019e3ae0-0000-7000-8000-000000000010".to_owned(),
            cluster_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011".to_owned(),
            device_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011/clusterDevices/019e3ae0-0000-7000-8000-000000000012".to_owned(),
            run_uid: "019e3ae0-0000-7000-8000-000000000001".to_owned(),
            artifact_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
            policy_revision: 1,
            run_expires_at_unix: now + 3_600,
            executable_sha256: "b".repeat(64),
            build_features: Vec::new(),
            consent: LocalTopConsent {
                uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
                tool_id: "top.rpc".to_owned(),
                classification: "L3".to_owned(),
                active: true,
                expires_at_unix: now + 3_600,
            },
        },
        limits: TopCaptureLimits::default(),
        window,
        export_validity: Duration::from_secs(300),
    }
}

async fn wait_for_subscription(previous: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while telemetry_trace_subscriber_count() <= previous {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("top.rpc should subscribe to the runtime source");
}

#[tokio::test]
#[serial]
async fn top_rpc_captures_classified_rpc_outcomes_only() {
    let request = request(Duration::from_millis(25));
    let previous = telemetry_trace_subscriber_count();
    let capture = tokio::spawn(async move { capture_top_rpc(&request, &CancellationToken::new()).await });
    wait_for_subscription(previous).await;

    assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
        TelemetryTraceOperation::InternalRpc,
        Duration::from_micros(7),
        TelemetryTraceStatus::Ok,
    )));
    assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
        TelemetryTraceOperation::InternalRpc,
        Duration::from_micros(11),
        TelemetryTraceStatus::Error,
    )));
    assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
        TelemetryTraceOperation::GetObject,
        Duration::from_micros(13),
        TelemetryTraceStatus::Error,
    )));

    let result = capture.await.expect("capture task").expect("top.rpc result");
    assert_eq!(result.outcome, TopOutcome::Succeeded);
    assert_eq!(result.reason_code, TopReasonCode::Complete);
    let data = result.data.expect("successful capture data");
    assert_eq!(data.request_count, 2);
    assert_eq!(data.error_count, 1);
    assert_eq!(data.total_duration_micros, 18);
    let encoded = serde_json::to_value(data).expect("serialize top.rpc data");
    let mut keys = encoded
        .as_object()
        .expect("top.rpc object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    assert_eq!(keys, ["errorCount", "requestCount", "totalDurationMicros", "windowMillis"]);
}

#[tokio::test]
#[serial]
async fn top_rpc_cancellation_stops_without_exportable_data() {
    let request = request(Duration::from_secs(1));
    let cancel = CancellationToken::new();
    let task_cancel = cancel.clone();
    let previous = telemetry_trace_subscriber_count();
    let capture = tokio::spawn(async move { capture_top_rpc(&request, &task_cancel).await });
    wait_for_subscription(previous).await;
    cancel.cancel();

    let result = capture.await.expect("capture task").expect("cancelled result");
    assert_eq!(result.outcome, TopOutcome::Cancelled);
    assert_eq!(result.reason_code, TopReasonCode::Cancelled);
    assert!(result.data.is_none());
}

#[tokio::test]
#[serial]
async fn top_rpc_refuses_to_publish_a_truncated_window() {
    let mut request = request(Duration::from_secs(1));
    request.limits.max_operations = 1;
    request.limits.max_records = 1;
    let previous = telemetry_trace_subscriber_count();
    let capture = tokio::spawn(async move { capture_top_rpc(&request, &CancellationToken::new()).await });
    wait_for_subscription(previous).await;
    for _ in 0..2 {
        assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
            TelemetryTraceOperation::InternalRpc,
            Duration::from_micros(1),
            TelemetryTraceStatus::Ok,
        )));
    }

    let result = capture.await.expect("capture task").expect("limited result");
    assert_eq!(result.outcome, TopOutcome::Failed);
    assert_eq!(result.reason_code, TopReasonCode::LimitExceeded);
    assert!(result.data.is_none());
}

#[tokio::test(flavor = "current_thread")]
#[serial]
async fn top_rpc_fails_closed_when_the_source_lags() {
    let request = request(Duration::from_secs(1));
    let previous = telemetry_trace_subscriber_count();
    let capture = tokio::spawn(async move { capture_top_rpc(&request, &CancellationToken::new()).await });
    wait_for_subscription(previous).await;
    for _ in 0..1_025 {
        assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
            TelemetryTraceOperation::InternalRpc,
            Duration::from_micros(1),
            TelemetryTraceStatus::Ok,
        )));
    }

    let result = capture.await.expect("capture task").expect("lagged result");
    assert_eq!(result.outcome, TopOutcome::Failed);
    assert_eq!(result.reason_code, TopReasonCode::LimitExceeded);
    assert!(result.data.is_none());
}

#[tokio::test]
#[serial]
async fn top_rpc_rejects_duration_overflow() {
    let request = request(Duration::from_secs(1));
    let previous = telemetry_trace_subscriber_count();
    let capture = tokio::spawn(async move { capture_top_rpc(&request, &CancellationToken::new()).await });
    wait_for_subscription(previous).await;
    assert!(telemetry_trace_emit(|| TelemetryTraceEvent::new(
        TelemetryTraceOperation::InternalRpc,
        Duration::MAX,
        TelemetryTraceStatus::Ok,
    )));

    let result = capture.await.expect("capture task").expect("overflow result");
    assert_eq!(result.outcome, TopOutcome::Failed);
    assert_eq!(result.reason_code, TopReasonCode::CollectionFailed);
    assert!(result.data.is_none());
}
