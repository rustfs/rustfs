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

use rustfs::connect::DeviceIdentity;
use rustfs::connect::diagnostics::{
    LocalTopConsent, TopApiOperation, TopCaptureError, TopCaptureLimits, TopCaptureRequest, TopCaptureScope, TopOutcome,
    TopReasonCode, capture_top_api, sign_top_export,
};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

fn request(tool_id: &str) -> TopCaptureRequest {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    TopCaptureRequest {
        scope: TopCaptureScope {
            organization_name: "organizations/019e3ae0-0000-7000-8000-000000000010".to_owned(),
            cluster_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011"
                .to_owned(),
            device_name: "organizations/019e3ae0-0000-7000-8000-000000000010/clusters/019e3ae0-0000-7000-8000-000000000011/clusterDevices/019e3ae0-0000-7000-8000-000000000012".to_owned(),
            run_uid: "019e3ae0-0000-7000-8000-000000000001".to_owned(),
            artifact_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
            policy_revision: 1,
            run_expires_at_unix: now + 3_600,
            executable_sha256: "b".repeat(64),
            build_features: Vec::new(),
            consent: LocalTopConsent {
                uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
                tool_id: tool_id.to_owned(),
                classification: "L3".to_owned(),
                active: true,
                expires_at_unix: now + 3_600,
            },
        },
        limits: TopCaptureLimits::default(),
        window: Duration::from_millis(1),
        export_validity: Duration::from_secs(300),
    }
}

#[tokio::test]
async fn top_api_refuses_to_invent_the_missing_duration_counter() {
    let result = capture_top_api(&request("top.api"), TopApiOperation::GetObject, &CancellationToken::new())
        .await
        .expect("structured unsupported result");

    assert_eq!(result.outcome, TopOutcome::Unsupported);
    assert_eq!(result.reason_code, TopReasonCode::UnsupportedTool);
    assert!(result.data.is_none());
    let value = serde_json::to_value(&result).expect("result json");
    assert_eq!(value["toolId"], "top.api");
    assert_eq!(value["capability"], "top.api@1");
    assert_eq!(value["coverage"]["completedUnits"], 0);
    assert!(value["data"].is_null());
    assert_eq!(
        sign_top_export(&request("top.api"), &result, &DeviceIdentity::generate(), &CancellationToken::new()),
        Err(TopCaptureError::Result)
    );
}

#[tokio::test]
async fn top_api_enforces_local_consent_expiry_scope_and_limits_before_capture() {
    let mut inactive = request("top.api");
    inactive.scope.consent.active = false;
    assert_eq!(
        capture_top_api(&inactive, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::ConsentRequired)
    );

    let mut expired = request("top.api");
    expired.scope.consent.expires_at_unix = OffsetDateTime::now_utc().unix_timestamp();
    assert_eq!(
        capture_top_api(&expired, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::ConsentExpired)
    );

    let mut foreign_tool = request("top.disk");
    assert_eq!(
        capture_top_api(&foreign_tool, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::ConsentScope)
    );
    foreign_tool.scope.consent.tool_id = "top.api".to_owned();
    foreign_tool.scope.consent.classification = "L2".to_owned();
    assert_eq!(
        capture_top_api(&foreign_tool, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::ConsentScope)
    );

    let mut too_long = request("top.api");
    too_long.window = Duration::from_millis(30_001);
    assert_eq!(
        capture_top_api(&too_long, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::Limits)
    );
    let mut too_large = request("top.api");
    too_large.limits.max_result_bytes = 262_145;
    assert_eq!(
        capture_top_api(&too_large, TopApiOperation::GetObject, &CancellationToken::new()).await,
        Err(TopCaptureError::Limits)
    );
}

#[tokio::test]
async fn top_api_acknowledges_cancellation_without_data() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = capture_top_api(&request("top.api"), TopApiOperation::GetObject, &cancel)
        .await
        .expect("cancelled result");
    assert_eq!(result.outcome, TopOutcome::Cancelled);
    assert_eq!(result.reason_code, TopReasonCode::Cancelled);
    assert!(result.data.is_none());
}
