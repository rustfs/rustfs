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
    LocalTopConsent, TopCaptureLimits, TopCaptureRequest, TopCaptureScope, TopOutcome, TopReasonCode, capture_top_locks,
};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn top_locks_reports_unsupported_instead_of_inventing_waiters_or_exposing_names() {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let request = TopCaptureRequest {
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
                tool_id: "top.locks".to_owned(),
                classification: "L3".to_owned(),
                active: true,
                expires_at_unix: now + 3_600,
            },
        },
        limits: TopCaptureLimits::default(),
        window: Duration::from_millis(1),
        export_validity: Duration::from_secs(300),
    };

    let result = capture_top_locks(&request, &CancellationToken::new())
        .await
        .expect("structured unsupported result");
    assert_eq!(result.outcome, TopOutcome::Unsupported);
    assert_eq!(result.reason_code, TopReasonCode::UnsupportedTool);
    let json = serde_json::to_string(&result).expect("locks json");
    assert!(json.contains(r#""data":null"#));
    assert!(!json.contains("resource"));
    assert!(!json.contains("owner"));
}
