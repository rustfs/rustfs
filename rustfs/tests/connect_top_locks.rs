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
    evaluate_lock_snapshot,
};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;

fn request() -> TopCaptureRequest {
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
                tool_id: "top.locks".to_owned(),
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
async fn top_locks_fails_when_the_server_lock_runtime_is_not_in_process() {
    let request = request();
    let result = capture_top_locks(&request, &CancellationToken::new())
        .await
        .expect("structured unsupported result");
    assert_eq!(result.outcome, TopOutcome::Failed);
    assert_eq!(result.reason_code, TopReasonCode::SourceUnavailable);
    assert!(result.data.is_none());
    let json = serde_json::to_string(&result).expect("locks json");
    assert!(!json.contains("resource"));
    assert!(!json.contains("owner"));
}

#[test]
fn top_locks_maps_real_runtime_counts_without_exposing_names() {
    let request = request();
    let result = evaluate_lock_snapshot(&request, 2, 1, 1_000).expect("lock count result");
    assert_eq!(result.outcome, TopOutcome::Succeeded);
    assert_eq!(result.reason_code, TopReasonCode::Complete);
    let data = result.data.expect("lock counts");
    assert_eq!(data.held_count, 2);
    assert_eq!(data.waiting_count, 1);
    assert!(!data.truncated);
    let json = serde_json::to_string(&data).expect("locks JSON");
    assert!(!json.contains("resource"));
    assert!(!json.contains("owner"));
}
