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

mod connect {
    pub use rustfs::connect::DeviceIdentity;
}

#[allow(dead_code)]
#[path = "../src/connect/diagnostics/profile_cpu.rs"]
mod profile_cpu;
#[path = "../src/connect/diagnostics/profile_threads.rs"]
mod profile_threads;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use profile_cpu::{
    LocalProfileConsent, ProfileCaptureRequest, ProfileError, ProfileOutcome, ProfileProvenance, ProfileReasonCode,
    THREAD_PROFILE_CAPABILITY, ThreadProfileScope,
};
use profile_threads::capture_thread_profile;
use tokio_util::sync::CancellationToken;

fn request() -> ProfileCaptureRequest {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64;
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000021";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000022");
    ProfileCaptureRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000023"),
        run_uid: "019e3ae0-0000-7000-8000-000000000024".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000025".to_string(),
        schema_version: 1,
        capability: THREAD_PROFILE_CAPABILITY.to_string(),
        consent: LocalProfileConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000026".to_string(),
            policy_revision: 1,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x7c; 32],
        duration: Duration::from_secs(1),
        sample_period: Duration::from_millis(10),
        provenance: ProfileProvenance::new("e".repeat(40), "f".repeat(64), "1.0.0-rc.6", vec!["default".to_string()]),
    }
}

#[test]
fn tokio_and_native_thread_scopes_remain_explicitly_unsupported() {
    for scope in [ThreadProfileScope::TokioRuntime, ThreadProfileScope::NativeThreads] {
        let result = capture_thread_profile(&request(), scope, &CancellationToken::new()).expect("unsupported result");
        assert_eq!(result.outcome(), ProfileOutcome::Unsupported);
        assert_eq!(result.reason_code(), ProfileReasonCode::UnsupportedTool);
        assert!(result.data().is_none(), "unsupported scope must not publish zero state counts");
        let json = serde_json::to_value(result).expect("result JSON");
        assert_eq!(json["toolId"], "profile.threads");
        assert_eq!(json["capability"], "profile.threads@1");
        assert!(json["data"].is_null());
    }
}

#[test]
fn thread_profile_rejects_wrong_negotiation_and_cancellation() {
    let mut invalid = request();
    invalid.capability = "profile.threads@2".to_string();
    assert!(matches!(
        capture_thread_profile(&invalid, ThreadProfileScope::TokioRuntime, &CancellationToken::new()),
        Err(ProfileError::UnsupportedCapability)
    ));

    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        capture_thread_profile(&request(), ThreadProfileScope::NativeThreads, &cancel),
        Err(ProfileError::Cancelled)
    ));
}
