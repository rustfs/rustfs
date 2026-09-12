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

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use profile_cpu::{
    CPU_PROFILE_CAPABILITY, LocalProfileConsent, MAX_PROFILE_DURATION, ProfileCaptureRequest, ProfileError, ProfileOutcome,
    ProfileProvenance, ProfileReasonCode, capture_cpu_profile,
};
use tokio_util::sync::CancellationToken;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn request() -> ProfileCaptureRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000001";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000002");
    ProfileCaptureRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000003"),
        run_uid: "019e3ae0-0000-7000-8000-000000000004".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000005".to_string(),
        schema_version: 1,
        capability: CPU_PROFILE_CAPABILITY.to_string(),
        consent: LocalProfileConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000006".to_string(),
            policy_revision: 1,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x5a; 32],
        duration: Duration::from_secs(1),
        sample_period: Duration::from_millis(10),
        provenance: ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-rc.6", vec!["default".to_string()]),
    }
}

#[test]
fn cpu_without_reviewed_symbol_catalog_is_explicitly_unsupported() {
    let result = capture_cpu_profile(&request(), &CancellationToken::new()).expect("unsupported is a typed result");
    assert_eq!(result.outcome(), ProfileOutcome::Unsupported);
    assert_eq!(result.reason_code(), ProfileReasonCode::UnsupportedTool);
    assert!(result.data().is_none(), "unsupported CPU must not fabricate zero samples");

    let json = serde_json::to_value(result).expect("result JSON");
    assert_eq!(json["toolId"], "profile.cpu");
    assert_eq!(json["capability"], "profile.cpu@1");
    assert_eq!(json["coverage"]["requestedUnits"], 0);
    assert_eq!(json["coverage"]["completedUnits"], 0);
    assert!(json["data"].is_null());
}

#[test]
fn cpu_refuses_missing_or_expired_local_consent_before_capability_disclosure() {
    let mut missing = request();
    missing.consent.confirmed = false;
    assert!(matches!(
        capture_cpu_profile(&missing, &CancellationToken::new()),
        Err(ProfileError::ConsentRequired)
    ));

    let mut expired = request();
    expired.consent.expires_at_unix = now() - 1;
    assert!(matches!(
        capture_cpu_profile(&expired, &CancellationToken::new()),
        Err(ProfileError::ConsentExpired)
    ));
}

#[test]
fn cpu_negotiation_and_resource_limits_are_closed_at_the_boundary() {
    let mut invalid = request();
    invalid.schema_version = 2;
    assert!(matches!(
        capture_cpu_profile(&invalid, &CancellationToken::new()),
        Err(ProfileError::UnsupportedVersion)
    ));

    let mut invalid = request();
    invalid.capability = "profile.cpu@2".to_string();
    assert!(matches!(
        capture_cpu_profile(&invalid, &CancellationToken::new()),
        Err(ProfileError::UnsupportedCapability)
    ));

    let mut boundary = request();
    boundary.duration = MAX_PROFILE_DURATION;
    boundary.sample_period = MAX_PROFILE_DURATION;
    assert!(capture_cpu_profile(&boundary, &CancellationToken::new()).is_ok());

    let mut over = request();
    over.duration = MAX_PROFILE_DURATION + Duration::from_nanos(1);
    assert!(matches!(
        capture_cpu_profile(&over, &CancellationToken::new()),
        Err(ProfileError::LimitExceeded)
    ));

    let mut features = request();
    features.provenance = ProfileProvenance::new(
        "a".repeat(40),
        "b".repeat(64),
        "1.0.0",
        (0..65).map(|index| format!("feature_{index}")).collect(),
    );
    assert!(matches!(
        capture_cpu_profile(&features, &CancellationToken::new()),
        Err(ProfileError::InvalidRequest)
    ));

    let mut empty_version_suffix = request();
    empty_version_suffix.provenance =
        ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-", vec!["default".to_string()]);
    assert!(matches!(
        capture_cpu_profile(&empty_version_suffix, &CancellationToken::new()),
        Err(ProfileError::InvalidRequest)
    ));

    let mut overflowing_window = request();
    overflowing_window.produced_at_unix = i64::MIN;
    overflowing_window.expires_at_unix = i64::MAX;
    overflowing_window.consent.expires_at_unix = i64::MAX;
    assert!(matches!(
        capture_cpu_profile(&overflowing_window, &CancellationToken::new()),
        Err(ProfileError::Expired)
    ));
}

#[test]
fn cpu_honors_pre_cancelled_capture() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(capture_cpu_profile(&request(), &cancel), Err(ProfileError::Cancelled)));
}
