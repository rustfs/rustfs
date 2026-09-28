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

use std::io::Read as _;
use std::time::Duration;

use base64_simd::URL_SAFE_NO_PAD;
use chrono::{TimeDelta, Utc};
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::{
    DeviceIdentity, HEALTH_CATALOG_CHECKS, HEALTH_SCHEMA_VERSION, HEALTH_SERVICE_CAPABILITY, HealthError, HealthServiceRequest,
    HealthSourceObservation, LocalHealthConsent, ProfileProvenance, collect_runtime_health, evaluate_health_observation,
    sign_health_export,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

const SIGNATURE_DOMAIN: &[u8] = b"rustfs-diagnostic-envelope-v1\0";

fn request(now: i64) -> HealthServiceRequest {
    HealthServiceRequest {
        organization_name: "organizations/018cc251-f400-7abc-8def-0123456789ab".to_owned(),
        cluster_name: "organizations/018cc251-f400-7abc-8def-0123456789ab/clusters/018cc251-f400-7abc-8def-0123456789ac"
            .to_owned(),
        device_name: "organizations/018cc251-f400-7abc-8def-0123456789ab/clusters/018cc251-f400-7abc-8def-0123456789ac/clusterDevices/018cc251-f400-7abc-8def-0123456789ad".to_owned(),
        run_uid: "018cc251-f400-7abc-8def-0123456789ab".to_owned(),
        artifact_uid: "018cc251-f400-7abc-8def-0123456789ae".to_owned(),
        schema_version: HEALTH_SCHEMA_VERSION,
        capability: HEALTH_SERVICE_CAPABILITY.to_owned(),
        consent: LocalHealthConsent {
            consent_uid: "018cc251-f400-7abc-8def-0123456789af".to_owned(),
            policy_revision: 1,
            expires_at_unix: now + 120,
            active: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [7; 32],
        max_evidence_age_seconds: 300,
        provenance: ProfileProvenance::new("a".repeat(40), "b".repeat(64), "1.0.1", vec![]),
    }
}

fn observation(now: i64, total: u64, used: u64, flags: Vec<&str>) -> HealthSourceObservation {
    HealthSourceObservation {
        observed_at_unix: Some(now),
        capacity_complete: true,
        total_bytes: Some(total),
        used_bytes: Some(used),
        coarse_flags_complete: true,
        coarse_flags: Some(flags.into_iter().map(str::to_owned).collect()),
    }
}

fn json_result(
    request: &HealthServiceRequest,
    observation: HealthSourceObservation,
    evaluated_at: chrono::DateTime<Utc>,
) -> Value {
    serde_json::to_value(evaluate_health_observation(request, observation, evaluated_at, Duration::from_millis(25)))
        .expect("health result")
}

#[test]
fn health_rules_keep_exact_boundaries_and_full_catalog_coverage() {
    let now = Utc::now();
    let request = request(now.timestamp());

    let remaining = json_result(&request, observation(now.timestamp(), 100, 99, vec![]), now);
    assert_eq!(remaining["data"]["checks"][0]["outcome"], "PASS");
    assert_eq!(remaining["data"]["checks"][0]["reasonCode"], "CAPACITY_REMAINING");
    assert_eq!(remaining["data"]["checks"][1]["outcome"], "PASS");
    assert_eq!(remaining["coverage"]["requestedUnits"], HEALTH_CATALOG_CHECKS);
    assert_eq!(remaining["coverage"]["completedUnits"], 2);
    assert_eq!(remaining["coverage"]["unsupportedUnits"], 11);
    assert_eq!(remaining["data"]["unsupportedChecks"].as_array().expect("unsupported").len(), 11);
    assert_eq!(remaining["data"]["unsupportedChecks"][0]["checkId"], "memory.reportedPressure");

    let exhausted = json_result(&request, observation(now.timestamp(), 100, 100, vec![]), now);
    assert_eq!(exhausted["data"]["checks"][0]["outcome"], "FAIL");
    assert_eq!(exhausted["data"]["checks"][0]["reasonCode"], "CAPACITY_EXHAUSTED");

    let zero = json_result(&request, observation(now.timestamp(), 0, 0, vec![]), now);
    assert_eq!(zero["data"]["checks"][0]["outcome"], "UNKNOWN");
    assert_eq!(zero["data"]["checks"][0]["reasonCode"], "CAPACITY_UNAVAILABLE");

    let invalid = json_result(&request, observation(now.timestamp(), 99, 100, vec![]), now);
    assert_eq!(invalid["data"]["checks"][0]["outcome"], "UNKNOWN");
    assert_eq!(invalid["data"]["checks"][0]["reasonCode"], "INVALID_EVIDENCE");

    let maximum = json_result(
        &request,
        observation(now.timestamp(), 9_007_199_254_740_991, 9_007_199_254_740_991, vec![]),
        now,
    );
    assert_eq!(maximum["data"]["checks"][0]["outcome"], "FAIL");
    assert_eq!(maximum["data"]["checks"][0]["reasonCode"], "CAPACITY_EXHAUSTED");
}

#[tokio::test]
async fn cancelled_health_collection_stops_before_runtime_access() {
    let now = Utc::now();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        collect_runtime_health(&request(now.timestamp()), &DeviceIdentity::generate(), &cancel).await,
        Err(HealthError::Cancelled)
    );
}

#[test]
fn health_freshness_requires_real_time_and_has_no_silent_default() {
    let now = Utc::now();
    let request = request(now.timestamp());

    let equality = json_result(&request, observation((now - TimeDelta::seconds(300)).timestamp(), 100, 50, vec![]), now);
    assert_eq!(equality["data"]["observations"]["capacity"]["freshness"], "CURRENT");

    let stale = json_result(&request, observation((now - TimeDelta::seconds(301)).timestamp(), 100, 50, vec![]), now);
    assert_eq!(stale["reasonCode"], "EVIDENCE_STALE");
    assert_eq!(stale["data"]["checks"][0]["reasonCode"], "EVIDENCE_STALE");

    let future = json_result(&request, observation((now + TimeDelta::seconds(1)).timestamp(), 100, 50, vec![]), now);
    assert_eq!(future["reasonCode"], "CLOCK_SKEW");
    assert_eq!(future["data"]["checks"][0]["reasonCode"], "CLOCK_SKEW");

    let missing = json_result(
        &request,
        HealthSourceObservation {
            observed_at_unix: None,
            capacity_complete: false,
            total_bytes: None,
            used_bytes: None,
            coarse_flags_complete: false,
            coarse_flags: None,
        },
        now,
    );
    assert_eq!(missing["reasonCode"], "EVIDENCE_FRESHNESS_UNKNOWN");
    assert_eq!(missing["data"]["checks"][1]["outcome"], "UNKNOWN");
}

#[test]
fn every_allowed_coarse_flag_is_a_fixed_failure_without_raw_source_identity() {
    let now = Utc::now();
    let request = request(now.timestamp());
    for flag in [
        "capacity.critical",
        "capacity.warning",
        "clock.skew",
        "cluster.degraded",
        "cluster.healing",
        "cluster.readonly",
        "drive.offline",
        "node.offline",
    ] {
        let result = json_result(&request, observation(now.timestamp(), 100, 50, vec![flag]), now);
        assert_eq!(result["data"]["checks"][1]["outcome"], "FAIL", "{flag}");
        assert_eq!(result["data"]["checks"][1]["reasonCode"], "COARSE_CONDITION_REPORTED");
        let encoded = serde_json::to_string(&result).expect("encoded result");
        assert!(!encoded.contains("hostname"));
        assert!(!encoded.contains("endpoint"));
        assert!(!encoded.contains("mountPath"));
        assert!(!encoded.contains("commandLine"));
    }
}

#[test]
fn signed_health_export_binds_target_nonce_consent_and_result_digest() {
    let now = Utc::now();
    let request = request(now.timestamp());
    let identity = DeviceIdentity::generate();
    let result = evaluate_health_observation(
        &request,
        observation(now.timestamp(), 100, 50, vec!["cluster.healing"]),
        now,
        Duration::from_millis(25),
    );
    let export = sign_health_export(&request, &result, &identity, &CancellationToken::new()).expect("signed export");
    assert!(export.archive_bytes.len() < 262_144);

    let envelope: Value = serde_json::from_slice(&export.envelope_json).expect("envelope");
    assert_eq!(envelope["organizationName"], request.organization_name);
    assert_eq!(envelope["clusterName"], request.cluster_name);
    assert_eq!(envelope["deviceName"], request.device_name);
    assert_eq!(envelope["runUid"], request.run_uid);
    assert_eq!(envelope["consentUid"], request.consent.consent_uid);
    assert_eq!(envelope["nonce"], URL_SAFE_NO_PAD.encode_to_string(request.nonce));
    assert_eq!(envelope["payload"]["sha256"], hex_lower(&Sha256::digest(&export.result_json)));
    assert_eq!(envelope["classification"], "L0");

    let signature_document: Value = serde_json::from_slice(&export.envelope_signature).expect("signature document");
    let signature_bytes = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value").as_bytes())
        .expect("base64 signature");
    let signature = Signature::from_slice(&signature_bytes).expect("signature");
    let verifying_key = VerifyingKey::from_public_key_der(&identity.public_key_der()).expect("public key");
    let mut signed = SIGNATURE_DOMAIN.to_vec();
    signed.extend_from_slice(&export.envelope_json);
    verifying_key.verify(&signed, &signature).expect("valid ES256 signature");

    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(export.archive_bytes)).expect("health archive");
    let mut archived_result = Vec::new();
    archive
        .by_name("result.json")
        .expect("result entry")
        .read_to_end(&mut archived_result)
        .expect("read result");
    assert_eq!(archived_result, export.result_json);
}

#[test]
fn revoked_consent_cancellation_and_oversized_results_fail_closed() {
    let now = Utc::now();
    let identity = DeviceIdentity::generate();
    let mut request = request(now.timestamp());
    let result =
        evaluate_health_observation(&request, observation(now.timestamp(), 100, 50, vec![]), now, Duration::from_millis(1));

    request.consent.active = false;
    assert_eq!(
        sign_health_export(&request, &result, &identity, &CancellationToken::new()),
        Err(HealthError::ConsentRequired)
    );

    request.consent.active = true;
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(sign_health_export(&request, &result, &identity, &cancelled), Err(HealthError::Cancelled));

    let mut invalid_provenance = request.clone();
    invalid_provenance.provenance =
        ProfileProvenance::new("not-a-commit", "not-a-digest", "development build", vec!["unsafe feature".to_owned()]);
    assert_eq!(
        sign_health_export(&invalid_provenance, &result, &identity, &CancellationToken::new()),
        Err(HealthError::InvalidRequest)
    );

    let oversized = evaluate_health_observation(
        &request,
        HealthSourceObservation {
            coarse_flags: Some(vec!["cluster.healing".to_owned(); 32_768]),
            ..observation(now.timestamp(), 100, 50, vec![])
        },
        now,
        Duration::from_millis(1),
    );
    assert_eq!(
        sign_health_export(&request, &oversized, &identity, &CancellationToken::new()),
        Err(HealthError::LimitExceeded)
    );
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut value, byte| {
        use std::fmt::Write as _;
        let _ = write!(value, "{byte:02x}");
        value
    })
}
