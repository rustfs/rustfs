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
#[path = "../src/connect/diagnostics/profile_memory.rs"]
mod profile_memory;

use std::fs;
use std::io::{Cursor, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use chrono::{SecondsFormat, Utc};
use ed25519_dalek::{Signer as _, SigningKey};
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use profile_cpu::{
    LocalProfileConsent, MEMORY_PROFILE_CAPABILITY, ProfileCaptureRequest, ProfileError, ProfileProvenance,
    save_signed_profile_export,
};
use profile_memory::{
    AllocationProfileSource, MEMORY_PROFILE_SERVICE_CAPABILITY, export_memory_profile, export_memory_profile_from,
    parse_allocator_stats,
};
use rustfs::connect::{
    DiagnosticJobEnvelope, DiagnosticJobError, DiagnosticJobTarget, TrustedDiagnosticJobSigner, execute_diagnostic_job,
};
use sha2::{Digest as _, Sha256};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;
use zip::ZipArchive;

static TEST_PROFILE_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

#[global_allocator]
static GLOBAL: rustfs_mimalloc::MiMalloc = rustfs_mimalloc::MiMalloc;

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn request() -> ProfileCaptureRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000011";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000012");
    ProfileCaptureRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000013"),
        run_uid: "019e3ae0-0000-7000-8000-000000000014".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000015".to_string(),
        schema_version: 1,
        capability: MEMORY_PROFILE_CAPABILITY.to_string(),
        consent: LocalProfileConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000016".to_string(),
            policy_revision: 7,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x6b; 32],
        duration: Duration::from_secs(1),
        sample_period: Duration::from_millis(10),
        provenance: ProfileProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec!["default".to_string()]),
    }
}

struct SequenceSource {
    values: Mutex<Vec<&'static str>>,
}

impl SequenceSource {
    fn new(first: &'static str, second: &'static str) -> Self {
        Self {
            values: Mutex::new(vec![second, first]),
        }
    }
}

impl AllocationProfileSource for SequenceSource {
    fn snapshot(&self) -> Result<profile_memory::AllocationSnapshot, ProfileError> {
        let value = self.values.lock().expect("sequence lock").pop().expect("two samples");
        parse_allocator_stats(value)
    }
}

fn stats(bytes: u64, count: u64) -> String {
    format!(
        r#"{{"malloc_normal":{{"total":{bytes}}},"malloc_huge":{{"total":0}},"malloc_normal_count":{{"total":{count}}},"malloc_huge_count":{{"total":0}}}}"#
    )
}

fn nested_string_stats(bytes: u64, count: u64) -> String {
    format!(
        r#"{{"process":{{"malloc_normal":{{"total":"{bytes}"}},"malloc_huge":{{"total":"0"}},"malloc_normal_count":{{"total":"{count}"}},"malloc_huge_count":{{"total":"0"}}}}}}"#
    )
}

fn archive_entry(archive: &mut ZipArchive<Cursor<Vec<u8>>>, name: &str) -> Vec<u8> {
    let mut entry = archive.by_name(name).expect("archive entry");
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).expect("read archive entry");
    bytes
}

fn service_job() -> DiagnosticJobEnvelope {
    let request = request();
    let now = Utc::now();
    serde_json::from_value(serde_json::json!({
        "jobId": request.run_uid,
        "protocolVersion": "v1",
        "jobType": "profile.memory",
        "schemaVersion": 1,
        "organizationName": request.organization_name,
        "clusterName": request.cluster_name,
        "deviceName": request.device_name,
        "createTime": now.to_rfc3339_opts(SecondsFormat::Secs, true),
        "expireTime": (now + chrono::Duration::seconds(30)).to_rfc3339_opts(SecondsFormat::Secs, true),
        "nonce": URL_SAFE_NO_PAD.encode_to_string(request.nonce),
        "requiredCapabilities": [MEMORY_PROFILE_SERVICE_CAPABILITY],
        "authorization": {
            "actorType": "BROWSER_USER",
            "actorName": "users/019e3ae0-0000-7000-8000-000000000018",
            "requestId": "123e4567-e89b-42d3-a456-426614174001"
        },
        "limits": {
            "timeoutSeconds": 30,
            "maxOutputBytes": 524288,
            "maxMemoryBytes": 67108864,
            "maxCpuMillis": 5000
        },
        "parameters": {
            "artifactUid": request.artifact_uid,
            "consentUid": request.consent.consent_uid,
            "consentPolicyRevision": request.consent.policy_revision,
            "consentExpiresAt": (now + chrono::Duration::seconds(60)).to_rfc3339_opts(SecondsFormat::Secs, true),
            "durationMillis": 1000,
            "samplePeriodMicros": 10000
        },
        "signature": {"algorithm": "Ed25519", "keyId": "", "value": ""}
    }))
    .expect("service job fixture")
}

fn sign_service_job(job: DiagnosticJobEnvelope) -> (DiagnosticJobEnvelope, TrustedDiagnosticJobSigner) {
    let key = SigningKey::from_bytes(&[0x17; 32]);
    let key_id = hex(&Sha256::digest(key.verifying_key().as_bytes()));
    let signer = TrustedDiagnosticJobSigner::new(key_id.clone(), *key.verifying_key().as_bytes()).expect("trusted signer");
    let signature = URL_SAFE_NO_PAD.encode_to_string(key.sign(&job.signing_payload().expect("job payload")).to_bytes());
    let mut value = serde_json::to_value(job).expect("job JSON");
    value["signature"]["keyId"] = key_id.into();
    value["signature"]["value"] = signature.into();
    (serde_json::from_value(value).expect("signed job"), signer)
}

fn service_target(job: &DiagnosticJobEnvelope) -> DiagnosticJobTarget {
    DiagnosticJobTarget {
        organization_name: job.organization_name.clone(),
        cluster_name: job.cluster_name.clone(),
        device_name: job.device_name.clone(),
    }
}

#[test]
fn service_job_requires_the_independently_negotiated_capability_and_signed_bounds() {
    let (job, signer) = sign_service_job(service_job());
    let target = service_target(&job);
    signer.verify(&job, &target, Utc::now()).expect("bounded service job");

    for capabilities in [
        vec![MEMORY_PROFILE_CAPABILITY.to_owned()],
        vec![
            MEMORY_PROFILE_SERVICE_CAPABILITY.to_owned(),
            MEMORY_PROFILE_CAPABILITY.to_owned(),
        ],
        vec![],
    ] {
        let mut candidate = job.clone();
        candidate.required_capabilities = capabilities;
        let (candidate, signer) = sign_service_job(candidate);
        assert_eq!(signer.verify(&candidate, &target, Utc::now()), Err(DiagnosticJobError::Unsupported));
    }
    let mut limits = vec![job.limits.clone(); 4];
    limits[0].max_cpu_millis = 5001;
    limits[1].max_memory_bytes = 67108863;
    limits[2].max_output_bytes = 524289;
    limits[3].timeout_seconds = 31;
    for limit in limits {
        let mut candidate = job.clone();
        candidate.limits = limit;
        let (candidate, signer) = sign_service_job(candidate);
        assert_eq!(signer.verify(&candidate, &target, Utc::now()), Err(DiagnosticJobError::LimitExceeded));
    }
    let mut tampered = job.clone();
    tampered.parameters.sample_period_micros += 1;
    assert_eq!(signer.verify(&tampered, &target, Utc::now()), Err(DiagnosticJobError::SignatureInvalid));

    let mut wrong_target = target.clone();
    wrong_target.organization_name.push('x');
    assert_eq!(signer.verify(&job, &wrong_target, Utc::now()), Err(DiagnosticJobError::TargetMismatch));
    wrong_target = target.clone();
    wrong_target.device_name.push('x');
    assert_eq!(signer.verify(&job, &wrong_target, Utc::now()), Err(DiagnosticJobError::TargetMismatch));

    let mut expired = job;
    expired.parameters.consent_expires_at =
        (Utc::now() - chrono::Duration::seconds(1)).to_rfc3339_opts(SecondsFormat::Secs, true);
    let (expired, signer) = sign_service_job(expired);
    assert_eq!(signer.verify(&expired, &target, Utc::now()), Err(DiagnosticJobError::Expired));
}

#[tokio::test]
async fn signed_service_memory_job_exports_the_same_process_allocator_with_original_bindings() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let (job, signer) = sign_service_job(service_job());
    let verified = signer
        .verify(&job, &service_target(&job), Utc::now())
        .expect("verified memory job");
    let identity = connect::DeviceIdentity::generate();
    let provenance = rustfs::connect::ProfileProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec![]);
    // This invokes the service dispatcher directly in this mimalloc-using process,
    // not the standalone CLI or a fixture allocation source.
    let execution = execute_diagnostic_job(verified, &identity, provenance, &CancellationToken::new()).await;
    #[cfg(target_os = "windows")]
    {
        assert_eq!(execution, Err(DiagnosticJobError::ProfileSourceUnavailable));
    }
    #[cfg(not(target_os = "windows"))]
    {
        let execution = execution.expect("service allocator capture");
        assert_eq!(execution.job_id, job.job_id);
        assert_eq!(execution.outcome, "SUCCEEDED");
        assert_eq!(execution.reason, "COMPLETE");
        assert_eq!(execution.artifact_uid.as_deref(), Some(job.parameters.artifact_uid.as_str()));
        let bytes = execution.artifact_bytes.expect("signed artifact");
        assert_eq!(execution.artifact_sha256, Some(hex(&Sha256::digest(&bytes))));
        let mut archive = ZipArchive::new(Cursor::new(bytes)).expect("profile archive");
        let envelope_bytes = archive_entry(&mut archive, "envelope.json");
        let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
        let result_bytes = archive_entry(&mut archive, "result.json");
        let result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");
        assert_eq!(result["toolId"], "profile.memory");
        assert_eq!(result["schemaVersion"], 1);
        assert_eq!(result["capability"], MEMORY_PROFILE_CAPABILITY);
        assert_eq!(result["runUid"], job.job_id);
        assert_eq!(result["provenance"]["repository"], "rustfs/rustfs");
        assert_eq!(result["provenance"]["sourceCommit"], "c".repeat(40));
        assert_eq!(result["provenance"]["executableSha256"], "d".repeat(64));
        assert_eq!(result["data"]["scope"], "ALLOCATION_AGGREGATES");
        assert!(result["data"]["allocatedBytes"].is_u64());
        assert!(result["data"]["allocationCount"].is_u64());
        assert_eq!(envelope["organizationName"], job.organization_name);
        assert_eq!(envelope["clusterName"], job.cluster_name);
        assert_eq!(envelope["deviceName"], job.device_name);
        assert_eq!(envelope["artifactUid"], job.parameters.artifact_uid);
        assert_eq!(envelope["runUid"], job.job_id);
        assert_eq!(envelope["consentUid"], job.parameters.consent_uid);
        assert_eq!(envelope["policyRevision"], job.parameters.consent_policy_revision);
        assert_eq!(envelope["classification"], "L3");
        assert_eq!(envelope["nonce"], job.nonce);
        assert_eq!(envelope["payload"]["sha256"], hex(&Sha256::digest(&result_bytes)));
        let signature: serde_json::Value =
            serde_json::from_slice(&archive_entry(&mut archive, "envelope.sig")).expect("signature JSON");
        let raw = URL_SAFE_NO_PAD
            .decode_to_vec(signature["value"].as_str().expect("signature"))
            .expect("base64 signature");
        let signature = Signature::from_slice(&raw).expect("ES256 signature");
        let public = VerifyingKey::from_public_key_der(&identity.public_key_der()).expect("device public key");
        let mut input = b"rustfs-diagnostic-envelope-v1\0".to_vec();
        input.extend_from_slice(&envelope_bytes);
        public.verify(&input, &signature).expect("original device signed artifact");
    }
}

#[tokio::test]
async fn cancelled_service_memory_job_produces_no_artifact() {
    let (job, signer) = sign_service_job(service_job());
    let verified = signer
        .verify(&job, &service_target(&job), Utc::now())
        .expect("verified memory job");
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert_eq!(
        execute_diagnostic_job(
            verified,
            &connect::DeviceIdentity::generate(),
            rustfs::connect::ProfileProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec![]),
            &cancel,
        )
        .await,
        Err(DiagnosticJobError::Cancelled)
    );
}

#[tokio::test]
async fn service_memory_job_honors_cancellation_during_capture_and_the_signed_output_limit() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let mut job = service_job();
    job.parameters.sample_period_micros = 500_000;
    let (job, signer) = sign_service_job(job);
    let verified = signer
        .verify(&job, &service_target(&job), Utc::now())
        .expect("verified memory job");
    let identity = connect::DeviceIdentity::generate();
    let provenance = rustfs::connect::ProfileProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec![]);
    let cancel = CancellationToken::new();
    let execution = execute_diagnostic_job(verified, &identity, provenance.clone(), &cancel);
    let cancellation = async {
        // The first join branch polls the collector through its first snapshot
        // into the bounded wait before this branch cancels the same token.
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(biased; execution, cancellation);
    #[cfg(not(target_os = "windows"))]
    assert_eq!(result, Err(DiagnosticJobError::Cancelled));
    #[cfg(target_os = "windows")]
    assert_eq!(result, Err(DiagnosticJobError::ProfileSourceUnavailable));

    let mut job = service_job();
    job.limits.max_output_bytes = 1;
    let (job, signer) = sign_service_job(job);
    let verified = signer
        .verify(&job, &service_target(&job), Utc::now())
        .expect("bounded output memory job");
    let result = execute_diagnostic_job(verified, &identity, provenance, &CancellationToken::new()).await;
    #[cfg(not(target_os = "windows"))]
    assert_eq!(result, Err(DiagnosticJobError::LimitExceeded));
    #[cfg(target_os = "windows")]
    assert_eq!(result, Err(DiagnosticJobError::ProfileSourceUnavailable));
}

#[tokio::test]
async fn real_mimalloc_profile_produces_a_signed_three_file_export() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let key = connect::DeviceIdentity::generate();
    let export = export_memory_profile(&request(), &key, &CancellationToken::new())
        .await
        .expect("real allocation aggregate export");
    assert!(export.archive_bytes.len() <= profile_cpu::MAX_ARCHIVE_BYTES);
    assert_eq!(export.archive_sha256, hex(&Sha256::digest(&export.archive_bytes)));

    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("profile archive");
    assert_eq!(archive.len(), 3);
    let envelope_bytes = archive_entry(&mut archive, "envelope.json");
    let signature_bytes = archive_entry(&mut archive, "envelope.sig");
    let result_bytes = archive_entry(&mut archive, "result.json");
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
    let signature: serde_json::Value = serde_json::from_slice(&signature_bytes).expect("signature JSON");
    let result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");

    assert_eq!(envelope["classification"], "L3");
    assert_eq!(envelope["payload"]["path"], "result.json");
    assert_eq!(envelope["payload"]["sizeBytes"], result_bytes.len());
    assert_eq!(envelope["payload"]["sha256"], hex(&Sha256::digest(&result_bytes)));
    assert_eq!(result["toolId"], "profile.memory");
    assert_eq!(result["outcome"], "SUCCEEDED");
    assert_eq!(result["data"]["scope"], "ALLOCATION_AGGREGATES");
    assert!(result["data"]["allocatedBytes"].is_u64());
    assert!(result["data"]["allocationCount"].is_u64());
    assert!(result["data"]["samplePeriodMicros"].as_u64().is_some_and(|value| value > 0));
    let encoded = serde_json::to_string(&result).expect("encoded result");
    for forbidden in ["/Users/", "/proc/", "AKIA", "secret", "stack", "symbol"] {
        assert!(!encoded.contains(forbidden), "result leaked forbidden material: {forbidden}");
    }

    assert_eq!(signature["algorithm"], "ES256");
    let raw_signature = URL_SAFE_NO_PAD
        .decode_to_vec(signature["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    assert_eq!(raw_signature.len(), 64);
    let signature_value = Signature::from_slice(&raw_signature).expect("P-256 signature");
    assert_eq!(signature_value.normalize_s(), signature_value);
    let public = VerifyingKey::from_public_key_der(&key.public_key_der()).expect("public key");
    let mut input = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    input.extend_from_slice(&envelope_bytes);
    public
        .verify(&input, &signature_value)
        .expect("signature over exact envelope bytes");
}

#[tokio::test]
async fn memory_profile_reports_counter_reset_and_cancellation_without_an_artifact() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let first = Box::leak(stats(100, 10).into_boxed_str());
    let second = Box::leak(stats(90, 11).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let key = connect::DeviceIdentity::generate();
    assert!(matches!(
        export_memory_profile_from(&request(), &key, &CancellationToken::new(), &source).await,
        Err(ProfileError::CounterReset)
    ));

    let first = Box::leak(stats(100, 10).into_boxed_str());
    let second = Box::leak(stats(120, 12).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let mut request = request();
    request.sample_period = Duration::from_secs(1);
    request.duration = Duration::from_secs(2);
    let cancel = CancellationToken::new();
    let cancellation = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::join!(export_memory_profile_from(&request, &key, &cancel, &source), cancellation);
    assert!(matches!(result, Err(ProfileError::Cancelled)));
}

#[tokio::test]
async fn memory_profile_uses_only_bounded_allocator_aggregates() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let first = Box::leak(stats(1_000, 20).into_boxed_str());
    let second = Box::leak(stats(1_250, 24).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let key = connect::DeviceIdentity::generate();
    let export = export_memory_profile_from(&request(), &key, &CancellationToken::new(), &source)
        .await
        .expect("aggregate export");
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("profile archive");
    let result: serde_json::Value = serde_json::from_slice(&archive_entry(&mut archive, "result.json")).expect("result JSON");
    assert_eq!(result["data"]["allocatedBytes"], 250);
    assert_eq!(result["data"]["allocationCount"], 4);

    let oversized = format!("{}{}", stats(1, 1), " ".repeat(262_145));
    assert!(matches!(parse_allocator_stats(&oversized), Err(ProfileError::SourceUnavailable)));
}

#[tokio::test(start_paused = true)]
async fn memory_profile_accepts_nested_string_mimalloc_totals() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let first = Box::leak(nested_string_stats(1_000, 20).into_boxed_str());
    let second = Box::leak(nested_string_stats(1_250, 24).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let key = connect::DeviceIdentity::generate();
    let export = export_memory_profile_from(&request(), &key, &CancellationToken::new(), &source)
        .await
        .expect("nested aggregate export");
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes)).expect("profile archive");
    let result: serde_json::Value = serde_json::from_slice(&archive_entry(&mut archive, "result.json")).expect("result JSON");

    assert_eq!(result["data"]["allocatedBytes"], 250);
    assert_eq!(result["data"]["allocationCount"], 4);
}

#[tokio::test]
async fn memory_profile_allows_only_one_collector_at_a_time() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let first = Box::leak(stats(100, 10).into_boxed_str());
    let second = Box::leak(stats(120, 12).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let second_source = SequenceSource::new(first, second);
    let key = connect::DeviceIdentity::generate();
    let mut request = request();
    request.sample_period = Duration::from_millis(30);

    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let first_capture = export_memory_profile_from(&request, &key, &first_cancel, &source);
    let second_capture = async {
        tokio::time::sleep(Duration::from_millis(5)).await;
        export_memory_profile_from(&request, &key, &second_cancel, &second_source).await
    };
    let (first_result, second_result) = tokio::join!(first_capture, second_capture);
    assert!(first_result.is_ok());
    assert!(matches!(second_result, Err(ProfileError::Busy)));
}

#[tokio::test]
async fn signed_export_is_private_no_clobber_and_cancel_safe() {
    let _guard = TEST_PROFILE_LOCK.lock().await;
    let first = Box::leak(stats(100, 10).into_boxed_str());
    let second = Box::leak(stats(150, 12).into_boxed_str());
    let source = SequenceSource::new(first, second);
    let key = connect::DeviceIdentity::generate();
    let export = export_memory_profile_from(&request(), &key, &CancellationToken::new(), &source)
        .await
        .expect("aggregate export");
    let directory = tempfile::tempdir().expect("temporary output");
    let output = directory.path().join("profile.zip");
    let receipt = save_signed_profile_export(&output, &export, &CancellationToken::new()).expect("save export");
    assert_eq!(receipt.archive_sha256, export.archive_sha256);
    assert_eq!(receipt.archive_size_bytes, export.archive_bytes.len() as u64);
    assert_eq!(fs::read(&output).expect("saved archive"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("metadata").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_profile_export(&output, &export, &CancellationToken::new()),
        Err(ProfileError::AlreadyExists)
    ));

    let cancelled_output = directory.path().join("cancelled.zip");
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(matches!(
        save_signed_profile_export(&cancelled_output, &export, &cancelled),
        Err(ProfileError::Cancelled)
    ));
    assert!(!cancelled_output.exists());
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
