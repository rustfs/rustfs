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

use std::fs;
use std::io::{Cursor, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::DeviceIdentity;
use rustfs::connect::diagnostics::{
    LocalTopConsent, NetworkCounterSnapshot, TopCaptureLimits, TopCaptureRequest, TopCaptureScope, TopOutcome, TopReasonCode,
    evaluate_network_window, save_signed_top_export, sign_top_export,
};
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use zip::ZipArchive;

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
                tool_id: "top.net".to_owned(),
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

fn archive_entry(archive: &mut ZipArchive<Cursor<Vec<u8>>>, name: &str) -> Vec<u8> {
    let mut entry = archive.by_name(name).expect("archive member");
    let mut bytes = Vec::new();
    entry.read_to_end(&mut bytes).expect("read archive member");
    bytes
}

#[test]
fn top_network_uses_exact_counter_deltas_and_fails_closed_on_reset() {
    let succeeded = evaluate_network_window(
        &request(),
        NetworkCounterSnapshot {
            received_bytes: 100,
            sent_bytes: 50,
        },
        NetworkCounterSnapshot {
            received_bytes: 4_196,
            sent_bytes: 178,
        },
        1_000,
    )
    .expect("network result");
    assert_eq!(succeeded.outcome, TopOutcome::Succeeded);
    let value = serde_json::to_value(succeeded).expect("network json");
    assert_eq!(value["data"]["receivedBytes"], 4_096);
    assert_eq!(value["data"]["sentBytes"], 128);
    assert_eq!(value["data"]["windowMillis"], 1_000);

    let reset = evaluate_network_window(
        &request(),
        NetworkCounterSnapshot {
            received_bytes: 100,
            sent_bytes: 50,
        },
        NetworkCounterSnapshot {
            received_bytes: 99,
            sent_bytes: 51,
        },
        1_000,
    )
    .expect("failed reset result");
    assert_eq!(reset.outcome, TopOutcome::Failed);
    assert_eq!(reset.reason_code, TopReasonCode::CollectionFailed);
    assert!(reset.data.is_none());
}

#[test]
fn top_network_export_is_bounded_redacted_and_signed_over_exact_envelope_bytes() {
    let request = request();
    let result = evaluate_network_window(
        &request,
        NetworkCounterSnapshot {
            received_bytes: 100,
            sent_bytes: 50,
        },
        NetworkCounterSnapshot {
            received_bytes: 4_196,
            sent_bytes: 178,
        },
        1_000,
    )
    .expect("network result");
    let identity = DeviceIdentity::generate();
    let export = sign_top_export(&request, &result, &identity, &CancellationToken::new()).expect("signed export");

    assert_eq!(export.artifact_uid, request.scope.artifact_uid);
    assert_eq!(
        export.archive_sha256,
        hex_simd::encode_to_string(Sha256::digest(&export.archive_bytes), hex_simd::AsciiCase::Lower)
    );
    assert!(export.envelope_json.len() <= 16_384);
    assert!(export.result_json.len() <= request.limits.max_result_bytes);
    assert_eq!(
        hex_simd::encode_to_string(Sha256::digest(&export.result_json), hex_simd::AsciiCase::Lower),
        export.result_sha256
    );
    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes.clone())).expect("top archive");
    assert_eq!(archive.len(), 3);
    assert_eq!(archive_entry(&mut archive, "envelope.json"), export.envelope_json);
    assert_eq!(archive_entry(&mut archive, "envelope.sig"), export.envelope_signature);
    assert_eq!(archive_entry(&mut archive, "result.json"), export.result_json);
    let envelope: serde_json::Value = serde_json::from_slice(&export.envelope_json).expect("envelope");
    assert_eq!(envelope["toolId"], "top.net");
    assert_eq!(envelope["classification"], "L3");
    assert_eq!(envelope["payload"]["path"], "result.json");
    assert_eq!(envelope["payload"]["sha256"], export.result_sha256);
    let result_text = String::from_utf8_lossy(&export.result_json);
    assert!(!result_text.contains("address"));
    assert!(!result_text.contains("path"));

    let signature_document: serde_json::Value = serde_json::from_slice(&export.envelope_signature).expect("signature");
    assert_eq!(signature_document["algorithm"], "ES256");
    let signature_bytes = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&signature_bytes).expect("fixed signature");
    assert_eq!(signature, signature.normalize_s());
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&export.envelope_json);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("device public key")
        .verify(&signed, &signature)
        .expect("exact envelope signature");
}

#[test]
fn top_export_rejects_forged_coverage_reason_and_tool() {
    let request = request();
    let result = evaluate_network_window(
        &request,
        NetworkCounterSnapshot {
            received_bytes: 100,
            sent_bytes: 50,
        },
        NetworkCounterSnapshot {
            received_bytes: 4_196,
            sent_bytes: 178,
        },
        1_000,
    )
    .expect("network result");
    let identity = DeviceIdentity::generate();

    for reason_code in [
        TopReasonCode::LimitExceeded,
        TopReasonCode::SourceUnavailable,
        TopReasonCode::CounterReset,
    ] {
        let mut valid_partial = result.clone();
        valid_partial.outcome = TopOutcome::Partial;
        valid_partial.reason_code = reason_code;
        valid_partial.coverage.requested_units = 2;
        assert!(sign_top_export(&request, &valid_partial, &identity, &CancellationToken::new()).is_ok());
    }

    let mut forged_coverage = result.clone();
    forged_coverage.coverage.completed_units = 0;
    assert_eq!(
        sign_top_export(&request, &forged_coverage, &identity, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    );

    let mut forged_reason = result.clone();
    forged_reason.reason_code = TopReasonCode::LimitExceeded;
    assert_eq!(
        sign_top_export(&request, &forged_reason, &identity, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    );

    let mut forged_tool = result;
    forged_tool.tool_id = "logs.capture";
    forged_tool.capability = "logs.capture@1".to_owned();
    assert_eq!(
        sign_top_export(&request, &forged_tool, &identity, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    );
}

#[test]
fn local_top_export_is_private_no_clobber_cancel_safe_and_rejects_forged_artifact_uid() {
    let request = request();
    let result = evaluate_network_window(
        &request,
        NetworkCounterSnapshot {
            received_bytes: 100,
            sent_bytes: 50,
        },
        NetworkCounterSnapshot {
            received_bytes: 4_196,
            sent_bytes: 178,
        },
        1_000,
    )
    .expect("network result");
    let export =
        sign_top_export(&request, &result, &DeviceIdentity::generate(), &CancellationToken::new()).expect("signed export");
    let directory = tempfile::tempdir().expect("output directory");
    let output = directory.path().join("top.zip");
    let receipt = save_signed_top_export(&output, &export, &CancellationToken::new()).expect("saved export");
    assert_eq!(receipt.artifact_uid, export.artifact_uid);
    assert_eq!(receipt.archive_sha256, export.archive_sha256);
    assert_eq!(receipt.archive_size_bytes, export.archive_bytes.len() as u64);
    assert_eq!(fs::read(&output).expect("saved bytes"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("saved mode").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_top_export(&output, &export, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::AlreadyExists)
    ));

    let cancel = CancellationToken::new();
    cancel.cancel();
    let cancelled_output = directory.path().join("cancelled.zip");
    assert!(matches!(
        save_signed_top_export(&cancelled_output, &export, &cancel),
        Err(rustfs::connect::diagnostics::TopCaptureError::Cancelled)
    ));
    assert!(!cancelled_output.exists());

    let mut forged = export.clone();
    forged.artifact_uid = "../../escape".to_owned();
    let forged_output = directory.path().join("forged.zip");
    assert!(matches!(
        save_signed_top_export(&forged_output, &forged, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Scope)
    ));
    assert!(!forged_output.exists());

    let mut tampered = export.clone();
    tampered.archive_bytes[0] ^= 1;
    assert!(matches!(
        save_signed_top_export(&directory.path().join("tampered.zip"), &tampered, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    ));
    let mut empty = export.clone();
    empty.archive_bytes.clear();
    empty.archive_sha256 = hex_simd::encode_to_string(Sha256::digest(&empty.archive_bytes), hex_simd::AsciiCase::Lower);
    assert!(matches!(
        save_signed_top_export(&directory.path().join("empty.zip"), &empty, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    ));
    let mut oversized = export;
    oversized.archive_bytes = vec![0; 524_289];
    oversized.archive_sha256 = hex_simd::encode_to_string(Sha256::digest(&oversized.archive_bytes), hex_simd::AsciiCase::Lower);
    assert!(matches!(
        save_signed_top_export(&directory.path().join("oversized.zip"), &oversized, &CancellationToken::new()),
        Err(rustfs::connect::diagnostics::TopCaptureError::Result)
    ));
}

#[test]
fn production_cli_exports_top_net_and_fails_closed_for_unsupported_and_invalid_runs() {
    let directory = tempfile::tempdir().expect("CLI directory");
    let state = directory.path().join("state");
    let identity = rustfs::connect::IdentityStore::new(state.join("identity"))
        .load_or_create()
        .expect("enrolled identity");

    let net_output = directory.path().join("net.zip");
    let net = top_command("net", &state, &net_output, "019e3ae0-0000-7000-8000-000000000021", 1, true)
        .output()
        .expect("run top.net");
    assert!(net.status.success(), "stderr: {}", String::from_utf8_lossy(&net.stderr));
    let stdout = String::from_utf8(net.stdout).expect("UTF-8 stdout");
    let result: serde_json::Value = stdout
        .lines()
        .find_map(|line| line.strip_prefix("result="))
        .map(|line| serde_json::from_str(line).expect("result JSON"))
        .expect("result line");
    assert_eq!(result["toolId"], "top.net");
    assert_eq!(result["outcome"], "SUCCEEDED");
    assert_eq!(result["reasonCode"], "COMPLETE");
    assert_eq!(result["coverage"]["requestedUnits"], 1);
    assert_eq!(result["coverage"]["completedUnits"], 1);
    assert_eq!(result["provenance"]["sourceCommit"], rustfs::version::build::COMMIT_HASH);
    assert_eq!(
        result["provenance"]["executableSha256"],
        sha256_file(Path::new(env!("CARGO_BIN_EXE_rustfs")))
    );

    let bytes = fs::read(&net_output).expect("top.net archive");
    #[cfg(unix)]
    assert_eq!(fs::metadata(&net_output).expect("output metadata").permissions().mode() & 0o777, 0o600);
    let mut archive = ZipArchive::new(Cursor::new(bytes)).expect("top.net archive");
    let names = (0..archive.len())
        .map(|index| archive.by_index(index).expect("archive member").name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["envelope.json", "envelope.sig", "result.json"]);

    let envelope = archive_entry(&mut archive, "envelope.json");
    let signature_document = archive_entry(&mut archive, "envelope.sig");
    let signature_document: serde_json::Value = serde_json::from_slice(&signature_document).expect("signature JSON");
    let raw = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&raw).expect("P-256 signature");
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&envelope);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("public key")
        .verify(&signed, &signature)
        .expect("valid ES256 signature");

    for (index, tool) in ["api", "locks", "rpc"].into_iter().enumerate() {
        let output = directory.path().join(format!("{tool}.zip"));
        let artifact_uid = format!("019e3ae0-0000-7000-8000-00000000003{}", index + 1);
        let run = top_command(tool, &state, &output, &artifact_uid, 1, true)
            .output()
            .expect("run unsupported top command");
        assert!(!run.status.success());
        let stdout = String::from_utf8(run.stdout).expect("UTF-8 stdout");
        assert!(stdout.contains("\"outcome\":\"UNSUPPORTED\""));
        assert!(stdout.contains("\"reasonCode\":\"UNSUPPORTED_TOOL\""));
        assert!(!output.exists());
    }

    let no_consent = top_command(
        "net",
        &state,
        &directory.path().join("no-consent.zip"),
        "019e3ae0-0000-7000-8000-000000000041",
        1,
        false,
    )
    .output()
    .expect("run without consent");
    assert!(!no_consent.status.success());
    assert!(String::from_utf8_lossy(&no_consent.stderr).contains("--acknowledge-l3"));

    let missing_state = directory.path().join("missing-state");
    let over_limit = top_command(
        "net",
        &missing_state,
        &directory.path().join("over-limit.zip"),
        "019e3ae0-0000-7000-8000-000000000042",
        30_001,
        true,
    )
    .output()
    .expect("run over limit");
    assert!(!over_limit.status.success());
    assert!(String::from_utf8_lossy(&over_limit.stderr).contains("connect_top_limits_invalid"));
    assert!(!missing_state.exists(), "limit preflight must precede identity access");
}

fn top_command(tool: &str, state: &Path, output: &Path, artifact_uid: &str, window_millis: u64, acknowledge_l3: bool) -> Command {
    let now = OffsetDateTime::now_utc().unix_timestamp();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs"));
    command
        .args(["connect", "top", tool, "--state-dir"])
        .arg(state)
        .arg("--output")
        .arg(output)
        .args(["--organization", organization, "--cluster", &cluster, "--device"])
        .arg(format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"))
        .args([
            "--run-uid",
            "019e3ae0-0000-7000-8000-000000000013",
            "--artifact-uid",
            artifact_uid,
            "--consent-uid",
            "019e3ae0-0000-7000-8000-000000000015",
            "--policy-revision",
            "7",
            "--consent-expires-at",
            &(now + 120).to_string(),
            "--run-expires-at",
            &(now + 60).to_string(),
            "--window-millis",
            &window_millis.to_string(),
        ]);
    if acknowledge_l3 {
        command.arg("--acknowledge-l3");
    }
    command
}

fn sha256_file(path: &Path) -> String {
    let mut file = fs::File::open(path).expect("open exact binary");
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).expect("read exact binary");
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    hex_simd::encode_to_string(digest.finalize(), hex_simd::AsciiCase::Lower)
}
