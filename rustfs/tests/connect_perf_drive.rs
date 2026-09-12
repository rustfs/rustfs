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
#[path = "../src/connect/diagnostics/perf_drive.rs"]
mod perf_drive;

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::io::{Cursor, Read as _};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
use p256::pkcs8::DecodePublicKey as _;
use perf_drive::{
    DRIVE_CAPABILITY, DriveOutcome, DrivePerformanceError, DrivePerformanceRequest, DriveProvenance, DriveReadMode,
    DriveReasonCode, DriveTargetReasonCode, LocalDriveConsent, MAX_DRIVE_DURATION, MAX_IO_BYTES, MAX_TEMPORARY_BYTES,
    classify_io, measure_drive, save_signed_drive_export, sign_drive_export,
};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn now() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs() as i64
}

fn request(scratch_root: &Path) -> DrivePerformanceRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    DrivePerformanceRequest {
        organization_name: organization.to_owned(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000012"),
        run_uid: "019e3ae0-0000-7000-8000-000000000013".to_owned(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000014".to_owned(),
        schema_version: 1,
        capability: DRIVE_CAPABILITY.to_owned(),
        consent: LocalDriveConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000015".to_owned(),
            policy_revision: 7,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x6b; 32],
        duration: Duration::from_secs(1),
        target_alias: "drive-1".to_owned(),
        scratch_root: scratch_root.to_owned(),
        scratch_bytes: 32_768,
        block_bytes: 4_096,
        provenance: DriveProvenance::new("c".repeat(40), "d".repeat(64), "1.0.0-rc.6", vec!["default".to_owned()]),
    }
}

#[tokio::test]
async fn real_local_write_read_is_measured_and_scratch_is_removed() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    let request = request(root.path());
    let measurement = measure_drive(&request, &CancellationToken::new())
        .await
        .expect("real drive benchmark");

    assert_eq!(measurement.result.outcome(), DriveOutcome::Succeeded);
    assert_eq!(measurement.result.reason_code(), DriveReasonCode::Complete);
    let data = measurement.result.data().expect("aggregate data");
    assert_eq!(data.read_bytes, 32_768);
    assert_eq!(data.write_bytes, 32_768);
    assert_eq!(data.io_count, 16);
    assert_eq!(data.error_count, 0);
    assert!((1..=1_000).contains(&data.duration_millis));
    assert_eq!(measurement.target.target_alias, "drive-1");
    assert_eq!(measurement.target.outcome, DriveOutcome::Succeeded);
    assert_eq!(measurement.target.reason_code, DriveTargetReasonCode::Complete);
    assert_eq!(measurement.target.parameters.scratch_bytes, 32_768);
    assert_eq!(measurement.target.parameters.block_bytes, 4_096);
    assert_eq!(measurement.target.parameters.duration_millis, 1_000);
    assert_eq!(measurement.target.parameters.concurrency, 1);
    assert_eq!(measurement.target.units.bytes, "BYTE");
    assert_eq!(measurement.target.units.duration, "MILLISECOND");
    assert_eq!(measurement.target.units.latency, "MICROSECOND");
    assert_eq!(measurement.target.units.io_count, "OPERATION");
    assert_eq!(measurement.target.read_mode, Some(DriveReadMode::WarmPageCache));
    assert_eq!(measurement.target.read_bytes, data.read_bytes);
    assert_eq!(measurement.target.write_bytes, data.write_bytes);
    assert!(measurement.target.cached_read_latency_micros.is_some());
    assert!(measurement.target.write_latency_micros.is_some());
    assert!(fs::read_dir(root.path()).expect("empty root").next().is_none());

    let value = serde_json::to_value(&measurement.result).expect("result JSON");
    assert_eq!(value["toolId"], "performance.drive");
    assert_eq!(value["capability"], "performance.drive@1");
    assert_eq!(value["outcome"], "SUCCEEDED");
    assert_eq!(value["reasonCode"], "COMPLETE");
    assert_eq!(value["coverage"]["requestedUnits"], 1);
    assert_eq!(value["coverage"]["completedUnits"], 1);
    assert_eq!(value["coverage"]["unit"], "WINDOW");
    assert_eq!(value["provenance"]["repository"], "rustfs/rustfs");
    assert_eq!(value["provenance"]["sourceCommit"], "c".repeat(40));
    assert_eq!(value["provenance"]["executableSha256"], "d".repeat(64));
    let data_keys = value["data"]
        .as_object()
        .expect("data object")
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(
        data_keys,
        BTreeSet::from([
            "durationMillis".to_owned(),
            "errorCount".to_owned(),
            "ioCount".to_owned(),
            "readBytes".to_owned(),
            "writeBytes".to_owned(),
        ])
    );
    assert!(value.get("target").is_none(), "frozen aggregate schema has no target field");
}

#[tokio::test]
async fn consent_and_resource_budgets_fail_before_scratch_io() {
    let root = tempfile::tempdir().expect("scratch root");
    let untouched = root.path().join("not-created");

    let mut no_consent = request(&untouched);
    no_consent.consent.confirmed = false;
    assert!(matches!(
        measure_drive(&no_consent, &CancellationToken::new()).await,
        Err(DrivePerformanceError::ConsentRequired)
    ));

    let mut over_duration = request(&untouched);
    over_duration.duration = MAX_DRIVE_DURATION + Duration::from_millis(1);
    assert!(matches!(
        measure_drive(&over_duration, &CancellationToken::new()).await,
        Err(DrivePerformanceError::LimitExceeded)
    ));

    let mut over_storage = request(&untouched);
    over_storage.scratch_bytes = MAX_TEMPORARY_BYTES + 1;
    assert!(matches!(
        measure_drive(&over_storage, &CancellationToken::new()).await,
        Err(DrivePerformanceError::LimitExceeded)
    ));

    let mut over_io = request(&untouched);
    over_io.scratch_bytes = MAX_IO_BYTES / 2 + 1;
    over_io.block_bytes = 4_096;
    assert!(matches!(
        measure_drive(&over_io, &CancellationToken::new()).await,
        Err(DrivePerformanceError::LimitExceeded)
    ));

    let mut over_operations = request(&untouched);
    over_operations.scratch_bytes = MAX_IO_BYTES / 2;
    over_operations.block_bytes = 1;
    assert!(matches!(
        measure_drive(&over_operations, &CancellationToken::new()).await,
        Err(DrivePerformanceError::LimitExceeded)
    ));
    assert!(!untouched.exists(), "validation must precede scratch access");
}

#[tokio::test]
async fn cancellation_and_single_collector_leave_no_scratch() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    let mut request = request(root.path());
    request.scratch_bytes = MAX_IO_BYTES / 2;
    request.block_bytes = 1_024;
    let held = perf_drive::CollectorLease::acquire().expect("hold collector");
    assert!(matches!(
        measure_drive(&request, &CancellationToken::new()).await,
        Err(DrivePerformanceError::Busy)
    ));
    drop(held);

    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let cancellation = async move {
        tokio::task::yield_now().await;
        trigger.cancel();
    };
    let (measurement, ()) = tokio::join!(measure_drive(&request, &cancel), cancellation);
    let measurement = measurement.expect("typed cancellation");
    assert_eq!(measurement.result.outcome(), DriveOutcome::Cancelled);
    assert_eq!(measurement.result.reason_code(), DriveReasonCode::Cancelled);
    assert!(measurement.result.data().is_none());
    assert!(fs::read_dir(root.path()).expect("empty root").next().is_none());
}

#[tokio::test]
async fn cleanup_failure_overrides_cancellation_and_remains_attributed() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    let mut request = request(root.path());
    request.scratch_bytes = MAX_IO_BYTES / 2;
    request.block_bytes = 1_024;
    let scratch = root.path().join(format!(".rustfs-connect-drive-{}", request.artifact_uid));
    let blocker = scratch.join("cleanup-blocker");
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let interfere = async {
        loop {
            if scratch.is_dir() {
                fs::write(&blocker, b"block cleanup").expect("create cleanup blocker");
                trigger.cancel();
                break;
            }
            tokio::task::yield_now().await;
        }
    };
    let (measurement, ()) = tokio::join!(measure_drive(&request, &cancel), interfere);
    let measurement = measurement.expect("typed cleanup failure");
    assert_eq!(measurement.result.outcome(), DriveOutcome::Failed);
    assert_eq!(measurement.result.reason_code(), DriveReasonCode::CollectionFailed);
    assert_eq!(measurement.target.reason_code, DriveTargetReasonCode::CleanupFailed);
    assert!(blocker.is_file(), "cleanup residue remains visible instead of being hidden");
    fs::remove_file(blocker).expect("remove blocker");
    fs::remove_dir(scratch).expect("remove scratch directory");
}

#[tokio::test]
async fn invalid_roots_and_existing_scratch_fail_without_clobbering() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    let root_file = root.path().join("not-a-directory");
    fs::write(&root_file, b"sentinel").expect("root file");
    let unavailable = measure_drive(&request(&root_file), &CancellationToken::new())
        .await
        .expect("typed unavailable result");
    assert_eq!(unavailable.result.outcome(), DriveOutcome::Failed);
    assert_eq!(unavailable.result.reason_code(), DriveReasonCode::SourceUnavailable);
    assert_eq!(fs::read(&root_file).expect("sentinel remains"), b"sentinel");

    #[cfg(unix)]
    {
        let symlink_root = root.path().join("symlink-root");
        std::os::unix::fs::symlink(root.path(), &symlink_root).expect("scratch-root symlink");
        let rejected = measure_drive(&request(&symlink_root), &CancellationToken::new())
            .await
            .expect("typed symlink rejection");
        assert_eq!(rejected.result.reason_code(), DriveReasonCode::SourceUnavailable);
    }

    let request = request(root.path());
    let collision = root.path().join(format!(".rustfs-connect-drive-{}", request.artifact_uid));
    fs::create_dir(&collision).expect("collision directory");
    fs::write(collision.join("sentinel"), b"keep").expect("collision sentinel");
    let failed = measure_drive(&request, &CancellationToken::new())
        .await
        .expect("typed collision result");
    assert_eq!(failed.result.outcome(), DriveOutcome::Failed);
    assert_eq!(failed.target.reason_code, DriveTargetReasonCode::IoFailure);
    assert_eq!(fs::read(collision.join("sentinel")).expect("sentinel remains"), b"keep");
}

#[cfg(unix)]
#[tokio::test]
async fn read_only_root_and_full_drive_errors_are_classified() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o500)).expect("make root read only");
    let measurement = measure_drive(&request(root.path()), &CancellationToken::new())
        .await
        .expect("typed permission result");
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).expect("restore permissions");
    assert_eq!(measurement.result.outcome(), DriveOutcome::Failed);
    assert_eq!(measurement.result.reason_code(), DriveReasonCode::PermissionDenied);
    assert_eq!(measurement.target.reason_code, DriveTargetReasonCode::PermissionDenied);
    let failed = serde_json::to_value(&measurement.result).expect("failed result JSON");
    assert_eq!(failed["outcome"], "FAILED");
    assert_eq!(failed["reasonCode"], "PERMISSION_DENIED");
    assert_eq!(failed["coverage"]["requestedUnits"], 1);
    assert_eq!(failed["coverage"]["completedUnits"], 0);
    assert_eq!(failed["coverage"]["unit"], "WINDOW");
    assert_eq!(failed["data"], serde_json::Value::Null);

    assert_eq!(
        classify_io(&std::io::Error::from_raw_os_error(libc::ENOSPC)),
        DriveTargetReasonCode::LimitExceeded
    );
    assert_eq!(
        classify_io(&std::io::Error::from_raw_os_error(libc::EROFS)),
        DriveTargetReasonCode::PermissionDenied
    );
}

#[test]
fn runtime_timeout_and_full_drive_use_schema_valid_failed_reason() {
    let root = tempfile::tempdir().expect("scratch root");
    let request = request(root.path());
    for target_reason in [DriveTargetReasonCode::TimedOut, DriveTargetReasonCode::LimitExceeded] {
        let measurement = perf_drive::failed_measurement(&request, Duration::from_millis(10), target_reason, 0, 0, 0);
        assert_eq!(measurement.result.outcome(), DriveOutcome::Failed);
        assert_eq!(measurement.result.reason_code(), DriveReasonCode::CollectionFailed);
        assert_eq!(measurement.target.reason_code, target_reason);
        let value = serde_json::to_value(&measurement.result).expect("failed result JSON");
        assert_eq!(value["reasonCode"], "COLLECTION_FAILED");
        assert_eq!(value["coverage"]["unit"], "WINDOW");
        assert_eq!(value["coverage"]["completedUnits"], 0);
        assert_eq!(value["data"], serde_json::Value::Null);
    }
}

#[tokio::test]
async fn successful_result_has_signed_bounded_private_offline_export() {
    let _guard = TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("scratch root");
    let request = request(root.path());
    let measurement = measure_drive(&request, &CancellationToken::new())
        .await
        .expect("drive measurement");
    let identity = connect::DeviceIdentity::generate();
    let export = sign_drive_export(&request, &measurement, &identity, &CancellationToken::new()).expect("signed export");
    assert!(export.result_json.len() <= perf_drive::MAX_RESULT_BYTES);
    assert!(export.envelope_json.len() <= perf_drive::MAX_ENVELOPE_BYTES);
    assert!(export.archive_bytes.len() <= perf_drive::MAX_ARCHIVE_BYTES);
    assert_eq!(export.archive_sha256, hex_lower(&Sha256::digest(&export.archive_bytes)));

    let mut archive = zip::ZipArchive::new(Cursor::new(&export.archive_bytes)).expect("three-file archive");
    assert_eq!(archive.len(), 3);
    assert_eq!(archive.by_index(0).expect("envelope").name(), "envelope.json");
    assert_eq!(archive.by_index(1).expect("signature").name(), "envelope.sig");
    assert_eq!(archive.by_index(2).expect("result").name(), "result.json");

    let envelope: serde_json::Value = serde_json::from_slice(&export.envelope_json).expect("envelope JSON");
    assert_eq!(envelope["toolId"], "performance.drive");
    assert_eq!(envelope["classification"], "L1");
    assert_eq!(envelope["payload"]["path"], "result.json");
    assert_eq!(envelope["payload"]["sizeBytes"], export.result_json.len());
    assert_eq!(envelope["payload"]["sha256"], hex_lower(&Sha256::digest(&export.result_json)));

    let signature: serde_json::Value = serde_json::from_slice(&export.envelope_signature).expect("signature JSON");
    assert_eq!(signature["algorithm"], "ES256");
    let raw = URL_SAFE_NO_PAD
        .decode_to_vec(signature["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&raw).expect("P-256 signature");
    assert_eq!(signature, signature.normalize_s());
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&export.envelope_json);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("public key")
        .verify(&signed, &signature)
        .expect("valid signature");

    let output_root = tempfile::tempdir().expect("output root");
    let output = output_root.path().join("drive-performance.zip");
    let saved = save_signed_drive_export(&output, &export, &CancellationToken::new()).expect("save export");
    assert_eq!(saved.archive_sha256, export.archive_sha256);
    assert_eq!(fs::read(&output).expect("saved archive"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("output metadata").permissions().mode(), 0o100600);
    assert!(matches!(
        save_signed_drive_export(&output, &export, &CancellationToken::new()),
        Err(DrivePerformanceError::AlreadyExists)
    ));
    assert_eq!(fs::read_dir(output_root.path()).expect("output directory").count(), 1);

    let failed_root = output_root.path().join("not-a-directory");
    fs::write(&failed_root, b"sentinel").expect("failed root file");
    let mut failed_request = request.clone();
    failed_request.scratch_root = failed_root;
    let failed = measure_drive(&failed_request, &CancellationToken::new())
        .await
        .expect("typed failed result");
    assert!(matches!(
        sign_drive_export(&failed_request, &failed, &identity, &CancellationToken::new()),
        Err(DrivePerformanceError::InvalidRequest)
    ));

    let collection_cancel = CancellationToken::new();
    collection_cancel.cancel();
    let cancelled = measure_drive(&request, &collection_cancel)
        .await
        .expect("typed cancelled result");
    assert!(matches!(
        sign_drive_export(&request, &cancelled, &identity, &CancellationToken::new()),
        Err(DrivePerformanceError::InvalidRequest)
    ));

    let mut forged = export.clone();
    forged.artifact_uid = "../escape".to_owned();
    assert!(matches!(
        save_signed_drive_export(&output_root.path().join("forged.zip"), &forged, &CancellationToken::new()),
        Err(DrivePerformanceError::InvalidRequest)
    ));
    assert_eq!(fs::read_dir(output_root.path()).expect("output directory").count(), 2);

    let mut tampered = export.clone();
    tampered.archive_bytes[0] ^= 0xff;
    assert!(matches!(
        save_signed_drive_export(&output_root.path().join("tampered.zip"), &tampered, &CancellationToken::new()),
        Err(DrivePerformanceError::InvalidRequest)
    ));
    let mut empty = export.clone();
    empty.archive_bytes.clear();
    empty.archive_sha256 = hex_lower(&Sha256::digest(&empty.archive_bytes));
    assert!(matches!(
        save_signed_drive_export(&output_root.path().join("empty.zip"), &empty, &CancellationToken::new()),
        Err(DrivePerformanceError::InvalidRequest)
    ));
    let mut oversized = export.clone();
    oversized.archive_bytes = vec![0; perf_drive::MAX_ARCHIVE_BYTES + 1];
    oversized.archive_sha256 = hex_lower(&Sha256::digest(&oversized.archive_bytes));
    assert!(matches!(
        save_signed_drive_export(&output_root.path().join("oversized.zip"), &oversized, &CancellationToken::new()),
        Err(DrivePerformanceError::LimitExceeded)
    ));
    assert_eq!(fs::read_dir(output_root.path()).expect("output directory").count(), 2);

    let cancelled_output = output_root.path().join("cancelled.zip");
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        save_signed_drive_export(&cancelled_output, &export, &cancel),
        Err(DrivePerformanceError::Cancelled)
    ));
    assert!(!cancelled_output.exists());
}

#[test]
fn production_cli_measures_and_signs_exact_binary_provenance() {
    let temp = tempfile::tempdir().expect("CLI tempdir");
    let state = temp.path().join("state");
    let scratch = temp.path().join("scratch");
    let output = temp.path().join("drive.zip");
    fs::create_dir(&scratch).expect("scratch root");
    let identity = rustfs::connect::IdentityStore::new(state.join("identity"))
        .load_or_create()
        .expect("enrolled identity");
    let result = drive_command(&state, &scratch, &output, "019e3ae0-0000-7000-8000-000000000014", 32_768, true)
        .output()
        .expect("run production rustfs binary");

    assert!(result.status.success(), "stderr: {}", String::from_utf8_lossy(&result.stderr));
    assert!(fs::read_dir(&scratch).expect("scratch directory").next().is_none());
    #[cfg(unix)]
    assert_eq!(fs::metadata(&output).expect("output metadata").permissions().mode(), 0o100600);
    let stdout = String::from_utf8(result.stdout).expect("UTF-8 stdout");
    assert!(stdout.contains("tool=performance.drive outcome=SUCCEEDED reason=COMPLETE\n"));
    assert!(stdout.contains("upload=not-performed\n"));
    let target = target_from_stdout(&stdout);
    assert_eq!(target["targetAlias"], "drive-1");
    assert_eq!(target["outcome"], "SUCCEEDED");
    assert_eq!(target["reasonCode"], "COMPLETE");
    assert_eq!(target["parameters"]["scratchBytes"], 32_768);
    assert_eq!(target["parameters"]["blockBytes"], 4_096);
    assert_eq!(target["parameters"]["durationMillis"], 1_000);
    assert_eq!(target["parameters"]["concurrency"], 1);
    assert_eq!(target["units"]["bytes"], "BYTE");
    assert_eq!(target["units"]["duration"], "MILLISECOND");
    assert_eq!(target["units"]["latency"], "MICROSECOND");
    assert_eq!(target["units"]["ioCount"], "OPERATION");
    assert_eq!(target["readMode"], "WARM_PAGE_CACHE");

    let archive_bytes = fs::read(&output).expect("saved archive");
    let mut archive = zip::ZipArchive::new(Cursor::new(archive_bytes.as_slice())).expect("signed archive");
    let names = (0..archive.len())
        .map(|index| archive.by_index(index).expect("archive member").name().to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, ["envelope.json", "envelope.sig", "result.json"]);
    let envelope_bytes = read_archive_member(&mut archive, "envelope.json");
    let signature_bytes = read_archive_member(&mut archive, "envelope.sig");
    let result_bytes = read_archive_member(&mut archive, "result.json");
    let envelope: serde_json::Value = serde_json::from_slice(&envelope_bytes).expect("envelope JSON");
    let signed_result: serde_json::Value = serde_json::from_slice(&result_bytes).expect("result JSON");
    assert_eq!(envelope["classification"], "L1");
    assert_eq!(envelope["payload"]["path"], "result.json");
    assert_eq!(envelope["payload"]["sizeBytes"], result_bytes.len());
    assert_eq!(envelope["payload"]["sha256"], hex_lower(&Sha256::digest(&result_bytes)));
    assert_eq!(signed_result["provenance"]["repository"], "rustfs/rustfs");
    assert_eq!(signed_result["provenance"]["sourceCommit"], rustfs::version::build::COMMIT_HASH);
    assert_eq!(
        signed_result["provenance"]["executableSha256"],
        sha256_file(Path::new(env!("CARGO_BIN_EXE_rustfs")))
    );
    assert!(signed_result.get("target").is_none(), "frozen result schema contains only the aggregate");

    let signature_document: serde_json::Value = serde_json::from_slice(&signature_bytes).expect("signature JSON");
    assert_eq!(signature_document["algorithm"], "ES256");
    let raw = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&raw).expect("P-256 signature");
    assert_eq!(signature, signature.normalize_s());
    let mut signed = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed.extend_from_slice(&envelope_bytes);
    VerifyingKey::from_public_key_der(&identity.public_key_der())
        .expect("public key")
        .verify(&signed, &signature)
        .expect("valid ES256 signature");
}

#[test]
fn production_cli_rejects_missing_l1_consent_and_limits_before_artifacts() {
    let temp = tempfile::tempdir().expect("CLI tempdir");
    let state = temp.path().join("state");
    let scratch = temp.path().join("scratch");
    fs::create_dir(&scratch).expect("scratch root");
    rustfs::connect::IdentityStore::new(state.join("identity"))
        .load_or_create()
        .expect("enrolled identity");

    let no_consent_output = temp.path().join("no-consent.zip");
    let no_consent = drive_command(
        &state,
        &scratch,
        &no_consent_output,
        "019e3ae0-0000-7000-8000-000000000024",
        32_768,
        false,
    )
    .output()
    .expect("run without L1 acknowledgement");
    assert!(!no_consent.status.success());
    assert!(String::from_utf8_lossy(&no_consent.stderr).contains("--acknowledge-l1"));
    assert!(!no_consent_output.exists());
    assert!(fs::read_dir(&scratch).expect("scratch directory").next().is_none());

    let over_limit_output = temp.path().join("over-limit.zip");
    let mut over_limit = drive_command(
        &temp.path().join("missing-state"),
        &scratch,
        &over_limit_output,
        "019e3ae0-0000-7000-8000-000000000034",
        524_289,
        true,
    );
    let over_limit = over_limit.output().expect("run over limit");
    assert!(!over_limit.status.success());
    assert!(String::from_utf8_lossy(&over_limit.stderr).contains("drive_performance_limit_exceeded"));
    assert!(!over_limit_output.exists());
    assert!(
        !temp.path().join("missing-state").exists(),
        "budget preflight must precede identity access"
    );
    assert!(fs::read_dir(&scratch).expect("scratch directory").next().is_none());

    let unavailable_root = temp.path().join("not-a-directory");
    fs::write(&unavailable_root, b"sentinel").expect("scratch root file");
    let failed_output = temp.path().join("failed.zip");
    let failed = drive_command(
        &state,
        &unavailable_root,
        &failed_output,
        "019e3ae0-0000-7000-8000-000000000044",
        32_768,
        true,
    )
    .output()
    .expect("run typed source failure");
    assert!(!failed.status.success());
    let stdout = String::from_utf8(failed.stdout).expect("UTF-8 stdout");
    assert!(stdout.contains("tool=performance.drive outcome=FAILED reason=SOURCE_UNAVAILABLE\n"));
    let target = target_from_stdout(&stdout);
    assert_eq!(target["outcome"], "FAILED");
    assert_eq!(target["reasonCode"], "SOURCE_UNAVAILABLE");
    assert_eq!(target["parameters"]["scratchBytes"], 32_768);
    assert_eq!(target["units"]["latency"], "MICROSECOND");
    assert_eq!(target["readMode"], serde_json::Value::Null);
    assert!(!failed_output.exists());
    assert_eq!(fs::read(&unavailable_root).expect("sentinel remains"), b"sentinel");
}

fn drive_command(
    state: &Path,
    scratch: &Path,
    output: &Path,
    artifact_uid: &str,
    scratch_bytes: u64,
    acknowledge_l1: bool,
) -> Command {
    let current = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000010";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000011");
    let mut command = Command::new(env!("CARGO_BIN_EXE_rustfs"));
    command
        .args(["connect", "performance", "drive", "--state-dir"])
        .arg(state)
        .arg("--scratch-dir")
        .arg(scratch)
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
            &(current + 120).to_string(),
            "--expires-at",
            &(current + 60).to_string(),
            "--duration-millis",
            "1000",
        ]);
    command
        .arg("--scratch-bytes")
        .arg(scratch_bytes.to_string())
        .args(["--block-bytes", "4096"]);
    if acknowledge_l1 {
        command.arg("--acknowledge-l1");
    }
    command
}

fn target_from_stdout(stdout: &str) -> serde_json::Value {
    let line = stdout
        .lines()
        .find_map(|line| line.strip_prefix("target="))
        .expect("typed target sidecar");
    serde_json::from_str(line).expect("target JSON")
}

fn read_archive_member(archive: &mut zip::ZipArchive<Cursor<&[u8]>>, name: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    archive
        .by_name(name)
        .expect("archive member")
        .read_to_end(&mut bytes)
        .expect("read member");
    bytes
}

fn sha256_file(path: &Path) -> String {
    let mut file = File::open(path).expect("open exact binary");
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer).expect("read exact binary");
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    hex_lower(&digest.finalize())
}

fn hex_lower(bytes: &[u8]) -> String {
    hex_simd::encode_to_string(bytes, hex_simd::AsciiCase::Lower)
}
