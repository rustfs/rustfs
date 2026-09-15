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
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::signature::Verifier as _;
use p256::ecdsa::{Signature, VerifyingKey};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::DeviceIdentity;
use rustfs::connect::diagnostics::{
    INSPECT_CAPABILITY, InspectArtifactConsent, InspectError, InspectOutcome, InspectProvenance, InspectReasonCode,
    InspectRequest, InspectRule, InspectRun, export_inspect_summary, save_signed_inspect_export,
};
use rustfs_ecstore::api::erasure::Erasure;
use rustfs_filemeta::{ChecksumInfo, FileInfo, FileMeta, ObjectPartInfo};
use rustfs_utils::HashAlgorithm;
use sha2::{Digest as _, Sha256};
use time::OffsetDateTime;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zip::ZipArchive;

const VERSION_ID: &str = "11111111-1111-4111-8111-111111111111";
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn now() -> i64 {
    i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).expect("current time").as_secs()).expect("time fits i64")
}

fn request(drives: &[tempfile::TempDir]) -> InspectRequest {
    let now = now();
    let organization = "organizations/019e3ae0-0000-7000-8000-000000000021";
    let cluster = format!("{organization}/clusters/019e3ae0-0000-7000-8000-000000000022");
    InspectRequest {
        organization_name: organization.to_string(),
        cluster_name: cluster.clone(),
        device_name: format!("{cluster}/clusterDevices/019e3ae0-0000-7000-8000-000000000023"),
        run_uid: "019e3ae0-0000-7000-8000-000000000024".to_string(),
        artifact_uid: "019e3ae0-0000-7000-8000-000000000025".to_string(),
        schema_version: 1,
        capability: INSPECT_CAPABILITY.to_string(),
        consent: InspectArtifactConsent {
            consent_uid: "019e3ae0-0000-7000-8000-000000000026".to_string(),
            policy_revision: 3,
            expires_at_unix: now + 120,
            confirmed: true,
        },
        produced_at_unix: now,
        expires_at_unix: now + 60,
        nonce: [0x5a; 32],
        drive_roots: drives.iter().map(|drive| drive.path().to_path_buf()).collect(),
        bucket: "customer-bucket".to_string(),
        object: "private/quarterly-report.bin".to_string(),
        version_id: Some(VERSION_ID.to_string()),
        rules: vec![
            InspectRule::ShardBitrot,
            InspectRule::ShardAvailability,
            InspectRule::MetadataIdentity,
        ],
        max_duration: Duration::from_secs(30),
        max_read_bytes: 256 * 1024 * 1024,
        max_memory_bytes: 64 * 1024 * 1024,
        provenance: InspectProvenance::new("a".repeat(40), "b".repeat(64), "1.0.0-rc.6".to_string(), vec!["gcs".to_string()]),
    }
}

fn fixture(
    indices: &[usize],
    inline: bool,
    corrupt_index: Option<usize>,
    mismatched_index: Option<usize>,
) -> Vec<tempfile::TempDir> {
    let payload = b"synthetic customer object payload kept only in the disposable local fixture";
    let erasure = Erasure::try_new(2, 2, 64).expect("test erasure geometry");
    let mut encoded = vec![Vec::new(); 4];
    for block in payload.chunks(64) {
        let shards = erasure.encode_data(block).expect("encode test block");
        for (target, shard) in encoded.iter_mut().zip(shards) {
            target.extend_from_slice(&shard);
        }
    }
    let drives = indices
        .iter()
        .map(|_| tempfile::tempdir().expect("drive tempdir"))
        .collect::<Vec<_>>();
    let data_dir = Uuid::parse_str("22222222-2222-4222-8222-222222222222").expect("data directory UUID");
    let version_id = Uuid::parse_str(VERSION_ID).expect("version UUID");
    for (drive, index) in drives.iter().zip(indices.iter().copied()) {
        let algorithm = HashAlgorithm::HighwayHash256S;
        let mut framed = Vec::new();
        for block in encoded[index - 1].chunks(erasure.shard_size()) {
            let digest = algorithm.hash_encode(block);
            framed.extend_from_slice(digest.as_ref());
            framed.extend_from_slice(block);
        }
        if corrupt_index == Some(index) {
            framed[0] ^= 0xff;
        }
        let mut file_info = FileInfo::new("private/quarterly-report.bin", 2, 2);
        file_info.name = "private/quarterly-report.bin".to_string();
        file_info.version_id = Some(version_id);
        file_info.data_dir = Some(data_dir);
        file_info.mod_time = Some(
            OffsetDateTime::from_unix_timestamp(if mismatched_index == Some(index) { 11 } else { 10 }).expect("fixture time"),
        );
        file_info.size = i64::try_from(payload.len()).expect("payload size");
        file_info.parts = vec![ObjectPartInfo {
            number: 1,
            size: payload.len(),
            actual_size: i64::try_from(payload.len()).expect("payload size"),
            ..Default::default()
        }];
        file_info.erasure.block_size = 64;
        file_info.erasure.index = index;
        file_info.erasure.distribution = vec![1, 2, 3, 4];
        file_info.erasure.checksums = vec![ChecksumInfo {
            part_number: 1,
            algorithm,
            ..Default::default()
        }];
        if inline {
            file_info.data = Some(framed.clone().into());
            file_info.set_inline_data();
        }
        let mut metadata = FileMeta::new();
        metadata.add_version(file_info).expect("add fixture version");
        let object_dir = drive.path().join("customer-bucket/private/quarterly-report.bin");
        fs::create_dir_all(&object_dir).expect("object directory");
        fs::write(object_dir.join("xl.meta"), metadata.marshal_msg().expect("marshal xl.meta")).expect("write xl.meta");
        if !inline {
            let part_dir = object_dir.join(data_dir.to_string());
            fs::create_dir(&part_dir).expect("part directory");
            fs::write(part_dir.join("part.1"), framed).expect("write shard");
        }
    }
    drives
}

fn signed(run: InspectRun) -> rustfs::connect::diagnostics::SignedInspectExport {
    match run {
        InspectRun::Signed(export) => export,
        InspectRun::Terminal(result) => panic!("expected signed export, got {:?}/{:?}", result.outcome(), result.reason_code()),
    }
}

fn terminal(run: InspectRun) -> rustfs::connect::diagnostics::InspectDiagnosticResult {
    match run {
        InspectRun::Terminal(result) => result,
        InspectRun::Signed(_) => panic!("expected terminal result"),
    }
}

#[test]
fn non_inline_versioned_object_produces_a_signed_redacted_export() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], false, None, None);
    let key = DeviceIdentity::generate();
    let export = signed(export_inspect_summary(&request(&drives), &key, &CancellationToken::new()).expect("inspect export"));
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    assert_eq!(result["toolId"], "inspect.object");
    assert_eq!(result["capability"], "inspect.object@1");
    assert_eq!(result["outcome"], "SUCCEEDED");
    assert_eq!(result["data"]["scope"], "LOCAL_OBJECT_SUMMARY");
    assert_eq!(result["data"]["findings"].as_array().expect("findings").len(), 3);
    assert_eq!(result["data"]["findings"][0]["outcome"], "PASS");
    assert_eq!(result["data"]["findings"][1]["reconstruction"], "POSSIBLE");
    assert_eq!(result["data"]["findings"][2]["metadataMatch"], true);

    let encoded = serde_json::to_string(&result).expect("encode result");
    for forbidden in [
        "customer-bucket",
        "quarterly-report",
        VERSION_ID,
        "22222222-2222-4222-8222-222222222222",
        "synthetic customer object",
        "xl.meta",
        "part.1",
        "metadata_sha256",
    ] {
        assert!(!encoded.contains(forbidden), "result leaked prohibited input: {forbidden}");
    }

    let signature_document: serde_json::Value = serde_json::from_slice(&export.envelope_signature).expect("signature JSON");
    let signature_bytes = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature"))
        .expect("base64url signature");
    let signature = Signature::from_slice(&signature_bytes).expect("P-256 signature");
    let public = VerifyingKey::from_public_key_der(&key.public_key_der()).expect("public key");
    let mut signed_bytes = b"rustfs-diagnostic-envelope-v1\0".to_vec();
    signed_bytes.extend_from_slice(&export.envelope_json);
    public
        .verify(&signed_bytes, &signature)
        .expect("signature over exact envelope");

    let mut archive = ZipArchive::new(Cursor::new(export.archive_bytes.clone())).expect("archive");
    assert_eq!(archive.len(), 3);
    let mut archived_result = Vec::new();
    archive
        .by_name("result.json")
        .expect("result entry")
        .read_to_end(&mut archived_result)
        .expect("read result");
    assert_eq!(archived_result, export.result_json);

    let output = tempfile::tempdir().expect("output directory");
    let path = output.path().join("inspect.zip");
    let receipt = save_signed_inspect_export(&path, &export, &CancellationToken::new()).expect("save export");
    assert_eq!(receipt.archive_sha256, export.archive_sha256);
    assert_eq!(fs::read(&path).expect("saved export"), export.archive_bytes);
    #[cfg(unix)]
    assert_eq!(fs::metadata(path).expect("output metadata").permissions().mode() & 0o777, 0o600);
    assert!(matches!(
        save_signed_inspect_export(&output.path().join("inspect.zip"), &export, &CancellationToken::new()),
        Err(InspectError::AlreadyExists)
    ));
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let cancelled_path = output.path().join("cancelled.zip");
    assert!(matches!(
        save_signed_inspect_export(&cancelled_path, &export, &cancelled),
        Err(InspectError::Cancelled)
    ));
    assert!(!cancelled_path.exists());
}

#[test]
fn corrupt_shard_is_reported_without_exporting_its_bytes() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], true, Some(1), None);
    let export = signed(
        export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("corrupt shard conclusion"),
    );
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    let finding = &result["data"]["findings"][0];
    assert_eq!(finding["ruleId"], "SHARD_BITROT");
    assert_eq!(finding["outcome"], "FAIL");
    assert_eq!(finding["reason"], "CORRUPT_SHARD");
    assert_eq!(finding["corruptShardCount"], 1);
}

#[test]
fn missing_shard_reports_reconstruction_possible_after_real_decode() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[2, 3, 4], true, None, None);
    let export = signed(
        export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("missing shard conclusion"),
    );
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    let finding = &result["data"]["findings"][1];
    assert_eq!(finding["ruleId"], "SHARD_AVAILABILITY");
    assert_eq!(finding["outcome"], "FAIL");
    assert_eq!(finding["reason"], "MISSING_SHARD");
    assert_eq!(finding["missingShardCount"], 1);
    assert_eq!(finding["reconstruction"], "POSSIBLE");
}

#[test]
fn mismatched_metadata_identity_never_becomes_pass() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], true, None, Some(4));
    let export = signed(
        export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("metadata mismatch conclusion"),
    );
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    let finding = &result["data"]["findings"][2];
    assert_eq!(finding["ruleId"], "METADATA_IDENTITY");
    assert_eq!(finding["outcome"], "FAIL");
    assert_eq!(finding["reason"], "IDENTITY_MISMATCH");
    assert_eq!(finding["metadataMatch"], false);
}

#[test]
fn cancellation_limit_and_unsupported_format_are_terminal_without_data() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], true, None, None);
    let cancel = CancellationToken::new();
    cancel.cancel();
    let cancelled =
        terminal(export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &cancel).expect("cancelled result"));
    assert_eq!(cancelled.outcome(), InspectOutcome::Cancelled);
    assert_eq!(cancelled.reason_code(), InspectReasonCode::Cancelled);
    assert!(cancelled.findings().is_empty());

    let mut limited = request(&drives);
    limited.max_read_bytes = 1;
    let limited = terminal(
        export_inspect_summary(&limited, &DeviceIdentity::generate(), &CancellationToken::new()).expect("limited result"),
    );
    assert_eq!(limited.outcome(), InspectOutcome::Failed);
    assert_eq!(limited.reason_code(), InspectReasonCode::LimitExceeded);
    assert!(limited.findings().is_empty());

    let unsupported_drive = tempfile::tempdir().expect("unsupported drive");
    let object_dir = unsupported_drive.path().join("customer-bucket/private/quarterly-report.bin");
    fs::create_dir_all(&object_dir).expect("object directory");
    fs::write(object_dir.join("xl.meta"), b"XL2 \x02\x00\x00\x00unsupported").expect("unsupported xl.meta");
    let unsupported = terminal(
        export_inspect_summary(&request(&[unsupported_drive]), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("unsupported result"),
    );
    assert_eq!(unsupported.outcome(), InspectOutcome::Unsupported);
    assert_eq!(unsupported.reason_code(), InspectReasonCode::UnsupportedVersion);
    assert!(unsupported.findings().is_empty());
}

#[test]
fn no_usable_metadata_is_indeterminate_and_invalid_requests_fail_closed() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drive = tempfile::tempdir().expect("empty drive");
    fs::create_dir_all(drive.path().join("customer-bucket/private/quarterly-report.bin")).expect("object directory");
    let export = signed(
        export_inspect_summary(&request(&[drive]), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("indeterminate result"),
    );
    let result: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    assert_eq!(result["data"]["findings"][0]["outcome"], "INDETERMINATE");
    assert_eq!(result["data"]["findings"][0]["reason"], "NO_USABLE_METADATA");
    assert!(result["data"]["findings"][0].get("missingShardCount").is_none());

    let drives = fixture(&[1, 2, 3, 4], true, None, None);
    let mut denied = request(&drives);
    denied.consent.confirmed = false;
    assert!(matches!(
        export_inspect_summary(&denied, &DeviceIdentity::generate(), &CancellationToken::new()),
        Err(InspectError::ConsentRequired)
    ));
    let mut duplicate = request(&drives);
    duplicate.rules = vec![InspectRule::ShardBitrot, InspectRule::ShardBitrot];
    assert!(matches!(
        export_inspect_summary(&duplicate, &DeviceIdentity::generate(), &CancellationToken::new()),
        Err(InspectError::LimitExceeded)
    ));
    let mut traversal = request(&drives);
    traversal.object = "../secret".to_string();
    assert!(matches!(
        export_inspect_summary(&traversal, &DeviceIdentity::generate(), &CancellationToken::new()),
        Err(InspectError::InvalidRequest)
    ));
}

#[cfg(unix)]
#[test]
fn denied_source_does_not_fabricate_success() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1], false, None, None);
    let object_dir = drives[0].path().join("customer-bucket/private/quarterly-report.bin");
    fs::set_permissions(&object_dir, fs::Permissions::from_mode(0o000)).expect("deny fixture directory");
    let run = export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &CancellationToken::new());
    fs::set_permissions(&object_dir, fs::Permissions::from_mode(0o700)).expect("restore fixture directory");
    let result = terminal(run.expect("permission result"));
    assert_eq!(result.outcome(), InspectOutcome::Failed);
    assert_eq!(result.reason_code(), InspectReasonCode::PermissionDenied);
    assert!(result.findings().is_empty());
}

#[test]
fn typed_accessors_match_the_closed_contract() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[2, 3, 4], true, None, None);
    let export = signed(
        export_inspect_summary(&request(&drives), &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("inspect result"),
    );
    let value: serde_json::Value = serde_json::from_slice(&export.result_json).expect("result JSON");
    assert_eq!(value["data"]["findings"][1]["reason"], "MISSING_SHARD");
    assert_eq!(value["data"]["findings"][1]["reconstruction"], "POSSIBLE");

    let digest = hex(&Sha256::digest(&export.archive_bytes));
    assert_eq!(export.archive_sha256, digest);
}

#[test]
fn cumulative_read_budget_accepts_n_and_rejects_n_plus_one() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], true, None, None);
    let exact = drives
        .iter()
        .map(|drive| {
            let path = drive.path().join("customer-bucket/private/quarterly-report.bin/xl.meta");
            let bytes = fs::read(path).expect("fixture xl.meta");
            let metadata = FileMeta::load(&bytes).expect("parse fixture xl.meta");
            let info = metadata
                .into_fileinfo("customer-bucket", "private/quarterly-report.bin", VERSION_ID, true, false, true)
                .expect("fixture file info");
            u64::try_from(bytes.len() + info.data.expect("inline fixture").len()).expect("read size")
        })
        .sum::<u64>();
    let mut at_limit = request(&drives);
    at_limit.max_read_bytes = exact;
    assert!(matches!(
        export_inspect_summary(&at_limit, &DeviceIdentity::generate(), &CancellationToken::new()),
        Ok(InspectRun::Signed(_))
    ));

    let mut one_byte_over = request(&drives);
    one_byte_over.max_read_bytes = exact - 1;
    let result = terminal(
        export_inspect_summary(&one_byte_over, &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("N+1 terminal result"),
    );
    assert_eq!(result.outcome(), InspectOutcome::Failed);
    assert_eq!(result.reason_code(), InspectReasonCode::LimitExceeded);
    assert!(result.findings().is_empty());
}

#[test]
fn duration_and_cumulative_memory_limits_fail_without_signed_output() {
    let _guard = TEST_LOCK.lock().expect("test lock");
    let drives = fixture(&[1, 2, 3, 4], true, None, None);

    let mut timed_out = request(&drives);
    timed_out.max_duration = Duration::from_nanos(1);
    let timed_out = terminal(
        export_inspect_summary(&timed_out, &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("duration limit result"),
    );
    assert_eq!(timed_out.outcome(), InspectOutcome::Failed);
    assert_eq!(timed_out.reason_code(), InspectReasonCode::LimitExceeded);
    assert!(timed_out.findings().is_empty());

    let mut one_drive = request(&drives[..1]);
    one_drive.max_memory_bytes = 300;
    assert!(matches!(
        export_inspect_summary(&one_drive, &DeviceIdentity::generate(), &CancellationToken::new()),
        Ok(InspectRun::Signed(_))
    ));

    let mut memory_limited = request(&drives);
    memory_limited.max_memory_bytes = 300;
    let memory_limited = terminal(
        export_inspect_summary(&memory_limited, &DeviceIdentity::generate(), &CancellationToken::new())
            .expect("memory limit result"),
    );
    assert_eq!(memory_limited.outcome(), InspectOutcome::Failed);
    assert_eq!(memory_limited.reason_code(), InspectReasonCode::LimitExceeded);
    assert!(memory_limited.findings().is_empty());
}

fn hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        write!(&mut output, "{byte:02x}").expect("hex string");
    }
    output
}
