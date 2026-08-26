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

#![cfg(target_os = "linux")]

//! Deterministic archive, signature, cleanup, and CLI coverage for R07.

use std::fs::{self, File};
use std::io::Read as _;
use std::process::Command;
#[cfg(target_os = "linux")]
use std::time::Duration;

use base64_simd::URL_SAFE_NO_PAD;
use p256::ecdsa::{Signature, VerifyingKey, signature::Verifier as _};
use p256::pkcs8::DecodePublicKey as _;
use rustfs::connect::DeviceIdentity;
use rustfs::connect::offline::collectors::DataClassification;
use rustfs::connect::offline::redaction::{REDACTION_VERSION, RULESET_HASH};
use rustfs::connect::offline::{BundleContext, BundleError, ManifestEntry, OfflineKeyStore, write_offline_bundle};
#[cfg(target_os = "linux")]
use rustfs::connect::{
    CredentialStore, HeartbeatConfig, IdentityStore, InventoryFlag, InventorySchedule, InventorySnapshot, InventoryStatus,
    spawn_inventory_runtime,
};
use serde_json::Value;
use sha2::{Digest as _, Sha256};
#[cfg(target_os = "linux")]
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use zip::{CompressionMethod, ZipArchive};

const BUNDLE_UID: &str = "0198f3a1-8000-7e50-8f61-4a5b6c7d8e94";
const DEVICE_NAME: &str = "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50/clusters/0198f3a1-5d00-7b20-9c31-1d2e3f4a5b61/clusterDevices/0198f3a1-6e00-7c30-ad41-2e3f4a5b6c72";
const PATHS: [&str; 14] = [
    "offline/rustfs-version.json",
    "offline/node-count.json",
    "offline/drive-count.json",
    "offline/capacity-used-bytes.json",
    "offline/capacity-total-bytes.json",
    "offline/coarse-health-flags.json",
    "offline/os-summary.json",
    "offline/kernel-summary.json",
    "offline/cpu-summary.json",
    "offline/memory-summary.json",
    "offline/filesystem-summary.json",
    "offline/network-summary.json",
    "manifest.json",
    "manifest.sig",
];

fn context() -> BundleContext {
    BundleContext {
        bundle_uid: BUNDLE_UID.to_owned(),
        device_name: DEVICE_NAME.to_owned(),
        nonce: [0x2a; 32],
        produced_at_unix: 1_777_860_000,
    }
}

fn entries() -> Vec<ManifestEntry> {
    let cpu_summary = format!(r#"{{"cpuSummary":{{"architecture":"{}","cores":8}}}}"#, std::env::consts::ARCH);
    let values = [
        ("offline.rustfsVersion", DataClassification::L0, r#"{"rustfsVersion":"1.4.2"}"#),
        ("offline.nodeCount", DataClassification::L0, r#"{"nodeCount":2}"#),
        ("offline.driveCount", DataClassification::L0, r#"{"driveCount":3}"#),
        ("offline.capacityUsedBytes", DataClassification::L0, r#"{"capacityUsedBytes":1500}"#),
        ("offline.capacityTotalBytes", DataClassification::L0, r#"{"capacityTotalBytes":6000}"#),
        (
            "offline.coarseHealthFlags",
            DataClassification::L0,
            r#"{"coarseHealthFlags":["cluster.degraded","drive.offline"]}"#,
        ),
        ("offline.osSummary", DataClassification::L1, r#"{"osSummary":"Linux"}"#),
        ("offline.kernelSummary", DataClassification::L1, r#"{"kernelSummary":"6.8.0"}"#),
        ("offline.cpuSummary", DataClassification::L1, cpu_summary.as_str()),
        (
            "offline.memorySummary",
            DataClassification::L1,
            r#"{"memorySummary":{"totalBytes":17179869184,"underPressure":false}}"#,
        ),
        (
            "offline.filesystemSummary",
            DataClassification::L1,
            r#"{"filesystemSummary":["ext4","xfs"]}"#,
        ),
        (
            "offline.networkSummary",
            DataClassification::L1,
            r#"{"networkSummary":{"bondCount":1,"interfaceCount":4}}"#,
        ),
    ];
    values
        .into_iter()
        .map(|(field_id, classification, canonical_json)| ManifestEntry {
            field_id,
            classification,
            canonical_json: canonical_json.to_owned(),
            redaction_version: REDACTION_VERSION,
            ruleset_hash: RULESET_HASH,
            redacted_count: 0,
        })
        .collect()
}

fn read_member(archive: &mut ZipArchive<File>, path: &str) -> Vec<u8> {
    let mut file = archive.by_name(path).unwrap_or_else(|error| panic!("read {path}: {error}"));
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .unwrap_or_else(|error| panic!("read {path} bytes: {error}"));
    bytes
}

#[test]
fn connect_offline_bundle_is_deterministic_bounded_and_signed_over_exact_manifest_bytes() {
    let temp = tempfile::tempdir().expect("bundle tempdir");
    let first = temp.path().join("first.zip");
    let second = temp.path().join("second.zip");
    let key = DeviceIdentity::generate();
    let cancel = CancellationToken::new();
    let first_receipt = write_offline_bundle(&first, &context(), &entries(), &key, &cancel).expect("first bundle");
    let second_receipt = write_offline_bundle(&second, &context(), &entries(), &key, &cancel).expect("second bundle");

    let first_bytes = fs::read(&first).expect("read first bundle");
    assert_eq!(first_bytes, fs::read(&second).expect("read second bundle"));
    let private_key = key.to_pkcs8_der().expect("encode test key");
    assert!(
        !first_bytes
            .windows(private_key.len())
            .any(|window| window == private_key.as_slice())
    );
    assert_eq!(first_receipt, second_receipt);
    assert_eq!(first_receipt.archive_size_bytes, first_bytes.len() as u64);
    assert_eq!(first_receipt.archive_sha256, hex_lower(&Sha256::digest(&first_bytes)));
    assert_eq!((first_receipt.l0_count, first_receipt.l1_count), (6, 6));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(fs::metadata(&first).expect("bundle metadata").permissions().mode() & 0o777, 0o600);
    }

    let mut archive = ZipArchive::new(File::open(&first).expect("open bundle")).expect("parse bundle");
    assert_eq!(archive.len(), PATHS.len());
    for (index, expected_path) in PATHS.into_iter().enumerate() {
        let file = archive.by_index(index).expect("archive member");
        assert_eq!(file.name(), expected_path);
        assert_eq!(file.compression(), CompressionMethod::Stored);
        assert_eq!(file.last_modified(), Some(zip::DateTime::DEFAULT));
        assert_eq!(file.unix_mode().map(|mode| mode & 0o777), Some(0o600));
        assert!(file.is_file());
        assert!(!file.is_symlink());
    }

    let manifest_bytes = read_member(&mut archive, "manifest.json");
    let signature_document: Value = serde_json::from_slice(&read_member(&mut archive, "manifest.sig")).expect("signature json");
    assert_eq!(signature_document["algorithm"], "ES256");
    assert_eq!(signature_document["signedFile"], "manifest.json");
    assert_eq!(signature_document["domainSeparationTag"], "rustfs-support-bundle-v1");
    let signature_bytes: [u8; 64] = URL_SAFE_NO_PAD
        .decode_to_vec(signature_document["value"].as_str().expect("signature value"))
        .expect("signature base64url")
        .try_into()
        .expect("fixed-width signature");
    let signature = Signature::from_slice(&signature_bytes).expect("P-256 signature");
    assert_eq!(signature, signature.normalize_s(), "signature must be low-S");
    let verifying_key = VerifyingKey::from_public_key_der(&key.public_key_der()).expect("device public key");
    let mut signature_input = b"rustfs-support-bundle-v1\0".to_vec();
    signature_input.extend_from_slice(&manifest_bytes);
    verifying_key
        .verify(&signature_input, &signature)
        .expect("manifest signature");
    signature_input
        .last_mut()
        .map(|byte| *byte ^= 1)
        .expect("manifest is non-empty");
    assert!(verifying_key.verify(&signature_input, &signature).is_err());

    let manifest: Value = serde_json::from_slice(&manifest_bytes).expect("manifest json");
    assert_eq!(manifest["formatVersion"], "rustfs.connect.support.bundleManifest/1");
    assert_eq!(manifest["protocolVersion"], "v1");
    assert_eq!(manifest["bundleUid"], BUNDLE_UID);
    assert_eq!(manifest["organizationName"], "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50");
    assert_eq!(
        manifest["clusterName"],
        "organizations/0198f3a1-4c00-7a10-8b21-0c1d2e3f4a50/clusters/0198f3a1-5d00-7b20-9c31-1d2e3f4a5b61"
    );
    assert_eq!(manifest["deviceName"], DEVICE_NAME);
    assert_eq!(manifest["nonce"], URL_SAFE_NO_PAD.encode_to_string([0x2a; 32]));
    assert_eq!(manifest["producedAt"], "2026-05-04T02:00:00Z");
    assert_eq!(manifest["redactionVersion"], REDACTION_VERSION);
    assert_eq!(manifest["rulesetHash"], RULESET_HASH);
    assert_eq!(manifest["classificationRegistryVersion"], 1);
    assert_eq!(manifest["deviceKeyId"], hex_lower(&Sha256::digest(key.public_key_der())));
    for (index, entry) in manifest["entries"].as_array().expect("manifest entries").iter().enumerate() {
        let path = entry["path"].as_str().expect("entry path");
        assert_eq!(path, PATHS[index]);
        let bytes = read_member(&mut archive, path);
        assert_eq!(entry["type"], "offline-diagnostic");
        assert_eq!(entry["sizeBytes"], bytes.len() as u64);
        assert_eq!(entry["sha256"], hex_lower(&Sha256::digest(&bytes)));
        assert_eq!(entry["classification"], if index < 6 { "L0" } else { "L1" });
    }
}

#[test]
fn connect_offline_bundle_rejects_schema_drift_and_removes_every_temporary_file() {
    let temp = tempfile::tempdir().expect("bundle tempdir");
    let key = DeviceIdentity::generate();
    let active = CancellationToken::new();

    let mut invalid = Vec::new();
    let mut missing = entries();
    missing.pop();
    invalid.push(missing);
    let mut duplicate = entries();
    duplicate[1] = duplicate[0].clone();
    invalid.push(duplicate);
    let mut wrong_order = entries();
    wrong_order.swap(0, 1);
    invalid.push(wrong_order);
    let mut wrong_classification = entries();
    wrong_classification[0].classification = DataClassification::L1;
    invalid.push(wrong_classification);
    let mut wrong_redaction = entries();
    wrong_redaction[0].redaction_version = "rustfs.connect.redaction.v2";
    invalid.push(wrong_redaction);
    let mut wrong_hash = entries();
    wrong_hash[0].ruleset_hash = "foreign";
    invalid.push(wrong_hash);
    let mut oversized = entries();
    oversized[0].canonical_json = format!(r#"{{"rustfsVersion":"{}"}}"#, "x".repeat(16 * 1024));
    invalid.push(oversized);
    let mut noncanonical = entries();
    noncanonical[0].canonical_json = r#"{"rustfsVersion":"1.4.2" }"#.to_owned();
    invalid.push(noncanonical);
    let mut wrong_payload = entries();
    wrong_payload[0].canonical_json = r#"{"nodeCount":2}"#.to_owned();
    invalid.push(wrong_payload);
    let mut wrong_payload_type = entries();
    wrong_payload_type[1].canonical_json = r#"{"nodeCount":"customer-alpha"}"#.to_owned();
    invalid.push(wrong_payload_type);
    let mut out_of_range = entries();
    out_of_range[1].canonical_json = r#"{"nodeCount":0}"#.to_owned();
    invalid.push(out_of_range);
    let mut impossible_capacity = entries();
    impossible_capacity[3].canonical_json = r#"{"capacityUsedBytes":6001}"#.to_owned();
    invalid.push(impossible_capacity);
    let mut unknown_health_flag = entries();
    unknown_health_flag[5].canonical_json = r#"{"coarseHealthFlags":["customer.alpha"]}"#.to_owned();
    invalid.push(unknown_health_flag);
    let mut unknown_payload_field = entries();
    unknown_payload_field[8].canonical_json = format!(
        r#"{{"cpuSummary":{{"architecture":"{}","cores":8,"customerName":"Acme"}}}}"#,
        std::env::consts::ARCH
    );
    invalid.push(unknown_payload_field);
    let mut secret_canary = entries();
    secret_canary[6].canonical_json = r#"{"osSummary":"AKIAIOSFODNN7EXAMPLE"}"#.to_owned();
    invalid.push(secret_canary);

    for (index, entries) in invalid.iter().enumerate() {
        let output = temp.path().join(format!("invalid-{index}.zip"));
        assert!(matches!(
            write_offline_bundle(&output, &context(), entries, &key, &active),
            Err(BundleError::InvalidEntries)
        ));
        assert!(!output.exists());
    }

    for (index, invalid_context) in [
        BundleContext {
            bundle_uid: "0198f3a1-8000-7e50-cf61-4a5b6c7d8e94".to_owned(),
            ..context()
        },
        BundleContext {
            device_name: DEVICE_NAME.replace("8b21", "cb21"),
            ..context()
        },
    ]
    .iter()
    .enumerate()
    {
        let output = temp.path().join(format!("invalid-identity-{index}.zip"));
        assert!(matches!(
            write_offline_bundle(&output, invalid_context, &entries(), &key, &active),
            Err(BundleError::InvalidIdentity)
        ));
        assert!(!output.exists());
    }

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    let output = temp.path().join("cancelled.zip");
    assert!(matches!(
        write_offline_bundle(&output, &context(), &entries(), &key, &cancelled),
        Err(BundleError::Cancelled)
    ));
    assert!(!output.exists());

    let output_directory = temp.path().join("cannot-replace-directory");
    fs::create_dir(&output_directory).expect("output directory");
    assert!(write_offline_bundle(&output_directory, &context(), &entries(), &key, &active).is_err());
    assert!(output_directory.is_dir());
    let residue = fs::read_dir(temp.path())
        .expect("read tempdir")
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".tmp"))
        .count();
    assert_eq!(residue, 0);
}

#[test]
fn connect_offline_bundle_accepts_an_already_redacted_entry_with_its_original_count() {
    let temp = tempfile::tempdir().expect("bundle tempdir");
    let output = temp.path().join("redacted.zip");
    let mut entries = entries();
    entries[6].canonical_json = r#"{"osSummary":"[REDACTED]"}"#.to_owned();
    entries[6].redacted_count = 1;

    write_offline_bundle(&output, &context(), &entries, &DeviceIdentity::generate(), &CancellationToken::new())
        .expect("bundle with an already redacted entry");
    assert!(output.exists());
}

#[test]
fn connect_offline_bundle_cli_requires_an_existing_offline_key() {
    let temp = tempfile::tempdir().expect("CLI tempdir");
    let output = temp.path().join("bundle.zip");
    let key_directory = temp.path().join("keys");
    let result = Command::new(env!("CARGO_BIN_EXE_rustfs-cli"))
        .args([
            "connect",
            "offline",
            "bundle",
            "--state-dir",
            temp.path().to_string_lossy().as_ref(),
            "--device-name",
            DEVICE_NAME,
            "--output",
            output.to_string_lossy().as_ref(),
            "--key-dir",
            key_directory.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run rustfs-cli");

    assert!(!result.status.success());
    assert!(String::from_utf8_lossy(&result.stderr).contains("offline enrollment key is missing"));
    assert!(!OfflineKeyStore::new(&key_directory).key_path().exists());
    assert!(!output.exists());
}

#[cfg(target_os = "linux")]
async fn wait_for_inventory(status: &mut watch::Receiver<InventoryStatus>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(status.borrow_and_update().clone(), InventoryStatus::Unchanged { .. }) {
                return;
            }
            status.changed().await.expect("inventory status channel");
        }
    })
    .await
    .expect("inventory persistence timeout");
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn connect_offline_bundle_cli_builds_from_stopped_persisted_inventory_without_uploading() {
    use std::os::unix::fs::PermissionsExt as _;

    let temp = tempfile::tempdir_in(env!("CARGO_MANIFEST_DIR")).expect("CLI tempdir");
    let state = temp.path().join("state");
    let keys = temp.path().join("keys");
    let output = temp.path().join("bundle.zip");
    fs::create_dir(&state).expect("state root");
    fs::set_permissions(&state, fs::Permissions::from_mode(0o700)).expect("state permissions");
    let config = HeartbeatConfig::new(
        "",
        Vec::new(),
        IdentityStore::new(state.join("identity")),
        CredentialStore::new(state.join("credential")),
        state.join("heartbeat/state.json"),
    );
    let shutdown = CancellationToken::new();
    let runtime = spawn_inventory_runtime(
        Some(config),
        InventorySchedule {
            cadence: Duration::from_secs(60),
            jitter: Duration::ZERO,
        },
        &shutdown,
        || {
            std::future::ready(InventorySnapshot::new(
                "1.4.2",
                None,
                2,
                3,
                6_000,
                1_500,
                [InventoryFlag::ClusterDegraded, InventoryFlag::DriveOffline],
            ))
        },
    )
    .expect("state-only inventory")
    .expect("configured inventory");
    let mut status = runtime.status();
    wait_for_inventory(&mut status).await;
    runtime.shutdown().await;
    OfflineKeyStore::new(&keys).load_or_create().expect("offline key");

    let result = Command::new(env!("CARGO_BIN_EXE_rustfs-cli"))
        .args([
            "connect",
            "offline",
            "bundle",
            "--state-dir",
            state.to_string_lossy().as_ref(),
            "--device-name",
            DEVICE_NAME,
            "--output",
            output.to_string_lossy().as_ref(),
            "--key-dir",
            keys.to_string_lossy().as_ref(),
        ])
        .output()
        .expect("run rustfs-cli");

    assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(stdout.contains("L0: 6 entries"));
    assert!(stdout.contains("L1: 6 entries"));
    assert!(stdout.contains("Upload: not performed"));
    let mut archive = ZipArchive::new(File::open(&output).expect("open CLI bundle")).expect("parse CLI bundle");
    let manifest: Value = serde_json::from_slice(&read_member(&mut archive, "manifest.json")).expect("manifest json");
    assert_eq!(manifest["deviceName"], DEVICE_NAME);
    assert_eq!(manifest["entries"].as_array().expect("manifest entries").len(), 12);
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
