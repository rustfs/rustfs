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

//! Black-box behavior: exporting the Local backend as a sealed backup bundle.
//!
//! A backup is only worth having if it is *provably* restorable, so the bundle
//! format is fail-closed at every step. The properties asserted here:
//!
//! * **Nothing leaves unwrapped.** Every artifact is AEAD-encrypted under a
//!   backup KEK that is deliberately outside the KMS hierarchy — including
//!   plaintext-dev-only key records, whose material would otherwise be readable
//!   straight out of the bundle.
//! * **Sealing is all-or-nothing.** The manifest is written last and carries a
//!   completeness marker plus a digest over its own canonical bytes, so an
//!   interrupted export is permanently non-restorable rather than subtly short.
//! * **The KEK is checked before anything else.** Presenting the wrong KEK
//!   identity fails on identity, not on a decryption error, so a mismatch is
//!   diagnosable rather than looking like corruption.
//! * **Tampering is detected.** The manifest digest covers the manifest, and
//!   each artifact carries a digest of its *encrypted* bytes, so verification
//!   never needs the KEK.

mod common;

use std::path::{Path, PathBuf};

use common::{TestKms, without_probe_key};
use rustfs_kms::backends::local::LocalKmsClient;
use rustfs_kms::backup::{
    ArtifactKind, BackupError, BackupKek, BackupManifest, CompletenessState, ContentDigest, LOCAL_BUNDLE_MANIFEST_FILE,
    LocalBackupExportRequest, decrypt_bundle_artifact, export_local_backup, read_local_bundle_manifest,
};
use rustfs_kms::{KmsError, LocalConfig};
use tempfile::TempDir;

const KEK_ID: &str = "backup-kek";
const KEK_VERSION: u32 = 3;

fn kek() -> BackupKek {
    BackupKek::new(KEK_ID, KEK_VERSION, [0x7eu8; 32]).expect("KEK should build")
}

fn export_request(destination: PathBuf) -> LocalBackupExportRequest {
    LocalBackupExportRequest {
        backup_id: "backup-behavior-001".to_string(),
        deployment_identity: "deployment-under-test".to_string(),
        rustfs_version: "0.0.0-behavior".to_string(),
        snapshot_generation: 7,
        destination,
        // These specs cover the key-material path; the sanitized configuration
        // is the admin layer's own artifact and is exercised separately.
        sanitized_config: None,
    }
}

/// A KMS with a few keys, plus a client over the same directory — the shape an
/// admin-layer export takes.
async fn seeded_kms(key_names: &[&str]) -> (TestKms, LocalKmsClient) {
    let kms = TestKms::local().await;
    for name in key_names {
        kms.create_key(name).await;
    }
    let key_dir = kms.key_dir().expect("local backend has a key dir");
    let client = LocalKmsClient::new(LocalConfig {
        key_dir,
        master_key: None,
        file_permissions: Some(0o600),
    })
    .await
    .expect("client over the same key directory");
    (kms, client)
}

async fn read_bytes(path: &Path) -> Vec<u8> {
    tokio::fs::read(path).await.expect("bundle file should be readable")
}

#[tokio::test]
async fn a_sealed_bundle_describes_and_yields_its_contents() {
    let (_kms, client) = seeded_kms(&["backup-a", "backup-b"]).await;
    let out = TempDir::new().expect("temp dir");
    let bundle_dir = out.path().join("bundle");

    let manifest = export_local_backup(&client, &kek(), &export_request(bundle_dir.clone()))
        .await
        .expect("export should succeed");

    // --- manifest identity ------------------------------------------------
    assert_eq!(manifest.format_version, BackupManifest::FORMAT_VERSION);
    assert_eq!(manifest.backup_id, "backup-behavior-001");
    assert_eq!(manifest.deployment_identity, "deployment-under-test");
    assert_eq!(manifest.rustfs_version, "0.0.0-behavior");
    assert_eq!(
        manifest.snapshot_generation, 7,
        "the caller-supplied generation must be recorded verbatim"
    );
    assert_eq!(manifest.completeness, CompletenessState::Complete, "a returned manifest must be sealed");
    assert_eq!(manifest.backup_kek.kek_id, KEK_ID, "the manifest records the KEK identity");
    assert_eq!(manifest.backup_kek.kek_version, KEK_VERSION);
    assert!(
        manifest.local_kdf.is_some(),
        "a Local bundle must record its KDF parameters so a restore can detect drift"
    );
    assert!(
        manifest.key_versions.is_none() && manifest.capability_discovery.is_none(),
        "reserved slots must stay empty in format version 1"
    );
    manifest.validate().expect("the produced manifest must validate");
    manifest.verify_digest().expect("the produced manifest's digest must verify");

    // --- what was read back off disk agrees -------------------------------
    let from_disk = read_local_bundle_manifest(&bundle_dir)
        .await
        .expect("the written manifest must read back");
    assert_eq!(from_disk, manifest, "the manifest on disk must equal the returned one");

    // --- artifacts --------------------------------------------------------
    assert!(!manifest.artifacts.is_empty(), "a bundle must carry artifacts");
    let key_material = manifest
        .require_artifact(ArtifactKind::KeyMaterial)
        .expect("a Local bundle must carry key material");
    assert!(!key_material.path.starts_with('/'), "artifact paths must stay bundle-relative");
    assert!(!key_material.path.contains(".."), "artifact paths must not traverse");

    for artifact in &manifest.artifacts {
        assert!(!artifact.kind.is_reserved(), "reserved artifact kinds must not be produced");
        let payload = read_bytes(&bundle_dir.join(&artifact.path)).await;
        assert_eq!(
            payload.len() as u64,
            artifact.len,
            "artifact {} must be exactly the declared length",
            artifact.path
        );
        assert_eq!(
            ContentDigest::sha256_of(&payload),
            artifact.encrypted_digest,
            "artifact {} must match its declared digest of the *encrypted* bytes",
            artifact.path
        );

        // The KEK opens it, and the plaintext is real content.
        let plaintext = decrypt_bundle_artifact(&bundle_dir, &manifest, artifact, &kek())
            .await
            .unwrap_or_else(|error| panic!("artifact {} must decrypt under the right KEK: {error:?}", artifact.path));
        assert!(!plaintext.is_empty(), "artifact {} decrypted to nothing", artifact.path);
    }

    // Key material is one artifact per key, so every key must be represented
    // and every artifact must decrypt to the record it claims.
    let exported = exported_key_ids(&bundle_dir, &manifest).await;
    assert_eq!(
        exported,
        vec!["backup-a".to_string(), "backup-b".to_string()],
        "every key must be exported exactly once"
    );
    for key_id in ["backup-a", "backup-b"] {
        assert!(
            manifest
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == ArtifactKind::KeyMaterial && artifact.path.contains(key_id)),
            "the bundle must carry a dedicated key-material artifact for {key_id}"
        );
    }
}

/// Decrypt every key-material artifact and return the key ids it contains,
/// sorted, so a bundle's coverage can be asserted as a set.
async fn exported_key_ids(bundle_dir: &Path, manifest: &BackupManifest) -> Vec<String> {
    let mut found = Vec::new();
    for artifact in manifest
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == ArtifactKind::KeyMaterial)
    {
        let plaintext = decrypt_bundle_artifact(bundle_dir, manifest, artifact, &kek())
            .await
            .unwrap_or_else(|error| panic!("artifact {} must decrypt: {error:?}", artifact.path));
        let record: serde_json::Value = serde_json::from_slice(&plaintext)
            .unwrap_or_else(|error| panic!("artifact {} must be a key record: {error:?}", artifact.path));
        let key_id = record
            .get("key_id")
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| panic!("artifact {} record has no key_id", artifact.path));
        found.push(key_id.to_string());
    }
    found.sort();
    // The probe key is exported like any other — correctly so — but these specs
    // assert over the keys they seeded.
    without_probe_key(found)
}

#[tokio::test]
async fn nothing_readable_leaves_the_bundle_unwrapped() {
    // Dev-mode Local records store key material in the clear on disk; the whole
    // point of the bundle KEK is that such material must not be readable from a
    // backup. Compare the on-disk record against every bundle byte.
    let (kms, client) = seeded_kms(&["leak-check"]).await;
    let key_dir = kms.key_dir().expect("key dir");
    let on_disk = read_bytes(&key_dir.join("leak-check.key")).await;

    let out = TempDir::new().expect("temp dir");
    let bundle_dir = out.path().join("bundle");
    let manifest = export_local_backup(&client, &kek(), &export_request(bundle_dir.clone()))
        .await
        .expect("export should succeed");

    for artifact in &manifest.artifacts {
        let payload = read_bytes(&bundle_dir.join(&artifact.path)).await;
        assert!(
            !payload
                .windows(on_disk.len().min(payload.len()).max(1))
                .any(|window| window == on_disk.as_slice()),
            "artifact {} carries the raw on-disk record",
            artifact.path
        );
    }

    // The manifest itself is not encrypted, so assert directly that it carries
    // no material — only identities and digests.
    let manifest_bytes = read_bytes(&bundle_dir.join(LOCAL_BUNDLE_MANIFEST_FILE)).await;
    let manifest_text = String::from_utf8(manifest_bytes).expect("the manifest is JSON");
    assert!(
        !manifest_text.contains("encrypted_key_material"),
        "the manifest must not inline key material"
    );
}

#[tokio::test]
async fn the_wrong_kek_is_refused_on_identity_not_on_decryption() {
    let (_kms, client) = seeded_kms(&["kek-check"]).await;
    let out = TempDir::new().expect("temp dir");
    let bundle_dir = out.path().join("bundle");
    let manifest = export_local_backup(&client, &kek(), &export_request(bundle_dir.clone()))
        .await
        .expect("export should succeed");
    let artifact = manifest
        .require_artifact(ArtifactKind::KeyMaterial)
        .expect("key material artifact");

    // Wrong id, wrong version, and right identity with wrong material are all
    // distinct failure modes and must be reported as such.
    let wrong_id = BackupKek::new("some-other-kek", KEK_VERSION, [0x7eu8; 32]).expect("KEK");
    match decrypt_bundle_artifact(&bundle_dir, &manifest, artifact, &wrong_id).await {
        Err(KmsError::Backup(BackupError::WrongKek {
            required_kek_id,
            supplied_kek_id,
            ..
        })) => {
            assert_eq!(required_kek_id, KEK_ID);
            assert_eq!(supplied_kek_id, "some-other-kek");
        }
        other => panic!("expected WrongKek for a mismatched id, got {other:?}"),
    }

    let wrong_version = BackupKek::new(KEK_ID, KEK_VERSION + 1, [0x7eu8; 32]).expect("KEK");
    match decrypt_bundle_artifact(&bundle_dir, &manifest, artifact, &wrong_version).await {
        Err(KmsError::Backup(BackupError::WrongKek {
            required_kek_version,
            supplied_kek_version,
            ..
        })) => {
            assert_eq!(required_kek_version, KEK_VERSION);
            assert_eq!(supplied_kek_version, KEK_VERSION + 1);
        }
        other => panic!("expected WrongKek for a mismatched version, got {other:?}"),
    }

    // Right identity, wrong material: identity passes, AEAD does not.
    let impostor = BackupKek::new(KEK_ID, KEK_VERSION, [0x00u8; 32]).expect("KEK");
    let error = decrypt_bundle_artifact(&bundle_dir, &manifest, artifact, &impostor)
        .await
        .expect_err("a KEK with the right identity but wrong material must not open the bundle");
    assert!(
        !matches!(error, KmsError::Backup(BackupError::WrongKek { .. })),
        "an identity match followed by an AEAD failure must not be reported as WrongKek: {error:?}"
    );

    // The correct KEK still works, so the failures above are about the KEK.
    decrypt_bundle_artifact(&bundle_dir, &manifest, artifact, &kek())
        .await
        .expect("the correct KEK must still open the artifact");
}

#[tokio::test]
async fn a_tampered_bundle_is_detected() {
    let (_kms, client) = seeded_kms(&["tamper-check"]).await;
    let out = TempDir::new().expect("temp dir");
    let bundle_dir = out.path().join("bundle");
    let manifest = export_local_backup(&client, &kek(), &export_request(bundle_dir.clone()))
        .await
        .expect("export should succeed");
    let artifact = manifest
        .require_artifact(ArtifactKind::KeyMaterial)
        .expect("key material artifact")
        .clone();
    let artifact_path = bundle_dir.join(&artifact.path);
    let original = read_bytes(&artifact_path).await;

    // Flipping a byte breaks the declared digest, which is checked before the
    // KEK is ever applied — so detection does not depend on holding the KEK.
    let mut tampered = original.clone();
    let middle = tampered.len() / 2;
    tampered[middle] ^= 0xff;
    tokio::fs::write(&artifact_path, &tampered).await.expect("write tampered");
    assert_ne!(
        ContentDigest::sha256_of(&tampered),
        artifact.encrypted_digest,
        "the tampered payload must no longer match its declared digest"
    );
    assert!(
        decrypt_bundle_artifact(&bundle_dir, &manifest, &artifact, &kek())
            .await
            .is_err(),
        "a tampered artifact must be refused"
    );

    // Truncation is caught by the declared length.
    tokio::fs::write(&artifact_path, &original[..original.len() / 2])
        .await
        .expect("write truncated");
    match decrypt_bundle_artifact(&bundle_dir, &manifest, &artifact, &kek()).await {
        Err(KmsError::Backup(BackupError::Truncated { .. })) => {}
        other => panic!("expected Truncated for a short artifact, got {other:?}"),
    }

    // A missing artifact is its own failure mode.
    tokio::fs::remove_file(&artifact_path).await.expect("remove artifact");
    match decrypt_bundle_artifact(&bundle_dir, &manifest, &artifact, &kek()).await {
        Err(KmsError::Backup(BackupError::MissingArtifact { .. })) => {}
        other => panic!("expected MissingArtifact, got {other:?}"),
    }

    // Restoring the bytes restores the bundle: the failures were the tampering.
    tokio::fs::write(&artifact_path, &original).await.expect("restore artifact");
    decrypt_bundle_artifact(&bundle_dir, &manifest, &artifact, &kek())
        .await
        .expect("the restored artifact must open again");
}

#[tokio::test]
async fn a_tampered_manifest_fails_its_own_digest() {
    let (_kms, client) = seeded_kms(&["manifest-tamper"]).await;
    let out = TempDir::new().expect("temp dir");
    let bundle_dir = out.path().join("bundle");
    export_local_backup(&client, &kek(), &export_request(bundle_dir.clone()))
        .await
        .expect("export should succeed");

    let manifest_path = bundle_dir.join(LOCAL_BUNDLE_MANIFEST_FILE);
    let original = read_bytes(&manifest_path).await;
    let text = String::from_utf8(original.clone()).expect("manifest is JSON");

    // Rewrite a semantically meaningful field, leaving the digest untouched.
    let tampered = text.replace("\"snapshot_generation\":7", "\"snapshot_generation\":99");
    assert_ne!(tampered, text, "the rewrite must actually change the manifest");
    tokio::fs::write(&manifest_path, tampered.as_bytes())
        .await
        .expect("write tampered manifest");

    assert!(
        read_local_bundle_manifest(&bundle_dir).await.is_err(),
        "a manifest whose contents no longer match its digest must be rejected"
    );

    // Truncating the manifest is rejected too, and so is deleting it — the
    // latter is exactly what an interrupted export leaves behind.
    tokio::fs::write(&manifest_path, &original[..original.len() / 2])
        .await
        .expect("write truncated manifest");
    assert!(
        read_local_bundle_manifest(&bundle_dir).await.is_err(),
        "a truncated manifest must be rejected"
    );

    tokio::fs::remove_file(&manifest_path).await.expect("remove manifest");
    match read_local_bundle_manifest(&bundle_dir).await {
        Err(KmsError::Backup(BackupError::IncompleteBundle { .. })) => {}
        other => panic!("a bundle with no manifest is an unsealed export, got {other:?}"),
    }

    tokio::fs::write(&manifest_path, &original).await.expect("restore manifest");
    read_local_bundle_manifest(&bundle_dir)
        .await
        .expect("the restored manifest must read back");
}

#[tokio::test]
async fn export_refuses_inputs_that_would_produce_an_unrestorable_bundle() {
    let (_kms, client) = seeded_kms(&["input-validation"]).await;
    let out = TempDir::new().expect("temp dir");

    // Identity fields are the only way a restore can tell bundles apart.
    for (field, mutate) in [
        (
            "backup_id",
            (|r: &mut LocalBackupExportRequest| r.backup_id.clear()) as fn(&mut LocalBackupExportRequest),
        ),
        ("deployment_identity", |r: &mut LocalBackupExportRequest| r.deployment_identity.clear()),
        ("rustfs_version", |r: &mut LocalBackupExportRequest| r.rustfs_version.clear()),
    ] {
        let destination = out.path().join(format!("bundle-{field}"));
        let mut request = export_request(destination.clone());
        mutate(&mut request);
        let error = export_local_backup(&client, &kek(), &request)
            .await
            .err()
            .unwrap_or_else(|| panic!("an empty {field} must be refused, but the export produced a bundle"));
        match error {
            KmsError::ValidationError { message } => {
                assert!(message.contains(field), "the refusal must name the offending field {field}: {message}")
            }
            other => panic!("an empty {field} must be a validation error, got {other:?}"),
        }
        assert!(
            !destination.join(LOCAL_BUNDLE_MANIFEST_FILE).exists(),
            "a refused export must not leave a sealed manifest behind for {field}"
        );
    }

    // An empty KEK id cannot identify a trust root.
    assert!(BackupKek::new("", KEK_VERSION, [0u8; 32]).is_err(), "an empty KEK id must be refused");
}

#[tokio::test]
async fn exporting_an_empty_key_directory_is_refused() {
    // An empty bundle would restore to an empty KMS, silently destroying state.
    let kms = TestKms::local().await;
    let key_dir = kms.key_dir().expect("key dir");
    let client = LocalKmsClient::new(LocalConfig {
        key_dir,
        master_key: None,
        file_permissions: Some(0o600),
    })
    .await
    .expect("client");

    let out = TempDir::new().expect("temp dir");
    let error = export_local_backup(&client, &kek(), &export_request(out.path().join("empty")))
        .await
        .expect_err("an export with no key records must be refused");
    assert!(
        matches!(error, KmsError::InvalidOperation { .. }),
        "refusing an empty bundle is an invalid-operation, got {error:?}"
    );
}

#[tokio::test]
async fn a_bundle_taken_after_a_change_reflects_that_change() {
    // Two generations of the same deployment: the second bundle must contain
    // the key the first did not, proving the export reads live state rather
    // than a cached snapshot.
    let (kms, client) = seeded_kms(&["generation-one"]).await;
    let out = TempDir::new().expect("temp dir");

    let first_dir = out.path().join("gen-1");
    let first = export_local_backup(&client, &kek(), &export_request(first_dir.clone()))
        .await
        .expect("first export");
    assert_eq!(
        exported_key_ids(&first_dir, &first).await,
        vec!["generation-one".to_string()],
        "the first bundle must contain exactly the key that existed when it was taken"
    );

    kms.create_key("generation-two").await;

    let second_dir = out.path().join("gen-2");
    let mut request = export_request(second_dir.clone());
    request.backup_id = "backup-behavior-002".to_string();
    request.snapshot_generation = 8;
    let second = export_local_backup(&client, &kek(), &request).await.expect("second export");
    assert_eq!(second.snapshot_generation, 8, "the newer bundle carries the newer generation");

    assert_eq!(
        exported_key_ids(&second_dir, &second).await,
        vec!["generation-one".to_string(), "generation-two".to_string()],
        "the second bundle must contain both the old and the newly created key"
    );

    // The first bundle is untouched by the second export.
    read_local_bundle_manifest(&first_dir)
        .await
        .expect("the earlier bundle must still verify");
}
