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

//! Black-box behavior: the wire and on-disk serialization contracts.
//!
//! These names outlive the process that wrote them. Object encryption headers
//! sit in `xl.meta` for the life of an object; persisted `KmsConfig` documents
//! are read back by later versions; backup manifests must stay decodable by a
//! future restore. Renaming any of them is a compatibility break that no
//! behavioral test would catch, because a fresh write and a fresh read agree
//! with each other perfectly.
//!
//! Two techniques, deliberately:
//!
//! * **Enum wire spellings are asserted directly.** They are short, they are
//!   the highest-risk rename, and an explicit `assert_eq!` documents the
//!   intended spelling at the point of the check.
//! * **Struct shapes are snapshotted as sorted field-name lists**, not as
//!   values. Values contain UUIDs, timestamps, and random key material, so a
//!   value snapshot would be unstable; the field set is exactly the part that
//!   constitutes the contract, and a change to it should be reviewed.

mod common;

use std::collections::BTreeSet;

use common::{STATIC_KEY_ID, TestKms, static_secret_key};
use rustfs_kms::backup::{AeadAlgorithm, ArtifactKind, CompletenessState, DigestAlgorithm};
use rustfs_kms::{EncryptionAlgorithm, KeySpec, KeyState, KeyStatus, KeyUsage, KmsBackend, KmsConfig, KmsServiceStatus};
use serde::Serialize;

/// Sorted top-level field names of a value's JSON object form.
fn field_names<T: Serialize>(value: &T) -> Vec<String> {
    let json = serde_json::to_value(value).expect("value should serialize");
    match json {
        serde_json::Value::Object(map) => map.keys().cloned().collect::<BTreeSet<_>>().into_iter().collect(),
        other => panic!("expected a JSON object, got {other}"),
    }
}

fn wire<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("value should serialize")
}

/// The tags a persisted document is matched on. A rename here silently
/// orphans every previously written record, so each spelling is stated
/// explicitly rather than snapshotted.
#[test]
fn enum_wire_spellings_are_stable() {
    // Backend discriminators appear in persisted KMS configuration.
    assert_eq!(wire(&KmsBackend::Local), r#""Local""#);
    assert_eq!(wire(&KmsBackend::Static), r#""Static""#);
    assert_eq!(wire(&KmsBackend::VaultKv2), r#""VaultKV2""#);
    assert_eq!(wire(&KmsBackend::VaultTransit), r#""VaultTransit""#);
    // The pre-rename spelling must still deserialize.
    assert_eq!(
        serde_json::from_str::<KmsBackend>(r#""Vault""#).expect("legacy label"),
        KmsBackend::VaultKv2,
        "the legacy `Vault` label must keep deserializing to VaultKV2"
    );

    // Key lifecycle vocabulary, persisted in key records and returned by the
    // admin API.
    assert_eq!(wire(&KeyState::Enabled), r#""Enabled""#);
    assert_eq!(wire(&KeyState::Disabled), r#""Disabled""#);
    assert_eq!(wire(&KeyState::PendingDeletion), r#""PendingDeletion""#);
    assert_eq!(wire(&KeyState::PendingImport), r#""PendingImport""#);
    assert_eq!(wire(&KeyState::Unavailable), r#""Unavailable""#);

    assert_eq!(wire(&KeyStatus::Active), r#""Active""#);
    assert_eq!(wire(&KeyStatus::Disabled), r#""Disabled""#);
    assert_eq!(wire(&KeyStatus::PendingDeletion), r#""PendingDeletion""#);
    assert_eq!(wire(&KeyStatus::Deleted), r#""Deleted""#);

    assert_eq!(wire(&KeyUsage::EncryptDecrypt), r#""EncryptDecrypt""#);
    assert_eq!(wire(&KeyUsage::SignVerify), r#""SignVerify""#);

    assert_eq!(wire(&KeySpec::Aes256), r#""Aes256""#);
    assert_eq!(wire(&KeySpec::Aes128), r#""Aes128""#);
    assert_eq!(wire(&KeySpec::ChaCha20), r#""ChaCha20""#);

    // Algorithm names double as S3 header values, so they are externally
    // visible as well as persisted.
    assert_eq!(wire(&EncryptionAlgorithm::Aes256), r#""AES256""#);
    assert_eq!(wire(&EncryptionAlgorithm::ChaCha20Poly1305), r#""ChaCha20Poly1305""#);
    assert_eq!(wire(&EncryptionAlgorithm::AwsKms), r#""aws:kms""#);
    // The string form used on the wire must match the serde form exactly.
    for algorithm in [
        EncryptionAlgorithm::Aes256,
        EncryptionAlgorithm::ChaCha20Poly1305,
        EncryptionAlgorithm::AwsKms,
    ] {
        assert_eq!(
            wire(&algorithm),
            format!("\"{}\"", algorithm.as_str()),
            "as_str and the serde spelling must not drift apart"
        );
        assert_eq!(
            algorithm.as_str().parse::<EncryptionAlgorithm>().expect("must parse back"),
            algorithm,
            "as_str must round-trip through FromStr"
        );
    }

    // Service status is returned by the admin status endpoint.
    assert_eq!(wire(&KmsServiceStatus::NotConfigured), r#""NotConfigured""#);
    assert_eq!(wire(&KmsServiceStatus::Configured), r#""Configured""#);
    assert_eq!(wire(&KmsServiceStatus::Running), r#""Running""#);
    assert_eq!(wire(&KmsServiceStatus::Error("boom".to_string())), r#"{"Error":"boom"}"#);

    // Backup bundle vocabulary: written into manifests that a future version
    // must still decode.
    assert_eq!(wire(&ArtifactKind::KeyMaterial), r#""key-material""#);
    assert_eq!(wire(&ArtifactKind::KeyMetadata), r#""key-metadata""#);
    assert_eq!(wire(&ArtifactKind::MasterKeySalt), r#""master-key-salt""#);
    assert_eq!(wire(&ArtifactKind::KmsConfig), r#""kms-config""#);
    assert_eq!(wire(&ArtifactKind::Alias), r#""alias""#);
    assert_eq!(wire(&ArtifactKind::Policy), r#""policy""#);
    assert_eq!(wire(&CompletenessState::InProgress), r#""in-progress""#);
    assert_eq!(wire(&CompletenessState::Complete), r#""complete""#);
    assert_eq!(wire(&AeadAlgorithm::Aes256Gcm), r#""aes-256-gcm""#);
    assert_eq!(wire(&DigestAlgorithm::Sha256), r#""sha-256""#);
}

/// The header names an encrypted object carries for the rest of its life.
#[tokio::test]
async fn object_encryption_header_names_are_stable() {
    let kms = TestKms::local_with(|config| config.default_key_id = Some("serde-key".to_string())).await;
    kms.create_key("serde-key").await;
    let service = kms.service().await;
    let data = b"serde contract".to_vec();

    let sse_s3 = service
        .encrypt_object("bucket", "object", data.as_slice(), &EncryptionAlgorithm::Aes256, None, None)
        .await
        .expect("SSE-S3 encrypt");
    let sse_kms = service
        .encrypt_object(
            "bucket",
            "object",
            data.as_slice(),
            &EncryptionAlgorithm::ChaCha20Poly1305,
            Some("serde-key"),
            None,
        )
        .await
        .expect("SSE-KMS encrypt");
    let sse_c = service
        .encrypt_object_with_customer_key("bucket", "object", data.as_slice(), &[0x5cu8; 32], None)
        .await
        .expect("SSE-C encrypt");

    let names = |result: &rustfs_kms::EncryptionMetadata| {
        let mut names: Vec<String> = service.metadata_to_headers(result).into_keys().collect();
        names.sort();
        names
    };

    insta::assert_yaml_snapshot!("sse_s3_header_names", names(&sse_s3.metadata));
    insta::assert_yaml_snapshot!("sse_kms_header_names", names(&sse_kms.metadata));
    insta::assert_yaml_snapshot!("sse_c_header_names", names(&sse_c.metadata));

    // The encryption context is embedded as a JSON object under one header;
    // its key set is part of the same contract.
    let mut context_keys: Vec<String> = sse_s3.metadata.encryption_context.keys().cloned().collect();
    context_keys.sort();
    insta::assert_yaml_snapshot!("sse_s3_encryption_context_keys", context_keys);
}

/// The shape of a persisted `KmsConfig` document, per backend.
#[test]
fn persisted_configuration_shape_is_stable() {
    let local = KmsConfig::local("/var/lib/rustfs/kms".into());
    insta::assert_yaml_snapshot!("kms_config_field_names", field_names(&local));

    let local_backend = serde_json::to_value(&local.backend_config).expect("serialize");
    insta::assert_yaml_snapshot!("backend_config_local_shape", shape_of(&local_backend));

    let static_config = KmsConfig::static_kms(STATIC_KEY_ID.to_string(), static_secret_key());
    let static_backend = serde_json::to_value(&static_config.backend_config).expect("serialize");
    insta::assert_yaml_snapshot!("backend_config_static_shape", shape_of(&static_backend));
    assert!(
        !serde_json::to_string(&static_config.backend_config)
            .expect("serialize")
            .contains(&static_secret_key()),
        "the static secret is `skip_serializing` and must never appear in a persisted document"
    );

    let vault = KmsConfig::vault(url::Url::parse("https://vault.example.com:8200").expect("url"), "token".to_string());
    let vault_backend = serde_json::to_value(&vault.backend_config).expect("serialize");
    insta::assert_yaml_snapshot!("backend_config_vault_kv2_shape", shape_of(&vault_backend));

    let transit = KmsConfig::vault_transit(url::Url::parse("https://vault.example.com:8200").expect("url"), "token".to_string());
    let transit_backend = serde_json::to_value(&transit.backend_config).expect("serialize");
    insta::assert_yaml_snapshot!("backend_config_vault_transit_shape", shape_of(&transit_backend));
}

/// Every persisted `KmsConfig` must survive a round trip unchanged, so a
/// document written by this build is readable by it.
#[test]
fn persisted_configuration_round_trips() {
    for (label, config) in [
        ("local", KmsConfig::local("/var/lib/rustfs/kms".into())),
        (
            "vault-kv2",
            KmsConfig::vault(url::Url::parse("https://vault.example.com:8200").expect("url"), "token".to_string()),
        ),
        (
            "vault-transit",
            KmsConfig::vault_transit(url::Url::parse("https://vault.example.com:8200").expect("url"), "token".to_string()),
        ),
    ] {
        let encoded = serde_json::to_string(&config).unwrap_or_else(|error| panic!("{label} should serialize: {error}"));
        let decoded: KmsConfig =
            serde_json::from_str(&encoded).unwrap_or_else(|error| panic!("{label} should deserialize: {error}"));
        assert_eq!(decoded.backend, config.backend, "{label}: backend must survive");
        assert_eq!(decoded.timeout, config.timeout, "{label}: timeout must survive");
        assert_eq!(decoded.retry_attempts, config.retry_attempts, "{label}: retries must survive");
        assert_eq!(decoded.enable_cache, config.enable_cache, "{label}: cache flag must survive");
        assert_eq!(decoded.default_key_id, config.default_key_id, "{label}: default key must survive");
        assert_eq!(
            serde_json::to_string(&decoded).expect("re-serialize"),
            encoded,
            "{label}: re-encoding a decoded document must be byte-identical"
        );
    }

    // The Static backend is the documented exception: its secret is dropped on
    // serialization, so a round trip cannot restore it. Recording that here
    // keeps a future reader from treating it as a bug.
    let static_config = KmsConfig::static_kms(STATIC_KEY_ID.to_string(), static_secret_key());
    let decoded: KmsConfig =
        serde_json::from_str(&serde_json::to_string(&static_config).expect("serialize")).expect("deserialize");
    assert!(
        decoded.static_config().expect("static config").secret_key.is_empty(),
        "a persisted static config never carries its secret; the operator re-supplies it"
    );
    assert_eq!(
        decoded.static_config().expect("static config").key_id,
        STATIC_KEY_ID,
        "the key id does survive persistence"
    );
}

/// The manifest a future restore has to decode.
#[tokio::test]
async fn backup_manifest_shape_is_stable() {
    use rustfs_kms::LocalConfig;
    use rustfs_kms::backends::local::LocalKmsClient;
    use rustfs_kms::backup::{BackupKek, LocalBackupExportRequest, export_local_backup};

    let kms = TestKms::local().await;
    kms.create_key("manifest-shape").await;
    let client = LocalKmsClient::new(LocalConfig {
        key_dir: kms.key_dir().expect("key dir"),
        master_key: None,
        file_permissions: Some(0o600),
    })
    .await
    .expect("client");

    let out = tempfile::TempDir::new().expect("temp dir");
    let manifest = export_local_backup(
        &client,
        &BackupKek::new("kek", 1, [0x11u8; 32]).expect("kek"),
        &LocalBackupExportRequest {
            backup_id: "shape".to_string(),
            deployment_identity: "shape-deployment".to_string(),
            rustfs_version: "0.0.0".to_string(),
            snapshot_generation: 1,
            destination: out.path().join("bundle"),
        },
    )
    .await
    .expect("export");

    insta::assert_yaml_snapshot!("backup_manifest_field_names", field_names(&manifest));

    let artifact = manifest.artifacts.first().expect("at least one artifact");
    insta::assert_yaml_snapshot!("backup_artifact_field_names", field_names(artifact));
    insta::assert_yaml_snapshot!("backup_kek_descriptor_field_names", field_names(&manifest.backup_kek));
    insta::assert_yaml_snapshot!(
        "backup_local_kdf_field_names",
        field_names(manifest.local_kdf.as_ref().expect("local kdf"))
    );
}

/// Describe a JSON value as a structural fingerprint: object keys are kept,
/// leaf values are replaced by their type name. Stable across runs while still
/// catching a renamed or retyped field.
fn shape_of(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            // Sort explicitly rather than relying on `serde_json::Map` being a
            // `BTreeMap`: that depends on the `preserve_order` feature, and an
            // unordered fingerprint would make these snapshots flaky.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), shape_of(&map[key]));
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => serde_json::Value::Array(items.iter().map(shape_of).take(1).collect()),
        serde_json::Value::String(_) => serde_json::Value::String("<string>".to_string()),
        serde_json::Value::Number(_) => serde_json::Value::String("<number>".to_string()),
        serde_json::Value::Bool(_) => serde_json::Value::String("<bool>".to_string()),
        serde_json::Value::Null => serde_json::Value::String("<null>".to_string()),
    }
}
