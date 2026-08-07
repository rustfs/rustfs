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

//! Shared key state × operation contract tests for KMS backends.
//!
//! Every stateful backend must satisfy the same lifecycle matrix (see
//! `ensure_key_state_permits`): Enabled permits everything, Disabled permits
//! decryption and lifecycle recovery but rejects new cryptographic use, and
//! PendingDeletion rejects everything except decryption and cancellation.
//! Decryption staying available in Disabled/PendingDeletion is an explicit,
//! tested deviation from AWS KMS: disabling a key must not break reads of
//! objects already encrypted under it.
//!
//! The full matrix runs offline against the Local backend. The Vault KV2 and
//! Vault Transit runs exercise the same helper but need a live Vault dev
//! server, so they are `#[ignore]`d in CI. Static is covered by its own
//! stateless contract below.

use super::KmsBackend;
use super::local::LocalKmsBackend;
use super::static_kms::StaticKmsBackend;
use super::vault::VaultKmsBackend;
use super::vault_transit::VaultTransitKmsBackend;
use crate::config::KmsConfig;
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use crate::service::ObjectEncryptionService;
use crate::types::{
    CancelKeyDeletionRequest, CreateKeyRequest, DecryptRequest, DeleteKeyRequest, DescribeDataKeyWrappingRequest,
    DescribeKeyRequest, EncryptRequest, GenerateDataKeyRequest, KeySpec, KeyState, KeyUsage, ObjectEncryptionContext,
    RewrapDataKeyRequest,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use rand::RngExt as _;
use std::collections::HashMap;
use std::sync::Arc;

fn expect_unsupported<T: std::fmt::Debug>(result: Result<T>) {
    match result {
        Err(KmsError::UnsupportedCapability { .. }) => {}
        other => panic!("expected UnsupportedCapability, got {other:?}"),
    }
}

/// Rotation while not Enabled: backends with rotation support must reject it
/// through the state machine; backends without it report the capability gap.
async fn expect_rotate_rejected(backend: &dyn KmsBackend, key_id: &str) {
    let result = backend.rotate_key(key_id).await;
    if backend.capabilities().rotate {
        expect_invalid_key_state(result, "");
    } else {
        expect_unsupported(result);
    }
}

/// Rewrap while not Enabled: it produces a new wrapping, so backends that
/// support it must gate it through the state machine exactly as encryption is
/// gated; backends without version history report the capability gap instead.
async fn expect_rewrap_rejected(backend: &dyn KmsBackend, ciphertext: Vec<u8>) {
    let result = backend
        .rewrap_data_key(RewrapDataKeyRequest {
            ciphertext,
            encryption_context: context(),
        })
        .await;
    if backend.capabilities().rewrap {
        expect_invalid_key_state(result, "");
    } else {
        expect_unsupported(result);
    }
}

fn expect_invalid_key_state<T: std::fmt::Debug>(result: Result<T>, expected_fragment: &str) {
    match result {
        Err(KmsError::InvalidOperation { message }) => assert!(
            message.contains(expected_fragment),
            "expected invalid-key-state message containing {expected_fragment:?}, got {message:?}"
        ),
        other => panic!("expected InvalidOperation (invalid key state), got {other:?}"),
    }
}

fn context() -> HashMap<String, String> {
    HashMap::from([("bucket".to_string(), "contract".to_string())])
}

fn generate_request(key_id: &str) -> GenerateDataKeyRequest {
    GenerateDataKeyRequest {
        key_id: key_id.to_string(),
        key_spec: KeySpec::Aes256,
        encryption_context: context(),
    }
}

fn encrypt_request(key_id: &str) -> EncryptRequest {
    EncryptRequest {
        key_id: key_id.to_string(),
        plaintext: b"contract-plaintext".to_vec(),
        encryption_context: context(),
        grant_tokens: Vec::new(),
    }
}

fn decrypt_request(ciphertext: Vec<u8>) -> DecryptRequest {
    DecryptRequest {
        ciphertext,
        encryption_context: context(),
        grant_tokens: Vec::new(),
    }
}

fn schedule_request(key_id: &str) -> DeleteKeyRequest {
    DeleteKeyRequest {
        key_id: key_id.to_string(),
        pending_window_in_days: Some(7),
        force_immediate: None,
        confirm_key_id: None,
    }
}

fn cancel_request(key_id: &str) -> CancelKeyDeletionRequest {
    CancelKeyDeletionRequest {
        key_id: key_id.to_string(),
    }
}

fn create_request(key_name: String) -> CreateKeyRequest {
    CreateKeyRequest {
        key_name: Some(key_name),
        key_usage: KeyUsage::EncryptDecrypt,
        ..Default::default()
    }
}

async fn assert_key_state(backend: &dyn KmsBackend, key_id: &str, expected: KeyState) {
    let described = backend
        .describe_key(DescribeKeyRequest {
            key_id: key_id.to_string(),
        })
        .await
        .expect("describe_key must succeed for an existing key");
    assert_eq!(described.key_metadata.key_state, expected, "unexpected state for key {key_id}");
}

/// Drives one freshly created (Enabled) key through the full state matrix,
/// entirely through the `KmsBackend` product surface.
async fn assert_state_machine_contract(backend: &dyn KmsBackend, key_id: &str) {
    // Enabled: cryptographic use is allowed. Keep an envelope around to prove
    // decryption keeps working in later states.
    let data_key = backend
        .generate_data_key(generate_request(key_id))
        .await
        .expect("Enabled key must generate data keys");
    backend
        .encrypt(encrypt_request(key_id))
        .await
        .expect("Enabled key must encrypt");

    // Enabled -> Disabled.
    backend.disable_key(key_id).await.expect("disable from Enabled must succeed");
    assert_key_state(backend, key_id, KeyState::Disabled).await;

    // Disabled: new cryptographic use and rotation are rejected...
    expect_invalid_key_state(backend.encrypt(encrypt_request(key_id)).await, "disabled");
    expect_invalid_key_state(backend.generate_data_key(generate_request(key_id)).await, "disabled");
    expect_rotate_rejected(backend, key_id).await;
    expect_rewrap_rejected(backend, data_key.ciphertext_blob.clone()).await;
    // ...but decryption of existing data keeps working (explicit AWS deviation)...
    let decrypted = backend
        .decrypt(decrypt_request(data_key.ciphertext_blob.clone()))
        .await
        .expect("decrypt with a disabled key must keep working");
    assert_eq!(decrypted.plaintext, data_key.plaintext_key, "decrypt must recover the original data key");
    assert_eq!(decrypted.key_id, key_id, "decrypt must report the master key that opened the envelope");
    // ...disable stays idempotent, cancel has nothing to cancel, and enable recovers.
    backend.disable_key(key_id).await.expect("disable must be idempotent");
    expect_invalid_key_state(backend.cancel_key_deletion(cancel_request(key_id)).await, "not pending deletion");
    backend.enable_key(key_id).await.expect("enable from Disabled must succeed");
    assert_key_state(backend, key_id, KeyState::Enabled).await;

    // Disabled keys may still be scheduled for deletion.
    backend
        .disable_key(key_id)
        .await
        .expect("disable before scheduling must succeed");
    backend
        .delete_key(schedule_request(key_id))
        .await
        .expect("scheduling deletion of a disabled key must succeed");
    assert_key_state(backend, key_id, KeyState::PendingDeletion).await;

    // PendingDeletion: everything except decryption and cancellation is rejected.
    expect_invalid_key_state(backend.encrypt(encrypt_request(key_id)).await, "pending deletion");
    expect_invalid_key_state(backend.generate_data_key(generate_request(key_id)).await, "pending deletion");
    expect_invalid_key_state(backend.enable_key(key_id).await, "pending deletion");
    expect_invalid_key_state(backend.disable_key(key_id).await, "pending deletion");
    expect_rotate_rejected(backend, key_id).await;
    expect_rewrap_rejected(backend, data_key.ciphertext_blob.clone()).await;
    expect_invalid_key_state(backend.delete_key(schedule_request(key_id)).await, "pending deletion");
    let decrypted = backend
        .decrypt(decrypt_request(data_key.ciphertext_blob.clone()))
        .await
        .expect("decrypt with a pending-deletion key must keep working");
    assert_eq!(decrypted.plaintext, data_key.plaintext_key);

    // PendingDeletion -> Enabled through cancellation.
    backend
        .cancel_key_deletion(cancel_request(key_id))
        .await
        .expect("cancel from PendingDeletion must succeed");
    assert_key_state(backend, key_id, KeyState::Enabled).await;
    backend
        .generate_data_key(generate_request(key_id))
        .await
        .expect("cancelled key must be usable again");

    // Cancel without a pending deletion is an invalid state transition.
    expect_invalid_key_state(backend.cancel_key_deletion(cancel_request(key_id)).await, "not pending deletion");
}

async fn local_fixture() -> (tempfile::TempDir, KmsConfig, LocalKmsBackend, String) {
    let temp_dir = tempfile::tempdir().expect("temp dir should be created");
    let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
    let backend = LocalKmsBackend::new(config.clone())
        .await
        .expect("local backend should build");
    let created = backend
        .create_key(create_request("contract-key".to_string()))
        .await
        .expect("key should be created");
    (temp_dir, config, backend, created.key_id)
}

#[tokio::test]
async fn local_backend_state_machine_contract() {
    let (_temp_dir, _config, backend, key_id) = local_fixture().await;
    assert_state_machine_contract(&backend, &key_id).await;
}

/// SSE-shaped regression: disabling a key must not break decryption of data
/// keys created while it was enabled, while new data key creation must fail.
#[tokio::test]
async fn local_disabled_key_keeps_decrypting_existing_envelopes() {
    let (_temp_dir, config, backend, key_id) = local_fixture().await;
    let backend = Arc::new(backend);
    let service = ObjectEncryptionService::new(KmsManager::new(backend.clone(), config));

    let object_context = ObjectEncryptionContext::new("sse-bucket".to_string(), "dir/object.bin".to_string());
    let kms_key = Some(key_id.clone());
    let (_data_key, encrypted_blob) = service
        .create_data_key(&kms_key, &object_context)
        .await
        .expect("data key creation must succeed while the key is enabled");

    backend
        .lifecycle_client()
        .disable_key(&key_id, None)
        .await
        .expect("disable must succeed");

    service
        .decrypt_data_key(&encrypted_blob, &object_context)
        .await
        .expect("existing objects must stay readable after their KMS key is disabled");
    expect_invalid_key_state(service.create_data_key(&kms_key, &object_context).await, "disabled");
}

/// Static is a stateless read-only backend: cryptographic operations always
/// work against the single configured key and every lifecycle mutation is
/// rejected as an invalid operation.
#[tokio::test]
async fn static_backend_stateless_contract() {
    let key_id = "static-contract-key";
    let mut raw_key = [0u8; 32];
    rand::rng().fill(&mut raw_key[..]);
    let config = KmsConfig::static_kms(key_id.to_string(), BASE64.encode(raw_key));
    let static_backend = StaticKmsBackend::new(config).await.expect("static backend should build");
    let backend: &dyn KmsBackend = &static_backend;

    let data_key = backend
        .generate_data_key(generate_request(key_id))
        .await
        .expect("static backend must generate data keys");
    let decrypted = backend
        .decrypt(decrypt_request(data_key.ciphertext_blob.clone()))
        .await
        .expect("static backend must decrypt its own envelopes");
    assert_eq!(decrypted.plaintext, data_key.plaintext_key);
    assert_key_state(backend, key_id, KeyState::Enabled).await;

    expect_invalid_key_state(backend.create_key(create_request("another-key".to_string())).await, "read-only");
    expect_invalid_key_state(backend.delete_key(schedule_request(key_id)).await, "read-only");
    expect_invalid_key_state(backend.cancel_key_deletion(cancel_request(key_id)).await, "read-only");
    // Enable/disable, rotation and rewrap are capability gaps at the product
    // surface, not state-machine rejections. A single fixed key has no second
    // version to rewrap onto, so reporting the gap is the only honest answer —
    // re-wrapping with the same material would look like progress while
    // changing nothing.
    expect_unsupported(backend.enable_key(key_id).await);
    expect_unsupported(backend.disable_key(key_id).await);
    expect_unsupported(backend.rotate_key(key_id).await);
    expect_unsupported(
        backend
            .rewrap_data_key(RewrapDataKeyRequest {
                ciphertext: data_key.ciphertext_blob.clone(),
                encryption_context: context(),
            })
            .await,
    );
    expect_unsupported(
        backend
            .describe_data_key_wrapping(DescribeDataKeyWrappingRequest {
                ciphertext: data_key.ciphertext_blob,
                encryption_context: context(),
            })
            .await,
    );
}

fn vault_dev_config(constructor: fn(url::Url, String) -> KmsConfig) -> KmsConfig {
    let address = std::env::var("RUSTFS_KMS_VAULT_ADDR").unwrap_or_else(|_| "http://127.0.0.1:8200".to_string());
    let token = std::env::var("RUSTFS_KMS_VAULT_TOKEN").unwrap_or_else(|_| "dev-token".to_string());
    let mut config = constructor(url::Url::parse(&address).expect("vault address should parse"), token);
    config.allow_insecure_dev_defaults = true;
    config
}

#[tokio::test]
#[ignore] // Requires a running Vault instance (dev mode) with a KV2 mount
async fn vault_kv2_backend_state_machine_contract() {
    let config = vault_dev_config(KmsConfig::vault);
    let backend = VaultKmsBackend::new(config).await.expect("vault kv2 backend should build");
    let created = backend
        .create_key(create_request(format!("contract-{}", uuid::Uuid::new_v4())))
        .await
        .expect("key should be created");

    assert_state_machine_contract(&backend, &created.key_id).await;

    // KV2 additionally supports version-retaining rotation, which must only
    // work while the key is Enabled (the shared matrix covered the
    // rejections).
    backend
        .rotate_key(&created.key_id)
        .await
        .expect("rotation of an Enabled KV2 key must succeed");

    // Cleanup: leave the key pending deletion so repeated runs stay tidy.
    let _ = backend.delete_key(schedule_request(&created.key_id)).await;
}

#[tokio::test]
#[ignore] // Requires a running Vault instance (dev mode) with the transit engine enabled
async fn vault_transit_backend_state_machine_contract() {
    let config = vault_dev_config(KmsConfig::vault_transit);
    let backend = VaultTransitKmsBackend::new(config)
        .await
        .expect("vault transit backend should build");
    let created = backend
        .create_key(create_request(format!("contract-{}", uuid::Uuid::new_v4())))
        .await
        .expect("key should be created");

    assert_state_machine_contract(&backend, &created.key_id).await;

    // Transit additionally supports rotation, which must only work while the
    // key is Enabled (the shared matrix already covered the rejections).
    backend
        .rotate_key(&created.key_id)
        .await
        .expect("rotation of an Enabled transit key must succeed");

    let _ = backend.delete_key(schedule_request(&created.key_id)).await;
}
