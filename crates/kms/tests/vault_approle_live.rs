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

//! Ignored live-Vault checks for the AppRole authentication path.
//!
//! `scripts/test/vault_approle_kms_live.sh` starts an ephemeral Vault, creates
//! a narrowly scoped AppRole, and runs each test with only the generated
//! role_id and secret_id. The tests then build the KMS configuration from the
//! same environment variables used by RustFS and exercise the real KV2 and
//! Transit backend calls with the AppRole-issued token.

use rustfs_kms::backends::KmsBackend as KmsBackendTrait;
use rustfs_kms::backends::vault::VaultKmsBackend;
use rustfs_kms::backends::vault_transit::VaultTransitKmsBackend;
use rustfs_kms::{
    BackendConfig, CreateKeyRequest, DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT,
    DecryptRequest, GenerateDataKeyRequest, KeySpec, KeyUsage, KmsBackend, KmsConfig, ListKeysRequest, VaultAuthMethod,
};
use std::collections::HashMap;

fn assert_approle_config(config: &KmsConfig, expected_backend: KmsBackend) {
    assert_eq!(config.backend, expected_backend);
    let auth_method = match &config.backend_config {
        BackendConfig::VaultKv2(vault) => &vault.auth_method,
        BackendConfig::VaultTransit(vault) => &vault.auth_method,
        _ => panic!("expected Vault configuration"),
    };
    assert!(
        matches!(auth_method, VaultAuthMethod::AppRole { .. }),
        "live check must use Vault AppRole auth"
    );
}

async fn exercise_backend<B: KmsBackendTrait + ?Sized>(backend: &B, key_prefix: &str) -> rustfs_kms::Result<()> {
    let key_id = format!("{key_prefix}-{}", uuid::Uuid::new_v4());
    let created = backend
        .create_key(CreateKeyRequest {
            key_name: Some(key_id.clone()),
            key_usage: KeyUsage::EncryptDecrypt,
            ..Default::default()
        })
        .await?;
    assert_eq!(created.key_id, key_id);

    let described = backend
        .describe_key(rustfs_kms::DescribeKeyRequest { key_id: key_id.clone() })
        .await?;
    assert_eq!(described.key_metadata.key_id, key_id);

    let listed = backend
        .list_keys(ListKeysRequest {
            limit: Some(100),
            ..Default::default()
        })
        .await?;
    assert!(
        listed.keys.iter().any(|key| key.key_id == key_id),
        "created key must be visible in the backend listing"
    );

    let context = HashMap::from([("live".to_string(), "approle".to_string())]);
    let generated = backend
        .generate_data_key(GenerateDataKeyRequest {
            key_id: key_id.clone(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        })
        .await?;
    assert_eq!(generated.key_id, key_id);
    assert_eq!(generated.plaintext_key.len(), 32, "AES-256 must return a 32-byte data key");

    let unwrapped = backend
        .decrypt(DecryptRequest {
            ciphertext: generated.ciphertext_blob,
            encryption_context: context,
            grant_tokens: Vec::new(),
        })
        .await?;
    assert_eq!(unwrapped.plaintext, generated.plaintext_key);
    Ok(())
}

#[tokio::test]
#[ignore = "requires a real Vault AppRole; run scripts/test/vault_approle_kms_live.sh"]
async fn vault_kv2_approle_auth_live() -> rustfs_kms::Result<()> {
    let config = KmsConfig::from_env()?;
    assert_approle_config(&config, KmsBackend::VaultKv2);
    let backend = VaultKmsBackend::new(config).await?;
    exercise_backend(&backend, "rustfs-approle-kv2").await
}

#[tokio::test]
#[ignore = "requires a real Vault AppRole; run scripts/test/vault_approle_kms_live.sh"]
async fn vault_transit_approle_auth_live() -> rustfs_kms::Result<()> {
    let config = KmsConfig::from_env()?;
    assert_approle_config(&config, KmsBackend::VaultTransit);
    let transit = match &config.backend_config {
        BackendConfig::VaultTransit(vault) => vault,
        _ => panic!("expected Vault Transit configuration"),
    };
    assert_eq!(transit.metadata_kv_mount, DEFAULT_VAULT_TRANSIT_METADATA_KV_MOUNT);
    assert_eq!(transit.metadata_key_prefix, DEFAULT_VAULT_TRANSIT_METADATA_KEY_PREFIX);

    let backend = VaultTransitKmsBackend::new(config).await?;
    exercise_backend(&backend, "rustfs-approle-transit").await
}
