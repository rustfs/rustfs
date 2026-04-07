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

//! Vault Transit-based KMS backend.

use crate::backends::{BackendInfo, KmsBackend, KmsClient};
use crate::config::{KmsConfig, VaultTransitConfig};
use crate::encryption::{DataKeyEnvelope, generate_key_material};
use crate::error::{KmsError, Result};
use crate::types::*;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jiff::Zoned;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;
use tokio::sync::RwLock;
use vaultrs::{
    api::transit::{
        KeyType,
        requests::{CreateKeyRequestBuilder, DecryptDataRequestBuilder, EncryptDataRequestBuilder},
    },
    client::{VaultClient, VaultClientSettingsBuilder},
    transit::{data, key},
};

#[derive(Debug, Clone)]
struct TransitKeyMetadata {
    key_usage: KeyUsage,
    description: Option<String>,
    tags: HashMap<String, String>,
    key_state: KeyState,
    created_at: Zoned,
    deletion_date: Option<Zoned>,
    origin: String,
    created_by: Option<String>,
    current_version: u32,
}

impl TransitKeyMetadata {
    fn from_create_request(request: &CreateKeyRequest) -> Self {
        Self {
            key_usage: request.key_usage.clone(),
            description: request.description.clone(),
            tags: request.tags.clone(),
            key_state: KeyState::Enabled,
            created_at: Zoned::now(),
            deletion_date: None,
            origin: request.origin.clone().unwrap_or_else(|| "VAULT_TRANSIT".to_string()),
            created_by: None,
            current_version: 1,
        }
    }

    fn synthesized() -> Self {
        Self {
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            tags: HashMap::new(),
            key_state: KeyState::Enabled,
            created_at: Zoned::now(),
            deletion_date: None,
            origin: "VAULT_TRANSIT".to_string(),
            created_by: None,
            current_version: 1,
        }
    }
}

pub struct VaultTransitKmsClient {
    client: VaultClient,
    config: VaultTransitConfig,
    metadata_cache: RwLock<HashMap<String, TransitKeyMetadata>>,
}

impl VaultTransitKmsClient {
    pub async fn new(config: VaultTransitConfig) -> Result<Self> {
        let mut settings_builder = VaultClientSettingsBuilder::default();
        settings_builder.address(&config.address);

        let token = match &config.auth_method {
            crate::config::VaultAuthMethod::Token { token } => token.clone(),
            crate::config::VaultAuthMethod::AppRole { .. } => {
                return Err(KmsError::backend_error(
                    "AppRole authentication not yet implemented. Please use token authentication.",
                ));
            }
        };

        settings_builder.token(&token);

        if let Some(namespace) = &config.namespace {
            settings_builder.namespace(Some(namespace.clone()));
        }

        let settings = settings_builder
            .build()
            .map_err(|e| KmsError::backend_error(format!("Failed to build Vault client settings: {e}")))?;

        let client =
            VaultClient::new(settings).map_err(|e| KmsError::backend_error(format!("Failed to create Vault client: {e}")))?;

        Ok(Self {
            client,
            config,
            metadata_cache: RwLock::new(HashMap::new()),
        })
    }

    fn canonicalize_context(encryption_context: &HashMap<String, String>) -> Result<Option<String>> {
        if encryption_context.is_empty() {
            return Ok(None);
        }

        let ordered: BTreeMap<_, _> = encryption_context
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        let serialized = serde_json::to_vec(&ordered)?;
        Ok(Some(BASE64.encode(serialized)))
    }

    fn map_vault_error<T>(key_id: &str, error: vaultrs::error::ClientError, operation: &str) -> Result<T> {
        match error {
            vaultrs::error::ClientError::ResponseWrapError => Err(KmsError::key_not_found(key_id)),
            vaultrs::error::ClientError::APIError { code: 404, .. } => Err(KmsError::key_not_found(key_id)),
            other => Err(KmsError::backend_error(format!(
                "Vault Transit {operation} failed for key {key_id}: {other}"
            ))),
        }
    }

    async fn read_transit_key(&self, key_id: &str) -> Result<vaultrs::api::transit::responses::ReadKeyResponse> {
        key::read(&self.client, &self.config.mount_path, key_id)
            .await
            .or_else(|e| Self::map_vault_error(key_id, e, "read"))
    }

    async fn create_transit_key(&self, key_id: &str) -> Result<()> {
        let mut builder = CreateKeyRequestBuilder::default();
        builder.key_type(KeyType::Aes256Gcm96);
        key::create(&self.client, &self.config.mount_path, key_id, Some(&mut builder))
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to create Vault Transit key {key_id}: {e}")))
    }

    async fn transit_encrypt(
        &self,
        key_id: &str,
        plaintext: &[u8],
        encryption_context: &HashMap<String, String>,
    ) -> Result<String> {
        let plaintext_b64 = BASE64.encode(plaintext);
        let mut builder = EncryptDataRequestBuilder::default();
        if let Some(aad) = Self::canonicalize_context(encryption_context)? {
            builder.associated_data(aad);
        }

        let response = data::encrypt(&self.client, &self.config.mount_path, key_id, &plaintext_b64, Some(&mut builder))
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to encrypt data with Vault Transit key {key_id}: {e}")))?;

        Ok(response.ciphertext)
    }

    async fn transit_decrypt(
        &self,
        key_id: &str,
        ciphertext: &str,
        encryption_context: &HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let mut builder = DecryptDataRequestBuilder::default();
        if let Some(aad) = Self::canonicalize_context(encryption_context)? {
            builder.associated_data(aad);
        }

        let response = data::decrypt(&self.client, &self.config.mount_path, key_id, ciphertext, Some(&mut builder))
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to decrypt data with Vault Transit key {key_id}: {e}")))?;

        BASE64
            .decode(response.plaintext)
            .map_err(|e| KmsError::cryptographic_error("base64_decode", e.to_string()))
    }

    async fn get_key_metadata(&self, key_id: &str) -> Result<TransitKeyMetadata> {
        if let Some(metadata) = self.metadata_cache.read().await.get(key_id).cloned() {
            return Ok(metadata);
        }

        self.read_transit_key(key_id).await?;
        let metadata = TransitKeyMetadata::synthesized();
        self.metadata_cache.write().await.insert(key_id.to_string(), metadata.clone());
        Ok(metadata)
    }

    async fn store_key_metadata(&self, key_id: &str, metadata: &TransitKeyMetadata) -> Result<()> {
        self.metadata_cache.write().await.insert(key_id.to_string(), metadata.clone());
        Ok(())
    }

    async fn delete_key_metadata(&self, key_id: &str) -> Result<()> {
        self.metadata_cache.write().await.remove(key_id);
        Ok(())
    }

    async fn key_info(&self, key_id: &str) -> Result<KeyInfo> {
        self.read_transit_key(key_id).await?;
        let metadata = self.get_key_metadata(key_id).await?;

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            description: metadata.description.clone(),
            algorithm: "AES_256".to_string(),
            usage: metadata.key_usage.clone(),
            status: match metadata.key_state {
                KeyState::Enabled => KeyStatus::Active,
                KeyState::Disabled => KeyStatus::Disabled,
                KeyState::PendingDeletion => KeyStatus::PendingDeletion,
                KeyState::PendingImport | KeyState::Unavailable => KeyStatus::Deleted,
            },
            version: metadata.current_version,
            metadata: metadata.tags.clone(),
            tags: metadata.tags,
            created_at: metadata.created_at,
            rotated_at: None,
            created_by: metadata.created_by,
        })
    }

    async fn key_metadata_response(&self, key_id: &str) -> Result<KeyMetadata> {
        self.read_transit_key(key_id).await?;
        let metadata = self.get_key_metadata(key_id).await?;

        Ok(KeyMetadata {
            key_id: key_id.to_string(),
            key_state: metadata.key_state,
            key_usage: metadata.key_usage,
            description: metadata.description,
            creation_date: metadata.created_at,
            deletion_date: metadata.deletion_date,
            origin: metadata.origin,
            key_manager: "VAULT_TRANSIT".to_string(),
            tags: metadata.tags,
        })
    }

    async fn ensure_key_active(&self, key_id: &str) -> Result<TransitKeyMetadata> {
        let metadata = self.get_key_metadata(key_id).await?;
        if metadata.key_state != KeyState::Enabled {
            return Err(KmsError::invalid_operation(format!(
                "Key {key_id} is not active (state: {:?})",
                metadata.key_state
            )));
        }
        Ok(metadata)
    }
}

#[async_trait]
impl KmsClient for VaultTransitKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKeyInfo> {
        self.ensure_key_active(&request.master_key_id).await?;

        let plaintext_key = generate_key_material(&request.key_spec)?;
        let encrypted_key = self
            .transit_encrypt(&request.master_key_id, &plaintext_key, &request.encryption_context)
            .await?;

        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key: encrypted_key.into_bytes(),
            nonce: Vec::new(),
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
        };

        let ciphertext = serde_json::to_vec(&envelope)?;
        Ok(DataKeyInfo::new(
            envelope.key_id,
            1,
            Some(plaintext_key),
            ciphertext,
            request.key_spec.clone(),
        ))
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        let metadata = self.ensure_key_active(&request.key_id).await?;
        let ciphertext = self
            .transit_encrypt(&request.key_id, &request.plaintext, &request.encryption_context)
            .await?;

        Ok(EncryptResponse {
            ciphertext: ciphertext.into_bytes(),
            key_id: request.key_id.clone(),
            key_version: metadata.current_version,
            algorithm: "vault-transit".to_string(),
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;

        for (key, expected_value) in &envelope.encryption_context {
            if let Some(actual_value) = request.encryption_context.get(key) {
                if actual_value != expected_value {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
            } else if !request.encryption_context.is_empty() {
                return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
            }
        }

        let encrypted_key = std::str::from_utf8(&envelope.encrypted_key)
            .map_err(|e| KmsError::cryptographic_error("utf8", format!("Invalid Transit ciphertext: {e}")))?;
        self.transit_decrypt(&envelope.master_key_id, encrypted_key, &envelope.encryption_context)
            .await
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        if algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(algorithm));
        }

        if self.read_transit_key(key_id).await.is_ok() {
            return Err(KmsError::key_already_exists(key_id));
        }

        self.create_transit_key(key_id).await?;

        let metadata = TransitKeyMetadata {
            created_by: Some("vault-transit".to_string()),
            ..TransitKeyMetadata::from_create_request(&CreateKeyRequest {
                key_name: Some(key_id.to_string()),
                ..Default::default()
            })
        };
        self.store_key_metadata(key_id, &metadata).await?;

        Ok(MasterKeyInfo {
            key_id: key_id.to_string(),
            version: metadata.current_version,
            algorithm: algorithm.to_string(),
            usage: metadata.key_usage,
            status: KeyStatus::Active,
            description: metadata.description,
            metadata: metadata.tags,
            created_at: metadata.created_at,
            rotated_at: None,
            created_by: metadata.created_by,
        })
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        self.key_info(key_id).await
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        let all_keys = key::list(&self.client, &self.config.mount_path)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to list Vault Transit keys: {e}")))?
            .keys;

        let mut filtered = Vec::new();
        for key_id in all_keys {
            let key_info = self.key_info(&key_id).await?;
            let usage_matches = request.usage_filter.as_ref().is_none_or(|usage| usage == &key_info.usage);
            let status_matches = request.status_filter.as_ref().is_none_or(|status| status == &key_info.status);
            if usage_matches && status_matches {
                filtered.push(key_info);
            }
        }

        let start_idx = request
            .marker
            .as_ref()
            .and_then(|marker| filtered.iter().position(|info| &info.key_id == marker))
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let limit = request.limit.unwrap_or(100) as usize;
        let end_idx = std::cmp::min(start_idx + limit, filtered.len());
        let keys = filtered[start_idx..end_idx].to_vec();
        let next_marker = if end_idx < filtered.len() {
            Some(filtered[end_idx - 1].key_id.clone())
        } else {
            None
        };

        Ok(ListKeysResponse {
            keys,
            next_marker,
            truncated: end_idx < filtered.len(),
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        let mut metadata = self.get_key_metadata(key_id).await?;
        metadata.key_state = KeyState::Enabled;
        metadata.deletion_date = None;
        self.store_key_metadata(key_id, &metadata).await
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        let mut metadata = self.get_key_metadata(key_id).await?;
        metadata.key_state = KeyState::Disabled;
        self.store_key_metadata(key_id, &metadata).await
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        let mut metadata = self.get_key_metadata(key_id).await?;
        metadata.key_state = KeyState::PendingDeletion;
        metadata.deletion_date = Some(Zoned::now() + Duration::from_secs(pending_window_days as u64 * 86400));
        self.store_key_metadata(key_id, &metadata).await
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        let mut metadata = self.get_key_metadata(key_id).await?;
        metadata.key_state = KeyState::Enabled;
        metadata.deletion_date = None;
        self.store_key_metadata(key_id, &metadata).await
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        key::rotate(&self.client, &self.config.mount_path, key_id)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to rotate Vault Transit key {key_id}: {e}")))?;

        let mut metadata = self.get_key_metadata(key_id).await?;
        metadata.current_version += 1;
        self.store_key_metadata(key_id, &metadata).await?;

        Ok(MasterKeyInfo {
            key_id: key_id.to_string(),
            version: metadata.current_version,
            algorithm: "AES_256".to_string(),
            usage: metadata.key_usage,
            status: KeyStatus::Active,
            description: metadata.description,
            metadata: metadata.tags,
            created_at: metadata.created_at,
            rotated_at: Some(Zoned::now()),
            created_by: metadata.created_by,
        })
    }

    async fn health_check(&self) -> Result<()> {
        key::list(&self.client, &self.config.mount_path)
            .await
            .map(|_| ())
            .map_err(|e| KmsError::backend_error(format!("Vault Transit health check failed: {e}")))
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo::new("vault-transit".to_string(), "0.1.0".to_string(), self.config.address.clone(), true)
            .with_metadata("mount_path".to_string(), self.config.mount_path.clone())
    }
}

pub struct VaultTransitKmsBackend {
    client: VaultTransitKmsClient,
}

impl VaultTransitKmsBackend {
    pub async fn new(config: KmsConfig) -> Result<Self> {
        let vault_config = match &config.backend_config {
            crate::config::BackendConfig::VaultTransit(vault_config) => (**vault_config).clone(),
            crate::config::BackendConfig::VaultKv2(vault_config) => VaultTransitConfig {
                address: vault_config.address.clone(),
                auth_method: vault_config.auth_method.clone(),
                namespace: vault_config.namespace.clone(),
                mount_path: vault_config.mount_path.clone(),
                tls: vault_config.tls.clone(),
            },
            crate::config::BackendConfig::Local(_) => {
                return Err(KmsError::configuration_error("Expected Vault Transit backend configuration"));
            }
        };

        let client = VaultTransitKmsClient::new(vault_config).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl KmsBackend for VaultTransitKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        if self.client.read_transit_key(&key_id).await.is_ok() {
            return Err(KmsError::key_already_exists(&key_id));
        }

        self.client.create_transit_key(&key_id).await?;
        let metadata = TransitKeyMetadata::from_create_request(&request);
        self.client.store_key_metadata(&key_id, &metadata).await?;

        Ok(CreateKeyResponse {
            key_id: key_id.clone(),
            key_metadata: KeyMetadata {
                key_id,
                key_state: metadata.key_state,
                key_usage: metadata.key_usage,
                description: metadata.description,
                creation_date: metadata.created_at,
                deletion_date: metadata.deletion_date,
                origin: metadata.origin,
                key_manager: "VAULT_TRANSIT".to_string(),
                tags: metadata.tags,
            },
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        self.client.encrypt(&request, None).await
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let envelope = DataKeyEnvelope::deserialize(&request.ciphertext_blob)?;
        let plaintext = self.client.decrypt(&request, None).await?;
        Ok(DecryptResponse {
            plaintext,
            key_id: envelope.master_key_id,
            encryption_algorithm: Some("vault-transit".to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let generate_request = GenerateKeyRequest {
            master_key_id: request.key_id.clone(),
            key_spec: request.key_spec.as_str().to_string(),
            key_length: Some(request.key_spec.key_size() as u32),
            encryption_context: request.encryption_context,
            grant_tokens: Vec::new(),
        };

        let data_key = self.client.generate_data_key(&generate_request, None).await?;
        let plaintext_key = data_key.plaintext.clone().unwrap_or_default();
        let ciphertext_blob = data_key.ciphertext.clone();
        Ok(GenerateDataKeyResponse {
            key_id: request.key_id,
            plaintext_key,
            ciphertext_blob,
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        Ok(DescribeKeyResponse {
            key_metadata: self.client.key_metadata_response(&request.key_id).await?,
        })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        self.client.list_keys(&request, None).await
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        let key_id = request.key_id;
        let mut key_metadata = self.client.key_metadata_response(&key_id).await?;

        let deletion_date = if request.force_immediate.unwrap_or(false) {
            if key_metadata.key_state == KeyState::PendingDeletion {
                key::delete(&self.client.client, &self.client.config.mount_path, &key_id)
                    .await
                    .map_err(|e| KmsError::backend_error(format!("Failed to delete Vault Transit key {key_id}: {e}")))?;
                self.client.delete_key_metadata(&key_id).await?;
                None
            } else {
                let mut metadata = self.client.get_key_metadata(&key_id).await?;
                metadata.key_state = KeyState::PendingDeletion;
                metadata.deletion_date = Some(Zoned::now());
                self.client.store_key_metadata(&key_id, &metadata).await?;
                key_metadata = self.client.key_metadata_response(&key_id).await?;
                None
            }
        } else {
            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(KmsError::invalid_parameter("pending_window_in_days must be between 7 and 30"));
            }

            let mut metadata = self.client.get_key_metadata(&key_id).await?;
            let scheduled = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            metadata.key_state = KeyState::PendingDeletion;
            metadata.deletion_date = Some(scheduled.clone());
            self.client.store_key_metadata(&key_id, &metadata).await?;
            key_metadata = self.client.key_metadata_response(&key_id).await?;
            Some(scheduled.to_string())
        };

        Ok(DeleteKeyResponse {
            key_id,
            deletion_date,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let mut metadata = self.client.get_key_metadata(&request.key_id).await?;
        if metadata.key_state != KeyState::PendingDeletion {
            return Err(KmsError::invalid_key_state(format!("Key {} is not pending deletion", request.key_id)));
        }

        metadata.key_state = KeyState::Enabled;
        metadata.deletion_date = None;
        self.client.store_key_metadata(&request.key_id, &metadata).await?;

        Ok(CancelKeyDeletionResponse {
            key_id: request.key_id.clone(),
            key_metadata: self.client.key_metadata_response(&request.key_id).await?,
        })
    }

    async fn health_check(&self) -> Result<bool> {
        self.client.health_check().await.map(|_| true)
    }
}
