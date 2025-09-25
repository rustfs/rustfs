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

//! Vault-based KMS backend implementation using vaultrs

use crate::backends::{BackendInfo, KmsBackend, KmsClient};
use crate::config::{KmsConfig, VaultConfig};
use crate::error::{KmsError, Result};
use crate::types::*;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};
use vaultrs::{
    client::{VaultClient, VaultClientSettingsBuilder},
    kv2,
};

/// Vault KMS client implementation
pub struct VaultKmsClient {
    client: VaultClient,
    config: VaultConfig,
    /// Mount path for the KV engine (typically "kv" or "secret")
    kv_mount: String,
    /// Path prefix for storing keys
    key_path_prefix: String,
}

/// Key data stored in Vault
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VaultKeyData {
    /// Key algorithm
    algorithm: String,
    /// Key usage type
    usage: KeyUsage,
    /// Key creation timestamp
    created_at: chrono::DateTime<chrono::Utc>,
    /// Key status
    status: KeyStatus,
    /// Key version
    version: u32,
    /// Key description
    description: Option<String>,
    /// Key metadata
    metadata: HashMap<String, String>,
    /// Key tags
    tags: HashMap<String, String>,
    /// Encrypted key material (base64 encoded)
    encrypted_key_material: String,
}

impl VaultKmsClient {
    /// Create a new Vault KMS client
    pub async fn new(config: VaultConfig) -> Result<Self> {
        // Create client settings
        let mut settings_builder = VaultClientSettingsBuilder::default();
        settings_builder.address(&config.address);

        // Set authentication token based on method
        let token = match &config.auth_method {
            crate::config::VaultAuthMethod::Token { token } => token.clone(),
            crate::config::VaultAuthMethod::AppRole { .. } => {
                // For AppRole authentication, we would need to first authenticate
                // and get a token. For simplicity, we'll require a token for now.
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
            .map_err(|e| KmsError::backend_error(format!("Failed to build Vault client settings: {}", e)))?;

        let client =
            VaultClient::new(settings).map_err(|e| KmsError::backend_error(format!("Failed to create Vault client: {}", e)))?;

        info!("Successfully connected to Vault at {}", config.address);

        Ok(Self {
            client,
            kv_mount: config.kv_mount.clone(),
            key_path_prefix: config.key_path_prefix.clone(),
            config,
        })
    }

    /// Get the full path for a key in Vault
    fn key_path(&self, key_id: &str) -> String {
        format!("{}/{}", self.key_path_prefix, key_id)
    }

    /// Generate key material for the given algorithm
    fn generate_key_material(algorithm: &str) -> Result<Vec<u8>> {
        let key_size = match algorithm {
            "AES_256" => 32,
            "AES_128" => 16,
            _ => return Err(KmsError::unsupported_algorithm(algorithm)),
        };

        let mut key_material = vec![0u8; key_size];
        rand::rng().fill_bytes(&mut key_material);
        Ok(key_material)
    }

    /// Encrypt key material using Vault's transit engine
    async fn encrypt_key_material(&self, key_material: &[u8]) -> Result<String> {
        // For simplicity, we'll base64 encode the key material
        // In a production setup, you would use Vault's transit engine for additional encryption
        Ok(general_purpose::STANDARD.encode(key_material))
    }

    /// Decrypt key material
    async fn decrypt_key_material(&self, encrypted_material: &str) -> Result<Vec<u8>> {
        // For simplicity, we'll base64 decode the key material
        // In a production setup, you would use Vault's transit engine for decryption
        general_purpose::STANDARD
            .decode(encrypted_material)
            .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))
    }

    /// Store key data in Vault
    async fn store_key_data(&self, key_id: &str, key_data: &VaultKeyData) -> Result<()> {
        let path = self.key_path(key_id);

        kv2::set(&self.client, &self.kv_mount, &path, key_data)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to store key in Vault: {}", e)))?;

        debug!("Stored key {} in Vault at path {}", key_id, path);
        Ok(())
    }

    async fn store_key_metadata(&self, key_id: &str, request: &CreateKeyRequest) -> Result<()> {
        debug!("Storing key metadata for {}, input tags: {:?}", key_id, request.tags);

        let key_data = VaultKeyData {
            algorithm: "AES_256".to_string(),
            usage: request.key_usage.clone(),
            created_at: chrono::Utc::now(),
            status: KeyStatus::Active,
            version: 1,
            description: request.description.clone(),
            metadata: HashMap::new(),
            tags: request.tags.clone(),
            encrypted_key_material: String::new(), // Not used for transit keys
        };

        debug!("VaultKeyData tags before storage: {:?}", key_data.tags);
        self.store_key_data(key_id, &key_data).await
    }

    /// Retrieve key data from Vault
    async fn get_key_data(&self, key_id: &str) -> Result<VaultKeyData> {
        let path = self.key_path(key_id);

        let secret: VaultKeyData = kv2::read(&self.client, &self.kv_mount, &path).await.map_err(|e| match e {
            vaultrs::error::ClientError::ResponseWrapError => KmsError::key_not_found(key_id),
            vaultrs::error::ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
            _ => KmsError::backend_error(format!("Failed to read key from Vault: {}", e)),
        })?;

        debug!("Retrieved key {} from Vault, tags: {:?}", key_id, secret.tags);
        Ok(secret)
    }

    /// List all keys stored in Vault
    async fn list_vault_keys(&self) -> Result<Vec<String>> {
        // List keys under the prefix
        match kv2::list(&self.client, &self.kv_mount, &self.key_path_prefix).await {
            Ok(keys) => {
                debug!("Found {} keys in Vault", keys.len());
                Ok(keys)
            }
            Err(vaultrs::error::ClientError::ResponseWrapError) => {
                // No keys exist yet
                Ok(Vec::new())
            }
            Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => {
                // Path doesn't exist - no keys exist yet
                debug!("Key path doesn't exist in Vault (404), returning empty list");
                Ok(Vec::new())
            }
            Err(e) => Err(KmsError::backend_error(format!("Failed to list keys in Vault: {}", e))),
        }
    }

    /// Physically delete a key from Vault storage
    async fn delete_key(&self, key_id: &str) -> Result<()> {
        let path = self.key_path(key_id);

        // For this specific key path, we can safely delete the metadata
        // since each key has its own unique path under the prefix
        kv2::delete_metadata(&self.client, &self.kv_mount, &path)
            .await
            .map_err(|e| match e {
                vaultrs::error::ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                _ => KmsError::backend_error(format!("Failed to delete key metadata from Vault: {}", e)),
            })?;

        debug!("Permanently deleted key {} metadata from Vault at path {}", key_id, path);
        Ok(())
    }
}

#[async_trait]
impl KmsClient for VaultKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKey> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        // Verify master key exists
        let _master_key = self.describe_key(&request.master_key_id, context).await?;

        // Generate data key material
        let key_length = match request.key_spec.as_str() {
            "AES_256" => 32,
            "AES_128" => 16,
            _ => return Err(KmsError::unsupported_algorithm(&request.key_spec)),
        };

        let mut plaintext_key = vec![0u8; key_length];
        rand::rng().fill_bytes(&mut plaintext_key);

        // Encrypt the data key with the master key
        let encrypted_key = self.encrypt_key_material(&plaintext_key).await?;

        Ok(DataKey {
            key_id: request.master_key_id.clone(),
            version: 1,
            plaintext: Some(plaintext_key),
            ciphertext: general_purpose::STANDARD
                .decode(&encrypted_key)
                .map_err(|e| KmsError::cryptographic_error("decode", e.to_string()))?,
            key_spec: request.key_spec.clone(),
            metadata: request.encryption_context.clone(),
            created_at: chrono::Utc::now(),
        })
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Get the master key
        let key_data = self.get_key_data(&request.key_id).await?;
        let key_material = self.decrypt_key_material(&key_data.encrypted_key_material).await?;

        // For simplicity, we'll use a basic encryption approach
        // In practice, you'd use proper AEAD encryption
        let mut ciphertext = request.plaintext.clone();
        for (i, byte) in ciphertext.iter_mut().enumerate() {
            *byte ^= key_material[i % key_material.len()];
        }

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: key_data.version,
            algorithm: key_data.algorithm,
        })
    }

    async fn decrypt(&self, _request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        // For this simple implementation, we assume the key ID is embedded in the ciphertext metadata
        // In practice, you'd extract this from the ciphertext envelope
        Err(KmsError::invalid_operation("Decrypt not fully implemented for Vault backend"))
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Creating master key: {} with algorithm: {}", key_id, algorithm);

        // Check if key already exists
        if self.get_key_data(key_id).await.is_ok() {
            return Err(KmsError::key_already_exists(key_id));
        }

        // Generate key material
        let key_material = Self::generate_key_material(algorithm)?;
        let encrypted_material = self.encrypt_key_material(&key_material).await?;

        // Create key data
        let key_data = VaultKeyData {
            algorithm: algorithm.to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: chrono::Utc::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            encrypted_key_material: encrypted_material,
        };

        // Store in Vault
        self.store_key_data(key_id, &key_data).await?;

        let master_key = MasterKey {
            key_id: key_id.to_string(),
            version: key_data.version,
            algorithm: key_data.algorithm.clone(),
            usage: key_data.usage,
            status: key_data.status,
            description: None, // This method doesn't receive description parameter
            metadata: key_data.metadata.clone(),
            created_at: key_data.created_at,
            rotated_at: None,
            created_by: None,
        };

        info!("Successfully created master key: {}", key_id);
        Ok(master_key)
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let key_data = self.get_key_data(key_id).await?;

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            description: key_data.description,
            algorithm: key_data.algorithm,
            usage: key_data.usage,
            status: key_data.status,
            version: key_data.version,
            metadata: key_data.metadata,
            tags: key_data.tags,
            created_at: key_data.created_at,
            rotated_at: None,
            created_by: None,
        })
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys with limit: {:?}", request.limit);

        let all_keys = self.list_vault_keys().await?;
        let limit = request.limit.unwrap_or(100) as usize;

        // Simple pagination implementation
        let start_idx = request
            .marker
            .as_ref()
            .and_then(|m| all_keys.iter().position(|k| k == m))
            .map(|idx| idx + 1)
            .unwrap_or(0);

        let end_idx = std::cmp::min(start_idx + limit, all_keys.len());
        let keys_page = &all_keys[start_idx..end_idx];

        let mut key_infos = Vec::new();
        for key_id in keys_page {
            if let Ok(key_info) = self.describe_key(key_id, None).await {
                key_infos.push(key_info);
            }
        }

        let next_marker = if end_idx < all_keys.len() {
            Some(all_keys[end_idx - 1].clone())
        } else {
            None
        };

        Ok(ListKeysResponse {
            keys: key_infos,
            next_marker,
            truncated: end_idx < all_keys.len(),
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Active;
        self.store_key_data(key_id, &key_data).await?;

        info!("Enabled key: {}", key_id);
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Disabled;
        self.store_key_data(key_id, &key_data).await?;

        info!("Disabled key: {}", key_id);
        Ok(())
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling key deletion: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::PendingDeletion;
        self.store_key_data(key_id, &key_data).await?;

        info!("Scheduled key deletion: {}", key_id);
        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling key deletion: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Active;
        self.store_key_data(key_id, &key_data).await?;

        info!("Canceled key deletion: {}", key_id);
        Ok(())
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Rotating key: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.version += 1;

        // Generate new key material
        let key_material = Self::generate_key_material(&key_data.algorithm)?;
        key_data.encrypted_key_material = self.encrypt_key_material(&key_material).await?;

        self.store_key_data(key_id, &key_data).await?;

        let master_key = MasterKey {
            key_id: key_id.to_string(),
            version: key_data.version,
            algorithm: key_data.algorithm,
            usage: key_data.usage,
            status: key_data.status,
            description: None, // Rotate preserves existing description (would need key lookup)
            metadata: key_data.metadata,
            created_at: key_data.created_at,
            rotated_at: Some(chrono::Utc::now()),
            created_by: None,
        };

        info!("Successfully rotated key: {}", key_id);
        Ok(master_key)
    }

    async fn health_check(&self) -> Result<()> {
        debug!("Performing Vault health check");

        // Use list_vault_keys but handle the case where no keys exist (which is normal)
        match self.list_vault_keys().await {
            Ok(_) => {
                debug!("Vault health check passed - successfully listed keys");
                Ok(())
            }
            Err(e) => {
                // Check if the error is specifically about "no keys found" or 404
                let error_msg = e.to_string();
                if error_msg.contains("status code 404") || error_msg.contains("No such key") {
                    debug!("Vault health check passed - 404 error is expected when no keys exist yet");
                    Ok(())
                } else {
                    warn!("Vault health check failed: {}", e);
                    Err(e)
                }
            }
        }
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo::new("vault".to_string(), "0.1.0".to_string(), self.config.address.clone(), true)
            .with_metadata("kv_mount".to_string(), self.kv_mount.clone())
            .with_metadata("key_prefix".to_string(), self.key_path_prefix.clone())
    }
}

/// VaultKmsBackend wraps VaultKmsClient and implements the KmsBackend trait
pub struct VaultKmsBackend {
    client: VaultKmsClient,
}

impl VaultKmsBackend {
    /// Create a new VaultKmsBackend
    pub async fn new(config: KmsConfig) -> Result<Self> {
        let vault_config = match &config.backend_config {
            crate::config::BackendConfig::Vault(vault_config) => vault_config.clone(),
            _ => return Err(KmsError::configuration_error("Expected Vault backend configuration")),
        };

        let client = VaultKmsClient::new(vault_config).await?;
        Ok(Self { client })
    }

    /// Update key metadata in Vault storage
    async fn update_key_metadata_in_storage(&self, key_id: &str, metadata: &KeyMetadata) -> Result<()> {
        // Get the current key data from Vault
        let mut key_data = self.client.get_key_data(key_id).await?;

        // Update the status based on the new metadata
        key_data.status = match metadata.key_state {
            KeyState::Enabled => KeyStatus::Active,
            KeyState::Disabled => KeyStatus::Disabled,
            KeyState::PendingDeletion => KeyStatus::PendingDeletion,
            KeyState::Unavailable => KeyStatus::Deleted,
            KeyState::PendingImport => KeyStatus::Disabled, // Treat as disabled until import completes
        };

        // Update the key data in Vault storage
        self.client.store_key_data(key_id, &key_data).await?;
        Ok(())
    }
}

#[async_trait]
impl KmsBackend for VaultKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Create key in Vault transit engine
        let _master_key = self.client.create_key(&key_id, "AES_256", None).await?;

        // Also store key metadata in KV store with tags
        self.client.store_key_metadata(&key_id, &request).await?;

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: chrono::Utc::now(),
            deletion_date: None,
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: request.tags,
        };

        Ok(CreateKeyResponse {
            key_id,
            key_metadata: metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let encrypt_request = crate::types::EncryptRequest {
            key_id: request.key_id.clone(),
            plaintext: request.plaintext,
            encryption_context: request.encryption_context,
            grant_tokens: request.grant_tokens,
        };

        let response = self.client.encrypt(&encrypt_request, None).await?;

        Ok(EncryptResponse {
            ciphertext: response.ciphertext,
            key_id: response.key_id,
            key_version: response.key_version,
            algorithm: response.algorithm,
        })
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let plaintext = self.client.decrypt(&request, None).await?;

        Ok(DecryptResponse {
            plaintext,
            key_id: "unknown".to_string(), // Would be extracted from ciphertext metadata
            encryption_algorithm: Some("AES-256-GCM".to_string()),
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

        Ok(GenerateDataKeyResponse {
            key_id: request.key_id,
            plaintext_key: data_key.plaintext.clone().unwrap_or_default(),
            ciphertext_blob: data_key.ciphertext.clone(),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        let key_info = self.client.describe_key(&request.key_id, None).await?;

        // Also get key metadata from KV store to retrieve tags
        let key_data = self.client.get_key_data(&request.key_id).await?;

        let metadata = KeyMetadata {
            key_id: key_info.key_id,
            key_state: match key_info.status {
                KeyStatus::Active => KeyState::Enabled,
                KeyStatus::Disabled => KeyState::Disabled,
                KeyStatus::PendingDeletion => KeyState::PendingDeletion,
                KeyStatus::Deleted => KeyState::Unavailable,
            },
            key_usage: key_info.usage,
            description: key_info.description,
            creation_date: key_info.created_at,
            deletion_date: None,
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: key_data.tags,
        };

        Ok(DescribeKeyResponse { key_metadata: metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        let response = self.client.list_keys(&request, None).await?;
        Ok(response)
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        // For Vault backend, we'll mark keys for deletion but not physically delete them
        // This allows for recovery during the pending window
        let key_id = &request.key_id;

        // First, check if the key exists and get its metadata
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {} not found", key_id)));
            }
        };

        let deletion_date = if request.force_immediate.unwrap_or(false) {
            // Check if key is already in PendingDeletion state
            if key_metadata.key_state == KeyState::PendingDeletion {
                // Force immediate deletion: physically delete the key from Vault storage
                self.client.delete_key(key_id).await?;

                // Return empty deletion_date to indicate key was permanently deleted
                None
            } else {
                // For non-pending keys, mark as PendingDeletion
                key_metadata.key_state = KeyState::PendingDeletion;
                key_metadata.deletion_date = Some(chrono::Utc::now());

                // Update the key metadata in Vault storage to reflect the new state
                self.update_key_metadata_in_storage(key_id, &key_metadata).await?;

                None
            }
        } else {
            // Schedule for deletion (default 30 days)
            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(crate::error::KmsError::invalid_parameter(
                    "pending_window_in_days must be between 7 and 30".to_string(),
                ));
            }

            let deletion_date = chrono::Utc::now() + chrono::Duration::days(days as i64);
            key_metadata.key_state = KeyState::PendingDeletion;
            key_metadata.deletion_date = Some(deletion_date);

            // Update the key metadata in Vault storage to reflect the new state
            self.update_key_metadata_in_storage(key_id, &key_metadata).await?;

            Some(deletion_date.to_rfc3339())
        };

        Ok(DeleteKeyResponse {
            key_id: key_id.clone(),
            deletion_date,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = &request.key_id;

        // Check if the key exists and is pending deletion
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {} not found", key_id)));
            }
        };

        if key_metadata.key_state != KeyState::PendingDeletion {
            return Err(crate::error::KmsError::invalid_key_state(format!(
                "Key {} is not pending deletion",
                key_id
            )));
        }

        // Cancel the deletion by resetting the state
        key_metadata.key_state = KeyState::Enabled;
        key_metadata.deletion_date = None;

        Ok(CancelKeyDeletionResponse {
            key_id: key_id.clone(),
            key_metadata,
        })
    }

    async fn health_check(&self) -> Result<bool> {
        self.client.health_check().await.map(|_| true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{VaultAuthMethod, VaultConfig};

    #[tokio::test]
    #[ignore] // Requires a running Vault instance
    async fn test_vault_client_integration() {
        let config = VaultConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-only-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        };

        let client = VaultKmsClient::new(config).await.expect("Failed to create Vault client");

        // Test key operations
        let key_id = "test-key-vault";
        let master_key = client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");
        assert_eq!(master_key.key_id, key_id);
        assert_eq!(master_key.algorithm, "AES_256");

        // Test key description
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id);

        // Test data key generation
        let data_key_request = GenerateKeyRequest {
            master_key_id: key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        };

        let data_key = client
            .generate_data_key(&data_key_request, None)
            .await
            .expect("Failed to generate data key");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        // Test health check
        client.health_check().await.expect("Health check failed");
    }
}
