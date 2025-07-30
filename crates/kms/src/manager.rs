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

//! KMS manager and abstract interface

use crate::{
    config::{KmsConfig, KmsType},
    error::{KmsError, Result},
    types::{
        DataKey, DecryptRequest, EncryptRequest, EncryptResponse, GenerateKeyRequest, KeyInfo, ListKeysRequest, ListKeysResponse,
        MasterKey, OperationContext,
    },
};
use async_trait::async_trait;
use std::{fmt::Debug, sync::Arc};
use tracing::{debug, error, info, warn};

/// Abstract KMS client interface
///
/// This trait defines the core operations that all KMS backends must implement.
/// It provides a unified interface for different key management services.
#[async_trait]
pub trait KmsClient: Send + Sync + Debug {
    /// Generate a new data encryption key (DEK)
    ///
    /// Creates a new data key using the specified master key. The returned DataKey
    /// contains both the plaintext and encrypted versions of the key.
    async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKey>;

    /// Encrypt data directly using a master key
    ///
    /// Encrypts the provided plaintext using the specified master key.
    /// This is different from generate_data_key as it encrypts user data directly.
    async fn encrypt(&self, request: &EncryptRequest, context: Option<&OperationContext>) -> Result<EncryptResponse>;

    /// Decrypt data using a master key
    ///
    /// Decrypts the provided ciphertext. The KMS automatically determines
    /// which key was used for encryption based on the ciphertext metadata.
    async fn decrypt(&self, request: &DecryptRequest, context: Option<&OperationContext>) -> Result<Vec<u8>>;

    /// Create a new master key
    ///
    /// Creates a new master key in the KMS with the specified ID.
    /// Returns an error if a key with the same ID already exists.
    async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKey>;

    /// Get information about a specific key
    ///
    /// Returns metadata and information about the specified key.
    async fn describe_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<KeyInfo>;

    /// List available keys
    ///
    /// Returns a paginated list of keys available in the KMS.
    async fn list_keys(&self, request: &ListKeysRequest, context: Option<&OperationContext>) -> Result<ListKeysResponse>;

    /// Enable a key
    ///
    /// Enables a previously disabled key, allowing it to be used for cryptographic operations.
    async fn enable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Disable a key
    ///
    /// Disables a key, preventing it from being used for new cryptographic operations.
    /// Existing encrypted data can still be decrypted.
    async fn disable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Schedule key deletion
    ///
    /// Schedules a key for deletion after a specified number of days.
    /// This allows for a grace period to recover the key if needed.
    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        context: Option<&OperationContext>,
    ) -> Result<()>;

    /// Cancel key deletion
    ///
    /// Cancels a previously scheduled key deletion.
    async fn cancel_key_deletion(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Rotate a key
    ///
    /// Creates a new version of the specified key. Previous versions remain
    /// available for decryption but new operations will use the new version.
    async fn rotate_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<MasterKey>;

    /// Generate a data key for object encryption
    ///
    /// Generates a new data encryption key specifically for object encryption.
    /// Returns both plaintext and encrypted versions of the key.
    async fn generate_object_data_key(
        &self,
        master_key_id: &str,
        key_spec: &str,
        context: Option<&OperationContext>,
    ) -> Result<DataKey>;

    /// Decrypt an object data key
    ///
    /// Decrypts an encrypted data key for object decryption.
    /// Returns the plaintext data key.
    async fn decrypt_object_data_key(&self, encrypted_key: &[u8], context: Option<&OperationContext>) -> Result<Vec<u8>>;

    /// Encrypt object metadata
    ///
    /// Encrypts object metadata using the specified master key.
    async fn encrypt_object_metadata(
        &self,
        master_key_id: &str,
        metadata: &[u8],
        context: Option<&OperationContext>,
    ) -> Result<Vec<u8>>;

    /// Decrypt object metadata
    ///
    /// Decrypts object metadata.
    async fn decrypt_object_metadata(&self, encrypted_metadata: &[u8], context: Option<&OperationContext>) -> Result<Vec<u8>>;

    /// Generate a data key with encryption context
    ///
    /// Generates a new data encryption key with additional encryption context.
    /// The context is used for additional authentication and access control.
    async fn generate_data_key_with_context(
        &self,
        master_key_id: &str,
        key_spec: &str,
        encryption_context: &std::collections::HashMap<String, String>,
        operation_context: Option<&OperationContext>,
    ) -> Result<DataKey>;

    /// Decrypt data with encryption context
    ///
    /// Decrypts data using the provided encryption context for validation.
    /// The context must match the one used during encryption.
    async fn decrypt_with_context(
        &self,
        ciphertext: &[u8],
        encryption_context: &std::collections::HashMap<String, String>,
        operation_context: Option<&OperationContext>,
    ) -> Result<Vec<u8>>;

    /// Health check
    ///
    /// Performs a health check on the KMS backend to ensure it's operational.
    async fn health_check(&self) -> Result<()>;

    /// Get backend information
    ///
    /// Returns information about the KMS backend (type, version, etc.).
    fn backend_info(&self) -> BackendInfo;
}

/// Information about a KMS backend
#[derive(Debug, Clone)]
pub struct BackendInfo {
    /// Backend type name
    pub backend_type: String,
    /// Backend version
    pub version: String,
    /// Backend endpoint or location
    pub endpoint: String,
    /// Whether the backend is healthy
    pub healthy: bool,
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

/// KMS Manager
///
/// The main entry point for KMS operations. It handles backend selection,
/// configuration, and provides a unified interface for all KMS operations.
pub struct KmsManager {
    client: Arc<dyn KmsClient>,
    config: KmsConfig,
}

impl KmsManager {
    /// Create a new KMS manager with the given configuration
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate().map_err(KmsError::configuration_error)?;

        let client = Self::create_client(&config).await?;

        // Perform initial health check
        if let Err(e) = client.health_check().await {
            warn!("KMS health check failed during initialization: {}", e);
        }

        info!("KMS manager initialized with backend: {:?}", config.kms_type);

        Ok(Self { client, config })
    }

    /// Create a KMS client based on the configuration
    async fn create_client(config: &KmsConfig) -> Result<Arc<dyn KmsClient>> {
        match config.kms_type {
            #[cfg(feature = "vault")]
            KmsType::Vault => {
                use crate::vault_client::VaultKmsClient;
                let vault_config = config
                    .vault_config()
                    .ok_or_else(|| KmsError::configuration_error("Missing Vault configuration"))?;
                let client = VaultKmsClient::new(vault_config.clone()).await?;
                Ok(Arc::new(client))
            }
            KmsType::Local => {
                use crate::local_client::LocalKmsClient;
                let local_config = config
                    .local_config()
                    .ok_or_else(|| KmsError::configuration_error("Missing Local configuration"))?;
                let client = LocalKmsClient::new(local_config.clone()).await?;
                Ok(Arc::new(client))
            }
            _ => Err(KmsError::configuration_error("KMS backend not yet implemented")),
        }
    }

    /// Generate a new data encryption key
    pub async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKey> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        let result = self.client.generate_data_key(request, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully generated data key for master key: {}", request.master_key_id);
            }
            Err(e) => {
                error!("Failed to generate data key for master key {}: {}", request.master_key_id, e);
            }
        }

        result
    }

    /// Encrypt data
    pub async fn encrypt(&self, request: &EncryptRequest, context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        let result = self.client.encrypt(request, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully encrypted data with key: {}", request.key_id);
            }
            Err(e) => {
                error!("Failed to encrypt data with key {}: {}", request.key_id, e);
            }
        }

        result
    }

    /// Decrypt data
    pub async fn decrypt(&self, request: &DecryptRequest, context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        let result = self.client.decrypt(request, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully decrypted data");
            }
            Err(e) => {
                error!("Failed to decrypt data: {}", e);
            }
        }

        result
    }

    /// Create a new master key
    pub async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Creating new master key: {}", key_id);

        let result = self.client.create_key(key_id, algorithm, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully created master key: {}", key_id);
            }
            Err(e) => {
                error!("Failed to create master key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Get key information
    pub async fn describe_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);
        self.client.describe_key(key_id, context).await
    }

    /// List keys
    pub async fn list_keys(&self, request: &ListKeysRequest, context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys");
        self.client.list_keys(request, context).await
    }

    /// Enable a key
    pub async fn enable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let result = self.client.enable_key(key_id, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully enabled key: {}", key_id);
            }
            Err(e) => {
                error!("Failed to enable key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Disable a key
    pub async fn disable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let result = self.client.disable_key(key_id, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully disabled key: {}", key_id);
            }
            Err(e) => {
                error!("Failed to disable key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Schedule key deletion
    pub async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling deletion for key: {} in {} days", key_id, pending_window_days);

        let result = self.client.schedule_key_deletion(key_id, pending_window_days, context).await;

        match &result {
            Ok(_) => {
                warn!("Scheduled key deletion: {} in {} days", key_id, pending_window_days);
            }
            Err(e) => {
                error!("Failed to schedule deletion for key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Cancel key deletion
    pub async fn cancel_key_deletion(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling deletion for key: {}", key_id);

        let result = self.client.cancel_key_deletion(key_id, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully canceled deletion for key: {}", key_id);
            }
            Err(e) => {
                error!("Failed to cancel deletion for key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Rotate a key
    pub async fn rotate_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Rotating key: {}", key_id);

        let result = self.client.rotate_key(key_id, context).await;

        match &result {
            Ok(_) => {
                info!("Successfully rotated key: {}", key_id);
            }
            Err(e) => {
                error!("Failed to rotate key {}: {}", key_id, e);
            }
        }

        result
    }

    /// Perform health check
    pub async fn health_check(&self) -> Result<()> {
        self.client.health_check().await
    }

    /// Get backend information
    pub fn backend_info(&self) -> BackendInfo {
        self.client.backend_info()
    }

    /// Get the current configuration
    pub fn config(&self) -> &KmsConfig {
        &self.config
    }

    /// Get the default key ID from configuration
    pub fn default_key_id(&self) -> Option<&str> {
        self.config.default_key_id.as_deref()
    }
}

impl Debug for KmsManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsManager")
            .field("backend_type", &self.config.kms_type)
            .field("default_key_id", &self.config.default_key_id)
            .finish()
    }
}
