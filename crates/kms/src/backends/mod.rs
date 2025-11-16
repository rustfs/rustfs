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

//! KMS backend implementations

use crate::error::Result;
use crate::types::*;
use async_trait::async_trait;
use std::collections::HashMap;

pub mod local;
pub mod vault;

/// Abstract KMS client interface that all backends must implement
#[async_trait]
pub trait KmsClient: Send + Sync {
    /// Generate a new data encryption key (DEK)
    ///
    /// Creates a new data key using the specified master key. The returned DataKey
    /// contains both the plaintext and encrypted versions of the key.
    ///
    /// # Arguments
    /// * `request` - The key generation request
    /// * `context` - Optional operation context for auditing
    ///
    /// # Returns
    /// Returns a DataKey containing both plaintext and encrypted key material
    async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKey>;

    /// Encrypt data directly using a master key
    ///
    /// Encrypts the provided plaintext using the specified master key.
    /// This is different from generate_data_key as it encrypts user data directly.
    ///
    /// # Arguments
    /// * `request` - The encryption request containing plaintext and key ID
    /// * `context` - Optional operation context for auditing
    async fn encrypt(&self, request: &EncryptRequest, context: Option<&OperationContext>) -> Result<EncryptResponse>;

    /// Decrypt data using a master key
    ///
    /// Decrypts the provided ciphertext. The KMS automatically determines
    /// which key was used for encryption based on the ciphertext metadata.
    ///
    /// # Arguments
    /// * `request` - The decryption request containing ciphertext
    /// * `context` - Optional operation context for auditing
    async fn decrypt(&self, request: &DecryptRequest, context: Option<&OperationContext>) -> Result<Vec<u8>>;

    /// Create a new master key
    ///
    /// Creates a new master key in the KMS with the specified ID.
    /// Returns an error if a key with the same ID already exists.
    ///
    /// # Arguments
    /// * `key_id` - Unique identifier for the new key
    /// * `algorithm` - Key algorithm (e.g., "AES_256")
    /// * `context` - Optional operation context for auditing
    async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKey>;

    /// Get information about a specific key
    ///
    /// Returns metadata and information about the specified key.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `context` - Optional operation context for auditing
    async fn describe_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<KeyInfo>;

    /// List available keys
    ///
    /// Returns a paginated list of keys available in the KMS.
    ///
    /// # Arguments
    /// * `request` - List request parameters (pagination, filters)
    /// * `context` - Optional operation context for auditing
    async fn list_keys(&self, request: &ListKeysRequest, context: Option<&OperationContext>) -> Result<ListKeysResponse>;

    /// Enable a key
    ///
    /// Enables a previously disabled key, allowing it to be used for cryptographic operations.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `context` - Optional operation context for auditing
    async fn enable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Disable a key
    ///
    /// Disables a key, preventing it from being used for new cryptographic operations.
    /// Existing encrypted data can still be decrypted.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `context` - Optional operation context for auditing
    async fn disable_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Schedule key deletion
    ///
    /// Schedules a key for deletion after a specified number of days.
    /// This allows for a grace period to recover the key if needed.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `pending_window_days` - Number of days before actual deletion
    /// * `context` - Optional operation context for auditing
    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        pending_window_days: u32,
        context: Option<&OperationContext>,
    ) -> Result<()>;

    /// Cancel key deletion
    ///
    /// Cancels a previously scheduled key deletion.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `context` - Optional operation context for auditing
    async fn cancel_key_deletion(&self, key_id: &str, context: Option<&OperationContext>) -> Result<()>;

    /// Rotate a key
    ///
    /// Creates a new version of the specified key. Previous versions remain
    /// available for decryption but new operations will use the new version.
    ///
    /// # Arguments
    /// * `key_id` - The key identifier
    /// * `context` - Optional operation context for auditing
    async fn rotate_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<MasterKey>;

    /// Health check
    ///
    /// Performs a health check on the KMS backend to ensure it's operational.
    async fn health_check(&self) -> Result<()>;

    /// Get backend information
    ///
    /// Returns information about the KMS backend (type, version, etc.).
    fn backend_info(&self) -> BackendInfo;
}

/// Simplified KMS backend interface for manager
#[async_trait]
pub trait KmsBackend: Send + Sync {
    /// Create a new master key
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse>;

    /// Encrypt data
    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse>;

    /// Decrypt data
    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse>;

    /// Generate a data key
    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse>;

    /// Describe a key
    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse>;

    /// List keys
    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse>;

    /// Delete a key
    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse>;

    /// Cancel key deletion
    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse>;

    /// Health check
    async fn health_check(&self) -> Result<bool>;
}

/// Information about a KMS backend
#[derive(Debug, Clone)]
pub struct BackendInfo {
    /// Backend type name (e.g., "local", "vault")
    pub backend_type: String,
    /// Backend version
    pub version: String,
    /// Backend endpoint or location
    pub endpoint: String,
    /// Whether the backend is currently healthy
    pub healthy: bool,
    /// Additional metadata about the backend
    pub metadata: HashMap<String, String>,
}

impl BackendInfo {
    /// Create a new backend info
    ///
    /// # Arguments
    /// * `backend_type` - The type of the backend
    /// * `version` - The version of the backend
    /// * `endpoint` - The endpoint or location of the backend
    /// * `healthy` - Whether the backend is healthy
    ///
    /// # Returns
    /// A new BackendInfo instance
    ///
    pub fn new(backend_type: String, version: String, endpoint: String, healthy: bool) -> Self {
        Self {
            backend_type,
            version,
            endpoint,
            healthy,
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the backend info
    ///
    /// # Arguments
    /// * `key` - Metadata key
    /// * `value` - Metadata value
    ///
    /// # Returns
    /// Updated BackendInfo instance
    ///
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }
}
