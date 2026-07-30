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

use crate::error::{KmsError, Result};
use crate::types::*;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[cfg(test)]
mod contract_tests;
pub mod local;
pub mod static_kms;
pub mod vault;
pub(crate) mod vault_credentials;
pub mod vault_transit;

/// Operations whose availability depends on the key's lifecycle state.
///
/// Decryption is deliberately absent: RustFS allows decryption with
/// `Disabled` and `PendingDeletion` keys — an explicit deviation from AWS
/// KMS — because rejecting it would break reads of every object encrypted
/// under a key the moment it is disabled. Deletion cancellation is also
/// absent: it is valid exactly when the key is `PendingDeletion`, which call
/// sites enforce directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateGatedOperation {
    Encrypt,
    GenerateDataKey,
    Rotate,
    Enable,
    Disable,
    ScheduleDeletion,
}

impl StateGatedOperation {
    fn describe(self) -> &'static str {
        match self {
            Self::Encrypt => "encryption",
            Self::GenerateDataKey => "data key generation",
            Self::Rotate => "rotation",
            Self::Enable => "enabling",
            Self::Disable => "disabling",
            Self::ScheduleDeletion => "deletion scheduling",
        }
    }
}

/// Enforce the shared key state × operation matrix.
///
/// - `Enabled`: every operation is allowed.
/// - `Disabled`: enabling, disabling (idempotent) and deletion scheduling are
///   allowed; encryption, data key generation and rotation are rejected.
/// - `PendingDeletion`: every state-gated operation is rejected, including a
///   repeated deletion schedule; only cancellation and decryption proceed.
/// - `PendingImport`/`Unavailable`: the key is not usable and is reported as
///   not found.
pub(crate) fn ensure_key_state_permits(key_id: &str, state: &KeyState, operation: StateGatedOperation) -> Result<()> {
    match state {
        KeyState::Enabled => Ok(()),
        KeyState::Disabled => match operation {
            StateGatedOperation::Enable | StateGatedOperation::Disable | StateGatedOperation::ScheduleDeletion => Ok(()),
            StateGatedOperation::Encrypt | StateGatedOperation::GenerateDataKey | StateGatedOperation::Rotate => Err(
                KmsError::invalid_key_state(format!("Key {key_id} is disabled: {} is not allowed", operation.describe())),
            ),
        },
        KeyState::PendingDeletion => Err(KmsError::invalid_key_state(format!(
            "Key {key_id} is pending deletion: {} is not allowed",
            operation.describe()
        ))),
        KeyState::PendingImport | KeyState::Unavailable => Err(KmsError::key_not_found(key_id)),
    }
}

/// [`ensure_key_state_permits`] for backends that persist [`KeyStatus`].
pub(crate) fn ensure_key_status_permits(key_id: &str, status: &KeyStatus, operation: StateGatedOperation) -> Result<()> {
    let state = match status {
        KeyStatus::Active => KeyState::Enabled,
        KeyStatus::Disabled => KeyState::Disabled,
        KeyStatus::PendingDeletion => KeyState::PendingDeletion,
        KeyStatus::Deleted => KeyState::Unavailable,
    };
    ensure_key_state_permits(key_id, &state, operation)
}

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
    async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKeyInfo>;

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
    async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKeyInfo>;

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
    async fn rotate_key(&self, key_id: &str, context: Option<&OperationContext>) -> Result<MasterKeyInfo>;

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

    /// Report which operations this backend actually supports.
    ///
    /// The default is conservative: only the operations every backend is
    /// required to implement by this trait are advertised. Optional lifecycle
    /// operations (rotation, enable/disable, deletion scheduling, ...) must be
    /// opted in by overriding this method.
    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities::minimal()
    }
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

/// Set of operations a KMS backend supports.
///
/// Reported by [`KmsBackend::capabilities`] so callers (manager, admin API)
/// can discover what the active backend can do without probing individual
/// operations. Marked `#[non_exhaustive]` so new capability flags can be
/// added without breaking downstream code; construct values through
/// [`BackendCapabilities::minimal`] and the `with_*` builders.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendCapabilities {
    /// Direct encryption of caller-provided plaintext with a master key
    pub encrypt: bool,
    /// Decryption of previously produced ciphertext
    pub decrypt: bool,
    /// Data encryption key (DEK) generation
    pub generate_data_key: bool,
    /// Key rotation that retains prior versions for decryption
    pub rotate: bool,
    /// Enabling and disabling keys
    pub enable_disable: bool,
    /// Scheduling key deletion with a pending window
    pub schedule_deletion: bool,
    /// Multiple key versions addressable after rotation
    pub versioning: bool,
    /// Irreversible physical deletion of key material
    pub physical_delete: bool,
}

impl BackendCapabilities {
    /// Conservative baseline: only the operations that every [`KmsBackend`]
    /// implementation is required to provide by the trait. All optional
    /// lifecycle capabilities default to unsupported.
    pub const fn minimal() -> Self {
        Self {
            encrypt: true,
            decrypt: true,
            generate_data_key: true,
            rotate: false,
            enable_disable: false,
            schedule_deletion: false,
            versioning: false,
            physical_delete: false,
        }
    }

    /// Set whether direct encryption is supported
    pub const fn with_encrypt(mut self, encrypt: bool) -> Self {
        self.encrypt = encrypt;
        self
    }

    /// Set whether decryption is supported
    pub const fn with_decrypt(mut self, decrypt: bool) -> Self {
        self.decrypt = decrypt;
        self
    }

    /// Set whether data key generation is supported
    pub const fn with_generate_data_key(mut self, generate_data_key: bool) -> Self {
        self.generate_data_key = generate_data_key;
        self
    }

    /// Set whether version-retaining key rotation is supported
    pub const fn with_rotate(mut self, rotate: bool) -> Self {
        self.rotate = rotate;
        self
    }

    /// Set whether enabling/disabling keys is supported
    pub const fn with_enable_disable(mut self, enable_disable: bool) -> Self {
        self.enable_disable = enable_disable;
        self
    }

    /// Set whether scheduled deletion with a pending window is supported
    pub const fn with_schedule_deletion(mut self, schedule_deletion: bool) -> Self {
        self.schedule_deletion = schedule_deletion;
        self
    }

    /// Set whether multiple key versions are supported
    pub const fn with_versioning(mut self, versioning: bool) -> Self {
        self.versioning = versioning;
        self
    }

    /// Set whether physical deletion of key material is supported
    pub const fn with_physical_delete(mut self, physical_delete: bool) -> Self {
        self.physical_delete = physical_delete;
        self
    }
}

impl Default for BackendCapabilities {
    fn default() -> Self {
        Self::minimal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsConfig;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;

    /// Backend that implements only the trait-mandated operations and relies
    /// on the default `capabilities` implementation.
    struct MinimalBackend;

    #[async_trait]
    impl KmsBackend for MinimalBackend {
        async fn create_key(&self, _request: CreateKeyRequest) -> Result<CreateKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn encrypt(&self, _request: EncryptRequest) -> Result<EncryptResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn decrypt(&self, _request: DecryptRequest) -> Result<DecryptResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn generate_data_key(&self, _request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn describe_key(&self, _request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn list_keys(&self, _request: ListKeysRequest) -> Result<ListKeysResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn delete_key(&self, _request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn cancel_key_deletion(&self, _request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
            unimplemented!("not exercised by capability tests")
        }

        async fn health_check(&self) -> Result<bool> {
            Ok(true)
        }
    }

    fn capabilities_snapshot(capabilities: BackendCapabilities) -> std::collections::BTreeMap<String, bool> {
        serde_json::from_value(serde_json::to_value(capabilities).expect("capabilities should serialize"))
            .expect("capabilities should deserialize into a flat bool map")
    }

    #[test]
    fn default_capabilities_are_conservative() {
        let capabilities = MinimalBackend.capabilities();
        assert_eq!(capabilities, BackendCapabilities::minimal());
        assert_eq!(capabilities, BackendCapabilities::default());

        // The conservative baseline advertises only trait-mandated operations.
        assert!(capabilities.encrypt);
        assert!(capabilities.decrypt);
        assert!(capabilities.generate_data_key);
        assert!(!capabilities.rotate);
        assert!(!capabilities.enable_disable);
        assert!(!capabilities.schedule_deletion);
        assert!(!capabilities.versioning);
        assert!(!capabilities.physical_delete);
    }

    #[tokio::test]
    async fn local_backend_capabilities_golden() {
        let temp_dir = tempfile::tempdir().expect("temp dir should be created");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = local::LocalKmsBackend::new(config).await.expect("local backend should build");

        insta::assert_json_snapshot!("local_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn vault_kv2_backend_capabilities_golden() {
        let config = KmsConfig::vault(
            url::Url::parse("http://127.0.0.1:8200").expect("vault URL should parse"),
            "dev-token".to_string(),
        )
        .with_insecure_development_defaults();
        // Constructing the client performs no network I/O with token auth.
        let backend = vault::VaultKmsBackend::new(config)
            .await
            .expect("vault kv2 backend should build");

        insta::assert_json_snapshot!("vault_kv2_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn vault_transit_backend_capabilities_golden() {
        let config = KmsConfig::vault_transit(
            url::Url::parse("http://127.0.0.1:8200").expect("vault URL should parse"),
            "dev-token".to_string(),
        )
        .with_insecure_development_defaults();
        // Constructing the client performs no network I/O with token auth.
        let backend = vault_transit::VaultTransitKmsBackend::new(config)
            .await
            .expect("vault transit backend should build");

        insta::assert_json_snapshot!("vault_transit_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }

    #[tokio::test]
    async fn static_backend_capabilities_golden() {
        let config = KmsConfig::static_kms("static-key".to_string(), BASE64.encode([0u8; 32]));
        let backend = static_kms::StaticKmsBackend::new(config)
            .await
            .expect("static backend should build");

        insta::assert_json_snapshot!("static_backend_capabilities", capabilities_snapshot(backend.capabilities()));
    }
}
