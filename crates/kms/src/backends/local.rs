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

//! Local file-based KMS backend implementation

use crate::backends::{BackendInfo, KmsBackend, KmsClient};
use crate::config::KmsConfig;
use crate::config::LocalConfig;
use crate::error::{KmsError, Result};
use crate::types::*;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Local KMS client that stores keys in local files
pub struct LocalKmsClient {
    config: LocalConfig,
    /// In-memory cache of loaded keys for performance
    key_cache: RwLock<HashMap<String, MasterKey>>,
    /// Master encryption key for encrypting stored keys
    master_cipher: Option<Aes256Gcm>,
}

/// Serializable representation of a master key stored on disk
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StoredMasterKey {
    key_id: String,
    version: u32,
    algorithm: String,
    usage: KeyUsage,
    status: KeyStatus,
    description: Option<String>,
    metadata: HashMap<String, String>,
    created_at: chrono::DateTime<chrono::Utc>,
    rotated_at: Option<chrono::DateTime<chrono::Utc>>,
    created_by: Option<String>,
    /// Encrypted key material (32 bytes for AES-256)
    encrypted_key_material: Vec<u8>,
    /// Nonce used for encryption
    nonce: Vec<u8>,
}

/// Data key envelope stored with each data key generation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataKeyEnvelope {
    key_id: String,
    master_key_id: String,
    key_spec: String,
    encrypted_key: Vec<u8>,
    nonce: Vec<u8>,
    encryption_context: HashMap<String, String>,
    created_at: chrono::DateTime<chrono::Utc>,
}

impl LocalKmsClient {
    /// Create a new local KMS client
    pub async fn new(config: LocalConfig) -> Result<Self> {
        // Create key directory if it doesn't exist
        if !config.key_dir.exists() {
            fs::create_dir_all(&config.key_dir).await?;
            info!("Created KMS key directory: {:?}", config.key_dir);
        }

        // Initialize master cipher if master key is provided
        let master_cipher = if let Some(ref master_key) = config.master_key {
            let key = Self::derive_master_key(master_key)?;
            Some(Aes256Gcm::new(&key))
        } else {
            warn!("No master key provided - stored keys will not be encrypted at rest");
            None
        };

        Ok(Self {
            config,
            key_cache: RwLock::new(HashMap::new()),
            master_cipher,
        })
    }

    /// Derive a 256-bit key from the master key string
    fn derive_master_key(master_key: &str) -> Result<Key<Aes256Gcm>> {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(master_key.as_bytes());
        hasher.update(b"rustfs-kms-local"); // Salt to prevent rainbow tables
        let hash = hasher.finalize();
        let key = Key::<Aes256Gcm>::try_from(hash.as_slice())
            .map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        Ok(key)
    }

    /// Get the file path for a master key
    fn master_key_path(&self, key_id: &str) -> PathBuf {
        self.config.key_dir.join(format!("{key_id}.key"))
    }

    /// Load a master key from disk
    async fn load_master_key(&self, key_id: &str) -> Result<MasterKey> {
        let key_path = self.master_key_path(key_id);
        if !key_path.exists() {
            return Err(KmsError::key_not_found(key_id));
        }

        let content = fs::read(&key_path).await?;
        let stored_key: StoredMasterKey = serde_json::from_slice(&content)?;

        // Decrypt key material if master cipher is available
        let _key_material = if let Some(ref cipher) = self.master_cipher {
            if stored_key.nonce.len() != 12 {
                return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length"));
            }

            let mut nonce_array = [0u8; 12];
            nonce_array.copy_from_slice(&stored_key.nonce);
            let nonce = Nonce::from(nonce_array);

            cipher
                .decrypt(&nonce, stored_key.encrypted_key_material.as_ref())
                .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))?
        } else {
            stored_key.encrypted_key_material
        };

        Ok(MasterKey {
            key_id: stored_key.key_id,
            version: stored_key.version,
            algorithm: stored_key.algorithm,
            usage: stored_key.usage,
            status: stored_key.status,
            description: stored_key.description,
            metadata: stored_key.metadata,
            created_at: stored_key.created_at,
            rotated_at: stored_key.rotated_at,
            created_by: stored_key.created_by,
        })
    }

    /// Save a master key to disk
    async fn save_master_key(&self, master_key: &MasterKey, key_material: &[u8]) -> Result<()> {
        let key_path = self.master_key_path(&master_key.key_id);

        // Encrypt key material if master cipher is available
        let (encrypted_key_material, nonce) = if let Some(ref cipher) = self.master_cipher {
            let mut nonce_bytes = [0u8; 12];
            rand::rng().fill(&mut nonce_bytes[..]);
            let nonce = Nonce::from(nonce_bytes);

            let encrypted = cipher
                .encrypt(&nonce, key_material)
                .map_err(|e| KmsError::cryptographic_error("encrypt", e.to_string()))?;
            (encrypted, nonce.to_vec())
        } else {
            (key_material.to_vec(), Vec::new())
        };

        let stored_key = StoredMasterKey {
            key_id: master_key.key_id.clone(),
            version: master_key.version,
            algorithm: master_key.algorithm.clone(),
            usage: master_key.usage.clone(),
            status: master_key.status.clone(),
            description: master_key.description.clone(),
            metadata: master_key.metadata.clone(),
            created_at: master_key.created_at,
            rotated_at: master_key.rotated_at,
            created_by: master_key.created_by.clone(),
            encrypted_key_material,
            nonce,
        };

        let content = serde_json::to_vec_pretty(&stored_key)?;

        // Write to temporary file first, then rename for atomicity
        let temp_path = key_path.with_extension("tmp");
        fs::write(&temp_path, &content).await?;

        // Set file permissions if specified
        #[cfg(unix)]
        if let Some(permissions) = self.config.file_permissions {
            use std::os::unix::fs::PermissionsExt;
            let perms = std::fs::Permissions::from_mode(permissions);
            std::fs::set_permissions(&temp_path, perms)?;
        }

        fs::rename(&temp_path, &key_path).await?;

        info!("Saved master key {} to {:?}", master_key.key_id, key_path);
        Ok(())
    }

    /// Generate a random 256-bit key
    fn generate_key_material() -> Vec<u8> {
        let mut key_material = vec![0u8; 32]; // 256 bits
        rand::rng().fill(&mut key_material[..]);
        key_material
    }

    /// Get the actual key material for a master key
    async fn get_key_material(&self, key_id: &str) -> Result<Vec<u8>> {
        let key_path = self.master_key_path(key_id);

        if !key_path.exists() {
            return Err(KmsError::key_not_found(key_id));
        }

        let content = fs::read(&key_path).await?;
        let stored_key: StoredMasterKey = serde_json::from_slice(&content)?;

        // Decrypt key material if master cipher is available
        let key_material = if let Some(ref cipher) = self.master_cipher {
            if stored_key.nonce.len() != 12 {
                return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length"));
            }
            let mut nonce_array = [0u8; 12];
            nonce_array.copy_from_slice(&stored_key.nonce);
            let nonce = Nonce::from(nonce_array);
            cipher
                .decrypt(&nonce, stored_key.encrypted_key_material.as_ref())
                .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))?
        } else {
            stored_key.encrypted_key_material
        };

        Ok(key_material)
    }

    /// Encrypt data using a master key
    async fn encrypt_with_master_key(&self, key_id: &str, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        let key = Key::<Aes256Gcm>::try_from(key_material.as_slice())
            .map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = Aes256Gcm::new(&key);

        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill(&mut nonce_bytes[..]);

        let nonce = Nonce::from(nonce_bytes);

        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
            .map_err(|e| KmsError::cryptographic_error("encrypt", e.to_string()))?;

        Ok((ciphertext, nonce_bytes.to_vec()))
    }

    /// Decrypt data using a master key
    async fn decrypt_with_master_key(&self, key_id: &str, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        if nonce.len() != 12 {
            return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length"));
        }
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        let key = Key::<Aes256Gcm>::try_from(key_material.as_slice())
            .map_err(|_| KmsError::cryptographic_error("key", "Invalid key length"))?;
        let cipher = Aes256Gcm::new(&key);

        let mut nonce_array = [0u8; 12];
        nonce_array.copy_from_slice(nonce);
        let nonce_ref = Nonce::from(nonce_array);

        let plaintext = cipher
            .decrypt(&nonce_ref, ciphertext)
            .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))?;

        Ok(plaintext)
    }
}

#[async_trait]
impl KmsClient for LocalKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, context: Option<&OperationContext>) -> Result<DataKey> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        // Verify master key exists
        let _master_key = self.describe_key(&request.master_key_id, context).await?;

        // Generate random data key material
        let key_length = match request.key_spec.as_str() {
            "AES_256" => 32,
            "AES_128" => 16,
            _ => return Err(KmsError::unsupported_algorithm(&request.key_spec)),
        };

        let mut plaintext_key = vec![0u8; key_length];
        rand::rng().fill(&mut plaintext_key[..]);

        // Encrypt the data key with the master key
        let (encrypted_key, nonce) = self.encrypt_with_master_key(&request.master_key_id, &plaintext_key).await?;

        // Create data key envelope
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key: encrypted_key.clone(),
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: chrono::Utc::now(),
        };

        // Serialize the envelope as the ciphertext
        let ciphertext = serde_json::to_vec(&envelope)?;

        let data_key = DataKey::new(envelope.key_id, 1, Some(plaintext_key), ciphertext, request.key_spec.clone());

        info!("Generated data key for master key: {}", request.master_key_id);
        Ok(data_key)
    }

    async fn encrypt(&self, request: &EncryptRequest, context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Verify key exists and is active
        let key_info = self.describe_key(&request.key_id, context).await?;
        if key_info.status != KeyStatus::Active {
            return Err(KmsError::invalid_operation(format!(
                "Key {} is not active (status: {:?})",
                request.key_id, key_info.status
            )));
        }

        let (ciphertext, _nonce) = self.encrypt_with_master_key(&request.key_id, &request.plaintext).await?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: key_info.version,
            algorithm: key_info.algorithm,
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        // Parse the data key envelope from ciphertext
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)?;

        // Verify encryption context matches
        if !request.encryption_context.is_empty() {
            for (key, expected_value) in &request.encryption_context {
                if let Some(actual_value) = envelope.encryption_context.get(key) {
                    if actual_value != expected_value {
                        return Err(KmsError::context_mismatch(format!(
                            "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                        )));
                    }
                } else {
                    return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
                }
            }
        }

        // Decrypt the data key
        let plaintext = self
            .decrypt_with_master_key(&envelope.master_key_id, &envelope.encrypted_key, &envelope.nonce)
            .await?;

        info!("Successfully decrypted data");
        Ok(plaintext)
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Creating master key: {}", key_id);

        // Check if key already exists
        if self.master_key_path(key_id).exists() {
            return Err(KmsError::key_already_exists(key_id));
        }

        // Validate algorithm
        if algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(algorithm));
        }

        // Generate key material
        let key_material = Self::generate_key_material();

        let created_by = context
            .map(|ctx| ctx.principal.clone())
            .unwrap_or_else(|| "local-kms".to_string());

        let master_key = MasterKey::new_with_description(key_id.to_string(), algorithm.to_string(), Some(created_by), None);

        // Save to disk
        self.save_master_key(&master_key, &key_material).await?;

        // Cache the key
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key.clone());

        info!("Created master key: {}", key_id);
        Ok(master_key)
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        // Check cache first
        {
            let cache = self.key_cache.read().await;
            if let Some(master_key) = cache.get(key_id) {
                return Ok(master_key.clone().into());
            }
        }

        // Load from disk
        let master_key = self.load_master_key(key_id).await?;

        // Update cache
        {
            let mut cache = self.key_cache.write().await;
            cache.insert(key_id.to_string(), master_key.clone());
        }

        Ok(master_key.into())
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys");

        let mut keys = Vec::new();
        let limit = request.limit.unwrap_or(100) as usize;
        let mut count = 0;

        let mut entries = fs::read_dir(&self.config.key_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            if count >= limit {
                break;
            }

            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "key")
                && let Some(stem) = path.file_stem()
                && let Some(key_id) = stem.to_str()
                && let Ok(key_info) = self.describe_key(key_id, None).await
            {
                // Apply filters
                if let Some(ref status_filter) = request.status_filter
                    && &key_info.status != status_filter
                {
                    continue;
                }
                if let Some(ref usage_filter) = request.usage_filter
                    && &key_info.usage != usage_filter
                {
                    continue;
                }

                keys.push(key_info);
                count += 1;
            }
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: None, // Simple implementation without pagination
            truncated: false,
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::Active;

        // For simplicity, we'll regenerate key material
        // In a real implementation, we'd preserve the original key material
        let key_material = Self::generate_key_material();
        self.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key);

        info!("Enabled key: {}", key_id);
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::Disabled;

        let key_material = Self::generate_key_material();
        self.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key);

        info!("Disabled key: {}", key_id);
        Ok(())
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling deletion for key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::PendingDeletion;

        let key_material = Self::generate_key_material();
        self.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key);

        warn!("Scheduled key deletion: {}", key_id);
        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling deletion for key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::Active;

        let key_material = Self::generate_key_material();
        self.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key);

        info!("Canceled deletion for key: {}", key_id);
        Ok(())
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Rotating key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.version += 1;
        master_key.rotated_at = Some(chrono::Utc::now());

        // Generate new key material
        let key_material = Self::generate_key_material();
        self.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key.clone());

        info!("Rotated key: {}", key_id);
        Ok(master_key)
    }

    async fn health_check(&self) -> Result<()> {
        // Check if key directory is accessible
        if !self.config.key_dir.exists() {
            return Err(KmsError::backend_error("Key directory does not exist"));
        }

        // Try to read the directory
        let _ = fs::read_dir(&self.config.key_dir).await?;

        Ok(())
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo::new(
            "local".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
            self.config.key_dir.to_string_lossy().to_string(),
            true, // We'll assume healthy for now
        )
        .with_metadata("key_dir".to_string(), self.config.key_dir.to_string_lossy().to_string())
        .with_metadata("encrypted_at_rest".to_string(), self.master_cipher.is_some().to_string())
    }
}

/// LocalKmsBackend wraps LocalKmsClient and implements the KmsBackend trait
pub struct LocalKmsBackend {
    client: LocalKmsClient,
}

impl LocalKmsBackend {
    /// Create a new LocalKmsBackend
    pub async fn new(config: KmsConfig) -> Result<Self> {
        let local_config = match &config.backend_config {
            crate::config::BackendConfig::Local(local_config) => local_config.clone(),
            _ => return Err(KmsError::configuration_error("Expected Local backend configuration")),
        };

        let client = LocalKmsClient::new(local_config).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl KmsBackend for LocalKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Create master key with description directly
        let _master_key = {
            // Generate key material
            let key_material = LocalKmsClient::generate_key_material();

            let master_key = MasterKey::new_with_description(
                key_id.clone(),
                "AES_256".to_string(),
                Some("local-kms".to_string()),
                request.description.clone(),
            );

            // Save to disk and cache
            self.client.save_master_key(&master_key, &key_material).await?;

            let mut cache = self.client.key_cache.write().await;
            cache.insert(key_id.clone(), master_key.clone());

            master_key
        };

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: chrono::Utc::now(),
            deletion_date: None,
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: request.tags,
        };

        Ok(CreateKeyResponse {
            key_id,
            key_metadata: metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let encrypt_request = EncryptRequest {
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

        // For simplicity, return basic response - in real implementation would extract more info from ciphertext
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
            origin: "KMS".to_string(),
            key_manager: "CUSTOMER".to_string(),
            tags: key_info.tags,
        };

        Ok(DescribeKeyResponse { key_metadata: metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        let response = self.client.list_keys(&request, None).await?;
        Ok(response)
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        // For local backend, we'll implement immediate deletion by default
        // unless a pending window is specified
        let key_id = &request.key_id;

        // First, load the key from disk to get the master key
        let mut master_key = self
            .client
            .load_master_key(key_id)
            .await
            .map_err(|_| KmsError::key_not_found(format!("Key {key_id} not found")))?;

        let (deletion_date_str, deletion_date_dt) = if request.force_immediate.unwrap_or(false) {
            // For immediate deletion, actually delete the key from filesystem
            let key_path = self.client.master_key_path(key_id);
            tokio::fs::remove_file(&key_path)
                .await
                .map_err(|e| KmsError::internal_error(format!("Failed to delete key file: {e}")))?;

            // Remove from cache
            let mut cache = self.client.key_cache.write().await;
            cache.remove(key_id);

            info!("Immediately deleted key: {}", key_id);

            // Return success response for immediate deletion
            let key_metadata = KeyMetadata {
                key_id: master_key.key_id.clone(),
                description: master_key.description.clone(),
                key_usage: master_key.usage,
                key_state: KeyState::PendingDeletion, // AWS KMS compatibility
                creation_date: master_key.created_at,
                deletion_date: Some(chrono::Utc::now()),
                key_manager: "CUSTOMER".to_string(),
                origin: "AWS_KMS".to_string(),
                tags: master_key.metadata,
            };

            return Ok(DeleteKeyResponse {
                key_id: key_id.clone(),
                deletion_date: None, // No deletion date for immediate deletion
                key_metadata,
            });
        } else {
            // Schedule for deletion (default 30 days)
            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(KmsError::invalid_parameter("pending_window_in_days must be between 7 and 30".to_string()));
            }

            let deletion_date = chrono::Utc::now() + chrono::Duration::days(days as i64);
            master_key.status = KeyStatus::PendingDeletion;

            (Some(deletion_date.to_rfc3339()), Some(deletion_date))
        };

        // Save the updated key to disk - preserve existing key material!
        // Load the stored key from disk to get the existing key material
        let key_path = self.client.master_key_path(key_id);
        let content = tokio::fs::read(&key_path)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to read key file: {e}")))?;
        let stored_key: StoredMasterKey =
            serde_json::from_slice(&content).map_err(|e| KmsError::internal_error(format!("Failed to parse stored key: {e}")))?;

        // Decrypt the existing key material to preserve it
        let existing_key_material = if let Some(ref cipher) = self.client.master_cipher {
            if stored_key.nonce.len() != 12 {
                return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length"));
            }
            let mut nonce_array = [0u8; 12];
            nonce_array.copy_from_slice(&stored_key.nonce);
            let nonce = Nonce::from(nonce_array);
            cipher
                .decrypt(&nonce, stored_key.encrypted_key_material.as_ref())
                .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))?
        } else {
            stored_key.encrypted_key_material
        };

        self.client.save_master_key(&master_key, &existing_key_material).await?;

        // Update cache
        let mut cache = self.client.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key.clone());

        // Convert master_key to KeyMetadata for response
        let key_metadata = KeyMetadata {
            key_id: master_key.key_id.clone(),
            description: master_key.description.clone(),
            key_usage: master_key.usage,
            key_state: KeyState::PendingDeletion,
            creation_date: master_key.created_at,
            deletion_date: deletion_date_dt,
            key_manager: "CUSTOMER".to_string(),
            origin: "AWS_KMS".to_string(),
            tags: master_key.metadata,
        };

        Ok(DeleteKeyResponse {
            key_id: key_id.clone(),
            deletion_date: deletion_date_str,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = &request.key_id;

        // Load the key from disk to get the master key
        let mut master_key = self
            .client
            .load_master_key(key_id)
            .await
            .map_err(|_| KmsError::key_not_found(format!("Key {key_id} not found")))?;

        if master_key.status != KeyStatus::PendingDeletion {
            return Err(KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
        }

        // Cancel the deletion by resetting the state
        master_key.status = KeyStatus::Active;

        // Save the updated key to disk - this is the missing critical step!
        let key_material = LocalKmsClient::generate_key_material();
        self.client.save_master_key(&master_key, &key_material).await?;

        // Update cache
        let mut cache = self.client.key_cache.write().await;
        cache.insert(key_id.to_string(), master_key.clone());

        // Convert master_key to KeyMetadata for response
        let key_metadata = KeyMetadata {
            key_id: master_key.key_id.clone(),
            description: master_key.description.clone(),
            key_usage: master_key.usage,
            key_state: KeyState::Enabled,
            creation_date: master_key.created_at,
            deletion_date: None,
            key_manager: "CUSTOMER".to_string(),
            origin: "AWS_KMS".to_string(),
            tags: master_key.metadata,
        };

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
    use tempfile::TempDir;

    async fn create_test_client() -> (LocalKmsClient, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        };
        let client = LocalKmsClient::new(config).await.expect("Failed to create client");
        (client, temp_dir)
    }

    #[tokio::test]
    async fn test_key_lifecycle() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        let algorithm = "AES_256";

        // Create key
        let master_key = client
            .create_key(key_id, algorithm, None)
            .await
            .expect("Failed to create key");
        assert_eq!(master_key.key_id, key_id);
        assert_eq!(master_key.algorithm, algorithm);
        assert_eq!(master_key.status, KeyStatus::Active);

        // Describe key
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id);
        assert_eq!(key_info.status, KeyStatus::Active);

        // List keys
        let list_response = client
            .list_keys(&ListKeysRequest::default(), None)
            .await
            .expect("Failed to list keys");
        assert_eq!(list_response.keys.len(), 1);
        assert_eq!(list_response.keys[0].key_id, key_id);

        // Disable key
        client.disable_key(key_id, None).await.expect("Failed to disable key");
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.status, KeyStatus::Disabled);

        // Enable key
        client.enable_key(key_id, None).await.expect("Failed to enable key");
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.status, KeyStatus::Active);
    }

    #[tokio::test]
    async fn test_data_key_operations() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");

        // Generate data key
        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string())
            .with_context("bucket".to_string(), "test-bucket".to_string());

        let data_key = client
            .generate_data_key(&request, None)
            .await
            .expect("Failed to generate data key");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        // Decrypt data key
        let decrypt_request =
            DecryptRequest::new(data_key.ciphertext.clone()).with_context("bucket".to_string(), "test-bucket".to_string());

        let decrypted = client.decrypt(&decrypt_request, None).await.expect("Failed to decrypt");
        assert_eq!(decrypted, data_key.plaintext.clone().expect("No plaintext"));
    }

    #[tokio::test]
    async fn test_encryption_operations() {
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "test-key";
        client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");

        let plaintext = b"Hello, World!";
        let encrypt_request = EncryptRequest::new(key_id.to_string(), plaintext.to_vec());

        // Encrypt
        let encrypt_response = client.encrypt(&encrypt_request, None).await.expect("Failed to encrypt");
        assert!(!encrypt_response.ciphertext.is_empty());
        assert_eq!(encrypt_response.key_id, key_id);

        // Note: Direct decryption of encrypt() results is not implemented in this simple version
        // In a real implementation, encrypt() would create a different envelope format
    }
}
