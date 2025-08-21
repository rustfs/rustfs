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

//! Local file-based KMS client for development and testing

use uuid::Uuid;

use crate::{
    config::LocalConfig,
    error::{KmsError, Result},
    manager::{BackendInfo, KmsClient},
    types::{
        DataKey, DecryptRequest, EncryptRequest, EncryptResponse, GenerateKeyRequest, KeyInfo, KeyStatus, KeyUsage,
        ListKeysRequest, ListKeysResponse, MasterKey, OperationContext,
    },
};
use async_trait::async_trait;
use rustfs_crypto::{decrypt_data, encrypt_data};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, path::PathBuf, time::SystemTime};
use tokio::fs;
use tracing::{debug, info, warn};

/// Local file-based KMS client
#[derive(Debug)]
pub struct LocalKmsClient {
    config: LocalConfig,
    master_key: Option<Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredKey {
    key_id: String,
    algorithm: String,
    usage: KeyUsage,
    status: KeyStatus,
    version: u32,
    created_at: SystemTime,
    rotated_at: Option<SystemTime>,
    encrypted_key_data: Vec<u8>,
}

impl LocalKmsClient {
    /// Create a new local KMS client
    pub async fn new(config: LocalConfig) -> Result<Self> {
        // Ensure key directory exists
        if !config.key_dir.exists() {
            fs::create_dir_all(&config.key_dir)
                .await
                .map_err(|e| KmsError::configuration_error(format!("Failed to create key directory: {e}")))?;
        }

        // Load or generate master key
        let master_key = config.master_key.as_ref().map(|key_str| key_str.as_bytes().to_vec());

        info!("Local KMS client initialized with key directory: {:?}", config.key_dir);

        Ok(Self { config, master_key })
    }

    /// Get the path for a key file
    fn key_file_path(&self, key_id: &str) -> PathBuf {
        self.config.key_dir.join(format!("{key_id}.key"))
    }

    /// Load a stored key from disk
    async fn load_key(&self, key_id: &str) -> Result<StoredKey> {
        let path = self.key_file_path(key_id);

        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                KmsError::key_not_found(key_id)
            } else {
                KmsError::internal_error(format!("Failed to read key file: {e}"))
            }
        })?;

        let stored_key: StoredKey = if self.config.encrypt_files && self.master_key.is_some() {
            let decrypted_data = decrypt_data(&data, self.master_key.as_ref().expect("Master key should be available"))
                .map_err(|e| KmsError::cryptographic_error(format!("Failed to decrypt key file: {e}")))?;

            serde_json::from_slice(&decrypted_data)
                .map_err(|e| KmsError::internal_error(format!("Failed to parse key file: {e}")))?
        } else {
            serde_json::from_slice(&data).map_err(|e| KmsError::internal_error(format!("Failed to parse key file: {e}")))?
        };

        Ok(stored_key)
    }

    /// Save a key to disk
    async fn save_key(&self, stored_key: &StoredKey) -> Result<()> {
        let path = self.key_file_path(&stored_key.key_id);

        let data =
            serde_json::to_vec(stored_key).map_err(|e| KmsError::internal_error(format!("Failed to serialize key: {e}")))?;

        let final_data = if self.config.encrypt_files && self.master_key.is_some() {
            encrypt_data(&data, self.master_key.as_ref().expect("Master key should be available"))
                .map_err(|e| KmsError::cryptographic_error(format!("Failed to encrypt key file: {e}")))?
        } else {
            data
        };

        fs::write(&path, final_data)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to write key file: {e}")))?;

        Ok(())
    }

    /// Generate a random key
    fn generate_random_key(&self, length: usize) -> Vec<u8> {
        // Use rand::random() to fill cryptographic randomness
        let mut key = vec![0u8; length];
        let mut i = 0;
        while i < length {
            let chunk: u64 = rand::random();
            let bytes = chunk.to_ne_bytes();
            let n = usize::min(8, length - i);
            key[i..i + n].copy_from_slice(&bytes[..n]);
            i += n;
        }
        key
    }

    /// Simple encryption for local storage (just XOR with master key for demo)
    fn encrypt_with_master_key(&self, data: &[u8]) -> Result<Vec<u8>> {
        if let Some(master_key) = &self.master_key {
            let mut encrypted = data.to_vec();
            for (i, byte) in encrypted.iter_mut().enumerate() {
                *byte ^= master_key[i % master_key.len()];
            }
            Ok(encrypted)
        } else {
            Ok(data.to_vec())
        }
    }

    /// Simple decryption for local storage
    fn decrypt_with_master_key(&self, data: &[u8]) -> Result<Vec<u8>> {
        // XOR is symmetric, so encryption = decryption
        self.encrypt_with_master_key(data)
    }
}

#[async_trait]
impl KmsClient for LocalKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKey> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        // Verify master key exists
        let _master_key = self.load_key(&request.master_key_id).await?;

        // Generate a new data encryption key
        let key_length = request.key_length.unwrap_or(32) as usize;
        let plaintext_key = self.generate_random_key(key_length);

        // Encrypt the key with the master key
        let encrypted_key = self.encrypt_with_master_key(&plaintext_key)?;

        let mut data_key = DataKey::new(request.master_key_id.clone(), 1, Some(plaintext_key), encrypted_key);

        // Add metadata
        for (key, value) in &request.encryption_context {
            data_key.metadata.insert(key.clone(), value.clone());
        }

        Ok(data_key)
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Load the key
        let stored_key = self.load_key(&request.key_id).await?;

        if stored_key.status != KeyStatus::Active {
            return Err(KmsError::invalid_input(format!("Key {} is not active", request.key_id)));
        }

        // For simplicity, use the master key for encryption
        let ciphertext = self.encrypt_with_master_key(&request.plaintext)?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: stored_key.version,
            algorithm: stored_key.algorithm,
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        // For this simple implementation, we just decrypt with master key
        let plaintext = self.decrypt_with_master_key(&request.ciphertext)?;

        Ok(plaintext)
    }

    async fn generate_object_data_key(
        &self,
        master_key_id: &str,
        _key_spec: &str,
        _context: Option<&OperationContext>,
    ) -> Result<DataKey> {
        debug!("Generating object data key for master key: {}", master_key_id);

        // Verify master key exists
        let _master_key = self.load_key(master_key_id).await?;

        // Generate a new data encryption key (32 bytes for AES-256)
        let plaintext_key = self.generate_random_key(32);

        // Encrypt the key with the master key
        let encrypted_key = self.encrypt_with_master_key(&plaintext_key)?;

        let data_key = DataKey::new(master_key_id.to_string(), 1, Some(plaintext_key), encrypted_key);

        Ok(data_key)
    }

    async fn decrypt_object_data_key(&self, encrypted_key: &[u8], _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting object data key");

        // Decrypt the key with master key
        let plaintext_key = self.decrypt_with_master_key(encrypted_key)?;

        Ok(plaintext_key)
    }

    async fn encrypt_object_metadata(
        &self,
        master_key_id: &str,
        metadata: &[u8],
        _context: Option<&OperationContext>,
    ) -> Result<Vec<u8>> {
        debug!("Encrypting object metadata with key: {}", master_key_id);

        // Verify master key exists
        let _master_key = self.load_key(master_key_id).await?;

        // Encrypt metadata with master key
        let encrypted_metadata = self.encrypt_with_master_key(metadata)?;

        Ok(encrypted_metadata)
    }

    async fn decrypt_object_metadata(&self, encrypted_metadata: &[u8], _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting object metadata");

        // Decrypt metadata with master key
        let plaintext_metadata = self.decrypt_with_master_key(encrypted_metadata)?;

        Ok(plaintext_metadata)
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Creating new key: {}", key_id);

        // Check if key already exists
        if self.key_file_path(key_id).exists() {
            return Err(KmsError::key_exists(key_id));
        }

        // Generate key material
        let key_data = self.generate_random_key(32); // 256-bit key

        let stored_key = StoredKey {
            key_id: key_id.to_string(),
            algorithm: algorithm.to_string(),
            usage: KeyUsage::Encrypt,
            status: KeyStatus::Active,
            version: 1,
            created_at: SystemTime::now(),
            rotated_at: None,
            encrypted_key_data: key_data,
        };

        self.save_key(&stored_key).await?;

        Ok(MasterKey {
            key_id: key_id.to_string(),
            version: 1,
            algorithm: algorithm.to_string(),
            usage: KeyUsage::Encrypt,
            status: KeyStatus::Active,
            metadata: HashMap::new(),
            created_at: stored_key.created_at,
            rotated_at: None,
        })
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let stored_key = self.load_key(key_id).await?;

        Ok(KeyInfo {
            key_id: stored_key.key_id,
            name: key_id.to_string(),
            description: Some("Local KMS key".to_string()),
            algorithm: stored_key.algorithm,
            usage: stored_key.usage,
            status: stored_key.status,
            version: stored_key.version,
            metadata: HashMap::new(),
            created_at: stored_key.created_at,
            rotated_at: stored_key.rotated_at,
            created_by: Some("local-kms".to_string()),
        })
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys");

        let mut entries = fs::read_dir(&self.config.key_dir)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to read key directory: {e}")))?;

        let mut keys = Vec::new();
        while let Some(entry) = entries
            .next_entry()
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to read directory entry: {e}")))?
        {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("key") {
                if let Some(file_stem) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(stored_key) = self.load_key(file_stem).await {
                        // Apply filters
                        if let Some(usage_filter) = &request.usage_filter {
                            if stored_key.usage != *usage_filter {
                                continue;
                            }
                        }

                        if let Some(status_filter) = &request.status_filter {
                            if stored_key.status != *status_filter {
                                continue;
                            }
                        }

                        keys.push(KeyInfo {
                            key_id: stored_key.key_id,
                            name: file_stem.to_string(),
                            description: Some("Local KMS key".to_string()),
                            algorithm: stored_key.algorithm,
                            usage: stored_key.usage,
                            status: stored_key.status,
                            version: stored_key.version,
                            metadata: HashMap::new(),
                            created_at: stored_key.created_at,
                            rotated_at: stored_key.rotated_at,
                            created_by: Some("local-kms".to_string()),
                        });
                    }
                }
            }
        }

        // Apply limit
        if let Some(limit) = request.limit {
            keys.truncate(limit as usize);
        }

        Ok(ListKeysResponse {
            keys,
            next_marker: None,
            truncated: false,
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let mut stored_key = self.load_key(key_id).await?;
        stored_key.status = KeyStatus::Active;
        self.save_key(&stored_key).await?;

        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let mut stored_key = self.load_key(key_id).await?;
        stored_key.status = KeyStatus::Disabled;
        self.save_key(&stored_key).await?;

        Ok(())
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling deletion for key: {}", key_id);

        let mut stored_key = self.load_key(key_id).await?;
        stored_key.status = KeyStatus::PendingDeletion;
        self.save_key(&stored_key).await?;

        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling deletion for key: {}", key_id);

        let mut stored_key = self.load_key(key_id).await?;
        if stored_key.status == KeyStatus::PendingDeletion {
            stored_key.status = KeyStatus::Active;
            self.save_key(&stored_key).await?;
        }

        Ok(())
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Rotating key: {}", key_id);

        let mut stored_key = self.load_key(key_id).await?;

        // Generate new key material
        let new_key_data = self.generate_random_key(32);

        stored_key.version += 1;
        stored_key.encrypted_key_data = new_key_data;
        stored_key.rotated_at = Some(SystemTime::now());

        self.save_key(&stored_key).await?;

        Ok(MasterKey {
            key_id: stored_key.key_id,
            version: stored_key.version,
            algorithm: stored_key.algorithm,
            usage: stored_key.usage,
            status: stored_key.status,
            metadata: HashMap::new(),
            created_at: stored_key.created_at,
            rotated_at: stored_key.rotated_at,
        })
    }

    async fn health_check(&self) -> Result<()> {
        debug!("Performing local KMS health check");

        // Check if key directory is accessible
        if !self.config.key_dir.exists() {
            return Err(KmsError::internal_error("Key directory does not exist"));
        }

        // Try to read the directory
        let _ = fs::read_dir(&self.config.key_dir)
            .await
            .map_err(|e| KmsError::internal_error(format!("Cannot access key directory: {e}")))?;

        Ok(())
    }

    async fn generate_data_key_with_context(
        &self,
        master_key_id: &str,
        key_spec: &str,
        context: &std::collections::HashMap<String, String>,
        _operation_context: Option<&OperationContext>,
    ) -> Result<DataKey> {
        debug!("Generating data key with context for master key: {}", master_key_id);

        // For local implementation, we'll use the existing generate_object_data_key
        // and add context validation
        if context.is_empty() {
            warn!("Empty encryption context provided");
        }

        let operation_context = OperationContext {
            operation_id: Uuid::new_v4(),
            principal: "system".to_string(),
            source_ip: None,
            user_agent: None,
            additional_context: context.clone(),
        };

        let data_key = self
            .generate_object_data_key(master_key_id, key_spec, Some(&operation_context))
            .await?;
        Ok(data_key)
    }

    async fn decrypt_with_context(
        &self,
        ciphertext: &[u8],
        context: &std::collections::HashMap<String, String>,
        _operation_context: Option<&OperationContext>,
    ) -> Result<Vec<u8>> {
        debug!("Decrypting data with context");

        // For local implementation, context is mainly for auditing
        if context.is_empty() {
            warn!("Empty encryption context provided for decryption");
        }

        // Use the existing decrypt_object_data_key method
        let operation_context = OperationContext {
            operation_id: Uuid::new_v4(),
            principal: "system".to_string(),
            source_ip: None,
            user_agent: None,
            additional_context: context.clone(),
        };

        self.decrypt_object_data_key(ciphertext, Some(&operation_context)).await
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo {
            backend_type: "Local File System".to_string(),
            version: "1.0.0".to_string(),
            endpoint: format!("file://{}", self.config.key_dir.display()),
            healthy: true,
            metadata: {
                let mut map = std::collections::HashMap::new();
                map.insert("key_dir".to_string(), self.config.key_dir.display().to_string());
                map.insert("encrypt_files".to_string(), self.config.encrypt_files.to_string());
                map.insert("has_master_key".to_string(), self.master_key.is_some().to_string());
                map
            },
        }
    }

    async fn rewrap_ciphertext(
        &self,
        ciphertext_with_header: &[u8],
        _context: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        // Local backend does not version keys in a way that requires rewrap; return input as-is.
        Ok(ciphertext_with_header.to_vec())
    }
}
