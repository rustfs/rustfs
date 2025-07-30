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

//! Object encryption service for RustFS
//!
//! This module provides high-level object encryption and decryption services
//! that integrate with the KMS for key management.

use crate::{
    cipher::{AesGcmCipher, ChaCha20Poly1305Cipher, ObjectCipher},
    error::{KmsError, Result},
    manager::KmsClient,
    types::{EncryptedObjectMetadata, ObjectEncryptionContext, OperationContext},
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use tracing::info;

/// Object encryption algorithms
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM encryption
    Aes256Gcm,
    /// ChaCha20-Poly1305 encryption
    ChaCha20Poly1305,
}

impl Default for EncryptionAlgorithm {
    fn default() -> Self {
        Self::Aes256Gcm
    }
}

impl std::fmt::Display for EncryptionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aes256Gcm => write!(f, "AES-256-GCM"),
            Self::ChaCha20Poly1305 => write!(f, "ChaCha20-Poly1305"),
        }
    }
}

/// Object encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectEncryptionConfig {
    /// Default master key ID
    pub default_master_key_id: String,
    /// Default encryption algorithm
    pub default_algorithm: EncryptionAlgorithm,
    /// Whether to encrypt object metadata
    pub encrypt_metadata: bool,
    /// Maximum object size for encryption (in bytes)
    pub max_object_size: Option<u64>,
    /// Chunk size for streaming encryption (in bytes)
    pub chunk_size: usize,
}

impl Default for ObjectEncryptionConfig {
    fn default() -> Self {
        Self {
            default_master_key_id: "default".to_string(),
            default_algorithm: EncryptionAlgorithm::default(),
            encrypt_metadata: true,
            max_object_size: None,
            chunk_size: 64 * 1024, // 64KB chunks
        }
    }
}

/// Encrypted object data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedObjectData {
    /// Encrypted data
    pub ciphertext: Vec<u8>,
    /// Encrypted data key
    pub encrypted_data_key: Vec<u8>,
    /// Encryption algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Initialization vector
    pub iv: Vec<u8>,
    /// Authentication tag
    pub tag: Vec<u8>,
    /// Master key ID used
    pub master_key_id: String,
    /// Encryption context
    pub encryption_context: HashMap<String, String>,
    /// Object metadata (if encrypted)
    pub encrypted_metadata: Option<EncryptedObjectMetadata>,
}

/// Object encryption service
#[derive(Debug)]
pub struct ObjectEncryptionService {
    kms_client: Arc<dyn KmsClient>,
    config: ObjectEncryptionConfig,
}

impl ObjectEncryptionService {
    /// Create a new object encryption service
    pub fn new(kms_client: Arc<dyn KmsClient>, config: ObjectEncryptionConfig) -> Self {
        Self { kms_client, config }
    }

    /// Encrypt object data
    pub async fn encrypt_object(
        &self,
        data: &[u8],
        object_context: ObjectEncryptionContext,
        master_key_id: Option<&str>,
        algorithm: Option<EncryptionAlgorithm>,
        operation_context: Option<&OperationContext>,
    ) -> Result<EncryptedObjectData> {
        let master_key_id = master_key_id.unwrap_or(&self.config.default_master_key_id);
        let algorithm = algorithm.unwrap_or_else(|| self.config.default_algorithm.clone());

        // Check object size limit
        if let Some(max_size) = self.config.max_object_size {
            if data.len() as u64 > max_size {
                return Err(KmsError::InternalError {
                    message: format!("Object size {} exceeds maximum allowed size {}", data.len(), max_size),
                });
            }
        }

        info!(
            "Encrypting object: bucket={}, key={}, size={}, algorithm={}",
            object_context.bucket,
            object_context.object_key,
            data.len(),
            algorithm
        );

        // Generate data key
        let data_key = self
            .kms_client
            .generate_object_data_key(master_key_id, &algorithm.to_string(), operation_context)
            .await?;

        let plaintext_key = data_key
            .plaintext
            .as_ref()
            .ok_or_else(|| KmsError::cryptographic_error("Data key plaintext not available"))?;

        // Encrypt the data
        let (ciphertext, iv, tag) = match algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = AesGcmCipher::new(plaintext_key)?;
                let aad = self.build_aad(&object_context);
                // Generate random IV
                let mut iv = vec![0u8; 12];
                rand::rng().fill_bytes(&mut iv);
                let (encrypted_data, tag) = cipher.encrypt(data, &iv, &aad)?;
                (encrypted_data, iv, tag)
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305Cipher::new(plaintext_key)?;
                let aad = self.build_aad(&object_context);
                // Generate random nonce
                let mut nonce = vec![0u8; 12];
                rand::rng().fill_bytes(&mut nonce);
                let (encrypted_data, tag) = cipher.encrypt(data, &nonce, &aad)?;
                (encrypted_data, nonce, tag)
            }
        };

        // Encrypt metadata if configured
        let encrypted_metadata = if self.config.encrypt_metadata {
            let metadata = self.serialize_object_metadata(&object_context)?;
            let encrypted_meta = self
                .kms_client
                .encrypt_object_metadata(master_key_id, &metadata, operation_context)
                .await?;

            Some(EncryptedObjectMetadata {
                ciphertext: encrypted_meta,
                key_id: master_key_id.to_string(),
                algorithm: algorithm.to_string(),
                iv: iv.clone(),
                tag: tag.clone(),
                encryption_context: object_context.encryption_context.clone(),
            })
        } else {
            None
        };

        let mut encryption_context = object_context.encryption_context.clone();
        encryption_context.insert("algorithm".to_string(), algorithm.to_string());
        encryption_context.insert("bucket".to_string(), object_context.bucket.clone());
        encryption_context.insert("object_key".to_string(), object_context.object_key.clone());

        Ok(EncryptedObjectData {
            ciphertext,
            encrypted_data_key: data_key.ciphertext,
            algorithm,
            iv,
            tag,
            master_key_id: master_key_id.to_string(),
            encryption_context,
            encrypted_metadata,
        })
    }

    /// Decrypt object data
    pub async fn decrypt_object(
        &self,
        encrypted_data: &EncryptedObjectData,
        operation_context: Option<&OperationContext>,
    ) -> Result<Vec<u8>> {
        info!(
            "Decrypting object: master_key_id={}, algorithm={}, size={}",
            encrypted_data.master_key_id,
            encrypted_data.algorithm,
            encrypted_data.ciphertext.len()
        );

        // Decrypt the data key
        let plaintext_key = self
            .kms_client
            .decrypt_object_data_key(&encrypted_data.encrypted_data_key, operation_context)
            .await?;

        // Decrypt the data
        let plaintext = match encrypted_data.algorithm {
            EncryptionAlgorithm::Aes256Gcm => {
                let cipher = AesGcmCipher::new(&plaintext_key)?;
                let aad = self.build_aad_from_context(&encrypted_data.encryption_context);
                cipher.decrypt(&encrypted_data.ciphertext, &encrypted_data.iv, &encrypted_data.tag, &aad)?
            }
            EncryptionAlgorithm::ChaCha20Poly1305 => {
                let cipher = ChaCha20Poly1305Cipher::new(&plaintext_key)?;
                let aad = self.build_aad_from_context(&encrypted_data.encryption_context);
                cipher.decrypt(&encrypted_data.ciphertext, &encrypted_data.iv, &encrypted_data.tag, &aad)?
            }
        };

        Ok(plaintext)
    }

    /// Build Additional Authenticated Data (AAD) from object context
    fn build_aad(&self, context: &ObjectEncryptionContext) -> Vec<u8> {
        let mut aad = Vec::new();
        aad.extend_from_slice(context.bucket.as_bytes());
        aad.push(0); // separator
        aad.extend_from_slice(context.object_key.as_bytes());
        aad.push(0); // separator

        if let Some(content_type) = &context.content_type {
            aad.extend_from_slice(content_type.as_bytes());
        }
        aad.push(0); // separator

        if let Some(size) = context.size {
            aad.extend_from_slice(&size.to_le_bytes());
        }

        aad
    }

    /// Build AAD from encryption context
    fn build_aad_from_context(&self, context: &HashMap<String, String>) -> Vec<u8> {
        let mut aad = Vec::new();

        if let Some(bucket) = context.get("bucket") {
            aad.extend_from_slice(bucket.as_bytes());
        }
        aad.push(0);

        if let Some(object_key) = context.get("object_key") {
            aad.extend_from_slice(object_key.as_bytes());
        }
        aad.push(0);

        if let Some(content_type) = context.get("content_type") {
            aad.extend_from_slice(content_type.as_bytes());
        }
        aad.push(0);

        if let Some(size_str) = context.get("size") {
            if let Ok(size) = size_str.parse::<u64>() {
                aad.extend_from_slice(&size.to_le_bytes());
            }
        }

        aad
    }

    /// Serialize object metadata
    fn serialize_object_metadata(&self, context: &ObjectEncryptionContext) -> Result<Vec<u8>> {
        serde_json::to_vec(context).map_err(|e| KmsError::InternalError {
            message: format!("Failed to serialize metadata: {}", e),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::local_client::LocalKmsClient;

    #[tokio::test]
    async fn test_object_encryption_service() {
        use crate::config::LocalConfig;
        let local_config = LocalConfig::default();
        let kms_client = Arc::new(LocalKmsClient::new(local_config).await.unwrap());

        // Create a default key for testing
        kms_client.create_key("default", "AES_256", None).await.unwrap();

        let config = ObjectEncryptionConfig::default();
        let service = ObjectEncryptionService::new(kms_client, config);

        let data = b"Hello, World!";
        let context = ObjectEncryptionContext::new("test-bucket".to_string(), "test-object".to_string());

        // Test encryption
        let encrypted = service
            .encrypt_object(data, context, None, None, None)
            .await
            .expect("Encryption should succeed");

        assert!(!encrypted.ciphertext.is_empty());
        assert!(!encrypted.encrypted_data_key.is_empty());
        assert!(!encrypted.iv.is_empty());
        assert!(!encrypted.tag.is_empty());

        // Test decryption
        let decrypted = service
            .decrypt_object(&encrypted, None)
            .await
            .expect("Decryption should succeed");

        assert_eq!(decrypted, data);
    }
}
