use crate::{
    cipher::{AesGcmCipher, ChaCha20Poly1305Cipher, ObjectCipher},
    error::EncryptionResult,
    manager::KmsManager,
    types::EncryptionMetadata,
};
use tracing::info;
use base64::Engine;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Service for encrypting and decrypting S3 objects
pub struct ObjectEncryptionService {
    kms_manager: KmsManager,
}

impl ObjectEncryptionService {
    pub fn new(kms_manager: KmsManager) -> Self {
        Self { kms_manager }
    }

    /// Get a reference to the KMS manager
    pub fn kms_manager(&self) -> &KmsManager {
        &self.kms_manager
    }

    /// Encrypt object data and return encrypted stream with metadata
    pub async fn encrypt_object<R>(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        algorithm: &str,
        kms_key_id: Option<&str>,
        encryption_context: Option<String>,
    ) -> EncryptionResult<(Box<dyn AsyncRead + Send + Sync + Unpin>, EncryptionMetadata)> {
        // Read all data from reader
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        // Determine the actual key ID to use
        let actual_key_id = kms_key_id.unwrap_or_else(|| {
            self.kms_manager.default_key_id().unwrap_or("rustfs-default-key")
        });

        // Only auto-create keys for SSE-S3 and SSE-KMS
        let is_sse_s3 = algorithm == "AES256" && kms_key_id.is_none();
        let is_sse_kms = algorithm == "aws:kms" || kms_key_id.is_some();
        
        if is_sse_s3 || is_sse_kms {
            let key_exists = self.kms_manager.describe_key(actual_key_id, None).await.is_ok();
            if !key_exists {
                info!("Key {} not found, attempting to create key for {} encryption", 
                      actual_key_id, if is_sse_s3 { "SSE-S3" } else { "SSE-KMS" });
                // Attempt to create key, but don't fail if creation fails (for testing environments)
                let _ = self.kms_manager.create_key(actual_key_id, "AES_256", None).await
                    .map_err(|e| {
                        info!("Failed to create key {}: {:?}, continuing with existing key", actual_key_id, e);
                        e
                    });
            }
        }

        // Generate data encryption key
        let _context = encryption_context.unwrap_or_else(|| format!("bucket={bucket}&key={key}"));

        let request = crate::types::GenerateKeyRequest::new(actual_key_id.to_string(), "AES_256".to_string())
            .with_length(32);

        let data_key_result = self.kms_manager.generate_data_key(&request, None).await?;

        let data_key = data_key_result
            .plaintext
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("No plaintext key in data key result"))?;
        let encrypted_data_key = data_key_result.ciphertext;

        // Create cipher based on algorithm
        // Note: aws:kms uses AES256 for actual encryption, the difference is in key management
        let cipher: Box<dyn ObjectCipher> = match algorithm {
            "AES256" | "aws:kms" => Box::new(AesGcmCipher::new(&data_key)?),
            "ChaCha20Poly1305" => Box::new(ChaCha20Poly1305Cipher::new(&data_key)?),
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };

        // Generate IV and encrypt the data
        let iv = match algorithm {
            "AES256" | "aws:kms" => {
                let mut iv = vec![0u8; 12]; // AES-GCM uses 12-byte IV
                use rand::RngCore;
                rand::rng().fill_bytes(&mut iv);
                iv
            }
            "ChaCha20Poly1305" => {
                let mut iv = vec![0u8; 12]; // ChaCha20Poly1305 uses 12-byte nonce
                use rand::RngCore;
                rand::rng().fill_bytes(&mut iv);
                iv
            }
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };
        let aad = b"";
        let (ciphertext, tag) = cipher.encrypt(&data, &iv, aad)?;

        // Create encryption metadata
        let metadata = crate::types::EncryptionMetadata {
            algorithm: algorithm.to_string(),
            key_id: kms_key_id.map(|s| s.to_string()).unwrap_or_default(),
            key_version: 1,
            iv: iv.clone(),
            tag: Some(tag.clone()),
            encryption_context: std::collections::HashMap::new(),
            encrypted_at: chrono::Utc::now(),
            original_size: data.len() as u64,
            encrypted_data_key,
        };

        // Create encrypted reader
        let encrypted_reader: Box<dyn AsyncRead + Send + Sync + Unpin> = Box::new(std::io::Cursor::new(ciphertext));

        Ok((encrypted_reader, metadata))
    }

    /// Decrypt object data stream
    #[allow(clippy::too_many_arguments)]
    pub async fn decrypt_object<S>(
        &self,
        bucket: &str,
        key: &str,
        mut stream: S,
        algorithm: &str,
        _kms_key_id: Option<&str>,
        encryption_context: Option<String>,
        metadata: HashMap<String, String>,
    ) -> EncryptionResult<Box<dyn AsyncRead + Send + Sync + Unpin>>
    where
        S: AsyncRead + Send + Unpin + 'static,
    {
        // Read encrypted data
        let mut encrypted_data = Vec::new();
        stream.read_to_end(&mut encrypted_data).await?;

        // Extract encrypted data key from metadata
        let encrypted_data_key = metadata
            .get("x-amz-server-side-encryption-key")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing encryption key"))?;

        let encrypted_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(encrypted_data_key)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid base64 key: {e}")))?;

        // Decrypt data key using KMS
        let context = encryption_context.unwrap_or_else(|| format!("bucket={bucket}&key={key}"));

        let mut context_map = std::collections::HashMap::new();
        context_map.insert("context".to_string(), context);

        let decrypt_request = crate::types::DecryptRequest {
            ciphertext: encrypted_key_bytes,
            encryption_context: context_map,
            grant_tokens: Vec::new(),
        };

        let data_key: Vec<u8> = self
            .kms_manager
            .decrypt(&decrypt_request, None)
            .await
            .map_err(crate::error::EncryptionError::KmsError)?;

        // Create cipher based on algorithm
        // Note: aws:kms uses AES256 for actual encryption, the difference is in key management
        let cipher: Box<dyn ObjectCipher> = match algorithm {
            "AES256" | "aws:kms" => Box::new(AesGcmCipher::new(&data_key)?),
            "ChaCha20Poly1305" => Box::new(ChaCha20Poly1305Cipher::new(&data_key)?),
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };

        // Extract encryption parameters from metadata
        let iv_str = metadata
            .get("x-amz-server-side-encryption-iv")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing IV in metadata"))?;
        let tag_str = metadata
            .get("x-amz-server-side-encryption-tag")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing tag in metadata"))?;

        let iv = base64::engine::general_purpose::STANDARD
            .decode(iv_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid IV: {e}")))?;
        let tag = base64::engine::general_purpose::STANDARD
            .decode(tag_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid tag: {e}")))?;
        
        // Use empty AAD as default (consistent with encryption)
        let aad = b"";

        // Decrypt the data
        let decrypted_data = cipher.decrypt(&encrypted_data, &iv, &tag, aad)?;

        // Create reader from decrypted data
        let decrypted_reader: Box<dyn AsyncRead + Send + Sync + Unpin> = Box::new(std::io::Cursor::new(decrypted_data));

        Ok(decrypted_reader)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsConfig;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let mut config = KmsConfig::default();
        config.kms_type = crate::config::KmsType::Local;
        config.default_key_id = Some("default".to_string());
        let kms_manager = KmsManager::new(config.clone()).await.expect("Failed to create KMS manager");

        // Create default key if it doesn't exist
        if kms_manager.describe_key("default", None).await.is_err() {
            kms_manager
                .create_key("default", "AES_256", None)
                .await
                .expect("Failed to create default key");
        }

        // Create service
        let service = ObjectEncryptionService::new(kms_manager);

        let test_data = b"Hello, World! This is test data for encryption.";
        let reader = Box::new(std::io::Cursor::new(test_data.to_vec())) as Box<dyn AsyncRead + Send + Unpin>;

        // Encrypt
        let (encrypted_reader, metadata) = service
            .encrypt_object::<std::io::Cursor<Vec<u8>>>("test-bucket", "default", reader, "AES256", Some("default"), None)
            .await
            .expect("Failed to encrypt object");

        // Convert EncryptionMetadata to HashMap for decrypt_object
        let mut metadata_map = std::collections::HashMap::new();
        metadata_map.insert("x-amz-server-side-encryption".to_string(), metadata.algorithm.clone());
        metadata_map.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), metadata.key_id.clone());
        metadata_map.insert(
            "x-amz-server-side-encryption-context".to_string(),
            serde_json::to_string(&metadata.encryption_context).unwrap_or_default(),
        );
        metadata_map.insert("iv".to_string(), base64::engine::general_purpose::STANDARD.encode(&metadata.iv));
        if let Some(tag) = &metadata.tag {
            metadata_map.insert("tag".to_string(), base64::engine::general_purpose::STANDARD.encode(tag));
        }
        // Add AAD for testing
        metadata_map.insert("aad".to_string(), base64::engine::general_purpose::STANDARD.encode(b""));
        // Use the actual encrypted data key from encrypt_object
        metadata_map.insert(
            "x-amz-server-side-encryption-key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.encrypted_data_key),
        );

        // Decrypt
        let decrypted_reader = service
            .decrypt_object("test-bucket", "default", encrypted_reader, "AES256", Some("default"), None, metadata_map)
            .await
            .expect("Failed to decrypt object");

        // Verify
        let mut decrypted_data = Vec::new();
        let mut reader = decrypted_reader;
        reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to read decrypted data");

        assert_eq!(decrypted_data, test_data);
    }
}
