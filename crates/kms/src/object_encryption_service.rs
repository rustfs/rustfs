use crate::{
    cipher::{ObjectCipher, AesGcmCipher, ChaCha20Poly1305Cipher},
    error::EncryptionResult,
    manager::KmsManager,
    object_encryption::EncryptionAlgorithm,
    security::{SecretKey, SecretVec},
    types::{EncryptionMetadata, DecryptionInput},
};
use async_trait::async_trait;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};
use bytes::Bytes;

/// Service for encrypting and decrypting S3 objects
pub struct ObjectEncryptionService {
    kms_manager: KmsManager,
}

impl ObjectEncryptionService {
    pub fn new(kms_manager: KmsManager) -> Self {
        Self { kms_manager }
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

        // Generate data encryption key
        let context = encryption_context.unwrap_or_else(|| {
            format!("bucket={}&key={}", bucket, key)
        });
        
        let request = crate::types::GenerateKeyRequest::new(
            kms_key_id.clone().unwrap_or_default().to_string(),
            "AES_256".to_string()
        ).with_length(32);
        
        let data_key_result = self.kms_manager
            .generate_data_key(&request, None)
            .await?;
        
        let data_key = data_key_result.plaintext.ok_or_else(|| {
            crate::error::EncryptionError::metadata_error("No plaintext key in data key result")
        })?;
        let encrypted_data_key = data_key_result.ciphertext;

        // Create cipher based on algorithm
        let cipher: Box<dyn ObjectCipher> = match algorithm {
            "AES256" => Box::new(AesGcmCipher::new(&data_key)?),
            "ChaCha20Poly1305" => Box::new(ChaCha20Poly1305Cipher::new(&data_key)?),
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };

        // Generate IV and encrypt the data
        let iv = match algorithm {
            "AES256" => {
                let mut iv = vec![0u8; 12]; // AES-GCM uses 12-byte IV
                use rand::RngCore;
                rand::thread_rng().fill_bytes(&mut iv);
                iv
            },
            "ChaCha20Poly1305" => {
                let mut iv = vec![0u8; 12]; // ChaCha20Poly1305 uses 12-byte nonce
                use rand::RngCore;
                rand::thread_rng().fill_bytes(&mut iv);
                iv
            },
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
        };
        
        // Create encrypted reader
        let encrypted_reader: Box<dyn AsyncRead + Send + Sync + Unpin> = Box::new(std::io::Cursor::new(ciphertext));
        
        Ok((encrypted_reader, metadata))
    }

    /// Decrypt object data stream
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
        
        let encrypted_key_bytes = base64::decode(encrypted_data_key)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid base64 key: {}", e)))?;

        // Decrypt data key using KMS
        let context = encryption_context.unwrap_or_else(|| {
            format!("bucket={}&key={}", bucket, key)
        });
        
        let mut context_map = std::collections::HashMap::new();
        context_map.insert("context".to_string(), context);
        
        let decrypt_request = crate::types::DecryptRequest {
            ciphertext: encrypted_key_bytes,
            encryption_context: context_map,
            grant_tokens: Vec::new(),
        };
        
        let data_key: Vec<u8> = self.kms_manager
            .decrypt(&decrypt_request, None)
            .await
            .map_err(|e| crate::error::EncryptionError::KmsError(e))?;

        // Create cipher based on algorithm
        let cipher: Box<dyn ObjectCipher> = match algorithm {
            "AES256" => Box::new(AesGcmCipher::new(&data_key)?),
            "ChaCha20Poly1305" => Box::new(ChaCha20Poly1305Cipher::new(&data_key)?),
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };

        // Extract encryption parameters from metadata
        let iv_str = metadata.get("iv")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing IV in metadata"))?;
        let tag_str = metadata.get("tag")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing tag in metadata"))?;
        
        let iv = base64::decode(iv_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid IV: {}", e)))?;
        let tag = base64::decode(tag_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid tag: {}", e)))?;
        let aad_str = metadata.get("aad")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing AAD in metadata"))?;
        let aad = base64::decode(aad_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid AAD: {}", e)))?;

        // Decrypt the data
        let decrypted_data = cipher.decrypt(&encrypted_data, &iv, &tag, &aad)?;

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
        let config = KmsConfig::default();
        let kms_manager = KmsManager::new(config).await.unwrap();
        let service = ObjectEncryptionService::new(kms_manager);

        let test_data = b"Hello, World! This is test data for encryption.";
        let reader: Box<dyn AsyncRead + Send + Unpin> = Box::new(std::io::Cursor::new(test_data.to_vec()));

        // Encrypt
        let (encrypted_reader, metadata) = service
            .encrypt_object("test-bucket", "test-key", reader, "AES256", None, None)
            .await
            .unwrap();

        // Decrypt
        let decrypted_reader = service
            .decrypt_object(
                "test-bucket",
                "test-key",
                encrypted_reader,
                "AES256",
                None,
                None,
                metadata,
            )
            .await
            .unwrap();

        // Verify
        let mut decrypted_data = Vec::new();
        let mut reader = decrypted_reader;
        reader.read_to_end(&mut decrypted_data).await.unwrap();
        
        assert_eq!(decrypted_data, test_data);
    }
}