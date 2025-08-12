use crate::{
    cipher::{AesGcmCipher, ChaCha20Poly1305Cipher, ObjectCipher},
    error::EncryptionResult,
    manager::KmsManager,
    types::EncryptionMetadata,
};
use base64::Engine;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::info;

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

    /// Encrypt object using a customer-provided key (SSE-C). No KMS interaction, key is not stored.
    pub async fn encrypt_object_with_customer_key<R>(
        &self,
        bucket: &str,
        key: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        algorithm: &str,
        customer_key: &[u8],
        encryption_context: Option<String>,
    ) -> EncryptionResult<(Vec<u8>, EncryptionMetadata)>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        // Read all plaintext
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        if algorithm != "AES256" {
            return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm));
        }
        if customer_key.len() != 32 {
            return Err(crate::error::EncryptionError::metadata_error("Invalid SSE-C key size (expected 256-bit)"));
        }

        // Build encryption context (still include bucket/key for deterministic AAD if we want to extend later)
        let mut ctx_map: HashMap<String, String> = match encryption_context.as_deref() {
            Some(s) if s.trim_start().starts_with('{') => serde_json::from_str::<HashMap<String, String>>(s).unwrap_or_default(),
            Some(s) => {
                let mut m = HashMap::new();
                m.insert("context".to_string(), s.to_string());
                m
            }
            None => HashMap::new(),
        };
        ctx_map.entry("bucket".to_string()).or_insert_with(|| bucket.to_string());
        ctx_map.entry("key".to_string()).or_insert_with(|| key.to_string());

        // Create cipher directly from customer key
        let cipher: Box<dyn ObjectCipher> = Box::new(AesGcmCipher::new(customer_key)?);
        // Generate IV
        let iv_arr: [u8; 12] = rand::random();
        let iv = iv_arr.to_vec();
        let aad = b""; // reserved for future use
        let (ciphertext, tag) = cipher.encrypt(&data, &iv, aad)?;

        // Construct metadata (encrypted_data_key is empty for SSE-C)
        let metadata = crate::types::EncryptionMetadata {
            algorithm: algorithm.to_string(),
            key_id: "sse-c".to_string(),
            key_version: 1,
            iv: iv.clone(),
            tag: Some(tag.clone()),
            encryption_context: ctx_map,
            encrypted_at: chrono::Utc::now(),
            original_size: data.len() as u64,
            encrypted_data_key: Vec::new(),
        };

        Ok((ciphertext, metadata))
    }

    /// Decrypt object using customer-provided key (SSE-C)
    pub async fn decrypt_object_with_customer_key<S>(
        &self,
        _bucket: &str,
        _key: &str,
        mut stream: S,
        algorithm: &str,
        customer_key: &[u8],
        metadata: HashMap<String, String>,
    ) -> EncryptionResult<Box<dyn AsyncRead + Send + Sync + Unpin>>
    where
        S: AsyncRead + Send + Unpin + 'static,
    {
        if algorithm != "AES256" {
            return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm));
        }
        if customer_key.len() != 32 {
            return Err(crate::error::EncryptionError::metadata_error("Invalid SSE-C key size (expected 256-bit)"));
        }

        let mut encrypted_data = Vec::new();
        stream.read_to_end(&mut encrypted_data).await?;

        let iv_str = metadata
            .get("x-rustfs-internal-sse-iv")
            .or_else(|| metadata.get("x-amz-server-side-encryption-iv"))
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing IV in metadata"))?;
        let tag_str = metadata
            .get("x-rustfs-internal-sse-tag")
            .or_else(|| metadata.get("x-amz-server-side-encryption-tag"))
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing tag in metadata"))?;

        let iv = base64::engine::general_purpose::STANDARD
            .decode(iv_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid IV: {e}")))?;
        let tag = base64::engine::general_purpose::STANDARD
            .decode(tag_str)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid tag: {e}")))?;

        let cipher: Box<dyn ObjectCipher> = Box::new(AesGcmCipher::new(customer_key)?);
        let aad = b"";
        let decrypted_data = cipher.decrypt(&encrypted_data, &iv, &tag, aad)?;
        Ok(Box::new(std::io::Cursor::new(decrypted_data)))
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
        let actual_key_id = kms_key_id.unwrap_or_else(|| self.kms_manager.default_key_id().unwrap_or("rustfs-default-key"));

        // Key existence / auto-create policy:
        // 1. SSE-S3 (algorithm AES256 and no explicit kms_key_id) -> lazy auto-create internal key.
        // 2. SSE-KMS (algorithm aws:kms OR explicit kms_key_id) -> MUST already exist; if missing return error instructing user to create.
        let is_sse_s3 = algorithm == "AES256" && kms_key_id.is_none();
        let is_sse_kms = algorithm == "aws:kms" || kms_key_id.is_some();

        if is_sse_s3 {
            let key_exists = self.kms_manager.describe_key(actual_key_id, None).await.is_ok();
            if !key_exists {
                info!("SSE-S3 internal key '{}' not found, auto-creating", actual_key_id);
                if let Err(e) = self.kms_manager.create_key(actual_key_id, "AES_256", None).await {
                    return Err(crate::error::EncryptionError::KmsError(e));
                }
            }
        } else if is_sse_kms {
            // For SSE-KMS enforce explicit key existence
            if self.kms_manager.describe_key(actual_key_id, None).await.is_err() {
                return Err(crate::error::EncryptionError::metadata_error(format!(
                    "SSE-KMS master key '{}' not found. Please create it via admin API before use.",
                    actual_key_id
                )));
            }
        }

        // Build encryption context map (prefer explicit JSON, otherwise default bucket/key)
        let mut ctx_map: HashMap<String, String> = match encryption_context.as_deref() {
            Some(s) if s.trim_start().starts_with('{') => serde_json::from_str::<HashMap<String, String>>(s).unwrap_or_default(),
            Some(s) => {
                let mut m = HashMap::new();
                m.insert("context".to_string(), s.to_string());
                m
            }
            None => HashMap::new(),
        };
        // Always include bucket/key for deterministic AAD
        ctx_map.entry("bucket".to_string()).or_insert_with(|| bucket.to_string());
        ctx_map.entry("key".to_string()).or_insert_with(|| key.to_string());

        // Generate data encryption key with context
        let mut request = crate::types::GenerateKeyRequest::new(actual_key_id.to_string(), "AES_256".to_string()).with_length(32);
        request.encryption_context = ctx_map.clone();

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
                // AES-GCM uses 12-byte IV
                let iv_arr: [u8; 12] = rand::random();
                iv_arr.to_vec()
            }
            "ChaCha20Poly1305" => {
                // ChaCha20Poly1305 uses 12-byte nonce
                let iv_arr: [u8; 12] = rand::random();
                iv_arr.to_vec()
            }
            _ => return Err(crate::error::EncryptionError::unsupported_algorithm(algorithm)),
        };
        let aad = b"";
        let (ciphertext, tag) = cipher.encrypt(&data, &iv, aad)?;

        // Create encryption metadata
        let metadata = crate::types::EncryptionMetadata {
            algorithm: algorithm.to_string(),
            key_id: actual_key_id.to_string(),
            key_version: 1,
            iv: iv.clone(),
            tag: Some(tag.clone()),
            encryption_context: ctx_map.clone(),
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

        // Extract encrypted data key from internal sealed metadata
        let encrypted_data_key = metadata
            .get("x-rustfs-internal-sse-key")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing encryption key"))?;

        let encrypted_key_bytes = base64::engine::general_purpose::STANDARD
            .decode(encrypted_data_key)
            .map_err(|e| crate::error::EncryptionError::metadata_error(format!("Invalid base64 key: {e}")))?;

        // Decrypt data key using KMS with consistent context
        // Prefer explicit context param; otherwise try internal stored context; finally default bucket/key
        let mut context_map: HashMap<String, String> = match encryption_context
            .as_deref()
            .or_else(|| metadata.get("x-rustfs-internal-sse-context").map(|s| s.as_str()))
        {
            Some(s) if s.trim_start().starts_with('{') => serde_json::from_str::<HashMap<String, String>>(s).unwrap_or_default(),
            Some(s) => {
                let mut m = HashMap::new();
                m.insert("context".to_string(), s.to_string());
                m
            }
            None => HashMap::new(),
        };
        context_map.entry("bucket".to_string()).or_insert_with(|| bucket.to_string());
        context_map.entry("key".to_string()).or_insert_with(|| key.to_string());

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

        // Extract encryption parameters from internal metadata only
        let iv_str = metadata
            .get("x-rustfs-internal-sse-iv")
            .ok_or_else(|| crate::error::EncryptionError::metadata_error("Missing IV in metadata"))?;
        let tag_str = metadata
            .get("x-rustfs-internal-sse-tag")
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

        // Convert EncryptionMetadata to internal sealed metadata map for decrypt_object
        let mut metadata_map = std::collections::HashMap::new();
        metadata_map.insert("x-amz-server-side-encryption".to_string(), metadata.algorithm.clone());
        metadata_map.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), metadata.key_id.clone());
        metadata_map.insert(
            "x-rustfs-internal-sse-context".to_string(),
            serde_json::to_string(&metadata.encryption_context).unwrap_or_default(),
        );
        metadata_map.insert(
            "x-rustfs-internal-sse-iv".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.iv),
        );
        if let Some(tag) = &metadata.tag {
            metadata_map.insert(
                "x-rustfs-internal-sse-tag".to_string(),
                base64::engine::general_purpose::STANDARD.encode(tag),
            );
        }
        // Use the actual encrypted data key from encrypt_object in internal field
        metadata_map.insert(
            "x-rustfs-internal-sse-key".to_string(),
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

    #[tokio::test]
    async fn test_decrypt_rejects_legacy_public_keys() {
        let mut config = crate::config::KmsConfig::default();
        config.kms_type = crate::config::KmsType::Local;
        config.default_key_id = Some("default".to_string());
        let kms_manager = KmsManager::new(config.clone()).await.expect("Failed to create KMS manager");

        if kms_manager.describe_key("default", None).await.is_err() {
            kms_manager
                .create_key("default", "AES_256", None)
                .await
                .expect("Failed to create default key");
        }

        let service = ObjectEncryptionService::new(kms_manager);
        let test_data = b"reject legacy";
        let reader = Box::new(std::io::Cursor::new(test_data.to_vec())) as Box<dyn AsyncRead + Send + Unpin>;
        let (encrypted_reader, metadata) = service
            .encrypt_object::<std::io::Cursor<Vec<u8>>>("b", "k", reader, "AES256", Some("default"), None)
            .await
            .expect("encrypt");

        // Deliberately provide only legacy public keys, not internal ones
        let mut legacy_meta = std::collections::HashMap::new();
        legacy_meta.insert("x-amz-server-side-encryption".to_string(), metadata.algorithm.clone());
        legacy_meta.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), metadata.key_id.clone());
        legacy_meta.insert(
            "x-amz-server-side-encryption-context".to_string(),
            serde_json::to_string(&metadata.encryption_context).unwrap_or_default(),
        );
        legacy_meta.insert(
            "x-amz-server-side-encryption-iv".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.iv),
        );
        if let Some(tag) = &metadata.tag {
            legacy_meta.insert(
                "x-amz-server-side-encryption-tag".to_string(),
                base64::engine::general_purpose::STANDARD.encode(tag),
            );
        }
        legacy_meta.insert(
            "x-amz-server-side-encryption-key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.encrypted_data_key),
        );

        let res = service
            .decrypt_object("b", "k", encrypted_reader, "AES256", Some("default"), None, legacy_meta)
            .await;
        assert!(res.is_err(), "decrypt should fail without internal sealed metadata");
    }
}
