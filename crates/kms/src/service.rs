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

//! Object encryption service for S3-compatible encryption

use crate::encryption::ciphers::{create_cipher, generate_iv};
use crate::error::{KmsError, Result};
use crate::manager::KmsManager;
use crate::types::*;
use base64::Engine;
use jiff::Zoned;
use rand::random;
use std::collections::HashMap;
use std::io::Cursor;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{debug, info};
use zeroize::Zeroize;

/// Data key for object encryption
/// SECURITY: This struct automatically zeros sensitive key material when dropped
#[derive(Debug, Clone)]
pub struct DataKey {
    /// 256-bit encryption key - automatically zeroed on drop
    pub plaintext_key: [u8; 32],
    /// 96-bit nonce for GCM mode - not secret so no need to zero
    pub nonce: [u8; 12],
}

// SECURITY: Implement Drop to automatically zero sensitive key material
impl Drop for DataKey {
    fn drop(&mut self) {
        self.plaintext_key.zeroize();
    }
}

/// Service for encrypting and decrypting S3 objects with KMS integration
pub struct ObjectEncryptionService {
    kms_manager: KmsManager,
}

/// Result of object encryption
#[derive(Debug, Clone)]
pub struct EncryptionResult {
    /// Encrypted data
    pub ciphertext: Vec<u8>,
    /// Encryption metadata to be stored with the object
    pub metadata: EncryptionMetadata,
}

impl ObjectEncryptionService {
    /// Create a new object encryption service
    ///
    /// # Arguments
    /// * `kms_manager` - KMS manager to use for key operations
    ///
    /// # Returns
    /// New ObjectEncryptionService instance
    ///
    pub fn new(kms_manager: KmsManager) -> Self {
        Self { kms_manager }
    }

    /// Create a new master key (delegates to KMS manager)
    ///
    /// # Arguments
    /// * `request` - CreateKeyRequest with key parameters
    ///
    /// # Returns
    /// CreateKeyResponse with created key details
    ///
    pub async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        self.kms_manager.create_key(request).await
    }

    /// Describe a master key (delegates to KMS manager)
    ///
    /// # Arguments
    /// * `request` - DescribeKeyRequest with key ID
    ///
    /// # Returns
    /// DescribeKeyResponse with key metadata
    ///
    pub async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        self.kms_manager.describe_key(request).await
    }

    /// List master keys (delegates to KMS manager)
    ///
    /// # Arguments
    /// * `request` - ListKeysRequest with listing parameters
    ///
    /// # Returns
    /// ListKeysResponse with list of keys
    ///
    pub async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        self.kms_manager.list_keys(request).await
    }

    /// Generate a data encryption key (delegates to KMS manager)
    ///
    /// # Arguments
    /// * `request` - GenerateDataKeyRequest with key parameters
    ///
    /// # Returns
    /// GenerateDataKeyResponse with generated key details
    ///
    pub async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        self.kms_manager.generate_data_key(request).await
    }

    /// Get the default key ID
    ///
    /// # Returns
    /// Option with default key ID if configured
    ///
    pub fn get_default_key_id(&self) -> Option<&String> {
        self.kms_manager.get_default_key_id()
    }

    /// Get cache statistics
    ///
    /// # Returns
    /// Option with (hits, misses) if caching is enabled
    ///
    pub async fn cache_stats(&self) -> Option<(u64, u64)> {
        self.kms_manager.cache_stats().await
    }

    /// Clear the cache
    ///
    /// # Returns
    /// Result indicating success or failure
    ///
    pub async fn clear_cache(&self) -> Result<()> {
        self.kms_manager.clear_cache().await
    }

    /// Get backend health status
    ///
    /// # Returns
    /// Result indicating if backend is healthy
    ///
    pub async fn health_check(&self) -> Result<bool> {
        self.kms_manager.health_check().await
    }

    /// Create a data encryption key for object encryption
    ///
    /// # Arguments
    /// * `kms_key_id` - Optional KMS key ID to use (uses default if None)
    /// * `context` - ObjectEncryptionContext with bucket and object key
    ///
    /// # Returns
    /// Tuple with DataKey and encrypted key blob
    ///
    pub async fn create_data_key(
        &self,
        kms_key_id: &Option<String>,
        context: &ObjectEncryptionContext,
    ) -> Result<(DataKey, Vec<u8>)> {
        // Determine the KMS key ID to use
        let actual_key_id = kms_key_id
            .as_ref()
            .map(|s| s.as_str())
            .or_else(|| self.kms_manager.get_default_key_id().map(|s| s.as_str()))
            .ok_or_else(|| KmsError::configuration_error("No KMS key ID specified and no default configured"))?;

        // Build encryption context
        let mut enc_context = context.encryption_context.clone();
        enc_context.insert("bucket".to_string(), context.bucket.clone());
        enc_context.insert("object_key".to_string(), context.object_key.clone());

        let request = GenerateDataKeyRequest {
            key_id: actual_key_id.to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: enc_context,
        };

        let data_key_response = self.kms_manager.generate_data_key(request).await?;

        // Generate a unique random nonce for this data key
        // This ensures each object/part gets a unique base nonce for streaming encryption
        let nonce: [u8; 12] = random();
        tracing::info!("Generated random nonce for data key: {:02x?}", nonce);

        let data_key = DataKey {
            plaintext_key: data_key_response
                .plaintext_key
                .try_into()
                .map_err(|_| KmsError::internal_error("Invalid key length"))?,
            nonce,
        };

        Ok((data_key, data_key_response.ciphertext_blob))
    }

    /// Decrypt a data encryption key
    ///
    /// # Arguments
    /// * `encrypted_key` - Encrypted data key blob
    /// * `context` - ObjectEncryptionContext with bucket and object key
    ///
    /// # Returns
    /// DataKey with decrypted key
    ///
    pub async fn decrypt_data_key(&self, encrypted_key: &[u8], _context: &ObjectEncryptionContext) -> Result<DataKey> {
        let decrypt_request = DecryptRequest {
            ciphertext: encrypted_key.to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        let decrypt_response = self.kms_manager.decrypt(decrypt_request).await?;

        let data_key = DataKey {
            plaintext_key: decrypt_response
                .plaintext
                .try_into()
                .map_err(|_| KmsError::internal_error("Invalid key length"))?,
            nonce: [0u8; 12], // This will be replaced by stored nonce during GET
        };

        Ok(data_key)
    }

    /// Encrypt object data using server-side encryption
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object_key` - S3 object key
    /// * `reader` - Data reader
    /// * `algorithm` - Encryption algorithm to use
    /// * `kms_key_id` - Optional KMS key ID (uses default if None)
    /// * `encryption_context` - Additional encryption context
    ///
    /// # Returns
    /// EncryptionResult containing encrypted data and metadata
    pub async fn encrypt_object<R>(
        &self,
        bucket: &str,
        object_key: &str,
        mut reader: R,
        algorithm: &EncryptionAlgorithm,
        kms_key_id: Option<&str>,
        encryption_context: Option<&HashMap<String, String>>,
    ) -> Result<EncryptionResult>
    where
        R: AsyncRead + Unpin,
    {
        debug!("Encrypting object {}/{} with algorithm {:?}", bucket, object_key, algorithm);

        // Read all data (for simplicity - in production, use streaming)
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;

        let original_size = data.len() as u64;

        // Determine the KMS key ID to use
        let actual_key_id = kms_key_id
            .or_else(|| self.kms_manager.get_default_key_id().map(|s| s.as_str()))
            .ok_or_else(|| KmsError::configuration_error("No KMS key ID specified and no default configured"))?;

        // Build encryption context
        let mut context = encryption_context.cloned().unwrap_or_default();
        context.insert("bucket".to_string(), bucket.to_string());
        context.insert("object_key".to_string(), object_key.to_string());
        // Backward compatibility: also include legacy "object" context key
        context.insert("object".to_string(), object_key.to_string());
        context.insert("algorithm".to_string(), algorithm.as_str().to_string());

        // Auto-create key for SSE-S3 if it doesn't exist
        if algorithm == &EncryptionAlgorithm::Aes256 {
            let describe_req = DescribeKeyRequest {
                key_id: actual_key_id.to_string(),
            };
            if let Err(KmsError::KeyNotFound { .. }) = self.kms_manager.describe_key(describe_req).await {
                info!("Auto-creating SSE-S3 key: {}", actual_key_id);
                let create_req = CreateKeyRequest {
                    key_name: Some(actual_key_id.to_string()),
                    key_usage: KeyUsage::EncryptDecrypt,
                    description: Some("Auto-created SSE-S3 key".to_string()),
                    policy: None,
                    tags: HashMap::new(),
                    origin: None,
                };
                self.kms_manager
                    .create_key(create_req)
                    .await
                    .map_err(|e| KmsError::backend_error(format!("Failed to auto-create SSE-S3 key {actual_key_id}: {e}")))?;
            }
        } else {
            // For SSE-KMS, key must exist
            let describe_req = DescribeKeyRequest {
                key_id: actual_key_id.to_string(),
            };
            self.kms_manager.describe_key(describe_req).await.map_err(|_| {
                KmsError::invalid_operation(format!("SSE-KMS key '{actual_key_id}' not found. Please create it first."))
            })?;
        }

        // Generate data encryption key
        let request = GenerateDataKeyRequest {
            key_id: actual_key_id.to_string(),
            key_spec: KeySpec::Aes256,
            encryption_context: context.clone(),
        };

        let data_key = self
            .kms_manager
            .generate_data_key(request)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to generate data key: {e}")))?;

        let plaintext_key = data_key.plaintext_key;

        // Create cipher and generate IV
        let cipher = create_cipher(algorithm, &plaintext_key)?;
        let iv = generate_iv(algorithm);

        // Build AAD from encryption context
        let aad = serde_json::to_vec(&context)?;

        // Encrypt the data
        let (ciphertext, tag) = cipher.encrypt(&data, &iv, &aad)?;

        // Create encryption metadata
        let metadata = EncryptionMetadata {
            algorithm: algorithm.as_str().to_string(),
            key_id: actual_key_id.to_string(),
            key_version: 1, // Default to version 1 for now
            iv,
            tag: Some(tag),
            encryption_context: context,
            encrypted_at: Zoned::now(),
            original_size,
            encrypted_data_key: data_key.ciphertext_blob,
        };

        info!("Successfully encrypted object {}/{} ({} bytes)", bucket, object_key, original_size);

        Ok(EncryptionResult { ciphertext, metadata })
    }

    /// Decrypt object data
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object_key` - S3 object key
    /// * `ciphertext` - Encrypted data
    /// * `metadata` - Encryption metadata
    /// * `expected_context` - Expected encryption context for validation
    ///
    /// # Returns
    /// Decrypted data as a reader
    pub async fn decrypt_object(
        &self,
        bucket: &str,
        object_key: &str,
        ciphertext: Vec<u8>,
        metadata: &EncryptionMetadata,
        expected_context: Option<&HashMap<String, String>>,
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        debug!("Decrypting object {}/{} with algorithm {}", bucket, object_key, metadata.algorithm);

        // Validate encryption context if provided
        if let Some(expected) = expected_context {
            self.validate_encryption_context(&metadata.encryption_context, expected)?;
        }

        // Parse algorithm
        let algorithm = metadata
            .algorithm
            .parse::<EncryptionAlgorithm>()
            .map_err(|_| KmsError::unsupported_algorithm(&metadata.algorithm))?;

        // Decrypt the data key
        let decrypt_request = DecryptRequest {
            ciphertext: metadata.encrypted_data_key.clone(),
            encryption_context: metadata.encryption_context.clone(),
            grant_tokens: Vec::new(),
        };

        let decrypt_response = self
            .kms_manager
            .decrypt(decrypt_request)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to decrypt data key: {e}")))?;

        // Create cipher
        let cipher = create_cipher(&algorithm, &decrypt_response.plaintext)?;

        // Build AAD from encryption context
        let aad = serde_json::to_vec(&metadata.encryption_context)?;

        // Get tag from metadata
        let tag = metadata
            .tag
            .as_ref()
            .ok_or_else(|| KmsError::invalid_operation("Missing authentication tag"))?;

        // Decrypt the data
        let plaintext = cipher.decrypt(&ciphertext, &metadata.iv, tag, &aad)?;

        info!("Successfully decrypted object {}/{} ({} bytes)", bucket, object_key, plaintext.len());

        Ok(Box::new(Cursor::new(plaintext)))
    }

    /// Encrypt object with customer-provided key (SSE-C)
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object_key` - S3 object key
    /// * `reader` - Data reader
    /// * `customer_key` - Customer-provided 256-bit key
    /// * `customer_key_md5` - Optional MD5 hash of the customer key for validation
    ///
    /// # Returns
    /// EncryptionResult with SSE-C metadata
    pub async fn encrypt_object_with_customer_key<R>(
        &self,
        bucket: &str,
        object_key: &str,
        mut reader: R,
        customer_key: &[u8],
        customer_key_md5: Option<&str>,
    ) -> Result<EncryptionResult>
    where
        R: AsyncRead + Unpin,
    {
        debug!("Encrypting object {}/{} with customer-provided key (SSE-C)", bucket, object_key);

        // Validate key size
        if customer_key.len() != 32 {
            return Err(KmsError::invalid_key_size(32, customer_key.len()));
        }

        // Validate key MD5 if provided
        if let Some(expected_md5) = customer_key_md5 {
            let actual_md5 = md5::compute(customer_key);
            let actual_md5_hex = format!("{actual_md5:x}");
            if actual_md5_hex != expected_md5.to_lowercase() {
                return Err(KmsError::validation_error("Customer key MD5 mismatch"));
            }
        }

        // Read all data
        let mut data = Vec::new();
        reader.read_to_end(&mut data).await?;
        let original_size = data.len() as u64;

        // Create cipher and generate IV
        let algorithm = EncryptionAlgorithm::Aes256;
        let cipher = create_cipher(&algorithm, customer_key)?;
        let iv = generate_iv(&algorithm);

        // Build minimal encryption context for SSE-C
        let context = HashMap::from([
            ("bucket".to_string(), bucket.to_string()),
            ("object".to_string(), object_key.to_string()),
            ("sse_type".to_string(), "customer".to_string()),
        ]);

        let aad = serde_json::to_vec(&context)?;

        // Encrypt the data
        let (ciphertext, tag) = cipher.encrypt(&data, &iv, &aad)?;

        // Create metadata (no encrypted data key for SSE-C)
        let metadata = EncryptionMetadata {
            algorithm: algorithm.as_str().to_string(),
            key_id: "sse-c".to_string(), // Special marker for SSE-C
            key_version: 1,
            iv,
            tag: Some(tag),
            encryption_context: context,
            encrypted_at: Zoned::now(),
            original_size,
            encrypted_data_key: Vec::new(), // Empty for SSE-C
        };

        info!(
            "Successfully encrypted object {}/{} with SSE-C ({} bytes)",
            bucket, object_key, original_size
        );

        Ok(EncryptionResult { ciphertext, metadata })
    }

    /// Decrypt object with customer-provided key (SSE-C)
    ///
    /// # Arguments
    /// * `bucket` - S3 bucket name
    /// * `object_key` - S3 object key
    /// * `ciphertext` - Encrypted data
    /// * `metadata` - Encryption metadata
    /// * `customer_key` - Customer-provided 256-bit key
    ///
    /// # Returns
    /// Decrypted data as a reader
    ///
    pub async fn decrypt_object_with_customer_key(
        &self,
        bucket: &str,
        object_key: &str,
        ciphertext: Vec<u8>,
        metadata: &EncryptionMetadata,
        customer_key: &[u8],
    ) -> Result<Box<dyn AsyncRead + Send + Sync + Unpin>> {
        debug!("Decrypting object {}/{} with customer-provided key (SSE-C)", bucket, object_key);

        // Validate key size
        if customer_key.len() != 32 {
            return Err(KmsError::invalid_key_size(32, customer_key.len()));
        }

        // Validate that this is SSE-C
        if metadata.key_id != "sse-c" {
            return Err(KmsError::invalid_operation("This object was not encrypted with SSE-C"));
        }

        // Parse algorithm
        let algorithm = metadata
            .algorithm
            .parse::<EncryptionAlgorithm>()
            .map_err(|_| KmsError::unsupported_algorithm(&metadata.algorithm))?;

        // Create cipher
        let cipher = create_cipher(&algorithm, customer_key)?;

        // Build AAD from encryption context
        let aad = serde_json::to_vec(&metadata.encryption_context)?;

        // Get tag from metadata
        let tag = metadata
            .tag
            .as_ref()
            .ok_or_else(|| KmsError::invalid_operation("Missing authentication tag"))?;

        // Decrypt the data
        let plaintext = cipher.decrypt(&ciphertext, &metadata.iv, tag, &aad)?;

        info!(
            "Successfully decrypted SSE-C object {}/{} ({} bytes)",
            bucket,
            object_key,
            plaintext.len()
        );

        Ok(Box::new(Cursor::new(plaintext)))
    }

    /// Validate encryption context
    ///
    /// # Arguments
    /// * `actual` - Actual encryption context from metadata
    /// * `expected` - Expected encryption context to validate against
    ///
    /// # Returns
    /// Result indicating success or context mismatch
    ///
    fn validate_encryption_context(&self, actual: &HashMap<String, String>, expected: &HashMap<String, String>) -> Result<()> {
        for (key, expected_value) in expected {
            match actual.get(key) {
                Some(actual_value) if actual_value == expected_value => continue,
                Some(actual_value) => {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
                None => {
                    return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
                }
            }
        }
        Ok(())
    }

    /// Convert encryption metadata to HTTP headers for S3 compatibility
    ///
    /// # Arguments
    /// * `metadata` - EncryptionMetadata to convert
    ///
    /// # Returns
    /// HashMap of HTTP headers
    ///
    pub fn metadata_to_headers(&self, metadata: &EncryptionMetadata) -> HashMap<String, String> {
        let mut headers = HashMap::new();

        // Standard S3 encryption headers
        if metadata.key_id == "sse-c" {
            headers.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
            headers.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        } else if metadata.algorithm == "AES256" {
            headers.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
            // For SSE-S3, we still need to store the key ID for internal use
            headers.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), metadata.key_id.clone());
        } else {
            headers.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
            headers.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), metadata.key_id.clone());
        }

        // Internal headers for decryption
        headers.insert(
            "x-rustfs-encryption-iv".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.iv),
        );

        if let Some(ref tag) = metadata.tag {
            headers.insert(
                "x-rustfs-encryption-tag".to_string(),
                base64::engine::general_purpose::STANDARD.encode(tag),
            );
        }

        headers.insert(
            "x-rustfs-encryption-key".to_string(),
            base64::engine::general_purpose::STANDARD.encode(&metadata.encrypted_data_key),
        );

        headers.insert(
            "x-rustfs-encryption-context".to_string(),
            serde_json::to_string(&metadata.encryption_context).unwrap_or_default(),
        );

        headers
    }

    /// Parse encryption metadata from HTTP headers
    ///
    /// # Arguments
    /// * `headers` - HashMap of HTTP headers
    ///
    /// # Returns
    /// EncryptionMetadata parsed from headers
    ///
    pub fn headers_to_metadata(&self, headers: &HashMap<String, String>) -> Result<EncryptionMetadata> {
        let algorithm = headers
            .get("x-amz-server-side-encryption")
            .ok_or_else(|| KmsError::validation_error("Missing encryption algorithm header"))?
            .clone();

        let key_id = if algorithm == "AES256" && headers.contains_key("x-amz-server-side-encryption-customer-algorithm") {
            "sse-c".to_string()
        } else if let Some(kms_key_id) = headers.get("x-amz-server-side-encryption-aws-kms-key-id") {
            kms_key_id.clone()
        } else {
            return Err(KmsError::validation_error("Missing key ID"));
        };

        let iv = headers
            .get("x-rustfs-encryption-iv")
            .ok_or_else(|| KmsError::validation_error("Missing IV header"))?;
        let iv = base64::engine::general_purpose::STANDARD
            .decode(iv)
            .map_err(|e| KmsError::validation_error(format!("Invalid IV: {e}")))?;

        let tag = if let Some(tag_str) = headers.get("x-rustfs-encryption-tag") {
            Some(
                base64::engine::general_purpose::STANDARD
                    .decode(tag_str)
                    .map_err(|e| KmsError::validation_error(format!("Invalid tag: {e}")))?,
            )
        } else {
            None
        };

        let encrypted_data_key = if let Some(key_str) = headers.get("x-rustfs-encryption-key") {
            base64::engine::general_purpose::STANDARD
                .decode(key_str)
                .map_err(|e| KmsError::validation_error(format!("Invalid encrypted key: {e}")))?
        } else {
            Vec::new() // Empty for SSE-C
        };

        let encryption_context = if let Some(context_str) = headers.get("x-rustfs-encryption-context") {
            serde_json::from_str(context_str)
                .map_err(|e| KmsError::validation_error(format!("Invalid encryption context: {e}")))?
        } else {
            HashMap::new()
        };

        Ok(EncryptionMetadata {
            algorithm,
            key_id,
            key_version: 1, // Default for parsing
            iv,
            tag,
            encryption_context,
            encrypted_at: Zoned::now(),
            original_size: 0, // Not available from headers
            encrypted_data_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::KmsConfig;
    use std::sync::Arc;
    use tempfile::TempDir;

    async fn create_test_service() -> (ObjectEncryptionService, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_default_key("test-key".to_string());
        let backend = Arc::new(
            crate::backends::local::LocalKmsBackend::new(config.clone())
                .await
                .expect("local backend should initialize"),
        );
        let kms_manager = KmsManager::new(backend, config);
        let service = ObjectEncryptionService::new(kms_manager);
        (service, temp_dir)
    }

    #[tokio::test]
    async fn test_sse_s3_encryption() {
        let (service, _temp_dir) = create_test_service().await;

        let bucket = "test-bucket";
        let object_key = "test-object";
        let data = b"Hello, SSE-S3!";
        let reader = Cursor::new(data.to_vec());

        // Encrypt with SSE-S3 (auto-create key)
        let result = service
            .encrypt_object(
                bucket,
                object_key,
                reader,
                &EncryptionAlgorithm::Aes256,
                None, // Use default key
                None,
            )
            .await
            .expect("Encryption failed");

        assert!(!result.ciphertext.is_empty());
        assert_eq!(result.metadata.algorithm, "AES256");
        assert_eq!(result.metadata.original_size, data.len() as u64);

        // Decrypt
        let decrypted_reader = service
            .decrypt_object(bucket, object_key, result.ciphertext, &result.metadata, None)
            .await
            .expect("Decryption failed");

        let mut decrypted_data = Vec::new();
        let mut reader = decrypted_reader;
        reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to read decrypted data");
        assert_eq!(decrypted_data, data);
    }

    #[tokio::test]
    async fn test_sse_c_encryption() {
        let (service, _temp_dir) = create_test_service().await;

        let bucket = "test-bucket";
        let object_key = "test-object";
        let data = b"Hello, SSE-C!";
        let reader = Cursor::new(data.to_vec());
        let customer_key = [0u8; 32]; // 256-bit key

        // Encrypt with SSE-C
        let result = service
            .encrypt_object_with_customer_key(bucket, object_key, reader, &customer_key, None)
            .await
            .expect("SSE-C encryption failed");

        assert!(!result.ciphertext.is_empty());
        assert_eq!(result.metadata.key_id, "sse-c");
        assert_eq!(result.metadata.original_size, data.len() as u64);

        // Decrypt with same customer key
        let decrypted_reader = service
            .decrypt_object_with_customer_key(bucket, object_key, result.ciphertext, &result.metadata, &customer_key)
            .await
            .expect("SSE-C decryption failed");

        let mut decrypted_data = Vec::new();
        let mut reader = decrypted_reader;
        reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to read decrypted data");
        assert_eq!(decrypted_data, data);
    }

    #[tokio::test]
    async fn test_metadata_headers_conversion() {
        let (service, _temp_dir) = create_test_service().await;

        let metadata = EncryptionMetadata {
            algorithm: "AES256".to_string(),
            key_id: "test-key".to_string(),
            key_version: 1,
            iv: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            tag: Some(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            encryption_context: HashMap::from([("bucket".to_string(), "test-bucket".to_string())]),
            encrypted_at: Zoned::now(),
            original_size: 100,
            encrypted_data_key: vec![1, 2, 3, 4],
        };

        // Convert to headers
        let headers = service.metadata_to_headers(&metadata);
        assert!(headers.contains_key("x-amz-server-side-encryption"));
        assert!(headers.contains_key("x-rustfs-encryption-iv"));

        // Convert back to metadata
        let parsed_metadata = service.headers_to_metadata(&headers).expect("Failed to parse headers");
        assert_eq!(parsed_metadata.algorithm, metadata.algorithm);
        assert_eq!(parsed_metadata.key_id, metadata.key_id);
        assert_eq!(parsed_metadata.iv, metadata.iv);
        assert_eq!(parsed_metadata.tag, metadata.tag);
    }

    #[tokio::test]
    async fn test_encryption_context_validation() {
        let (service, _temp_dir) = create_test_service().await;

        let actual_context = HashMap::from([
            ("bucket".to_string(), "test-bucket".to_string()),
            ("object".to_string(), "test-object".to_string()),
        ]);

        let valid_expected = HashMap::from([("bucket".to_string(), "test-bucket".to_string())]);

        let invalid_expected = HashMap::from([("bucket".to_string(), "wrong-bucket".to_string())]);

        // Valid context should pass
        assert!(service.validate_encryption_context(&actual_context, &valid_expected).is_ok());

        // Invalid context should fail
        assert!(
            service
                .validate_encryption_context(&actual_context, &invalid_expected)
                .is_err()
        );
    }
}
