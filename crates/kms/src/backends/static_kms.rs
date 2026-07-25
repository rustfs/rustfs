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

//! Static single-key KMS backend implementation
//!
//! This backend holds one pre-configured AES-256 key and uses it directly
//! to encrypt/decrypt data encryption keys (DEKs) via AES-256-GCM.
//!
//! ## Ciphertext format
//!
//! encrypted_data(plaintext_len+16) || nonce (12 bytes)

use crate::backends::{BackendInfo, KmsBackend, KmsClient};
use crate::config::{BackendConfig, KmsConfig};
use crate::error::{KmsError, Result};
use crate::types::*;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use jiff::Zoned;
use rand::RngExt;
use std::collections::HashMap;
use tracing::debug;
use zeroize::Zeroizing;

/// AES-GCM nonce size in bytes, appended to each ciphertext.
const NONCE_SIZE: usize = 12;
/// AES-256 key size in bytes.
const KEY_SIZE: usize = 32;

/// Static single-key KMS backend.
///
/// Uses a pre-configured AES-256 key to derive data encryption keys. This is a
/// read-only backend: it cannot create, delete, or rotate keys.
pub struct StaticKmsBackend {
    /// The configured key identifier (name).
    key_id: String,
    /// The raw 32-byte AES-256 key material (zeroed on drop).
    key: Zeroizing<[u8; KEY_SIZE]>,
}

impl StaticKmsBackend {
    /// Create a new static KMS backend from configuration.
    pub async fn new(mut config: KmsConfig) -> Result<Self> {
        let BackendConfig::Static(ref mut static_config) = config.backend_config else {
            return Err(KmsError::configuration_error("Static KMS backend requires StaticConfig"));
        };

        let key = static_config.decode_key()?;

        // Zeroize the base64-encoded secret key in the config after extracting the raw key.
        use zeroize::Zeroize;
        static_config.secret_key.zeroize();

        debug!(
            key_id = %static_config.key_id,
            "Static KMS backend initialized"
        );

        Ok(Self {
            key_id: static_config.key_id.clone(),
            key: key.into(),
        })
    }

    /// Build the key metadata for the single configured key.
    fn key_metadata(&self) -> KeyMetadata {
        KeyMetadata {
            key_id: self.key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Static single-key KMS backend".to_string()),
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "EXTERNAL".to_string(),
            key_manager: "STATIC".to_string(),
            tags: HashMap::new(),
        }
    }
}

#[async_trait]
impl KmsClient for StaticKmsBackend {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKeyInfo> {
        if request.master_key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.master_key_id));
        }

        // Generate 12-byte random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill(&mut nonce_bytes[..]);

        // Generate 32 random bytes as plaintext DEK
        let mut plaintext = [0u8; KEY_SIZE];
        rand::rng().fill(&mut plaintext[..]);

        // Encrypt DEK with AES-256-GCM using the static key directly
        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from(nonce_bytes);

        let encrypted = cipher
            .encrypt(&nonce, plaintext.as_ref())
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM encrypt", e.to_string()))?;

        // Ciphertext format: encrypted_dek || nonce
        let mut ciphertext = encrypted;
        ciphertext.extend_from_slice(&nonce_bytes);

        Ok(DataKeyInfo::new(
            self.key_id.clone(),
            0, // version is always 0 for static KMS
            Some(plaintext.to_vec()),
            ciphertext,
            "AES_256".to_string(),
        ))
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        if request.key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.key_id));
        }

        // Generate 12-byte random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill(&mut nonce_bytes[..]);

        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from(nonce_bytes);

        let encrypted = cipher
            .encrypt(&nonce, request.plaintext.as_ref())
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM encrypt", e.to_string()))?;

        // Ciphertext format: encrypted_data || nonce
        let mut ciphertext = encrypted;
        ciphertext.extend_from_slice(&nonce_bytes);

        Ok(EncryptResponse {
            ciphertext,
            key_id: self.key_id.clone(),
            key_version: 0,
            algorithm: "AES-256-GCM".to_string(),
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        if request.ciphertext.len() < NONCE_SIZE + 1 {
            return Err(KmsError::cryptographic_error("decrypt", "Ciphertext too short for static KMS format"));
        }

        // Split ciphertext: encrypted_data || nonce(12)
        let split_at = request.ciphertext.len() - NONCE_SIZE;
        let encrypted = &request.ciphertext[..split_at];
        let nonce_slice = &request.ciphertext[split_at..];

        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::try_from(nonce_slice).map_err(|_| KmsError::cryptographic_error("nonce", "invalid nonce length"))?;

        let plaintext = cipher
            .decrypt(&nonce, encrypted)
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM decrypt", e.to_string()))?;

        Ok(plaintext)
    }

    async fn create_key(&self, key_id: &str, _algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        if key_id == self.key_id {
            return Err(KmsError::key_already_exists(key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot create new keys"))
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }

        let metadata = self.key_metadata();
        Ok(KeyInfo {
            key_id: metadata.key_id,
            description: metadata.description,
            algorithm: "AES_256".to_string(),
            usage: metadata.key_usage,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            created_at: metadata.creation_date,
            rotated_at: None,
            created_by: None,
        })
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        let key_info = KeyInfo {
            key_id: self.key_id.clone(),
            description: Some("Static single-key KMS backend".to_string()),
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            created_at: Zoned::now(),
            rotated_at: None,
            created_by: None,
        };

        // Apply prefix filter if provided
        if let Some(ref marker) = request.marker
            && self.key_id <= *marker
        {
            return Ok(ListKeysResponse {
                keys: vec![],
                next_marker: None,
                truncated: false,
            });
        }

        Ok(ListKeysResponse {
            keys: vec![key_info],
            next_marker: None,
            truncated: false,
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }
        // Static KMS key is always enabled
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot disable keys"))
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot schedule key deletion"))
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot cancel key deletion"))
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        if key_id != self.key_id {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot rotate keys"))
    }

    async fn health_check(&self) -> Result<()> {
        // Static KMS is always healthy if it was successfully initialized
        Ok(())
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo::new("static".to_string(), env!("CARGO_PKG_VERSION").to_string(), "local".to_string(), true)
            .with_metadata("key_id".to_string(), self.key_id.clone())
    }
}

#[async_trait]
impl KmsBackend for StaticKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_name = request.key_name.as_deref().unwrap_or("");
        if key_name == self.key_id {
            return Err(KmsError::key_already_exists(&self.key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot create new keys"))
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        <Self as KmsClient>::encrypt(self, &request, None).await
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let key_id = self.key_id.clone();
        let plaintext = <Self as KmsClient>::decrypt(self, &request, None).await?;
        Ok(DecryptResponse {
            plaintext,
            key_id,
            encryption_algorithm: Some("AES-256-GCM".to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let gen_req = GenerateKeyRequest::new(request.key_id.clone(), request.key_spec.as_str().to_string());
        let data_key = <Self as KmsClient>::generate_data_key(self, &gen_req, None).await?;

        let plaintext_key = data_key
            .plaintext
            .clone()
            .ok_or_else(|| KmsError::internal_error("Generated data key is missing plaintext"))?;

        Ok(GenerateDataKeyResponse {
            key_id: data_key.key_id.clone(),
            plaintext_key,
            ciphertext_blob: data_key.ciphertext.clone(),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        let key_info = <Self as KmsClient>::describe_key(self, &request.key_id, None).await?;
        let key_metadata = KeyMetadata {
            key_id: key_info.key_id.clone(),
            key_state: if key_info.status == KeyStatus::Active {
                KeyState::Enabled
            } else {
                KeyState::Disabled
            },
            key_usage: key_info.usage,
            description: key_info.description,
            creation_date: key_info.created_at,
            deletion_date: None,
            origin: "EXTERNAL".to_string(),
            key_manager: "STATIC".to_string(),
            tags: key_info.tags,
        };
        Ok(DescribeKeyResponse { key_metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        <Self as KmsClient>::list_keys(self, &request, None).await
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        if request.key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot delete keys"))
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        if request.key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.key_id));
        }
        Err(KmsError::invalid_operation("Static KMS is read-only: cannot cancel key deletion"))
    }

    async fn health_check(&self) -> Result<bool> {
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::KmsClient;
    use crate::config::{BackendConfig, KmsBackend, StaticConfig};
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;

    /// Generate a random 32-byte key and return (key_id, raw_key).
    fn random_static_key(key_id: &str) -> (String, [u8; 32]) {
        let mut key = [0u8; 32];
        rand::rng().fill(&mut key[..]);
        (key_id.to_string(), key)
    }

    fn static_config(key_id: &str, raw_key: &[u8; 32]) -> StaticConfig {
        StaticConfig {
            key_id: key_id.to_string(),
            secret_key: BASE64.encode(raw_key),
        }
    }

    fn kms_config(config: StaticConfig) -> KmsConfig {
        KmsConfig {
            backend: KmsBackend::Static,
            default_key_id: Some(config.key_id.clone()),
            backend_config: BackendConfig::Static(config),
            ..Default::default()
        }
    }

    async fn create_test_backend() -> (StaticKmsBackend, String, [u8; 32]) {
        let (key_id, key) = random_static_key("test-static-key");
        let config = static_config(&key_id, &key);
        let backend = StaticKmsBackend::new(kms_config(config))
            .await
            .expect("Failed to create backend");
        (backend, key_id, key)
    }

    #[tokio::test]
    async fn test_generate_and_decrypt_data_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Generate data key
        let request = GenerateKeyRequest::new(key_id.clone(), "AES_256".to_string())
            .with_context("bucket".to_string(), "test-bucket".to_string());
        let data_key = KmsClient::generate_data_key(&backend, &request, None)
            .await
            .expect("Failed to generate data key");

        assert_eq!(data_key.key_id, key_id);
        assert_eq!(data_key.version, 0);
        assert!(data_key.plaintext.is_some());
        assert_eq!(data_key.plaintext.as_ref().expect("plaintext should be set").len(), 32);
        // Ciphertext should be: encrypted(32) + tag(16) + nonce(12)
        assert_eq!(data_key.ciphertext.len(), 32 + 16 + NONCE_SIZE);

        // Decrypt the data key
        let decrypt_request =
            DecryptRequest::new(data_key.ciphertext.clone()).with_context("bucket".to_string(), "test-bucket".to_string());
        let decrypted = KmsClient::decrypt(&backend, &decrypt_request, None)
            .await
            .expect("Failed to decrypt");

        assert_eq!(decrypted.as_slice(), data_key.plaintext.as_deref().expect("plaintext should exist"));
    }

    #[tokio::test]
    async fn test_generate_data_key_wrong_key_id() {
        let (backend, _key_id, _key) = create_test_backend().await;

        let request = GenerateKeyRequest::new("wrong-key-id".to_string(), "AES_256".to_string());
        let result = KmsClient::generate_data_key(&backend, &request, None).await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("wrong-key-id"));
    }

    #[tokio::test]
    async fn test_decrypt_invalid_ciphertext() {
        let (backend, _key_id, _key) = create_test_backend().await;

        // Ciphertext too short
        let short = vec![0u8; 10];
        let request = DecryptRequest::new(short);
        let result = KmsClient::decrypt(&backend, &request, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decrypt_tampered_ciphertext() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Generate a valid ciphertext first
        let gen_request = GenerateKeyRequest::new(key_id, "AES_256".to_string());
        let data_key = KmsClient::generate_data_key(&backend, &gen_request, None)
            .await
            .expect("generate");

        // Tamper with the ciphertext (flip a bit in the encrypted portion)
        let mut tampered = data_key.ciphertext.clone();
        if !tampered.is_empty() {
            tampered[0] ^= 0x01;
        }

        let request = DecryptRequest::new(tampered);
        let result = KmsClient::decrypt(&backend, &request, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_key_returns_exists_for_configured_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Creating the pre-configured key should return KeyAlreadyExists
        let result = KmsClient::create_key(&backend, &key_id, "AES_256", None).await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_key_returns_error_for_other_keys() {
        let (backend, _key_id, _key) = create_test_backend().await;

        // Creating any other key should return invalid operation (read-only)
        let result = KmsClient::create_key(&backend, "other-key", "AES_256", None).await;
        assert!(result.is_err());
        let err_msg = result.expect_err("should be Err").to_string();
        assert!(err_msg.contains("read-only") || err_msg.contains("cannot create"));
    }

    #[tokio::test]
    async fn test_describe_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        let key_info = KmsClient::describe_key(&backend, &key_id, None)
            .await
            .expect("describe_key should succeed");
        assert_eq!(key_info.key_id, key_id);
        assert_eq!(key_info.status, KeyStatus::Active);
        assert_eq!(key_info.algorithm, "AES_256");

        // Wrong key ID
        let result = KmsClient::describe_key(&backend, "nonexistent", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_keys() {
        let (backend, key_id, _key) = create_test_backend().await;

        let response = KmsClient::list_keys(&backend, &ListKeysRequest::default(), None)
            .await
            .expect("list_keys should succeed");
        assert_eq!(response.keys.len(), 1);
        assert_eq!(response.keys[0].key_id, key_id);
        assert!(!response.truncated);
    }

    #[tokio::test]
    async fn test_disable_key_returns_error() {
        let (backend, key_id, _key) = create_test_backend().await;

        let result = KmsClient::disable_key(&backend, &key_id, None).await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn test_enable_key_is_noop() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Enable should succeed (no-op for static KMS)
        KmsClient::enable_key(&backend, &key_id, None)
            .await
            .expect("enable_key should be no-op");

        // Wrong key should still fail
        let result = KmsClient::enable_key(&backend, "wrong", None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_delete_key_returns_error() {
        let (backend, key_id, _key) = create_test_backend().await;

        let result = KmsClient::schedule_key_deletion(&backend, &key_id, 7, None).await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn test_rotate_key_returns_error() {
        let (backend, key_id, _key) = create_test_backend().await;

        let result = KmsClient::rotate_key(&backend, &key_id, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_health_check() {
        let (backend, _key_id, _key) = create_test_backend().await;

        KmsClient::health_check(&backend).await.expect("health_check should succeed");
    }

    #[tokio::test]
    async fn test_backend_info() {
        let (backend, key_id, _key) = create_test_backend().await;

        let info = KmsClient::backend_info(&backend);
        assert_eq!(info.backend_type, "static");
        assert_eq!(info.endpoint, "local");
        assert!(info.healthy);
        assert_eq!(info.metadata.get("key_id"), Some(&key_id));
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_direct() {
        let (backend, key_id, _key) = create_test_backend().await;

        let plaintext = b"Hello, static KMS world!";
        let enc_request = EncryptRequest::new(key_id.clone(), plaintext.to_vec());
        let enc_response = KmsClient::encrypt(&backend, &enc_request, None)
            .await
            .expect("encrypt should succeed");

        assert_eq!(enc_response.key_id, key_id);
        assert!(!enc_response.ciphertext.is_empty());

        let dec_request = DecryptRequest::new(enc_response.ciphertext);
        let decrypted = KmsClient::decrypt(&backend, &dec_request, None)
            .await
            .expect("decrypt should succeed");

        assert_eq!(decrypted, plaintext);
    }

    #[tokio::test]
    async fn test_kms_backend_trait_methods() {
        use crate::backends::KmsBackend;

        let (backend, key_id, _key) = create_test_backend().await;

        // Test KmsBackend::generate_data_key
        let gen_req = GenerateDataKeyRequest::new(key_id.clone(), KeySpec::Aes256);
        let gen_resp = KmsBackend::generate_data_key(&backend, gen_req)
            .await
            .expect("KmsBackend::generate_data_key should succeed");
        assert_eq!(gen_resp.key_id, key_id);
        assert_eq!(gen_resp.plaintext_key.len(), 32);
        assert!(!gen_resp.ciphertext_blob.is_empty());

        // Test KmsBackend::decrypt via the generated ciphertext
        let dec_req = DecryptRequest::new(gen_resp.ciphertext_blob);
        let dec_resp = KmsBackend::decrypt(&backend, dec_req)
            .await
            .expect("KmsBackend::decrypt should succeed");
        assert_eq!(dec_resp.plaintext, gen_resp.plaintext_key);

        // Test KmsBackend::describe_key
        let desc_req = DescribeKeyRequest { key_id: key_id.clone() };
        let desc_resp = KmsBackend::describe_key(&backend, desc_req)
            .await
            .expect("describe_key should succeed");
        assert_eq!(desc_resp.key_metadata.key_id, key_id);

        // Test KmsBackend::create_key for the configured key (should return KeyAlreadyExists, same as KmsClient)
        let create_req = CreateKeyRequest {
            key_name: Some(key_id.clone()),
            ..Default::default()
        };
        let create_err = KmsBackend::create_key(&backend, create_req)
            .await
            .expect_err("create_key for configured key should return KeyAlreadyExists");
        assert!(create_err.to_string().contains("already exists"));
    }
}
