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
//! A JSON-serialized `DataKeyEnvelope` carrying the AES-256-GCM ciphertext, the
//! 12-byte nonce and the authenticated encryption context. This is a
//! RustFS-internal format; it is not interchangeable with MinIO's KMS
//! ciphertext (see the note on `StaticConfig`).

use crate::backends::{BackendCapabilities, KmsBackend, empty_key_page, list_keys_page_size};
use crate::config::{BackendConfig, KmsConfig};
use crate::encryption::{DataKeyEnvelope, context_aad, generate_key_material};
use crate::error::{KmsError, Result};
use crate::types::*;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
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
    /// When this backend was constructed, reported as the key's creation date.
    ///
    /// A statically configured key has no creation event to report, but the
    /// value still has to be *stable*: reading it from the clock on each call
    /// made `describe_key` and `list_keys` answer differently every time, so no
    /// caller could diff an inventory or cache a description.
    ///
    /// The stability this buys is per process. The reported date still moves
    /// across a restart and differs between nodes of one cluster, because there
    /// is no creation event to anchor it to; callers must treat it as "when this
    /// node loaded the key", not as the key's birth date.
    created_at: Zoned,
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
            created_at: Zoned::now(),
        })
    }

    /// Build the key metadata for the single configured key.
    fn key_metadata(&self) -> KeyMetadata {
        KeyMetadata {
            key_id: self.key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("Static single-key KMS backend".to_string()),
            creation_date: self.created_at.clone(),
            deletion_date: None,
            origin: "EXTERNAL".to_string(),
            key_manager: "STATIC".to_string(),
            tags: HashMap::new(),
        }
    }
}

impl StaticKmsBackend {
    /// Generate a fresh data key and wrap it in the standard KMS envelope,
    /// authenticated against the canonical encryption context.
    pub(crate) fn generate_data_key_envelope(&self, request: &GenerateKeyRequest) -> Result<DataKeyInfo> {
        if request.master_key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.master_key_id));
        }

        // Generate 12-byte random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill(&mut nonce_bytes[..]);

        // The requested spec decides the DEK length; a caller that asked for
        // AES_128 and silently got 256 bits would build objects whose recorded
        // spec does not match their key material.
        let plaintext = generate_key_material(&request.key_spec)?;

        // Encrypt DEK with AES-256-GCM using the static key directly
        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from(nonce_bytes);
        let aad = context_aad(&request.encryption_context)?;

        let encrypted = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext.as_slice(),
                    aad: &aad,
                },
            )
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM encrypt", e.to_string()))?;

        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key: encrypted,
            nonce: nonce_bytes.to_vec(),
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            // The static backend has a single fixed key with no rotation.
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope)?;

        Ok(DataKeyInfo::new(
            self.key_id.clone(),
            0,
            Some(plaintext),
            ciphertext,
            request.key_spec.clone(),
        ))
    }

    /// Encrypt caller-provided plaintext into the standard KMS envelope.
    pub(crate) fn encrypt_to_envelope(&self, request: &EncryptRequest) -> Result<EncryptResponse> {
        if request.key_id != self.key_id {
            return Err(KmsError::key_not_found(&request.key_id));
        }

        // Generate 12-byte random nonce
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill(&mut nonce_bytes[..]);

        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from(nonce_bytes);
        let aad = context_aad(&request.encryption_context)?;

        let encrypted = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: request.plaintext.as_ref(),
                    aad: &aad,
                },
            )
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM encrypt", e.to_string()))?;

        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.key_id.clone(),
            key_spec: "AES_256".to_string(),
            encrypted_key: encrypted,
            nonce: nonce_bytes.to_vec(),
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            // The static backend has a single fixed key with no rotation.
            master_key_version: None,
        };
        let ciphertext = serde_json::to_vec(&envelope)?;

        Ok(EncryptResponse {
            ciphertext,
            key_id: self.key_id.clone(),
            key_version: 0,
            algorithm: "AES-256-GCM".to_string(),
        })
    }

    /// Open a KMS envelope produced by this backend.
    pub(crate) fn decrypt_envelope(&self, request: &DecryptRequest) -> Result<Vec<u8>> {
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|error| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {error}")))?;
        if envelope.master_key_id != self.key_id {
            return Err(KmsError::key_not_found(&envelope.master_key_id));
        }

        for (key, expected_value) in &envelope.encryption_context {
            match request.encryption_context.get(key) {
                Some(actual_value) if actual_value == expected_value => {}
                Some(actual_value) => {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
                None if request.encryption_context.is_empty() => {}
                None => return Err(KmsError::context_mismatch(format!("Missing context key '{key}'"))),
            }
        }

        let key = Key::<Aes256Gcm>::from(*self.key);
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::try_from(envelope.nonce.as_slice())
            .map_err(|_| KmsError::cryptographic_error("nonce", "invalid nonce length"))?;
        let aad = context_aad(&envelope.encryption_context)?;

        let plaintext = cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: envelope.encrypted_key.as_ref(),
                    aad: &aad,
                },
            )
            .map_err(|e| KmsError::cryptographic_error("AES-256-GCM decrypt", e.to_string()))?;

        Ok(plaintext)
    }

    /// Describe the single configured key.
    pub(crate) fn configured_key_info(&self, key_id: &str) -> Result<KeyInfo> {
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
            rotation_due: false,
            rotation_due_reason: None,
            wrap_budget_reserved: None,
        })
    }

    /// List the single configured key, honouring the pagination marker and the
    /// status and usage filters.
    pub(crate) fn list_configured_key(&self, request: &ListKeysRequest) -> Result<ListKeysResponse> {
        // A caller asking for no keys gets none, even from a backend whose
        // whole key set is one key.
        if list_keys_page_size(request.limit).is_none() {
            return Ok(empty_key_page());
        }

        // Built through the same constructor `describe_key` uses, so a listed
        // key and a described key can never drift apart.
        let key_info = self.configured_key_info(&self.key_id)?;

        // The marker is an exclusive lower bound on the identifier, as it is
        // for every other backend.
        if let Some(ref marker) = request.marker
            && self.key_id <= *marker
        {
            return Ok(empty_key_page());
        }

        // The configured key is filtered like any other: a caller narrowing the
        // listing to disabled or signing keys must get an empty page rather
        // than this active encryption key, which it would otherwise have to
        // recognise as a non-match on its own.
        if request
            .status_filter
            .as_ref()
            .is_some_and(|status| status != &key_info.status)
            || request.usage_filter.as_ref().is_some_and(|usage| usage != &key_info.usage)
        {
            return Ok(empty_key_page());
        }

        Ok(ListKeysResponse {
            keys: vec![key_info],
            next_marker: None,
            truncated: false,
            // The configured key is described from memory, so it can never be
            // present-but-unreadable.
            unreadable_key_ids: Vec::new(),
        })
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
        self.encrypt_to_envelope(&request)
    }

    async fn decrypt(&self, request: DecryptRequest) -> Result<DecryptResponse> {
        let key_id = self.key_id.clone();
        let plaintext = self.decrypt_envelope(&request)?;
        Ok(DecryptResponse {
            plaintext,
            key_id,
            encryption_algorithm: Some("AES-256-GCM".to_string()),
        })
    }

    async fn generate_data_key(&self, request: GenerateDataKeyRequest) -> Result<GenerateDataKeyResponse> {
        let gen_req = GenerateKeyRequest {
            master_key_id: request.key_id.clone(),
            key_spec: request.key_spec.as_str().to_string(),
            key_length: None,
            encryption_context: request.encryption_context,
            grant_tokens: Vec::new(),
        };
        let mut data_key = self.generate_data_key_envelope(&gen_req)?;

        // Fields are taken, not destructured or cloned: `DataKeyInfo` has a
        // `Drop` impl, and a clone would leave a second un-zeroized plaintext
        // DEK on the heap.
        let plaintext_key = data_key
            .plaintext
            .take()
            .ok_or_else(|| KmsError::internal_error("Generated data key is missing plaintext"))?;

        Ok(GenerateDataKeyResponse {
            key_id: std::mem::take(&mut data_key.key_id),
            plaintext_key,
            ciphertext_blob: std::mem::take(&mut data_key.ciphertext),
        })
    }

    async fn describe_key(&self, request: DescribeKeyRequest) -> Result<DescribeKeyResponse> {
        let key_info = self.configured_key_info(&request.key_id)?;
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
        self.list_configured_key(&request)
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

    fn capabilities(&self) -> BackendCapabilities {
        // Static KMS is a read-only single-key backend: it only performs
        // cryptographic operations and rejects every lifecycle mutation.
        BackendCapabilities::minimal()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::KmsBackend as KmsBackendTrait;
    use crate::config::{BackendConfig, KmsBackend, StaticConfig};
    use crate::encryption::is_data_key_envelope;
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
    async fn metadata_updates_report_the_capability_gap() {
        let (backend, key_id, _key) = create_test_backend().await;
        assert!(
            !backend.capabilities().update_key_metadata,
            "Static KMS owns no mutable key record, so it must not advertise metadata updates"
        );

        for (operation, result) in [
            ("update_key_description", backend.update_key_description(&key_id, Some("new")).await),
            (
                "tag_key",
                backend
                    .tag_key(&key_id, &HashMap::from([("team".to_string(), "storage".to_string())]))
                    .await,
            ),
            ("untag_key", backend.untag_key(&key_id, &["team".to_string()]).await),
        ] {
            let error = result.expect_err("Static KMS is read-only: metadata updates must be rejected");
            assert!(
                matches!(error, KmsError::UnsupportedCapability { .. }),
                "expected UnsupportedCapability for {operation}, got {error:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_generate_and_decrypt_data_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Generate data key
        let request = GenerateKeyRequest::new(key_id.clone(), "AES_256".to_string())
            .with_context("bucket".to_string(), "test-bucket".to_string());
        let data_key = backend
            .generate_data_key_envelope(&request)
            .expect("Failed to generate data key");

        assert_eq!(data_key.key_id, key_id);
        assert_eq!(data_key.version, 0);
        assert!(data_key.plaintext.is_some());
        assert_eq!(data_key.plaintext.as_ref().expect("plaintext should be set").len(), 32);
        let envelope: DataKeyEnvelope =
            serde_json::from_slice(&data_key.ciphertext).expect("static data key should use a KMS envelope");
        assert_eq!(envelope.master_key_id, key_id);
        assert_eq!(envelope.encrypted_key.len(), 32 + 16);
        assert_eq!(envelope.nonce.len(), NONCE_SIZE);
        assert_eq!(envelope.encryption_context.get("bucket").map(String::as_str), Some("test-bucket"));

        // Decrypt the data key
        let decrypt_request =
            DecryptRequest::new(data_key.ciphertext.clone()).with_context("bucket".to_string(), "test-bucket".to_string());
        let decrypted = backend.decrypt_envelope(&decrypt_request).expect("Failed to decrypt");

        assert_eq!(decrypted.as_slice(), data_key.plaintext.as_deref().expect("plaintext should exist"));
    }

    #[tokio::test]
    async fn generated_data_key_uses_kms_envelope_for_sse_read_routing() {
        let (backend, key_id, _key) = create_test_backend().await;
        let request = GenerateKeyRequest::new(key_id, "AES_256".to_string())
            .with_context("bucket".to_string(), "source-bucket".to_string())
            .with_context("object".to_string(), "source-object".to_string());

        let data_key = backend
            .generate_data_key_envelope(&request)
            .expect("generate static KMS data key");

        assert!(
            is_data_key_envelope(&data_key.ciphertext),
            "static KMS ciphertext must use the KMS envelope recognized by the SSE read path"
        );
    }

    #[tokio::test]
    async fn generated_data_key_rejects_a_different_encryption_context() {
        let (backend, key_id, _key) = create_test_backend().await;
        let generate_request = GenerateDataKeyRequest {
            key_id,
            key_spec: KeySpec::Aes256,
            encryption_context: HashMap::from([
                ("bucket".to_string(), "source-bucket".to_string()),
                ("object".to_string(), "source-object".to_string()),
            ]),
        };
        let generated = KmsBackendTrait::generate_data_key(&backend, generate_request)
            .await
            .expect("generate context-bound static KMS data key");
        let decrypt_request = DecryptRequest {
            ciphertext: generated.ciphertext_blob,
            encryption_context: HashMap::from([
                ("bucket".to_string(), "different-bucket".to_string()),
                ("object".to_string(), "different-object".to_string()),
            ]),
            grant_tokens: Vec::new(),
        };

        let error = KmsBackendTrait::decrypt(&backend, decrypt_request)
            .await
            .expect_err("a static KMS data key must not decrypt under a different object context");

        assert!(matches!(error, KmsError::ContextMismatch { .. }));
    }

    #[tokio::test]
    async fn generated_data_key_rejects_tampered_envelope_context() {
        let (backend, key_id, _key) = create_test_backend().await;
        let request = GenerateKeyRequest::new(key_id, "AES_256".to_string())
            .with_context("bucket".to_string(), "source-bucket".to_string());
        let generated = backend
            .generate_data_key_envelope(&request)
            .expect("generate context-bound data key");
        let mut envelope: DataKeyEnvelope = serde_json::from_slice(&generated.ciphertext).expect("parse static KMS envelope");
        envelope
            .encryption_context
            .insert("bucket".to_string(), "different-bucket".to_string());
        let decrypt_request = DecryptRequest::new(serde_json::to_vec(&envelope).expect("serialize tampered envelope"))
            .with_context("bucket".to_string(), "different-bucket".to_string());

        let error = backend
            .decrypt_envelope(&decrypt_request)
            .expect_err("tampering with authenticated envelope context must fail");

        assert!(matches!(error, KmsError::CryptographicError { .. }));
    }

    #[tokio::test]
    async fn test_generate_data_key_wrong_key_id() {
        let (backend, _key_id, _key) = create_test_backend().await;

        let request = GenerateKeyRequest::new("wrong-key-id".to_string(), "AES_256".to_string());
        let result = backend.generate_data_key_envelope(&request);
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("wrong-key-id"));
    }

    #[tokio::test]
    async fn test_decrypt_invalid_ciphertext() {
        let (backend, _key_id, _key) = create_test_backend().await;

        // Ciphertext too short
        let short = vec![0u8; 10];
        let request = DecryptRequest::new(short);
        let result = backend.decrypt_envelope(&request);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_decrypt_tampered_ciphertext() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Generate a valid ciphertext first
        let gen_request = GenerateKeyRequest::new(key_id, "AES_256".to_string());
        let data_key = backend.generate_data_key_envelope(&gen_request).expect("generate");

        // Tamper with the ciphertext (flip a bit in the encrypted portion)
        let mut tampered = data_key.ciphertext.clone();
        if !tampered.is_empty() {
            tampered[0] ^= 0x01;
        }

        let request = DecryptRequest::new(tampered);
        let result = backend.decrypt_envelope(&request);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_key_returns_exists_for_configured_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        // Creating the pre-configured key should return KeyAlreadyExists
        let result = KmsBackendTrait::create_key(
            &backend,
            CreateKeyRequest {
                key_name: Some(key_id.clone()),
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("already exists"));
    }

    #[tokio::test]
    async fn test_create_key_returns_error_for_other_keys() {
        let (backend, _key_id, _key) = create_test_backend().await;

        // Creating any other key should return invalid operation (read-only)
        let result = KmsBackendTrait::create_key(
            &backend,
            CreateKeyRequest {
                key_name: Some("other-key".to_string()),
                ..Default::default()
            },
        )
        .await;
        assert!(result.is_err());
        let err_msg = result.expect_err("should be Err").to_string();
        assert!(err_msg.contains("read-only") || err_msg.contains("cannot create"));
    }

    #[tokio::test]
    async fn test_describe_key() {
        let (backend, key_id, _key) = create_test_backend().await;

        let key_info = backend.configured_key_info(&key_id).expect("describe_key should succeed");
        assert_eq!(key_info.key_id, key_id);
        assert_eq!(key_info.status, KeyStatus::Active);
        assert_eq!(key_info.algorithm, "AES_256");

        // Wrong key ID
        let result = backend.configured_key_info("nonexistent");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_keys() {
        let (backend, key_id, _key) = create_test_backend().await;

        let response = backend
            .list_configured_key(&ListKeysRequest::default())
            .expect("list_keys should succeed");
        assert_eq!(response.keys.len(), 1);
        assert_eq!(response.keys[0].key_id, key_id);
        assert!(!response.truncated);
    }

    /// A zero limit means zero keys, even for a backend whose whole key set is
    /// a single configured key.
    #[tokio::test]
    async fn zero_limit_list_returns_an_empty_page() {
        let (backend, _key_id, _key) = create_test_backend().await;

        let response = backend
            .list_configured_key(&ListKeysRequest {
                limit: Some(0),
                ..Default::default()
            })
            .expect("a zero-limit list must succeed");
        assert!(response.keys.is_empty());
        assert!(!response.truncated);
        assert!(response.next_marker.is_none());
    }

    /// A filter that excludes the configured key must empty the page. Handing
    /// the key back regardless would answer "list the disabled keys" with an
    /// active one, and the response says nothing about the filter having been
    /// dropped.
    #[tokio::test]
    async fn a_filter_the_configured_key_does_not_match_empties_the_page() {
        let (backend, key_id, _key) = create_test_backend().await;

        for request in [
            ListKeysRequest {
                status_filter: Some(KeyStatus::Disabled),
                ..Default::default()
            },
            ListKeysRequest {
                usage_filter: Some(KeyUsage::SignVerify),
                ..Default::default()
            },
        ] {
            let response = backend.list_configured_key(&request).expect("a filtered list must succeed");
            assert!(response.keys.is_empty(), "excluded key was listed for {request:?}");
            assert!(!response.truncated);
            assert!(response.next_marker.is_none());
        }

        // The filters the key does match still list it.
        let response = backend
            .list_configured_key(&ListKeysRequest {
                status_filter: Some(KeyStatus::Active),
                usage_filter: Some(KeyUsage::EncryptDecrypt),
                ..Default::default()
            })
            .expect("a matching list must succeed");
        assert_eq!(response.keys.len(), 1);
        assert_eq!(response.keys[0].key_id, key_id);
    }

    #[tokio::test]
    async fn lifecycle_mutations_are_unsupported_at_the_product_surface() {
        let (backend, key_id, _key) = create_test_backend().await;

        // The static backend advertises no enable/disable or rotation
        // capability, so the shared KmsBackend defaults reject all three.
        for result in [
            KmsBackendTrait::enable_key(&backend, &key_id).await,
            KmsBackendTrait::disable_key(&backend, &key_id).await,
            KmsBackendTrait::rotate_key(&backend, &key_id).await,
        ] {
            let error = result.expect_err("static lifecycle mutations must be rejected");
            assert!(matches!(error, KmsError::UnsupportedCapability { .. }), "got {error:?}");
        }
    }

    #[tokio::test]
    async fn test_delete_key_returns_error() {
        let (backend, key_id, _key) = create_test_backend().await;

        let result = KmsBackendTrait::delete_key(
            &backend,
            DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: None,
                confirm_key_id: None,
            },
        )
        .await;
        assert!(result.is_err());
        assert!(result.expect_err("should be Err").to_string().contains("read-only"));
    }

    #[tokio::test]
    async fn test_health_check() {
        let (backend, _key_id, _key) = create_test_backend().await;

        KmsBackendTrait::health_check(&backend)
            .await
            .expect("health_check should succeed");
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_direct() {
        let (backend, key_id, _key) = create_test_backend().await;

        let plaintext = b"Hello, static KMS world!";
        let enc_request = EncryptRequest::new(key_id.clone(), plaintext.to_vec());
        let enc_response = backend.encrypt_to_envelope(&enc_request).expect("encrypt should succeed");

        assert_eq!(enc_response.key_id, key_id);
        assert!(!enc_response.ciphertext.is_empty());

        let dec_request = DecryptRequest::new(enc_response.ciphertext);
        let decrypted = backend.decrypt_envelope(&dec_request).expect("decrypt should succeed");

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

    /// The configured key's reported creation date must not move within a
    /// process.
    ///
    /// It used to be read from the clock inside every describe and every list,
    /// so two reads of the same unchanged key disagreed and no caller could
    /// diff an inventory or cache a description. It also made `describe_key`
    /// and `list_keys` report different dates for the one key that exists.
    /// Stability across restarts and across nodes is not claimed — see the
    /// field's own doc for why there is nothing to anchor it to.
    #[tokio::test]
    async fn configured_key_reports_a_stable_creation_date_within_a_process() {
        let (backend, key_id, _key) = create_test_backend().await;

        let first = KmsBackendTrait::describe_key(&backend, DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe_key should succeed")
            .key_metadata
            .creation_date;
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let second = KmsBackendTrait::describe_key(&backend, DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe_key should succeed")
            .key_metadata
            .creation_date;
        assert_eq!(first, second, "describing the same unchanged key twice must report one date");

        let listed = KmsBackendTrait::list_keys(&backend, ListKeysRequest::default())
            .await
            .expect("list_keys should succeed");
        let listed = listed.keys.first().expect("the configured key must be listed");
        assert_eq!(listed.created_at, first, "the listed key must report the described date");
    }
}
