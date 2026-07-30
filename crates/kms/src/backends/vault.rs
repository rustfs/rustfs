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

//! Vault-based KMS backend implementation using vaultrs

use crate::backends::vault_credentials::{VaultClientHandle, VaultConnectionSettings, VaultCredentialProvider, token_source_for};
use crate::backends::{BackendInfo, KmsBackend, KmsClient};
use crate::config::{KmsConfig, VaultConfig};
use crate::encryption::{AesDekCrypto, DataKeyEnvelope, DekCrypto, generate_key_material};
use crate::error::{KmsError, Result};
use crate::types::*;
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use jiff::Zoned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};
use vaultrs::{api::kv2::requests::SetSecretRequestOptions, error::ClientError, kv2};

/// Vault KMS client implementation
pub struct VaultKmsClient {
    credentials: VaultCredentialProvider,
    config: VaultConfig,
    /// Mount path for the KV engine (typically "kv" or "secret")
    kv_mount: String,
    /// Path prefix for storing keys
    key_path_prefix: String,
    /// DEK encryption implementation
    dek_crypto: AesDekCrypto,
}

/// Key data stored in Vault
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VaultKeyData {
    /// Key algorithm
    algorithm: String,
    /// Key usage type
    usage: KeyUsage,
    /// Key creation timestamp
    created_at: Zoned,
    /// Key status
    status: KeyStatus,
    /// Key version
    version: u32,
    /// Key description
    description: Option<String>,
    /// Key metadata
    metadata: HashMap<String, String>,
    /// Key tags
    tags: HashMap<String, String>,
    /// Encrypted key material (base64 encoded)
    encrypted_key_material: String,
    /// Version that pre-versioning envelopes (no `master_key_version`) resolve to.
    ///
    /// Recorded once, at the key's first rotation, when the then-current material is
    /// frozen as an immutable version record. `None` means the key has never been
    /// rotated, so legacy envelopes keep resolving to the current version — exactly
    /// the pre-versioning behavior. Optional so records written by older builds keep
    /// deserializing.
    #[serde(default)]
    baseline_version: Option<u32>,
}

/// Immutable per-version master key material record stored under
/// `{prefix}/{key_id}/versions/{N}`.
///
/// Version records are created with a KV2 check-and-set of 0 (create-only) and are
/// never rewritten, so every master key version that ever wrapped a DEK stays
/// readable after rotation. The top-level `{prefix}/{key_id}` record keeps a copy of
/// the current material as a fast path and so binaries that predate versioned
/// storage can still read never-rotated keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VaultKeyVersionRecord {
    /// Master key version this record holds material for
    version: u32,
    /// Encrypted key material (base64 encoded)
    encrypted_key_material: String,
    /// When this version's material was created
    created_at: Zoned,
}

/// Sub-path (under each key path) reserved for immutable version records.
const KEY_VERSIONS_SUBPATH: &str = "versions";

/// Drop KV2 directory entries from a key listing.
///
/// Once a key has version records, listing the key prefix returns both the key
/// record itself ("my-key") and a directory entry for its version sub-path
/// ("my-key/"); only the former is a key.
fn filter_key_directory_entries(keys: Vec<String>) -> Vec<String> {
    keys.into_iter().filter(|key| !key.ends_with('/')).collect()
}

/// Resolve which master key version wrapped an envelope.
///
/// `Some` versions are honored verbatim: if the record for that version is missing
/// the lookup must fail closed with [`KmsError::KeyVersionNotFound`], never fall
/// back to the current material. `None` (a pre-versioning envelope) resolves to the
/// key's baseline version — the deterministic version whose material was current
/// before the first rotation froze it — and, for keys that were never rotated and
/// thus have no baseline, to the current version, which matches pre-versioning
/// behavior exactly.
fn resolve_envelope_master_key_version(
    envelope_version: Option<u32>,
    baseline_version: Option<u32>,
    current_version: u32,
) -> u32 {
    envelope_version.or(baseline_version).unwrap_or(current_version)
}

/// Whether a KV2 write failed its check-and-set precondition.
fn is_cas_conflict(error: &ClientError) -> bool {
    matches!(
        error,
        ClientError::APIError { code: 400, errors } if errors.iter().any(|message| message.contains("check-and-set"))
    )
}

/// Decode and validate the stored master key material of a [`VaultKeyData`] record.
///
/// This is the single read-side gate for KV2 key material: missing or undecodable
/// material must fail closed with a typed error and must never be regenerated or
/// written back (regenerating would orphan every DEK wrapped by the original key).
/// Kept synchronous and free of Vault I/O so the poison matrix is unit-testable
/// without a live Vault.
fn decode_stored_key_material(key_id: &str, encrypted_material: &str) -> Result<Vec<u8>> {
    if encrypted_material.is_empty() {
        return Err(KmsError::material_missing(key_id));
    }

    // Mirrors `decrypt_key_material`: stored material is currently base64 without an
    // additional encryption layer.
    let key_material = general_purpose::STANDARD
        .decode(encrypted_material)
        .map_err(|e| KmsError::material_corrupt(key_id, format!("stored key material is not valid base64: {e}")))?;

    // Key material must be exactly 32 bytes for AES-256.
    if key_material.len() != 32 {
        return Err(KmsError::material_corrupt(
            key_id,
            format!("stored key material has invalid length ({} bytes, expected 32)", key_material.len()),
        ));
    }

    Ok(key_material)
}

impl VaultKmsClient {
    /// Create a new Vault KMS client
    ///
    /// `attempt_timeout` caps every HTTP request issued through this client.
    pub async fn new(config: VaultConfig, attempt_timeout: Duration) -> Result<Self> {
        let source = token_source_for(&config.auth_method)?;
        let settings = VaultConnectionSettings {
            address: config.address.clone(),
            namespace: config.namespace.clone(),
            attempt_timeout,
        };
        let credentials = VaultCredentialProvider::new(settings, source).await?;

        info!(address = %config.address, "Vault KMS backend connected");

        Ok(Self {
            credentials,
            kv_mount: config.kv_mount.clone(),
            key_path_prefix: config.key_path_prefix.clone(),
            config,
            dek_crypto: AesDekCrypto::new(),
        })
    }

    /// Snapshot the authenticated Vault client for a single request.
    ///
    /// Every Vault call takes its own snapshot so a credential rotation
    /// applies to subsequent calls without interrupting in-flight ones.
    fn vault(&self) -> Arc<VaultClientHandle> {
        self.credentials.current()
    }

    /// Get the full path for a key in Vault
    fn key_path(&self, key_id: &str) -> String {
        format!("{}/{}", self.key_path_prefix, key_id)
    }

    /// Get the path of the immutable record holding one version's material
    fn key_version_path(&self, key_id: &str, version: u32) -> String {
        format!("{}/{}/{}/{}", self.key_path_prefix, key_id, KEY_VERSIONS_SUBPATH, version)
    }

    /// Get the directory path holding a key's version records
    fn key_versions_dir(&self, key_id: &str) -> String {
        format!("{}/{}/{}", self.key_path_prefix, key_id, KEY_VERSIONS_SUBPATH)
    }

    /// Encode key material for KV2 storage.
    ///
    /// This is plain Base64 encoding, not encryption: the KV2 backend stores master key
    /// material as-is and relies on Vault ACLs plus KV2 at-rest encryption for
    /// confidentiality. Any identity with KV read access to the key path can recover the
    /// plaintext master key.
    async fn encrypt_key_material(&self, key_material: &[u8]) -> Result<String> {
        Ok(general_purpose::STANDARD.encode(key_material))
    }

    /// Decode key material from KV2 storage (plain Base64, see `encrypt_key_material`).
    async fn decrypt_key_material(&self, encrypted_material: &str) -> Result<Vec<u8>> {
        general_purpose::STANDARD
            .decode(encrypted_material)
            .map_err(|e| KmsError::cryptographic_error("decrypt", e.to_string()))
    }

    /// Read the immutable material record of one key version.
    ///
    /// A missing record fails closed with [`KmsError::KeyVersionNotFound`]; falling
    /// back to the current material would decrypt with the wrong key at best and
    /// mask a tampered envelope version at worst.
    async fn get_key_version_record(&self, key_id: &str, version: u32) -> Result<VaultKeyVersionRecord> {
        let path = self.key_version_path(key_id, version);

        let record: VaultKeyVersionRecord =
            kv2::read(&self.vault().client, &self.kv_mount, &path)
                .await
                .map_err(|e| match e {
                    ClientError::ResponseWrapError => KmsError::key_version_not_found(key_id, version),
                    ClientError::APIError { code: 404, .. } => KmsError::key_version_not_found(key_id, version),
                    _ => KmsError::backend_error(format!("Failed to read key version record from Vault: {e}")),
                })?;

        if record.version != version {
            return Err(KmsError::material_corrupt(
                key_id,
                format!("version record at {path} claims version {} instead of {version}", record.version),
            ));
        }

        Ok(record)
    }

    /// Load master key material for a specific key version.
    ///
    /// The top-level record is the authoritative copy for the current version (a
    /// never-rotated key has no version records at all); any other version must have
    /// an immutable version record.
    async fn get_key_material_for_version(&self, key_id: &str, key_data: &VaultKeyData, version: u32) -> Result<Vec<u8>> {
        let encrypted_material = if version == key_data.version {
            key_data.encrypted_key_material.clone()
        } else {
            self.get_key_version_record(key_id, version).await?.encrypted_key_material
        };

        decode_stored_key_material(key_id, &encrypted_material).inspect_err(|error| {
            warn!(key_id, version, %error, "Vault KMS key material failed validation");
        })
    }

    /// Read the key record together with the KV2 secret version holding it, so a
    /// later write can be check-and-set against exactly this snapshot.
    async fn get_key_data_versioned(&self, key_id: &str) -> Result<(u32, VaultKeyData)> {
        let path = self.key_path(key_id);

        let metadata = kv2::read_metadata(&self.vault().client, &self.kv_mount, &path)
            .await
            .map_err(|e| match e {
                ClientError::ResponseWrapError => KmsError::key_not_found(key_id),
                ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                _ => KmsError::backend_error(format!("Failed to read key metadata from Vault: {e}")),
            })?;
        let cas = u32::try_from(metadata.current_version)
            .map_err(|_| KmsError::backend_error(format!("KV2 secret version for key {key_id} exceeds u32")))?;

        // Read the exact secret version from the metadata to keep the (cas, data)
        // pair consistent even if another writer lands in between.
        let key_data: VaultKeyData = kv2::read_version(&self.vault().client, &self.kv_mount, &path, metadata.current_version)
            .await
            .map_err(|e| match e {
                ClientError::ResponseWrapError => KmsError::key_not_found(key_id),
                ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                _ => KmsError::backend_error(format!("Failed to read key from Vault: {e}")),
            })?;

        Ok((cas, key_data))
    }

    /// Check-and-set write of the key record.
    ///
    /// `cas` must match the KV2 secret version currently holding the record.
    /// Returns the secret version created by this write so a caller can chain
    /// further check-and-set writes.
    async fn cas_store_key_data(&self, key_id: &str, key_data: &VaultKeyData, cas: u32) -> Result<u32> {
        let path = self.key_path(key_id);

        let written =
            kv2::set_with_options(&self.vault().client, &self.kv_mount, &path, key_data, SetSecretRequestOptions { cas })
                .await
                .map_err(|e| {
                    if is_cas_conflict(&e) {
                        KmsError::invalid_operation(format!(
                            "Concurrent modification of key {key_id} detected, retry the rotation"
                        ))
                    } else {
                        KmsError::backend_error(format!("Failed to store key in Vault: {e}"))
                    }
                })?;

        u32::try_from(written.version)
            .map_err(|_| KmsError::backend_error(format!("KV2 secret version for key {key_id} exceeds u32")))
    }

    /// Create-only write of an immutable version record (KV2 check-and-set of 0).
    ///
    /// Returns `Ok(true)` when this call created the record and `Ok(false)` when a
    /// record already exists at that version; the caller decides whether the
    /// existing record is acceptable. The record is never overwritten.
    async fn try_create_key_version_record(&self, key_id: &str, record: &VaultKeyVersionRecord) -> Result<bool> {
        let path = self.key_version_path(key_id, record.version);

        match kv2::set_with_options(&self.vault().client, &self.kv_mount, &path, record, SetSecretRequestOptions { cas: 0 }).await
        {
            Ok(_) => Ok(true),
            Err(e) if is_cas_conflict(&e) => Ok(false),
            Err(e) => Err(KmsError::backend_error(format!("Failed to store key version record in Vault: {e}"))),
        }
    }

    /// Store key data in Vault
    async fn store_key_data(&self, key_id: &str, key_data: &VaultKeyData) -> Result<()> {
        let path = self.key_path(key_id);

        kv2::set(&self.vault().client, &self.kv_mount, &path, key_data)
            .await
            .map_err(|e| KmsError::backend_error(format!("Failed to store key in Vault: {e}")))?;

        debug!("Stored key {} in Vault at path {}", key_id, path);
        Ok(())
    }

    async fn store_key_metadata(&self, key_id: &str, request: &CreateKeyRequest) -> Result<()> {
        debug!("Storing key metadata for {}, input tags: {:?}", key_id, request.tags);

        // Get existing key data to preserve encrypted_key_material and other fields
        // This is called after create_key, so the key should already exist
        let existing_key_data = self.get_key_data(key_id).await?;

        // A key that was just created must already carry material; an empty value means
        // the create flow failed to persist it. Fail closed instead of minting replacement
        // material: silently generating a new key here would mask the broken create and
        // orphan any DEK already wrapped by a different copy of this key.
        if existing_key_data.encrypted_key_material.is_empty() {
            warn!(key_id, "Vault KMS key metadata missing encrypted key material");
            return Err(KmsError::material_missing(key_id));
        }

        // Update only the metadata fields, preserving the encrypted_key_material
        let key_data = VaultKeyData {
            algorithm: existing_key_data.algorithm.clone(),
            usage: request.key_usage.clone(),
            created_at: existing_key_data.created_at,
            status: existing_key_data.status,
            version: existing_key_data.version,
            description: request.description.clone(),
            metadata: existing_key_data.metadata.clone(),
            tags: request.tags.clone(),
            encrypted_key_material: existing_key_data.encrypted_key_material.clone(), // Preserve the key material
            baseline_version: existing_key_data.baseline_version,
        };

        debug!(
            "VaultKeyData tags before storage: {:?}, encrypted_key_material length: {}",
            key_data.tags,
            key_data.encrypted_key_material.len()
        );
        self.store_key_data(key_id, &key_data).await
    }

    /// Retrieve key data from Vault
    async fn get_key_data(&self, key_id: &str) -> Result<VaultKeyData> {
        let path = self.key_path(key_id);

        let secret: VaultKeyData = kv2::read(&self.vault().client, &self.kv_mount, &path)
            .await
            .map_err(|e| match e {
                vaultrs::error::ClientError::ResponseWrapError => KmsError::key_not_found(key_id),
                vaultrs::error::ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                _ => KmsError::backend_error(format!("Failed to read key from Vault: {e}")),
            })?;

        debug!("Retrieved key {} from Vault, tags: {:?}", key_id, secret.tags);
        Ok(secret)
    }

    /// List all keys stored in Vault
    async fn list_vault_keys(&self) -> Result<Vec<String>> {
        // List keys under the prefix
        match kv2::list(&self.vault().client, &self.kv_mount, &self.key_path_prefix).await {
            Ok(keys) => {
                let keys = filter_key_directory_entries(keys);
                debug!("Found {} keys in Vault", keys.len());
                Ok(keys)
            }
            Err(vaultrs::error::ClientError::ResponseWrapError) => {
                // No keys exist yet
                Ok(Vec::new())
            }
            Err(vaultrs::error::ClientError::APIError { code: 404, .. }) => {
                // Path doesn't exist - no keys exist yet
                debug!("Key path doesn't exist in Vault (404), returning empty list");
                Ok(Vec::new())
            }
            Err(e) => Err(KmsError::backend_error(format!("Failed to list keys in Vault: {e}"))),
        }
    }

    /// Physically delete a key from Vault storage
    async fn delete_key(&self, key_id: &str) -> Result<()> {
        let path = self.key_path(key_id);

        // Purge immutable version records first: if any purge fails, the top-level
        // record still exists and the deletion can be retried. The reverse order
        // would leave orphaned master key material in Vault after the key vanished.
        let versions_dir = self.key_versions_dir(key_id);
        match kv2::list(&self.vault().client, &self.kv_mount, &versions_dir).await {
            Ok(versions) => {
                for version in versions {
                    let version_path = format!("{versions_dir}/{version}");
                    kv2::delete_metadata(&self.vault().client, &self.kv_mount, &version_path)
                        .await
                        .map_err(|e| KmsError::backend_error(format!("Failed to delete key version record from Vault: {e}")))?;
                }
            }
            // No version records exist (the key was never rotated).
            Err(ClientError::ResponseWrapError) | Err(ClientError::APIError { code: 404, .. }) => {}
            Err(e) => return Err(KmsError::backend_error(format!("Failed to list key version records in Vault: {e}"))),
        }

        // For this specific key path, we can safely delete the metadata
        // since each key has its own unique path under the prefix
        kv2::delete_metadata(&self.vault().client, &self.kv_mount, &path)
            .await
            .map_err(|e| match e {
                vaultrs::error::ClientError::APIError { code: 404, .. } => KmsError::key_not_found(key_id),
                _ => KmsError::backend_error(format!("Failed to delete key metadata from Vault: {e}")),
            })?;

        debug!("Permanently deleted key {} metadata from Vault at path {}", key_id, path);
        Ok(())
    }
}

#[async_trait]
impl KmsClient for VaultKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKeyInfo> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        // Generate random data key material using the existing method
        let plaintext_key = generate_key_material(&request.key_spec)?;

        // Encrypt the data key with the current master key material. Single read of
        // the key record: the material we wrap with and the version we stamp into
        // the envelope must come from the same snapshot, or a concurrent rotation
        // could stamp a version that never wrapped this DEK.
        let key_data = self.get_key_data(&request.master_key_id).await?;
        let key_material =
            decode_stored_key_material(&request.master_key_id, &key_data.encrypted_key_material).inspect_err(|error| {
                warn!(key_id = %request.master_key_id, %error, "Vault KMS key material failed validation");
            })?;
        let (encrypted_key, nonce) = self.dek_crypto.encrypt(&key_material, &plaintext_key).await?;

        // Create data key envelope with master key version for rotation support
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
            master_key_version: Some(key_data.version),
        };

        // Serialize the envelope as the ciphertext
        let ciphertext = serde_json::to_vec(&envelope)?;

        let data_key = DataKeyInfo::new(envelope.key_id, 1, Some(plaintext_key), ciphertext, request.key_spec.clone());

        debug!(key_id = %request.master_key_id, "Vault KMS data key generated");
        Ok(data_key)
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        // Get the master key
        let key_data = self.get_key_data(&request.key_id).await?;
        let key_material = self.decrypt_key_material(&key_data.encrypted_key_material).await?;

        // For simplicity, we'll use a basic encryption approach
        // In practice, you'd use proper AEAD encryption
        let mut ciphertext = request.plaintext.clone();
        for (i, byte) in ciphertext.iter_mut().enumerate() {
            *byte ^= key_material[i % key_material.len()];
        }

        Ok(EncryptResponse {
            ciphertext,
            key_id: request.key_id.clone(),
            key_version: key_data.version,
            algorithm: key_data.algorithm,
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting data");

        // Parse the data key envelope from ciphertext
        let envelope: DataKeyEnvelope = serde_json::from_slice(&request.ciphertext)
            .map_err(|e| KmsError::cryptographic_error("parse", format!("Failed to parse data key envelope: {e}")))?;

        // Verify encryption context matches
        // Check that all keys in envelope.encryption_context are present in request.encryption_context
        // and their values match. This ensures the context used for decryption matches what was used for encryption.
        for (key, expected_value) in &envelope.encryption_context {
            if let Some(actual_value) = request.encryption_context.get(key) {
                if actual_value != expected_value {
                    return Err(KmsError::context_mismatch(format!(
                        "Context mismatch for key '{key}': expected '{expected_value}', got '{actual_value}'"
                    )));
                }
            } else {
                // If request.encryption_context is empty, allow decryption (backward compatibility)
                // Otherwise, require all envelope context keys to be present
                if !request.encryption_context.is_empty() {
                    return Err(KmsError::context_mismatch(format!("Missing context key '{key}'")));
                }
            }
        }

        // Decrypt the data key with the master key version that wrapped it
        let key_data = self.get_key_data(&envelope.master_key_id).await?;
        let version =
            resolve_envelope_master_key_version(envelope.master_key_version, key_data.baseline_version, key_data.version);
        let key_material = self
            .get_key_material_for_version(&envelope.master_key_id, &key_data, version)
            .await?;
        let plaintext = self
            .dek_crypto
            .decrypt(&key_material, &envelope.encrypted_key, &envelope.nonce)
            .await?;

        debug!("Vault KMS data decrypted");
        Ok(plaintext)
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        debug!("Creating master key: {} with algorithm: {}", key_id, algorithm);

        // Check if key already exists
        if self.get_key_data(key_id).await.is_ok() {
            return Err(KmsError::key_already_exists(key_id));
        }

        // Generate key material
        let key_material = generate_key_material(algorithm)?;
        let encrypted_material = self.encrypt_key_material(&key_material).await?;

        // Create key data
        let key_data = VaultKeyData {
            algorithm: algorithm.to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            encrypted_key_material: encrypted_material,
            baseline_version: None,
        };

        // Store in Vault
        self.store_key_data(key_id, &key_data).await?;

        let master_key = MasterKeyInfo {
            key_id: key_id.to_string(),
            version: key_data.version,
            algorithm: key_data.algorithm.clone(),
            usage: key_data.usage,
            status: key_data.status,
            description: None, // This method doesn't receive description parameter
            metadata: key_data.metadata.clone(),
            created_at: key_data.created_at,
            rotated_at: None,
            created_by: None,
        };

        debug!(key_id, "Vault KMS master key created");
        Ok(master_key)
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let key_data = self.get_key_data(key_id).await?;

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            description: key_data.description,
            algorithm: key_data.algorithm,
            usage: key_data.usage,
            status: key_data.status,
            version: key_data.version,
            metadata: key_data.metadata,
            tags: key_data.tags,
            created_at: key_data.created_at,
            rotated_at: None,
            created_by: None,
        })
    }

    async fn list_keys(&self, request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys with limit: {:?}", request.limit);

        let all_keys = self.list_vault_keys().await?;
        let limit = request.limit.unwrap_or(100) as usize;

        // Simple pagination implementation
        let start_idx = request
            .marker
            .as_ref()
            .and_then(|m| all_keys.iter().position(|k| k == m))
            .map(|idx| idx + 1)
            .unwrap_or(0);

        let end_idx = std::cmp::min(start_idx + limit, all_keys.len());
        let keys_page = &all_keys[start_idx..end_idx];

        let mut key_infos = Vec::new();
        for key_id in keys_page {
            if let Ok(key_info) = self.describe_key(key_id, None).await {
                key_infos.push(key_info);
            }
        }

        let next_marker = if end_idx < all_keys.len() {
            Some(all_keys[end_idx - 1].clone())
        } else {
            None
        };

        Ok(ListKeysResponse {
            keys: key_infos,
            next_marker,
            truncated: end_idx < all_keys.len(),
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Active;
        self.store_key_data(key_id, &key_data).await?;

        debug!(key_id, "Vault KMS key enabled");
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Disabled;
        self.store_key_data(key_id, &key_data).await?;

        debug!(key_id, "Vault KMS key disabled");
        Ok(())
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling key deletion: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::PendingDeletion;
        self.store_key_data(key_id, &key_data).await?;

        debug!(key_id, "Vault KMS key deletion scheduled");
        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling key deletion: {}", key_id);

        let mut key_data = self.get_key_data(key_id).await?;
        key_data.status = KeyStatus::Active;
        self.store_key_data(key_id, &key_data).await?;

        debug!(key_id, "Vault KMS key deletion canceled");
        Ok(())
    }

    /// Rotate the master key while keeping every historical version decryptable.
    ///
    /// Commit protocol (all writes check-and-set, in this order):
    /// 1. First rotation only: freeze the current material as an immutable version
    ///    record and persist `baseline_version` so pre-versioning envelopes resolve
    ///    to it deterministically.
    /// 2. Persist the next version's material as an immutable version record
    ///    (create-only) before anything references it.
    /// 3. Switch the current pointer: bump `version` and mirror the new material
    ///    into the top-level record in a single check-and-set write.
    ///
    /// If any step fails the current pointer is untouched, so a failed, cancelled,
    /// or interrupted rotation never exposes half-committed material. Concurrent
    /// rotations are serialized by the check-and-set writes: at most one caller
    /// commits each version and the losers fail without side effects on current.
    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        debug!("Rotating master key: {}", key_id);

        let (mut cas, mut key_data) = self.get_key_data_versioned(key_id).await?;

        // The material about to be frozen must be decodable: freezing poisoned
        // material would give legacy envelopes a permanently broken baseline. This
        // surfaces the same typed Material* errors as the read path.
        decode_stored_key_material(key_id, &key_data.encrypted_key_material)
            .inspect_err(|error| warn!(key_id, %error, "Vault KMS key material failed validation"))?;

        // Step 1: freeze the baseline on first rotation.
        if key_data.baseline_version.is_none() {
            let baseline = VaultKeyVersionRecord {
                version: key_data.version,
                encrypted_key_material: key_data.encrypted_key_material.clone(),
                created_at: key_data.created_at.clone(),
            };
            if !self.try_create_key_version_record(key_id, &baseline).await? {
                // Either a previous rotation attempt crashed between freezing the
                // baseline and recording it in metadata, or a concurrent rotation
                // got here first. Both are benign only if the existing record holds
                // exactly the material being frozen; anything else means the
                // version history is inconsistent and rotation must not proceed.
                let existing = self.get_key_version_record(key_id, key_data.version).await?;
                if existing.encrypted_key_material != key_data.encrypted_key_material {
                    return Err(KmsError::internal_error(format!(
                        "version record {} of key {key_id} does not match the current key material; refusing to rotate",
                        key_data.version
                    )));
                }
            }
            key_data.baseline_version = Some(key_data.version);
            cas = self.cas_store_key_data(key_id, &key_data, cas).await?;
        }

        // Step 2: durably persist the next version's material before it can become
        // current.
        let new_version = key_data
            .version
            .checked_add(1)
            .ok_or_else(|| KmsError::internal_error(format!("key {key_id} exhausted the version space")))?;
        let generated = generate_key_material(&key_data.algorithm)?;
        let mut new_material = self.encrypt_key_material(&generated).await?;
        let record = VaultKeyVersionRecord {
            version: new_version,
            encrypted_key_material: new_material.clone(),
            created_at: Zoned::now(),
        };
        if !self.try_create_key_version_record(key_id, &record).await? {
            // A record for the next version already exists: an interrupted rotation
            // persisted it and stopped before switching the current pointer, or a
            // concurrent rotation just created it. Adopt the persisted material —
            // it is immutable, fully durable, and has never been current — instead
            // of failing the create-only write forever. The check-and-set switch
            // below still lets at most one caller commit this version.
            let existing = self.get_key_version_record(key_id, new_version).await?;
            decode_stored_key_material(key_id, &existing.encrypted_key_material)?;
            new_material = existing.encrypted_key_material;
        }

        // Step 3: switch the current pointer. The top-level copy of the material is
        // the fast path for new encryptions and must always match `version`.
        key_data.version = new_version;
        key_data.encrypted_key_material = new_material;
        self.cas_store_key_data(key_id, &key_data, cas).await?;

        info!(key_id, version = new_version, "Vault KMS master key rotated");

        Ok(MasterKeyInfo {
            key_id: key_id.to_string(),
            version: new_version,
            algorithm: key_data.algorithm.clone(),
            usage: key_data.usage.clone(),
            status: key_data.status,
            description: key_data.description.clone(),
            metadata: key_data.metadata.clone(),
            created_at: key_data.created_at.clone(),
            rotated_at: Some(Zoned::now()),
            created_by: None,
        })
    }

    async fn health_check(&self) -> Result<()> {
        debug!("Performing Vault health check");

        // Use list_vault_keys but handle the case where no keys exist (which is normal)
        match self.list_vault_keys().await {
            Ok(_) => {
                debug!("Vault health check passed - successfully listed keys");
                Ok(())
            }
            Err(e) => {
                // Check if the error is specifically about "no keys found" or 404
                let error_msg = e.to_string();
                if error_msg.contains("status code 404") || error_msg.contains("No such key") {
                    debug!("Vault health check passed - 404 error is expected when no keys exist yet");
                    Ok(())
                } else {
                    warn!(error = %e, "Vault KMS health check failed");
                    Err(e)
                }
            }
        }
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo::new("vault-kv2".to_string(), "0.1.0".to_string(), self.config.address.clone(), true)
            .with_metadata("kv_mount".to_string(), self.kv_mount.clone())
            .with_metadata("key_prefix".to_string(), self.key_path_prefix.clone())
            // Master key material is protected only by Vault ACLs and KV2 at-rest
            // encryption; there is no additional cryptographic wrapping.
            .with_metadata("at_rest_protection".to_string(), "vault-kv2-acl".to_string())
    }
}

/// VaultKmsBackend wraps VaultKmsClient and implements the KmsBackend trait
pub struct VaultKmsBackend {
    client: VaultKmsClient,
}

impl VaultKmsBackend {
    /// Create a new VaultKmsBackend
    pub async fn new(config: KmsConfig) -> Result<Self> {
        config.validate()?;

        let vault_config = match &config.backend_config {
            crate::config::BackendConfig::VaultKv2(vault_config) => (**vault_config).clone(),
            crate::config::BackendConfig::Local(_)
            | crate::config::BackendConfig::VaultTransit(_)
            | crate::config::BackendConfig::Static(_) => {
                return Err(KmsError::configuration_error("Expected Vault KV2 backend configuration"));
            }
        };

        let client = VaultKmsClient::new(vault_config, config.effective_timeout()).await?;
        Ok(Self { client })
    }

    /// Update key metadata in Vault storage
    async fn update_key_metadata_in_storage(&self, key_id: &str, metadata: &KeyMetadata) -> Result<()> {
        // Get the current key data from Vault
        let mut key_data = self.client.get_key_data(key_id).await?;

        // This is a read-modify-write of the whole VaultKeyData document. Refuse to write
        // back a record whose key material is missing: persisting it would cement the
        // empty-material state under a fresh document version. A damaged key must go
        // through an explicit repair operation, not a metadata update.
        if key_data.encrypted_key_material.is_empty() {
            return Err(KmsError::material_missing(key_id));
        }

        // Update the status based on the new metadata
        key_data.status = match metadata.key_state {
            KeyState::Enabled => KeyStatus::Active,
            KeyState::Disabled => KeyStatus::Disabled,
            KeyState::PendingDeletion => KeyStatus::PendingDeletion,
            KeyState::Unavailable => KeyStatus::Deleted,
            KeyState::PendingImport => KeyStatus::Disabled, // Treat as disabled until import completes
        };

        // Update the key data in Vault storage
        self.client.store_key_data(key_id, &key_data).await?;
        Ok(())
    }
}

#[async_trait]
impl KmsBackend for VaultKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.clone().unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        // Create key in Vault transit engine
        let _master_key = self.client.create_key(&key_id, "AES_256", None).await?;

        // Also store key metadata in KV store with tags
        self.client.store_key_metadata(&key_id, &request).await?;

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: Zoned::now(),
            deletion_date: None,
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: request.tags,
        };

        Ok(CreateKeyResponse {
            key_id,
            key_metadata: metadata,
        })
    }

    async fn encrypt(&self, request: EncryptRequest) -> Result<EncryptResponse> {
        let encrypt_request = crate::types::EncryptRequest {
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

        // Also get key metadata from KV store to retrieve tags
        let key_data = self.client.get_key_data(&request.key_id).await?;

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
            origin: "VAULT".to_string(),
            key_manager: "VAULT".to_string(),
            tags: key_data.tags,
        };

        Ok(DescribeKeyResponse { key_metadata: metadata })
    }

    async fn list_keys(&self, request: ListKeysRequest) -> Result<ListKeysResponse> {
        let response = self.client.list_keys(&request, None).await?;
        Ok(response)
    }

    async fn delete_key(&self, request: DeleteKeyRequest) -> Result<DeleteKeyResponse> {
        // For Vault backend, we'll mark keys for deletion but not physically delete them
        // This allows for recovery during the pending window
        let key_id = &request.key_id;

        // First, check if the key exists and get its metadata
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {key_id} not found")));
            }
        };

        let deletion_date = if request.force_immediate.unwrap_or(false) {
            // Check if key is already in PendingDeletion state
            if key_metadata.key_state == KeyState::PendingDeletion {
                // Force immediate deletion: physically delete the key from Vault storage
                self.client.delete_key(key_id).await?;

                // Return empty deletion_date to indicate key was permanently deleted
                None
            } else {
                // For non-pending keys, mark as PendingDeletion
                key_metadata.key_state = KeyState::PendingDeletion;
                key_metadata.deletion_date = Some(Zoned::now());

                // Update the key metadata in Vault storage to reflect the new state
                self.update_key_metadata_in_storage(key_id, &key_metadata).await?;

                None
            }
        } else {
            // Schedule for deletion (default 30 days)
            let days = request.pending_window_in_days.unwrap_or(30);
            if !(7..=30).contains(&days) {
                return Err(crate::error::KmsError::invalid_parameter(
                    "pending_window_in_days must be between 7 and 30".to_string(),
                ));
            }

            let deletion_date = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            key_metadata.key_state = KeyState::PendingDeletion;
            key_metadata.deletion_date = Some(deletion_date.clone());

            // Update the key metadata in Vault storage to reflect the new state
            self.update_key_metadata_in_storage(key_id, &key_metadata).await?;

            Some(deletion_date.to_string())
        };

        Ok(DeleteKeyResponse {
            key_id: key_id.clone(),
            deletion_date,
            key_metadata,
        })
    }

    async fn cancel_key_deletion(&self, request: CancelKeyDeletionRequest) -> Result<CancelKeyDeletionResponse> {
        let key_id = &request.key_id;

        // Check if the key exists and is pending deletion
        let describe_request = DescribeKeyRequest { key_id: key_id.clone() };
        let mut key_metadata = match self.describe_key(describe_request).await {
            Ok(response) => response.key_metadata,
            Err(_) => {
                return Err(crate::error::KmsError::key_not_found(format!("Key {key_id} not found")));
            }
        };

        if key_metadata.key_state != KeyState::PendingDeletion {
            return Err(crate::error::KmsError::invalid_key_state(format!("Key {key_id} is not pending deletion")));
        }

        // Cancel the deletion by resetting the state
        key_metadata.key_state = KeyState::Enabled;
        key_metadata.deletion_date = None;

        // Persist the reset state back to Vault. Without this the key stays PendingDeletion in
        // storage and would still be reaped, so we must fail the request if the write fails
        // rather than report a false success.
        self.update_key_metadata_in_storage(key_id, &key_metadata).await?;

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
    use crate::config::{VaultAuthMethod, VaultConfig};

    /// Poison matrix for the read-side material gate. Every corruption class must fail
    /// closed with its typed error; reintroducing any "self-heal" (regenerate on empty or
    /// undecodable material) turns one of these expected errors into an Ok and fails the
    /// test. Offline on purpose: `decode_stored_key_material` has no Vault I/O.
    #[test]
    fn decode_stored_key_material_fails_closed_on_poisoned_values() {
        // Empty material means the record lost its key, not that a new one may be minted.
        assert!(matches!(
            decode_stored_key_material("poisoned", ""),
            Err(KmsError::MaterialMissing { key_id }) if key_id == "poisoned"
        ));

        // Invalid base64.
        assert!(matches!(
            decode_stored_key_material("poisoned", "!!!not-base64!!!"),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Truncated material: valid base64 of fewer than 32 bytes.
        let truncated = general_purpose::STANDARD.encode([0x42u8; 16]);
        assert!(matches!(
            decode_stored_key_material("poisoned", &truncated),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Oversized material: valid base64 of more than 32 bytes.
        let oversized = general_purpose::STANDARD.encode([0x42u8; 33]);
        assert!(matches!(
            decode_stored_key_material("poisoned", &oversized),
            Err(KmsError::MaterialCorrupt { key_id, .. }) if key_id == "poisoned"
        ));

        // Well-formed material still decodes.
        let valid = general_purpose::STANDARD.encode([0x42u8; 32]);
        assert_eq!(
            decode_stored_key_material("healthy", &valid).expect("valid material must decode"),
            vec![0x42u8; 32]
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance
    async fn test_vault_client_integration() {
        let config = VaultConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-only-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        };

        let client = VaultKmsClient::new(config, Duration::from_secs(30))
            .await
            .expect("Failed to create Vault client");

        // Test key operations
        let key_id = "test-key-vault";
        let master_key = client
            .create_key(key_id, "AES_256", None)
            .await
            .expect("Failed to create key");
        assert_eq!(master_key.key_id, key_id);
        assert_eq!(master_key.algorithm, "AES_256");

        // Test key description
        let key_info = client.describe_key(key_id, None).await.expect("Failed to describe key");
        assert_eq!(key_info.key_id, key_id);

        // Test data key generation
        let data_key_request = GenerateKeyRequest {
            master_key_id: key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        };

        let data_key = client
            .generate_data_key(&data_key_request, None)
            .await
            .expect("Failed to generate data key");
        assert!(data_key.plaintext.is_some());
        assert!(!data_key.ciphertext.is_empty());

        // Test health check
        client.health_check().await.expect("Health check failed");
    }

    fn integration_vault_config() -> VaultConfig {
        VaultConfig {
            address: "http://127.0.0.1:8200".to_string(),
            auth_method: VaultAuthMethod::Token {
                token: "dev-only-token".to_string(),
            },
            kv_mount: "secret".to_string(),
            key_path_prefix: "rustfs/kms/keys".to_string(),
            mount_path: "transit".to_string(),
            namespace: None,
            tls: None,
        }
    }

    #[tokio::test]
    async fn test_key_version_paths_stay_under_the_key() {
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        assert_eq!(client.key_path("my-key"), "rustfs/kms/keys/my-key");
        assert_eq!(client.key_versions_dir("my-key"), "rustfs/kms/keys/my-key/versions");
        assert_eq!(client.key_version_path("my-key", 3), "rustfs/kms/keys/my-key/versions/3");
    }

    #[test]
    fn test_filter_key_directory_entries_drops_version_dirs() {
        // Listing the key prefix returns "my-key/" as a directory entry once
        // my-key has version records; only real key records may be listed.
        let listed = vec!["alpha".to_string(), "alpha/".to_string(), "beta".to_string()];
        assert_eq!(filter_key_directory_entries(listed), vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn test_resolve_envelope_master_key_version_rules() {
        // An explicit envelope version is honored verbatim, even when it differs
        // from both the baseline and the current version: whether material exists
        // for it is decided by the versioned lookup, never by falling back.
        assert_eq!(resolve_envelope_master_key_version(Some(2), Some(1), 5), 2);
        assert_eq!(resolve_envelope_master_key_version(Some(9), Some(1), 5), 9);

        // A pre-versioning envelope resolves to the frozen baseline, not to
        // whatever version happens to be current.
        assert_eq!(resolve_envelope_master_key_version(None, Some(1), 5), 1);

        // Never-rotated keys have no baseline; the current version is the only
        // material that ever existed, matching pre-versioning behavior.
        assert_eq!(resolve_envelope_master_key_version(None, None, 1), 1);
    }

    #[test]
    fn test_vault_key_data_without_baseline_version_deserializes() {
        // Key records written before versioned storage have no baseline_version
        // field and must keep deserializing with None.
        let key_data = VaultKeyData {
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            created_at: Zoned::now(),
            status: KeyStatus::Active,
            version: 1,
            description: None,
            metadata: HashMap::new(),
            tags: HashMap::new(),
            encrypted_key_material: general_purpose::STANDARD.encode([0x42u8; 32]),
            baseline_version: Some(1),
        };

        let mut value = serde_json::to_value(&key_data).expect("serialize key data");
        value
            .as_object_mut()
            .expect("key data serializes to an object")
            .remove("baseline_version");

        let legacy: VaultKeyData = serde_json::from_value(value).expect("legacy record must deserialize");
        assert_eq!(legacy.baseline_version, None);
        assert_eq!(legacy.version, 1);
    }

    #[test]
    fn test_is_cas_conflict_only_matches_cas_failures() {
        let cas = ClientError::APIError {
            code: 400,
            errors: vec!["check-and-set parameter did not match the current version".to_string()],
        };
        assert!(is_cas_conflict(&cas));

        let other_400 = ClientError::APIError {
            code: 400,
            errors: vec!["invalid request".to_string()],
        };
        assert!(!is_cas_conflict(&other_400));

        let not_found = ClientError::APIError {
            code: 404,
            errors: Vec::new(),
        };
        assert!(!is_cas_conflict(&not_found));
    }

    #[tokio::test]
    async fn test_vault_kv2_backend_info_reports_at_rest_protection() {
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let info = client.backend_info();
        assert_eq!(info.backend_type, "vault-kv2");
        assert_eq!(info.metadata.get("at_rest_protection").map(String::as_str), Some("vault-kv2-acl"));
        // The KV2 backend must not present itself as Transit-backed.
        assert!(!format!("{info:?}").contains("Transit"));
    }

    fn integration_generate_request(key_id: &str) -> GenerateKeyRequest {
        GenerateKeyRequest {
            master_key_id: key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32),
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        }
    }

    fn integration_decrypt_request(ciphertext: Vec<u8>) -> DecryptRequest {
        DecryptRequest {
            ciphertext,
            encryption_context: Default::default(),
            grant_tokens: Vec::new(),
        }
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_decrypt_after_rotate() {
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let key_id = format!("rotate-retain-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");
        let request = integration_generate_request(&key_id);

        let dk_v1 = client.generate_data_key(&request, None).await.expect("generate under v1");
        let env_v1: DataKeyEnvelope = serde_json::from_slice(&dk_v1.ciphertext).expect("parse v1 envelope");
        assert_eq!(env_v1.master_key_version, Some(1));

        let rotated = client.rotate_key(&key_id, None).await.expect("rotate to v2");
        assert_eq!(rotated.version, 2);
        let dk_v2 = client.generate_data_key(&request, None).await.expect("generate under v2");
        let env_v2: DataKeyEnvelope = serde_json::from_slice(&dk_v2.ciphertext).expect("parse v2 envelope");
        assert_eq!(env_v2.master_key_version, Some(2), "new envelopes must carry the latest version");

        let rotated = client.rotate_key(&key_id, None).await.expect("rotate to v3");
        assert_eq!(rotated.version, 3);
        let dk_v3 = client.generate_data_key(&request, None).await.expect("generate under v3");
        let env_v3: DataKeyEnvelope = serde_json::from_slice(&dk_v3.ciphertext).expect("parse v3 envelope");
        assert_eq!(env_v3.master_key_version, Some(3));

        // A mixed batch of envelopes from every historical version must decrypt.
        for (data_key, label) in [(&dk_v1, "v1"), (&dk_v3, "v3"), (&dk_v2, "v2"), (&dk_v1, "v1 again")] {
            let plaintext = client
                .decrypt(&integration_decrypt_request(data_key.ciphertext.clone()), None)
                .await
                .unwrap_or_else(|error| panic!("envelope wrapped under {label} must stay decryptable: {error}"));
            assert_eq!(Some(plaintext), data_key.plaintext, "{label} plaintext must round-trip");
        }
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_rotate_does_not_orphan_legacy_envelopes() {
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let key_id = format!("rotate-legacy-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        // Simulate an envelope written by a pre-versioning build: same wrapped DEK,
        // but without the master_key_version field.
        let data_key = client
            .generate_data_key(&integration_generate_request(&key_id), None)
            .await
            .expect("generate");
        let mut envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
        envelope
            .as_object_mut()
            .expect("envelope is an object")
            .remove("master_key_version");
        let legacy_ciphertext = serde_json::to_vec(&envelope).expect("serialize legacy envelope");

        client.rotate_key(&key_id, None).await.expect("rotate to v2");
        client.rotate_key(&key_id, None).await.expect("rotate to v3");

        // The baseline rule must route the legacy envelope to the frozen version 1
        // material even though the current version has moved on.
        let plaintext = client
            .decrypt(&integration_decrypt_request(legacy_ciphertext), None)
            .await
            .expect("legacy envelope must stay decryptable after rotation");
        assert_eq!(Some(plaintext), data_key.plaintext);

        let key_data = client.get_key_data(&key_id).await.expect("read");
        assert_eq!(key_data.baseline_version, Some(1), "first rotation must pin the baseline");
        assert_eq!(key_data.version, 3);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_envelope_version_tampering_fails_closed() {
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let key_id = format!("rotate-tamper-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");
        let data_key = client
            .generate_data_key(&integration_generate_request(&key_id), None)
            .await
            .expect("generate");
        client.rotate_key(&key_id, None).await.expect("rotate");

        // Point the envelope at a version that has no material record.
        let mut envelope: serde_json::Value = serde_json::from_slice(&data_key.ciphertext).expect("parse envelope");
        envelope
            .as_object_mut()
            .expect("envelope is an object")
            .insert("master_key_version".to_string(), serde_json::json!(999));
        let tampered = serde_json::to_vec(&envelope).expect("serialize tampered envelope");

        let error = client
            .decrypt(&integration_decrypt_request(tampered), None)
            .await
            .expect_err("nonexistent version must fail closed, not fall back to current");
        assert!(
            matches!(error, KmsError::KeyVersionNotFound { version: 999, key_id: ref error_key_id } if *error_key_id == key_id),
            "expected KeyVersionNotFound for version 999, got {error:?}"
        );

        // The untampered envelope still decrypts through its recorded version.
        let plaintext = client
            .decrypt(&integration_decrypt_request(data_key.ciphertext.clone()), None)
            .await
            .expect("untampered envelope must still decrypt");
        assert_eq!(Some(plaintext), data_key.plaintext);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_kv2_concurrent_rotate_versions_monotonic() {
        use std::sync::Arc;

        let client = Arc::new(
            VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
                .await
                .expect("client"),
        );

        let key_id = format!("rotate-concurrent-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        let attempts = 4;
        let tasks: Vec<_> = (0..attempts)
            .map(|_| {
                let client = Arc::clone(&client);
                let key_id = key_id.clone();
                tokio::spawn(async move { client.rotate_key(&key_id, None).await })
            })
            .collect();

        let mut successes = 0u32;
        for task in tasks {
            // Losing a check-and-set race is an expected error; committing is not
            // required, but every commit must account for exactly one version bump.
            if task.await.expect("join rotate task").is_ok() {
                successes += 1;
            }
        }
        assert!(successes >= 1, "at least one rotation must commit");

        let key_data = client.get_key_data(&key_id).await.expect("read");
        assert_eq!(
            key_data.version,
            1 + successes,
            "each successful rotation must commit exactly one new version"
        );
        assert_eq!(key_data.baseline_version, Some(1));

        // Every version has an immutable record with unique material, and the
        // top-level fast-path copy matches the current version's record.
        let mut materials = std::collections::HashSet::new();
        for version in 1..=key_data.version {
            let record = client
                .get_key_version_record(&key_id, version)
                .await
                .unwrap_or_else(|error| panic!("version {version} must have a record: {error}"));
            assert_eq!(record.version, version);
            assert!(materials.insert(record.encrypted_key_material), "version materials must be unique");
        }
        let current_record = client
            .get_key_version_record(&key_id, key_data.version)
            .await
            .expect("current version record");
        assert_eq!(current_record.encrypted_key_material, key_data.encrypted_key_material);
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_corrupted_key_material_does_not_regenerate() {
        // Regression: get_key_material previously "self-healed" a decrypt/length failure by
        // minting a fresh random master key and overwriting the stored value — destroying the
        // original key and making every DEK wrapped by it permanently undecryptable.
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let key_id = format!("corrupt-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        // Corrupt the stored material to an invalid base64 string.
        let mut key_data = client.get_key_data(&key_id).await.expect("read");
        key_data.encrypted_key_material = "!!!not-base64!!!".to_string();
        client.store_key_data(&key_id, &key_data).await.expect("store corrupt");

        // Reading the material must now ERROR, not silently regenerate + overwrite.
        let poisoned = client.get_key_data(&key_id).await.expect("read poisoned");
        let error = client
            .get_key_material_for_version(&key_id, &poisoned, poisoned.version)
            .await
            .expect_err("corrupted key material must yield an error, not a fresh key");
        assert!(
            matches!(error, KmsError::MaterialCorrupt { .. }),
            "expected MaterialCorrupt, got {error:?}"
        );

        // And the stored (corrupted) material must be UNCHANGED.
        let after = client.get_key_data(&key_id).await.expect("reread");
        assert_eq!(
            after.encrypted_key_material, "!!!not-base64!!!",
            "the material read path must not overwrite stored master key material on failure"
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_empty_key_material_does_not_regenerate() {
        // Regression: get_key_material previously treated empty stored material as a
        // bootstrap case and silently generated + persisted a fresh master key on the
        // read path. Empty material must instead fail closed as MaterialMissing and
        // leave the stored record untouched.
        let client = VaultKmsClient::new(integration_vault_config(), Duration::from_secs(30))
            .await
            .expect("client");

        let key_id = format!("empty-{}", uuid::Uuid::new_v4());
        client.create_key(&key_id, "AES_256", None).await.expect("create");

        let mut key_data = client.get_key_data(&key_id).await.expect("read");
        key_data.encrypted_key_material = String::new();
        client.store_key_data(&key_id, &key_data).await.expect("store empty");

        let poisoned = client.get_key_data(&key_id).await.expect("read poisoned");
        let error = client
            .get_key_material_for_version(&key_id, &poisoned, poisoned.version)
            .await
            .expect_err("empty key material must yield an error, not a fresh key");
        assert!(
            matches!(error, KmsError::MaterialMissing { .. }),
            "expected MaterialMissing, got {error:?}"
        );

        // The stored record must still hold the empty value: no regeneration, no write.
        let after = client.get_key_data(&key_id).await.expect("reread");
        assert!(
            after.encrypted_key_material.is_empty(),
            "the material read path must not backfill missing master key material"
        );
    }

    #[tokio::test]
    #[ignore] // Requires a running Vault instance (dev mode)
    async fn test_vault_cancel_key_deletion_persists_state() {
        use crate::config::{BackendConfig, KmsConfig};
        use crate::types::{CancelKeyDeletionRequest, CreateKeyRequest, DeleteKeyRequest, KeyStatus, KeyUsage};

        let kms_config = KmsConfig {
            backend_config: BackendConfig::VaultKv2(Box::new(integration_vault_config())),
            ..Default::default()
        };
        let backend = VaultKmsBackend::new(kms_config).await.expect("backend");

        let key_id = format!("cancel-persist-{}", uuid::Uuid::new_v4());
        backend
            .create_key(CreateKeyRequest {
                key_name: Some(key_id.clone()),
                key_usage: KeyUsage::EncryptDecrypt,
                ..Default::default()
            })
            .await
            .expect("create");

        backend
            .delete_key(DeleteKeyRequest {
                key_id: key_id.clone(),
                pending_window_in_days: Some(7),
                force_immediate: Some(false),
            })
            .await
            .expect("schedule delete");

        backend
            .cancel_key_deletion(CancelKeyDeletionRequest { key_id: key_id.clone() })
            .await
            .expect("cancel");

        // Re-read the PERSISTED state from Vault. Before the fix, storage still held
        // PendingDeletion because cancel only mutated the response, never wrote back.
        let persisted = backend.client.get_key_data(&key_id).await.expect("reread");
        assert_eq!(
            persisted.status,
            KeyStatus::Active,
            "cancel_key_deletion must persist Active status to Vault, not only mutate the response"
        );
    }
}
