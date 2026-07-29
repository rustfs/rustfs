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
use crate::encryption::{AesDekCrypto, DataKeyEnvelope, DekCrypto, generate_key_material};
use crate::error::{KmsError, Result};
use crate::types::*;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::{Algorithm, Argon2, Params, Version};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use jiff::Zoned;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use tokio::fs;
use tracing::{debug, warn};

/// Reject key identifiers that would not name a single file directly inside the key
/// directory.
///
/// The rule is containment, not a character allowlist: anything that stays inside
/// `key_dir` is accepted, so identifiers already in use by existing deployments keep
/// resolving. Only separators, traversal and the degenerate cases are refused, which is
/// what stops `key_dir.join(...)` from escaping.
fn validate_key_id(key_id: &str) -> Result<()> {
    if key_id.is_empty() {
        return Err(KmsError::invalid_key("key identifier must not be empty"));
    }
    if key_id.contains('/') || key_id.contains('\\') || key_id.contains('\0') {
        return Err(KmsError::invalid_key(format!(
            "key identifier must not contain path separators or NUL: {key_id:?}"
        )));
    }

    // Catches `.`, `..`, absolute paths, and platform-specific forms such as Windows
    // drive prefixes, all of which would move the join outside key_dir.
    let file_name = format!("{key_id}.key");
    let mut components = Path::new(&file_name).components();
    match (components.next(), components.next()) {
        (Some(Component::Normal(_)), None) => Ok(()),
        _ => Err(KmsError::invalid_key(format!(
            "key identifier must name a single file inside the key directory: {key_id:?}"
        ))),
    }
}

const LOCAL_KMS_MASTER_KEY_SALT_FILE: &str = ".master-key.salt";
const LOCAL_KMS_MASTER_KEY_SALT_LEN: usize = 16;
const LOCAL_KMS_MASTER_KEY_LEN: usize = 32;
const LOCAL_KMS_ARGON2_M_COST_KIB: u32 = 19 * 1024;
const LOCAL_KMS_ARGON2_T_COST: u32 = 2;
const LOCAL_KMS_ARGON2_P_COST: u32 = 1;

/// Local KMS client that stores keys in local files
pub struct LocalKmsClient {
    config: LocalConfig,
    /// Master encryption key for encrypting stored keys
    master_cipher: Option<Aes256Gcm>,
    /// Legacy pre-beta.9 master cipher for reading pre-Argon2 key files
    legacy_master_cipher: Option<Aes256Gcm>,
    /// DEK encryption implementation
    dek_crypto: AesDekCrypto,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum StoredKeyProtection {
    #[default]
    LegacyUnspecified,
    EncryptedMasterKey,
    PlaintextDevOnly,
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
    #[serde(with = "crate::time_serde::zoned")]
    created_at: Zoned,
    #[serde(with = "crate::time_serde::option_zoned")]
    rotated_at: Option<Zoned>,
    created_by: Option<String>,
    /// Encrypted key material (32 bytes encoded in base64 for AES-256)
    encrypted_key_material: String,
    /// Nonce used for encryption
    nonce: Vec<u8>,
    #[serde(default)]
    at_rest_protection: StoredKeyProtection,
}

impl LocalKmsClient {
    /// Create a new local KMS client
    pub async fn new(config: LocalConfig) -> Result<Self> {
        // Create key directory if it doesn't exist
        if !fs::try_exists(&config.key_dir).await? {
            fs::create_dir_all(&config.key_dir).await?;
            debug!(path = ?config.key_dir, "KMS key directory created");
        }

        // Initialize master cipher if master key is provided
        let (master_cipher, legacy_master_cipher) = if let Some(ref master_key) = config.master_key {
            let salt = Self::load_or_create_master_key_salt(&config).await?;
            let key = Self::derive_master_key(master_key, &salt)?;
            let legacy_key = Self::derive_legacy_master_key(master_key)?;
            (Some(Aes256Gcm::new(&key)), Some(Aes256Gcm::new(&legacy_key)))
        } else {
            warn!("No master key provided - local KMS key material will use explicit plaintext-dev-only storage");
            (None, None)
        };

        let client = Self {
            config,
            master_cipher,
            legacy_master_cipher,
            dek_crypto: AesDekCrypto::new(),
        };
        client.validate_existing_keys().await?;
        Ok(client)
    }

    /// Derive a 256-bit key from the master key string using a persistent Argon2id salt.
    fn derive_master_key(master_key: &str, salt: &[u8]) -> Result<Key<Aes256Gcm>> {
        let params = Params::new(
            LOCAL_KMS_ARGON2_M_COST_KIB,
            LOCAL_KMS_ARGON2_T_COST,
            LOCAL_KMS_ARGON2_P_COST,
            Some(LOCAL_KMS_MASTER_KEY_LEN),
        )
        .map_err(|err| KmsError::configuration_error(format!("invalid local KMS Argon2 params: {err}")))?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        let mut derived = [0u8; LOCAL_KMS_MASTER_KEY_LEN];
        argon2
            .hash_password_into(master_key.as_bytes(), salt, &mut derived)
            .map_err(|err| KmsError::cryptographic_error("argon2id_kdf", err.to_string()))?;
        let key = Key::<Aes256Gcm>::from(derived);
        Ok(key)
    }

    fn derive_legacy_master_key(master_key: &str) -> Result<Key<Aes256Gcm>> {
        let mut hasher = Sha256::new();
        hasher.update(master_key.as_bytes());
        hasher.update(b"rustfs-kms-local");
        Key::<Aes256Gcm>::try_from(hasher.finalize().as_slice())
            .map_err(|_| KmsError::cryptographic_error("legacy_key", "Invalid key length"))
    }

    fn master_key_salt_path(config: &LocalConfig) -> PathBuf {
        config.key_dir.join(LOCAL_KMS_MASTER_KEY_SALT_FILE)
    }

    async fn load_or_create_master_key_salt(config: &LocalConfig) -> Result<[u8; LOCAL_KMS_MASTER_KEY_SALT_LEN]> {
        let salt_path = Self::master_key_salt_path(config);
        if fs::try_exists(&salt_path).await? {
            let bytes = fs::read(&salt_path).await?;
            return bytes.try_into().map_err(|_| {
                KmsError::configuration_error(format!(
                    "Local KMS master key salt at {} must be exactly {} bytes",
                    salt_path.display(),
                    LOCAL_KMS_MASTER_KEY_SALT_LEN
                ))
            });
        }

        let mut salt = [0u8; LOCAL_KMS_MASTER_KEY_SALT_LEN];
        rand::rng().fill(&mut salt[..]);
        let temp_path = config
            .key_dir
            .join(format!("{LOCAL_KMS_MASTER_KEY_SALT_FILE}.tmp-{}", uuid::Uuid::new_v4()));
        fs::write(&temp_path, salt).await?;
        Self::set_file_permissions(&temp_path, config.file_permissions).await?;
        match fs::hard_link(&temp_path, &salt_path).await {
            Ok(()) => {
                if let Err(error) = fs::remove_file(&temp_path).await {
                    warn!(path = ?temp_path, %error, "Failed to remove Local KMS salt temporary file");
                }
                debug!(path = ?salt_path, "Local KMS master key salt created");
                Ok(salt)
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path).await;
                let bytes = fs::read(&salt_path).await?;
                bytes.try_into().map_err(|_| {
                    KmsError::configuration_error(format!(
                        "Local KMS master key salt at {} must be exactly {} bytes",
                        salt_path.display(),
                        LOCAL_KMS_MASTER_KEY_SALT_LEN
                    ))
                })
            }
            Err(error) => {
                let _ = fs::remove_file(&temp_path).await;
                Err(error.into())
            }
        }
    }

    #[cfg(unix)]
    async fn set_file_permissions(path: &std::path::Path, permissions: Option<u32>) -> Result<()> {
        if let Some(mode) = permissions {
            use std::os::unix::fs::PermissionsExt;

            let perms = std::fs::Permissions::from_mode(mode);
            fs::set_permissions(path, perms).await?;
        }

        Ok(())
    }

    #[cfg(not(unix))]
    async fn set_file_permissions(_path: &std::path::Path, _permissions: Option<u32>) -> Result<()> {
        Ok(())
    }

    /// Get the file path for a master key.
    ///
    /// Key identifiers reach this from request input (the `name` tag on CreateKey, the
    /// `keyId` body field or query parameter on DeleteKey), so they are joined onto
    /// `key_dir` only after being confirmed to name a single file inside it. Without that
    /// check an identifier such as `../../tmp/evil` escapes the configured key directory,
    /// turning key creation into a constrained arbitrary-file write and key deletion into
    /// a cross-directory delete.
    ///
    /// Every filesystem path in this backend is derived here, so validating at this one
    /// point covers `decode_stored_key`, `load_master_key`, `save_master_key`, `create_key`
    /// and `delete_key`.
    fn master_key_path(&self, key_id: &str) -> Result<PathBuf> {
        validate_key_id(key_id)?;
        Ok(self.config.key_dir.join(format!("{key_id}.key")))
    }

    /// Decode and decrypt a stored key file, returning both the metadata and decrypted key material
    async fn decode_stored_key(&self, key_id: &str) -> Result<(StoredMasterKey, Vec<u8>)> {
        let key_path = self.master_key_path(key_id)?;
        if !fs::try_exists(&key_path).await? {
            return Err(KmsError::key_not_found(key_id));
        }

        let content = fs::read(&key_path).await?;
        let stored_key: StoredMasterKey = serde_json::from_slice(&content)?;
        if stored_key.key_id != key_id {
            return Err(KmsError::invalid_key(format!(
                "Local KMS key file identity mismatch: expected {key_id:?}, found {:?}",
                stored_key.key_id
            )));
        }

        let encrypted_bytes = BASE64
            .decode(&stored_key.encrypted_key_material)
            .map_err(|e| KmsError::cryptographic_error("base64_decode", e.to_string()))?;

        let effective_protection = if stored_key.at_rest_protection == StoredKeyProtection::LegacyUnspecified {
            if stored_key.nonce.is_empty() {
                StoredKeyProtection::PlaintextDevOnly
            } else {
                StoredKeyProtection::EncryptedMasterKey
            }
        } else {
            stored_key.at_rest_protection
        };

        // Decrypt key material if master cipher is available.
        let key_material = match effective_protection {
            StoredKeyProtection::EncryptedMasterKey => {
                // RUSTFS_COMPAT_TODO(rustfs-5063): Remove after upgrades rewrite all pre-beta.9 key files.
                // Pre-beta.9 files have no protection marker and use the legacy
                // SHA-256 KDF, while later pre-marker files use Argon2.
                let cipher = self.master_cipher.as_ref().ok_or_else(|| {
                    KmsError::configuration_error(format!(
                        "Local KMS key {key_id} is encrypted at rest and requires a configured master key"
                    ))
                })?;
                if stored_key.nonce.len() != 12 {
                    return Err(KmsError::cryptographic_error("nonce", "Invalid nonce length"));
                }

                let mut nonce_array = [0u8; 12];
                nonce_array.copy_from_slice(&stored_key.nonce);
                let nonce = Nonce::from(nonce_array);

                match cipher.decrypt(&nonce, encrypted_bytes.as_ref()) {
                    Ok(key_material) => key_material,
                    Err(current_error) if stored_key.at_rest_protection == StoredKeyProtection::LegacyUnspecified => {
                        let legacy_cipher = self.legacy_master_cipher.as_ref().ok_or_else(|| {
                            KmsError::configuration_error(format!(
                                "Local KMS key {key_id} is encrypted at rest and requires a configured master key"
                            ))
                        })?;
                        legacy_cipher
                            .decrypt(&nonce, encrypted_bytes.as_ref())
                            .map_err(|_| KmsError::cryptographic_error("decrypt", current_error.to_string()))?
                    }
                    Err(error) => return Err(KmsError::cryptographic_error("decrypt", error.to_string())),
                }
            }
            StoredKeyProtection::PlaintextDevOnly | StoredKeyProtection::LegacyUnspecified => {
                if self.master_cipher.is_some() && stored_key.at_rest_protection == StoredKeyProtection::PlaintextDevOnly {
                    warn!(
                        key_id,
                        "Local KMS loaded plaintext-dev-only key material while a master key is configured"
                    );
                }
                encrypted_bytes
            }
        };

        Ok((stored_key, key_material))
    }

    /// Load a master key from disk
    async fn load_master_key(&self, key_id: &str) -> Result<MasterKeyInfo> {
        let (stored_key, _key_material) = self.decode_stored_key(key_id).await?;

        Ok(MasterKeyInfo {
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
    async fn save_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<()> {
        let key_path = self.master_key_path(&master_key.key_id)?;
        let content = self.encode_master_key(master_key, key_material)?;
        let temp_path = key_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        fs::write(&temp_path, &content).await?;
        Self::set_file_permissions(&temp_path, self.config.file_permissions).await?;
        fs::rename(&temp_path, &key_path).await?;

        debug!(key_id = %master_key.key_id, path = ?key_path, "Local KMS master key saved");
        Ok(())
    }

    async fn save_new_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<()> {
        let key_path = self.master_key_path(&master_key.key_id)?;
        let content = self.encode_master_key(master_key, key_material)?;
        let temp_path = key_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        fs::write(&temp_path, &content).await?;
        Self::set_file_permissions(&temp_path, self.config.file_permissions).await?;

        match fs::hard_link(&temp_path, &key_path).await {
            Ok(()) => {
                if let Err(error) = fs::remove_file(&temp_path).await {
                    warn!(path = ?temp_path, %error, "Failed to remove Local KMS key temporary file");
                }
                debug!(key_id = %master_key.key_id, path = ?key_path, "Local KMS master key created");
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = fs::remove_file(&temp_path).await;
                Err(KmsError::key_already_exists(&master_key.key_id))
            }
            Err(error) => {
                let _ = fs::remove_file(&temp_path).await;
                Err(error.into())
            }
        }
    }

    fn encode_master_key(&self, master_key: &MasterKeyInfo, key_material: &[u8]) -> Result<Vec<u8>> {
        let (encrypted_key_material, nonce, at_rest_protection) = if let Some(ref cipher) = self.master_cipher {
            let mut nonce_bytes = [0u8; 12];
            rand::rng().fill(&mut nonce_bytes[..]);
            let nonce = Nonce::from(nonce_bytes);

            let encrypted = cipher
                .encrypt(&nonce, key_material)
                .map_err(|e| KmsError::cryptographic_error("encrypt", e.to_string()))?;
            // Encode encrypted bytes to base64 string
            (BASE64.encode(&encrypted), nonce.to_vec(), StoredKeyProtection::EncryptedMasterKey)
        } else {
            warn!(
                key_id = %master_key.key_id,
                "Local KMS is storing key material as plaintext-dev-only because no master key is configured"
            );
            (BASE64.encode(key_material), Vec::new(), StoredKeyProtection::PlaintextDevOnly)
        };

        let stored_key = StoredMasterKey {
            key_id: master_key.key_id.clone(),
            version: master_key.version,
            algorithm: master_key.algorithm.clone(),
            usage: master_key.usage.clone(),
            status: master_key.status.clone(),
            description: master_key.description.clone(),
            metadata: master_key.metadata.clone(),
            created_at: master_key.created_at.clone(),
            rotated_at: master_key.rotated_at.clone(),
            created_by: master_key.created_by.clone(),
            encrypted_key_material,
            nonce,
            at_rest_protection,
        };

        serde_json::to_vec_pretty(&stored_key).map_err(Into::into)
    }

    /// Get the actual key material for a master key
    async fn get_key_material(&self, key_id: &str) -> Result<Vec<u8>> {
        let (_stored_key, key_material) = self.decode_stored_key(key_id).await?;
        Ok(key_material)
    }

    async fn validate_existing_keys(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.config.key_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_none_or(|extension| extension != "key") {
                continue;
            }

            let key_id = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| KmsError::configuration_error("Local KMS key file name must be valid UTF-8"))?;
            self.decode_stored_key(key_id).await?;
        }
        Ok(())
    }

    /// Encrypt data using a master key
    async fn encrypt_with_master_key(&self, key_id: &str, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        self.dek_crypto.encrypt(&key_material, plaintext).await
    }

    /// Decrypt data using a master key
    async fn decrypt_with_master_key(&self, key_id: &str, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>> {
        // Load the actual master key material
        let key_material = self.get_key_material(key_id).await?;
        self.dek_crypto.decrypt(&key_material, ciphertext, nonce).await
    }
}

#[async_trait]
impl KmsClient for LocalKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKeyInfo> {
        debug!("Generating data key for master key: {}", request.master_key_id);

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

        // Create data key envelope with master key version for rotation support
        let envelope = DataKeyEnvelope {
            key_id: uuid::Uuid::new_v4().to_string(),
            master_key_id: request.master_key_id.clone(),
            key_spec: request.key_spec.clone(),
            encrypted_key,
            nonce,
            encryption_context: request.encryption_context.clone(),
            created_at: Zoned::now(),
        };

        // Serialize the envelope as the ciphertext
        let ciphertext = serde_json::to_vec(&envelope)?;

        let data_key = DataKeyInfo::new(envelope.key_id, 1, Some(plaintext_key), ciphertext, request.key_spec.clone());

        debug!(key_id = %request.master_key_id, "Local KMS data key generated");
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

        // Decrypt the data key
        let plaintext = self
            .decrypt_with_master_key(&envelope.master_key_id, &envelope.encrypted_key, &envelope.nonce)
            .await?;

        debug!("Local KMS data decrypted");
        Ok(plaintext)
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        debug!("Creating master key: {}", key_id);

        // Check if key already exists
        if self.master_key_path(key_id)?.exists() {
            return Err(KmsError::key_already_exists(key_id));
        }

        // Validate algorithm
        if algorithm != "AES_256" {
            return Err(KmsError::unsupported_algorithm(algorithm));
        }

        // Generate key material
        let key_material = generate_key_material(algorithm)?;

        let created_by = context
            .map(|ctx| ctx.principal.clone())
            .unwrap_or_else(|| "local-kms".to_string());

        let master_key = MasterKeyInfo::new_with_description(key_id.to_string(), algorithm.to_string(), Some(created_by), None);

        // Save to disk
        self.save_new_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS master key created");
        Ok(master_key)
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let master_key = self.load_master_key(key_id).await?;
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

        // Preserve the existing key material. Regenerating it on a pure status change would
        // destroy the original master key and make every DEK ever wrapped by it permanently
        // undecryptable (silent data loss).
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key enabled");
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::Disabled;

        // Preserve the existing key material (see enable_key): a status change must never
        // regenerate the master key, or every DEK wrapped by it becomes undecryptable.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key disabled");
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

        // Preserve the existing key material (see enable_key): scheduling deletion must not
        // regenerate the master key, or cancelling the deletion later would recover a key that
        // can no longer decrypt existing data.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key deletion scheduled");
        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling deletion for key: {}", key_id);

        let mut master_key = self.load_master_key(key_id).await?;
        master_key.status = KeyStatus::Active;

        // Preserve the existing key material (see enable_key): cancelling deletion must recover
        // the ORIGINAL key, not mint a new one that cannot decrypt existing data.
        let key_material = self.get_key_material(key_id).await?;
        self.save_master_key(&master_key, &key_material).await?;

        debug!(key_id, "Local KMS key deletion canceled");
        Ok(())
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKeyInfo> {
        if !fs::try_exists(self.master_key_path(key_id)?).await? {
            return Err(KmsError::key_not_found(key_id));
        }
        Err(KmsError::invalid_operation(
            "Local KMS key rotation is unavailable until historical key versions can be retained",
        ))
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
        config.validate()?;

        let local_config = match &config.backend_config {
            crate::config::BackendConfig::Local(local_config) => local_config.clone(),
            crate::config::BackendConfig::VaultKv2(_)
            | crate::config::BackendConfig::VaultTransit(_)
            | crate::config::BackendConfig::Static(_) => {
                return Err(KmsError::configuration_error("Expected Local backend configuration"));
            }
        };

        let client = LocalKmsClient::new(local_config).await?;
        Ok(Self { client })
    }
}

#[async_trait]
impl KmsBackend for LocalKmsBackend {
    async fn create_key(&self, request: CreateKeyRequest) -> Result<CreateKeyResponse> {
        let key_id = request.key_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if self.client.master_key_path(&key_id)?.exists() {
            return Err(KmsError::key_already_exists(&key_id));
        }

        // Create master key with description directly
        let _master_key = {
            let algorithm = "AES_256";
            // Generate key material
            let key_material = generate_key_material(algorithm)?;

            let master_key = MasterKeyInfo::new_with_description(
                key_id.clone(),
                algorithm.to_string(),
                Some("local-kms".to_string()),
                request.description.clone(),
            );

            // Save to disk
            self.client.save_new_master_key(&master_key, &key_material).await?;

            master_key
        };

        let metadata = KeyMetadata {
            key_id: key_id.clone(),
            key_state: KeyState::Enabled,
            key_usage: request.key_usage,
            description: request.description,
            creation_date: Zoned::now(),
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
            let key_path = self.client.master_key_path(key_id)?;
            tokio::fs::remove_file(&key_path)
                .await
                .map_err(|e| KmsError::internal_error(format!("Failed to delete key file: {e}")))?;

            debug!(key_id, "Local KMS key deleted immediately");

            // Return success response for immediate deletion
            let key_metadata = KeyMetadata {
                key_id: master_key.key_id.clone(),
                description: master_key.description.clone(),
                key_usage: master_key.usage,
                key_state: KeyState::PendingDeletion, // AWS KMS compatibility
                creation_date: master_key.created_at,
                deletion_date: Some(Zoned::now()),
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

            let deletion_date = Zoned::now() + Duration::from_secs(days as u64 * 86400);
            master_key.status = KeyStatus::PendingDeletion;

            (Some(deletion_date.to_string()), Some(deletion_date))
        };

        // Save the updated key to disk - preserve existing key material!
        // Load and decode the stored key to get the existing key material
        let (_stored_key, existing_key_material) = self
            .client
            .decode_stored_key(key_id)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to decode key: {e}")))?;

        self.client.save_master_key(&master_key, &existing_key_material).await?;

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
        // Preserve existing key material instead of generating new one
        let (_stored_key, existing_key_material) = self
            .client
            .decode_stored_key(key_id)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to decode key: {e}")))?;

        self.client.save_master_key(&master_key, &existing_key_material).await?;

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
    use std::collections::HashMap;
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

    async fn create_dev_mode_client() -> (LocalKmsClient, TempDir) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        };
        let client = LocalKmsClient::new(config).await.expect("Failed to create dev-mode client");
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
    async fn key_state_transitions_preserve_master_key_material() {
        // Regression: enable/disable/schedule_deletion/cancel_deletion previously regenerated the
        // master key material on a pure status change, permanently destroying the ability to
        // decrypt any DEK wrapped by that key. A status cycle must preserve the material.
        let (client, _temp_dir) = create_test_client().await;

        let key_id = "state-cycle-key";
        client.create_key(key_id, "AES_256", None).await.expect("create");

        let request = GenerateKeyRequest::new(key_id.to_string(), "AES_256".to_string())
            .with_context("bucket".to_string(), "b".to_string());
        let data_key = client.generate_data_key(&request, None).await.expect("generate data key");
        let ciphertext = data_key.ciphertext.clone();
        let plaintext = data_key.plaintext.clone().expect("no plaintext");

        // Cycle through every status-changing method the fix touches.
        client.disable_key(key_id, None).await.expect("disable");
        client.enable_key(key_id, None).await.expect("enable");
        client
            .schedule_key_deletion(key_id, 7, None)
            .await
            .expect("schedule deletion");
        client.cancel_key_deletion(key_id, None).await.expect("cancel deletion");

        // Pre-fix, each of those regenerated the master key, so this unwrap fails with an AEAD
        // error. Post-fix, the original material is preserved and the DEK still decrypts.
        let decrypt_request = DecryptRequest::new(ciphertext).with_context("bucket".to_string(), "b".to_string());
        let decrypted = client
            .decrypt(&decrypt_request, None)
            .await
            .expect("DEK must still decrypt after status transitions");
        assert_eq!(decrypted, plaintext, "master key material must survive status transitions");
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

    #[tokio::test]
    async fn test_encrypted_master_key_storage_uses_explicit_protection_and_salt() {
        let (client, _temp_dir) = create_test_client().await;
        client
            .create_key("encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let salt = fs::read(LocalKmsClient::master_key_salt_path(&client.config))
            .await
            .expect("master key salt should exist");
        assert_eq!(salt.len(), LOCAL_KMS_MASTER_KEY_SALT_LEN);

        let stored: StoredMasterKey = serde_json::from_slice(
            &fs::read(client.master_key_path("encrypted-key").expect("valid key id"))
                .await
                .expect("stored key should exist"),
        )
        .expect("stored encrypted key should deserialize");
        assert_eq!(stored.at_rest_protection, StoredKeyProtection::EncryptedMasterKey);
        assert_eq!(stored.nonce.len(), 12);

        let wrong_master_error = match LocalKmsClient::new(LocalConfig {
            key_dir: client.config.key_dir.clone(),
            master_key: Some("wrong-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("wrong master key must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(wrong_master_error, KmsError::CryptographicError { .. }));
    }

    #[tokio::test]
    async fn test_plaintext_dev_only_storage_is_explicit_and_loadable() {
        let (client, _temp_dir) = create_dev_mode_client().await;
        client
            .create_key("plaintext-key", "AES_256", None)
            .await
            .expect("Failed to create plaintext-dev-only key");

        let stored: StoredMasterKey = serde_json::from_slice(
            &fs::read(client.master_key_path("plaintext-key").expect("valid key id"))
                .await
                .expect("stored key should exist"),
        )
        .expect("stored plaintext key should deserialize");
        assert_eq!(stored.at_rest_protection, StoredKeyProtection::PlaintextDevOnly);
        assert!(stored.nonce.is_empty(), "plaintext-dev-only keys should not store a nonce");

        let key_info = client
            .describe_key("plaintext-key", None)
            .await
            .expect("plaintext-dev-only key should remain readable");
        assert_eq!(key_info.key_id, "plaintext-key");
    }

    #[tokio::test]
    async fn test_encrypted_key_requires_master_key_to_load() {
        let (client, temp_dir) = create_test_client().await;
        client
            .create_key("encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let config = LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        };
        let err = match LocalKmsClient::new(config).await {
            Ok(_) => panic!("initialization must reject an unreadable encrypted key"),
            Err(error) => error,
        };
        assert!(err.to_string().contains("requires a configured master key"));
    }

    #[tokio::test]
    async fn local_key_rotation_is_rejected_without_overwriting_key_material() {
        let (client, _temp_dir) = create_test_client().await;
        let key_id = "rotation-key";
        client.create_key(key_id, "AES_256", None).await.expect("create key");
        let original_material = client.get_key_material(key_id).await.expect("load original material");

        let error = client
            .rotate_key(key_id, None)
            .await
            .expect_err("rotation must remain unavailable without historical key versions");

        assert!(matches!(error, KmsError::InvalidOperation { .. }));
        assert_eq!(
            client.get_key_material(key_id).await.expect("reload original material"),
            original_material
        );
    }

    #[tokio::test]
    async fn startup_rejects_key_file_with_mismatched_embedded_id() {
        let (client, temp_dir) = create_dev_mode_client().await;
        client.create_key("file-name", "AES_256", None).await.expect("create key");
        let key_path = client.master_key_path("file-name").expect("valid key id");
        let mut stored: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("read key file")).expect("decode key file");
        stored["key_id"] = serde_json::json!("embedded-name");
        fs::write(&key_path, serde_json::to_vec_pretty(&stored).expect("encode mismatched key"))
            .await
            .expect("write mismatched key");

        let error = match LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: None,
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("mismatched key identity must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(error, KmsError::InvalidKey { .. }));
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_legacy_rfc3339_timestamp() {
        let (client, _temp_dir) = create_dev_mode_client().await;

        let stored_key = serde_json::json!({
            "key_id": "legacy-key",
            "version": 1u32,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "description": serde_json::Value::Null,
            "metadata": HashMap::<String, String>::new(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "rotated_at": serde_json::Value::Null,
            "created_by": "legacy-test",
            "encrypted_key_material": BASE64.encode([7u8; 32]),
            "nonce": Vec::<u8>::new()
        });

        let key_path = client.master_key_path("legacy-key").expect("valid key id");
        fs::write(&key_path, serde_json::to_vec_pretty(&stored_key).expect("serialize test key"))
            .await
            .expect("write legacy key");

        let key_info = client.load_master_key("legacy-key").await.expect("legacy key should load");
        assert_eq!(key_info.key_id, "legacy-key");
        assert_eq!(key_info.created_at.time_zone().iana_name(), Some("UTC"));
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_legacy_encrypted_record_without_protection_field() {
        let (client, temp_dir) = create_test_client().await;
        client
            .create_key("legacy-encrypted-key", "AES_256", None)
            .await
            .expect("Failed to create encrypted key");

        let key_path = client.master_key_path("legacy-encrypted-key").expect("valid key id");
        let mut stored_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&key_path).await.expect("stored key should exist"))
                .expect("stored key should deserialize");
        stored_json
            .as_object_mut()
            .expect("stored key should be an object")
            .remove("at_rest_protection");
        fs::write(
            &key_path,
            serde_json::to_vec_pretty(&stored_json).expect("legacy record should serialize"),
        )
        .await
        .expect("legacy record should be writable");

        let legacy_client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("legacy client should initialize");

        let key_info = legacy_client
            .describe_key("legacy-encrypted-key", None)
            .await
            .expect("legacy encrypted record should remain readable");
        assert_eq!(key_info.key_id, "legacy-encrypted-key");
    }

    #[tokio::test]
    async fn test_load_master_key_accepts_beta5_sha256_encrypted_record() {
        let temp_dir = TempDir::new().expect("create beta.5 fixture directory");
        let stored_key = serde_json::json!({
            "key_id": "beta5-key",
            "version": 1u32,
            "algorithm": "AES_256",
            "usage": "EncryptDecrypt",
            "status": "Active",
            "description": serde_json::Value::Null,
            "metadata": HashMap::<String, String>::new(),
            "created_at": "2024-01-01T00:00:00+00:00",
            "rotated_at": serde_json::Value::Null,
            "created_by": "beta5-fixture",
            "encrypted_key_material": "xjwGa4Lj4qzKg6XQl8s2btyFkPHPChMAkjqs268TFGyvFUv8WjDD5HQCUDLViZmt",
            "nonce": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        });
        fs::write(
            temp_dir.path().join("beta5-key.key"),
            serde_json::to_vec_pretty(&stored_key).expect("serialize beta.5 fixture"),
        )
        .await
        .expect("write beta.5 fixture");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("beta5-test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("initialize local KMS for beta.5 fixture");

        let material = client
            .get_key_material("beta5-key")
            .await
            .expect("decrypt beta.5 SHA-256 protected key");
        assert_eq!(material, vec![0x42; 32]);

        let mut explicit_protection = stored_key.clone();
        let explicit_object = explicit_protection.as_object_mut().expect("beta.5 fixture is a JSON object");
        explicit_object.insert("key_id".to_string(), serde_json::json!("beta5-explicit-key"));
        explicit_object.insert("at_rest_protection".to_string(), serde_json::json!("encrypted-master-key"));
        fs::write(
            temp_dir.path().join("beta5-explicit-key.key"),
            serde_json::to_vec_pretty(&explicit_protection).expect("serialize explicit-protection fixture"),
        )
        .await
        .expect("write explicit-protection fixture");
        let explicit_error = client
            .get_key_material("beta5-explicit-key")
            .await
            .expect_err("explicit current protection must not fall back to the beta.5 KDF");
        assert!(matches!(explicit_error, KmsError::CryptographicError { .. }));

        let wrong_key_error = match LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("wrong-beta5-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        {
            Ok(_) => panic!("wrong beta.5 master key must fail initialization"),
            Err(error) => error,
        };
        assert!(matches!(wrong_key_error, KmsError::CryptographicError { .. }));
    }

    /// R03-CAN-072 / R03-CAN-073: key identifiers arrive from request input, so every path
    /// derived from one must stay inside the configured key directory. Traversal here would
    /// turn CreateKey into a constrained arbitrary-file write and DeleteKey into a
    /// cross-directory delete.
    #[tokio::test]
    async fn master_key_path_confines_key_ids_to_the_key_directory() {
        let (client, temp_dir) = create_test_client().await;

        // The invariant is containment, so assert that directly: whatever the input, the
        // result is either refused or a path whose parent is exactly the key directory.
        // Note `.` and `..` are contained rather than refused — the `.key` suffix turns
        // them into the ordinary filenames `..key` and `...key`.
        for candidate in [
            "../escape",
            "../../etc/rustfs",
            "sub/dir",
            "..",
            ".",
            "",
            "/absolute",
            "back\\slash",
            "nul\0byte",
            "....//....//escape",
        ] {
            match client.master_key_path(candidate) {
                Err(KmsError::InvalidKey { .. }) => {}
                Err(other) => panic!("unexpected error kind for {candidate:?}: {other:?}"),
                Ok(path) => assert_eq!(
                    path.parent(),
                    Some(temp_dir.path()),
                    "{candidate:?} was accepted but escapes the key directory: {path:?}"
                ),
            }
        }

        // The traversal forms specifically must be refused, not merely contained.
        for escaping in ["../escape", "sub/dir", "/absolute", "back\\slash", "nul\0byte", ""] {
            let err = client.master_key_path(escaping).expect_err("traversal must be refused");
            assert!(
                matches!(err, KmsError::InvalidKey { .. }),
                "expected InvalidKey for {escaping:?}, got {err:?}"
            );
        }

        // Ordinary identifiers, including the UUID form used when no name is supplied,
        // must still resolve — and must land directly in the key directory.
        for ok in ["test-key", "a.b_c-1", "3f2504e0-4f89-11d3-9a0c-0305e82c3301"] {
            let path = client.master_key_path(ok).expect("valid key id must be accepted");
            assert_eq!(
                path.parent(),
                Some(temp_dir.path()),
                "{ok:?} must resolve directly inside the key directory"
            );
        }
    }

    /// R07-CAN-103: creating a duplicate key must preserve its original material.
    #[tokio::test]
    async fn backend_create_key_refuses_to_replace_existing_key_material() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let client = LocalKmsClient::new(LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        })
        .await
        .expect("Failed to create client");
        let backend = LocalKmsBackend { client };

        let request = || CreateKeyRequest {
            key_name: Some("duplicate-key".to_string()),
            ..Default::default()
        };

        backend.create_key(request()).await.expect("first create must succeed");
        let original = backend
            .client
            .get_key_material("duplicate-key")
            .await
            .expect("key material must be readable after creation");

        let err = backend
            .create_key(request())
            .await
            .expect_err("creating a key under an existing name must be refused");
        assert!(matches!(err, KmsError::KeyAlreadyExists { .. }), "expected KeyAlreadyExists, got {err:?}");

        let after = backend
            .client
            .get_key_material("duplicate-key")
            .await
            .expect("original key material must survive the refused create");
        assert_eq!(original, after, "existing key material must not be replaced");
    }

    #[tokio::test]
    async fn concurrent_backend_create_allows_only_one_writer() {
        let temp_dir = TempDir::new().expect("create key directory");
        let config = || LocalConfig {
            key_dir: temp_dir.path().to_path_buf(),
            master_key: Some("test-master-key".to_string()),
            file_permissions: Some(0o600),
        };
        let first = LocalKmsBackend {
            client: LocalKmsClient::new(config()).await.expect("create first client"),
        };
        let second = LocalKmsBackend {
            client: LocalKmsClient::new(config()).await.expect("create second client"),
        };
        let request = || CreateKeyRequest {
            key_name: Some("concurrent-key".to_string()),
            ..Default::default()
        };

        let (first_result, second_result) = tokio::join!(first.create_key(request()), second.create_key(request()));
        assert_ne!(first_result.is_ok(), second_result.is_ok(), "exactly one create must succeed");
        let error = first_result
            .err()
            .or_else(|| second_result.err())
            .expect("one create must fail");
        assert!(matches!(error, KmsError::KeyAlreadyExists { .. }));
        assert_eq!(first.client.get_key_material("concurrent-key").await.expect("load key").len(), 32);
    }
}
