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

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use std::collections::HashMap;
use std::time::SystemTime;
use tokio::time::Duration;
use tracing::{debug, info, warn};
use vaultrs::auth::approle;
use vaultrs::client::{Client, VaultClient, VaultClientSettingsBuilder};
use vaultrs::error::ClientError;
use vaultrs::sys;

use crate::{
    config::{VaultAuthMethod, VaultConfig},
    error::{KmsError, Result},
    manager::{BackendInfo, KmsClient},
    types::{
        DataKey, DecryptRequest, EncryptRequest, EncryptResponse, GenerateKeyRequest, KeyInfo, KeyStatus, KeyUsage,
        ListKeysRequest, ListKeysResponse, MasterKey, OperationContext,
    },
};
use vaultrs::{
    api::transit::{
        KeyType,
        requests::{
            DataKeyType, DecryptDataRequest as VaultDecryptRequest, EncryptDataRequest as VaultEncryptRequest,
            GenerateDataKeyRequest as VaultGenerateDataKeyRequest, RewrapDataRequest as VaultRewrapDataRequest,
        },
    },
    transit::{data, generate, key},
};

/// Vault KMS client implementation using vaultrs library with Transit engine
pub struct VaultKmsClient {
    client: VaultClient,
    mount_path: String,
    namespace: Option<String>,
}

impl VaultKmsClient {
    /// Create a new Vault KMS client
    pub async fn new(config: VaultConfig) -> Result<Self> {
        let mut settings_builder = VaultClientSettingsBuilder::default();

        let settings = if let Some(ref namespace) = config.namespace {
            settings_builder
                .address(&config.address)
                .timeout(Some(Duration::from_secs(30)))
                .namespace(Some(namespace.clone()))
                .build()?
        } else {
            settings_builder
                .address(&config.address)
                .timeout(Some(Duration::from_secs(30)))
                .build()?
        };

        // Configure TLS if provided
        if let Some(_tls_config) = &config.tls_config {
            // TODO: Configure TLS settings
        }
        let mut client = VaultClient::new(settings)?;

        // Authenticate based on the auth method
        match &config.auth_method {
            VaultAuthMethod::Token { token } => {
                client.set_token(token);
            }
            VaultAuthMethod::AppRole { role_id, secret_id } => {
                let auth_info = approle::login(&client, role_id, secret_id, "").await?;
                client.set_token(&auth_info.client_token);
            }
            VaultAuthMethod::Kubernetes { .. } | VaultAuthMethod::AwsIam { .. } | VaultAuthMethod::Cert { .. } => {
                return Err(KmsError::configuration_error("Authentication method not yet implemented".to_string()));
            }
        }

        // Verify authentication
        // Token validation - simplified for now
        // let _token_info = vaultrs::token::lookup_self(&client).await
        //     .map_err(|e| KmsError::authentication_failed(format!("Token verification failed: {}", e)))?;

        info!("Successfully authenticated with Vault");

        Ok(Self {
            client,
            mount_path: if config.mount_path.is_empty() {
                "transit".to_string()
            } else {
                config.mount_path
            },
            namespace: config.namespace,
        })
    }

    /// Generate a random data key and encrypt it with the master key
    async fn generate_and_encrypt_data_key(&self, key_name: &str, key_length: usize) -> Result<DataKey> {
        // Generate random data key
        let mut data_key = vec![0u8; key_length];
        let mut i = 0;
        while i < key_length {
            let chunk: u64 = rand::random();
            let bytes = chunk.to_ne_bytes();
            let n = usize::min(8, key_length - i);
            data_key[i..i + n].copy_from_slice(&bytes[..n]);
            i += n;
        }

        // Encrypt the data key using Transit engine
        let plaintext = general_purpose::STANDARD.encode(&data_key);
        let _encrypt_request = VaultEncryptRequest::builder().plaintext(&plaintext).build()?;

        let response = data::encrypt(&self.client, &self.mount_path, key_name, &plaintext, None)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to encrypt data: {e}")))?;

        // Format ciphertext with key_id prefix to match encrypt() method format
        let key_id_bytes = key_name.as_bytes();
        let key_id_len = key_id_bytes.len() as u32;
        let mut final_ciphertext = Vec::new();
        final_ciphertext.extend_from_slice(&key_id_len.to_be_bytes());
        final_ciphertext.extend_from_slice(key_id_bytes);
        final_ciphertext.extend_from_slice(&response.ciphertext.into_bytes());

        Ok(DataKey {
            key_id: key_name.to_string(),
            version: 1,
            plaintext: Some(data_key),
            ciphertext: final_ciphertext,
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
        })
    }

    // Canonicalize encryption context (stable JSON with sorted keys) and base64-encode for Vault context
    fn encode_context(ctx: &HashMap<String, String>) -> Option<String> {
        if ctx.is_empty() {
            return None;
        }
        // Use BTreeMap to guarantee deterministic key order
        let mut ordered = std::collections::BTreeMap::new();
        for (k, v) in ctx.iter() {
            ordered.insert(k.clone(), v.clone());
        }
        match serde_json::to_string(&ordered) {
            Ok(json) => Some(general_purpose::STANDARD.encode(json.as_bytes())),
            Err(_) => None,
        }
    }

    /// Rewrap a ciphertext to the latest key version, preserving our custom header format
    #[allow(dead_code)]
    pub async fn rewrap_ciphertext(&self, ciphertext_with_header: &[u8], enc_ctx: &HashMap<String, String>) -> Result<Vec<u8>> {
        if ciphertext_with_header.len() < 4 {
            return Err(KmsError::internal_error("Invalid ciphertext format: too short".to_string()));
        }

        let key_id_len = u32::from_be_bytes([
            ciphertext_with_header[0],
            ciphertext_with_header[1],
            ciphertext_with_header[2],
            ciphertext_with_header[3],
        ]) as usize;

        if ciphertext_with_header.len() < 4 + key_id_len {
            return Err(KmsError::internal_error("Invalid ciphertext format: insufficient data".to_string()));
        }

        let key_id = String::from_utf8(ciphertext_with_header[4..4 + key_id_len].to_vec())
            .map_err(|e| KmsError::internal_error(format!("Invalid key_id in ciphertext: {e}")))?;
        let vault_ciphertext = &ciphertext_with_header[4 + key_id_len..];
        let vault_ciphertext = std::str::from_utf8(vault_ciphertext)
            .map_err(|e| KmsError::internal_error(format!("Invalid ciphertext bytes: {e}")))?;

        let ctx_b64 = Self::encode_context(enc_ctx);
        let mut builder = VaultRewrapDataRequest::builder();
        builder.ciphertext(vault_ciphertext);
        if let Some(c) = ctx_b64.as_deref() {
            builder.context(c);
        }

        let resp = data::rewrap(&self.client, &self.mount_path, &key_id, vault_ciphertext, Some(&mut builder))
            .await
            .map_err(|e| KmsError::internal_error(format!("Rewrap failed: {e}")))?;

        let key_id_bytes = key_id.as_bytes();
        let key_id_len = key_id_bytes.len() as u32;
        let mut final_ciphertext = Vec::new();
        final_ciphertext.extend_from_slice(&key_id_len.to_be_bytes());
        final_ciphertext.extend_from_slice(key_id_bytes);
        final_ciphertext.extend_from_slice(resp.ciphertext.as_bytes());
        Ok(final_ciphertext)
    }
}

impl std::fmt::Debug for VaultKmsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultKmsClient")
            .field("mount_path", &self.mount_path)
            .field("client", &"VaultClient")
            .finish()
    }
}

#[async_trait]
impl KmsClient for VaultKmsClient {
    async fn generate_data_key(&self, request: &GenerateKeyRequest, _context: Option<&OperationContext>) -> Result<DataKey> {
        debug!("Generating data key for master key: {}", request.master_key_id);

        // Ensure the master key exists
        self.describe_key(&request.master_key_id, None).await?;

        // Prefer using Vault transit datakey/plaintext so plaintext and wrapped key are generated atomically
        // Determine key size in bits (Vault expects bits: 128/256/512). Our request.key_length is bytes when provided.
        let bits: u16 = if let Some(bytes) = request.key_length {
            (bytes * 8) as u16
        } else {
            match request.key_spec.as_str() {
                "AES_128" => 128,
                "AES_256" => 256,
                _ => 256,
            }
        };

        let ctx_b64 = Self::encode_context(&request.encryption_context);
        let mut builder = VaultGenerateDataKeyRequest::builder();
        builder.bits(bits);
        if let Some(c) = ctx_b64.as_deref() {
            builder.context(c);
        }

        match generate::data_key(
            &self.client,
            &self.mount_path,
            &request.master_key_id,
            DataKeyType::Plaintext,
            Some(&mut builder),
        )
        .await
        {
            Ok(resp) => {
                // Vault returns plaintext (base64) and ciphertext (string like vault:vN:...)
                if let Some(pt_b64) = resp.plaintext {
                    let pt = general_purpose::STANDARD
                        .decode(pt_b64.as_bytes())
                        .map_err(|e| KmsError::internal_error(format!("Failed to decode data key plaintext: {e}")))?;

                    // Format ciphertext with key_id prefix for compatibility
                    let key_id_bytes = request.master_key_id.as_bytes();
                    let key_id_len = key_id_bytes.len() as u32;
                    let mut final_ciphertext = Vec::new();
                    final_ciphertext.extend_from_slice(&key_id_len.to_be_bytes());
                    final_ciphertext.extend_from_slice(key_id_bytes);
                    final_ciphertext.extend_from_slice(resp.ciphertext.as_bytes());

                    Ok(DataKey {
                        key_id: request.master_key_id.clone(),
                        version: 1,
                        plaintext: Some(pt),
                        ciphertext: final_ciphertext,
                        metadata: HashMap::new(),
                        created_at: SystemTime::now(),
                    })
                } else {
                    // Policy may deny returning plaintext; fall back to local RNG + encrypt path
                    let key_length_bytes = (bits / 8) as usize;
                    self.generate_and_encrypt_data_key(&request.master_key_id, key_length_bytes)
                        .await
                }
            }
            Err(e) => {
                warn!("Vault datakey/plaintext generation failed, falling back to RNG+encrypt: {}", e);
                let key_length_bytes = (bits / 8) as usize;
                self.generate_and_encrypt_data_key(&request.master_key_id, key_length_bytes)
                    .await
            }
        }
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        let plaintext = general_purpose::STANDARD.encode(&request.plaintext);

        let ctx_b64 = Self::encode_context(&request.encryption_context);
        let mut builder = VaultEncryptRequest::builder();
        builder.plaintext(&plaintext);
        if let Some(c) = ctx_b64.as_deref() {
            builder.context(c);
        }
        let response = data::encrypt(&self.client, &self.mount_path, &request.key_id, &plaintext, Some(&mut builder))
            .await
            .map_err(|e| KmsError::internal_error(format!("Encryption failed: {e}")))?;

        // Prepend key_id to ciphertext for later extraction during decryption
        let key_id_bytes = request.key_id.as_bytes();
        let key_id_len = key_id_bytes.len() as u32;
        let mut final_ciphertext = Vec::new();
        final_ciphertext.extend_from_slice(&key_id_len.to_be_bytes());
        final_ciphertext.extend_from_slice(key_id_bytes);
        final_ciphertext.extend_from_slice(&response.ciphertext.into_bytes());

        Ok(EncryptResponse {
            key_id: request.key_id.clone(),
            ciphertext: final_ciphertext,
            key_version: 1,
            algorithm: "AES-256-GCM".to_string(),
        })
    }

    async fn decrypt(&self, request: &DecryptRequest, _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        // Extract key_id from ciphertext
        if request.ciphertext.len() < 4 {
            return Err(KmsError::internal_error("Invalid ciphertext format: too short".to_string()));
        }

        let key_id_len = u32::from_be_bytes([
            request.ciphertext[0],
            request.ciphertext[1],
            request.ciphertext[2],
            request.ciphertext[3],
        ]) as usize;

        if request.ciphertext.len() < 4 + key_id_len {
            return Err(KmsError::internal_error("Invalid ciphertext format: insufficient data".to_string()));
        }

        let key_id = String::from_utf8(request.ciphertext[4..4 + key_id_len].to_vec())
            .map_err(|e| KmsError::internal_error(format!("Invalid key_id in ciphertext: {e}")))?;

        let vault_ciphertext = &request.ciphertext[4 + key_id_len..];

        debug!("Decrypting data with key: {}", key_id);

        let ciphertext = String::from_utf8(vault_ciphertext.to_vec())
            .map_err(|e| KmsError::internal_error(format!("Invalid ciphertext format: {e}")))?;

        let ctx_b64 = Self::encode_context(&request.encryption_context);
        let mut builder = VaultDecryptRequest::builder();
        builder.ciphertext(&ciphertext);
        if let Some(c) = ctx_b64.as_deref() {
            builder.context(c);
        }
        let response = data::decrypt(&self.client, &self.mount_path, &key_id, &ciphertext, Some(&mut builder))
            .await
            .map_err(|e| KmsError::internal_error(format!("Decryption failed: {e}")))?;

        let plaintext = general_purpose::STANDARD
            .decode(&response.plaintext)
            .map_err(|e| KmsError::internal_error(format!("Failed to decode plaintext: {e}")))?;

        Ok(plaintext)
    }

    async fn create_key(&self, key_id: &str, algorithm: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Creating master key: {}", key_id);

        // Key type is determined by Vault's default configuration

        key::create(&self.client, &self.mount_path, key_id, None)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to create key: {e}")))?;

        info!("Successfully created master key: {}", key_id);

        Ok(MasterKey {
            key_id: key_id.to_string(),
            version: 1,
            algorithm: algorithm.to_string(),
            usage: KeyUsage::Encrypt,
            status: KeyStatus::Active,
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
            rotated_at: None,
        })
    }

    async fn describe_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<KeyInfo> {
        debug!("Describing key: {}", key_id);

        let key_info = key::read(&self.client, &self.mount_path, key_id).await.map_err(|e| {
            if e.to_string().contains("404") {
                KmsError::key_not_found(key_id)
            } else {
                KmsError::backend_error("vault", format!("Failed to describe key: {e}"))
            }
        })?;

        let creation_time = SystemTime::now();

        Ok(KeyInfo {
            key_id: key_id.to_string(),
            name: key_id.to_string(),
            description: None,
            algorithm: match key_info.key_type {
                KeyType::Aes128Gcm96 => "AES_128".to_string(),
                KeyType::Aes256Gcm96 => "AES_256".to_string(),
                KeyType::Rsa2048 => "RSA_2048".to_string(),
                KeyType::Rsa4096 => "RSA_4096".to_string(),
                _ => "AES_256".to_string(),
            },
            usage: KeyUsage::Encrypt,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::new(),
            created_at: creation_time,
            rotated_at: None,
            created_by: None,
        })
    }

    async fn list_keys(&self, _request: &ListKeysRequest, _context: Option<&OperationContext>) -> Result<ListKeysResponse> {
        debug!("Listing keys in mount: {}", self.mount_path);

        // Vault may return 404 when there are no keys under the transit mount.
        // Treat that as a normal empty list.
        let key_list: Vec<String> = match key::list(&self.client, &self.mount_path).await {
            Ok(keys) => keys.keys,
            Err(e) => {
                let es = e.to_string();
                if es.contains("404") || es.to_lowercase().contains("not found") {
                    debug!("Transit has no keys yet at '{}' (404), returning empty list", self.mount_path);
                    Vec::new()
                } else {
                    return Err(KmsError::backend_error("vault", format!("Failed to list keys: {e}")));
                }
            }
        };

        let key_infos: Vec<KeyInfo> = key_list
            .into_iter()
            .map(|key_name| KeyInfo {
                key_id: key_name.clone(),
                name: key_name,
                description: None,
                algorithm: "AES_256".to_string(),
                usage: KeyUsage::Encrypt,
                status: KeyStatus::Active,
                version: 1,
                metadata: HashMap::new(),
                created_at: SystemTime::now(),
                rotated_at: None,
                created_by: None,
            })
            .collect();

    Ok(ListKeysResponse {
            keys: key_infos,
            truncated: false,
            next_marker: None,
        })
    }

    async fn enable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Enabling key: {}", key_id);

        // Transit engine doesn't have explicit enable/disable operations
        // We simulate this by checking if the key exists
        self.describe_key(key_id, None).await?;

        info!("Key {} is available (Transit keys are always enabled)", key_id);
        Ok(())
    }

    async fn disable_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Disabling key: {}", key_id);

        // Transit engine doesn't have explicit enable/disable operations
        // This operation is not supported in Transit engine
        warn!("Key disable operation is not supported in Transit engine for key: {}", key_id);

        Err(KmsError::internal_error("Key disable operation is not supported in Transit engine"))
    }

    async fn schedule_key_deletion(
        &self,
        key_id: &str,
        _pending_window_days: u32,
        _context: Option<&OperationContext>,
    ) -> Result<()> {
        debug!("Scheduling key deletion: {}", key_id);

        key::delete(&self.client, &self.mount_path, key_id)
            .await
            .map_err(|e| KmsError::backend_error("vault", format!("Failed to delete key: {e}")))?;

        info!("Successfully scheduled key deletion: {}", key_id);
        Ok(())
    }

    async fn cancel_key_deletion(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<()> {
        debug!("Canceling key deletion: {}", key_id);

        // Transit engine doesn't support canceling deletion
        // Once a key is deleted, it cannot be recovered
        warn!("Cancel key deletion is not supported in Transit engine for key: {}", key_id);

        Err(KmsError::internal_error("Cancel key deletion is not supported in Transit engine"))
    }

    async fn rotate_key(&self, key_id: &str, _context: Option<&OperationContext>) -> Result<MasterKey> {
        debug!("Rotating key: {}", key_id);

        key::rotate(&self.client, &self.mount_path, key_id)
            .await
            .map_err(|e| KmsError::backend_error("vault", format!("Failed to rotate key: {e}")))?;

        info!("Successfully rotated key: {}", key_id);

        Ok(MasterKey {
            key_id: key_id.to_string(),
            version: 2,
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::Encrypt,
            status: KeyStatus::Active,
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
            rotated_at: Some(SystemTime::now()),
        })
    }

    async fn health_check(&self) -> Result<()> {
        debug!("Performing health check");

        // Check Vault system health
        match sys::health(&self.client).await {
            Ok(health) => {
                if health.sealed {
                    warn!("Vault is sealed");
                    return Err(KmsError::backend_error("vault", "Vault is sealed"));
                }
                Ok(())
            }
            Err(e) => {
                warn!("Vault health check failed: {}", e);
                Err(KmsError::backend_error("vault", format!("Vault health check failed: {e}")))
            }
        }
    }

    async fn generate_object_data_key(
        &self,
        master_key_id: &str,
        _key_spec: &str,
        _context: Option<&OperationContext>,
    ) -> Result<DataKey> {
        debug!("Generating object data key for master key: {}", master_key_id);

        // Use the existing generate_data_key method
        let request = GenerateKeyRequest {
            master_key_id: master_key_id.to_string(),
            key_spec: "AES_256".to_string(),
            key_length: Some(32), // 32 bytes for AES-256
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        self.generate_data_key(&request, _context).await
    }

    async fn decrypt_object_data_key(&self, encrypted_key: &[u8], _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting object data key");

        // Use the existing decrypt method
        let request = DecryptRequest {
            ciphertext: encrypted_key.to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        self.decrypt(&request, _context).await
    }

    async fn encrypt_object_metadata(
        &self,
        master_key_id: &str,
        metadata: &[u8],
        _context: Option<&OperationContext>,
    ) -> Result<Vec<u8>> {
        debug!("Encrypting object metadata with key: {}", master_key_id);

        // Use the existing encrypt method
        let request = EncryptRequest {
            key_id: master_key_id.to_string(),
            plaintext: metadata.to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        let response = self.encrypt(&request, _context).await?;
        Ok(response.ciphertext)
    }

    async fn decrypt_object_metadata(&self, encrypted_metadata: &[u8], _context: Option<&OperationContext>) -> Result<Vec<u8>> {
        debug!("Decrypting object metadata");

        // Use the existing decrypt method
        let request = DecryptRequest {
            ciphertext: encrypted_metadata.to_vec(),
            encryption_context: HashMap::new(),
            grant_tokens: Vec::new(),
        };

        self.decrypt(&request, _context).await
    }

    async fn generate_data_key_with_context(
        &self,
        master_key_id: &str,
        key_spec: &str,
        context: &std::collections::HashMap<String, String>,
        _operation_context: Option<&OperationContext>,
    ) -> Result<DataKey> {
        debug!("Generating data key with context for master key: {} using Vault", master_key_id);

        // Use Vault's transit engine to generate a data key with encryption context
        let operation_context = OperationContext {
            operation_id: uuid::Uuid::new_v4(),
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
        debug!("Decrypting data with context using Vault");

        // Use Vault's transit engine to decrypt with encryption context
        let operation_context = OperationContext {
            operation_id: uuid::Uuid::new_v4(),
            principal: "system".to_string(),
            source_ip: None,
            user_agent: None,
            additional_context: context.clone(),
        };

        self.decrypt_object_data_key(ciphertext, Some(&operation_context)).await
    }

    fn backend_info(&self) -> BackendInfo {
        BackendInfo {
            backend_type: "vault".to_string(),
            version: "1.0.0".to_string(),
            endpoint: "vault://unknown".to_string(),
            healthy: false, // Cannot use async health_check in sync method
            metadata: {
                let mut meta = HashMap::new();
                meta.insert("mount_path".to_string(), self.mount_path.clone());
                meta.insert("client_type".to_string(), "vaultrs".to_string());
                meta.insert("engine_type".to_string(), "transit".to_string());
                if let Some(ref namespace) = self.namespace {
                    meta.insert("namespace".to_string(), namespace.clone());
                }
                meta
            },
        }
    }

    async fn rewrap_ciphertext(&self, ciphertext_with_header: &[u8], context: &HashMap<String, String>) -> Result<Vec<u8>> {
        self.rewrap_ciphertext(ciphertext_with_header, context).await
    }
}

// Convert vaultrs errors to KmsError
impl From<ClientError> for KmsError {
    fn from(err: ClientError) -> Self {
        KmsError::backend_error("vault", err.to_string())
    }
}

impl From<vaultrs::api::transit::requests::CreateKeyRequestBuilderError> for KmsError {
    fn from(err: vaultrs::api::transit::requests::CreateKeyRequestBuilderError) -> Self {
        KmsError::backend_error("vault", err.to_string())
    }
}

impl From<vaultrs::api::transit::requests::EncryptDataRequestBuilderError> for KmsError {
    fn from(err: vaultrs::api::transit::requests::EncryptDataRequestBuilderError) -> Self {
        KmsError::BackendError {
            service: "vault".to_string(),
            message: err.to_string(),
        }
    }
}

impl From<vaultrs::api::transit::requests::DecryptDataRequestBuilderError> for KmsError {
    fn from(err: vaultrs::api::transit::requests::DecryptDataRequestBuilderError) -> Self {
        KmsError::BackendError {
            service: "vault".to_string(),
            message: err.to_string(),
        }
    }
}

impl From<vaultrs::client::VaultClientSettingsBuilderError> for KmsError {
    fn from(err: vaultrs::client::VaultClientSettingsBuilderError) -> Self {
        KmsError::BackendError {
            service: "vault".to_string(),
            message: err.to_string(),
        }
    }
}
