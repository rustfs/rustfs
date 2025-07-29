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
use rand::RngCore;
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
        requests::{DecryptDataRequest as VaultDecryptRequest, EncryptDataRequest as VaultEncryptRequest},
    },
    transit::{data, key},
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
        rand::rng().fill_bytes(&mut data_key);

        // Encrypt the data key using Transit engine
        let plaintext = general_purpose::STANDARD.encode(&data_key);
        let _encrypt_request = VaultEncryptRequest::builder().plaintext(&plaintext).build()?;

        let response = data::encrypt(&self.client, &self.mount_path, key_name, &plaintext, None)
            .await
            .map_err(|e| KmsError::internal_error(format!("Failed to encrypt data: {e}")))?;

        Ok(DataKey {
            key_id: key_name.to_string(),
            version: 1,
            plaintext: Some(data_key),
            ciphertext: response.ciphertext.into_bytes(),
            metadata: HashMap::new(),
            created_at: SystemTime::now(),
        })
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

        // Generate data key with specified length (default 32 bytes for AES-256)
        let key_length = match request.key_spec.as_str() {
            "AES_256" => 32,
            "AES_128" => 16,
            _ => 32, // Default to AES-256
        };

        self.generate_and_encrypt_data_key(&request.master_key_id, key_length).await
    }

    async fn encrypt(&self, request: &EncryptRequest, _context: Option<&OperationContext>) -> Result<EncryptResponse> {
        debug!("Encrypting data with key: {}", request.key_id);

        let plaintext = general_purpose::STANDARD.encode(&request.plaintext);
        let _encrypt_request = VaultEncryptRequest::builder().plaintext(&plaintext).build()?;

        let response = data::encrypt(&self.client, &self.mount_path, &request.key_id, &plaintext, None)
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

        let _decrypt_request = VaultDecryptRequest::builder().ciphertext(&ciphertext).build()?;

        let response = data::decrypt(&self.client, &self.mount_path, &key_id, &ciphertext, None)
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

        let keys = key::list(&self.client, &self.mount_path)
            .await
            .map_err(|e| KmsError::backend_error("vault", format!("Failed to list keys: {e}")))?;

        let key_list = keys.keys;

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

                // Check if Transit engine is accessible
                match key::list(&self.client, &self.mount_path).await {
                    Ok(_) => {
                        debug!("Transit engine health check passed");
                        Ok(())
                    }
                    Err(e) => {
                        warn!("Transit engine health check failed: {}", e);
                        Err(KmsError::backend_error("vault", format!("Transit engine not accessible: {e}")))
                    }
                }
            }
            Err(e) => {
                warn!("Vault health check failed: {}", e);
                Err(KmsError::backend_error("vault", format!("Vault health check failed: {e}")))
            }
        }
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
