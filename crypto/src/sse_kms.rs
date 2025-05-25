// sse_kms.rs - Implementation of SSE-KMS (Server-Side Encryption with Key Management Service)
// This file implements encryption/decryption using KMS-managed keys

use crate::{
    Error,
    sse::{SSEOptions, Encryptable, DefaultKMSConfig, get_default_kms_config},
    rusty_vault_client::{self, Client, ClientError},
};
use aes_gcm::{
    aead::KeyInit,
    Aes256Gcm, Key, Nonce
};
use rand::RngCore;
use std::sync::{Arc, RwLock, Once};
use serde_json::{json, Map, Value};
use tracing::{debug, error};

// Lazily initialized KMS client
static INIT_KMS_CLIENT: Once = Once::new();
static KMS_CLIENT: RwLock<Option<Arc<KMSClient>>> = RwLock::new(None);

/// KMSClient wraps the RustyVault client for key management operations
#[derive(Clone)]
pub struct KMSClient {
    client: Arc<Client>,
    key_name: String,
}

impl KMSClient {
    /// Create a new KMS client with the given configuration
    pub fn new(config: &DefaultKMSConfig) -> Result<Self, Error> {
        // Create basic client configuration
        let mut client_builder = Client::new()
            .with_addr(&config.endpoint)
            .with_token(&config.token);
            
        // Add TLS related configurations
        if let Some(ca_path) = &config.ca_path {
            client_builder = client_builder.with_ca_cert(ca_path);
        }
        
        if config.skip_tls_verify {
            client_builder = client_builder.skip_tls_verify();
        }
        
        // Configure client certificates (if provided)
        if let (Some(cert_path), Some(key_path)) = (&config.client_cert_path, &config.client_key_path) {
            client_builder = client_builder.with_client_cert(cert_path, key_path);
        }
        
        // Build the client
        let client = match client_builder.build() {
            Ok(c) => c,
            Err(e) => return Err(Error::ErrKMS(format!("Failed to create Vault client: {}", e))),
        };
            
        Ok(Self {
            client: Arc::new(client),
            key_name: config.key_id.clone(),
        })
    }
    
    /// Set global KMS client instance
    pub fn set_global_client(client: Self) -> Result<(), Error> {
        match KMS_CLIENT.write() {
            Ok(mut global_client) => {
                *global_client = Some(Arc::new(client));
                Ok(())
            },
            Err(_) => Err(Error::ErrKMS("Failed to acquire write lock for global KMS client".to_string()))
        }
    }
    
    /// Initialize the global KMS client
    pub fn init_global_client() -> Result<(), Error> {
        INIT_KMS_CLIENT.call_once(|| {
            if let Some(config) = get_default_kms_config() {
                match Self::new(&config) {
                    Ok(client) => {
                        let _ = KMS_CLIENT.write().unwrap().insert(Arc::new(client));
                    },
                    Err(e) => {
                        error!("Failed to initialize KMS client: {}", e);
                    }
                }
            }
        });
        
        // Check if client is initialized
        if KMS_CLIENT.read().unwrap().is_none() {
            return Err(Error::ErrMissingKMSConfig);
        }
        
        Ok(())
    }
    
    /// Get the global KMS client instance
    pub fn get_global_client() -> Result<Arc<KMSClient>, Error> {
        Self::init_global_client()?;
        
        KMS_CLIENT.read().unwrap()
            .as_ref()
            .cloned()
            .ok_or(Error::ErrMissingKMSConfig)
    }
    
    /// Test client connection and key access
    pub async fn test_connection(&self) -> Result<bool, Error> {
        // Try to get key information to verify client connection and permissions
        let path = format!("transit/keys/{}", self.key_name);
        match self.client.read(&path, None).await {
            Ok(_) => {
                debug!("Successfully verified KMS client connection and key access");
                Ok(true)
            },
            Err(e) => {
                let err_msg = format!("KMS connection test failed: {}", e);
                error!("{}", &err_msg);
                Err(Error::ErrKMS(err_msg))
            }
        }
    }
    
    /// Encrypt data using the KMS key
    pub async fn encrypt(&self, data: &[u8], context: Option<&str>) -> Result<Vec<u8>, Error> {
        let plaintext_b64 = base64::encode(data);
        
        let mut request = Map::new();
        request.insert("plaintext".to_string(), Value::String(plaintext_b64));
        
        // Add context if provided
        if let Some(ctx) = context {
            request.insert("context".to_string(), Value::String(base64::encode(ctx)));
        }
        
        // Call Vault API to encrypt
        let path = format!("transit/encrypt/{}", self.key_name);
        let response = match self.client.write(&path, Some(request)).await {
            Ok(resp) => resp,
            Err(e) => return Err(Error::ErrKMS(format!("KMS encrypt error: {}", e))),
        };
            
        // Extract ciphertext from response - fix ownership issue
        if let Some(response_data) = &response.response_data {
            if let Some(Value::String(ciphertext)) = response_data.get("ciphertext") {
                return Ok(ciphertext.as_bytes().to_vec());
            }
        }
        
        Err(Error::ErrKMS("No ciphertext in response".to_string()))
    }
    
    /// Decrypt data using the KMS key
    pub async fn decrypt(&self, ciphertext: &[u8], context: Option<&str>) -> Result<Vec<u8>, Error> {
        let ciphertext_str = std::str::from_utf8(ciphertext)
            .map_err(|_| Error::ErrInvalidEncryptedDataFormat)?;
            
        let mut request = Map::new();
        request.insert("ciphertext".to_string(), Value::String(ciphertext_str.to_string()));
        
        // Add context if provided
        if let Some(ctx) = context {
            request.insert("context".to_string(), Value::String(base64::encode(ctx)));
        }
        
        // Call Vault API to decrypt
        let path = format!("transit/decrypt/{}", self.key_name);
        let response = match self.client.write(&path, Some(request)).await {
            Ok(resp) => resp,
            Err(e) => return Err(Error::ErrKMS(format!("KMS decrypt error: {}", e))),
        };
            
        // Extract plaintext from response - fix ownership issue
        if let Some(response_data) = &response.response_data {
            if let Some(Value::String(plaintext_b64)) = response_data.get("plaintext") {
                return base64::decode(plaintext_b64).map_err(Error::ErrBase64DecodeError);
            }
        }
        
        Err(Error::ErrKMS("No plaintext in response".to_string()))
    }
}

/// SSEKMSEncryption provides SSE-KMS encryption capabilities
pub struct SSEKMSEncryption {
    kms_client: Arc<KMSClient>,
}

impl SSEKMSEncryption {
    /// Create a new SSEKMSEncryption instance with the default KMS client
    pub fn new() -> Result<Self, Error> {
        let kms_client = KMSClient::get_global_client()?;
        
        Ok(Self {
            kms_client,
        })
    }
    
    /// Create a new SSEKMSEncryption instance with a specific KMS client
    pub fn with_client(kms_client: Arc<KMSClient>) -> Self {
        Self {
            kms_client,
        }
    }
    
    /// Generate a random initialization vector
    fn generate_iv() -> Vec<u8> {
        let mut iv = vec![0u8; 12]; // AES-GCM typically uses 12 bytes IV
        rand::thread_rng().fill_bytes(&mut iv);
        iv
    }
    
    /// Generate a random data key for encrypting the object
    fn generate_data_key() -> Vec<u8> {
        let mut key = vec![0u8; 32]; // 256 bits for AES-256
        rand::thread_rng().fill_bytes(&mut key);
        key
    }
}

#[cfg(feature = "kms")]
impl Encryptable for SSEKMSEncryption {
    /// Encrypt data using KMS-managed keys (SSE-KMS)
    fn encrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // In this synchronous function, we'll need to run the async code in a blocking manner
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            Error::ErrKMS(format!("Failed to create tokio runtime: {}", e))
        })?;
        
        rt.block_on(async {
            // Get KMS key ID from options or use default
            let kms_key_id = options.kms_key_id.as_deref()
                .unwrap_or(&self.kms_client.key_name);
                
            let kms_context = options.kms_context.as_deref();
            
            // Generate a random data key for this object
            let data_key = Self::generate_data_key();
            
            // Generate a random IV for the data encryption
            let iv = Self::generate_iv();
            
            // Create the cipher for encrypting the data
            let key = Key::<Aes256Gcm>::from_slice(&data_key);
            let cipher = Aes256Gcm::new(key);
            let nonce = Nonce::from_slice(&iv);
            
            // Encrypt the data
            let ciphertext = cipher.encrypt(nonce, data)
                .map_err(|e| Error::ErrEncryptFailed(e))?;
            
            // Encrypt the data key with KMS
            let encrypted_key = self.kms_client.encrypt(&data_key, kms_context).await?;
            
            // Create encryption metadata to store with the encrypted data
            let info = EncryptionInfo::new_sse_kms(
                iv,
                encrypted_key,
                kms_key_id.to_string(),
                options.kms_context.clone()
            );
            
            // Format: [metadata length (4 bytes)][metadata JSON][ciphertext]
            let metadata_json = serde_json::to_vec(&info)
                .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
            
            let metadata_len = metadata_json.len() as u32;
            let mut result = Vec::with_capacity(4 + metadata_len as usize + ciphertext.len());
            
            // Add metadata length as big-endian u32
            result.extend_from_slice(&metadata_len.to_be_bytes());
            
            // Add metadata and ciphertext
            result.extend_from_slice(&metadata_json);
            result.extend_from_slice(&ciphertext);
            
            Ok(result)
        })
    }
    
    /// Decrypt data using KMS-managed keys (SSE-KMS)
    fn decrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // In this synchronous function, we'll need to run the async code in a blocking manner
        let rt = tokio::runtime::Runtime::new().map_err(|e| {
            Error::ErrKMS(format!("Failed to create tokio runtime: {}", e))
        })?;
        
        rt.block_on(async {
            // Ensure data is long enough to contain metadata length (4 bytes)
            if data.len() < 4 {
                return Err(Error::ErrInvalidEncryptedDataFormat);
            }
            
            // Extract the metadata length
            let mut metadata_len_bytes = [0u8; 4];
            metadata_len_bytes.copy_from_slice(&data[0..4]);
            let metadata_len = u32::from_be_bytes(metadata_len_bytes) as usize;
            
            // Ensure data is long enough to contain metadata
            if data.len() < 4 + metadata_len {
                return Err(Error::ErrInvalidEncryptedDataFormat);
            }
            
            // Extract and parse metadata
            let metadata_json = &data[4..4 + metadata_len];
            let info: EncryptionInfo = serde_json::from_slice(metadata_json)
                .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
            
            // Verify this is SSE-KMS encrypted data
            if !info.is_sse_kms() {
                return Err(Error::ErrInvalidSSEAlgorithm);
            }
            
            // Extract the encrypted key and ciphertext
            let encrypted_key = info.key.ok_or(Error::ErrEncryptedObjectKeyMissing)?;
            let ciphertext = &data[4 + metadata_len..];
            
            // Get KMS context if available
            let kms_context = info.context.as_deref();
            
            // Decrypt the data key using KMS
            let data_key = self.kms_client.decrypt(&encrypted_key, kms_context).await?;
            
            // Create the cipher for decrypting the data
            let key = Key::<Aes256Gcm>::from_slice(&data_key);
            let cipher = Aes256Gcm::new(key);
            let nonce = Nonce::from_slice(&info.iv);
            
            // Decrypt the data
            let plaintext = cipher.decrypt(nonce, ciphertext)
                .map_err(|e| Error::ErrDecryptFailed(e))?;
            
            Ok(plaintext)
        })
    }
}

// Non-async version for when KMS feature is disabled
#[cfg(not(feature = "kms"))]
impl Encryptable for SSEKMSEncryption {
    fn encrypt(&self, _data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        Err(Error::ErrMissingKMSConfig)
    }
    
    fn decrypt(&self, _data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        Err(Error::ErrMissingKMSConfig)
    }
}