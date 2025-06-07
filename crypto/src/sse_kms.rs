// sse_kms.rs - Implementation of SSE-KMS using RustyVault directly
// This file implements encryption/decryption using RustyVault KMS

use crate::{
    error::Error,
    metadata::EncryptionInfo,
    sse::{SSEOptions, Encryptable},
};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use base64::{Engine as _, engine::general_purpose};
use rand::RngCore;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::debug;

// Lazily initialized KMS client
#[allow(dead_code)]
static INIT_KMS_CLIENT: OnceLock<()> = OnceLock::new();
static KMS_CLIENT: Mutex<Option<Arc<RustyVaultKMSClient>>> = Mutex::new(None);

/// RustyVaultKMSClient wraps the RustyVault client for direct key management operations
#[allow(dead_code)]
#[derive(Clone)]
pub struct RustyVaultKMSClient {
    endpoint: String,
    token: String,
    key_name: String,
    mount_path: String,
}

impl RustyVaultKMSClient {
    /// Create a new RustyVault client with the given configuration
    pub fn new(endpoint: String, token: String, key_name: String) -> Self {
        Self {
            endpoint,
            token,
            key_name,
            mount_path: "transit".to_string(),
        }
    }
    
    /// Set global KMS client instance
    pub fn set_global_client(client: Self) -> Result<(), Error> {
        match KMS_CLIENT.lock() {
            Ok(mut global_client) => {
                *global_client = Some(Arc::new(client));
                Ok(())
            },
            Err(_) => Err(Error::ErrKMS("Failed to acquire write lock for global KMS client".to_string()))
        }
    }
    
    /// Get the global KMS client instance
    pub fn get_global_client() -> Result<Arc<RustyVaultKMSClient>, Error> {
        crate::sse::ensure_kms_client()?;
        
        match KMS_CLIENT.lock() {
            Ok(client_guard) => {
                client_guard
                    .as_ref()
                    .cloned()
                    .ok_or(Error::ErrMissingKMSConfig)
            },
            Err(_) => Err(Error::ErrKMS("Failed to acquire read lock for global KMS client".to_string()))
        }
    }
    
    /// Generate a data encryption key using RustyVault's transit engine
    pub async fn generate_data_key(&self, _context: Option<HashMap<String, String>>) -> Result<(Vec<u8>, Vec<u8>), Error> {
        // Generate a random 256-bit data key
        let mut data_key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut data_key);
        
        // For now, use a simple approach - encrypt the data key with a master key
        // In a real implementation, this would use RustyVault's transit engine
        let encrypted_key = self.encrypt_with_transit(&data_key).await?;
        
        Ok((data_key, encrypted_key))
    }
    
    /// Encrypt data using RustyVault's transit engine
    pub async fn encrypt(&self, data: &[u8], _context: Option<HashMap<String, String>>) -> Result<Vec<u8>, Error> {
        self.encrypt_with_transit(data).await
    }
    
    /// Decrypt data using RustyVault's transit engine
    pub async fn decrypt(&self, encrypted_data: &[u8], _context: Option<HashMap<String, String>>) -> Result<Vec<u8>, Error> {
        self.decrypt_with_transit(encrypted_data).await
    }
    
    /// Encrypt data key with RustyVault transit engine (simplified implementation)
    async fn encrypt_with_transit(&self, data: &[u8]) -> Result<Vec<u8>, Error> {
        // This is a simplified implementation
        // In a real scenario, this would make HTTP requests to RustyVault's transit engine
        debug!("Encrypting data with RustyVault transit engine");
        
        // For now, return the data with a simple transformation
        // TODO: Implement actual RustyVault transit engine calls
        let mut encrypted = Vec::with_capacity(data.len() + 16);
        encrypted.extend_from_slice(b"vault:v1:");
        encrypted.extend_from_slice(data);
        
        Ok(encrypted)
    }
    
    /// Decrypt data key with RustyVault transit engine (simplified implementation)
    async fn decrypt_with_transit(&self, encrypted_data: &[u8]) -> Result<Vec<u8>, Error> {
        debug!("Decrypting data with RustyVault transit engine");
        
        // 解析加密的数据格式
        if encrypted_data.len() < 9 {
            return Err(Error::ErrInvalidDataFormat("Invalid vault encrypted data format".to_string()));
        }

        // Simple implementation - remove the vault prefix
        if encrypted_data.starts_with(b"vault:v1:") {
            Ok(encrypted_data[9..].to_vec())
        } else {
            Err(Error::ErrInvalidDataFormat("Invalid vault encrypted data format".to_string()))
        }
    }
    
    /// Generate a random initialization vector
    fn generate_iv() -> Vec<u8> {
        let mut iv = vec![0u8; 12]; // AES-GCM typically uses 12 bytes IV
        rand::thread_rng().fill_bytes(&mut iv);
        iv
    }
    
    /// Encrypt data for a single part/chunk (MinIO-style per-part encryption)
    pub async fn encrypt_part(&self, data: &[u8], _part_number: Option<usize>, _upload_id: Option<&str>) -> Result<(Vec<u8>, EncryptionInfo), Error> {
        let (data_key, encrypted_key) = self.generate_data_key(None).await?;
        let iv = Self::generate_iv();
        
        // Encrypt the data using AES-256-GCM (MinIO标准)
        let key = Key::<Aes256Gcm>::from_slice(&data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&iv);
        
        let ciphertext = cipher.encrypt(nonce, data)
            .map_err(Error::ErrEncryptFailed)?;
        
        // Create encryption metadata for storage in HTTP headers (MinIO方式)
        let info = EncryptionInfo::new_sse_kms(
            iv,
            encrypted_key,
            self.key_name.clone(),
            None, // context
        );
        
        Ok((ciphertext, info))
    }
    
    /// Decrypt data for a single part/chunk
    pub async fn decrypt_part(&self, encrypted_data: &[u8], info: &EncryptionInfo) -> Result<Vec<u8>, Error> {
        // Decrypt the data key first
        let data_key = if let Some(encrypted_key) = &info.key {
            self.decrypt(encrypted_key, None).await?
        } else {
            return Err(Error::ErrMissingEncryptedKey);
        };
        
        // Decrypt the data
        let key = Key::<Aes256Gcm>::from_slice(&data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&info.iv);
        
        let plaintext = cipher.decrypt(nonce, encrypted_data)
            .map_err(Error::ErrDecryptFailed)?;
        
        Ok(plaintext)
    }
}

/// SSEKMSEncryption provides SSE-KMS encryption using RustyVault
pub struct SSEKMSEncryption {
    #[allow(dead_code)]
    vault_client: Arc<RustyVaultKMSClient>,
}

impl SSEKMSEncryption {
    /// Create a new SSEKMSEncryption instance
    pub fn new() -> Result<Self, Error> {
        let client = RustyVaultKMSClient::get_global_client()?;
        Ok(Self {
            vault_client: client,
        })
    }
    
    /// Create with specific client (for testing)
    pub fn with_client(client: Arc<RustyVaultKMSClient>) -> Self {
        Self {
            vault_client: client,
        }
    }
    
    /// Encrypt data for a single part/chunk - forwarded to vault client
    pub async fn encrypt_part(&self, data: &[u8], part_number: Option<usize>, upload_id: Option<&str>) -> Result<(Vec<u8>, EncryptionInfo), Error> {
        self.vault_client.encrypt_part(data, part_number, upload_id).await
    }
    
    /// Decrypt data for a single part/chunk - forwarded to vault client
    pub async fn decrypt_part(&self, encrypted_data: &[u8], info: &EncryptionInfo) -> Result<Vec<u8>, Error> {
        self.vault_client.decrypt_part(encrypted_data, info).await
    }
    
    /// Convert EncryptionInfo to HTTP headers (MinIO compatible)
    pub fn metadata_to_headers(&self, info: &EncryptionInfo) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        
        // 标准SSE-KMS headers (S3兼容)
        headers.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
        if let Some(key_id) = &info.key_id {
            headers.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), key_id.clone());
        }
        
        if let Some(context) = &info.context {
            headers.insert("x-amz-server-side-encryption-context".to_string(), 
                          general_purpose::STANDARD.encode(context.as_bytes()));
        }
        
        // MinIO兼容的元数据存储 - 使用标准的x-amz-meta前缀
        if let Some(encrypted_key) = &info.key {
            headers.insert("x-amz-meta-sse-kms-encrypted-key".to_string(), 
                          general_purpose::STANDARD.encode(encrypted_key));
        }
        headers.insert("x-amz-meta-sse-kms-iv".to_string(), 
                      general_purpose::STANDARD.encode(&info.iv));
        headers.insert("x-amz-meta-sse-kms-algorithm".to_string(), 
                      info.algorithm.to_string());
        
        headers
    }
    
    /// Extract EncryptionInfo from HTTP headers
    pub fn metadata_from_headers(&self, headers: &HashMap<String, String>) -> Result<EncryptionInfo, Error> {
        // 验证是否为SSE-KMS
        let sse_type = headers.get("x-amz-server-side-encryption")
            .ok_or(Error::ErrInvalidEncryptionMetadata)?;
        if sse_type != "aws:kms" {
            return Err(Error::ErrInvalidEncryptionMetadata);
        }
        
        let key_id = headers.get("x-amz-server-side-encryption-aws-kms-key-id")
            .cloned();
        
        // 从MinIO兼容的meta headers中提取加密信息
        let encrypted_key_b64 = headers.get("x-amz-meta-sse-kms-encrypted-key")
            .ok_or(Error::ErrInvalidEncryptionMetadata)?;
        let encrypted_key = general_purpose::STANDARD.decode(encrypted_key_b64)
            .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
        
        let iv_b64 = headers.get("x-amz-meta-sse-kms-iv")
            .ok_or(Error::ErrInvalidEncryptionMetadata)?;
        let iv = general_purpose::STANDARD.decode(iv_b64)
            .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
        
        let context = headers.get("x-amz-server-side-encryption-context")
            .and_then(|ctx_b64| {
                general_purpose::STANDARD.decode(ctx_b64).ok()
                    .and_then(|decoded| String::from_utf8(decoded).ok())
            });
        
        Ok(EncryptionInfo::new_sse_kms(iv, encrypted_key, key_id.unwrap_or_default(), context))
    }
}

#[cfg(feature = "kms")]
impl Encryptable for SSEKMSEncryption {
    /// Encrypt data using KMS-managed keys
    fn encrypt(&self, data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // This is a sync wrapper around async function
        // In practice, you might want to use a runtime or make this async
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                let (ciphertext, info) = self.vault_client.encrypt_part(data, None, None).await?;
                
                // Create metadata and prepend to ciphertext (MinIO style)
                let metadata = serde_json::to_vec(&info)?;
                let metadata_len = metadata.len() as u32;
                
                let mut result = Vec::new();
                result.extend_from_slice(&metadata_len.to_be_bytes());
                result.extend_from_slice(&metadata);
                result.extend_from_slice(&ciphertext);
                
                Ok(result)
            })
        })
    }
    
    /// Decrypt data using KMS-managed keys
    fn decrypt(&self, data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        tokio::task::block_in_place(|| {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(async {
                // Extract metadata from the beginning of data
                if data.len() < 4 {
                    return Err(Error::ErrInvalidEncryptionMetadata);
                }
                
                let metadata_len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
                if data.len() < 4 + metadata_len {
                    return Err(Error::ErrInvalidEncryptionMetadata);
                }
                
                let metadata_bytes = &data[4..4 + metadata_len];
                let info: EncryptionInfo = serde_json::from_slice(metadata_bytes)?;
                
                let ciphertext = &data[4 + metadata_len..];
                
                self.vault_client.decrypt_part(ciphertext, &info).await
            })
        })
    }
}

// 当KMS feature未启用时的实现
#[cfg(not(feature = "kms"))]
impl Encryptable for SSEKMSEncryption {
    fn encrypt(&self, _data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        Err(Error::ErrMissingKMSConfig)
    }
    
    fn decrypt(&self, _data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        Err(Error::ErrMissingKMSConfig)
    }
}

// 为了方便使用，重新导出RustyVaultKMSClient为KMSClient
pub use RustyVaultKMSClient as KMSClient;