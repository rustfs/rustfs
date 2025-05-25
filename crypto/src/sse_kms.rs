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
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
};
use tracing::{debug, error};
use uuid::Uuid;

// Lazily initialized KMS client
static INIT_KMS_CLIENT: OnceLock<()> = OnceLock::new();
static KMS_CLIENT: Mutex<Option<Arc<RustyVaultKMSClient>>> = Mutex::new(None);

/// RustyVaultKMSClient wraps the RustyVault client for direct key management operations
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
    pub async fn generate_data_key(&self, context: Option<HashMap<String, String>>) -> Result<(Vec<u8>, Vec<u8>), Error> {
        // For now, generate a random key and return it
        // In a real implementation, this would call RustyVault's API
        let mut key = vec![0u8; 32]; // AES-256 key
        rand::thread_rng().fill_bytes(&mut key);
        
        // Mock encrypted key (in practice this would be encrypted by RustyVault)
        let encrypted_key = format!("vault:v1:{}", general_purpose::STANDARD.encode(&key));
        
        Ok((key, encrypted_key.as_bytes().to_vec()))
    }
    
    /// Encrypt data using RustyVault's transit engine
    pub async fn encrypt(&self, data: &[u8], context: Option<HashMap<String, String>>) -> Result<Vec<u8>, Error> {
        // Mock implementation - in practice this would call RustyVault
        let plaintext_b64 = general_purpose::STANDARD.encode(data);
        let ciphertext = format!("vault:v1:{}", plaintext_b64);
        Ok(ciphertext.as_bytes().to_vec())
    }
    
    /// Decrypt data using RustyVault's transit engine
    pub async fn decrypt(&self, ciphertext: &[u8], context: Option<HashMap<String, String>>) -> Result<Vec<u8>, Error> {
        // Mock implementation - in practice this would call RustyVault
        let ciphertext_str = std::str::from_utf8(ciphertext)
            .map_err(|_| Error::ErrInvalidEncryptedDataFormat)?;
            
        if ciphertext_str.starts_with("vault:v1:") {
            let data_b64 = &ciphertext_str[9..]; // Remove "vault:v1:" prefix
            if data_b64.len() == 44 { // Base64 encoded 32-byte key
                // This is a data key, decode it directly
                general_purpose::STANDARD.decode(data_b64)
                    .map_err(|_| Error::ErrKMS("Failed to decode data key".to_string()))
            } else {
                // This is encrypted data, decode it
                general_purpose::STANDARD.decode(data_b64)
                    .map_err(|_| Error::ErrKMS("Failed to decode encrypted data".to_string()))
            }
        } else {
            Err(Error::ErrInvalidEncryptedDataFormat)
        }
    }
    
    /// Test connection to RustyVault and verify key access
    pub async fn test_connection(&self) -> Result<bool, Error> {
        // Mock implementation - always return success for now
        debug!("Mock RustyVault connection test successful");
        Ok(true)
    }
}

/// SSEKMSEncryption provides SSE-KMS encryption using RustyVault
pub struct SSEKMSEncryption {
    vault_client: Arc<RustyVaultKMSClient>,
}

impl SSEKMSEncryption {
    /// Create a new SSEKMSEncryption instance with the default RustyVault client
    pub fn new() -> Result<Self, Error> {
        let vault_client = RustyVaultKMSClient::get_global_client()?;
        
        Ok(Self {
            vault_client,
        })
    }
    
    /// Create a new SSEKMSEncryption instance with a specific RustyVault client
    pub fn with_client(vault_client: Arc<RustyVaultKMSClient>) -> Self {
        Self {
            vault_client,
        }
    }
    
    /// Generate a random initialization vector for AES-256-GCM
    fn generate_iv() -> Vec<u8> {
        let mut iv = vec![0u8; 12]; // AES-GCM standard IV size
        rand::thread_rng().fill_bytes(&mut iv);
        iv
    }
    
    /// Encrypt data for a single part/chunk (MinIO-style per-part encryption)
    pub async fn encrypt_part(&self, data: &[u8], part_number: Option<usize>, upload_id: Option<&str>) -> Result<(Vec<u8>, EncryptionInfo), Error> {
        // Create context for this specific part
        let mut context = HashMap::new();
        context.insert("algorithm".to_string(), "AES256-GCM".to_string());
        context.insert("purpose".to_string(), "object-encryption".to_string());
        
        if let Some(part_num) = part_number {
            context.insert("part_number".to_string(), part_num.to_string());
        }
        if let Some(upload) = upload_id {
            context.insert("upload_id".to_string(), upload.to_string());
        }
        
        // Generate data key using RustyVault
        let (data_key, encrypted_key) = self.vault_client.generate_data_key(Some(context.clone())).await?;
        
        // Generate IV for this encryption
        let iv = Self::generate_iv();
        
        // Encrypt the data using AES-256-GCM (MinIO标准)
        let key = Key::<Aes256Gcm>::from_slice(&data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&iv);
        
        let ciphertext = cipher.encrypt(nonce, data)
            .map_err(|e| Error::ErrEncryptFailed(e))?;
        
        // Create encryption metadata for storage in HTTP headers (MinIO方式)
        let info = EncryptionInfo::new_sse_kms(
            iv,
            encrypted_key,
            self.vault_client.key_name.clone(),
            Some(serde_json::to_string(&context).unwrap_or_default())
        );
        
        Ok((ciphertext, info))
    }
    
    /// Decrypt data for a single part/chunk
    pub async fn decrypt_part(&self, data: &[u8], info: &EncryptionInfo) -> Result<Vec<u8>, Error> {
        // Verify this is SSE-KMS encrypted data
        if !info.is_sse_kms() {
            return Err(Error::ErrInvalidSSEAlgorithm);
        }
        
        let encrypted_key = info.key.as_ref()
            .ok_or(Error::ErrEncryptedObjectKeyMissing)?;
        
        // Parse context if present
        let context: Option<HashMap<String, String>> = info.context.as_ref()
            .and_then(|ctx| serde_json::from_str(ctx).ok());
        
        // Decrypt the data key using RustyVault
        let data_key = self.vault_client.decrypt(encrypted_key, context).await?;
        
        // Decrypt the data using AES-256-GCM
        let key = Key::<Aes256Gcm>::from_slice(&data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&info.iv);
        
        let plaintext = cipher.decrypt(nonce, data)
            .map_err(|e| Error::ErrDecryptFailed(e))?;
        
        Ok(plaintext)
    }
    
    /// Convert EncryptionInfo to HTTP headers (MinIO compatible)
    pub fn metadata_to_headers(&self, info: &EncryptionInfo) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        
        // 标准SSE-KMS headers
        headers.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
        headers.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), 
                      info.key_id.clone().unwrap_or_default());
        
        if let Some(context) = &info.context {
            headers.insert("x-amz-server-side-encryption-context".to_string(), 
                          general_purpose::STANDARD.encode(context.as_bytes()));
        }
        
        // 存储加密元数据在自定义headers中
        headers.insert("x-amz-meta-sse-kms-encrypted-key".to_string(), 
                      general_purpose::STANDARD.encode(info.key.as_ref().unwrap_or(&vec![])));
        headers.insert("x-amz-meta-sse-kms-iv".to_string(), 
                      general_purpose::STANDARD.encode(&info.iv));
        headers.insert("x-amz-meta-sse-kms-algorithm".to_string(), 
                      info.algorithm.to_string());
        
        headers
    }
    
    /// Extract EncryptionInfo from HTTP headers
    pub fn metadata_from_headers(&self, headers: &HashMap<String, String>) -> Result<EncryptionInfo, Error> {
        // 验证这是SSE-KMS加密
        let encryption_type = headers.get("x-amz-server-side-encryption")
            .ok_or(Error::ErrInvalidEncryptionMetadata)?;
        
        if encryption_type != "aws:kms" {
            return Err(Error::ErrInvalidSSEAlgorithm);
        }
        
        let key_id = headers.get("x-amz-server-side-encryption-aws-kms-key-id")
            .ok_or(Error::ErrInvalidEncryptionMetadata)?
            .clone();
        
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
        
        Ok(EncryptionInfo::new_sse_kms(iv, encrypted_key, key_id, context))
    }
}

#[cfg(feature = "kms")]
impl Encryptable for SSEKMSEncryption {
    /// Encrypt data using RustyVault KMS (legacy interface for backward compatibility)
    fn encrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // 对于完整对象加密，使用async runtime
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::ErrKMS(format!("Failed to create tokio runtime: {}", e)))?;
        
        rt.block_on(async {
            let (ciphertext, info) = self.encrypt_part(data, None, None).await?;
            
            // 将元数据嵌入到结果中（为了向后兼容）
            let metadata_json = serde_json::to_vec(&info)
                .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
            
            let metadata_len = metadata_json.len() as u32;
            let mut result = Vec::with_capacity(4 + metadata_len as usize + ciphertext.len());
            
            result.extend_from_slice(&metadata_len.to_be_bytes());
            result.extend_from_slice(&metadata_json);
            result.extend_from_slice(&ciphertext);
            
            Ok(result)
        })
    }
    
    /// Decrypt data using RustyVault KMS (legacy interface for backward compatibility)
    fn decrypt(&self, data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
        let rt = tokio::runtime::Runtime::new()
            .map_err(|e| Error::ErrKMS(format!("Failed to create tokio runtime: {}", e)))?;
        
        rt.block_on(async {
            // 解析嵌入的元数据
            if data.len() < 4 {
                return Err(Error::ErrInvalidEncryptedDataFormat);
            }
            
            let mut metadata_len_bytes = [0u8; 4];
            metadata_len_bytes.copy_from_slice(&data[0..4]);
            let metadata_len = u32::from_be_bytes(metadata_len_bytes) as usize;
            
            if data.len() < 4 + metadata_len {
                return Err(Error::ErrInvalidEncryptedDataFormat);
            }
            
            let metadata_json = &data[4..4 + metadata_len];
            let info: EncryptionInfo = serde_json::from_slice(metadata_json)
                .map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
            
            let ciphertext = &data[4 + metadata_len..];
            
            self.decrypt_part(ciphertext, &info).await
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