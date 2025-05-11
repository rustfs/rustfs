// sse_s3.rs - Implementation of SSE-S3 (Server-Side Encryption with Server-Managed Keys)
// This file implements encryption/decryption using server-managed keys

use crate::{
    Error,
    metadata::EncryptionInfo,
    sse::{SSEOptions, Encryptable}
};
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit},
    Aes256Gcm, Key, Nonce
};
use rand::RngCore;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Once};
use lazy_static::lazy_static;

// Master key for SSE-S3 (this key encrypts the per-object keys)
static MASTER_KEY: OnceLock<Arc<Vec<u8>>> = OnceLock::new();
static INIT_MASTER_KEY: Once = Once::new();

/// Initialize the SSE-S3 master key with provided key
pub fn init_master_key(key: Vec<u8>) {
    INIT_MASTER_KEY.call_once(|| {
        if key.len() == 32 {
            let _ = MASTER_KEY.set(Arc::new(key));
        }
    });
}

/// Generate a random master key if none is set
fn ensure_master_key() -> Arc<Vec<u8>> {
    INIT_MASTER_KEY.call_once(|| {
        let mut key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        let _ = MASTER_KEY.set(Arc::new(key));
    });
    
    MASTER_KEY.get().unwrap().clone()
}

/// SSES3Encryption provides SSE-S3 encryption capabilities
#[derive(Default)]
pub struct SSES3Encryption {
    keys_cache: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl SSES3Encryption {
    /// Create a new SSES3Encryption instance
    pub fn new() -> Self {
        Self {
            keys_cache: Arc::new(Mutex::new(Vec::new())),
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
    
    /// Encrypt a data key using the master key
    fn encrypt_data_key(&self, data_key: &[u8]) -> Result<Vec<u8>, Error> {
        let master_key = ensure_master_key();
        
        // Generate a random IV for the key encryption
        let iv = Self::generate_iv();
        
        // Create the cipher for encrypting the data key
        let key = Key::<Aes256Gcm>::from_slice(&master_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&iv);
        
        // Encrypt the data key
        let mut encrypted_key = cipher.encrypt(nonce, data_key)
            .map_err(|e| Error::ErrEncryptFailed(e))?;
        
        // Format: [iv_length (1 byte)][iv][encrypted_key]
        let mut result = Vec::with_capacity(1 + iv.len() + encrypted_key.len());
        result.push(iv.len() as u8);
        result.extend_from_slice(&iv);
        result.append(&mut encrypted_key);
        
        Ok(result)
    }
    
    /// Decrypt an encrypted data key using the master key
    fn decrypt_data_key(&self, encrypted_key_data: &[u8]) -> Result<Vec<u8>, Error> {
        let master_key = ensure_master_key();
        
        // Ensure the data is long enough to contain the IV length
        if encrypted_key_data.is_empty() {
            return Err(Error::ErrInvalidEncryptedDataFormat);
        }
        
        // Extract IV length and IV
        let iv_len = encrypted_key_data[0] as usize;
        if encrypted_key_data.len() < 1 + iv_len {
            return Err(Error::ErrInvalidEncryptedDataFormat);
        }
        
        let iv = &encrypted_key_data[1..1+iv_len];
        let encrypted_key = &encrypted_key_data[1+iv_len..];
        
        // Create the cipher for decrypting the data key
        let key = Key::<Aes256Gcm>::from_slice(&master_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(iv);
        
        // Decrypt the data key
        let data_key = cipher.decrypt(nonce, encrypted_key)
            .map_err(|e| Error::ErrDecryptFailed(e))?;
        
        Ok(data_key)
    }
}

impl Encryptable for SSES3Encryption {
    /// Encrypt data using server-managed keys (SSE-S3)
    fn encrypt(&self, data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
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
        
        // Encrypt the data key with the master key
        let encrypted_key = self.encrypt_data_key(&data_key)?;
        
        // Create encryption metadata to store with the encrypted data
        let info = EncryptionInfo::new_sse_s3(iv, encrypted_key);
        
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
    }
    
    /// Decrypt data using server-managed keys (SSE-S3)
    fn decrypt(&self, data: &[u8], _options: &SSEOptions) -> Result<Vec<u8>, Error> {
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
        
        // Verify this is SSE-S3 encrypted data
        if !info.is_sse_s3() {
            return Err(Error::ErrInvalidSSEAlgorithm);
        }
        
        // Extract the encrypted key and ciphertext
        let encrypted_key = info.key.ok_or(Error::ErrEncryptedObjectKeyMissing)?;
        let ciphertext = &data[4 + metadata_len..];
        
        // Decrypt the data key
        let data_key = self.decrypt_data_key(&encrypted_key)?;
        
        // Create the cipher for decrypting the data
        let key = Key::<Aes256Gcm>::from_slice(&data_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&info.iv);
        
        // Decrypt the data
        let plaintext = cipher.decrypt(nonce, ciphertext)
            .map_err(|e| Error::ErrDecryptFailed(e))?;
        
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_sse_s3_encrypt_decrypt() {
        // Ensure a deterministic master key for testing
        let test_master_key = vec![1u8; 32];
        init_master_key(test_master_key.clone());
        
        // Create test data
        let data = b"This is some test data to encrypt with SSE-S3";
        
        // Create encryption options (mostly unused for SSE-S3)
        let options = SSEOptions::default();
        
        // Encrypt
        let sse_s3 = SSES3Encryption::new();
        let encrypted = sse_s3.encrypt(data, &options).unwrap();
        
        // Decrypt
        let decrypted = sse_s3.decrypt(&encrypted, &options).unwrap();
        
        // Verify
        assert_eq!(decrypted, data);
    }
    
    #[test]
    fn test_sse_s3_data_key_encryption() {
        // Ensure a deterministic master key for testing
        let test_master_key = vec![1u8; 32];
        init_master_key(test_master_key.clone());
        
        // Create a data key
        let data_key = vec![5u8; 32];
        
        // Encrypt the data key
        let sse_s3 = SSES3Encryption::new();
        let encrypted_key = sse_s3.encrypt_data_key(&data_key).unwrap();
        
        // Decrypt the data key
        let decrypted_key = sse_s3.decrypt_data_key(&encrypted_key).unwrap();
        
        // Verify
        assert_eq!(decrypted_key, data_key);
    }
    
    #[test]
    fn test_sse_s3_master_key_auto_generation() {
        // Reset the once cell for testing (this is a hack that works only for tests)
        unsafe {
            // This is unsafe but necessary for testing the auto-generation of master keys
            let once = &INIT_MASTER_KEY as *const Once as *mut Once;
            std::ptr::write(once, Once::new());
            MASTER_KEY = OnceLock::new();
        }
        
        // Create test data
        let data = b"This is some test data to encrypt with auto-generated master key";
        
        // Create encryption options
        let options = SSEOptions::default();
        
        // Encrypt (this should auto-generate a master key)
        let sse_s3 = SSES3Encryption::new();
        let encrypted = sse_s3.encrypt(data, &options).unwrap();
        
        // Decrypt
        let decrypted = sse_s3.decrypt(&encrypted, &options).unwrap();
        
        // Verify
        assert_eq!(decrypted, data);
    }
}