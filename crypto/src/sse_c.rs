// sse_c.rs - Implementation of SSE-C (Server-Side Encryption with Customer-Provided Keys)
// This file implements encryption/decryption using customer-provided keys

use crate::{
    Error,
    metadata::EncryptionInfo,
    sse::{SSEOptions, Encryptable}
};
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use rand::RngCore;

/// SSECEncryption provides SSE-C encryption capabilities
#[derive(Default)]
pub struct SSECEncryption;

impl SSECEncryption {
    /// Create a new SSECEncryption instance
    pub fn new() -> Self {
        Self {}
    }
    
    /// Generate a random initialization vector
    fn generate_iv() -> Vec<u8> {
        let mut iv = vec![0u8; 12]; // AES-GCM typically uses 12 bytes IV
        rand::thread_rng().fill_bytes(&mut iv);
        iv
    }
}

impl Encryptable for SSECEncryption {
    /// Encrypt data using customer-provided key (SSE-C)
    /// 
    /// The customer key must be provided in the options
    fn encrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // Ensure we have a customer key
        let customer_key = options.customer_key.as_ref()
            .ok_or(Error::ErrMissingSSEKey)?;
        
        // AES-256 requires a 32-byte key
        if customer_key.len() != 32 {
            return Err(Error::ErrInvalidSSECustomerKey);
        }
        
        // Generate a random IV
        let iv = Self::generate_iv();
        
        // Create the cipher
        let key = Key::<Aes256Gcm>::from_slice(customer_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&iv);
        
        // Encrypt the data
        let ciphertext = cipher.encrypt(nonce, data)
            .map_err(Error::ErrEncryptFailed)?;
        
        // Create encryption metadata to store with the encrypted data
        let info = EncryptionInfo::new_ssec(iv);
        
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
    
    /// Decrypt data using customer-provided key (SSE-C)
    /// 
    /// The customer key must be provided in the options
    fn decrypt(&self, data: &[u8], options: &SSEOptions) -> Result<Vec<u8>, Error> {
        // Ensure we have a customer key
        let customer_key = options.customer_key.as_ref()
            .ok_or(Error::ErrMissingSSEKey)?;
        
        // AES-256 requires a 32-byte key
        if customer_key.len() != 32 {
            return Err(Error::ErrInvalidSSECustomerKey);
        }
        
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
        
        // Verify this is SSE-C encrypted data
        if !info.is_ssec() {
            return Err(Error::ErrInvalidSSEAlgorithm);
        }
        
        // Extract ciphertext
        let ciphertext = &data[4 + metadata_len..];
        
        // Create the cipher
        let key = Key::<Aes256Gcm>::from_slice(customer_key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&info.iv);
        
        // Decrypt the data
        let plaintext = cipher.decrypt(nonce, ciphertext)
            .map_err(Error::ErrDecryptFailed)?;
        
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_ssec_encrypt_decrypt() {
        // Generate a random 32-byte key
        let mut customer_key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut customer_key);
        
        let options = SSEOptions {
            customer_key: Some(customer_key.clone()),
            ..Default::default()
        };
        
        // Create test data
        let data = b"This is some test data to encrypt with SSE-C";
        
        // Encrypt
        let ssec = SSECEncryption::new();
        let encrypted = ssec.encrypt(data, &options).expect();
        
        // Decrypt
        let decrypted = ssec.decrypt(&encrypted, &options).expect();
        
        // Verify
        assert_eq!(decrypted, data);
    }
    
    #[test]
    fn test_ssec_wrong_key() {
        // Generate a random 32-byte key
        let mut customer_key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut customer_key);
        
        let options = SSEOptions {
            customer_key: Some(customer_key.clone()),
            ..Default::default()
        };
        
        // Create test data
        let data = b"This is some test data to encrypt with SSE-C";
        
        // Encrypt
        let ssec = SSECEncryption::new();
        let encrypted = ssec.encrypt(data, &options).expect();
        
        // Attempt to decrypt with wrong key
        let mut wrong_key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut wrong_key);
        
        let wrong_options = SSEOptions {
            customer_key: Some(wrong_key),
            ..Default::default()
        };
        
        // Should fail
        let result = ssec.decrypt(&encrypted, &wrong_options);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_ssec_missing_key() {
        // Create test data
        let data = b"This is some test data to encrypt with SSE-C";
        
        // Try to encrypt without key
        let ssec = SSECEncryption::new();
        let options = SSEOptions::default();
        
        let result = ssec.encrypt(data, &options);
        assert!(result.is_err());
    }
}