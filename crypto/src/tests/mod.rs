// SSE encryption integration tests
use crate::{
    SSE, Algorithm, SSEOptions, Encryptable,
    SSECEncryption, SSES3Encryption, CryptoFactory,
    EncryptionInfo, extract_encryption_metadata, remove_encryption_metadata
};
use rand::RngCore;
use std::collections::HashMap;

#[cfg(test)]
mod tests {
    use super::*;
    
    // Helper function to generate a random customer key
    fn generate_customer_key() -> Vec<u8> {
        let mut key = vec![0u8; 32];
        rand::thread_rng().fill_bytes(&mut key);
        key
    }
    
    #[test]
    fn test_sse_c_workflow() {
        // Generate a customer key
        let customer_key = generate_customer_key();
        
        // Create test data
        let data = b"This is test data for SSE-C encryption";
        
        // Create SSE-C options
        let options = SSEOptions {
            sse_type: Some(SSE::SSEC),
            algorithm: Some(Algorithm::AES256),
            customer_key: Some(customer_key.clone()),
            ..Default::default()
        };
        
        // Encrypt with SSE-C
        let ssec = SSECEncryption::new();
        let encrypted = ssec.encrypt(data, &options).unwrap();
        
        // Decrypt with SSE-C
        let decrypted = ssec.decrypt(&encrypted, &options).unwrap();
        
        // Verify
        assert_eq!(decrypted, data);
    }
    
    #[test]
    fn test_sse_s3_workflow() {
        // Set a deterministic master key for testing
        let master_key = vec![1u8; 32];
        crate::init_master_key(master_key);
        
        // Create test data
        let data = b"This is test data for SSE-S3 encryption";
        
        // Create SSE-S3 options
        let options = SSEOptions {
            sse_type: Some(SSE::SSES3),
            algorithm: Some(Algorithm::AES256),
            ..Default::default()
        };
        
        // Encrypt with SSE-S3
        let sses3 = SSES3Encryption::new();
        let encrypted = sses3.encrypt(data, &options).unwrap();
        
        // Decrypt with SSE-S3
        let decrypted = sses3.decrypt(&encrypted, &options).unwrap();
        
        // Verify
        assert_eq!(decrypted, data);
    }
    
    #[test]
    fn test_crypto_factory() {
        // Generate a customer key
        let customer_key = generate_customer_key();
        
        // Set a deterministic master key for testing
        let master_key = vec![1u8; 32];
        crate::init_master_key(master_key);
        
        // Test data
        let data = b"This is test data for CryptoFactory";
        
        // Test with SSE-C
        let ssec_options = SSEOptions {
            sse_type: Some(SSE::SSEC),
            algorithm: Some(Algorithm::AES256),
            customer_key: Some(customer_key.clone()),
            ..Default::default()
        };
        
        #[cfg(not(feature = "kms"))]
        {
            let encryptor = CryptoFactory::create_encryptor(Some(SSE::SSEC));
            let encrypted = encryptor.encrypt(data, &ssec_options).unwrap();
            let decrypted = encryptor.decrypt(&encrypted, &ssec_options).unwrap();
            assert_eq!(decrypted, data);
        }
        
        #[cfg(feature = "kms")]
        {
            let encryptor = CryptoFactory::create_encryptor(Some(SSE::SSEC)).unwrap();
            let encrypted = encryptor.encrypt(data, &ssec_options).unwrap();
            let decrypted = encryptor.decrypt(&encrypted, &ssec_options).unwrap();
            assert_eq!(decrypted, data);
        }
        
        // Test with SSE-S3
        let sses3_options = SSEOptions {
            sse_type: Some(SSE::SSES3),
            algorithm: Some(Algorithm::AES256),
            ..Default::default()
        };
        
        #[cfg(not(feature = "kms"))]
        {
            let encryptor = CryptoFactory::create_encryptor(Some(SSE::SSES3));
            let encrypted = encryptor.encrypt(data, &sses3_options).unwrap();
            let decrypted = encryptor.decrypt(&encrypted, &sses3_options).unwrap();
            assert_eq!(decrypted, data);
        }
        
        #[cfg(feature = "kms")]
        {
            let encryptor = CryptoFactory::create_encryptor(Some(SSE::SSES3)).unwrap();
            let encrypted = encryptor.encrypt(data, &sses3_options).unwrap();
            let decrypted = encryptor.decrypt(&encrypted, &sses3_options).unwrap();
            assert_eq!(decrypted, data);
        }
    }
    
    #[test]
    fn test_metadata_handling() {
        // Create encryption info for testing
        let iv = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let key = vec![11, 22, 33, 44, 55];
        
        let info = EncryptionInfo::new_sse_s3(iv.clone(), key.clone());
        
        // Convert to metadata
        let metadata = info.to_metadata();
        
        // Extract encryption metadata
        let extracted = extract_encryption_metadata(&metadata);
        
        // Check that extracted contains only encryption metadata
        assert_eq!(extracted.len(), metadata.len());
        
        // Create mixed metadata
        let mut mixed_metadata = metadata.clone();
        mixed_metadata.insert("X-Normal-Meta".to_string(), "normal-value".to_string());
        
        // Extract encryption metadata
        let extracted_from_mixed = extract_encryption_metadata(&mixed_metadata);
        
        // Check that extracted contains only encryption metadata
        assert_eq!(extracted_from_mixed.len(), metadata.len());
        assert!(!extracted_from_mixed.contains_key("X-Normal-Meta"));
        
        // Remove encryption metadata
        let mut to_clean = mixed_metadata.clone();
        remove_encryption_metadata(&mut to_clean);
        
        // Check that only non-encryption metadata remains
        assert_eq!(to_clean.len(), 1);
        assert!(to_clean.contains_key("X-Normal-Meta"));
        
        // Reconstruct encryption info from metadata
        let reconstructed = EncryptionInfo::from_metadata(&metadata).unwrap().unwrap();
        
        // Verify reconstructed info
        assert_eq!(reconstructed.sse_type, SSE::SSES3);
        assert_eq!(reconstructed.algorithm, Algorithm::AES256);
        assert_eq!(reconstructed.iv, iv);
        assert_eq!(reconstructed.key.unwrap(), key);
    }
}