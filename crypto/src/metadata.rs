// metadata.rs - Implementation of encryption metadata for RustFS
// This file handles the encryption metadata format used in the S3 object storage

use crate::{Error, sse::{SSE, Algorithm}};
use serde::{Serialize, Deserialize};
use std::collections::HashMap;
use uuid::Uuid;

// Metadata header constants
pub const CRYPTO_IV: &str = "X-Rustfs-Crypto-Iv";
pub const CRYPTO_KEY: &str = "X-Rustfs-Crypto-Key";
pub const CRYPTO_KEY_ID: &str = "X-Rustfs-Crypto-Key-Id";
pub const CRYPTO_ALGORITHM: &str = "X-Rustfs-Crypto-Algorithm";
pub const CRYPTO_SEAL_ALGORITHM: &str = "X-Rustfs-Crypto-Seal-Algorithm";
pub const CRYPTO_KMS_KEY_NAME: &str = "X-Rustfs-Crypto-Kms-Key-Name";
pub const CRYPTO_KMS_CONTEXT: &str = "X-Rustfs-Crypto-Kms-Context";
pub const CRYPTO_META_PREFIX: &str = "X-Rustfs-Crypto-";

/// EncryptionInfo - contains information about encryption
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EncryptionInfo {
    /// Type of SSE encryption
    pub sse_type: SSE,
    
    /// Algorithm used for encryption
    pub algorithm: Algorithm,
    
    /// Initialization vector used for encryption
    pub iv: Vec<u8>,
    
    /// Encrypted data key (encrypted with master key)
    pub key: Option<Vec<u8>>,
    
    /// Key ID for KMS
    pub key_id: Option<String>,
    
    /// KMS context
    pub context: Option<String>,
    
    /// Storage class
    pub storage_class: Option<String>,
    
    /// Unique ID for this encryption operation
    pub matdesc: String,
}

impl EncryptionInfo {
    /// Create a new EncryptionInfo for SSE-C
    pub fn new_ssec(iv: Vec<u8>) -> Self {
        Self {
            sse_type: SSE::SSEC,
            algorithm: Algorithm::AES256,
            iv,
            key: None,
            key_id: None,
            context: None,
            storage_class: None,
            matdesc: Uuid::new_v4().to_string(),
        }
    }
    
    /// Create a new EncryptionInfo for SSE-S3
    pub fn new_sse_s3(iv: Vec<u8>, encrypted_key: Vec<u8>) -> Self {
        Self {
            sse_type: SSE::SSES3,
            algorithm: Algorithm::AES256,
            iv,
            key: Some(encrypted_key),
            key_id: None,
            context: None,
            storage_class: None,
            matdesc: Uuid::new_v4().to_string(),
        }
    }
    
    /// Create a new EncryptionInfo for SSE-KMS
    pub fn new_sse_kms(iv: Vec<u8>, encrypted_key: Vec<u8>, key_id: String, context: Option<String>) -> Self {
        Self {
            sse_type: SSE::SSEKMS,
            algorithm: Algorithm::AWSKMS, 
            iv,
            key: Some(encrypted_key),
            key_id: Some(key_id),
            context,
            storage_class: None,
            matdesc: Uuid::new_v4().to_string(),
        }
    }
    
    /// Convert encryption info to user defined metadata map
    pub fn to_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        
        // Common fields
        metadata.insert(CRYPTO_ALGORITHM.to_string(), self.algorithm.to_string());
        metadata.insert(CRYPTO_IV.to_string(), base64::encode(&self.iv));
        
        // Type-specific fields
        match self.sse_type {
            SSE::SSEC => {
                // For SSE-C, we don't store any key material
            }
            SSE::SSES3 => {
                if let Some(key) = &self.key {
                    metadata.insert(CRYPTO_KEY.to_string(), base64::encode(key));
                }
            }
            SSE::SSEKMS => {
                if let Some(key) = &self.key {
                    metadata.insert(CRYPTO_KEY.to_string(), base64::encode(key));
                }
                if let Some(key_id) = &self.key_id {
                    metadata.insert(CRYPTO_KEY_ID.to_string(), key_id.clone());
                }
                if let Some(context) = &self.context {
                    metadata.insert(CRYPTO_KMS_CONTEXT.to_string(), context.clone());
                }
            }
        }
        
        metadata
    }
    
    /// Parse encryption info from user defined metadata
    pub fn from_metadata(metadata: &HashMap<String, String>) -> Result<Option<Self>, Error> {
        // Check if encryption metadata exists
        if !metadata.contains_key(CRYPTO_ALGORITHM) || !metadata.contains_key(CRYPTO_IV) {
            return Ok(None);
        }
        
        let algorithm_str = metadata.get(CRYPTO_ALGORITHM).unwrap();
        let iv_base64 = metadata.get(CRYPTO_IV).unwrap();
        
        let algorithm = match algorithm_str.as_str() {
            "AES256" => Algorithm::AES256,
            "aws:kms" => Algorithm::AWSKMS,
            _ => return Err(Error::ErrInvalidSSEAlgorithm),
        };
        
        let iv = base64::decode(iv_base64).map_err(|_| Error::ErrInvalidEncryptionMetadata)?;
        
        // Determine SSE type and parse additional fields
        let mut sse_type = SSE::SSES3; // Default assuming SSE-S3
        let mut key = None;
        let mut key_id = None;
        let mut context = None;
        
        if metadata.contains_key(CRYPTO_KEY) {
            let key_base64 = metadata.get(CRYPTO_KEY).unwrap();
            key = Some(base64::decode(key_base64).map_err(|_| Error::ErrInvalidEncryptionMetadata)?);
        }
        
        if metadata.contains_key(CRYPTO_KEY_ID) {
            sse_type = SSE::SSEKMS;
            key_id = metadata.get(CRYPTO_KEY_ID).cloned();
        }
        
        if metadata.contains_key(CRYPTO_KMS_CONTEXT) {
            context = metadata.get(CRYPTO_KMS_CONTEXT).cloned();
        }
        
        // If no key is present but we have algorithm and IV, it's SSE-C
        if key.is_none() && key_id.is_none() {
            sse_type = SSE::SSEC;
        }
        
        Ok(Some(Self {
            sse_type,
            algorithm,
            iv,
            key,
            key_id,
            context,
            storage_class: None,
            matdesc: Uuid::new_v4().to_string(), // Generate a new ID for restored metadata
        }))
    }
    
    /// Check if this encryption info is for SSE-C
    pub fn is_ssec(&self) -> bool {
        self.sse_type == SSE::SSEC
    }
    
    /// Check if this encryption info is for SSE-S3
    pub fn is_sse_s3(&self) -> bool {
        self.sse_type == SSE::SSES3
    }
    
    /// Check if this encryption info is for SSE-KMS
    pub fn is_sse_kms(&self) -> bool {
        self.sse_type == SSE::SSEKMS
    }
}

/// Extract encryption related metadata from a general metadata map
pub fn extract_encryption_metadata(metadata: &HashMap<String, String>) -> HashMap<String, String> {
    metadata
        .iter()
        .filter(|(k, _)| k.starts_with(CRYPTO_META_PREFIX))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Remove encryption metadata from general metadata
pub fn remove_encryption_metadata(metadata: &mut HashMap<String, String>) {
    metadata.retain(|k, _| !k.starts_with(CRYPTO_META_PREFIX));
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_encryption_info_to_metadata_ssec() {
        let iv = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let info = EncryptionInfo::new_ssec(iv.clone());
        
        let metadata = info.to_metadata();
        
        assert_eq!(metadata.get(CRYPTO_ALGORITHM).unwrap(), "AES256");
        assert_eq!(metadata.get(CRYPTO_IV).unwrap(), &base64::encode(&iv));
        assert!(!metadata.contains_key(CRYPTO_KEY));
        assert!(!metadata.contains_key(CRYPTO_KEY_ID));
    }
    
    #[test]
    fn test_encryption_info_to_metadata_sse_s3() {
        let iv = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let key = vec![11, 22, 33, 44, 55, 66, 77, 88];
        let info = EncryptionInfo::new_sse_s3(iv.clone(), key.clone());
        
        let metadata = info.to_metadata();
        
        assert_eq!(metadata.get(CRYPTO_ALGORITHM).unwrap(), "AES256");
        assert_eq!(metadata.get(CRYPTO_IV).unwrap(), &base64::encode(&iv));
        assert_eq!(metadata.get(CRYPTO_KEY).unwrap(), &base64::encode(&key));
        assert!(!metadata.contains_key(CRYPTO_KEY_ID));
    }
    
    #[test]
    fn test_encryption_info_to_metadata_sse_kms() {
        let iv = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let key = vec![11, 22, 33, 44, 55, 66, 77, 88];
        let key_id = "my-kms-key-id";
        let context = "my-context";
        
        let info = EncryptionInfo::new_sse_kms(
            iv.clone(),
            key.clone(),
            key_id.to_string(),
            Some(context.to_string())
        );
        
        let metadata = info.to_metadata();
        
        assert_eq!(metadata.get(CRYPTO_ALGORITHM).unwrap(), "aws:kms");
        assert_eq!(metadata.get(CRYPTO_IV).unwrap(), &base64::encode(&iv));
        assert_eq!(metadata.get(CRYPTO_KEY).unwrap(), &base64::encode(&key));
        assert_eq!(metadata.get(CRYPTO_KEY_ID).unwrap(), key_id);
        assert_eq!(metadata.get(CRYPTO_KMS_CONTEXT).unwrap(), context);
    }
    
    #[test]
    fn test_encryption_info_from_metadata() {
        let iv = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let key = vec![11, 22, 33, 44, 55, 66, 77, 88];
        let key_id = "my-kms-key-id";
        let context = "my-context";
        
        let mut metadata = HashMap::new();
        metadata.insert(CRYPTO_ALGORITHM.to_string(), "aws:kms".to_string());
        metadata.insert(CRYPTO_IV.to_string(), base64::encode(&iv));
        metadata.insert(CRYPTO_KEY.to_string(), base64::encode(&key));
        metadata.insert(CRYPTO_KEY_ID.to_string(), key_id.to_string());
        metadata.insert(CRYPTO_KMS_CONTEXT.to_string(), context.to_string());
        
        let info = EncryptionInfo::from_metadata(&metadata).unwrap().unwrap();
        
        assert_eq!(info.sse_type, SSE::SSEKMS);
        assert_eq!(info.algorithm, Algorithm::AWSKMS);
        assert_eq!(info.iv, iv);
        assert_eq!(info.key.unwrap(), key);
        assert_eq!(info.key_id.unwrap(), key_id);
        assert_eq!(info.context.unwrap(), context);
    }
    
    #[test]
    fn test_extract_encryption_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("X-Rustfs-Normal-Meta".to_string(), "value1".to_string());
        metadata.insert(CRYPTO_ALGORITHM.to_string(), "AES256".to_string());
        metadata.insert(CRYPTO_IV.to_string(), "iv_base64".to_string());
        
        let extracted = extract_encryption_metadata(&metadata);
        
        assert_eq!(extracted.len(), 2);
        assert!(!extracted.contains_key("X-Rustfs-Normal-Meta"));
        assert!(extracted.contains_key(CRYPTO_ALGORITHM));
        assert!(extracted.contains_key(CRYPTO_IV));
    }
    
    #[test]
    fn test_remove_encryption_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("X-Rustfs-Normal-Meta".to_string(), "value1".to_string());
        metadata.insert(CRYPTO_ALGORITHM.to_string(), "AES256".to_string());
        metadata.insert(CRYPTO_IV.to_string(), "iv_base64".to_string());
        
        remove_encryption_metadata(&mut metadata);
        
        assert_eq!(metadata.len(), 1);
        assert!(metadata.contains_key("X-Rustfs-Normal-Meta"));
        assert!(!metadata.contains_key(CRYPTO_ALGORITHM));
        assert!(!metadata.contains_key(CRYPTO_IV));
    }
}