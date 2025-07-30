# RustFS Object Encryption Design Document

## Overview

This document describes the complete design for object-level encryption in RustFS, based on MinIO's bucket encryption architecture but optimized for Rust's type safety and memory management capabilities.

## Architecture

### 1. Three-Layer Key Architecture

RustFS implements a hierarchical key management system:

```
Master Key (KMS)     →  Data Encryption Key (DEK)  →  Object Data
     ↓                           ↓                        ↓
  Vault/Local         Random Generated 256-bit      AES-256-GCM
  Transit Engine      Encrypted by Master Key       Encrypted Data
```

#### Key Types
- **Master Key**: Managed by KMS (Vault Transit Engine or Local), used to encrypt/decrypt DEKs
- **Data Encryption Key (DEK)**: 256-bit random key, used for actual object encryption
- **Sealed Key**: Encrypted DEK stored in object metadata

### 2. Core Components

```rust
// Bucket encryption configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketEncryptionConfig {
    pub algorithm: EncryptionAlgorithm,
    pub kms_key_id: Option<String>,
    pub auto_encrypt: bool,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    #[serde(rename = "AES256")]
    Aes256,
    #[serde(rename = "aws:kms")]
    KMS,
}

// Object encryption service
pub struct ObjectEncryptionService {
    kms: Arc<KmsManager>,
    bucket_configs: Arc<RwLock<HashMap<String, BucketEncryptionConfig>>>,
    cipher: Arc<dyn ObjectCipher + Send + Sync>,
}

// Encryption metadata stored with objects
#[derive(Debug, Serialize, Deserialize)]
pub struct EncryptionMetadata {
    pub algorithm: String,
    pub kms_key_id: String,
    pub sealed_key: Vec<u8>,
    pub iv: Vec<u8>,
    pub auth_tag: Vec<u8>,
    pub context: HashMap<String, String>,
}
```

## Upload Encryption Flow

### Step 1: Configuration Check

```rust
impl ObjectEncryptionService {
    pub async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<ObjectInfo, EncryptionError> {
        // 1. Get bucket encryption configuration
        let config = self.get_bucket_encryption_config(bucket).await?;
        
        // 2. Apply auto-encryption if enabled
        let encryption_headers = self.apply_encryption_policy(config, headers)?;
        
        // 3. Determine if encryption is needed
        if self.needs_encryption(&encryption_headers) {
            self.encrypt_and_store(bucket, object, data, &encryption_headers).await
        } else {
            self.store_plaintext(bucket, object, data).await
        }
    }
    
    fn apply_encryption_policy(
        &self,
        config: Option<&BucketEncryptionConfig>,
        headers: &HeaderMap,
    ) -> Result<HeaderMap, EncryptionError> {
        let mut result_headers = headers.clone();
        
        if let Some(config) = config {
            if config.auto_encrypt && !has_encryption_headers(headers) {
                match config.algorithm {
                    EncryptionAlgorithm::KMS => {
                        result_headers.insert(
                            "x-amz-server-side-encryption", 
                            "aws:kms".parse()?
                        );
                        if let Some(key_id) = &config.kms_key_id {
                            result_headers.insert(
                                "x-amz-server-side-encryption-aws-kms-key-id",
                                key_id.parse()?
                            );
                        }
                    }
                    EncryptionAlgorithm::Aes256 => {
                        result_headers.insert(
                            "x-amz-server-side-encryption", 
                            "AES256".parse()?
                        );
                    }
                }
            }
        }
        
        Ok(result_headers)
    }
}
```

### Step 2: Key Generation and Data Encryption

```rust
impl ObjectEncryptionService {
    async fn encrypt_and_store(
        &self,
        bucket: &str,
        object: &str,
        data: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<ObjectInfo, EncryptionError> {
        // Extract KMS key ID from headers
        let kms_key_id = self.extract_kms_key_id(headers)?;
        
        // Generate encryption context
        let context = self.create_encryption_context(bucket, object);
        
        // Generate data encryption key using KMS
        let dek_request = GenerateKeyRequest {
            key_id: kms_key_id.clone(),
            context: Some(context.clone()),
            key_spec: "AES_256".to_string(),
        };
        
        let dek_response = self.kms.generate_data_key(&dek_request, None).await?;
        
        // Encrypt object data with DEK
        let encryption_result = self.cipher.encrypt(
            &data,
            &dek_response.plaintext_key,
            &context,
        ).await?;
        
        // Create encryption metadata
        let metadata = EncryptionMetadata {
            algorithm: "AES256-GCM".to_string(),
            kms_key_id,
            sealed_key: dek_response.ciphertext_key,
            iv: encryption_result.iv,
            auth_tag: encryption_result.auth_tag,
            context,
        };
        
        // Store encrypted object with metadata
        self.store_encrypted_object(
            bucket,
            object,
            encryption_result.ciphertext,
            metadata,
        ).await
    }
    
    fn create_encryption_context(
        &self,
        bucket: &str,
        object: &str,
    ) -> HashMap<String, String> {
        let mut context = HashMap::new();
        context.insert("bucket".to_string(), bucket.to_string());
        context.insert("object".to_string(), object.to_string());
        context.insert("service".to_string(), "rustfs".to_string());
        context
    }
}
```

### Step 3: Secure Storage

```rust
impl ObjectEncryptionService {
    async fn store_encrypted_object(
        &self,
        bucket: &str,
        object: &str,
        encrypted_data: Vec<u8>,
        metadata: EncryptionMetadata,
    ) -> Result<ObjectInfo, EncryptionError> {
        // Serialize encryption metadata
        let metadata_json = serde_json::to_string(&metadata)?;
        
        // Create object metadata with encryption info
        let mut object_metadata = HashMap::new();
        object_metadata.insert(
            "x-amz-server-side-encryption".to_string(),
            "aws:kms".to_string(),
        );
        object_metadata.insert(
            "x-amz-server-side-encryption-aws-kms-key-id".to_string(),
            metadata.kms_key_id.clone(),
        );
        object_metadata.insert(
            "x-rustfs-encryption-metadata".to_string(),
            base64::encode(&metadata_json),
        );
        
        // Store to underlying storage
        self.storage.put_object(
            bucket,
            object,
            encrypted_data,
            object_metadata,
        ).await
    }
}
```

## Download Decryption Flow

### Step 1: Metadata Extraction

```rust
impl ObjectEncryptionService {
    pub async fn get_object(
        &self,
        bucket: &str,
        object: &str,
    ) -> Result<Vec<u8>, EncryptionError> {
        // Get object metadata
        let object_info = self.storage.get_object_info(bucket, object).await?;
        
        // Check if object is encrypted
        if self.is_encrypted(&object_info.metadata) {
            self.decrypt_and_return(bucket, object, &object_info).await
        } else {
            self.storage.get_object_data(bucket, object).await
        }
    }
    
    fn is_encrypted(&self, metadata: &HashMap<String, String>) -> bool {
        metadata.contains_key("x-amz-server-side-encryption") ||
        metadata.contains_key("x-rustfs-encryption-metadata")
    }
}
```

### Step 2: Key Unsealing

```rust
impl ObjectEncryptionService {
    async fn decrypt_and_return(
        &self,
        bucket: &str,
        object: &str,
        object_info: &ObjectInfo,
    ) -> Result<Vec<u8>, EncryptionError> {
        // Extract encryption metadata
        let encryption_metadata = self.extract_encryption_metadata(
            &object_info.metadata
        )?;
        
        // Recreate encryption context
        let context = self.create_encryption_context(bucket, object);
        
        // Decrypt DEK using KMS
        let decrypt_request = DecryptRequest {
            ciphertext: encryption_metadata.sealed_key,
            context: Some(context.clone()),
        };
        
        let plaintext_dek = self.kms.decrypt(&decrypt_request, None).await?;
        
        // Get encrypted object data
        let encrypted_data = self.storage.get_object_data(bucket, object).await?;
        
        // Decrypt object data
        let decryption_input = DecryptionInput {
            ciphertext: encrypted_data,
            key: plaintext_dek,
            iv: encryption_metadata.iv,
            auth_tag: encryption_metadata.auth_tag,
            context,
        };
        
        let plaintext = self.cipher.decrypt(&decryption_input).await?;
        
        Ok(plaintext)
    }
    
    fn extract_encryption_metadata(
        &self,
        metadata: &HashMap<String, String>,
    ) -> Result<EncryptionMetadata, EncryptionError> {
        let encoded_metadata = metadata
            .get("x-rustfs-encryption-metadata")
            .ok_or(EncryptionError::MetadataNotFound)?;
        
        let decoded = base64::decode(encoded_metadata)?;
        let encryption_metadata: EncryptionMetadata = serde_json::from_slice(&decoded)?;
        
        Ok(encryption_metadata)
    }
}
```

## Cipher Implementation

### AES-256-GCM Cipher

```rust
use aes_gcm::{Aes256Gcm, Key, Nonce};
use aes_gcm::aead::{Aead, NewAead};
use rand::RngCore;

#[derive(Debug)]
pub struct EncryptionResult {
    pub ciphertext: Vec<u8>,
    pub iv: Vec<u8>,
    pub auth_tag: Vec<u8>,
}

#[derive(Debug)]
pub struct DecryptionInput {
    pub ciphertext: Vec<u8>,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
    pub auth_tag: Vec<u8>,
    pub context: HashMap<String, String>,
}

#[async_trait]
pub trait ObjectCipher {
    async fn encrypt(
        &self,
        plaintext: &[u8],
        key: &[u8],
        context: &HashMap<String, String>,
    ) -> Result<EncryptionResult, EncryptionError>;
    
    async fn decrypt(
        &self,
        input: &DecryptionInput,
    ) -> Result<Vec<u8>, EncryptionError>;
}

pub struct AesGcmCipher;

#[async_trait]
impl ObjectCipher for AesGcmCipher {
    async fn encrypt(
        &self,
        plaintext: &[u8],
        key: &[u8],
        _context: &HashMap<String, String>,
    ) -> Result<EncryptionResult, EncryptionError> {
        if key.len() != 32 {
            return Err(EncryptionError::InvalidKeySize);
        }
        
        // Generate random IV
        let mut iv = vec![0u8; 12]; // 96-bit IV for GCM
        rand::thread_rng().fill_bytes(&mut iv);
        
        // Create cipher
        let key = Key::from_slice(key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&iv);
        
        // Encrypt
        let ciphertext = cipher.encrypt(nonce, plaintext)
            .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;
        
        // Extract auth tag (last 16 bytes)
        let auth_tag = ciphertext[ciphertext.len() - 16..].to_vec();
        let ciphertext = ciphertext[..ciphertext.len() - 16].to_vec();
        
        Ok(EncryptionResult {
            ciphertext,
            iv,
            auth_tag,
        })
    }
    
    async fn decrypt(
        &self,
        input: &DecryptionInput,
    ) -> Result<Vec<u8>, EncryptionError> {
        if input.key.len() != 32 {
            return Err(EncryptionError::InvalidKeySize);
        }
        
        // Reconstruct ciphertext with auth tag
        let mut full_ciphertext = input.ciphertext.clone();
        full_ciphertext.extend_from_slice(&input.auth_tag);
        
        // Create cipher
        let key = Key::from_slice(&input.key);
        let cipher = Aes256Gcm::new(key);
        let nonce = Nonce::from_slice(&input.iv);
        
        // Decrypt
        let plaintext = cipher.decrypt(nonce, full_ciphertext.as_ref())
            .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?;
        
        Ok(plaintext)
    }
}
```

## API Endpoints

### Bucket Encryption Configuration

```rust
// PUT /bucket/{bucket}/encryption
#[derive(Debug, Serialize, Deserialize)]
pub struct PutBucketEncryptionRequest {
    pub algorithm: EncryptionAlgorithm,
    pub kms_key_id: Option<String>,
    pub auto_encrypt: bool,
}

// GET /bucket/{bucket}/encryption
#[derive(Debug, Serialize, Deserialize)]
pub struct GetBucketEncryptionResponse {
    pub algorithm: EncryptionAlgorithm,
    pub kms_key_id: Option<String>,
    pub auto_encrypt: bool,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

// DELETE /bucket/{bucket}/encryption
pub async fn delete_bucket_encryption(
    bucket: &str,
) -> Result<(), EncryptionError> {
    // Remove bucket encryption configuration
}
```

### Object Operations with Encryption

```rust
// PUT /bucket/{bucket}/object/{object}
// Headers:
// - x-amz-server-side-encryption: aws:kms | AES256
// - x-amz-server-side-encryption-aws-kms-key-id: <key-id>

// GET /bucket/{bucket}/object/{object}
// Response Headers:
// - x-amz-server-side-encryption: aws:kms | AES256
// - x-amz-server-side-encryption-aws-kms-key-id: <key-id>
```

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("KMS error: {0}")]
    KmsError(#[from] KmsError),
    
    #[error("Invalid key size")]
    InvalidKeySize,
    
    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),
    
    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),
    
    #[error("Encryption metadata not found")]
    MetadataNotFound,
    
    #[error("Invalid encryption configuration: {0}")]
    InvalidConfiguration(String),
    
    #[error("Storage error: {0}")]
    StorageError(String),
}
```

## Security Considerations

### 1. Key Management
- DEKs are generated using cryptographically secure random number generators
- Master keys are managed by KMS and never exposed in plaintext
- Keys are automatically zeroized when dropped from memory
- Encryption context prevents key reuse across different objects

### 2. Memory Safety
- Use `zeroize` crate for secure memory cleanup
- Implement `Drop` trait for sensitive data structures
- Use `SecretVec` for storing keys in memory

```rust
use zeroize::{Zeroize, ZeroizeOnDrop};

#[derive(ZeroizeOnDrop)]
struct SecretKey {
    #[zeroize(skip)]
    key_id: String,
    key_material: Vec<u8>,
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        self.key_material.zeroize();
    }
}
```

### 3. Audit and Logging
- Log all encryption/decryption operations
- Include encryption context in audit logs
- Monitor KMS key usage patterns
- Implement rate limiting for KMS operations

## Performance Optimizations

### 1. Streaming Encryption
```rust
pub struct StreamingCipher {
    cipher: Aes256Gcm,
    buffer_size: usize,
}

impl StreamingCipher {
    pub async fn encrypt_stream<R, W>(
        &self,
        reader: R,
        writer: W,
        key: &[u8],
    ) -> Result<EncryptionResult, EncryptionError>
    where
        R: AsyncRead + Unpin,
        W: AsyncWrite + Unpin,
    {
        // Implement streaming encryption for large objects
    }
}
```

### 2. Configuration Caching
```rust
pub struct ConfigCache {
    cache: Arc<RwLock<LruCache<String, BucketEncryptionConfig>>>,
    ttl: Duration,
}

impl ConfigCache {
    pub async fn get_config(
        &self,
        bucket: &str,
    ) -> Option<BucketEncryptionConfig> {
        // Implement LRU cache with TTL
    }
}
```

### 3. Parallel Processing
- Use `rayon` for CPU-intensive encryption operations
- Implement async encryption for I/O bound operations
- Use SIMD instructions when available

## Testing Strategy

### 1. Unit Tests
```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_encrypt_decrypt_roundtrip() {
        let cipher = AesGcmCipher;
        let key = generate_random_key();
        let plaintext = b"test data";
        
        let encrypted = cipher.encrypt(plaintext, &key, &HashMap::new()).await.unwrap();
        let decrypted = cipher.decrypt(&DecryptionInput {
            ciphertext: encrypted.ciphertext,
            key,
            iv: encrypted.iv,
            auth_tag: encrypted.auth_tag,
            context: HashMap::new(),
        }).await.unwrap();
        
        assert_eq!(plaintext, decrypted.as_slice());
    }
    
    #[tokio::test]
    async fn test_bucket_encryption_config() {
        // Test bucket configuration management
    }
    
    #[tokio::test]
    async fn test_kms_integration() {
        // Test KMS integration
    }
}
```

### 2. Integration Tests
- End-to-end encryption flow testing
- S3 API compatibility testing
- KMS backend integration testing
- Performance benchmarking

### 3. Security Tests
- Key isolation verification
- Metadata encryption validation
- Error handling security
- Side-channel attack resistance

## Implementation Roadmap

### Phase 1: Core Infrastructure
1. Implement basic encryption/decryption interfaces
2. Create KMS integration layer
3. Implement AES-GCM cipher
4. Basic error handling

### Phase 2: Bucket Configuration
1. Bucket encryption configuration API
2. Auto-encryption policies
3. Configuration persistence
4. Cache implementation

### Phase 3: Object Operations
1. Encrypted object upload
2. Encrypted object download
3. Metadata management
4. S3 API compatibility

### Phase 4: Advanced Features
1. Streaming encryption
2. Performance optimizations
3. Advanced security features
4. Monitoring and audit

### Phase 5: Production Readiness
1. Comprehensive testing
2. Documentation
3. Performance tuning
4. Security audit

## Conclusion

This design provides a comprehensive, secure, and efficient object encryption system for RustFS. It leverages Rust's type safety and memory management capabilities while maintaining compatibility with S3 encryption standards. The modular design allows for easy testing, maintenance, and future enhancements.