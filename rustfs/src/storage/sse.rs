// Copyright 2024 RustFS Team
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

//! Server-Side Encryption (SSE) utilities
//!
//! This module provides reusable components for handling S3 Server-Side Encryption:
//! - SSE-S3 (AES256): Server-managed encryption with S3-managed keys
//! - SSE-KMS (aws:kms): Server-managed encryption with KMS-managed keys
//! - SSE-C (AES256): Customer-provided encryption keys
//!
//! ## Architecture
//!
//! ### Managed SSE (SSE-S3 / SSE-KMS)
//! - Keys are managed by the server-side KMS service
//! - Data keys are generated and encrypted by KMS
//! - Encryption metadata is stored in object metadata
//!
//! ### Customer-Provided Keys (SSE-C)
//! - Keys are provided by the client on every request
//! - Server validates key using MD5 hash
//! - Keys are NEVER stored on the server
//!
//! ## Usage Example
//!
//! ```rust,ignore
//! // Apply managed SSE encryption
//! if let Some(material) = apply_managed_sse(
//!     &bucket, &key, &sse_algorithm, kms_key_id, actual_size
//! ).await? {
//!     reader = material.wrap_encrypt_reader(reader)?;
//!     metadata.extend(material.headers);
//! }
//!
//! // Apply SSE-C encryption
//! if let Some(params) = sse_customer_params {
//!     let validated = validate_ssec_params(&params)?;
//!     reader = apply_ssec_encryption(reader, &validated, &bucket, &key)?;
//! }
//! ```

use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use chrono::Utc;
use rustfs_ecstore::error::StorageError;
use rustfs_filemeta::ObjectPartInfo;
use rustfs_kms::{
    DataKey,
    service_manager::get_global_encryption_service,
    types::{EncryptionMetadata, ObjectEncryptionContext},
};
use rustfs_rio::{DecryptReader, EncryptReader, Reader, WarpReader};
use s3s::dto::ServerSideEncryption;
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncSeek};
use tracing::error;

use crate::error::ApiError;

// ============================================================================
// Public Types
// ============================================================================

/// Material for managed server-side encryption (SSE-S3/SSE-KMS)
#[derive(Debug, Clone)]
pub struct ManagedEncryptionMaterial {
    /// Data encryption key
    pub data_key: DataKey,
    /// Metadata headers to store with the object
    pub headers: HashMap<String, String>,
    /// KMS key ID used for encryption
    pub kms_key_id: String,
}

/// Validated SSE-C parameters
#[derive(Debug, Clone)]
pub struct ValidatedSsecParams {
    /// Encryption algorithm (always "AES256" for SSE-C)
    pub algorithm: String,
    /// Decoded encryption key bytes (32 bytes for AES-256)
    pub key_bytes: [u8; 32],
    /// Base64-encoded MD5 of the key
    pub key_md5: String,
}

/// SSE-C parameters from client request
#[derive(Debug, Clone)]
pub struct SsecParams {
    /// Encryption algorithm
    pub algorithm: String,
    /// Base64-encoded encryption key
    pub key: String,
    /// Base64-encoded MD5 of the key
    pub key_md5: String,
}

// ============================================================================
// Managed SSE Functions (SSE-S3 / SSE-KMS)
// ============================================================================

/// Check if the algorithm is a managed SSE type (SSE-S3 or SSE-KMS)
#[inline]
pub fn is_managed_sse(algorithm: &ServerSideEncryption) -> bool {
    matches!(algorithm.as_str(), "AES256" | "aws:kms")
}

/// Create managed encryption material for SSE-S3 or SSE-KMS
///
/// This function:
/// 1. Validates the encryption algorithm
/// 2. Creates an encryption context
/// 3. Generates a data key via KMS
/// 4. Prepares metadata headers for storage
///
/// # Arguments
/// * `bucket` - Bucket name
/// * `key` - Object key
/// * `algorithm` - Encryption algorithm (AES256 or aws:kms)
/// * `kms_key_id` - Optional KMS key ID (uses default if None)
/// * `original_size` - Original object size before encryption
///
/// # Returns
/// `ManagedEncryptionMaterial` containing data key, headers, and key ID
pub async fn create_managed_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: &ServerSideEncryption,
    kms_key_id: Option<String>,
    original_size: i64,
) -> Result<ManagedEncryptionMaterial, ApiError> {
    let Some(service) = get_global_encryption_service().await else {
        return Err(ApiError::from(StorageError::other("KMS encryption service is not initialized")));
    };

    if !is_managed_sse(algorithm) {
        return Err(ApiError::from(StorageError::other(format!(
            "Unsupported server-side encryption algorithm: {}",
            algorithm.as_str()
        ))));
    }

    let algorithm_str = algorithm.as_str();

    let mut context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());
    if original_size >= 0 {
        context = context.with_size(original_size as u64);
    }

    let mut kms_key_candidate = kms_key_id;
    if kms_key_candidate.is_none() {
        kms_key_candidate = service.get_default_key_id().cloned();
    }

    let kms_key_to_use = kms_key_candidate
        .clone()
        .ok_or_else(|| ApiError::from(StorageError::other("No KMS key available for managed server-side encryption")))?;

    let (data_key, encrypted_data_key) = service
        .create_data_key(&kms_key_candidate, &context)
        .await
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to create data key: {e}"))))?;

    let metadata = EncryptionMetadata {
        algorithm: algorithm_str.to_string(),
        key_id: kms_key_to_use.clone(),
        key_version: 1,
        iv: data_key.nonce.to_vec(),
        tag: None,
        encryption_context: context.encryption_context.clone(),
        encrypted_at: Utc::now(),
        original_size: if original_size >= 0 { original_size as u64 } else { 0 },
        encrypted_data_key,
    };

    let mut headers = service.metadata_to_headers(&metadata);
    headers.insert("x-rustfs-encryption-original-size".to_string(), metadata.original_size.to_string());

    Ok(ManagedEncryptionMaterial {
        data_key,
        headers,
        kms_key_id: kms_key_to_use,
    })
}

/// Decrypt managed encryption key from object metadata
///
/// This function:
/// 1. Checks if object has managed encryption metadata
/// 2. Parses encryption metadata from headers
/// 3. Decrypts the data key using KMS
///
/// # Arguments
/// * `bucket` - Bucket name
/// * `key` - Object key
/// * `metadata` - Object metadata containing encryption headers
///
/// # Returns
/// `Some((key_bytes, nonce, original_size))` if object is encrypted, `None` otherwise
pub async fn decrypt_managed_encryption_key(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
) -> Result<Option<([u8; 32], [u8; 12], Option<i64>)>, ApiError> {
    if !metadata.contains_key("x-rustfs-encryption-key") {
        return Ok(None);
    }

    let Some(service) = get_global_encryption_service().await else {
        return Err(ApiError::from(StorageError::other("KMS encryption service is not initialized")));
    };

    let parsed = service
        .headers_to_metadata(metadata)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to parse encryption metadata: {e}"))))?;

    if parsed.iv.len() != 12 {
        return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
    }

    let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());
    let data_key = service
        .decrypt_data_key(&parsed.encrypted_data_key, &context)
        .await
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decrypt data key: {e}"))))?;

    let key_bytes = data_key.plaintext_key;
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&parsed.iv[..12]);

    let original_size = metadata
        .get("x-rustfs-encryption-original-size")
        .and_then(|s| s.parse::<i64>().ok());

    Ok(Some((key_bytes, nonce, original_size)))
}

/// Strip managed encryption metadata from object metadata
///
/// Removes all managed SSE-related headers before returning object metadata to client.
/// This is necessary because encryption is transparent to S3 clients.
pub fn strip_managed_encryption_metadata(metadata: &mut HashMap<String, String>) {
    const KEYS: [&str; 7] = [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-rustfs-encryption-iv",
        "x-rustfs-encryption-tag",
        "x-rustfs-encryption-key",
        "x-rustfs-encryption-context",
        "x-rustfs-encryption-original-size",
    ];

    for key in KEYS.iter() {
        metadata.remove(*key);
    }
}

// ============================================================================
// Multipart Encryption Support
// ============================================================================

/// Derive a unique nonce for each part in a multipart upload
///
/// Uses the base nonce and increments the counter portion by part number.
/// This ensures each part has a unique nonce while maintaining determinism.
pub fn derive_part_nonce(base: [u8; 12], part_number: usize) -> [u8; 12] {
    let mut nonce = base;
    let current = u32::from_be_bytes([nonce[8], nonce[9], nonce[10], nonce[11]]);
    let incremented = current.wrapping_add(part_number as u32);
    nonce[8..12].copy_from_slice(&incremented.to_be_bytes());
    nonce
}

/// In-memory async reader for decrypted multipart data
pub(crate) struct InMemoryAsyncReader {
    cursor: std::io::Cursor<Vec<u8>>,
}

impl InMemoryAsyncReader {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            cursor: std::io::Cursor::new(data),
        }
    }
}

impl AsyncRead for InMemoryAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let bytes_read = std::io::Read::read(&mut self.cursor, unfilled)?;
        buf.advance(bytes_read);
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for InMemoryAsyncReader {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        std::io::Seek::seek(&mut self.cursor, position)?;
        Ok(())
    }

    fn poll_complete(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.cursor.position()))
    }
}

/// Decrypt a multipart upload stream with managed SSE encryption
///
/// This function:
/// 1. Reads each encrypted part from the stream
/// 2. Derives a unique nonce for each part
/// 3. Decrypts each part individually
/// 4. Concatenates all plaintext parts into a single buffer
///
/// # Arguments
/// * `encrypted_stream` - Stream containing encrypted multipart data
/// * `parts` - Part info containing sizes and part numbers
/// * `key_bytes` - Decryption key
/// * `base_nonce` - Base nonce (unique nonce derived per part)
///
/// # Returns
/// Tuple of (decrypted_reader, total_plaintext_size)
pub async fn decrypt_multipart_managed_stream(
    mut encrypted_stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    parts: &[ObjectPartInfo],
    key_bytes: [u8; 32],
    base_nonce: [u8; 12],
) -> Result<(Box<dyn Reader>, i64), StorageError> {
    let total_plain_capacity: usize = parts.iter().map(|part| part.actual_size.max(0) as usize).sum();

    let mut plaintext = Vec::with_capacity(total_plain_capacity);

    for part in parts {
        if part.size == 0 {
            continue;
        }

        let mut encrypted_part = vec![0u8; part.size];
        tokio::io::AsyncReadExt::read_exact(&mut encrypted_stream, &mut encrypted_part)
            .await
            .map_err(|e| StorageError::other(format!("failed to read encrypted multipart segment {}: {}", part.number, e)))?;

        let part_nonce = derive_part_nonce(base_nonce, part.number);
        let cursor = std::io::Cursor::new(encrypted_part);
        let mut decrypt_reader = DecryptReader::new(WarpReader::new(cursor), key_bytes, part_nonce);

        tokio::io::AsyncReadExt::read_to_end(&mut decrypt_reader, &mut plaintext)
            .await
            .map_err(|e| StorageError::other(format!("failed to decrypt multipart segment {}: {}", part.number, e)))?;
    }

    let total_plain_size = plaintext.len() as i64;
    let reader = Box::new(WarpReader::new(InMemoryAsyncReader::new(plaintext))) as Box<dyn Reader>;

    Ok((reader, total_plain_size))
}

// ============================================================================
// Customer-Provided Key (SSE-C) Functions
// ============================================================================

/// Validate SSE-C parameters from client request
///
/// This function:
/// 1. Validates the algorithm is AES256
/// 2. Decodes the Base64-encoded key
/// 3. Validates key length is 32 bytes
/// 4. Verifies MD5 hash matches
///
/// # Arguments
/// * `params` - SSE-C parameters from client
///
/// # Returns
/// `ValidatedSsecParams` with decoded key bytes
pub fn validate_ssec_params(params: &SsecParams) -> Result<ValidatedSsecParams, ApiError> {
    // Validate algorithm
    if params.algorithm != "AES256" {
        return Err(ApiError::from(StorageError::other(format!(
            "Unsupported SSE-C algorithm: {}. Only AES256 is supported",
            params.algorithm
        ))));
    }

    // Decode Base64 key
    let key_bytes = BASE64_STANDARD.decode(&params.key).map_err(|e| {
        error!("Failed to decode SSE-C key: {}", e);
        ApiError::from(StorageError::other("Invalid SSE-C key: not valid Base64"))
    })?;

    // Validate key length (must be 32 bytes for AES-256)
    if key_bytes.len() != 32 {
        return Err(ApiError::from(StorageError::other(format!(
            "SSE-C key must be 32 bytes (256 bits), got {} bytes",
            key_bytes.len()
        ))));
    }

    // Verify MD5 hash
    let computed_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);
    if computed_md5 != params.key_md5 {
        error!("SSE-C key MD5 mismatch: expected '{}', got '{}'", params.key_md5, computed_md5);
        return Err(ApiError::from(StorageError::other("SSE-C key MD5 mismatch")));
    }

    // SAFETY: We validated the length is exactly 32 bytes above
    let key_array: [u8; 32] = key_bytes.try_into().expect("key length already validated to be 32 bytes");

    Ok(ValidatedSsecParams {
        algorithm: params.algorithm.clone(),
        key_bytes: key_array,
        key_md5: params.key_md5.clone(),
    })
}

/// Generate deterministic nonce for SSE-C encryption
///
/// The nonce is derived from the bucket and key to ensure:
/// 1. Same object always gets the same nonce (required for SSE-C)
/// 2. Different objects get different nonces
pub fn generate_ssec_nonce(bucket: &str, key: &str) -> [u8; 12] {
    let nonce_source = format!("{bucket}-{key}");
    let nonce_hash = md5::compute(nonce_source.as_bytes());
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_hash.0[..12]);
    nonce
}

/// Apply SSE-C encryption to a reader
///
/// # Arguments
/// * `reader` - Input reader to encrypt
/// * `validated` - Validated SSE-C parameters
/// * `bucket` - Bucket name (for nonce generation)
/// * `key` - Object key (for nonce generation)
///
/// # Returns
/// Encrypted reader wrapped in Box
pub fn apply_ssec_encryption<R>(reader: R, validated: &ValidatedSsecParams, bucket: &str, key: &str) -> Box<EncryptReader<R>>
where
    R: Reader + 'static,
{
    let nonce = generate_ssec_nonce(bucket, key);
    Box::new(EncryptReader::new(reader, validated.key_bytes, nonce))
}

/// Apply SSE-C decryption to a reader
///
/// # Arguments
/// * `reader` - Encrypted reader to decrypt
/// * `validated` - Validated SSE-C parameters
/// * `bucket` - Bucket name (for nonce generation)
/// * `key` - Object key (for nonce generation)
///
/// # Returns
/// Decrypted reader wrapped in Box
pub fn apply_ssec_decryption<R>(reader: R, validated: &ValidatedSsecParams, bucket: &str, key: &str) -> Box<DecryptReader<R>>
where
    R: Reader + 'static,
{
    let nonce = generate_ssec_nonce(bucket, key);
    Box::new(DecryptReader::new(reader, validated.key_bytes, nonce))
}

/// Store SSE-C metadata in object metadata
///
/// Stores the algorithm and key MD5 for later validation during GetObject.
/// Note: The encryption key itself is NEVER stored.
pub fn store_ssec_metadata(metadata: &mut HashMap<String, String>, validated: &ValidatedSsecParams, original_size: i64) {
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), validated.algorithm.clone());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), validated.key_md5.clone());
    metadata.insert(
        "x-amz-server-side-encryption-customer-original-size".to_string(),
        original_size.to_string(),
    );
}

/// Verify SSE-C key matches the stored metadata
///
/// Used during GetObject to ensure the client provided the correct key.
pub fn verify_ssec_key_match(provided_md5: &str, stored_md5: Option<&String>) -> Result<(), ApiError> {
    let Some(stored) = stored_md5 else {
        return Err(ApiError::from(StorageError::other(
            "Object encrypted with SSE-C but stored key MD5 not found",
        )));
    };

    if provided_md5 != stored {
        error!("SSE-C key MD5 mismatch: provided='{}', stored='{}'", provided_md5, stored);
        return Err(ApiError::from(StorageError::other("SSE-C key does not match object encryption key")));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_managed_sse() {
        assert!(is_managed_sse(&ServerSideEncryption::from_static("AES256")));
        assert!(is_managed_sse(&ServerSideEncryption::from_static("aws:kms")));
        assert!(!is_managed_sse(&ServerSideEncryption::from_static("invalid")));
    }

    #[test]
    fn test_derive_part_nonce() {
        let base_nonce: [u8; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 1];
        let part1_nonce = derive_part_nonce(base_nonce, 1);
        let part2_nonce = derive_part_nonce(base_nonce, 2);

        // First 8 bytes should be the same
        assert_eq!(&part1_nonce[..8], &base_nonce[..8]);
        assert_eq!(&part2_nonce[..8], &base_nonce[..8]);

        // Last 4 bytes should be different (counter)
        assert_ne!(&part1_nonce[8..], &part2_nonce[8..]);
    }

    #[test]
    fn test_generate_ssec_nonce() {
        let nonce1 = generate_ssec_nonce("bucket1", "key1");
        let nonce2 = generate_ssec_nonce("bucket1", "key1");
        let nonce3 = generate_ssec_nonce("bucket1", "key2");

        // Same bucket/key should generate same nonce
        assert_eq!(nonce1, nonce2);

        // Different key should generate different nonce
        assert_ne!(nonce1, nonce3);

        // Nonce should be 12 bytes
        assert_eq!(nonce1.len(), 12);
    }

    #[test]
    fn test_validate_ssec_params_success() {
        // Generate a valid 32-byte key
        let key_bytes = [42u8; 32];
        let key_b64 = BASE64_STANDARD.encode(key_bytes);
        let key_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key: key_b64,
            key_md5,
        };

        let result = validate_ssec_params(&params);
        assert!(result.is_ok());

        let validated = result.unwrap();
        assert_eq!(validated.algorithm, "AES256");
        assert_eq!(validated.key_bytes, key_bytes);
    }

    #[test]
    fn test_validate_ssec_params_wrong_algorithm() {
        let key_bytes = [42u8; 32];
        let key_b64 = BASE64_STANDARD.encode(key_bytes);
        let key_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);

        let params = SsecParams {
            algorithm: "AES128".to_string(),
            key: key_b64,
            key_md5,
        };

        let result = validate_ssec_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ssec_params_wrong_key_length() {
        let key_bytes = [42u8; 16]; // Wrong length
        let key_b64 = BASE64_STANDARD.encode(key_bytes);
        let key_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key: key_b64,
            key_md5,
        };

        let result = validate_ssec_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ssec_params_wrong_md5() {
        let key_bytes = [42u8; 32];
        let key_b64 = BASE64_STANDARD.encode(key_bytes);
        let wrong_md5 = "wrong_md5_hash_here==";

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key: key_b64,
            key_md5: wrong_md5.to_string(),
        };

        let result = validate_ssec_params(&params);
        assert!(result.is_err());
    }

    #[test]
    fn test_strip_managed_encryption_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
        metadata.insert("x-rustfs-encryption-key".to_string(), "encrypted_key".to_string());
        metadata.insert("content-type".to_string(), "text/plain".to_string());

        strip_managed_encryption_metadata(&mut metadata);

        assert!(!metadata.contains_key("x-amz-server-side-encryption"));
        assert!(!metadata.contains_key("x-rustfs-encryption-key"));
        assert!(metadata.contains_key("content-type")); // Should not be removed
    }

    #[test]
    fn test_verify_ssec_key_match_success() {
        let stored_md5 = "abc123".to_string();
        let result = verify_ssec_key_match("abc123", Some(&stored_md5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_ssec_key_match_mismatch() {
        let stored_md5 = "abc123".to_string();
        let result = verify_ssec_key_match("xyz789", Some(&stored_md5));
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_ssec_key_match_no_stored() {
        let result = verify_ssec_key_match("abc123", None);
        assert!(result.is_err());
    }
}
