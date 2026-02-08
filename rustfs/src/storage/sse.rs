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
//! ### Unified API
//! The module provides two core functions that automatically route to the correct encryption method:
//! - `apply_encryption()` - Unified encryption entry point
//! - `apply_decryption()` - Unified decryption entry point
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
//! // Unified encryption API
//! let request = EncryptionRequest {
//!     bucket: &bucket,
//!     key: &key,
//!     server_side_encryption: effective_sse.as_ref(),
//!     ssekms_key_id: effective_kms_key_id.as_deref(),
//!     sse_customer_algorithm: sse_customer_algorithm.as_ref(),
//!     sse_customer_key: sse_customer_key.as_deref(),
//!     sse_customer_key_md5: sse_customer_key_md5.as_deref(),
//!     content_size: actual_size,
//!     part_number: None,
//! };
//!
//! if let Some(material) = apply_encryption(request).await? {
//!     reader = material.wrap_reader(reader)?;
//!     metadata.extend(material.metadata);
//! }
//!
//! // Unified decryption API
//! let request = DecryptionRequest {
//!     bucket: &bucket,
//!     key: &key,
//!     metadata: &metadata,
//!     sse_customer_key: sse_customer_key.as_deref(),
//!     sse_customer_key_md5: sse_customer_key_md5.as_deref(),
//!     part_number: None,
//! };
//!
//! if let Some(material) = apply_decryption(request).await? {
//!     reader = material.wrap_reader(reader)?;
//! }
//! ```

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use rand::Rng;
use rustfs_ecstore::error::StorageError;
use rustfs_filemeta::ObjectPartInfo;
use rustfs_kms::{
    DataKey,
    service_manager::get_global_encryption_service,
    types::{EncryptionMetadata, ObjectEncryptionContext},
};
use rustfs_rio::{DecryptReader, EncryptReader, HardLimitReader, Reader, WarpReader};
use s3s::dto::ServerSideEncryption;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use tokio::io::AsyncRead;
use tracing::{debug, error};

use crate::error::ApiError;
use crate::storage::ecfs::InMemoryAsyncReader;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::error::Error;
use s3s::dto::{SSECustomerAlgorithm, SSECustomerKey, SSECustomerKeyMD5, SSEKMSKeyId};

// ============================================================================
// High-Level SSE Configuration
// ============================================================================

const DEFAULT_SSE_ALGORITHM: &str = "AES256";

const SUPPORT_SSE_ALGORITHMS: &[&str] = &[DEFAULT_SSE_ALGORITHM];

// check sse type
#[allow(unused)]
pub fn get_sse_type(
    server_side_encryption: Option<&ServerSideEncryption>,
    customer_algorithm: Option<&SSECustomerAlgorithm>,
    customer_key: Option<&SSECustomerKey>,
    customer_key_md5: Option<&SSECustomerKeyMD5>,
) -> Option<SSEType> {
    if customer_algorithm.is_some() && customer_key.is_some() && customer_key_md5.is_some() {
        return Some(SSEType::SseC);
    }

    let sse = server_side_encryption?;
    match sse.as_str() {
        ServerSideEncryption::AES256 => Some(SSEType::SseS3),
        ServerSideEncryption::AWS_KMS => Some(SSEType::SseKms),
        _ => None,
    }
}

/// SSE configuration resolved from request and bucket defaults
#[derive(Debug)]
pub struct SseConfiguration {
    /// Effective server-side encryption algorithm (after considering bucket defaults)
    pub effective_sse: ServerSideEncryption,
    /// Effective KMS key ID (after considering bucket defaults)
    pub effective_kms_key_id: Option<SSEKMSKeyId>,
}

/// Prepare SSE configuration by resolving request parameters with bucket defaults
///
/// This function:
/// 1. Queries bucket default encryption configuration
/// 2. Resolves effective encryption (request overrides bucket default)
/// 3. Prepares metadata headers for managed SSE
///
/// # Arguments
/// * `bucket` - Bucket name
/// * `server_side_encryption` - SSE algorithm from request (SSE-S3 or SSE-KMS)
/// * `ssekms_key_id` - KMS key ID from request
/// * `sse_customer_algorithm` - SSE-C algorithm from request
///
/// # Returns
/// `SseConfiguration` with resolved encryption parameters and metadata headers
async fn prepare_sse_configuration(
    bucket: &str,
    server_side_encryption: Option<ServerSideEncryption>,
    ssekms_key_id: Option<SSEKMSKeyId>,
) -> Result<Option<SseConfiguration>, ApiError> {
    if let Some(server_side_encryption) = server_side_encryption.clone()
        && let Some(ssekms_key_id) = ssekms_key_id
    {
        return Ok(Some(SseConfiguration {
            effective_sse: server_side_encryption,
            effective_kms_key_id: Some(ssekms_key_id),
        }));
    }

    // Get bucket default encryption configuration
    let bucket_sse_config_result = metadata_sys::get_sse_config(bucket).await;
    debug!("bucket_sse_config_result={:?}", bucket_sse_config_result);

    if let Ok((bucket_sse_config, _timestamp)) = bucket_sse_config_result {
        let effective_sse = server_side_encryption
            .clone()
            .or_else(|| {
                bucket_sse_config.rules.first().and_then(|rule| {
                    debug!("Processing SSE rule: {:?}", rule);
                    rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                        debug!("Found SSE default: {:?}", sse);
                        match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256), // fallback to AES256
                        }
                    })
                })
            })
            .unwrap_or_else(|| ServerSideEncryption::from_static(ServerSideEncryption::AES256));
        debug!("effective_sse={:?} (original={:?})", effective_sse, server_side_encryption);

        let effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.rules.first().and_then(|rule| {
                rule.apply_server_side_encryption_by_default
                    .as_ref()
                    .and_then(|sse| sse.kms_master_key_id.clone())
            })
        });

        Ok(Some(SseConfiguration {
            effective_sse,
            effective_kms_key_id,
        }))
    } else if let Err(e) = bucket_sse_config_result {
        match e {
            Error::ConfigNotFound => Ok(None),
            _ => Err(ApiError::from(e)),
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub enum SseTypeV2 {
    SseS3(ServerSideEncryption),
    SseKms(ServerSideEncryption, Option<SSEKMSKeyId>),
    SseC(SSECustomerAlgorithm, SSECustomerKey, SSECustomerKeyMD5),
}

impl SseTypeV2 {
    #[allow(unused)]
    pub fn to_metadata(&self) -> HashMap<String, String> {
        sse_configuration_to_metadata(self)
    }
}

pub async fn prepare_sse_configuration_v2(
    bucket: &str,
    server_side_encryption: Option<ServerSideEncryption>,
    customer_algorithm: Option<SSECustomerAlgorithm>,
    customer_key: Option<SSECustomerKey>,
    customer_key_md5: Option<SSECustomerKeyMD5>,
    ssekms_key_id: Option<SSEKMSKeyId>,
) -> Result<Option<SseTypeV2>, ApiError> {
    if let Some(customer_algorithm) = customer_algorithm
        && let Some(customer_key_md5) = customer_key_md5
    {
        // if create_multipart_upload request, customer_key is not provided
        let customer_key = customer_key.unwrap_or_default();

        return Ok(Some(SseTypeV2::SseC(customer_algorithm, customer_key, customer_key_md5)));
    }

    let sse_config = prepare_sse_configuration(bucket, server_side_encryption, ssekms_key_id).await?;

    if let Some(sse_config) = sse_config {
        return match sse_config.effective_sse.as_str() {
            ServerSideEncryption::AES256 => Ok(Some(SseTypeV2::SseS3(sse_config.effective_sse))),
            ServerSideEncryption::AWS_KMS => {
                Ok(Some(SseTypeV2::SseKms(sse_config.effective_sse.clone(), sse_config.effective_kms_key_id)))
            }
            _ => Ok(None),
        };
    }

    Ok(None)
}

#[allow(unused)]
pub fn sse_configuration_to_metadata(sse_configuration: &SseTypeV2) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    match sse_configuration {
        SseTypeV2::SseS3(sse) => {
            metadata.insert("x-amz-server-side-encryption".to_string(), sse.as_str().to_string());
        }
        SseTypeV2::SseKms(sse, kms_key_id) => {
            metadata.insert("x-amz-server-side-encryption".to_string(), sse.as_str().to_string());
            if let Some(kms_key_id) = kms_key_id {
                metadata.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), kms_key_id.to_string());
            }
        }
        SseTypeV2::SseC(algorithm, _key, key_md5) => {
            metadata.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
            metadata.insert(
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                algorithm.as_str().to_string(),
            );
            metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.to_string());
        }
    }

    metadata
}

// ============================================================================
// Core Types - Unified Encryption/Decryption API
// ============================================================================

/// Request parameters for unified encryption
#[derive(Debug, Clone)]
pub struct EncryptionRequest<'a> {
    /// Bucket name
    pub bucket: &'a str,
    /// Object key
    pub key: &'a str,
    /// Server-side encryption algorithm (SSE-S3 or SSE-KMS)
    pub server_side_encryption: Option<ServerSideEncryption>,
    /// KMS key ID (for SSE-KMS)
    pub ssekms_key_id: Option<SSEKMSKeyId>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    /// SSE-C key (Base64-encoded)
    pub sse_customer_key: Option<SSECustomerKey>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    /// Content size (for metadata)
    pub content_size: i64,

    /// Part number (for multipart upload, None for single-part)
    pub part_number: Option<usize>,
    pub part_key: Option<String>,
    pub part_nonce: Option<String>,
}

impl EncryptionRequest<'_> {
    pub fn check_upload_part_customer_key_md5(
        &self,
        user_defined: &HashMap<String, String>,
        customer_key_md5: Option<SSECustomerKeyMD5>,
    ) -> Result<(), ApiError> {
        if let Some(customer_key_md5) = customer_key_md5 {
            // if customer_key_md5 is provided, check if it matches the metadata
            let customer_key_md5_from_metadata = user_defined.get("x-amz-server-side-encryption-customer-key-md5");
            if let Some(customer_key_md5_from_metadata) = customer_key_md5_from_metadata
                && !customer_key_md5_from_metadata.eq_ignore_ascii_case(customer_key_md5.as_str())
            {
                return Err(ApiError::from(StorageError::other("Customer key MD5 mismatch")));
            }
        }

        Ok(())
    }
}

/// Request parameters for unified decryption
#[derive(Debug)]
pub struct DecryptionRequest<'a> {
    /// Bucket name
    pub bucket: &'a str,
    /// Object key
    pub key: &'a str,
    /// Object metadata containing encryption headers
    pub metadata: &'a HashMap<String, String>,
    /// SSE-C key (Base64-encoded) - required if object was encrypted with SSE-C
    pub sse_customer_key: Option<&'a SSECustomerKey>,
    /// SSE-C key MD5 (Base64-encoded) - required if object was encrypted with SSE-C
    pub sse_customer_key_md5: Option<&'a SSECustomerKeyMD5>,
    /// Part number (for multipart upload, None for single-part)
    pub part_number: Option<usize>,
    /// Parts information for multipart objects
    pub parts: &'a [ObjectPartInfo],
}

/// Unified encryption material returned by `apply_encryption()`
#[derive(Debug)]
pub struct EncryptionMaterial {
    #[allow(unused)]
    pub sse_type: SSEType,
    pub server_side_encryption: ServerSideEncryption,
    pub kms_key_id: Option<SSEKMSKeyId>,

    #[allow(unused)]
    pub algorithm: SSECustomerAlgorithm,

    /// Encryption key bytes
    pub key_bytes: [u8; 32],
    /// Nonce/IV for encryption
    pub nonce: [u8; 12],
    /// Metadata to store with the object
    pub metadata: HashMap<String, String>,
}

/// Unified decryption material returned by `apply_decryption()`
#[derive(Debug)]
pub struct DecryptionMaterial {
    #[allow(unused)]
    pub sse_type: SSEType,
    pub server_side_encryption: ServerSideEncryption,
    pub kms_key_id: Option<SSEKMSKeyId>,
    pub algorithm: SSECustomerAlgorithm,
    pub customer_key_md5: Option<SSECustomerKeyMD5>, // if use SSE-C, check key md5

    /// Decryption key bytes
    pub key_bytes: [u8; 32],
    /// Nonce/IV for decryption
    pub nonce: [u8; 12],
    /// Original unencrypted size (if available)
    pub original_size: Option<i64>,

    /// Whether this is a multipart object
    pub is_multipart: bool,
    /// Part information for multipart objects
    pub parts: Vec<ObjectPartInfo>,
}

/// Type of encryption used
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SSEType {
    /// SSE-S3 (AES256)
    SseS3,
    /// SSE-KMS (aws:kms)
    SseKms,
    /// SSE-C (customer-provided key)
    SseC,
}

impl EncryptionMaterial {
    /// Wrap a reader with encryption
    pub fn wrap_reader<R>(&self, reader: R) -> Box<EncryptReader<R>>
    where
        R: Reader + 'static,
    {
        Box::new(EncryptReader::new(reader, self.key_bytes, self.nonce))
    }
}

impl DecryptionMaterial {
    /// Wrap a reader with decryption
    /// For multipart objects, use `wrap_multipart_stream` instead
    pub fn wrap_single_reader<R>(&self, reader: R) -> Box<DecryptReader<R>>
    where
        R: Reader + 'static,
    {
        Box::new(DecryptReader::new(reader, self.key_bytes, self.nonce))
    }

    /// Wrap a stream with multipart decryption
    /// Returns the decrypted reader and the total plaintext size
    pub async fn wrap_multipart_stream(
        &self,
        encrypted_stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    ) -> Result<(Box<dyn Reader>, i64), StorageError> {
        decrypt_multipart_managed_stream(encrypted_stream, &self.parts, self.key_bytes, self.nonce).await
    }

    /// Unified method to wrap stream with decryption and hard limit
    /// Handles both single-part and multipart objects, applies decryption and size limiting
    /// Accepts AsyncRead stream (from object storage) and returns (decrypted_reader, plaintext_size)
    pub async fn wrap_reader(
        self,
        stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
        actual_size: i64,
    ) -> Result<(Box<dyn Reader>, i64), StorageError> {
        let (mut final_stream, response_content_length): (Box<dyn Reader>, i64) = if self.is_multipart {
            // Multipart decryption
            let (decrypted_reader, plain_size) = self.wrap_multipart_stream(stream).await?;
            (decrypted_reader, plain_size)
        } else {
            // Single-part decryption - wrap AsyncRead into Reader first
            let warp_reader = WarpReader::new(stream);
            let decrypt_reader = self.wrap_single_reader(warp_reader);
            let plain_size = self.original_size.unwrap_or(actual_size);
            (decrypt_reader, plain_size)
        };

        // Add hard limit reader to prevent over-reading
        // final_stream is already Box<dyn Reader>, no need to wrap with WarpReader
        let limit_reader = HardLimitReader::new(final_stream, response_content_length);
        final_stream = Box::new(limit_reader);

        debug!(
            "{:?} decryption applied: plaintext_size={}, encrypted_size={}",
            self.sse_type, response_content_length, actual_size
        );

        Ok((final_stream, response_content_length))
    }
}

// ============================================================================
// Core API - Unified Encryption/Decryption Entry Points
// ============================================================================

/// **Core API**: Apply encryption based on request parameters
///
/// This function automatically routes to the appropriate encryption method:
/// - SSE-C if customer key is provided
/// - SSE-S3/SSE-KMS if server-side encryption is requested
/// - None if no encryption is requested
///
/// # Arguments
/// * `request` - Encryption request with all possible encryption parameters
///
/// # Returns
/// * `Ok(Some(material))` - Encryption should be applied with the returned material
/// * `Ok(None)` - No encryption requested
/// * `Err` - Encryption configuration error
///
/// # Example
/// ```rust,ignore
/// let request = EncryptionRequest {
///     bucket: &bucket,
///     key: &key,
///     server_side_encryption: effective_sse.as_ref(),
///     ssekms_key_id: effective_kms_key_id.as_deref(),
///     sse_customer_algorithm: sse_customer_algorithm.as_ref(),
///     sse_customer_key: sse_customer_key.as_deref(),
///     sse_customer_key_md5: sse_customer_key_md5.as_deref(),
///     content_size: actual_size,
///     part_number: None,
/// };
///
/// if let Some(material) = apply_encryption(request).await? {
///     reader = material.wrap_reader(reader)?;
///     metadata.extend(material.metadata);
/// }
/// ```
pub async fn sse_encryption(request: EncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    // Priority 1: SSE-C (customer-provided key)
    if let (Some(algorithm), Some(key), Some(key_md5)) =
        (request.sse_customer_algorithm, request.sse_customer_key, request.sse_customer_key_md5)
    {
        return apply_ssec_encryption_material(
            request.bucket,
            request.key,
            algorithm,
            key,
            key_md5,
            request.content_size,
            request.part_number,
        )
        .await
        .map(Some);
    }

    // Priority 2: Managed SSE (SSE-S3 or SSE-KMS)
    let sse_config = prepare_sse_configuration(request.bucket, request.server_side_encryption, request.ssekms_key_id).await?;

    if let Some(sse_config) = sse_config
        && is_managed_sse(&sse_config.effective_sse)
    {
        return apply_managed_encryption_material(
            request.bucket,
            request.key,
            sse_config.effective_sse,
            sse_config.effective_kms_key_id,
            request.content_size,
            request.part_number,
            request.part_key,
            request.part_nonce,
        )
        .await
        .map(Some);
    }

    // No encryption requested
    Ok(None)
}

/// **Core API**: Apply encryption based on request parameters
///
/// sse_prepare_encryption, support SSE-C, SSE-S3, SSE-KMS
pub struct PrepareEncryptionRequest<'a> {
    /// Bucket name
    pub bucket: &'a str,
    /// Object key
    pub key: &'a str,
    /// Server-side encryption algorithm (SSE-S3 or SSE-KMS)
    pub server_side_encryption: Option<ServerSideEncryption>,
    /// KMS key ID (for SSE-KMS)
    pub ssekms_key_id: Option<SSEKMSKeyId>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
}

pub async fn sse_prepare_encryption(request: PrepareEncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    let sse_type = prepare_sse_configuration_v2(
        request.bucket,
        request.server_side_encryption,
        request.sse_customer_algorithm,
        None,
        request.sse_customer_key_md5,
        request.ssekms_key_id,
    )
    .await?;

    // apply encryption material
    let material = match sse_type {
        Some(SseTypeV2::SseS3(sse)) => {
            apply_managed_encryption_material(request.bucket, request.key, sse, None, 0, None, None, None).await?
        }
        Some(SseTypeV2::SseKms(sse, kms_key_id)) => {
            apply_managed_encryption_material(request.bucket, request.key, sse, kms_key_id, 0, None, None, None).await?
        }
        Some(SseTypeV2::SseC(algorithm, _, key_md5)) => apply_ssec_prepare_encryption_material(algorithm, key_md5).await?,
        None => return Ok(None),
    };

    Ok(Some(material))
}

/// **Core API**: Apply decryption based on stored metadata
///
/// This function automatically detects the encryption type from metadata:
/// - SSE-C if customer key is provided
/// - SSE-S3/SSE-KMS if managed encryption metadata is found
/// - None if object is not encrypted
///
/// # Arguments
/// * `request` - Decryption request with metadata and optional customer key
///
/// # Returns
/// * `Ok(Some(material))` - Decryption should be applied with the returned material
/// * `Ok(None)` - Object is not encrypted
/// * `Err` - Decryption configuration error or key mismatch
///
/// # Example
/// ```rust,ignore
/// let request = DecryptionRequest {
///     bucket: &bucket,
///     key: &key,
///     metadata: &metadata,
///     sse_customer_key: sse_customer_key.as_deref(),
///     sse_customer_key_md5: sse_customer_key_md5.as_deref(),
///     part_number: None,
/// };
///
/// if let Some(material) = apply_decryption(request).await? {
///     reader = material.wrap_reader(reader)?;
/// }
/// ```
pub async fn sse_decryption(request: DecryptionRequest<'_>) -> Result<Option<DecryptionMaterial>, ApiError> {
    let is_multipart = request.parts.len() > 1;

    // Check for SSE-C encryption
    if request
        .metadata
        .contains_key("x-amz-server-side-encryption-customer-algorithm")
    {
        let (key, key_md5) = match (request.sse_customer_key, request.sse_customer_key_md5) {
            (Some(k), Some(md5)) => (k, md5),
            _ => {
                return Err(ApiError::from(StorageError::other(
                    "Object is encrypted with SSE-C but no customer key provided",
                )));
            }
        };

        // Verify that the provided key MD5 matches the stored MD5 for security
        let stored_md5 = request.metadata.get("x-amz-server-side-encryption-customer-key-md5");
        verify_ssec_key_match(key_md5, stored_md5)?;

        let mut material =
            apply_ssec_decryption_material(request.bucket, request.key, request.metadata, key, key_md5, request.part_number)
                .await?;
        material.is_multipart = is_multipart;
        material.parts = request.parts.to_vec();
        material.customer_key_md5 = Some(key_md5.clone());

        return Ok(Some(material));
    }

    // Check for managed SSE encryption
    if request.metadata.contains_key("x-rustfs-encryption-key") {
        let mut material_opt =
            apply_managed_decryption_material(request.bucket, request.key, request.metadata, request.part_number).await?;
        if let Some(ref mut material) = material_opt {
            material.is_multipart = is_multipart;
            material.parts = request.parts.to_vec();
        }
        return Ok(material_opt);
    }

    // No encryption detected
    Ok(None)
}

// ============================================================================
// Internal Implementation - SSE-C
// ============================================================================

async fn apply_ssec_prepare_encryption_material(
    algorithm: SSECustomerAlgorithm,
    sse_key_md5: SSECustomerKeyMD5,
) -> Result<EncryptionMaterial, ApiError> {
    // Build metadata
    let mut metadata = HashMap::new();

    metadata.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), algorithm.clone());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), sse_key_md5);

    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::AES256.parse().unwrap(),
        kms_key_id: None,
        algorithm,
        key_bytes: [0; 32],
        nonce: [0; 12],
        metadata,
    })
}

async fn apply_ssec_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: SSECustomerAlgorithm,
    sse_key: SSECustomerKey,
    sse_key_md5: SSECustomerKeyMD5,
    content_size: i64,
    part_number: Option<usize>,
) -> Result<EncryptionMaterial, ApiError> {
    let params = SsecParams {
        algorithm,
        key: sse_key.to_string(),
        key_md5: sse_key_md5,
    };

    let validated = validate_ssec_params(params)?;

    // Generate nonce (deterministic for SSE-C)
    let base_nonce = generate_ssec_nonce(bucket, key);
    let nonce = if let Some(part_num) = part_number {
        derive_part_nonce(base_nonce, part_num)
    } else {
        base_nonce
    };

    // Build metadata
    let mut metadata = HashMap::new();

    metadata.insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), validated.algorithm.clone());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), validated.key_md5.clone());
    metadata.insert(
        "x-amz-server-side-encryption-customer-original-size".to_string(),
        content_size.to_string(),
    );

    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::AES256.parse().unwrap(),
        kms_key_id: None,
        algorithm: validated.algorithm,
        key_bytes: validated.key_bytes,
        nonce,
        metadata,
    })
}

async fn apply_ssec_decryption_material(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    sse_key: &str,
    sse_key_md5: &str,
    part_number: Option<usize>,
) -> Result<DecryptionMaterial, ApiError> {
    // Validate provided key
    let algorithm = metadata
        .get("x-amz-server-side-encryption-customer-algorithm")
        .map(|s| s.as_str())
        .unwrap_or("AES256");

    let params = SsecParams {
        algorithm: algorithm.to_string(),
        key: sse_key.to_string(),
        key_md5: sse_key_md5.to_string(),
    };

    let validated = validate_ssec_params(params)?;

    // Generate nonce (same as encryption)
    let base_nonce = generate_ssec_nonce(bucket, key);
    let nonce = if let Some(part_num) = part_number {
        derive_part_nonce(base_nonce, part_num)
    } else {
        base_nonce
    };

    let original_size = metadata
        .get("x-amz-server-side-encryption-customer-original-size")
        .and_then(|s| s.parse::<i64>().ok());

    Ok(DecryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::AES256.parse().unwrap(), // const
        kms_key_id: None,
        algorithm: SSECustomerAlgorithm::from(algorithm),

        customer_key_md5: None,
        key_bytes: validated.key_bytes,
        nonce,
        original_size,

        is_multipart: false,
        parts: Vec::new(),
    })
}

// ============================================================================
// Internal Implementation - Managed SSE (SSE-S3 / SSE-KMS)
// ============================================================================

#[allow(clippy::too_many_arguments)]
async fn apply_managed_encryption_material(
    bucket: &str,
    key: &str,
    server_side_encryption: ServerSideEncryption,
    kms_key_id: Option<SSEKMSKeyId>,
    content_size: i64,
    part_number: Option<usize>,
    part_key: Option<String>,
    part_nonce: Option<String>,
) -> Result<EncryptionMaterial, ApiError> {
    // For multipart, we only generate keys at CompleteMultipartUpload
    // During UploadPart, we use the same base nonce with incremented counter
    // This is handled externally, so here we just generate the base material

    if !is_managed_sse(&server_side_encryption) {
        return Err(ApiError::from(StorageError::other(format!(
            "Unsupported server-side encryption: {}",
            server_side_encryption.as_str()
        ))));
    }

    let encryption_type = match server_side_encryption.as_str() {
        "AES256" => SSEType::SseS3,
        "aws:kms" => SSEType::SseKms,
        _ => SSEType::SseS3,
    };

    let mut context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());
    if content_size >= 0 {
        context = context.with_size(content_size as u64);
    }

    // Determine KMS key ID to use
    let mut kms_key_candidate = kms_key_id.clone().map(|s| s.to_string());
    if kms_key_candidate.is_none() {
        // Try to get default key from KMS service (if available)
        if let Some(service) = get_global_encryption_service().await {
            kms_key_candidate = service.get_default_key_id().cloned();
        }
    }

    let kms_key_to_use = kms_key_candidate
        .clone()
        .ok_or_else(|| ApiError::from(StorageError::other("No KMS key available for managed server-side encryption")))?;

    let provider = get_sse_dek_provider().await?;

    let (data_key, encrypted_data_key) = if let Some(part_number) = part_number
        && let Some(part_nonce) = part_nonce
        && let Some(part_key) = part_key
        && part_number >= 1
    // upload_part mode, dek generate by create_multipart_upload
    {
        let _base_nonce = BASE64_STANDARD
            .decode(part_nonce.as_bytes())
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode nonce: {e}"))))?;
        if _base_nonce.len() != 12 {
            return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
        }
        let mut base_nonce_array = [0u8; 12];
        base_nonce_array.copy_from_slice(&_base_nonce[..12]);
        let encrypted_data_key = BASE64_STANDARD
            .decode(part_key.as_bytes())
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode data key: {e}"))))?;
        let _data_key = provider
            .decrypt_sse_dek(encrypted_data_key.as_slice(), &kms_key_to_use)
            .await?;
        let data_key = DataKey {
            plaintext_key: _data_key,
            nonce: derive_part_nonce(base_nonce_array, part_number),
        };

        // load original data key from metadata
        (data_key, encrypted_data_key)
    } else {
        // Use factory pattern to get provider (test or production mode)
        let (data_key, encrypted_data_key) = provider
            .generate_sse_dek(bucket, key, &kms_key_to_use)
            .await
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to create data key: {e}"))))?;
        (data_key, encrypted_data_key)
    };

    let algorithm = DEFAULT_SSE_ALGORITHM.to_string();

    let encryption_metadata = EncryptionMetadata {
        algorithm: algorithm.clone(),
        key_id: kms_key_to_use.clone(),
        key_version: 1,
        iv: data_key.nonce.to_vec(),
        tag: None,
        encryption_context: context.encryption_context.clone(),
        encrypted_at: jiff::Zoned::now(),
        original_size: if content_size >= 0 { content_size as u64 } else { 0 },
        encrypted_data_key,
    };

    // Build metadata headers
    let mut metadata = HashMap::new();

    // Try to use service for metadata formatting if available, otherwise build manually
    if let Some(service) = get_global_encryption_service().await {
        metadata = service.metadata_to_headers(&encryption_metadata);
    } else {
        // Manual metadata building for test mode
        metadata.insert(
            "x-rustfs-encryption-key".to_string(),
            BASE64_STANDARD.encode(&encryption_metadata.encrypted_data_key),
        );
        metadata.insert("x-rustfs-encryption-iv".to_string(), BASE64_STANDARD.encode(&encryption_metadata.iv));
        metadata.insert("x-rustfs-encryption-algorithm".to_string(), encryption_metadata.algorithm.clone());
        metadata.insert("x-amz-server-side-encryption".to_string(), server_side_encryption.as_str().to_string());

        // if kms_key is changed, we need to update the metadata
        if kms_key_id.is_none() {
            metadata.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), kms_key_to_use.clone());
        }
    }

    metadata.insert(
        "x-rustfs-encryption-original-size".to_string(),
        encryption_metadata.original_size.to_string(),
    );

    Ok(EncryptionMaterial {
        sse_type: encryption_type,
        server_side_encryption,
        kms_key_id: Some(kms_key_to_use),
        algorithm,

        key_bytes: data_key.plaintext_key,
        nonce: data_key.nonce,
        metadata,
    })
}

async fn apply_managed_decryption_material(
    _bucket: &str,
    _key: &str,
    metadata: &HashMap<String, String>,
    part_number: Option<usize>,
) -> Result<Option<DecryptionMaterial>, ApiError> {
    if !metadata.contains_key("x-rustfs-encryption-key") || !metadata.contains_key("x-amz-server-side-encryption") {
        return Ok(None);
    }

    let server_side_encryption = metadata.get("x-amz-server-side-encryption").unwrap().clone();

    // Parse metadata - try using service if available, otherwise parse manually
    let (encrypted_data_key, iv, algorithm) = if let Some(service) = get_global_encryption_service().await {
        // Production mode: use service for metadata parsing
        let parsed = service
            .headers_to_metadata(metadata)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to parse encryption metadata: {e}"))))?;

        if parsed.iv.len() != 12 {
            return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
        }

        (parsed.encrypted_data_key, parsed.iv, parsed.algorithm)
    } else {
        // Test mode: parse metadata manually
        let encrypted_key_b64 = metadata
            .get("x-rustfs-encryption-key")
            .ok_or_else(|| ApiError::from(StorageError::other("Missing encrypted key in metadata")))?;
        let encrypted_data_key = BASE64_STANDARD
            .decode(encrypted_key_b64)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode encrypted key: {e}"))))?;

        let iv_b64 = metadata
            .get("x-rustfs-encryption-iv")
            .ok_or_else(|| ApiError::from(StorageError::other("Missing IV in metadata")))?;
        let iv = BASE64_STANDARD
            .decode(iv_b64)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode IV: {e}"))))?;

        if iv.len() != 12 {
            return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
        }

        let algorithm = metadata
            .get("x-rustfs-encryption-algorithm")
            .cloned()
            .unwrap_or_else(|| "AES256".to_string());

        (encrypted_data_key, iv, algorithm)
    };

    // Extract KMS key ID from metadata (optional, used for provider context)
    let kms_key_id = metadata
        .get("x-amz-server-side-encryption-aws-kms-key-id")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    // Use factory pattern to get provider (test or production mode)
    let provider = get_sse_dek_provider().await?;
    let key_bytes = provider
        .decrypt_sse_dek(&encrypted_data_key, &kms_key_id)
        .await
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decrypt data key: {e}"))))?;

    let mut base_nonce = [0u8; 12];
    base_nonce.copy_from_slice(&iv[..12]);
    let nonce = if let Some(part_num) = part_number {
        derive_part_nonce(base_nonce, part_num)
    } else {
        base_nonce
    };

    let original_size = metadata
        .get("x-rustfs-encryption-original-size")
        .and_then(|s| s.parse::<i64>().ok());

    let encryption_type = match server_side_encryption.as_str() {
        ServerSideEncryption::AES256 => SSEType::SseS3,
        ServerSideEncryption::AWS_KMS => SSEType::SseKms,
        _ => SSEType::SseS3,
    };

    Ok(Some(DecryptionMaterial {
        sse_type: encryption_type,
        server_side_encryption: ServerSideEncryption::from(server_side_encryption),
        kms_key_id: Some(SSEKMSKeyId::from(kms_key_id)),
        algorithm,
        customer_key_md5: None,

        key_bytes,
        nonce,
        original_size,

        is_multipart: false,
        parts: Vec::new(),
    }))
}

// ============================================================================
// Legacy Types (for backward compatibility)
// ============================================================================

/// Validated SSE-C parameters
#[derive(Debug, Clone)]
pub struct ValidatedSsecParams {
    /// Encryption algorithm (always "AES256" for SSE-C)
    pub algorithm: SSECustomerAlgorithm,
    /// Decoded encryption key bytes (32 bytes for AES-256)
    pub key_bytes: [u8; 32],
    /// Base64-encoded MD5 of the key
    pub key_md5: SSECustomerKeyMD5,
}

/// SSE-C parameters from client request
#[derive(Debug, Clone)]
pub struct SsecParams {
    /// Encryption algorithm
    pub algorithm: SSECustomerAlgorithm,
    /// Base64-encoded encryption key
    pub key: SSECustomerKey,
    /// Base64-encoded MD5 of the key
    pub key_md5: SSECustomerKeyMD5,
}

// ============================================================================
// SSE DEK Provider Abstraction (Factory Pattern)
// ============================================================================

/// Trait for SSE data encryption key management
/// Abstracts the source of encryption keys (KMS, test provider, etc.)
#[async_trait]
pub trait SseDekProvider: Send + Sync {
    /// Generate an SSE data encryption key
    async fn generate_sse_dek(&self, bucket: &str, key: &str, kms_key_id: &str) -> Result<(DataKey, Vec<u8>), ApiError>;

    /// Decrypt an SSE data encryption key (returns only plaintext key, nonce should be read from metadata)
    async fn decrypt_sse_dek(&self, encrypted_dek: &[u8], kms_key_id: &str) -> Result<[u8; 32], ApiError>;
}

// ============================================================================
// Production KMS-backed DEK Provider
// ============================================================================

/// Production KMS-backed DEK provider
/// Wraps the global ObjectEncryptionService to provide SSE DEK operations
struct KmsSseDekProvider {
    service: Arc<rustfs_kms::service::ObjectEncryptionService>,
}

impl KmsSseDekProvider {
    /// Create a new KMS-backed provider
    pub async fn new() -> Result<Self, ApiError> {
        let service = get_global_encryption_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other("KMS encryption service is not initialized")))?;
        Ok(Self { service })
    }
}

#[async_trait]
impl SseDekProvider for KmsSseDekProvider {
    async fn generate_sse_dek(&self, bucket: &str, key: &str, kms_key_id: &str) -> Result<(DataKey, Vec<u8>), ApiError> {
        let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());

        let kms_key_option = Some(kms_key_id.to_string());
        let (data_key, encrypted_data_key) = self
            .service
            .create_data_key(&kms_key_option, &context)
            .await
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to create data key: {}", e))))?;

        Ok((data_key, encrypted_data_key))
    }

    async fn decrypt_sse_dek(&self, encrypted_dek: &[u8], _kms_key_id: &str) -> Result<[u8; 32], ApiError> {
        // Create a minimal context for decryption
        let context = ObjectEncryptionContext::new("".to_string(), "".to_string());
        let data_key = self
            .service
            .decrypt_data_key(encrypted_dek, &context)
            .await
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decrypt data key: {}", e))))?;

        Ok(data_key.plaintext_key)
    }
}

// ============================================================================
// Test/Simple DEK Provider
// ============================================================================

/// Simple SSE DEK provider for testing purposes
///
/// This provider reads a single 32-byte customer master key (CMK) from the
/// `__RUSTFS_SSE_SIMPLE_CMK` environment variable. The key must be base64-encoded.
///
/// # Environment Variable Format
///
/// ```text
/// __RUSTFS_SSE_SIMPLE_CMK=<base64_encoded_32_byte_key>
/// ```
///
/// Example:
/// ```bash
/// export __RUSTFS_SSE_SIMPLE_CMK="AKHul86TBMMJ3+VrGlh9X3dHJsOtSXOXHOODPwmAnOo="
/// ```
///
/// # Key Generation
///
/// Use the provided script to generate a valid key:
/// ```bash
/// # Windows
/// .\scripts\generate-sse-keys.ps1
///
/// # Linux/Unix/macOS
/// ./scripts/generate-sse-keys.sh
/// ```
pub(crate) struct TestSseDekProvider {
    master_key: [u8; 32],
}

impl TestSseDekProvider {
    /// Create a SimpleSseDekProvider with a predefined key (for testing)
    #[cfg(test)]
    pub fn new_with_key(master_key: [u8; 32]) -> Self {
        Self { master_key }
    }

    pub fn new() -> Self {
        let cmk_value = std::env::var("__RUSTFS_SSE_SIMPLE_CMK").unwrap_or_else(|_| "".to_string());

        let master_key = if !cmk_value.is_empty() {
            match BASE64_STANDARD.decode(cmk_value.trim()) {
                Ok(v) => {
                    let decoded_len = v.len();
                    match v.try_into() {
                        Ok(arr) => {
                            println!("✓ Successfully loaded master key (32 bytes)");
                            arr
                        }
                        Err(_) => {
                            eprintln!("✗ Failed to load master key: decoded key is not 32 bytes (got {} bytes)", decoded_len);
                            [0u8; 32]
                        }
                    }
                }
                Err(e) => {
                    eprintln!("✗ Failed to load master key: invalid base64 encoding: {}", e);
                    [0u8; 32]
                }
            }
        } else {
            [0u8; 32]
        };

        if master_key == [0u8; 32] {
            eprintln!("✗ Failed to load master key: no valid master key loaded! All encryption operations will fail.");
            eprintln!("    Set __RUSTFS_SSE_SIMPLE_CMK environment variable to a base64-encoded 32-byte key.");
            std::process::exit(1);
        }

        Self { master_key }
    }

    // Simple encryption of DEK
    pub(crate) fn encrypt_dek(dek: [u8; 32], cmk_value: [u8; 32]) -> Result<String, ApiError> {
        // Use AES-256-GCM to encrypt DEK
        let key = Key::<Aes256Gcm>::from(cmk_value);

        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from([0u8; 12]);
        let ciphertext = cipher
            .encrypt(&nonce, dek.as_slice())
            .map_err(|_| ApiError::from(StorageError::other("Failed to encrypt DEK")))?;

        // nonce:ciphertext
        Ok(format!("{}:{}", BASE64_STANDARD.encode(nonce), BASE64_STANDARD.encode(ciphertext)))
    }

    // Simple decryption of DEK
    pub(crate) fn decrypt_dek(encrypted_dek: &str, cmk_value: [u8; 32]) -> Result<[u8; 32], ApiError> {
        let parts: Vec<&str> = encrypted_dek.split(':').collect();
        if parts.len() != 2 {
            return Err(ApiError::from(StorageError::other("Invalid encrypted DEK format")));
        }

        let nonce_vec = BASE64_STANDARD
            .decode(parts[0])
            .map_err(|_| ApiError::from(StorageError::other("Invalid nonce format")))?;
        let ciphertext = BASE64_STANDARD
            .decode(parts[1])
            .map_err(|_| ApiError::from(StorageError::other("Invalid ciphertext format")))?;

        let key = Key::<Aes256Gcm>::from(cmk_value);
        let cipher = Aes256Gcm::new(&key);

        let nonce_array: [u8; 12] = nonce_vec
            .try_into()
            .map_err(|_| ApiError::from(StorageError::other("Invalid nonce length")))?;
        let nonce = Nonce::from(nonce_array);

        let plaintext = cipher
            .decrypt(&nonce, ciphertext.as_slice())
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decrypt DEK: {e}"))))?;

        let dek: [u8; 32] = plaintext
            .try_into()
            .map_err(|_| ApiError::from(StorageError::other("Decrypted DEK has invalid length")))?;

        Ok(dek)
    }
}

#[async_trait]
impl SseDekProvider for TestSseDekProvider {
    async fn generate_sse_dek(&self, _bucket: &str, _key: &str, _kms_key_id: &str) -> Result<(DataKey, Vec<u8>), ApiError> {
        // Generate a 32-byte array as data key
        let mut dek = [0u8; 32];
        rand::rng().fill_bytes(&mut dek);

        // Generate a 12-byte array as IV
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce);

        // Encrypt data key with master key
        let encrypted_dek = Self::encrypt_dek(dek, self.master_key)?;

        // Return data key and IV
        Ok((
            DataKey {
                plaintext_key: dek,
                nonce,
            },
            encrypted_dek.into_bytes(),
        ))
    }

    async fn decrypt_sse_dek(&self, encrypted_dek: &[u8], _kms_key_id: &str) -> Result<[u8; 32], ApiError> {
        // Decrypt data key with master key
        let encrypted_dek_str = std::str::from_utf8(encrypted_dek)
            .map_err(|_| ApiError::from(StorageError::other("Invalid UTF-8 in encrypted DEK")))?;
        let dek = Self::decrypt_dek(encrypted_dek_str, self.master_key)?;
        Ok(dek)
    }
}

// ============================================================================
// Factory Function for SSE DEK Provider
// ============================================================================

/// Global SSE DEK provider cache
static GLOBAL_SSE_DEK_PROVIDER: OnceLock<Arc<dyn SseDekProvider>> = OnceLock::new();

/// Get or initialize the global SSE DEK provider
///
/// Factory function that automatically selects the appropriate provider:
/// - If `__RUSTFS_SSE_SIMPLE_CMK` environment variable exists: use SimpleSseDekProvider (test mode)
/// - Otherwise: use KmsSseDekProvider (production mode with real KMS)
///
/// # Returns
/// Arc to the global SSE DEK provider instance
///
/// # Example
/// ```rust,ignore
/// let provider = get_sse_dek_provider().await?;
/// let (data_key, encrypted_dek) = provider
///     .generate_sse_dek("bucket", "key", "kms-key-id")
///     .await?;
/// ```
pub async fn get_sse_dek_provider() -> Result<Arc<dyn SseDekProvider>, ApiError> {
    // Check if already initialized
    if let Some(provider) = GLOBAL_SSE_DEK_PROVIDER.get() {
        return Ok(provider.clone());
    }

    // Determine provider based on environment variable
    let provider: Arc<dyn SseDekProvider> = if std::env::var("__RUSTFS_SSE_SIMPLE_CMK").is_ok() {
        debug!("Using SimpleSseDekProvider (test mode) based on __RUSTFS_SSE_SIMPLE_CMK");
        Arc::new(TestSseDekProvider::new())
    } else {
        debug!("Using KmsSseDekProvider (production mode)");
        Arc::new(KmsSseDekProvider::new().await?)
    };

    // Store in global cache
    GLOBAL_SSE_DEK_PROVIDER
        .set(provider.clone())
        .map_err(|_| ApiError::from(StorageError::other("Failed to initialize global SSE DEK provider (already set)")))?;

    Ok(provider)
}

// check encryption metadata
pub fn check_encryption_metadata(metadata: &HashMap<String, String>) -> bool {
    if !metadata.contains_key("x-rustfs-encryption-key") && !metadata.contains_key("x-amz-server-side-encryption") {
        return false;
    }

    true
}

/// Reset the global SSE DEK provider (for testing only)
///
/// Note: OnceLock doesn't support reset in stable Rust.
/// Tests should set environment variables before first call to `get_sse_dek_provider()`.
#[cfg(test)]
#[allow(dead_code)]
pub fn reset_sse_dek_provider() {
    // OnceLock doesn't support reset - this is a documentation placeholder
    // Consider using arc_swap::ArcSwap if runtime reset is needed
}

// ============================================================================
// Legacy Functions (SSE-S3 / SSE-KMS)
// ============================================================================

/// Check if the server_side_encryption is a managed SSE type (SSE-S3 or SSE-KMS)
#[inline]
pub fn is_managed_sse(server_side_encryption: &ServerSideEncryption) -> bool {
    matches!(server_side_encryption.as_str(), "AES256" | "aws:kms")
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

pub(crate) async fn decrypt_multipart_managed_stream(
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
// SSE-C Functions
// ============================================================================

/// Validate SSE-C parameters from client request
///
/// Validates:
/// 1. Algorithm is "AES256"
/// 2. Key is valid Base64 and exactly 32 bytes
/// 3. MD5 hash matches the key
///
/// # Returns
/// `ValidatedSsecParams` with decoded key bytes
pub fn validate_ssec_params(params: SsecParams) -> Result<ValidatedSsecParams, ApiError> {
    // Validate algorithm
    if !SUPPORT_SSE_ALGORITHMS.contains(&params.algorithm.as_str()) {
        return Err(ApiError::from(StorageError::other(format!(
            "Unsupported SSE-C algorithm: {}. Only {} is supported",
            params.algorithm, DEFAULT_SSE_ALGORITHM
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
        algorithm: params.algorithm,
        key_bytes: key_array,
        key_md5: params.key_md5,
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

/// Verify SSE-C key matches the stored metadata
///
/// Used during GetObject to ensure the client provided the correct key.
pub fn verify_ssec_key_match(provided_md5: &str, stored_md5: Option<&String>) -> Result<(), ApiError> {
    match stored_md5 {
        Some(stored) if stored == provided_md5 => Ok(()),
        Some(stored) => Err(ApiError::from(StorageError::other(format!(
            "SSE-C key MD5 mismatch: provided '{}' but expected '{}'",
            provided_md5, stored
        )))),
        None => Err(ApiError::from(StorageError::other("Object has no stored SSE-C key MD5"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_managed_sse() {
        assert!(is_managed_sse(&ServerSideEncryption::from_static("AES256")));
        assert!(is_managed_sse(&ServerSideEncryption::from_static("aws:kms")));
    }

    #[test]
    fn test_derive_part_nonce() {
        let base = [1, 2, 3, 4, 5, 6, 7, 8, 0, 0, 0, 10];
        let part1 = derive_part_nonce(base, 1);
        let part2 = derive_part_nonce(base, 2);

        // First 8 bytes should be unchanged
        assert_eq!(&base[..8], &part1[..8]);
        assert_eq!(&base[..8], &part2[..8]);

        // Last 4 bytes should be incremented
        assert_ne!(&base[8..], &part1[8..]);
        assert_ne!(&part1[8..], &part2[8..]);
    }

    #[test]
    fn test_generate_ssec_nonce() {
        let nonce1 = generate_ssec_nonce("bucket1", "key1");
        let nonce2 = generate_ssec_nonce("bucket1", "key1");
        let nonce3 = generate_ssec_nonce("bucket1", "key2");

        // Same inputs should produce same nonce
        assert_eq!(nonce1, nonce2);

        // Different inputs should produce different nonce
        assert_ne!(nonce1, nonce3);

        // Nonce should be exactly 12 bytes
        assert_eq!(nonce1.len(), 12);
    }

    #[test]
    fn test_validate_ssec_params_success() {
        let key = BASE64_STANDARD.encode([42u8; 32]);
        let key_md5 = BASE64_STANDARD.encode(md5::compute([42u8; 32]).0);

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key,
            key_md5,
        };

        let result = validate_ssec_params(params);
        assert!(result.is_ok());
        let validated = result.unwrap();
        assert_eq!(validated.key_bytes, [42u8; 32]);
    }

    #[test]
    fn test_validate_ssec_params_wrong_algorithm() {
        let key = BASE64_STANDARD.encode([42u8; 32]);
        let key_md5 = BASE64_STANDARD.encode(md5::compute([42u8; 32]).0);

        let params = SsecParams {
            algorithm: "AES128".to_string(), // Wrong algorithm
            key,
            key_md5,
        };

        let result = validate_ssec_params(params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ssec_params_wrong_key_length() {
        let key = BASE64_STANDARD.encode([42u8; 16]); // Only 16 bytes
        let key_md5 = BASE64_STANDARD.encode(md5::compute([42u8; 16]).0);

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key,
            key_md5,
        };

        let result = validate_ssec_params(params);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_ssec_params_wrong_md5() {
        let key = BASE64_STANDARD.encode([42u8; 32]);
        let key_md5 = BASE64_STANDARD.encode([99u8; 16]); // Wrong MD5

        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key,
            key_md5,
        };

        let result = validate_ssec_params(params);
        assert!(result.is_err());
    }

    #[test]
    fn test_strip_managed_encryption_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
        metadata.insert("x-rustfs-encryption-key".to_string(), "encrypted_key".to_string());
        metadata.insert("content-type".to_string(), "text/plain".to_string());

        strip_managed_encryption_metadata(&mut metadata);

        assert!(!metadata.contains_key("x-amz-server-side-encryption"));
        assert!(!metadata.contains_key("x-rustfs-encryption-key"));
        assert!(metadata.contains_key("content-type"));
    }

    #[test]
    fn test_verify_ssec_key_match_success() {
        let md5 = "test_md5".to_string();
        let result = verify_ssec_key_match("test_md5", Some(&md5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_ssec_key_match_mismatch() {
        let md5 = "stored_md5".to_string();
        let result = verify_ssec_key_match("provided_md5", Some(&md5));
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_ssec_key_match_no_stored() {
        let result = verify_ssec_key_match("provided_md5", None);
        assert!(result.is_err());
    }

    // ============================================================================
    // Integration Tests - Encrypt/Decrypt with SimpleSseDekProvider
    // ============================================================================

    #[tokio::test]
    async fn test_simple_sse_dek_provider_encrypt_decrypt() {
        use std::io::Cursor;
        use tokio::io::AsyncReadExt;

        // 1. Setup: Create SimpleSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([42u8; 32]);

        // 2. Generate a data encryption key
        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default"; // Key ID is ignored in simple provider

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // 3. Prepare test data (明文)
        let plaintext = b"Hello, World! This is a test message for encryption and decryption.";
        println!("Original plaintext: {:?}", String::from_utf8_lossy(plaintext));
        println!("Plaintext length: {} bytes", plaintext.len());

        // 4. Encrypt with EncryptReader (wrap Cursor with WarpReader)
        let plaintext_reader = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader = EncryptReader::new(plaintext_reader, data_key.plaintext_key, data_key.nonce);

        // Read encrypted data
        let mut encrypted_data = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted_data)
            .await
            .expect("Failed to read encrypted data");

        println!("Encrypted data length: {} bytes", encrypted_data.len());
        println!(
            "First 16 bytes of encrypted data: {:02x?}",
            &encrypted_data[..16.min(encrypted_data.len())]
        );

        // Verify encrypted data is different from plaintext
        assert_ne!(
            &encrypted_data[..plaintext.len()],
            plaintext,
            "Encrypted data should be different from plaintext"
        );

        // 5. Decrypt with DecryptReader (wrap Cursor with WarpReader)
        let encrypted_reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(encrypted_reader, data_key.plaintext_key, data_key.nonce);

        // Read decrypted data
        let mut decrypted_data = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to read decrypted data");

        println!("Decrypted data: {:?}", String::from_utf8_lossy(&decrypted_data));
        println!("Decrypted length: {} bytes", decrypted_data.len());

        // 6. Verify decrypted data matches original plaintext
        assert_eq!(decrypted_data, plaintext, "Decrypted data should match original plaintext");

        println!("✅ Encryption/Decryption test passed!");
    }

    #[tokio::test]
    async fn test_simple_sse_dek_provider_encrypt_decrypt_large_data() {
        use std::io::Cursor;
        use tokio::io::AsyncReadExt;

        // 1. Setup: Create SimpleSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key-large";
        let kms_key_id = "default";

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // Create 1MB of test data
        let plaintext_size = 1024 * 1024; // 1MB
        let plaintext: Vec<u8> = (0..plaintext_size).map(|i| (i % 256) as u8).collect();
        println!("Testing with {} bytes of data", plaintext.len());

        // Encrypt (wrap with WarpReader)
        let plaintext_reader = WarpReader::new(Cursor::new(plaintext.clone()));
        let mut encrypt_reader = EncryptReader::new(plaintext_reader, data_key.plaintext_key, data_key.nonce);

        let mut encrypted_data = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted_data)
            .await
            .expect("Failed to encrypt large data");

        println!("Encrypted {} bytes to {} bytes", plaintext.len(), encrypted_data.len());

        // Decrypt (wrap with WarpReader)
        let encrypted_reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(encrypted_reader, data_key.plaintext_key, data_key.nonce);

        let mut decrypted_data = Vec::new();
        decrypt_reader
            .read_to_end(&mut decrypted_data)
            .await
            .expect("Failed to decrypt large data");

        // Verify
        assert_eq!(decrypted_data.len(), plaintext.len(), "Decrypted size should match original");
        assert_eq!(decrypted_data, plaintext, "Decrypted data should match original plaintext");

        println!("✅ Large data encryption/decryption test passed!");
    }

    #[tokio::test]
    async fn test_simple_sse_dek_provider_different_nonces() {
        use std::io::Cursor;
        use tokio::io::AsyncReadExt;

        // 1. Setup: Create SimpleSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default";

        // Generate two different keys (with different nonces)
        let (data_key1, _) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK 1");

        let (data_key2, _) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK 2");

        // Verify nonces are different
        assert_ne!(data_key1.nonce, data_key2.nonce, "Different keys should have different nonces");

        // Same plaintext
        let plaintext = b"Same plaintext";

        // Encrypt with first key (wrap with WarpReader)
        let reader1 = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader1 = EncryptReader::new(reader1, data_key1.plaintext_key, data_key1.nonce);
        let mut encrypted1 = Vec::new();
        encrypt_reader1.read_to_end(&mut encrypted1).await.unwrap();

        // Encrypt with second key (wrap with WarpReader)
        let reader2 = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader2 = EncryptReader::new(reader2, data_key2.plaintext_key, data_key2.nonce);
        let mut encrypted2 = Vec::new();
        encrypt_reader2.read_to_end(&mut encrypted2).await.unwrap();

        // Verify ciphertexts are different (due to different nonces/keys)
        assert_ne!(
            encrypted1, encrypted2,
            "Same plaintext with different nonces should produce different ciphertext"
        );

        println!("✅ Different nonces produce different ciphertext - test passed!");
    }

    #[tokio::test]
    async fn test_simple_sse_dek_provider_decrypt_with_encrypted_dek() {
        use std::io::Cursor;
        use tokio::io::AsyncReadExt;

        // 1. Setup: Create SimpleSseDekProvider with test master key
        let provider = TestSseDekProvider::new_with_key([42u8; 32]);

        let bucket = "test-bucket";
        let key = "test-key";
        let kms_key_id = "default";

        // 1. Generate DEK and get encrypted DEK
        let (data_key, encrypted_dek) = provider
            .generate_sse_dek(bucket, key, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        let original_plaintext_key = data_key.plaintext_key;
        let original_nonce = data_key.nonce;

        // 2. Simulate storing encrypted_dek and nonce in metadata
        // In real scenario, nonce would be stored separately in metadata

        // 3. Later, decrypt the DEK
        let decrypted_plaintext_key = provider
            .decrypt_sse_dek(&encrypted_dek, kms_key_id)
            .await
            .expect("Failed to decrypt DEK");

        // 4. Verify decrypted key matches original
        assert_eq!(
            decrypted_plaintext_key, original_plaintext_key,
            "Decrypted DEK should match original plaintext key"
        );

        // 5. Use decrypted key to encrypt/decrypt data
        let plaintext = b"Test data with decrypted DEK";

        // Encrypt with original key (wrap with WarpReader)
        let reader = WarpReader::new(Cursor::new(plaintext.to_vec()));
        let mut encrypt_reader = EncryptReader::new(reader, original_plaintext_key, original_nonce);
        let mut encrypted_data = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted_data).await.unwrap();

        // Decrypt with recovered key (simulating GET operation) (wrap with WarpReader)
        let reader = WarpReader::new(Cursor::new(encrypted_data));
        let mut decrypt_reader = DecryptReader::new(
            reader,
            decrypted_plaintext_key,
            original_nonce, // In real scenario, read from metadata
        );
        let mut decrypted_data = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted_data).await.unwrap();

        // Verify
        assert_eq!(decrypted_data, plaintext, "Data decrypted with recovered key should match original");

        println!("✅ Full cycle (generate -> encrypt DEK -> decrypt DEK -> decrypt data) test passed!");
    }

    #[test]
    fn test_encryption_type_enum() {
        // Test EncryptionType enum
        assert_eq!(SSEType::SseS3, SSEType::SseS3);
        assert_eq!(SSEType::SseKms, SSEType::SseKms);
        assert_eq!(SSEType::SseC, SSEType::SseC);
        assert_ne!(SSEType::SseS3, SSEType::SseKms);

        // Test Debug format
        let debug_str = format!("{:?}", SSEType::SseKms);
        assert!(debug_str.contains("SseKms"));
    }
}
