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
//! - `sse_encryption()` - Unified encryption entry point
//! - `sse_decryption()` - Unified decryption entry point
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
//! };
//!
//! if let Some(material) = sse_encryption(request).await? {
//!     metadata.extend(encryption_material_to_metadata(&material)?);
//! }
//!
//! // Unified decryption API
//! let request = DecryptionRequest {
//!     bucket: &bucket,
//!     key: &key,
//!     metadata: &metadata,
//!     sse_customer_key: sse_customer_key.as_deref(),
//!     sse_customer_key_md5: sse_customer_key_md5.as_deref(),
//! };
//!
//! if let Some(material) = sse_decryption(request).await? {
//!     content_size = material.original_size.unwrap_or(actual_size);
//! }
//! ```

use super::StorageError;
use super::storage_api::ecstore_object::{
    EncryptionResolutionError, EncryptionResolutionErrorKind, ObjectEncryptionResolver, ReadEncryptionMaterial,
    ReadEncryptionMode, ReadEncryptionRequest,
};
use crate::storage::access::{ReqInfo, request_context_from_req, resource_free_condition_values};
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
#[cfg(feature = "rio-v2")]
use aes_gcm::aead::Payload;
use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit},
};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
#[cfg(feature = "rio-v2")]
use chacha20poly1305::ChaCha20Poly1305;
#[cfg(feature = "rio-v2")]
use hmac::{Hmac, Mac};
use http::{HeaderMap, HeaderValue};
use md5::{Digest as Md5Digest, Md5};
use rand::Rng;
#[cfg(feature = "rio-v2")]
use rand::RngExt;
use rustfs_config::{DEFAULT_KMS_ENFORCE_SSE_KEY_POLICY, ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY};
use rustfs_kms::{DataKey, KmsUnavailableError, is_data_key_envelope, types::ObjectEncryptionContext};
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::{Action, KmsAction};
use rustfs_utils::{get_env_bool, get_env_opt_str};
use s3s::S3ErrorCode;
use s3s::S3Request;
use s3s::dto::ServerSideEncryption;
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(feature = "rio-v2")]
use sha2::Sha256;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, LazyLock, Mutex, RwLock, Weak};
use tracing::{debug, error};

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_SSE: &str = "sse";

use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_CONTEXT_HEADER, INTERNAL_ENCRYPTION_IV_HEADER,
    INTERNAL_ENCRYPTION_KEY_HEADER, INTERNAL_ENCRYPTION_KEY_ID_HEADER, INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER,
    INTERNAL_ENCRYPTION_TAG_HEADER, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEC_ORIGINAL_SIZE_HEADER, normalize_managed_metadata,
    stored_managed_encryption_key,
};
// The managed-SSE classifier lives in the shared encryption-keys module so the
// scanner can reuse it (backlog#1643 PR-B0); these re-exports keep the
// historical `crate::storage::sse` paths compiling.
pub use rustfs_utils::http::object_encryption_keys::SSEType;
pub(crate) use rustfs_utils::http::object_encryption_keys::contains_managed_encryption_metadata;
#[cfg(feature = "rio-v2")]
const MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM: &str = "DAREv2-HMAC-SHA256";
#[cfg(feature = "rio-v2")]
const DARE_VERSION_20: u8 = 0x20;
#[cfg(feature = "rio-v2")]
const DARE_CIPHER_AES_256_GCM: u8 = 0x00;
#[cfg(feature = "rio-v2")]
const DARE_CIPHER_CHACHA20_POLY1305: u8 = 0x01;
#[cfg(feature = "rio-v2")]
const DARE_HEADER_SIZE: usize = 16;
#[cfg(feature = "rio-v2")]
const DARE_TAG_SIZE: usize = 16;
#[cfg(feature = "rio-v2")]
const SEALED_KEY_IV_SIZE: usize = 32;
#[cfg(feature = "rio-v2")]
const SEALED_KEY_SIZE: usize = DARE_HEADER_SIZE + 32 + DARE_TAG_SIZE;
#[cfg(feature = "rio-v2")]
const OBJECT_KEY_DERIVATION_CONTEXT: &[u8] = b"object-encryption-key generation";

fn md5_bytes(input: impl AsRef<[u8]>) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(input.as_ref());
    hasher.finalize().into()
}

fn md5_base64(input: impl AsRef<[u8]>) -> String {
    BASE64_STANDARD.encode(md5_bytes(input))
}

use super::Error;
use super::get_bucket_sse_config;
use crate::error::ApiError;
use rustfs_utils::http::headers::{
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
};
use rustfs_utils::path::path_join_buf;
use s3s::dto::{SSECustomerAlgorithm, SSECustomerKey, SSECustomerKeyMD5, SSEKMSKeyId, ServerSideEncryptionByDefault};
use std::borrow::Cow;

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
/// Managed SSE resolved from a bucket default encryption rule on a write path.
///
/// The single mapping shared by every writer: this resolver, and the PUT, COPY
/// and extract paths in `app::object_usecase`, which reach it through
/// `resolve_bucket_default_sse`. Unknown algorithms fall back to AES256 rather
/// than to `None`. Resolving `None` instead lets a same-name copy under a
/// malformed bucket default pass the `copy_changes_encryption` guard and take
/// the metadata-only shortcut while this layer still encrypts: fresh DEK
/// metadata is committed beside the untouched plaintext blocks and the object
/// becomes unreadable. Reachable only via corrupt or hand-edited bucket
/// metadata — PutBucketEncryption rejects unknown algorithms (backlog#1826).
pub(crate) fn bucket_default_write_sse(sse: &ServerSideEncryptionByDefault) -> ServerSideEncryption {
    match sse.sse_algorithm.as_str() {
        "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
        "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
        _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
    }
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
        && server_side_encryption.as_str() == ServerSideEncryption::AES256
    {
        return Ok(Some(SseConfiguration {
            effective_sse: server_side_encryption,
            effective_kms_key_id: None,
        }));
    }

    if let Some(server_side_encryption) = server_side_encryption.clone()
        && let Some(ssekms_key_id) = ssekms_key_id
    {
        return Ok(Some(SseConfiguration {
            effective_sse: server_side_encryption,
            effective_kms_key_id: Some(ssekms_key_id),
        }));
    }

    // Get bucket default encryption configuration.
    let bucket_sse_config_result = get_bucket_sse_config(bucket).await;
    debug!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "bucket_sse_config_lookup",
        bucket = %bucket,
        found = bucket_sse_config_result.is_ok(),
        "Bucket SSE configuration lookup completed"
    );

    if let Ok((bucket_sse_config, _timestamp)) = bucket_sse_config_result {
        let effective_sse = server_side_encryption.clone().or_else(|| {
            bucket_sse_config.rules.first().and_then(|rule| {
                rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                    debug!(
                        component = LOG_COMPONENT_STORAGE,
                        subsystem = LOG_SUBSYSTEM_SSE,
                        event = "bucket_sse_default_applied",
                        bucket = %bucket,
                        algorithm = sse.sse_algorithm.as_str(),
                        has_kms_key_id = sse.kms_master_key_id.is_some(),
                        "Bucket SSE default resolved"
                    );
                    bucket_default_write_sse(sse)
                })
            })
        });
        let Some(effective_sse) = effective_sse else {
            return Ok(None);
        };

        debug!(
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            event = "effective_sse_resolved",
            bucket = %bucket,
            requested = ?server_side_encryption,
            effective = ?effective_sse,
            "Resolved effective SSE configuration"
        );

        let effective_kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), ssekms_key_id, || {
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
            Error::ConfigNotFound => {
                // The bucket has no SSE config. If the user explicitly requested
                // aws:kms, we must honor that — return the explicit SSE header so
                // downstream logic can try (and fail if KMS is unavailable).
                if let Some(sse) = server_side_encryption {
                    Ok(Some(SseConfiguration {
                        effective_sse: sse,
                        effective_kms_key_id: ssekms_key_id,
                    }))
                } else {
                    Ok(None)
                }
            }
            _ => Err(ApiError::from(e)),
        }
    } else {
        Ok(None)
    }
}

fn resolve_effective_kms_key_id<F>(
    effective_sse: Option<&ServerSideEncryption>,
    requested_kms_key_id: Option<SSEKMSKeyId>,
    bucket_default_kms_key_id: F,
) -> Option<SSEKMSKeyId>
where
    F: FnOnce() -> Option<SSEKMSKeyId>,
{
    if effective_sse.is_none_or(|sse| sse.as_str() != ServerSideEncryption::AWS_KMS) {
        return requested_kms_key_id;
    }

    requested_kms_key_id.or_else(bucket_default_kms_key_id)
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
    /// Optional client-provided KMS context for SSE-KMS.
    pub ssekms_context: Option<HashMap<String, String>>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    /// SSE-C key (Base64-encoded)
    pub sse_customer_key: Option<SSECustomerKey>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    /// Content size (for metadata)
    pub content_size: i64,
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    pub principal: Option<&'a SseKmsPrincipal>,
}

impl EncryptionRequest<'_> {
    pub fn validate_multipart_ssec(&self, user_defined: &HashMap<String, String>) -> Result<(), ApiError> {
        let stored_algorithm = user_defined.get("x-amz-server-side-encryption-customer-algorithm");
        let stored_key_md5 = user_defined.get("x-amz-server-side-encryption-customer-key-md5");
        let session_uses_ssec = stored_algorithm.is_some() || stored_key_md5.is_some();
        let request_uses_ssec =
            self.sse_customer_algorithm.is_some() || self.sse_customer_key.is_some() || self.sse_customer_key_md5.is_some();

        if !session_uses_ssec {
            return if request_uses_ssec {
                Err(ssec_invalid_request(
                    "SSE-C parameters cannot be used for a multipart upload that was not initiated with SSE-C.",
                ))
            } else {
                Ok(())
            };
        }

        let (Some(algorithm), Some(key), Some(key_md5)) = (
            self.sse_customer_algorithm.as_ref(),
            self.sse_customer_key.as_ref(),
            self.sse_customer_key_md5.as_ref(),
        ) else {
            return Err(ssec_invalid_request(
                "Missing SSE-C parameters. Algorithm, customer key and customer key MD5 are all required.",
            ));
        };

        let validated = validate_ssec_params(SsecParams {
            algorithm: algorithm.to_string(),
            key: key.to_string(),
            key_md5: key_md5.to_string(),
        })?;
        if stored_algorithm.map(String::as_str) != Some(validated.algorithm.as_str()) {
            return Err(ssec_invalid_request(
                "The provided encryption parameters did not match the multipart upload.",
            ));
        }
        verify_ssec_key_match(&validated.key_md5, stored_key_md5)
    }
}

#[inline]
fn sse_invalid_argument(message: &str) -> ApiError {
    ApiError {
        code: S3ErrorCode::InvalidArgument,
        message: message.to_string(),
        source: None,
    }
}

/// SSE-C parameters extracted from headers (algorithm, key, key MD5).
pub(crate) type SsecParamsFromHeaders = (Option<SSECustomerAlgorithm>, Option<SSECustomerKey>, Option<SSECustomerKeyMD5>);

/// Extract SSE-C parameters from request headers.
/// Used as fallback when the S3 layer does not populate them in the input struct.
///
/// Returns an error if an SSE-C header is present but cannot be parsed as valid UTF-8,
/// ensuring malformed headers do not bypass validation.
pub(crate) fn extract_ssec_params_from_headers(headers: &HeaderMap) -> Result<SsecParamsFromHeaders, ApiError> {
    let algorithm = match headers.get("x-amz-server-side-encryption-customer-algorithm") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(SSECustomerAlgorithm::from(s.to_string())))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-algorithm header must be valid UTF-8.")),
    }?;

    let key = match headers.get("x-amz-server-side-encryption-customer-key") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(SSECustomerKey::from(s.to_string())))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-key header must be valid UTF-8.")),
    }?;

    let key_md5 = match headers.get("x-amz-server-side-encryption-customer-key-md5") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(SSECustomerKeyMD5::from(s.to_string())))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-key-md5 header must be valid UTF-8.")),
    }?;

    Ok((algorithm, key, key_md5))
}

/// Extract x-amz-server-side-encryption from request headers.
/// Used as fallback when the S3 layer does not populate it in the input struct.
///
/// Returns an error if the header is present but cannot be parsed as valid UTF-8,
/// ensuring malformed headers do not bypass validation.
pub(crate) fn extract_server_side_encryption_from_headers(headers: &HeaderMap) -> Result<Option<ServerSideEncryption>, ApiError> {
    match headers.get("x-amz-server-side-encryption") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(ServerSideEncryption::from(s.to_string())))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption header must be valid UTF-8.")),
    }
}

pub(crate) fn extract_ssekms_context_from_headers(headers: &HeaderMap) -> Result<Option<HashMap<String, String>>, ApiError> {
    let Some(v) = headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT) else {
        return Ok(None);
    };

    let value = v
        .to_str()
        .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-context header must be valid UTF-8."))?;
    let decoded = BASE64_STANDARD.decode(value).map_err(|_| {
        sse_invalid_argument("The x-amz-server-side-encryption-context header must be valid base64-encoded JSON.")
    })?;

    serde_json::from_slice(&decoded).map(Some).map_err(|_| {
        sse_invalid_argument("The x-amz-server-side-encryption-context header must be a base64-encoded JSON object.")
    })
}

#[inline]
pub(crate) fn validate_sse_headers_for_write(
    server_side_encryption: Option<&ServerSideEncryption>,
    ssekms_key_id: Option<&SSEKMSKeyId>,
    ssekms_context: Option<&HashMap<String, String>>,
    sse_customer_algorithm: Option<&SSECustomerAlgorithm>,
    sse_customer_key: Option<&SSECustomerKey>,
    sse_customer_key_md5: Option<&SSECustomerKeyMD5>,
    require_sse_customer_key: bool,
) -> Result<(), ApiError> {
    if let Some(sse) = server_side_encryption {
        let s = sse.as_str();
        if s != ServerSideEncryption::AES256 && s != ServerSideEncryption::AWS_KMS {
            return Err(sse_invalid_argument(
                "The SSE algorithm specified is not supported. The valid values are AES256 or aws:kms.",
            ));
        }
    }

    let has_ssec_headers = sse_customer_algorithm.is_some() || sse_customer_key.is_some() || sse_customer_key_md5.is_some();
    let has_managed_headers = server_side_encryption.is_some() || ssekms_key_id.is_some() || ssekms_context.is_some();

    if has_ssec_headers {
        if has_managed_headers {
            return Err(sse_invalid_argument(
                "The SSE-C and managed server-side encryption headers cannot be used together.",
            ));
        }

        let has_valid_ssec_headers = if require_sse_customer_key {
            matches!(
                (sse_customer_algorithm, sse_customer_key, sse_customer_key_md5),
                (Some(_), Some(_), Some(_))
            )
        } else {
            matches!((sse_customer_algorithm, sse_customer_key_md5), (Some(_), Some(_)))
        };

        if !has_valid_ssec_headers {
            let message = if require_sse_customer_key {
                "Missing SSE-C parameters. Algorithm, customer key and customer key MD5 are all required."
            } else {
                "Missing SSE-C parameters. Algorithm and customer key MD5 are required."
            };

            return Err(ssec_invalid_request(message));
        }
    }

    if ssekms_key_id.is_some() && server_side_encryption.is_none_or(|sse| sse.as_str() != ServerSideEncryption::AWS_KMS) {
        return Err(sse_invalid_argument(
            "The SSE-KMS key ID header can only be used when x-amz-server-side-encryption is set to aws:kms.",
        ));
    }

    if ssekms_context.is_some() && server_side_encryption.is_none_or(|sse| sse.as_str() != ServerSideEncryption::AWS_KMS) {
        return Err(sse_invalid_argument(
            "The SSE-KMS context header can only be used when x-amz-server-side-encryption is set to aws:kms.",
        ));
    }

    Ok(())
}

#[inline]
pub(crate) fn validate_sse_headers_for_read(metadata: &HashMap<String, String>, headers: &HeaderMap) -> Result<(), ApiError> {
    let has_req_ssec = headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-md5");

    let has_req_sse = headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id")
        || headers.contains_key("x-amz-server-side-encryption-context");

    let is_object_ssec = metadata.contains_key("x-amz-server-side-encryption-customer-algorithm");
    let is_object_sse = metadata.contains_key("x-amz-server-side-encryption");

    if is_object_ssec {
        if has_req_sse {
            return Err(sse_invalid_argument(
                "Server-side encryption headers cannot be used with an object encrypted using SSE-C.",
            ));
        }
        return Ok(());
    }

    if is_object_sse && has_req_ssec {
        return Err(sse_invalid_argument(
            "SSE-C headers cannot be used with an object encrypted using server-side managed encryption.",
        ));
    }

    if has_req_ssec {
        return Err(ssec_invalid_request(
            "The object was stored without SSE-C. The correct SSE-C parameters must not be provided.",
        ));
    }

    if has_req_sse {
        return Err(sse_invalid_argument(
            "The object is not encrypted with server-side encryption. Do not provide server-side encryption headers.",
        ));
    }

    Ok(())
}

pub(crate) fn map_get_object_reader_error(err: StorageError) -> ApiError {
    if let StorageError::Io(io_error) = &err
        && let Some(resolution_error) = io_error
            .get_ref()
            .and_then(|source| source.downcast_ref::<EncryptionResolutionError>())
    {
        let code = match resolution_error.kind() {
            EncryptionResolutionErrorKind::InvalidRequest => S3ErrorCode::InvalidRequest,
            EncryptionResolutionErrorKind::ServiceUnavailable => S3ErrorCode::ServiceUnavailable,
            _ => S3ErrorCode::InternalError,
        };
        return ApiError {
            code,
            message: resolution_error.to_string(),
            source: Some(Box::new(err)),
        };
    }

    if let Some(message) = map_ssec_get_object_reader_error_message(&err) {
        return ApiError {
            code: S3ErrorCode::InvalidRequest,
            message,
            source: Some(Box::new(err)),
        };
    }

    ApiError::from(err)
}

fn map_ssec_get_object_reader_error_message(err: &StorageError) -> Option<String> {
    let StorageError::Io(io_err) = err else {
        return None;
    };

    let detail = io_err.to_string();
    match detail.as_str() {
        "missing SSE-C algorithm header"
        | "invalid SSE-C algorithm header"
        | "missing SSE-C key header"
        | "invalid SSE-C key header"
        | "missing SSE-C key md5 header"
        | "invalid SSE-C key md5 header" => Some(
            "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
                .to_string(),
        ),
        "failed to decode SSE-C key" => Some("Invalid SSE-C key: not valid Base64.".to_string()),
        "SSE-C key must be 32 bytes" => Some("SSE-C key must be exactly 32 bytes.".to_string()),
        "SSE-C key MD5 mismatch" => {
            Some("The calculated MD5 hash of the key did not match the hash that was provided.".to_string())
        }
        "missing stored SSE-C key md5" => Some("Object has no stored SSE-C key metadata.".to_string()),
        "SSE-C key does not match object metadata" => Some(
            "The provided encryption parameters did not match the ones used originally to encrypt the object.".to_string(),
        ),
        _ => detail
            .strip_prefix("unsupported SSE-C algorithm ")
            .map(|_| format!("Unsupported SSE-C algorithm. Only {DEFAULT_SSE_ALGORITHM} is supported.")),
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
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    pub principal: Option<&'a SseKmsPrincipal>,
}

/// Encryption material returned by `sse_encryption()` / `sse_prepare_encryption()`.
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
    /// Base nonce/IV used by rio to derive block/part nonces.
    pub base_nonce: [u8; 12],
    /// Encrypted DEK for managed SSE. Absent for SSE-C.
    pub encrypted_data_key: Option<Vec<u8>>,
    /// SSE-C key MD5 if customer-managed encryption is in use.
    pub customer_key_md5: Option<SSECustomerKeyMD5>,
    /// Original plaintext size when it should be persisted alongside metadata.
    pub original_size: Option<i64>,
    /// Indicates whether `key_bytes` is a direct stream key or a MinIO-style object key.
    pub key_kind: EncryptionKeyKind,
    /// Original client-provided SSE-KMS context, stored in MinIO-compatible metadata.
    pub managed_kms_context: Option<HashMap<String, String>>,
    /// MinIO-compatible sealed object-key metadata for managed SSE object-key mode.
    #[cfg_attr(not(feature = "rio-v2"), allow(dead_code))]
    pub managed_sealed_key: Option<ManagedSealedKey>,
}

/// Decryption material returned by `sse_decryption()`.
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
    /// Base nonce/IV used by rio to derive block/part nonces.
    pub base_nonce: [u8; 12],
    /// Indicates whether `key_bytes` is a direct stream key or a MinIO-style object key.
    pub key_kind: EncryptionKeyKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionKeyKind {
    Direct,
    Object,
}

// ============================================================================
// Per-key KMS authorization (SSE-KMS data path)
// ============================================================================

/// Identity a data-path KMS key operation is attributed to.
///
/// Only requests that crossed the S3 authorization boundary carry one. Internal
/// callers — replication, lifecycle transition, heal, scanner — enter the object
/// layer directly and run with server credentials, so they never build a principal
/// and are exempt by construction; `None` therefore means "system principal".
#[derive(Debug, Clone)]
pub struct SseKmsPrincipal {
    account: String,
    groups: Option<Vec<String>>,
    is_owner: bool,
    claims: HashMap<String, Value>,
    conditions: HashMap<String, Vec<String>>,
    /// Audit slot of the S3 request this principal was built for, when the request
    /// is being audited. The principal is the only request-derived value that reaches
    /// the managed-SSE code paths, so it doubles as the carrier for the slot.
    request_audit: Option<Arc<KmsRequestAudit>>,
    /// Test-only decision overrides. Carried per principal rather than in a global slot so
    /// concurrent tests cannot observe each other's injection.
    #[cfg(test)]
    test_hooks: Option<TestAuthorizationHooks>,
}

impl SseKmsPrincipal {
    /// Build a principal from an S3 request.
    ///
    /// Anonymous requests use an empty account, matching the bucket-policy authorization
    /// path. This keeps every request that crossed the S3 boundary distinct from trusted
    /// internal callers, which are represented by `None`.
    pub(crate) fn from_request<T>(req: &S3Request<T>) -> Option<Self> {
        let req_info = req.extensions.get::<ReqInfo>()?;
        let cred = req_info.cred.as_ref();

        Some(Self {
            account: cred.map(|cred| cred.access_key.clone()).unwrap_or_default(),
            groups: cred.and_then(|cred| cred.groups.clone()),
            is_owner: req_info.is_owner,
            claims: cred.map(|cred| cred.claims_or_empty().clone()).unwrap_or_default(),
            conditions: cred.map(|cred| resource_free_condition_values(req, cred)).unwrap_or_default(),
            request_audit: request_context_from_req(req).and_then(|context| kms_request_audit(&context.request_id)),
            #[cfg(test)]
            test_hooks: None,
        })
    }

    #[cfg(test)]
    fn for_test(account: &str, enforced: bool, authorizer: Arc<dyn KmsKeyAuthorizer>) -> Self {
        Self {
            account: account.to_string(),
            groups: None,
            is_owner: false,
            claims: HashMap::new(),
            conditions: HashMap::new(),
            request_audit: None,
            test_hooks: Some(TestAuthorizationHooks { enforced, authorizer }),
        }
    }

    /// Bind this principal to `audit` so managed-SSE operations performed for it are
    /// summarised onto the request's S3 audit entry.
    #[cfg(test)]
    fn with_request_audit(mut self, audit: Arc<KmsRequestAudit>) -> Self {
        self.request_audit = Some(audit);
        self
    }
}

#[cfg(test)]
#[derive(Clone)]
struct TestAuthorizationHooks {
    enforced: bool,
    authorizer: Arc<dyn KmsKeyAuthorizer>,
}

#[cfg(test)]
impl std::fmt::Debug for TestAuthorizationHooks {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TestAuthorizationHooks")
            .field("enforced", &self.enforced)
            .finish_non_exhaustive()
    }
}

/// Decides whether a principal may use a specific KMS key on the data path.
///
/// Behind a trait so tests can pin the allow/deny contract of the SSE call sites
/// without a running IAM subsystem.
#[async_trait]
pub(crate) trait KmsKeyAuthorizer: Send + Sync {
    async fn is_allowed(&self, principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, ApiError>;
}

struct IamKmsKeyAuthorizer;

#[async_trait]
impl KmsKeyAuthorizer for IamKmsKeyAuthorizer {
    async fn is_allowed(&self, principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, ApiError> {
        let iam_store = runtime_sources::current_ready_iam_handle()
            .map_err(|err| ApiError::from(StorageError::other(format!("KMS key authorization requires IAM: {err:?}"))))?;

        Ok(iam_store
            .is_allowed(&Args {
                account: &principal.account,
                groups: &principal.groups,
                action: Action::KmsAction(action),
                // Call-site contract of `Statement::kms_key_scope_matches`: the requested key
                // identifier travels in `object` with `bucket` left empty.
                bucket: "",
                object: key_id,
                conditions: &principal.conditions,
                is_owner: principal.is_owner,
                claims: &principal.claims,
                deny_only: false,
            })
            .await)
    }
}

fn kms_key_authorizer(principal: &SseKmsPrincipal) -> Arc<dyn KmsKeyAuthorizer> {
    #[cfg(test)]
    if let Some(hooks) = principal.test_hooks.as_ref() {
        return hooks.authorizer.clone();
    }
    #[cfg(not(test))]
    let _ = principal;

    static DEFAULT: LazyLock<Arc<dyn KmsKeyAuthorizer>> = LazyLock::new(|| Arc::new(IamKmsKeyAuthorizer));
    DEFAULT.clone()
}

/// Whether per-key KMS authorization is enforced on the SSE-KMS data path.
fn sse_kms_key_policy_enforced(principal: Option<&SseKmsPrincipal>) -> bool {
    #[cfg(test)]
    if let Some(hooks) = principal.and_then(|candidate| candidate.test_hooks.as_ref()) {
        return hooks.enforced;
    }
    #[cfg(not(test))]
    let _ = principal;

    static ENFORCED: LazyLock<bool> =
        LazyLock::new(|| get_env_bool(ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY, DEFAULT_KMS_ENFORCE_SSE_KEY_POLICY));
    *ENFORCED
}

/// Report the configured SSE-KMS authorization mode once, at startup.
///
/// The disabled case warns rather than logs: it is the compatibility default for this
/// release only, and operators need the lead time to grant the kms actions before the
/// default flips.
pub(crate) fn log_sse_kms_key_policy_mode() {
    if sse_kms_key_policy_enforced(None) {
        tracing::info!(
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_SSE,
            event = "sse_kms_key_policy_mode",
            enforced = true,
            "SSE-KMS requests are authorized against the resolved KMS key (kms:GenerateDataKey / kms:Decrypt)"
        );
        return;
    }

    tracing::warn!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "sse_kms_key_policy_mode",
        enforced = false,
        "SSE-KMS requests are not authorized against the KMS key they name; any identity allowed to \
         write an object may encrypt it under any key, and any identity allowed to read it may have it \
         decrypted. Grant kms:GenerateDataKey and kms:Decrypt on the keys your workloads use, then set \
         {ENV_RUSTFS_KMS_ENFORCE_SSE_KEY_POLICY}=true. A later release defaults this to enabled."
    );
}

/// The principal whose KMS key permissions this operation must satisfy, if any.
///
/// Scoping is limited to SSE-KMS, matching AWS: SSE-S3 wraps its data key with a
/// server-owned key the caller never names, and SSE-C never reaches KMS at all.
fn kms_authorization_subject(enforced: bool, principal: Option<&SseKmsPrincipal>, sse_type: SSEType) -> Option<&SseKmsPrincipal> {
    if !enforced || !matches!(sse_type, SSEType::SseKms) {
        return None;
    }

    principal
}

/// Authorize `action` against `key_id` for the caller behind this SSE operation.
///
/// Runs before the key is used, so a denied request cannot distinguish an unauthorized
/// key from a disabled or pending-deletion one.
async fn authorize_sse_kms_key(
    principal: Option<&SseKmsPrincipal>,
    sse_type: SSEType,
    action: KmsAction,
    key_id: &str,
) -> Result<(), ApiError> {
    let Some(principal) = kms_authorization_subject(sse_kms_key_policy_enforced(principal), principal, sse_type) else {
        return Ok(());
    };

    if kms_key_authorizer(principal).is_allowed(principal, action, key_id).await? {
        return Ok(());
    }

    debug!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "sse_kms_key_authorization_denied",
        account = %principal.account,
        action = ?action,
        "Principal is not authorized for the KMS key resolved for this request"
    );

    Err(ApiError {
        code: S3ErrorCode::AccessDenied,
        message: "Access Denied".to_string(),
        source: None,
    })
}

/// Authorize `kms:Decrypt` for an object whose stored encryption material is unwrapped
/// outside [`sse_decryption`].
///
/// The copy-source read resolves its material inside the object layer, which has no
/// request identity, so the S3 layer has to run the check itself.
pub async fn authorize_sse_kms_object_read(
    principal: Option<&SseKmsPrincipal>,
    metadata: &HashMap<String, String>,
) -> Result<(), ApiError> {
    let Some((sse_type, key_id)) = stored_managed_encryption_key(metadata) else {
        return Ok(());
    };

    let result = authorize_sse_kms_key(principal, sse_type, KmsAction::DecryptAction, &key_id).await;
    // Only a denial is recorded here: an allowed read goes on to unwrap the key,
    // and that operation reports its own outcome.
    if let Err(error) = &result {
        record_managed_kms_outcome(
            principal,
            sse_type,
            Some(&key_id),
            || stored_envelope_master_key_version(metadata),
            Err(error),
        );
    }

    result
}

// ============================================================================
// Data-plane KMS audit attachment (SSE-S3 / SSE-KMS)
// ============================================================================

/// Tag keys carrying the KMS summary on an S3 audit entry.
///
/// Audit consumers key off these strings, so they are a wire contract: append
/// new keys rather than renaming existing ones.
const KMS_AUDIT_TAG_SSE_TYPE: &str = "sseType";
const KMS_AUDIT_TAG_KEY_ID: &str = "kmsKeyId";
const KMS_AUDIT_TAG_KEY_VERSION: &str = "kmsKeyVersion";
const KMS_AUDIT_TAG_OUTCOME: &str = "kmsOutcome";
const KMS_AUDIT_TAG_ERROR_CLASS: &str = "kmsErrorClass";

/// Separator for the rare request that touched more than one key or scheme.
const KMS_AUDIT_VALUE_SEPARATOR: &str = ",";

/// What the data path did with KMS while serving one S3 request.
///
/// This rides on the request's existing S3 audit entry instead of becoming its
/// own event: the entry already carries the principal, request ID and API, so
/// attaching the KMS outcome costs no extra event volume and needs no second
/// correlation key.
///
/// # Redaction
///
/// Only the fields below may ever be recorded. The S3 audit entry fans out to
/// every configured target, several of which sit outside the KMS trust
/// boundary, so nothing derived from a data key — plaintext, ciphertext,
/// envelope, nonce — nor any caller-supplied encryption-context value belongs
/// here. Key identifiers and scheme names are configuration, not secrets.
#[derive(Debug, Default)]
struct KmsRequestAuditState {
    sse_types: BTreeSet<&'static str>,
    key_ids: BTreeSet<String>,
    key_versions: BTreeSet<u32>,
    /// Set by the first failure. A request that failed any KMS interaction is
    /// reported as failed even if others succeeded: a partially completed
    /// envelope operation is not a success for an audit reader.
    error_class: Option<&'static str>,
    recorded: bool,
}

/// Shared accumulator for one request's KMS summary.
///
/// Written by the managed-SSE code paths through the request's principal, read
/// once by the [`OperationHelper`](crate::storage::helper::OperationHelper) that
/// owns the request's audit entry.
#[derive(Debug, Default)]
pub(crate) struct KmsRequestAudit(Mutex<KmsRequestAuditState>);

impl KmsRequestAudit {
    fn record(&self, sse_type: SSEType, key_id: Option<&str>, key_version: Option<u32>, error_class: Option<&'static str>) {
        // A poisoned lock costs the audit entry its KMS tags; it must never cost
        // the request its result.
        let Ok(mut state) = self.0.lock() else {
            return;
        };

        state.recorded = true;
        state.sse_types.insert(sse_type.audit_label());
        if let Some(key_id) = key_id.filter(|value| !value.is_empty()) {
            state.key_ids.insert(key_id.to_string());
        }
        if let Some(key_version) = key_version {
            state.key_versions.insert(key_version);
        }
        if state.error_class.is_none() {
            state.error_class = error_class;
        }
    }

    /// Render the summary as audit tags, or nothing when the request performed
    /// no KMS work at all (SSE-C and unencrypted objects never reach KMS).
    pub(crate) fn audit_tags(&self) -> Vec<(&'static str, Value)> {
        let Ok(state) = self.0.lock() else {
            return Vec::new();
        };
        if !state.recorded {
            return Vec::new();
        }

        let mut tags = Vec::with_capacity(5);
        tags.push((KMS_AUDIT_TAG_SSE_TYPE, join_audit_values(state.sse_types.iter().copied())));
        if !state.key_ids.is_empty() {
            tags.push((KMS_AUDIT_TAG_KEY_ID, join_audit_values(state.key_ids.iter().map(String::as_str))));
        }
        if !state.key_versions.is_empty() {
            tags.push((
                KMS_AUDIT_TAG_KEY_VERSION,
                join_audit_values(state.key_versions.iter().map(u32::to_string)),
            ));
        }
        let outcome = if state.error_class.is_some() { "failure" } else { "success" };
        tags.push((KMS_AUDIT_TAG_OUTCOME, Value::String(outcome.to_string())));
        if let Some(error_class) = state.error_class {
            tags.push((KMS_AUDIT_TAG_ERROR_CLASS, Value::String(error_class.to_string())));
        }

        tags
    }
}

/// Render a set of values as one tag value.
///
/// Always a string, including for the single-value case: a consumer that would
/// have to branch on the JSON type of a tag is a consumer that will get it
/// wrong for the request that happens to touch two keys.
fn join_audit_values(values: impl Iterator<Item = impl AsRef<str>>) -> Value {
    let joined = values.map(|value| value.as_ref().to_string()).collect::<Vec<_>>();
    Value::String(joined.join(KMS_AUDIT_VALUE_SEPARATOR))
}

/// Slots of the requests currently being audited, keyed by canonical request ID.
///
/// Only a weak reference is held: the owning [`KmsRequestAuditScope`] decides the
/// lifetime, so a scope that is somehow not dropped cannot keep a slot alive.
static KMS_REQUEST_AUDITS: LazyLock<Mutex<HashMap<String, Weak<KmsRequestAudit>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Registration of one request's KMS audit slot, dropped with the request's
/// audit entry.
pub(crate) struct KmsRequestAuditScope {
    request_id: String,
    audit: Arc<KmsRequestAudit>,
}

impl KmsRequestAuditScope {
    /// Open a slot for `request_id`, so managed-SSE operations served under it can
    /// be summarised onto its audit entry.
    pub(crate) fn register(request_id: &str) -> Self {
        let audit = Arc::new(KmsRequestAudit::default());
        if let Ok(mut registry) = KMS_REQUEST_AUDITS.lock() {
            registry.insert(request_id.to_string(), Arc::downgrade(&audit));
        }

        Self {
            request_id: request_id.to_string(),
            audit,
        }
    }

    /// Tags describing the KMS work recorded under this request.
    pub(crate) fn audit_tags(&self) -> Vec<(&'static str, Value)> {
        self.audit.audit_tags()
    }
}

impl Drop for KmsRequestAuditScope {
    fn drop(&mut self) {
        if let Ok(mut registry) = KMS_REQUEST_AUDITS.lock() {
            registry.remove(&self.request_id);
        }
    }
}

/// Resolve the audit slot opened for `request_id`, if the request is being audited.
fn kms_request_audit(request_id: &str) -> Option<Arc<KmsRequestAudit>> {
    KMS_REQUEST_AUDITS.lock().ok()?.get(request_id)?.upgrade()
}

/// Failure classes for data-path failures that never reached a KMS backend.
///
/// Backend failures carry the classification defined by the KMS audit contract;
/// everything else is classified at the S3 boundary it surfaced through.
fn kms_data_plane_error_class(error: &ApiError) -> &'static str {
    if let Some(failure) = error
        .source
        .as_ref()
        .and_then(|source| source.downcast_ref::<KmsDataPlaneFailure>())
    {
        return failure.class;
    }

    match error.code {
        S3ErrorCode::AccessDenied => "access_denied",
        S3ErrorCode::InvalidArgument | S3ErrorCode::InvalidRequest => "invalid_argument",
        _ => "sse_internal",
    }
}

/// Carries a KMS failure class across the conversion to an S3-facing error.
///
/// The class is decided while the `KmsError` is still typed; re-deriving it from
/// a rendered message downstream would be guesswork.
#[derive(Debug)]
struct KmsDataPlaneFailure {
    class: &'static str,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl std::fmt::Display for KmsDataPlaneFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.source {
            Some(source) => source.fmt(formatter),
            None => formatter.write_str(self.class),
        }
    }
}

impl std::error::Error for KmsDataPlaneFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// Record the outcome of one managed-SSE operation on the request's audit entry.
///
/// A `None` principal marks an internal caller — replication, lifecycle, heal —
/// which has no S3 audit entry to attach to. `key_version` is a closure for
/// exactly that caller: extracting the version means base64-decoding and
/// parsing the stored envelope, work that must not run on the internal hot
/// paths that discard it.
fn record_managed_kms_outcome(
    principal: Option<&SseKmsPrincipal>,
    sse_type: SSEType,
    key_id: Option<&str>,
    key_version: impl FnOnce() -> Option<u32>,
    result: Result<(), &ApiError>,
) {
    let Some(audit) = principal.and_then(|principal| principal.request_audit.as_ref()) else {
        return;
    };

    audit.record(sse_type, key_id, key_version(), result.err().map(kms_data_plane_error_class));
}

/// Master-key version recorded in a managed-SSE data-key envelope, if the
/// wrapping backend recorded one.
///
/// `None` is the honest answer for every other shape: Transit and AWS wrap
/// into opaque ciphertext that is not an envelope, Local records no version,
/// and pre-versioning envelopes never carried the field. The single field is
/// read through `serde_json::Value` rather than a full `DataKeyEnvelope`
/// parse, so the audit path cannot double-count the envelope's unknown-field
/// observability and touches nothing else in the envelope.
fn envelope_master_key_version(envelope_bytes: &[u8]) -> Option<u32> {
    if !is_data_key_envelope(envelope_bytes) {
        return None;
    }
    u32::try_from(
        serde_json::from_slice::<Value>(envelope_bytes)
            .ok()?
            .get("master_key_version")?
            .as_u64()?,
    )
    .ok()
}

/// Master-key version of the envelope stored on an object, for the audit
/// summary of a read against that object.
fn stored_envelope_master_key_version(metadata: &HashMap<String, String>) -> Option<u32> {
    // No context recoder: the recode only ever inserts the context key, which
    // this lookup never reads, so the normalized result is identical without it.
    let encoded = normalize_managed_metadata(metadata, None);
    let encoded = encoded.get(INTERNAL_ENCRYPTION_KEY_HEADER)?;
    let envelope = BASE64_STANDARD.decode(encoded).ok()?;
    envelope_master_key_version(&envelope)
}

pub(crate) struct SseObjectEncryptionResolver;

#[async_trait]
impl ObjectEncryptionResolver for SseObjectEncryptionResolver {
    async fn resolve_read_material(
        &self,
        request: ReadEncryptionRequest<'_>,
    ) -> Result<Option<ReadEncryptionMaterial>, EncryptionResolutionError> {
        let metadata = normalize_encryption_metadata_case(request.metadata)?;
        let (customer_algorithm, customer_key, customer_key_md5) =
            extract_ssec_params_from_headers(request.headers).map_err(map_encryption_resolution_error)?;
        if let Some(stored_algorithm) = metadata.get("x-amz-server-side-encryption-customer-algorithm") {
            let request_algorithm = customer_algorithm.as_ref().ok_or_else(|| {
                map_encryption_resolution_error(ssec_invalid_request(
                    "The object was stored using a form of Server Side Encryption. \
                     The correct parameters must be provided to retrieve the object.",
                ))
            })?;
            if stored_algorithm != request_algorithm.as_str() {
                return Err(map_encryption_resolution_error(ssec_invalid_request(
                    "The provided encryption parameters did not match the ones used originally to encrypt the object.",
                )));
            }
        }
        let material = sse_decryption(DecryptionRequest {
            bucket: request.bucket,
            key: request.object,
            metadata: &metadata,
            sse_customer_key: customer_key.as_ref(),
            sse_customer_key_md5: customer_key_md5.as_ref(),
            // The object layer resolves read material for internal readers too (replication,
            // lifecycle transition, heal, scanner) and carries no request identity, so this hook
            // never authorizes. Callers that cross the S3 boundary are authorized by the S3 layer
            // before their bytes are served.
            principal: None,
        })
        .await
        .map_err(map_encryption_resolution_error)?;

        Ok(material.map(|material| ReadEncryptionMaterial {
            key_bytes: material.key_bytes,
            mode: match material.key_kind {
                EncryptionKeyKind::Direct => ReadEncryptionMode::Direct {
                    base_nonce: material.base_nonce,
                },
                EncryptionKeyKind::Object => ReadEncryptionMode::Object,
            },
        }))
    }
}

fn normalize_encryption_metadata_case(
    metadata: &HashMap<String, String>,
) -> Result<Cow<'_, HashMap<String, String>>, EncryptionResolutionError> {
    const CANONICAL_KEYS: &[&str] = &[
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key-md5",
        SSEC_ORIGINAL_SIZE_HEADER,
        INTERNAL_ENCRYPTION_KEY_ID_HEADER,
        INTERNAL_ENCRYPTION_KEY_HEADER,
        INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
        INTERNAL_ENCRYPTION_IV_HEADER,
        INTERNAL_ENCRYPTION_CONTEXT_HEADER,
        INTERNAL_ENCRYPTION_TAG_HEADER,
        INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER,
        MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER,
        MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
        MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER,
    ];

    let needs_normalization = metadata.keys().any(|key| {
        CANONICAL_KEYS
            .iter()
            .any(|canonical| key != canonical && key.eq_ignore_ascii_case(canonical))
    });
    if !needs_normalization {
        return Ok(Cow::Borrowed(metadata));
    }

    let mut normalized = metadata.clone();
    for canonical in CANONICAL_KEYS {
        let mut matching_values = metadata
            .iter()
            .filter_map(|(key, value)| key.eq_ignore_ascii_case(canonical).then_some(value));
        let Some(value) = matching_values.next() else {
            continue;
        };
        if matching_values.any(|candidate| candidate != value) {
            return Err(EncryptionResolutionError::new(
                EncryptionResolutionErrorKind::InvalidMetadata,
                format!("conflicting object encryption metadata for {canonical}"),
            ));
        }
        if !normalized.contains_key(*canonical) {
            normalized.insert((*canonical).to_string(), value.clone());
        }
    }
    Ok(Cow::Owned(normalized))
}

fn map_encryption_resolution_error(error: ApiError) -> EncryptionResolutionError {
    let kind = match error.code {
        S3ErrorCode::InvalidArgument | S3ErrorCode::InvalidRequest => EncryptionResolutionErrorKind::InvalidRequest,
        S3ErrorCode::ServiceUnavailable => EncryptionResolutionErrorKind::ServiceUnavailable,
        _ => EncryptionResolutionErrorKind::DecryptionFailed,
    };
    EncryptionResolutionError::new(kind, error.message)
}

#[derive(Debug, Clone)]
pub struct ManagedSealedKey {
    #[cfg(feature = "rio-v2")]
    pub iv: [u8; SEALED_KEY_IV_SIZE],
    #[cfg(feature = "rio-v2")]
    pub sealed_key: [u8; SEALED_KEY_SIZE],
}

impl EncryptionMaterial {
    pub fn write_encryption(&self, multipart_part_number: Option<usize>) -> super::WriteEncryption {
        match (self.key_kind, multipart_part_number) {
            (EncryptionKeyKind::Object, Some(part_number)) => {
                super::WriteEncryption::multipart_object_key(self.key_bytes, part_number as u32)
            }
            (EncryptionKeyKind::Object, None) => super::WriteEncryption::singlepart_object_key(self.key_bytes),
            (EncryptionKeyKind::Direct, Some(part_number)) => {
                super::WriteEncryption::multipart(self.key_bytes, self.base_nonce, part_number)
            }
            (EncryptionKeyKind::Direct, None) => super::WriteEncryption::singlepart(self.key_bytes, self.base_nonce),
        }
    }
}

#[cfg(feature = "rio-v2")]
type HmacSha256 = Hmac<Sha256>;

#[cfg(feature = "rio-v2")]
fn managed_sse_domain(sse_type: SSEType) -> &'static str {
    match sse_type {
        SSEType::SseS3 => "SSE-S3",
        SSEType::SseKms => "SSE-KMS",
        SSEType::SseC => "SSE-C",
    }
}

/// The public `x-amz-server-side-encryption` value a managed scheme reports.
fn managed_sse_public_header(sse_type: SSEType) -> &'static str {
    match sse_type {
        SSEType::SseKms => ServerSideEncryption::AWS_KMS,
        // SSE-C never reaches the managed path; reporting AES256 keeps this
        // total without inventing a third public value.
        SSEType::SseS3 | SSEType::SseC => ServerSideEncryption::AES256,
    }
}

fn canonical_kms_bucket_path(bucket: &str, key: &str) -> String {
    path_join_buf(&[bucket, key])
}

fn build_kms_request_context(
    bucket: &str,
    key: &str,
    provided_context: Option<&HashMap<String, String>>,
) -> HashMap<String, String> {
    let mut context = provided_context.cloned().unwrap_or_default();
    context
        .entry(bucket.to_string())
        .or_insert_with(|| canonical_kms_bucket_path(bucket, key));
    context
}

fn build_object_encryption_context(
    bucket: &str,
    key: &str,
    provided_context: Option<&HashMap<String, String>>,
) -> ObjectEncryptionContext {
    let mut context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());
    for (ctx_key, ctx_value) in build_kms_request_context(bucket, key, provided_context) {
        context = context.with_encryption_context(ctx_key, ctx_value);
    }
    context
}

fn encode_minio_kms_context(context: &HashMap<String, String>) -> Result<String, ApiError> {
    let encoded = serde_json::to_vec(context)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to serialize KMS context: {e}"))))?;
    Ok(BASE64_STANDARD.encode(encoded))
}

fn decode_minio_kms_context(metadata: &HashMap<String, String>) -> Result<Option<HashMap<String, String>>, ApiError> {
    let Some(encoded) = metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER) else {
        return Ok(None);
    };
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode MinIO KMS context: {e}"))))?;
    serde_json::from_slice(&decoded)
        .map(Some)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to parse MinIO KMS context: {e}"))))
}

#[cfg(feature = "rio-v2")]
fn is_supported_sealed_object_key_cipher(cipher: u8) -> bool {
    matches!(cipher, DARE_CIPHER_AES_256_GCM | DARE_CIPHER_CHACHA20_POLY1305)
}

#[cfg(feature = "rio-v2")]
fn decrypt_sealed_object_key_payload(sealing_key: [u8; 32], header: &[u8], sealed_key: &[u8]) -> Result<Vec<u8>, ApiError> {
    let nonce = &header[4..16];
    let ciphertext = &sealed_key[DARE_HEADER_SIZE..];
    let aad = &header[..4];
    match header[1] {
        DARE_CIPHER_AES_256_GCM => {
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|err| ApiError::from(StorageError::other(format!("Invalid AES-GCM sealing key: {err}"))))?;
            let nonce = Nonce::try_from(nonce)
                .map_err(|_| ApiError::from(StorageError::other("Invalid sealed object-key package nonce")))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        DARE_CIPHER_CHACHA20_POLY1305 => {
            let cipher = ChaCha20Poly1305::new_from_slice(&sealing_key)
                .map_err(|err| ApiError::from(StorageError::other(format!("Invalid ChaCha20-Poly1305 sealing key: {err}"))))?;
            let nonce = chacha20poly1305::Nonce::try_from(nonce)
                .map_err(|_| ApiError::from(StorageError::other("Invalid sealed object-key package nonce")))?;
            cipher.decrypt(&nonce, Payload { msg: ciphertext, aad })
        }
        _ => return Err(ApiError::from(StorageError::other("Unsupported sealed object-key DARE header"))),
    }
    .map_err(|err| ApiError::from(StorageError::other(format!("Failed to unseal object key: {err}"))))
}

#[cfg(feature = "rio-v2")]
fn canonical_sse_path(bucket: &str, object: &str) -> String {
    let bucket = bucket.trim_matches('/');
    let object = object.trim_matches('/');
    if object.is_empty() {
        bucket.to_string()
    } else if bucket.is_empty() {
        object.to_string()
    } else {
        format!("{bucket}/{object}")
    }
}

#[cfg(feature = "rio-v2")]
fn derive_object_key(external_key: [u8; 32]) -> Result<[u8; 32], ApiError> {
    let mut random = [0u8; 32];
    rand::rng().fill(&mut random);

    let mut mac = HmacSha256::new_from_slice(&external_key)
        .map_err(|err| ApiError::from(StorageError::other(format!("Invalid HMAC key for object-key derivation: {err}"))))?;
    mac.update(OBJECT_KEY_DERIVATION_CONTEXT);
    mac.update(&random);

    let mut object_key = [0u8; 32];
    object_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    Ok(object_key)
}

#[cfg(feature = "rio-v2")]
fn derive_sealing_key(
    external_key: [u8; 32],
    iv: [u8; SEALED_KEY_IV_SIZE],
    domain: &str,
    bucket: &str,
    object: &str,
) -> Result<[u8; 32], ApiError> {
    let mut mac = HmacSha256::new_from_slice(&external_key)
        .map_err(|err| ApiError::from(StorageError::other(format!("Invalid HMAC key for sealing-key derivation: {err}"))))?;
    mac.update(&iv);
    mac.update(domain.as_bytes());
    mac.update(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.as_bytes());
    mac.update(canonical_sse_path(bucket, object).as_bytes());

    let mut sealing_key = [0u8; 32];
    sealing_key.copy_from_slice(mac.finalize().into_bytes().as_slice());
    Ok(sealing_key)
}

#[cfg(feature = "rio-v2")]
fn seal_object_key(
    object_key: [u8; 32],
    external_key: [u8; 32],
    sse_type: SSEType,
    bucket: &str,
    object: &str,
) -> Result<ManagedSealedKey, ApiError> {
    let mut iv = [0u8; SEALED_KEY_IV_SIZE];
    rand::rng().fill(&mut iv);
    let sealing_key = derive_sealing_key(external_key, iv, managed_sse_domain(sse_type), bucket, object)?;

    let mut header = [0u8; DARE_HEADER_SIZE];
    header[0] = DARE_VERSION_20;
    header[1] = DARE_CIPHER_AES_256_GCM;
    header[2..4].copy_from_slice(&(31u16).to_le_bytes());
    let mut stream_nonce = [0u8; 12];
    rand::rng().fill(&mut stream_nonce);
    stream_nonce[0] |= 0x80;
    header[4..16].copy_from_slice(&stream_nonce);

    let cipher = Aes256Gcm::new_from_slice(&sealing_key)
        .map_err(|err| ApiError::from(StorageError::other(format!("Invalid sealing key: {err}"))))?;
    let nonce = Nonce::try_from(stream_nonce.as_slice())
        .map_err(|_| ApiError::from(StorageError::other("Invalid sealed object-key stream nonce")))?;
    let ciphertext = cipher
        .encrypt(
            &nonce,
            aes_gcm::aead::Payload {
                msg: &object_key,
                aad: &header[..4],
            },
        )
        .map_err(|err| ApiError::from(StorageError::other(format!("Failed to seal object key: {err}"))))?;

    let mut sealed_key = [0u8; SEALED_KEY_SIZE];
    sealed_key[..DARE_HEADER_SIZE].copy_from_slice(&header);
    sealed_key[DARE_HEADER_SIZE..].copy_from_slice(&ciphertext);

    Ok(ManagedSealedKey { iv, sealed_key })
}

#[cfg(feature = "rio-v2")]
fn unseal_object_key(
    sealed: &ManagedSealedKey,
    external_key: [u8; 32],
    sse_type: SSEType,
    bucket: &str,
    object: &str,
) -> Result<[u8; 32], ApiError> {
    let header = &sealed.sealed_key[..DARE_HEADER_SIZE];
    if header[0] != DARE_VERSION_20 || !is_supported_sealed_object_key_cipher(header[1]) {
        return Err(ApiError::from(StorageError::other("Unsupported sealed object-key DARE header")));
    }
    if u16::from_le_bytes([header[2], header[3]]) != 31 || header[4] & 0x80 == 0 {
        return Err(ApiError::from(StorageError::other("Invalid sealed object-key payload header")));
    }

    let sealing_key = derive_sealing_key(external_key, sealed.iv, managed_sse_domain(sse_type), bucket, object)?;
    let plaintext = decrypt_sealed_object_key_payload(sealing_key, header, &sealed.sealed_key)?;

    let object_key: [u8; 32] = plaintext
        .as_slice()
        .try_into()
        .map_err(|_| ApiError::from(StorageError::other("Sealed object key must decrypt to 32 bytes")))?;
    Ok(object_key)
}

#[cfg(feature = "rio-v2")]
fn try_decode_minio_sealed_key(bytes: &str) -> Result<Option<[u8; SEALED_KEY_SIZE]>, ApiError> {
    let decoded = BASE64_STANDARD
        .decode(bytes)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode sealed object key: {e}"))))?;
    match decoded.as_slice().try_into() {
        Ok(sealed_key) => Ok(Some(sealed_key)),
        Err(_) => Ok(None),
    }
}

#[cfg(feature = "rio-v2")]
fn try_decode_minio_sealing_iv(bytes: &str) -> Result<Option<[u8; SEALED_KEY_IV_SIZE]>, ApiError> {
    let decoded = BASE64_STANDARD
        .decode(bytes)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode sealing IV: {e}"))))?;
    match decoded.as_slice().try_into() {
        Ok(iv) => Ok(Some(iv)),
        Err(_) => Ok(None),
    }
}

pub(crate) fn build_ssec_read_headers(
    algorithm: Option<&SSECustomerAlgorithm>,
    key: Option<&SSECustomerKey>,
    key_md5: Option<&SSECustomerKeyMD5>,
) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(algorithm) = algorithm
        && let Ok(mut value) = HeaderValue::from_str(algorithm.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, value);
    }

    if let Some(key) = key
        && let Ok(mut value) = HeaderValue::from_str(key.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, value);
    }

    if let Some(key_md5) = key_md5
        && let Ok(mut value) = HeaderValue::from_str(key_md5.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, value);
    }

    headers
}

pub fn encryption_material_to_metadata(material: &EncryptionMaterial) -> Result<HashMap<String, String>, ApiError> {
    let mut metadata = HashMap::new();

    match material.sse_type {
        SSEType::SseC => {
            metadata.insert(
                "x-amz-server-side-encryption".to_string(),
                material.server_side_encryption.as_str().to_string(),
            );
            metadata.insert(
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                material.algorithm.as_str().to_string(),
            );
            if let Some(customer_key_md5) = &material.customer_key_md5 {
                metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), customer_key_md5.to_string());
            }
            if let Some(original_size) = material.original_size {
                metadata.insert(SSEC_ORIGINAL_SIZE_HEADER.to_string(), original_size.to_string());
            }

            // Persist the random base nonce for the SSE-C Direct scheme (default,
            // non-`rio-v2` build) so decrypt can read it back instead of recomputing a
            // deterministic value. Written under both the RustFS and MinIO keys per repo
            // convention. This covers single-PUT persistence and multipart-session
            // persistence (CreateMultipartUpload builds session metadata here). Under
            // `rio-v2` the SSE-C path uses `EncryptionKeyKind::Object` and the sealed-key
            // block below, so this branch is not taken.
            if material.key_kind == EncryptionKeyKind::Direct {
                metadata.insert(INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(material.base_nonce));
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode(material.base_nonce),
                );
            }

            #[cfg(feature = "rio-v2")]
            if let Some(sealed) = &material.managed_sealed_key {
                metadata.insert(MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealed.iv));
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(sealed.sealed_key),
                );
            }
        }
        SSEType::SseS3 | SSEType::SseKms => {
            let encrypted_data_key = material
                .encrypted_data_key
                .as_deref()
                .ok_or_else(|| ApiError::from(StorageError::other("managed SSE materials must carry an encrypted data key")))?;
            metadata.insert(
                "x-amz-server-side-encryption".to_string(),
                material.server_side_encryption.as_str().to_string(),
            );

            let internal_key_id = material
                .kms_key_id
                .clone()
                .unwrap_or_else(|| SSEKMSKeyId::from("default".to_string()));
            metadata.insert(INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), internal_key_id.clone());

            if matches!(material.sse_type, SSEType::SseKms) {
                metadata.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), internal_key_id);
            }

            if let Some(original_size) = material.original_size {
                metadata.insert(INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER.to_string(), original_size.to_string());
            }

            if let Some(kms_context) = &material.managed_kms_context
                && !kms_context.is_empty()
            {
                if let Ok(serialized) = serde_json::to_string(kms_context) {
                    metadata.insert(INTERNAL_ENCRYPTION_CONTEXT_HEADER.to_string(), serialized);
                }
                if matches!(material.sse_type, SSEType::SseKms)
                    && let Ok(encoded) = encode_minio_kms_context(kms_context)
                {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), encoded);
                }
            }

            if material.key_kind == EncryptionKeyKind::Direct {
                metadata.insert(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(encrypted_data_key));
                metadata.insert(INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(material.base_nonce));
                metadata.insert(INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), material.algorithm.as_str().to_string());
            }

            #[cfg(feature = "rio-v2")]
            if let Some(sealed) = &material.managed_sealed_key {
                metadata.insert(MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(sealed.iv));
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                );
                if let Some(kms_key_id) = &material.kms_key_id {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), kms_key_id.to_string());
                }
                match material.sse_type {
                    SSEType::SseS3 => {
                        metadata.insert(
                            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                            BASE64_STANDARD.encode(sealed.sealed_key),
                        );
                    }
                    SSEType::SseKms => {
                        metadata.insert(
                            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(),
                            BASE64_STANDARD.encode(sealed.sealed_key),
                        );
                    }
                    SSEType::SseC => {}
                }
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode(encrypted_data_key),
                );
            } else if cfg!(feature = "rio-v2") {
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode(material.base_nonce),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                );
                if let Some(kms_key_id) = &material.kms_key_id {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), kms_key_id.to_string());
                }
                let encoded_key = BASE64_STANDARD.encode(encrypted_data_key);
                match material.sse_type {
                    SSEType::SseS3 => {
                        metadata.insert(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), encoded_key);
                    }
                    SSEType::SseKms => {
                        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), encoded_key.clone());
                        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), encoded_key);
                    }
                    SSEType::SseC => {}
                }
            }
        }
    }

    Ok(metadata)
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
/// if let Some(material) = sse_encryption(request).await? {
///     reader = material.wrap_reader(reader);
///     metadata.extend(material.metadata);
/// }
/// ```
pub async fn sse_encryption(request: EncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    validate_sse_headers_for_write(
        request.server_side_encryption.as_ref(),
        request.ssekms_key_id.as_ref(),
        request.ssekms_context.as_ref(),
        request.sse_customer_algorithm.as_ref(),
        request.sse_customer_key.as_ref(),
        request.sse_customer_key_md5.as_ref(),
        true,
    )?;

    // Priority 1: SSE-C (customer-provided key)
    if let (Some(algorithm), Some(key), Some(key_md5)) =
        (request.sse_customer_algorithm, request.sse_customer_key, request.sse_customer_key_md5)
    {
        return apply_ssec_encryption_material(request.bucket, request.key, algorithm, key, key_md5, request.content_size)
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
            request.ssekms_context,
            request.content_size,
            request.principal,
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
    /// Optional client-provided KMS context for SSE-KMS.
    pub ssekms_context: Option<HashMap<String, String>>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<SSECustomerAlgorithm>,
    /// SSE-C key (Base64-encoded)
    pub sse_customer_key: Option<SSECustomerKey>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<SSECustomerKeyMD5>,
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    ///
    /// This is the only point at which a multipart upload evaluates KMS key permissions:
    /// it is where the session data key is generated. `UploadPart`, `UploadPartCopy` and
    /// `CompleteMultipartUpload` reuse that envelope and are not re-authorized.
    pub principal: Option<&'a SseKmsPrincipal>,
}

pub async fn sse_prepare_encryption(request: PrepareEncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, ApiError> {
    validate_sse_headers_for_write(
        request.server_side_encryption.as_ref(),
        request.ssekms_key_id.as_ref(),
        request.ssekms_context.as_ref(),
        request.sse_customer_algorithm.as_ref(),
        request.sse_customer_key.as_ref(),
        request.sse_customer_key_md5.as_ref(),
        true,
    )?;

    let sse_type = prepare_sse_configuration_v2(
        request.bucket,
        request.server_side_encryption,
        request.sse_customer_algorithm,
        request.sse_customer_key.clone(),
        request.sse_customer_key_md5,
        request.ssekms_key_id,
    )
    .await?;

    // apply encryption material
    let material = match sse_type {
        Some(SseTypeV2::SseS3(sse)) => {
            apply_managed_encryption_material(
                request.bucket,
                request.key,
                sse,
                None,
                request.ssekms_context,
                0,
                request.principal,
            )
            .await?
        }
        Some(SseTypeV2::SseKms(sse, kms_key_id)) => {
            apply_managed_encryption_material(
                request.bucket,
                request.key,
                sse,
                kms_key_id,
                request.ssekms_context,
                0,
                request.principal,
            )
            .await?
        }
        Some(SseTypeV2::SseC(algorithm, _, key_md5)) => {
            apply_ssec_prepare_encryption_material(request.bucket, request.key, algorithm, request.sse_customer_key, key_md5)
                .await?
        }
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
/// };
///
/// if let Some(material) = sse_decryption(request).await? {
///     content_size = material.original_size.unwrap_or(actual_size);
/// }
/// ```
pub async fn sse_decryption(request: DecryptionRequest<'_>) -> Result<Option<DecryptionMaterial>, ApiError> {
    // Check for SSE-C encryption
    if request
        .metadata
        .contains_key("x-amz-server-side-encryption-customer-algorithm")
    {
        let (key, key_md5) = match (request.sse_customer_key, request.sse_customer_key_md5) {
            (Some(k), Some(md5)) => (k, md5),
            _ => {
                return Err(ssec_invalid_request(
                    "The object was stored using a form of Server Side Encryption. \
                     The correct parameters must be provided to retrieve the object.",
                ));
            }
        };

        // Verify that the provided key MD5 matches the stored MD5 for security
        let stored_md5 = request.metadata.get("x-amz-server-side-encryption-customer-key-md5");
        verify_ssec_key_match(key_md5, stored_md5)?;

        let mut material = apply_ssec_decryption_material(request.bucket, request.key, request.metadata, key, key_md5).await?;
        material.customer_key_md5 = Some(key_md5.clone());
        return Ok(Some(material));
    }

    // Check for managed SSE encryption
    if contains_managed_encryption_metadata(request.metadata) {
        return apply_managed_decryption_material(request.bucket, request.key, request.metadata, request.principal).await;
    }

    // No encryption detected
    Ok(None)
}

// ============================================================================
// Internal Implementation - SSE-C
// ============================================================================

async fn apply_ssec_prepare_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: SSECustomerAlgorithm,
    sse_key: Option<SSECustomerKey>,
    sse_key_md5: SSECustomerKeyMD5,
) -> Result<EncryptionMaterial, ApiError> {
    #[cfg(feature = "rio-v2")]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) = if let Some(sse_key) = sse_key {
        let validated = validate_ssec_params(SsecParams {
            algorithm: algorithm.clone(),
            key: sse_key,
            key_md5: sse_key_md5.clone(),
        })?;
        let object_key = derive_object_key(validated.key_bytes)?;
        let sealed_key = seal_object_key(object_key, validated.key_bytes, SSEType::SseC, bucket, key)?;
        (object_key, [0; 12], EncryptionKeyKind::Object, Some(sealed_key))
    } else {
        ([0; 32], [0; 12], EncryptionKeyKind::Direct, None)
    };

    #[cfg(not(feature = "rio-v2"))]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) = {
        let _ = (bucket, key, sse_key);
        // Generate a real random nonce for the multipart session and persist it (see
        // `encryption_material_to_metadata`). Every part of this upload reads the same
        // persisted nonce back via `apply_ssec_decryption_material`, so all parts share one
        // nonce without reintroducing the deterministic (bucket, key) reuse hazard.
        // key_bytes stays a placeholder here: it is neither used for encryption nor persisted;
        // the real customer key is validated per part on upload.
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut base_nonce);
        ([0; 32], base_nonce, EncryptionKeyKind::Direct, None)
    };

    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
        kms_key_id: None,
        algorithm,
        key_bytes,
        base_nonce,
        encrypted_data_key: None,
        customer_key_md5: Some(sse_key_md5),
        original_size: None,
        key_kind,
        managed_kms_context: None,
        managed_sealed_key,
    })
}

async fn apply_ssec_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: SSECustomerAlgorithm,
    sse_key: SSECustomerKey,
    sse_key_md5: SSECustomerKeyMD5,
    content_size: i64,
) -> Result<EncryptionMaterial, ApiError> {
    let params = SsecParams {
        algorithm,
        key: sse_key,
        key_md5: sse_key_md5,
    };

    let validated = validate_ssec_params(params)?;

    #[cfg(feature = "rio-v2")]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) = {
        let object_key = derive_object_key(validated.key_bytes)?;
        let sealed_key = seal_object_key(object_key, validated.key_bytes, SSEType::SseC, bucket, key)?;
        (object_key, [0; 12], EncryptionKeyKind::Object, Some(sealed_key))
    };

    #[cfg(not(feature = "rio-v2"))]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) = {
        // Use a fresh random nonce per encryption. A deterministic nonce derived from
        // (bucket, key) would repeat whenever the same object is overwritten under the same
        // SSE-C key, reusing an identical (key, nonce) pair and catastrophically breaking
        // AES-256-GCM. The nonce is persisted (see `encryption_material_to_metadata`) and read
        // back on decrypt (see `apply_ssec_decryption_material`).
        let _ = (bucket, key);
        let mut base_nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut base_nonce);
        (validated.key_bytes, base_nonce, EncryptionKeyKind::Direct, None)
    };

    // Build metadata
    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
        kms_key_id: None,
        algorithm: validated.algorithm,
        key_bytes,
        base_nonce,
        encrypted_data_key: None,
        customer_key_md5: Some(validated.key_md5),
        original_size: Some(content_size),
        key_kind,
        managed_kms_context: None,
        managed_sealed_key,
    })
}

/// Resolve the SSE-C Direct base nonce for decryption.
///
/// New objects persist a random nonce at encryption time (see
/// `encryption_material_to_metadata`) under `INTERNAL_ENCRYPTION_IV_HEADER` (preferred) or,
/// for MinIO interop, `MINIO_INTERNAL_ENCRYPTION_IV_HEADER`. Legacy objects written before
/// random nonces were persisted carry no stored IV; for those we fall back to the
/// deterministic `generate_ssec_nonce(bucket, key)` value they were originally encrypted
/// with, so previously stored objects still decrypt correctly.
fn read_stored_ssec_nonce(metadata: &HashMap<String, String>, bucket: &str, key: &str) -> [u8; 12] {
    metadata
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER))
        .and_then(|encoded| BASE64_STANDARD.decode(encoded).ok())
        .and_then(|bytes| <[u8; 12]>::try_from(bytes.as_slice()).ok())
        .unwrap_or_else(|| generate_ssec_nonce(bucket, key))
}

async fn apply_ssec_decryption_material(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    sse_key: &str,
    sse_key_md5: &str,
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

    #[cfg(feature = "rio-v2")]
    let (key_bytes, base_nonce, key_kind) = if let Some(sealed_key) = parse_minio_managed_sealed_key(metadata, SSEType::SseC)? {
        (
            unseal_object_key(&sealed_key, validated.key_bytes, SSEType::SseC, bucket, key)?,
            [0; 12],
            EncryptionKeyKind::Object,
        )
    } else {
        let base_nonce = read_stored_ssec_nonce(metadata, bucket, key);
        (validated.key_bytes, base_nonce, EncryptionKeyKind::Direct)
    };

    #[cfg(not(feature = "rio-v2"))]
    let (key_bytes, base_nonce, key_kind) = {
        let base_nonce = read_stored_ssec_nonce(metadata, bucket, key);
        (validated.key_bytes, base_nonce, EncryptionKeyKind::Direct)
    };

    Ok(DecryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AES256), // const
        kms_key_id: None,
        algorithm: SSECustomerAlgorithm::from(algorithm),

        customer_key_md5: None,
        key_bytes,
        base_nonce,
        key_kind,
    })
}

// ============================================================================
// Internal Implementation - Managed SSE (SSE-S3 / SSE-KMS)
// ============================================================================

async fn apply_managed_encryption_material(
    bucket: &str,
    key: &str,
    server_side_encryption: ServerSideEncryption,
    kms_key_id: Option<SSEKMSKeyId>,
    ssekms_context: Option<HashMap<String, String>>,
    content_size: i64,
    principal: Option<&SseKmsPrincipal>,
) -> Result<EncryptionMaterial, ApiError> {
    let requested_sse_type = managed_sse_type(server_side_encryption.as_str());
    let requested_key_id = kms_key_id.clone();
    let result = apply_managed_encryption_material_inner(
        bucket,
        key,
        server_side_encryption,
        kms_key_id,
        ssekms_context,
        content_size,
        principal,
    )
    .await;

    match &result {
        // The resolved key is only known on success: it may come from the request,
        // the bucket default or the KMS service default. On failure the audit entry
        // records what the caller asked for, which is what a reader needs to see.
        Ok(material) => record_managed_kms_outcome(
            principal,
            material.sse_type,
            material.kms_key_id.as_deref(),
            || material.encrypted_data_key.as_deref().and_then(envelope_master_key_version),
            Ok(()),
        ),
        Err(error) => record_managed_kms_outcome(principal, requested_sse_type, requested_key_id.as_deref(), || None, Err(error)),
    }

    result
}

/// Scheme a managed-SSE header names, defaulting to SSE-S3 the same way the
/// encryption and decryption paths do.
fn managed_sse_type(server_side_encryption: &str) -> SSEType {
    match server_side_encryption {
        ServerSideEncryption::AWS_KMS => SSEType::SseKms,
        _ => SSEType::SseS3,
    }
}

async fn apply_managed_encryption_material_inner(
    bucket: &str,
    key: &str,
    server_side_encryption: ServerSideEncryption,
    kms_key_id: Option<SSEKMSKeyId>,
    ssekms_context: Option<HashMap<String, String>>,
    content_size: i64,
    principal: Option<&SseKmsPrincipal>,
) -> Result<EncryptionMaterial, ApiError> {
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

    // Determine KMS key ID to use for internal key wrapping.
    let mut kms_key_candidate = kms_key_id.clone();
    if kms_key_candidate.is_none() {
        // Try to get default key from KMS service (if available)
        if let Some(service) = runtime_sources::current_encryption_service().await {
            kms_key_candidate = service.get_default_key_id().cloned();
            tracing::debug!(
                default_key_id = ?kms_key_candidate,
                "SSE-S3: KMS service available, default_key_id from config"
            );
        } else {
            tracing::debug!("SSE-S3: KMS encryption service not available");
        }
    }

    let kms_key_to_use = match (encryption_type, kms_key_candidate.clone()) {
        (SSEType::SseS3, Some(kms_key_id)) => kms_key_id,
        (SSEType::SseS3, None) => {
            tracing::debug!("SSE-S3: no KMS key configured, falling back to \"default\" key ID");
            "default".to_string()
        }
        (SSEType::SseKms, Some(kms_key_id)) => kms_key_id,
        (SSEType::SseKms, None) => {
            return Err(ApiError::from(StorageError::other(
                "No KMS key available for managed server-side encryption (required for SSE-KMS)",
            )));
        }
        _ => unreachable!("managed SSE branch only supports SSE-S3 or SSE-KMS"),
    };

    // The key is fully resolved here (request header, then bucket default, then the KMS
    // service default), so this is the first point at which the caller can be held to the
    // key it will actually be encrypted under.
    authorize_sse_kms_key(principal, encryption_type, KmsAction::GenerateDataKeyAction, &kms_key_to_use).await?;

    let provider = get_sse_dek_provider().await?;
    let object_context = build_object_encryption_context(bucket, key, ssekms_context.as_ref());
    let (data_key, encrypted_data_key) = provider.generate_sse_dek(&object_context, &kms_key_to_use).await?;

    let algorithm = server_side_encryption.as_str().to_string();
    #[cfg(feature = "rio-v2")]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) = {
        let object_key = derive_object_key(data_key.plaintext_key)?;
        let sealed_key = seal_object_key(object_key, data_key.plaintext_key, encryption_type, bucket, key)?;
        (object_key, [0u8; 12], EncryptionKeyKind::Object, Some(sealed_key))
    };

    #[cfg(not(feature = "rio-v2"))]
    let (key_bytes, base_nonce, key_kind, managed_sealed_key) =
        (data_key.plaintext_key, data_key.nonce, EncryptionKeyKind::Direct, None);

    Ok(EncryptionMaterial {
        sse_type: encryption_type,
        server_side_encryption,
        kms_key_id: Some(kms_key_to_use),
        algorithm,
        key_bytes,
        base_nonce,
        encrypted_data_key: Some(encrypted_data_key),
        customer_key_md5: None,
        original_size: Some(content_size),
        key_kind,
        managed_kms_context: matches!(encryption_type, SSEType::SseKms).then_some(ssekms_context.unwrap_or_default()),
        managed_sealed_key,
    })
}

async fn apply_managed_decryption_material(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    principal: Option<&SseKmsPrincipal>,
) -> Result<Option<DecryptionMaterial>, ApiError> {
    let result = apply_managed_decryption_material_inner(bucket, key, metadata, principal).await;

    match &result {
        // `None` means the object carries no managed-SSE metadata — SSE-C and
        // plaintext objects never reach KMS and must not appear in the summary.
        Ok(None) => {}
        Ok(Some(material)) => record_managed_kms_outcome(
            principal,
            material.sse_type,
            material.kms_key_id.as_deref(),
            || stored_envelope_master_key_version(metadata),
            Ok(()),
        ),
        Err(error) => {
            if let Some((sse_type, key_id)) = stored_managed_encryption_key(metadata) {
                record_managed_kms_outcome(
                    principal,
                    sse_type,
                    Some(&key_id),
                    || stored_envelope_master_key_version(metadata),
                    Err(error),
                );
            }
        }
    }

    result
}

async fn apply_managed_decryption_material_inner(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    principal: Option<&SseKmsPrincipal>,
) -> Result<Option<DecryptionMaterial>, ApiError> {
    #[cfg(not(feature = "rio-v2"))]
    let _ = (bucket, key);
    if !contains_managed_encryption_metadata(metadata) {
        return Ok(None);
    }

    let encryption_type = match metadata.get("x-amz-server-side-encryption").map(String::as_str) {
        Some(ServerSideEncryption::AWS_KMS) => SSEType::SseKms,
        Some(_) => SSEType::SseS3,
        // MinIO never persists the public scheme header: `crypto.S3.CreateMetadata`
        // writes only the `X-Minio-Internal-*` family and the public header is
        // synthesized onto the response by `DecryptObjectInfo`. Requiring it here
        // is what made every MinIO-encrypted object unreadable (backlog#1638).
        //
        // Inferring from the sealed-key slot is self-consistent by construction:
        // the slot decides which header the unseal reads AND which domain string
        // the sealing key is derived under, so a scheme that disagrees with the
        // slot cannot silently derive a wrong key — it finds no key at all.
        // Inferring from the KMS key id would NOT be safe: MinIO writes
        // `-S3-Kms-Key-Id` on SSE-S3 objects too.
        #[cfg(feature = "rio-v2")]
        None => match infer_minio_managed_sse_type(metadata) {
            Some(sse_type) => sse_type,
            // Still fail-closed, and deliberately not an error raised here: the
            // read plan independently classifies the object as encrypted from
            // its markers and refuses to serve it without material, so an
            // object whose scheme cannot be established never degrades into a
            // plaintext read.
            None => return Ok(None),
        },
        // Without the rio-v2 reader there is no MinIO-format read path to serve
        // such an object with, so it stays on the fail-closed branch.
        #[cfg(not(feature = "rio-v2"))]
        None => return Ok(None),
    };

    let normalized_metadata = normalize_managed_metadata(metadata, Some(recode_minio_kms_context));

    // Extract KMS key ID from metadata (optional, used for provider context)
    let kms_key_id = normalized_metadata
        .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
        .or_else(|| metadata.get("x-amz-server-side-encryption-aws-kms-key-id"))
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    // Ahead of every other failure mode below, so a denied caller learns nothing about the
    // key beyond "not yours" — not whether it is disabled, pending deletion, or unreadable.
    authorize_sse_kms_key(principal, encryption_type, KmsAction::DecryptAction, &kms_key_id).await?;

    #[cfg(feature = "rio-v2")]
    let minio_sealed_key = parse_minio_managed_sealed_key(metadata, encryption_type)?;
    #[cfg(not(feature = "rio-v2"))]
    let minio_sealed_key: Option<ManagedSealedKey> = None;

    let (encrypted_data_key, iv, algorithm) = if minio_sealed_key.is_some() {
        let encrypted_key_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
            .ok_or_else(|| ApiError::from(StorageError::other("Missing encrypted key in metadata")))?;
        let encrypted_data_key = BASE64_STANDARD
            .decode(encrypted_key_b64)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode encrypted key: {e}"))))?;
        (
            encrypted_data_key,
            Vec::new(),
            normalized_metadata
                .get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
                .cloned()
                .unwrap_or_else(|| "AES256".to_string()),
        )
    } else if let Some(service) = runtime_sources::current_encryption_service().await {
        // Production mode: use service for metadata parsing
        let parsed = service
            .headers_to_metadata(&normalized_metadata)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to parse encryption metadata: {e}"))))?;

        if parsed.iv.len() != 12 {
            return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
        }

        (parsed.encrypted_data_key, parsed.iv, parsed.algorithm)
    } else {
        // Test mode: parse metadata manually
        let encrypted_key_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .ok_or_else(|| ApiError::from(StorageError::other("Missing encrypted key in metadata")))?;
        let encrypted_data_key = BASE64_STANDARD
            .decode(encrypted_key_b64)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode encrypted key: {e}"))))?;

        let iv_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_IV_HEADER)
            .ok_or_else(|| ApiError::from(StorageError::other("Missing IV in metadata")))?;
        let iv = BASE64_STANDARD
            .decode(iv_b64)
            .map_err(|e| ApiError::from(StorageError::other(format!("Failed to decode IV: {e}"))))?;

        if iv.len() != 12 {
            return Err(ApiError::from(StorageError::other("Invalid encryption nonce length; expected 12 bytes")));
        }

        let algorithm = normalized_metadata
            .get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
            .cloned()
            .unwrap_or_else(|| "AES256".to_string());

        (encrypted_data_key, iv, algorithm)
    };

    let kms_context = if matches!(encryption_type, SSEType::SseKms) {
        decode_minio_kms_context(metadata)?
    } else {
        None
    };
    let object_context = build_object_encryption_context(bucket, key, kms_context.as_ref());

    // Persisted wrapping format is the read-side source of truth. The
    // advertised SSE scheme and current KMS availability are write policy
    // and runtime state, neither of which identifies the historical provider.
    let provider: Arc<dyn SseDekProvider> = if is_data_key_envelope(&encrypted_data_key) {
        // When a test-injected provider is registered via set_sse_dek_provider_for_test,
        // use it instead of creating a fresh KmsSseDekProvider which cannot resolve
        // a KMS service without an AppContext.
        //
        // Reads GLOBAL_KMS_DEK_PROVIDER (populated only by set_sse_dek_provider_for_test),
        // never GLOBAL_SSE_DEK_PROVIDER, so a local provider cached by a prior
        // get_local_sse_dek_provider call cannot be selected to unwrap a KMS envelope.
        if let Some(cached) = GLOBAL_KMS_DEK_PROVIDER.read().ok().and_then(|guard| guard.as_ref().cloned()) {
            cached
        } else {
            Arc::new(KmsSseDekProvider::new().await?)
        }
    } else {
        get_local_sse_dek_provider().await?
    };
    // A MinIO sealed key alone does not mean MinIO wrote the object: RustFS's own
    // writer fills MinIO's metadata slots too, while still storing a RustFS
    // envelope in them, so neither the slot nor the header name distinguishes the
    // two. The data key's own shape does. RustFS envelopes are strictly-parsed
    // JSON; MinIO's builtin-KMS ciphertext is opaque bytes that match neither, so
    // recognizing RustFS positively — and treating only the remainder as MinIO —
    // keeps a RustFS envelope from ever reaching MinIO's decoder.
    #[cfg(feature = "rio-v2")]
    let decrypted_data_key = if minio_sealed_key.is_some() && !is_rustfs_managed_data_key(&encrypted_data_key) {
        provider
            .decrypt_minio_sse_dek(&encrypted_data_key, &kms_key_id, &object_context)
            .await
    } else if is_legacy_rustfs_managed_metadata(&normalized_metadata) {
        provider
            .decrypt_legacy_sse_dek(&encrypted_data_key, &kms_key_id, &object_context)
            .await
    } else {
        provider
            .decrypt_sse_dek(&encrypted_data_key, &kms_key_id, &object_context)
            .await
    };
    #[cfg(not(feature = "rio-v2"))]
    let decrypted_data_key = provider
        .decrypt_sse_dek(&encrypted_data_key, &kms_key_id, &object_context)
        .await;
    let decrypted_data_key = decrypted_data_key?;
    #[cfg(feature = "rio-v2")]
    let (key_bytes, base_nonce, key_kind) = if let Some(sealed_key) = minio_sealed_key {
        (
            unseal_object_key(&sealed_key, decrypted_data_key, encryption_type, bucket, key)?,
            [0u8; 12],
            EncryptionKeyKind::Object,
        )
    } else {
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&iv[..12]);
        (decrypted_data_key, base_nonce, EncryptionKeyKind::Direct)
    };
    #[cfg(not(feature = "rio-v2"))]
    let (key_bytes, base_nonce, key_kind) = {
        let mut base_nonce = [0u8; 12];
        base_nonce.copy_from_slice(&iv[..12]);
        (decrypted_data_key, base_nonce, EncryptionKeyKind::Direct)
    };

    Ok(Some(DecryptionMaterial {
        sse_type: encryption_type,
        // Synthesized from the resolved scheme rather than read back from
        // metadata: a MinIO-written object has no stored scheme header, which is
        // exactly why the gate above had to infer it. MinIO synthesizes the same
        // header onto its own responses.
        server_side_encryption: ServerSideEncryption::from(managed_sse_public_header(encryption_type).to_string()),
        kms_key_id: Some(SSEKMSKeyId::from(kms_key_id)),
        algorithm,
        customer_key_md5: None,

        key_bytes,
        base_nonce,
        key_kind,
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
    async fn generate_sse_dek(&self, context: &ObjectEncryptionContext, kms_key_id: &str)
    -> Result<(DataKey, Vec<u8>), ApiError>;

    /// Decrypt an SSE data encryption key (returns only plaintext key, nonce should be read from metadata)
    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError>;

    /// Decrypt a DEK from positively identified legacy managed metadata.
    #[cfg(feature = "rio-v2")]
    async fn decrypt_legacy_sse_dek(
        &self,
        encrypted_dek: &[u8],
        kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        self.decrypt_sse_dek(encrypted_dek, kms_key_id, context).await
    }

    /// Unwrap a data key that MinIO's builtin KMS sealed.
    ///
    /// A separate entry point rather than a shape sniff inside
    /// [`Self::decrypt_sse_dek`]: the caller already knows the object carries a
    /// MinIO sealed key, and MinIO's raw ciphertext is unstructured bytes that
    /// no parser can reliably tell apart from anything else. Routing on the
    /// caller's knowledge keeps a RustFS envelope from ever reaching MinIO's
    /// decoder, and vice versa.
    ///
    /// Defaults to refusing: only a provider holding the MinIO master secret
    /// can serve these, and a provider that cannot must fail rather than fall
    /// back to a decoder that would misread the bytes.
    #[cfg(feature = "rio-v2")]
    async fn decrypt_minio_sse_dek(
        &self,
        _encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        Err(ApiError::from(StorageError::other(
            "This KMS provider cannot unwrap a data key sealed by MinIO's builtin KMS",
        )))
    }
}

// ============================================================================
// Production KMS-backed DEK Provider
// ============================================================================

/// Production KMS-backed DEK provider
/// Resolves the latest ObjectEncryptionService on each call.
struct KmsSseDekProvider {
    #[cfg(test)]
    service_manager: Option<Arc<rustfs_kms::KmsServiceManager>>,
}

fn kms_operation_error(error: rustfs_kms::KmsError) -> ApiError {
    let class = rustfs_kms::audit::error_class(&error);
    let mut api_error = ApiError::from(StorageError::other(error));
    // Wrap rather than replace the source so the original chain stays intact for
    // logging; the wrapper only adds the class the audit attachment needs.
    let source = api_error.source.take();
    api_error.source = Some(Box::new(KmsDataPlaneFailure { class, source }));
    api_error
}

impl KmsSseDekProvider {
    /// Create a new KMS-backed provider
    pub async fn new() -> Result<Self, ApiError> {
        let provider = Self {
            #[cfg(test)]
            service_manager: None,
        };
        provider
            .current_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other(KmsUnavailableError)))?;
        Ok(provider)
    }

    #[cfg(test)]
    async fn new_with_service_manager(service_manager: Arc<rustfs_kms::KmsServiceManager>) -> Result<Self, ApiError> {
        let provider = Self {
            service_manager: Some(service_manager),
        };
        provider
            .current_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other(KmsUnavailableError)))?;
        Ok(provider)
    }

    async fn current_service(&self) -> Option<Arc<rustfs_kms::service::ObjectEncryptionService>> {
        #[cfg(test)]
        if let Some(service_manager) = &self.service_manager {
            return service_manager.get_encryption_service().await;
        }

        runtime_sources::current_encryption_service().await
    }
}

#[async_trait]
impl SseDekProvider for KmsSseDekProvider {
    async fn generate_sse_dek(
        &self,
        context: &ObjectEncryptionContext,
        kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), ApiError> {
        let kms_key_option = Some(kms_key_id.to_string());
        let service = self
            .current_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other(KmsUnavailableError)))?;
        let (data_key, encrypted_data_key) = service
            .create_data_key(&kms_key_option, context)
            .await
            .map_err(kms_operation_error)?;

        Ok((data_key, encrypted_data_key))
    }

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        let service = self
            .current_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other(KmsUnavailableError)))?;
        let data_key = service
            .decrypt_data_key(encrypted_dek, context)
            .await
            .map_err(kms_operation_error)?;

        Ok(data_key.plaintext_key)
    }

    #[cfg(feature = "rio-v2")]
    async fn decrypt_legacy_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        let service = self
            .current_service()
            .await
            .ok_or_else(|| ApiError::from(StorageError::other(KmsUnavailableError)))?;
        let data_key = service
            .decrypt_legacy_data_key(encrypted_dek)
            .await
            .map_err(kms_operation_error)?;

        Ok(data_key.plaintext_key)
    }
}

// ============================================================================
// Test/Simple DEK Provider
// ============================================================================

/// Local SSE DEK provider for deployments without a KMS.
///
/// Uses `RUSTFS_SSE_S3_MASTER_KEY` (base64-encoded 32-byte key) to wrap
/// data-encryption keys with AES-256-GCM.  This is the production fallback
/// when no KMS service is configured.
///
/// # Environment Variable
///
/// ```text
/// RUSTFS_SSE_S3_MASTER_KEY=<base64_encoded_32_byte_key>
/// ```
pub(crate) struct LocalSseDekProvider {
    master_key: [u8; 32],
}

const LOCAL_SSE_DEK_FORMAT_VERSION: u8 = 1;

#[cfg(feature = "rio-v2")]
/// Returns true when a managed-SSE data key is one RustFS itself wrote.
///
/// Both RustFS envelope shapes are strict JSON — the KMS envelope
/// ([`rustfs_kms::is_data_key_envelope`]) and the local provider's
/// [`LocalSseDekEnvelope`], whose `deny_unknown_fields` keeps it from accepting
/// anything else. Recognition is deliberately positive: an unrecognized payload
/// is left to MinIO's decoder rather than guessed at, and neither decoder is
/// ever handed the other's format.
fn is_rustfs_managed_data_key(encrypted_dek: &[u8]) -> bool {
    if rustfs_kms::is_data_key_envelope(encrypted_dek) {
        return true;
    }
    std::str::from_utf8(encrypted_dek)
        .ok()
        .is_some_and(|text| serde_json::from_str::<LocalSseDekEnvelope<'_>>(text).is_ok())
}

#[cfg(feature = "rio-v2")]
/// Associated data MinIO binds when sealing a data key.
///
/// MinIO passes the object's encryption context as the AEAD's associated data,
/// serialized as canonical JSON with sorted keys — the same canonicalization
/// [`rustfs_kms::context_aad`] performs, which is why the context RustFS
/// already rebuilds for the read can be reused verbatim. For SSE-S3 that
/// context is `{bucket: "bucket/object"}`; for SSE-KMS it is whatever the
/// request supplied, recovered from the stored MinIO context header.
fn minio_kms_associated_data(context: &ObjectEncryptionContext) -> Result<Vec<u8>, ApiError> {
    let mut ctx = context.encryption_context.clone();
    ctx.entry(context.bucket.clone())
        .or_insert_with(|| canonical_kms_bucket_path(&context.bucket, &context.object_key));
    rustfs_kms::context_aad(&ctx)
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to canonicalize MinIO KMS context: {e}"))))
}

#[cfg(feature = "rio-v2")]
/// MinIO's builtin-KMS ciphertext in its JSON encoding.
///
/// Deliberately its own type rather than a relaxation of
/// [`LocalSseDekEnvelope`]: widening that envelope's `deny_unknown_fields`
/// to admit this shape would also admit malformed RustFS envelopes, which
/// backlog#1567 requires to keep failing closed.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MinioKmsCiphertextJson {
    aead: String,
    #[allow(
        dead_code,
        reason = "present in MinIO's encoding; the key is identified by metadata instead"
    )]
    #[serde(default)]
    id: String,
    iv: String,
    nonce: String,
    bytes: String,
}

/// Bytes of trailing randomness every MinIO builtin-KMS ciphertext carries:
/// a 16-byte IV followed by a 12-byte nonce, *after* the sealed bytes.
#[cfg(feature = "rio-v2")]
const MINIO_KMS_RANDOM_LEN: usize = 28;
#[cfg(feature = "rio-v2")]
const MINIO_KMS_IV_LEN: usize = 16;

#[cfg(feature = "rio-v2")]
const MINIO_KMS_AEAD_AES_GCM: &str = "AES-256-GCM-HMAC-SHA-256";
#[cfg(feature = "rio-v2")]
const MINIO_KMS_AEAD_CHACHA20: &str = "ChaCha20Poly1305";

#[cfg(feature = "rio-v2")]
/// Unwrap a data key sealed by MinIO's builtin (static-secret) KMS.
///
/// The wire format is `sealed_bytes || iv[16] || nonce[12]` — the randomness
/// trails the ciphertext rather than leading it, and MinIO's own decoder
/// normalizes its legacy JSON encoding into exactly that byte order before
/// opening it (`internal/kms/secret-key.go`, `parseCiphertext`). A raw
/// (non-JSON) ciphertext is AES-256-GCM by definition there; the JSON form
/// names its algorithm.
///
/// The sealing key is derived per ciphertext rather than being the master key:
/// `HMAC-SHA256(master, iv)` for AES-256-GCM, `HChaCha20(master, iv)` for
/// ChaCha20-Poly1305. The encryption context is bound as associated data.
fn decrypt_minio_kms_data_key(encrypted_dek: &[u8], master_key: &[u8; 32], aad: &[u8]) -> Result<[u8; 32], ApiError> {
    let (body, algorithm) = match std::str::from_utf8(encrypted_dek) {
        // MinIO only treats a payload as JSON when it both starts and ends like
        // an object, and falls back to the raw layout when it does not parse —
        // mirrored here so a ciphertext that merely looks like JSON is not
        // rejected outright.
        Ok(text)
            if text.starts_with('{')
                && text.ends_with('}')
                && let Ok(json) = serde_json::from_str::<MinioKmsCiphertextJson>(text) =>
        {
            let decode = |what: &str, value: &str| -> Result<Vec<u8>, ApiError> {
                BASE64_STANDARD
                    .decode(value)
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid MinIO KMS {what}: {e}"))))
            };
            let mut body = decode("ciphertext", &json.bytes)?;
            body.extend_from_slice(&decode("iv", &json.iv)?);
            body.extend_from_slice(&decode("nonce", &json.nonce)?);
            (body, json.aead)
        }
        _ => (encrypted_dek.to_vec(), MINIO_KMS_AEAD_AES_GCM.to_string()),
    };

    if body.len() <= MINIO_KMS_RANDOM_LEN {
        return Err(ApiError::from(StorageError::other(
            "MinIO KMS ciphertext is too short to carry its IV and nonce",
        )));
    }
    let (sealed, random) = body.split_at(body.len() - MINIO_KMS_RANDOM_LEN);
    let (iv, nonce) = random.split_at(MINIO_KMS_IV_LEN);

    let plaintext = match algorithm.as_str() {
        MINIO_KMS_AEAD_AES_GCM => {
            use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
            let mut mac = HmacSha256::new_from_slice(master_key)
                .map_err(|_| ApiError::from(StorageError::other("MinIO KMS sealing key derivation failed")))?;
            mac.update(iv);
            let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
            let cipher = Aes256Gcm::new_from_slice(&sealing_key)
                .map_err(|_| ApiError::from(StorageError::other("MinIO KMS sealing key is not a valid AES-256 key")))?;
            let nonce = aes_gcm::Nonce::try_from(nonce)
                .map_err(|_| ApiError::from(StorageError::other("MinIO KMS nonce is not 12 bytes")))?;
            cipher.decrypt(&nonce, aes_gcm::aead::Payload { msg: sealed, aad })
        }
        MINIO_KMS_AEAD_CHACHA20 => {
            use chacha20poly1305::{KeyInit, XChaCha20Poly1305, aead::Aead};
            // MinIO derives this branch's key with HChaCha20 over the 16-byte
            // IV, which is exactly XChaCha20-Poly1305's own construction, so the
            // extended-nonce cipher does the derivation rather than hand-rolling it.
            let mut extended = Vec::with_capacity(MINIO_KMS_IV_LEN + nonce.len());
            extended.extend_from_slice(iv);
            extended.extend_from_slice(nonce);
            let cipher = XChaCha20Poly1305::new_from_slice(master_key)
                .map_err(|_| ApiError::from(StorageError::other("MinIO KMS master key is not a valid ChaCha20 key")))?;
            let nonce = chacha20poly1305::XNonce::try_from(extended.as_slice())
                .map_err(|_| ApiError::from(StorageError::other("MinIO KMS extended nonce is not 24 bytes")))?;
            cipher.decrypt(&nonce, chacha20poly1305::aead::Payload { msg: sealed, aad })
        }
        other => {
            return Err(ApiError::from(StorageError::other(format!(
                "Unsupported MinIO KMS AEAD algorithm: {other}"
            ))));
        }
    }
    // An AEAD failure here is authentication, not a decode slip: a wrong master
    // key, a tampered ciphertext, and an encryption context that does not match
    // what sealed it all land here and must all fail closed.
    .map_err(|_| ApiError::from(StorageError::other("MinIO KMS data key failed authentication")))?;

    plaintext.try_into().map_err(|value: Vec<u8>| {
        ApiError::from(StorageError::other(format!("MinIO KMS data key must be 32 bytes, got {}", value.len())))
    })
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LocalSseDekEnvelope<'a> {
    version: u8,
    nonce: &'a str,
    ciphertext: &'a str,
}

/// Test-only alias so existing test code that references `TestSseDekProvider`
/// continues to compile without changes.
#[cfg(test)]
#[allow(non_camel_case_types)]
pub(crate) type TestSseDekProvider = LocalSseDekProvider;

/// Parse the base64-encoded 32-byte master key from `__RUSTFS_SSE_SIMPLE_CMK`.
///
/// Returns an error (never crashes) for a missing, non-base64, wrong-length, or
/// all-zero key so callers on the request path can fail the request instead of
/// taking the whole server down (backlog#806).
#[cfg(test)]
fn parse_simple_sse_cmk(cmk_value: &str) -> Result<[u8; 32], ApiError> {
    let trimmed = cmk_value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::from(StorageError::other(
            "SSE simple mode requires __RUSTFS_SSE_SIMPLE_CMK to be set to a base64-encoded 32-byte key",
        )));
    }
    let decoded = BASE64_STANDARD
        .decode(trimmed)
        .map_err(|e| ApiError::from(StorageError::other(format!("__RUSTFS_SSE_SIMPLE_CMK must be valid base64: {e}"))))?;
    let master_key: [u8; 32] = decoded.try_into().map_err(|v: Vec<u8>| {
        ApiError::from(StorageError::other(format!(
            "__RUSTFS_SSE_SIMPLE_CMK must decode to exactly 32 bytes, got {} bytes",
            v.len()
        )))
    })?;
    if master_key == [0u8; 32] {
        return Err(ApiError::from(StorageError::other("__RUSTFS_SSE_SIMPLE_CMK must not be an all-zero key")));
    }
    Ok(master_key)
}

impl LocalSseDekProvider {
    /// Create a LocalSseDekProvider with a predefined key (for testing)
    #[cfg(test)]
    pub fn new_with_key(master_key: [u8; 32]) -> Self {
        Self { master_key }
    }

    /// Create a TestSseDekProvider from `__RUSTFS_SSE_SIMPLE_CMK` (test-only).
    #[cfg(test)]
    pub fn new() -> Result<Self, ApiError> {
        let cmk_value = std::env::var("__RUSTFS_SSE_SIMPLE_CMK").unwrap_or_default();
        // A missing/invalid key must surface as a request error, never crash the
        // whole server: `LocalSseDekProvider::new` is reached from the SSE request
        // path (get_sse_dek_provider), so `process::exit(1)` here turned a bad
        // `__RUSTFS_SSE_SIMPLE_CMK` into a process crash-loop DoS (backlog#806).
        let master_key = parse_simple_sse_cmk(&cmk_value)?;
        tracing::info!("Successfully loaded SSE master key (32 bytes) from __RUSTFS_SSE_SIMPLE_CMK");
        Ok(Self { master_key })
    }

    /// Create a local SSE DEK provider for SSE-S3 when KMS is not configured.
    /// Requires RUSTFS_SSE_S3_MASTER_KEY to be a valid base64-encoded 32-byte key.
    ///
    /// The failures here are server configuration problems, not internal
    /// faults: surface them as `InvalidRequest` (HTTP 400) so a managed-SSE
    /// request against an unconfigured server does not report 500 (rustfs#4844).
    pub fn new_from_env() -> Result<Self, ApiError> {
        fn sse_not_configured(message: impl Into<String>) -> ApiError {
            ApiError {
                code: S3ErrorCode::InvalidRequest,
                message: message.into(),
                source: None,
            }
        }

        let Some(raw_value) = get_env_opt_str("RUSTFS_SSE_S3_MASTER_KEY").filter(|value| !value.trim().is_empty()) else {
            return Err(sse_not_configured(
                "SSE-S3 requires RUSTFS_SSE_S3_MASTER_KEY to be set to a base64-encoded 32-byte key when KMS is not configured",
            ));
        };

        let decoded = BASE64_STANDARD.decode(raw_value.trim()).map_err(|err| {
            sse_not_configured(format!(
                "RUSTFS_SSE_S3_MASTER_KEY must be valid base64 for SSE-S3 when KMS is not configured: {err}"
            ))
        })?;
        let master_key: [u8; 32] = decoded.try_into().map_err(|_| {
            sse_not_configured("RUSTFS_SSE_S3_MASTER_KEY must decode to exactly 32 bytes for SSE-S3 when KMS is not configured")
        })?;

        tracing::info!("Using RUSTFS_SSE_S3_MASTER_KEY for SSE-S3 (KMS not configured)");
        Ok(Self { master_key })
    }

    // Simple encryption of DEK
    pub(crate) fn encrypt_dek(dek: [u8; 32], cmk_value: [u8; 32]) -> Result<String, ApiError> {
        // Use AES-256-GCM to encrypt DEK
        let key = Key::<Aes256Gcm>::from(cmk_value);

        let cipher = Aes256Gcm::new(&key);
        let mut nonce_bytes = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from(nonce_bytes);
        let ciphertext = cipher
            .encrypt(&nonce, dek.as_slice())
            .map_err(|_| ApiError::from(StorageError::other("Failed to encrypt DEK")))?;

        let nonce = BASE64_STANDARD.encode(nonce);
        let ciphertext = BASE64_STANDARD.encode(ciphertext);
        serde_json::to_string(&LocalSseDekEnvelope {
            version: LOCAL_SSE_DEK_FORMAT_VERSION,
            nonce: &nonce,
            ciphertext: &ciphertext,
        })
        .map_err(|e| ApiError::from(StorageError::other(format!("Failed to serialize encrypted DEK: {e}"))))
    }

    // Simple decryption of DEK
    pub(crate) fn decrypt_dek(encrypted_dek: &str, cmk_value: [u8; 32]) -> Result<[u8; 32], ApiError> {
        let envelope = serde_json::from_str::<LocalSseDekEnvelope<'_>>(encrypted_dek);
        let (nonce, ciphertext) = match envelope {
            Ok(envelope) => {
                if envelope.version != LOCAL_SSE_DEK_FORMAT_VERSION {
                    return Err(ApiError::from(StorageError::other(format!(
                        "Unsupported encrypted DEK format version: {}",
                        envelope.version
                    ))));
                }
                (envelope.nonce, envelope.ciphertext)
            }
            Err(json_error) if encrypted_dek.trim_start().starts_with('{') => {
                return Err(ApiError::from(StorageError::other(format!(
                    "Invalid encrypted DEK JSON format: {json_error}"
                ))));
            }
            Err(_) => {
                // DEPRECATED: read-only compatibility for persisted colon-delimited DEKs.
                // RUSTFS_COMPAT_TODO(sse-local-dek-json-v1): Remove after all supported upgrades have rewritten legacy DEKs.
                let Some((nonce, ciphertext)) = encrypted_dek.split_once(':') else {
                    return Err(ApiError::from(StorageError::other("Invalid encrypted DEK format")));
                };
                if ciphertext.contains(':') {
                    return Err(ApiError::from(StorageError::other("Invalid encrypted DEK format")));
                }
                (nonce, ciphertext)
            }
        };
        let nonce_vec = BASE64_STANDARD
            .decode(nonce)
            .map_err(|_| ApiError::from(StorageError::other("Invalid nonce format")))?;
        let ciphertext = BASE64_STANDARD
            .decode(ciphertext)
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
impl SseDekProvider for LocalSseDekProvider {
    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), ApiError> {
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

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        // Decrypt data key with master key
        let encrypted_dek_str = std::str::from_utf8(encrypted_dek)
            .map_err(|_| ApiError::from(StorageError::other("Invalid UTF-8 in encrypted DEK")))?;
        let dek = Self::decrypt_dek(encrypted_dek_str, self.master_key)?;
        Ok(dek)
    }

    #[cfg(feature = "rio-v2")]
    async fn decrypt_minio_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], ApiError> {
        let aad = minio_kms_associated_data(context)?;
        decrypt_minio_kms_data_key(encrypted_dek, &self.master_key, &aad)
    }
}

// ============================================================================
// Factory Function for SSE DEK Provider
// ============================================================================

/// Global SSE DEK provider cache for local / test providers.
///
/// Populated by `get_local_sse_dek_provider` and (for backward-compat in tests)
/// `set_sse_dek_provider_for_test`.  Read by `get_local_sse_dek_provider` only —
/// the KMS-envelope decrypt path uses `GLOBAL_KMS_DEK_PROVIDER` so that a cached
/// local provider can never be selected for a KMS-wrapped data-key.
static GLOBAL_SSE_DEK_PROVIDER: LazyLock<RwLock<Option<Arc<dyn SseDekProvider>>>> = LazyLock::new(|| RwLock::new(None));

/// Global KMS DEK provider cache for test-injected KMS providers.
///
/// Populated **only** by `set_sse_dek_provider_for_test` (test-only).
/// Read **only** by the KMS-envelope branch of `apply_managed_decryption_material`.
/// Separation from `GLOBAL_SSE_DEK_PROVIDER` prevents a previously-cached local
/// provider from being selected to unwrap a KMS data-key envelope.
static GLOBAL_KMS_DEK_PROVIDER: LazyLock<RwLock<Option<Arc<dyn SseDekProvider>>>> = LazyLock::new(|| RwLock::new(None));

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
    if runtime_sources::current_encryption_service().await.is_some() {
        debug!("Using KmsSseDekProvider (KMS configured)");
        return Ok(Arc::new(KmsSseDekProvider::new().await?));
    }

    get_local_sse_dek_provider().await
}

async fn get_local_sse_dek_provider() -> Result<Arc<dyn SseDekProvider>, ApiError> {
    // An explicitly injected test provider overrides both the cache and the
    // environment-based selection below.
    #[cfg(test)]
    if let Some(injected) = test_injected_sse_dek_provider() {
        return Ok(injected);
    }

    // Check if already initialized
    if let Some(provider) = GLOBAL_SSE_DEK_PROVIDER
        .read()
        .map_err(|_| ApiError::from(StorageError::other("Failed to read global SSE DEK provider cache")))?
        .as_ref()
        .cloned()
    {
        return Ok(provider);
    }

    // In test mode, prefer the simple CMK provider when the env var is set.
    #[cfg(test)]
    {
        if std::env::var("__RUSTFS_SSE_SIMPLE_CMK").is_ok() {
            debug!("Using LocalSseDekProvider (test mode) based on __RUSTFS_SSE_SIMPLE_CMK");
            let provider: Arc<dyn SseDekProvider> = Arc::new(LocalSseDekProvider::new()?);
            let mut slot = GLOBAL_SSE_DEK_PROVIDER
                .write()
                .map_err(|_| ApiError::from(StorageError::other("Failed to update global SSE DEK provider cache")))?;
            if let Some(existing) = slot.as_ref() {
                return Ok(existing.clone());
            }
            *slot = Some(provider.clone());
            return Ok(provider);
        }
    }

    // Production fallback: local SSE-S3 provider (no KMS configured).
    debug!("Using local SSE-S3 provider (KMS not configured)");
    let provider: Arc<dyn SseDekProvider> = Arc::new(LocalSseDekProvider::new_from_env()?);

    let mut slot = GLOBAL_SSE_DEK_PROVIDER
        .write()
        .map_err(|_| ApiError::from(StorageError::other("Failed to update global SSE DEK provider cache")))?;
    if let Some(existing) = slot.as_ref() {
        return Ok(existing.clone());
    }
    *slot = Some(provider.clone());

    Ok(provider)
}

/// Reset both global SSE DEK provider caches (for testing only)
///
/// Clears GLOBAL_SSE_DEK_PROVIDER (local/test providers) and
/// GLOBAL_KMS_DEK_PROVIDER (test-injected KMS providers).
#[cfg(test)]
pub fn reset_sse_dek_provider() {
    if let Ok(mut slot) = GLOBAL_SSE_DEK_PROVIDER.write() {
        *slot = None;
    }
    if let Ok(mut slot) = GLOBAL_KMS_DEK_PROVIDER.write() {
        *slot = None;
    }
}

#[cfg(test)]
pub fn set_sse_dek_provider_for_test(provider: Arc<dyn SseDekProvider>) {
    if let Ok(mut slot) = GLOBAL_KMS_DEK_PROVIDER.write() {
        *slot = Some(provider.clone());
    }
    if let Ok(mut slot) = GLOBAL_SSE_DEK_PROVIDER.write() {
        *slot = Some(provider);
    }
}

/// Provider explicitly injected via `set_sse_dek_provider_for_test`, if any.
///
/// Reads `GLOBAL_KMS_DEK_PROVIDER` because that slot is populated *only* by the
/// test setter, so a hit here always means an explicit test injection.
#[cfg(test)]
fn test_injected_sse_dek_provider() -> Option<Arc<dyn SseDekProvider>> {
    GLOBAL_KMS_DEK_PROVIDER.read().ok().and_then(|guard| guard.as_ref().cloned())
}

// ============================================================================
// Legacy Functions (SSE-S3 / SSE-KMS)
// ============================================================================

/// Check if the server_side_encryption is a managed SSE type (SSE-S3 or SSE-KMS)
#[inline]
pub fn is_managed_sse(server_side_encryption: &ServerSideEncryption) -> bool {
    matches!(server_side_encryption.as_str(), "AES256" | "aws:kms")
}

/// Strip source encryption metadata before constructing metadata for a copy destination.
///
/// Encryption metadata describes the physical source representation and must never be
/// inherited by a plaintext destination or by a destination using a different key.
pub fn strip_managed_encryption_metadata(metadata: &mut HashMap<String, String>) {
    const KEYS: [&str; 21] = [
        "x-amz-server-side-encryption",
        "x-amz-server-side-encryption-aws-kms-key-id",
        "x-amz-server-side-encryption-customer-algorithm",
        "x-amz-server-side-encryption-customer-key-md5",
        SSEC_ORIGINAL_SIZE_HEADER,
        INTERNAL_ENCRYPTION_KEY_ID_HEADER,
        INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
        INTERNAL_ENCRYPTION_IV_HEADER,
        INTERNAL_ENCRYPTION_TAG_HEADER,
        INTERNAL_ENCRYPTION_KEY_HEADER,
        INTERNAL_ENCRYPTION_CONTEXT_HEADER,
        INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER,
        MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER,
        MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
        MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER,
        MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER,
    ];

    for key in KEYS.iter() {
        metadata.remove(*key);
    }
}

pub fn mark_encrypted_multipart_metadata(metadata: &mut HashMap<String, String>) {
    metadata.insert(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER.to_string(), String::new());
}

#[cfg(feature = "rio-v2")]
fn is_legacy_rustfs_managed_metadata(metadata: &HashMap<String, String>) -> bool {
    metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER)
        && metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER)
        && !metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
        && !metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
}

#[cfg(feature = "rio-v2")]
#[cfg(feature = "rio-v2")]
/// Infer the managed SSE scheme from the MinIO sealed-key slot that is present.
///
/// Returns `None` when no managed MinIO slot is present, which keeps callers on
/// their fail-closed path. SSE-C is not a managed scheme and is handled by the
/// SSE-C read path, so its slot is not considered here.
fn infer_minio_managed_sse_type(metadata: &HashMap<String, String>) -> Option<SSEType> {
    if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER) {
        Some(SSEType::SseS3)
    } else if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER) {
        Some(SSEType::SseKms)
    } else {
        None
    }
}

#[cfg(feature = "rio-v2")]
fn parse_minio_managed_sealed_key(
    metadata: &HashMap<String, String>,
    sse_type: SSEType,
) -> Result<Option<ManagedSealedKey>, ApiError> {
    let algorithm = match metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER) {
        Some(algorithm) => algorithm,
        None => return Ok(None),
    };
    if algorithm != MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM {
        return Ok(None);
    }

    let iv = match metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER) {
        Some(iv) => match try_decode_minio_sealing_iv(iv)? {
            Some(iv) => iv,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    let header_key = match sse_type {
        SSEType::SseS3 => MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
        SSEType::SseKms => MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        SSEType::SseC => MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
    };
    let sealed_key = match metadata.get(header_key) {
        Some(value) => match try_decode_minio_sealed_key(value)? {
            Some(sealed_key) => sealed_key,
            None => return Ok(None),
        },
        None => return Ok(None),
    };

    Ok(Some(ManagedSealedKey { iv, sealed_key }))
}

/// Recodes a stored MinIO KMS context value (base64-wrapped JSON) into the
/// plain-JSON form RustFS stores under [`INTERNAL_ENCRYPTION_CONTEXT_HEADER`].
///
/// Injected into the shared [`normalize_managed_metadata`] because the shared
/// crate carries no JSON codec; any decode failure returns `None`, which skips
/// the context mapping exactly like the historical inline `if let Ok` chain.
fn recode_minio_kms_context(value: &str) -> Option<String> {
    let decoded = BASE64_STANDARD.decode(value).ok()?;
    let context = serde_json::from_slice::<HashMap<String, String>>(&decoded).ok()?;
    serde_json::to_string(&context).ok()
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
    if !SUPPORT_SSE_ALGORITHMS.contains(&params.algorithm.as_str()) {
        return Err(ssec_invalid_request(&format!(
            "Unsupported SSE-C algorithm. Only {DEFAULT_SSE_ALGORITHM} is supported."
        )));
    }

    let key_bytes = BASE64_STANDARD.decode(&params.key).map_err(|e| {
        error!("Failed to decode SSE-C key: {}", e);
        ssec_invalid_request("Invalid SSE-C key: not valid Base64.")
    })?;

    if key_bytes.len() != 32 {
        return Err(ssec_invalid_request(&format!(
            "SSE-C key must be 32 bytes (256 bits), got {} bytes.",
            key_bytes.len()
        )));
    }

    let computed_md5 = md5_base64(&key_bytes);
    if computed_md5 != params.key_md5 {
        return Err(ssec_invalid_request(
            "The calculated MD5 hash of the key did not match the hash that was provided.",
        ));
    }

    let key_array: [u8; 32] = key_bytes
        .try_into()
        .map_err(|_| ssec_invalid_request("SSE-C key must be exactly 32 bytes."))?;

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
    let nonce_hash = md5_bytes(nonce_source.as_bytes());
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&nonce_hash[..12]);
    nonce
}

/// Verify SSE-C key matches the stored metadata.
///
/// Used during GetObject/HeadObject to ensure the client provided the correct key.
/// Returns 400 InvalidRequest on mismatch, consistent with AWS S3 behavior.
pub fn verify_ssec_key_match(provided_md5: &str, stored_md5: Option<&String>) -> Result<(), ApiError> {
    match stored_md5 {
        Some(stored) if stored == provided_md5 => Ok(()),
        Some(_) => Err(ssec_invalid_request(
            "The provided encryption parameters did not match the ones used originally to encrypt the object.",
        )),
        None => Err(ssec_invalid_request("Object has no stored SSE-C key metadata.")),
    }
}

/// Validate that the SSE-C headers required for reading an SSE-C encrypted object
/// are present in the request. This is used by HeadObject which does not decrypt
/// the data but still must verify the caller holds the correct key.
///
/// Performs full validation: decodes the customer key, recomputes its MD5,
/// verifies the client-provided MD5 header matches the key, then compares
/// the computed MD5 against the stored metadata. This prevents a client from
/// bypassing validation by guessing/obtaining only the stored MD5 without
/// possessing the actual encryption key.
///
/// Returns `Ok(())` if either the object is not SSE-C encrypted, or valid SSE-C
/// headers are provided and the key matches. Returns 400 InvalidRequest otherwise.
pub fn validate_ssec_for_read(
    metadata: &HashMap<String, String>,
    sse_customer_key: Option<&SSECustomerKey>,
    sse_customer_key_md5: Option<&SSECustomerKeyMD5>,
) -> Result<(), ApiError> {
    let stored_algorithm = metadata.get("x-amz-server-side-encryption-customer-algorithm");
    if stored_algorithm.is_none() {
        return Ok(());
    }

    let (key, key_md5) = match (sse_customer_key, sse_customer_key_md5) {
        (Some(k), Some(md5)) => (k, md5),
        _ => {
            return Err(ssec_invalid_request(
                "The object was stored using a form of Server Side Encryption. \
                 The correct parameters must be provided to retrieve the object.",
            ));
        }
    };

    // Full param validation: decode key, verify 32 bytes, recompute MD5
    // from actual key bytes and compare to the client-provided MD5 header.
    let algorithm = stored_algorithm.cloned().unwrap_or_else(|| DEFAULT_SSE_ALGORITHM.to_string());
    let validated = validate_ssec_params(SsecParams {
        algorithm,
        key: key.to_string(),
        key_md5: key_md5.clone(),
    })?;

    let stored_md5 = metadata.get("x-amz-server-side-encryption-customer-key-md5");
    verify_ssec_key_match(&validated.key_md5, stored_md5)
}

/// Build an `ApiError` with `InvalidRequest` (HTTP 400) for SSE-C related errors.
fn ssec_invalid_request(message: &str) -> ApiError {
    ApiError {
        code: S3ErrorCode::InvalidRequest,
        message: message.to_string(),
        source: None,
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        ApiError, DataKey, DecryptionRequest, EncryptionKeyKind, EncryptionMaterial, EncryptionRequest,
        EncryptionResolutionErrorKind, INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER,
        INTERNAL_ENCRYPTION_KEY_HEADER, INTERNAL_ENCRYPTION_KEY_ID_HEADER, KmsAction, KmsKeyAuthorizer, KmsSseDekProvider,
        KmsUnavailableError, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER,
        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
        ObjectEncryptionResolver, PrepareEncryptionRequest, ReadEncryptionMode, ReadEncryptionRequest, SSEC_ORIGINAL_SIZE_HEADER,
        SSEType, SseDekProvider, SseKmsPrincipal, SseObjectEncryptionResolver, SsecParams, StorageError, TestSseDekProvider,
        apply_managed_decryption_material, apply_managed_encryption_material, authorize_sse_kms_object_read,
        encryption_material_to_metadata, extract_server_side_encryption_from_headers, extract_ssec_params_from_headers,
        extract_ssekms_context_from_headers, generate_ssec_nonce, is_managed_sse, kms_operation_error,
        map_get_object_reader_error, mark_encrypted_multipart_metadata, md5_base64, normalize_managed_metadata,
        recode_minio_kms_context, reset_sse_dek_provider, resolve_effective_kms_key_id, sse_decryption, sse_encryption,
        sse_prepare_encryption, strip_managed_encryption_metadata, validate_sse_headers_for_read, validate_sse_headers_for_write,
        validate_ssec_for_read, validate_ssec_params, verify_ssec_key_match,
    };
    #[cfg(feature = "rio-v2")]
    use super::{
        DARE_CIPHER_AES_256_GCM, DARE_CIPHER_CHACHA20_POLY1305, MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM, SEALED_KEY_IV_SIZE,
        SEALED_KEY_SIZE, is_legacy_rustfs_managed_metadata, is_supported_sealed_object_key_cipher,
    };
    use rustfs_utils::http::headers::SSEC_ALGORITHM_HEADER;

    /// backlog#1643 PR-B0 acceptance guard: the managed-SSE classifier must
    /// have exactly one definition — in the shared encryption-keys module —
    /// so the scanner and the S3 layer can never disagree on attribution.
    /// This module may only re-export or call it.
    #[test]
    fn managed_sse_classifier_has_exactly_one_definition() {
        let classifier_fns = [
            "contains_managed_encryption_metadata",
            "normalize_managed_metadata",
            "stored_managed_encryption_key",
        ];

        let sse_src = include_str!("sse.rs");
        let shared_src =
            std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../crates/utils/src/http/object_encryption_keys.rs"))
                .expect("shared encryption-keys module should be readable");

        for name in classifier_fns {
            // Built at runtime so this test's own source cannot satisfy the scan.
            let definition = format!("fn {name}(");
            assert!(
                !sse_src.contains(&definition),
                "{name} must not be redefined in storage/sse.rs; call the shared rustfs_utils::http::object_encryption_keys implementation instead"
            );
            assert_eq!(
                shared_src.matches(&definition).count(),
                1,
                "{name} must be defined exactly once, in the shared encryption-keys module"
            );
        }
    }

    #[test]
    fn ssec_read_headers_are_sensitive() {
        let headers = super::build_ssec_read_headers(
            Some(&SSECustomerAlgorithm::from("AES256".to_string())),
            Some(&SSECustomerKey::from("dHJhbnNwb3J0LXNlY3JldA==".to_string())),
            Some(&SSECustomerKeyMD5::from("bWQ1LXNlY3JldA==".to_string())),
        );

        assert_eq!(headers.len(), 3);
        assert!(headers.values().all(HeaderValue::is_sensitive));
        let debug = format!("{headers:?}");
        assert!(!debug.contains("dHJhbnNwb3J0LXNlY3JldA=="));
        assert!(!debug.contains("bWQ1LXNlY3JldA=="));
    }

    #[test]
    fn anonymous_s3_request_builds_kms_principal() {
        let mut request = s3s::S3Request {
            input: (),
            method: http::Method::GET,
            uri: http::Uri::from_static("/bucket/object"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        request.extensions.insert(crate::storage::access::ReqInfo::default());

        let principal = SseKmsPrincipal::from_request(&request).expect("S3-boundary request should build a principal");

        assert!(principal.account.is_empty());
        assert!(principal.groups.is_none());
        assert!(principal.claims.is_empty());
    }

    #[test]
    fn parse_simple_sse_cmk_rejects_bad_keys_without_crashing() {
        // Empty / whitespace-only.
        assert!(super::parse_simple_sse_cmk("").is_err());
        assert!(super::parse_simple_sse_cmk("   ").is_err());
        // Not valid base64.
        assert!(super::parse_simple_sse_cmk("@@@not-base64@@@").is_err());
        // Valid base64 but wrong length (16 bytes).
        let short = BASE64_STANDARD.encode([1u8; 16]);
        assert!(super::parse_simple_sse_cmk(&short).is_err());
        // All-zero 32-byte key is rejected.
        let zero = BASE64_STANDARD.encode([0u8; 32]);
        assert!(super::parse_simple_sse_cmk(&zero).is_err());
    }

    #[test]
    fn kms_operation_errors_preserve_retryability_classification() {
        let unavailable = kms_operation_error(rustfs_kms::KmsError::backend_error("connection refused"));
        let corrupt = kms_operation_error(rustfs_kms::KmsError::cryptographic_error("decrypt", "authentication failed"));

        assert_eq!(unavailable.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(corrupt.code, S3ErrorCode::InternalError);
    }

    #[test]
    fn parse_simple_sse_cmk_accepts_valid_32_byte_key() {
        let mut key = [0u8; 32];
        key[0] = 7;
        let encoded = BASE64_STANDARD.encode(key);
        let got = super::parse_simple_sse_cmk(&encoded).expect("valid 32-byte key must parse");
        assert_eq!(got, key);
    }
    use aes_gcm::aead::{Aead, KeyInit};
    use aes_gcm::{Aes256Gcm, Key, Nonce};
    use async_trait::async_trait;
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use http::{HeaderMap, HeaderValue};
    use rustfs_kms::types::ObjectEncryptionContext;
    use rustfs_rio::{DecryptReader, EncryptReader};
    use rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT;
    use s3s::S3ErrorCode;
    use s3s::dto::{SSECustomerAlgorithm, SSECustomerKey, SSECustomerKeyMD5, ServerSideEncryption};
    use std::collections::HashMap;
    use std::sync::{Arc, OnceLock};
    use temp_env::async_with_vars;
    use tokio::sync::Mutex;

    static SSE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    static SSE_TEST_KMS_KEY_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();

    async fn lock_sse_test_state() -> tokio::sync::MutexGuard<'static, ()> {
        SSE_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
    }

    async fn configure_test_global_local_kms() -> Arc<rustfs_kms::KmsServiceManager> {
        let key_dir = SSE_TEST_KMS_KEY_DIR.get_or_init(|| tempfile::TempDir::new().expect("create KMS key directory"));
        let manager = rustfs_kms::init_global_kms_service_manager();
        manager
            .reconfigure(rustfs_kms::KmsConfig::local(key_dir.path().to_path_buf()).with_insecure_development_defaults())
            .await
            .expect("configure test KMS service");
        manager
    }

    #[tokio::test]
    async fn object_encryption_resolver_returns_ssec_read_material() {
        let key = [0x31; 32];
        let key_b64 = BASE64_STANDARD.encode(key);
        let key_md5 = md5_base64(key);
        let nonce = [0x42; 12];
        let metadata = HashMap::from([
            ("X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(), "AES256".to_string()),
            ("X-Amz-Server-Side-Encryption-Customer-Key-Md5".to_string(), key_md5.clone()),
            ("X-Rustfs-Encryption-Iv".to_string(), BASE64_STANDARD.encode(nonce)),
        ]);
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
        headers.insert(
            "x-amz-server-side-encryption-customer-key",
            HeaderValue::from_str(&key_b64).expect("base64 key is a valid header"),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            HeaderValue::from_str(&key_md5).expect("base64 MD5 is a valid header"),
        );

        let material = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &headers,
            })
            .await
            .expect("SSE-C material should resolve")
            .expect("SSE-C metadata should produce material");

        assert_eq!(material.key_bytes, key);
        assert_eq!(material.mode, ReadEncryptionMode::Direct { base_nonce: nonce });
    }

    #[tokio::test]
    async fn object_encryption_resolver_rejects_missing_or_invalid_ssec_algorithm() {
        let key = [0x31; 32];
        let key_b64 = BASE64_STANDARD.encode(key);
        let key_md5 = md5_base64(key);
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.clone()),
        ]);

        for algorithm in [None, Some("AES128")] {
            let mut headers = HeaderMap::new();
            if let Some(algorithm) = algorithm {
                headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static(algorithm));
            }
            headers.insert(
                "x-amz-server-side-encryption-customer-key",
                HeaderValue::from_str(&key_b64).expect("base64 key is a valid header"),
            );
            headers.insert(
                "x-amz-server-side-encryption-customer-key-md5",
                HeaderValue::from_str(&key_md5).expect("base64 MD5 is a valid header"),
            );

            let result = SseObjectEncryptionResolver
                .resolve_read_material(ReadEncryptionRequest {
                    bucket: "bucket",
                    object: "object",
                    metadata: &metadata,
                    headers: &headers,
                })
                .await;
            let error = match result {
                Err(error) => error,
                Ok(_) => panic!("missing or invalid SSE-C algorithm must fail closed"),
            };

            assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidRequest);
        }
    }

    #[tokio::test]
    async fn object_encryption_resolver_classifies_missing_ssec_key_as_invalid_request() {
        let metadata = HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string())]);
        let result = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("missing SSE-C key must fail closed"),
        };

        assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidRequest);
    }

    #[tokio::test]
    async fn object_encryption_resolver_rejects_conflicting_metadata_case_variants() {
        let metadata = HashMap::from([
            ("x-rustfs-encryption-key".to_string(), "first".to_string()),
            ("X-Rustfs-Encryption-Key".to_string(), "second".to_string()),
        ]);
        let result = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &HeaderMap::new(),
            })
            .await;
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("conflicting metadata aliases must fail closed"),
        };

        assert_eq!(error.kind(), EncryptionResolutionErrorKind::InvalidMetadata);
    }

    #[test]
    fn normalize_encryption_metadata_case_accepts_lowercase_minio_internal_keys() {
        let lowercase_key = MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_ascii_lowercase();
        let metadata = HashMap::from([(lowercase_key, "sealed-key".to_string())]);

        let normalized = super::normalize_encryption_metadata_case(&metadata).expect("metadata aliases should normalize");

        assert_eq!(
            normalized.get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER),
            Some(&"sealed-key".to_string())
        );
    }

    struct UnavailableSseDekProvider;

    #[async_trait::async_trait]
    impl SseDekProvider for UnavailableSseDekProvider {
        async fn generate_sse_dek(
            &self,
            _context: &ObjectEncryptionContext,
            _kms_key_id: &str,
        ) -> Result<(DataKey, Vec<u8>), ApiError> {
            Err(ApiError::from(StorageError::other(KmsUnavailableError)))
        }

        async fn decrypt_sse_dek(
            &self,
            _encrypted_dek: &[u8],
            _kms_key_id: &str,
            _context: &ObjectEncryptionContext,
        ) -> Result<[u8; 32], ApiError> {
            Err(ApiError::from(StorageError::other(KmsUnavailableError)))
        }
    }

    fn local_sse_master_key_b64() -> String {
        BASE64_STANDARD.encode([0x24u8; 32])
    }

    #[test]
    fn test_extract_ssec_params_from_headers() {
        let mut headers = http::HeaderMap::new();
        let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
        assert!(algo.is_none());
        assert!(key.is_none());
        assert!(md5.is_none());

        headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
        let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
        assert_eq!(algo.as_deref(), Some("AES256"));
        assert!(key.is_none());
        assert!(md5.is_none());

        headers.insert(
            "x-amz-server-side-encryption-customer-key",
            HeaderValue::from_static("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            HeaderValue::from_static("DWygnHRtgiJ77HCm+1rvHw=="),
        );
        let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
        assert_eq!(algo.as_deref(), Some("AES256"));
        assert!(key.is_some());
        assert!(md5.is_some());
    }

    #[test]
    fn test_extract_ssec_params_from_headers_rejects_invalid_utf8() {
        let mut headers = http::HeaderMap::new();
        // Header value with invalid UTF-8; to_str() will fail
        let invalid_utf8 = HeaderValue::from_bytes(b"invalid-\x80-utf8").unwrap();
        headers.insert("x-amz-server-side-encryption-customer-algorithm", invalid_utf8);
        let result = extract_ssec_params_from_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_extract_server_side_encryption_from_headers_rejects_invalid_utf8() {
        let mut headers = http::HeaderMap::new();
        let invalid_utf8 = HeaderValue::from_bytes(b"aes:kms-\x80-invalid").unwrap();
        headers.insert("x-amz-server-side-encryption", invalid_utf8);
        let result = extract_server_side_encryption_from_headers(&headers);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_extract_ssekms_context_from_headers_decodes_base64_json() {
        let mut headers = http::HeaderMap::new();
        let encoded =
            BASE64_STANDARD.encode(serde_json::to_vec(&HashMap::from([("tenant".to_string(), "alpha".to_string())])).unwrap());
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT, HeaderValue::from_str(&encoded).unwrap());

        let context = extract_ssekms_context_from_headers(&headers)
            .expect("kms context header should parse")
            .expect("kms context should exist");
        assert_eq!(context.get("tenant").map(String::as_str), Some("alpha"));
    }

    #[test]
    fn test_validate_sse_headers_for_write_rejects_algorithm_without_key() {
        let algorithm = SSECustomerAlgorithm::from("AES256".to_string());
        let result = validate_sse_headers_for_write(
            None,
            None,
            None,
            Some(&algorithm),
            None,
            None,
            true, // PutObject requires all three
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_sse_headers_for_write_rejects_algorithm_and_key_without_md5() {
        let algorithm = SSECustomerAlgorithm::from("AES256".to_string());
        let key = SSECustomerKey::from("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=".to_string());
        let result = validate_sse_headers_for_write(
            None,
            None,
            None,
            Some(&algorithm),
            Some(&key),
            None,
            true, // PutObject requires all three
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_sse_headers_for_write_rejects_invalid_sse_algorithm() {
        let bad_sse = ServerSideEncryption::from_static("aes:kms");
        let result = validate_sse_headers_for_write(Some(&bad_sse), None, None, None, None, None, true);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_validate_sse_headers_for_write_rejects_ssec_with_managed_sse() {
        let algorithm = SSECustomerAlgorithm::from("AES256".to_string());
        let key = SSECustomerKey::from("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=".to_string());
        let key_md5 = SSECustomerKeyMD5::from("DWygnHRtgiJ77HCm+1rvHw==".to_string());
        let server_side_encryption = ServerSideEncryption::from_static(ServerSideEncryption::AES256);
        let result = validate_sse_headers_for_write(
            Some(&server_side_encryption),
            None,
            None,
            Some(&algorithm),
            Some(&key),
            Some(&key_md5),
            true,
        );
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_is_managed_sse() {
        assert!(is_managed_sse(&ServerSideEncryption::from_static("AES256")));
        assert!(is_managed_sse(&ServerSideEncryption::from_static("aws:kms")));
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
        let key_md5 = md5_base64([42u8; 32]);

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
        let key_md5 = md5_base64([42u8; 32]);

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
        let key_md5 = md5_base64([42u8; 16]);

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

    #[tokio::test]
    async fn test_sse_encryption_rejects_partial_ssec_headers() {
        let bucket = "test-bucket";
        let key = "test-key";
        let sse_key = BASE64_STANDARD.encode([42u8; 32]);
        let sse_key_md5 = md5_base64([42u8; 32]);
        let content_size = 1024;

        let request_missing_md5 = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(sse_key.clone()),
            sse_customer_key_md5: None,
            content_size,
            principal: None,
        };

        let err = sse_encryption(request_missing_md5).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);

        let request_missing_key = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: None,
            sse_customer_key_md5: Some(sse_key_md5.clone()),
            content_size,
            principal: None,
        };

        let err = sse_encryption(request_missing_key).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);

        let request_missing_algorithm = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: Some(sse_key),
            sse_customer_key_md5: Some(sse_key_md5),
            content_size,
            principal: None,
        };

        let err = sse_encryption(request_missing_algorithm).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn test_sse_prepare_encryption_rejects_partial_ssec_headers() {
        let bucket = "test-bucket";
        let key = "test-key";
        let sse_key_md5 = md5_base64([42u8; 32]);

        let request_missing_algorithm = PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: Some(sse_key_md5),
            principal: None,
        };

        let err = sse_prepare_encryption(request_missing_algorithm).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_sse_headers_for_write_allows_aws_kms_without_key_id() {
        let server_side_encryption: ServerSideEncryption = "aws:kms".to_string().into();

        let result = validate_sse_headers_for_write(Some(&server_side_encryption), None, None, None, None, None, true);

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sse_prepare_encryption_rejects_ssec_headers_without_customer_key() {
        let bucket = "test-bucket";
        let key = "test-key";
        let sse_key_md5 = md5_base64([42u8; 32]);

        let request = PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: None,
            sse_customer_key_md5: Some(sse_key_md5),
            principal: None,
        };

        let error = sse_prepare_encryption(request)
            .await
            .expect_err("multipart preparation must require possession of the customer key");
        assert_eq!(error.code, S3ErrorCode::InvalidRequest);
    }

    // ------------------------------------------------------------------------
    // SSE-C Direct (default, non-`rio-v2` build) random-nonce persistence
    // ------------------------------------------------------------------------

    #[cfg(not(feature = "rio-v2"))]
    async fn ssec_direct_put_metadata(
        bucket: &str,
        key: &str,
        customer_key: &str,
        customer_key_md5: &str,
    ) -> HashMap<String, String> {
        let material = sse_encryption(EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.to_string()),
            sse_customer_key_md5: Some(customer_key_md5.to_string()),
            content_size: 128,
            principal: None,
        })
        .await
        .expect("sse-c encryption")
        .expect("sse-c material");
        assert_eq!(material.key_kind, EncryptionKeyKind::Direct);
        encryption_material_to_metadata(&material).expect("sse-c metadata should serialize")
    }

    // (a) Overwriting the same bucket/key under the same SSE-C key must persist a DIFFERENT
    // random IV each time, and each stored IV must be read back on decrypt. This is the core
    // of the fix: no (key, nonce) reuse across overwrites.
    #[cfg(not(feature = "rio-v2"))]
    #[tokio::test]
    async fn test_ssec_direct_random_nonce_is_unique_per_put_and_read_back() {
        let bucket = "bucket";
        let key = "object";
        let customer_key_bytes = [0x24u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let customer_key_md5 = md5_base64(customer_key_bytes);

        let metadata_one = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;
        let metadata_two = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

        let iv_one = metadata_one
            .get(INTERNAL_ENCRYPTION_IV_HEADER)
            .expect("first put persists a random IV");
        let iv_two = metadata_two
            .get(INTERNAL_ENCRYPTION_IV_HEADER)
            .expect("second put persists a random IV");

        assert_ne!(iv_one, iv_two, "overwriting the same object must not reuse the nonce");

        // Dual-key persistence for MinIO interop: both headers carry the same value.
        assert_eq!(metadata_one.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_one));
        assert_eq!(metadata_two.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_two));

        for (metadata, iv) in [(&metadata_one, iv_one), (&metadata_two, iv_two)] {
            let decrypted = super::apply_ssec_decryption_material(bucket, key, metadata, &customer_key, &customer_key_md5)
                .await
                .expect("sse-c decryption material");
            assert_eq!(
                BASE64_STANDARD.encode(decrypted.base_nonce),
                *iv,
                "decrypt must read the persisted random nonce back"
            );
            assert_eq!(decrypted.key_bytes, customer_key_bytes);
        }
    }

    // (b) Legacy compatibility: objects encrypted before this change carry no IV header. Decrypt
    // must fall back to the deterministic `generate_ssec_nonce(bucket, key)` they were encrypted
    // with, so previously stored objects keep decrypting.
    #[cfg(not(feature = "rio-v2"))]
    #[tokio::test]
    async fn test_ssec_direct_legacy_object_without_iv_falls_back_to_deterministic_nonce() {
        let bucket = "bucket";
        let key = "object";
        let customer_key_bytes = [0x24u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let customer_key_md5 = md5_base64(customer_key_bytes);

        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(!metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));

        let decrypted = super::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
            .await
            .expect("sse-c decryption material");

        assert_eq!(
            decrypted.base_nonce,
            generate_ssec_nonce(bucket, key),
            "legacy objects must decrypt via the deterministic nonce fallback"
        );
    }

    // (c) Byte-level round trip: data encrypted with the material's key + persisted nonce must
    // decrypt with the key + nonce resolved from the stored metadata.
    #[cfg(not(feature = "rio-v2"))]
    #[tokio::test]
    async fn test_ssec_direct_persisted_nonce_round_trips_plaintext() {
        use aes_gcm::{
            Aes256Gcm, Nonce,
            aead::{Aead, KeyInit},
        };

        let bucket = "bucket";
        let key = "object";
        let customer_key_bytes = [0x51u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let customer_key_md5 = md5_base64(customer_key_bytes);
        let plaintext = b"attack at dawn - sse-c round trip".to_vec();

        let metadata = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

        // Encrypt with the key + nonce that were persisted at PUT time.
        let enc_iv = BASE64_STANDARD
            .decode(metadata.get(INTERNAL_ENCRYPTION_IV_HEADER).expect("persisted IV"))
            .expect("valid base64 IV");
        let cipher = Aes256Gcm::new_from_slice(&customer_key_bytes).expect("cipher");
        let ciphertext = cipher
            .encrypt(&Nonce::try_from(enc_iv.as_slice()).expect("nonce"), plaintext.as_ref())
            .expect("encrypt");

        // Resolve key + nonce purely from stored metadata (as decrypt does in production).
        let decrypted_material = super::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
            .await
            .expect("sse-c decryption material");
        let dec_cipher = Aes256Gcm::new_from_slice(&decrypted_material.key_bytes).expect("cipher");
        let recovered = dec_cipher
            .decrypt(
                &Nonce::try_from(decrypted_material.base_nonce.as_slice()).expect("nonce"),
                ciphertext.as_ref(),
            )
            .expect("decrypt with resolved key + nonce");

        assert_eq!(recovered, plaintext);
    }

    // (d) Multipart: the random nonce generated at CreateMultipartUpload is persisted in the
    // session metadata, and every part resolves the SAME nonce via `sse_decryption` on that
    // session metadata. Parts must never diverge, and the value must not be deterministic.
    #[cfg(not(feature = "rio-v2"))]
    #[tokio::test]
    async fn test_ssec_direct_multipart_all_parts_share_one_persisted_nonce() {
        let bucket = "bucket";
        let key = "object";
        let customer_key_bytes = [0x33u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let customer_key_md5 = md5_base64(customer_key_bytes);

        let material = sse_prepare_encryption(PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            principal: None,
        })
        .await
        .expect("prepare ssec")
        .expect("prepare material");
        assert_eq!(material.key_kind, EncryptionKeyKind::Direct);

        let session_metadata = encryption_material_to_metadata(&material).expect("session metadata should serialize");
        let session_iv = session_metadata
            .get(INTERNAL_ENCRYPTION_IV_HEADER)
            .expect("multipart session persists a random IV")
            .clone();
        // Random, not the deterministic bucket/key derivation.
        assert_ne!(
            BASE64_STANDARD.decode(&session_iv).expect("valid IV")[..],
            generate_ssec_nonce(bucket, key)[..]
        );

        let resolve_part_nonce = |part_number: usize| {
            let session_metadata = session_metadata.clone();
            let customer_key = customer_key.clone();
            let customer_key_md5 = customer_key_md5.clone();
            async move {
                let _ = part_number;
                sse_decryption(DecryptionRequest {
                    bucket,
                    key,
                    metadata: &session_metadata,
                    sse_customer_key: Some(&customer_key),
                    sse_customer_key_md5: Some(&customer_key_md5),
                    principal: None,
                })
                .await
                .expect("part decryption")
                .expect("part material")
                .base_nonce
            }
        };

        let part_one_nonce = resolve_part_nonce(1).await;
        let part_two_nonce = resolve_part_nonce(2).await;

        assert_eq!(part_one_nonce, part_two_nonce, "all parts of one upload must share the persisted nonce");
        assert_eq!(BASE64_STANDARD.encode(part_one_nonce), session_iv);
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_sse_prepare_encryption_ssec_with_customer_key_stores_sealed_key_metadata() {
        let bucket = "test-bucket";
        let key = "test-key";
        let customer_key_bytes = [0x24u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let sse_key_md5 = md5_base64(customer_key_bytes);

        let request = PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key),
            sse_customer_key_md5: Some(sse_key_md5),
            principal: None,
        };

        let material = sse_prepare_encryption(request)
            .await
            .expect("prepare should accept full ssec headers")
            .expect("ssec metadata should be generated");
        assert_eq!(material.key_kind, EncryptionKeyKind::Object);

        let metadata = encryption_material_to_metadata(&material).expect("ssec metadata should serialize");
        assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));
    }

    #[test]
    fn test_encryption_material_to_metadata_persists_ssec_original_size() {
        let metadata = encryption_material_to_metadata(&EncryptionMaterial {
            sse_type: SSEType::SseC,
            server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AES256),
            kms_key_id: None,
            algorithm: SSECustomerAlgorithm::from("AES256".to_string()),
            key_bytes: [0u8; 32],
            base_nonce: [0u8; 12],
            encrypted_data_key: None,
            customer_key_md5: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
            original_size: Some(1024),
            key_kind: EncryptionKeyKind::Direct,
            managed_kms_context: None,
            managed_sealed_key: None,
        })
        .expect("ssec original-size metadata should serialize");

        assert_eq!(metadata.get(SSEC_ORIGINAL_SIZE_HEADER).map(String::as_str), Some("1024"));
    }

    fn material_variant(sse_type: SSEType, key_kind: EncryptionKeyKind) -> EncryptionMaterial {
        let (server_side_encryption, kms_key_id, encrypted_data_key, customer_key_md5, managed_kms_context) = match sse_type {
            SSEType::SseC => (
                ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                None,
                None,
                Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
                None,
            ),
            SSEType::SseS3 => (
                ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                None,
                Some(vec![0u8; 32]),
                None,
                None,
            ),
            SSEType::SseKms => (
                ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                Some("kms-key-1".to_string()),
                Some(vec![0u8; 32]),
                None,
                Some(HashMap::from([("app".to_string(), "test".to_string())])),
            ),
        };
        EncryptionMaterial {
            sse_type,
            server_side_encryption,
            kms_key_id,
            algorithm: SSECustomerAlgorithm::from("AES256".to_string()),
            key_bytes: [0u8; 32],
            base_nonce: [0u8; 12],
            encrypted_data_key,
            customer_key_md5,
            original_size: Some(1024),
            key_kind,
            managed_kms_context,
            managed_sealed_key: None,
        }
    }

    /// Reconciliation contract with the replication boundary (backlog#1783):
    /// every metadata key this module persists must either be remapped by the
    /// SSE-C transport table or be caught by the replication strip predicate.
    /// A new stored key that is in neither turns this test red before it can
    /// silently leak through outbound replication metadata.
    #[test]
    fn test_encryption_metadata_keys_reconcile_with_replication_transport_and_strip() {
        use rustfs_utils::http::object_encryption_keys::{
            is_replication_stripped_encryption_key, ssec_replication_transport_header,
        };

        let variants = [
            (SSEType::SseC, EncryptionKeyKind::Direct),
            (SSEType::SseS3, EncryptionKeyKind::Direct),
            (SSEType::SseKms, EncryptionKeyKind::Direct),
        ];

        for (sse_type, key_kind) in variants {
            let metadata = encryption_material_to_metadata(&material_variant(sse_type, key_kind))
                .expect("encryption material should serialize");
            assert!(!metadata.is_empty());

            for key in metadata.keys() {
                assert!(
                    ssec_replication_transport_header(key).is_some() || is_replication_stripped_encryption_key(key),
                    "stored key {key} ({sse_type:?}/{key_kind:?}) is neither transport-mapped nor stripped for replication"
                );
                if !matches!(sse_type, SSEType::SseC) {
                    // Managed SSE never takes the transport mapping; every key
                    // must be structurally stripped so envelopes cannot leave
                    // the source site.
                    assert!(
                        is_replication_stripped_encryption_key(key),
                        "managed-SSE stored key {key} ({sse_type:?}) escapes the replication strip predicate"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sse_encryption_rejects_kms_key_with_invalid_algorithm() {
        let bucket = "test-bucket";
        let key = "test-key";
        let content_size = 1024;

        let request = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: Some("AES256".to_string().into()),
            ssekms_key_id: Some("test-key".to_string()),
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size,
            principal: None,
        };

        let err = sse_encryption(request).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_sse_encryption_rejects_kms_key_without_algorithm() {
        let bucket = "test-bucket";
        let key = "test-key";
        let content_size = 1024;

        let request = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: Some("test-key".to_string()),
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size,
            principal: None,
        };

        let err = sse_encryption(request).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_sse_encryption_rejects_conflict_between_kms_and_ssec() {
        let bucket = "test-bucket";
        let key = "test-key";
        let content_size = 1024;
        let sse_key = BASE64_STANDARD.encode([42u8; 32]);
        let sse_key_md5 = md5_base64([42u8; 32]);

        let request = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: Some("aws:kms".to_string().into()),
            ssekms_key_id: Some("test-key".to_string()),
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(sse_key),
            sse_customer_key_md5: Some(sse_key_md5),
            content_size,
            principal: None,
        };

        let err = sse_encryption(request).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_resolve_effective_kms_key_id_ignores_bucket_default_for_explicit_sse_s3() {
        let effective_sse = ServerSideEncryption::from_static(ServerSideEncryption::AES256);

        let kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), None, || Some("bucket-default".to_string()));

        assert_eq!(kms_key_id, None);
    }

    #[test]
    fn test_resolve_effective_kms_key_id_uses_bucket_default_for_sse_kms() {
        let effective_sse = ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS);

        let kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), None, || Some("bucket-default".to_string()));

        assert_eq!(kms_key_id.as_deref(), Some("bucket-default"));
    }

    #[tokio::test]
    async fn test_sse_encryption_persists_aws_kms_header_for_kms_objects() {
        let metadata = encryption_material_to_metadata(&EncryptionMaterial {
            sse_type: SSEType::SseKms,
            server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
            kms_key_id: Some("test-key".to_string()),
            algorithm: SSECustomerAlgorithm::from(ServerSideEncryption::AWS_KMS.to_string()),
            key_bytes: [7u8; 32],
            base_nonce: [9u8; 12],
            encrypted_data_key: Some(vec![1, 2, 3, 4]),
            customer_key_md5: None,
            original_size: Some(1024),
            key_kind: EncryptionKeyKind::Direct,
            managed_kms_context: None,
            managed_sealed_key: None,
        })
        .expect("managed SSE metadata should serialize");

        assert_eq!(metadata.get("x-amz-server-side-encryption").map(String::as_str), Some("aws:kms"));
        assert_eq!(
            metadata
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(String::as_str),
            Some("test-key")
        );
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_sse_kms_roundtrip_persists_and_uses_minio_context() {
        use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
        let _guard = lock_sse_test_state().await;

        reset_sse_dek_provider();
        let manager = configure_test_global_local_kms().await;
        manager
            .get_encryption_service()
            .await
            .expect("encryption service should exist")
            .create_key(CreateKeyRequest {
                key_name: Some("kms-test".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("kms test key should be created");

        let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
            .await
            .expect("kms provider should initialize from the configured test manager");
        super::set_sse_dek_provider_for_test(Arc::new(provider));

        let client_context = HashMap::from([("tenant".to_string(), "alpha".to_string())]);
        let request = EncryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)),
            ssekms_key_id: Some("kms-test".to_string()),
            ssekms_context: Some(client_context.clone()),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 4096,
            principal: None,
        };

        let material = sse_encryption(request)
            .await
            .expect("sse-kms encryption")
            .expect("managed sse-kms material");
        let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
        let encoded_context = metadata
            .get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
            .expect("minio kms context header should exist");
        let decoded_context: HashMap<String, String> =
            serde_json::from_slice(&BASE64_STANDARD.decode(encoded_context).expect("decode base64 context"))
                .expect("decode json context");
        assert_eq!(decoded_context, client_context);

        let decrypted = sse_decryption(DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        })
        .await
        .expect("sse-kms decryption should succeed")
        .expect("sse-kms material should exist");
        assert_eq!(decrypted.key_bytes, material.key_bytes);

        let mut wrong_metadata = metadata.clone();
        wrong_metadata.insert(
            MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
            BASE64_STANDARD.encode(serde_json::to_vec(&HashMap::from([("tenant".to_string(), "beta".to_string())])).unwrap()),
        );
        let err = sse_decryption(DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &wrong_metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        })
        .await
        .expect_err("mismatched kms context should fail");
        assert_eq!(err.code, S3ErrorCode::InternalError);
        assert_eq!(err.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        assert_eq!(super::kms_data_plane_error_class(&err), "context_mismatch");

        manager.stop().await.expect("kms service should stop cleanly");
        reset_sse_dek_provider();
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_encryption_material_to_metadata_persists_minio_managed_headers() {
        let encoded_nonce = BASE64_STANDARD.encode([9u8; 12]);
        let encoded_key = BASE64_STANDARD.encode([1u8, 2, 3, 4]);
        let metadata = encryption_material_to_metadata(&EncryptionMaterial {
            sse_type: SSEType::SseKms,
            server_side_encryption: ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
            kms_key_id: Some("test-key".to_string()),
            algorithm: SSECustomerAlgorithm::from(ServerSideEncryption::AWS_KMS.to_string()),
            key_bytes: [7u8; 32],
            base_nonce: [9u8; 12],
            encrypted_data_key: Some(vec![1, 2, 3, 4]),
            customer_key_md5: None,
            original_size: Some(1024),
            key_kind: EncryptionKeyKind::Direct,
            managed_kms_context: None,
            managed_sealed_key: None,
        })
        .expect("managed SSE metadata should serialize");

        assert_eq!(
            metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str),
            Some(encoded_nonce.as_str())
        );
        assert_eq!(
            metadata
                .get(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
                .map(String::as_str),
            Some(encoded_key.as_str())
        );
        assert_eq!(
            metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER).map(String::as_str),
            Some("test-key")
        );
        assert_eq!(
            metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
            Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
        );
    }

    #[tokio::test]
    async fn test_sse_encryption_omits_kms_header_for_sse_s3_objects() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        let local_sse_master_key = local_sse_master_key_b64();

        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
            ],
            async {
                let request = EncryptionRequest {
                    bucket: "test-bucket",
                    key: "test-key",
                    server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 1024,
                    principal: None,
                };

                let material = sse_encryption(request).await.expect("sse-s3 encryption should succeed");
                let material = material.expect("managed sse-s3 encryption should return material");
                let metadata = encryption_material_to_metadata(&material).expect("managed SSE-S3 metadata should serialize");

                assert_eq!(material.kms_key_id.as_deref(), Some("default"));
                assert_eq!(metadata.get("x-amz-server-side-encryption").map(String::as_str), Some("AES256"));
                assert!(!metadata.contains_key("x-amz-server-side-encryption-aws-kms-key-id"));
                assert_eq!(metadata.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER).map(String::as_str), Some("default"));
            },
        )
        .await;

        reset_sse_dek_provider();
    }

    #[test]
    fn test_strip_managed_encryption_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), "source-key-md5".to_string());
        metadata.insert(SSEC_ORIGINAL_SIZE_HEADER.to_string(), "123".to_string());
        metadata.insert("x-rustfs-encryption-key".to_string(), "encrypted_key".to_string());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), "sealed".to_string());
        metadata.insert("content-type".to_string(), "text/plain".to_string());

        strip_managed_encryption_metadata(&mut metadata);

        assert!(!metadata.contains_key("x-amz-server-side-encryption"));
        assert!(!metadata.contains_key("x-amz-server-side-encryption-customer-algorithm"));
        assert!(!metadata.contains_key("x-amz-server-side-encryption-customer-key-md5"));
        assert!(!metadata.contains_key(SSEC_ORIGINAL_SIZE_HEADER));
        assert!(!metadata.contains_key("x-rustfs-encryption-key"));
        assert!(!metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER));
        assert!(metadata.contains_key("content-type"));
    }

    /// A copy destination must not inherit any key that `is_object_encryption_marker`
    /// accepts as proof the payload is encrypted.
    ///
    /// Guards this chain: `ObjectInfo::is_encrypted` is keyed on that predicate, so a
    /// single leftover marker makes a plaintext destination report itself encrypted.
    /// `object_api::readers` then takes its encrypted branch and demands read material,
    /// but the material itself was stripped, so `sse_decryption` reports no encryption
    /// and the read fails with "encrypted object metadata is incomplete" — a destination
    /// that CopyObject wrote successfully becomes permanently unreadable.
    #[test]
    fn test_strip_managed_encryption_metadata_clears_encryption_markers() {
        let mut metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
            ("x-amz-server-side-encryption-aws-kms-key-id".to_string(), "source-key".to_string()),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "source-key".to_string()),
            (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "wrapped-dek".to_string()),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "base-nonce".to_string()),
            ("content-type".to_string(), "text/plain".to_string()),
        ]);

        strip_managed_encryption_metadata(&mut metadata);

        let inherited: Vec<&str> = metadata
            .keys()
            .filter(|key| rustfs_utils::http::is_object_encryption_marker(key))
            .map(String::as_str)
            .collect();
        assert!(inherited.is_empty(), "copy destination inherited encryption markers: {inherited:?}");
        assert!(metadata.contains_key("content-type"));
    }

    /// A same-name CopyObject is normally serviced as a metadata-only update that reuses the
    /// stored ciphertext, while the handler rebuilds the destination metadata around a *fresh*
    /// DEK. `ObjectInfo::is_encrypted` — i.e. `is_object_encryption_marker` — is the guard that
    /// keeps that shortcut away from encrypted objects (see the `metadata_only` decision in
    /// `rustfs/src/app/object_usecase.rs`). Any encryption flavour whose headers
    /// `strip_managed_encryption_metadata` drops must therefore also be *detected* there — a
    /// flavour that is stripped but not detected would resurrect "new DEK + old ciphertext",
    /// which leaves the object permanently undecryptable.
    #[test]
    fn every_strippable_encryption_shape_is_detected_as_encrypted() {
        let shapes: [(&str, Vec<(&str, &str)>); 4] = [
            (
                "sse-s3",
                vec![
                    ("x-amz-server-side-encryption", "AES256"),
                    (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, "sealed"),
                    (MINIO_INTERNAL_ENCRYPTION_IV_HEADER, "iv"),
                ],
            ),
            (
                "sse-kms",
                vec![
                    ("x-amz-server-side-encryption", "aws:kms"),
                    (MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, "sealed"),
                    (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, "key-1"),
                ],
            ),
            (
                "legacy rustfs managed",
                vec![
                    ("x-amz-server-side-encryption", "AES256"),
                    (INTERNAL_ENCRYPTION_KEY_HEADER, "wrapped"),
                    (INTERNAL_ENCRYPTION_IV_HEADER, "iv"),
                ],
            ),
            (
                "sse-c",
                vec![
                    (SSEC_ALGORITHM_HEADER, "AES256"),
                    (MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, "sealed"),
                ],
            ),
        ];

        for (label, entries) in shapes {
            let metadata: HashMap<String, String> = entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect();

            assert!(
                metadata
                    .keys()
                    .any(|key| rustfs_utils::http::is_object_encryption_marker(key)),
                "{label}: an encrypted object must be detected, otherwise a same-name copy would re-key it \
                 without rewriting the ciphertext"
            );

            let mut stripped = metadata.clone();
            strip_managed_encryption_metadata(&mut stripped);
            assert_ne!(
                stripped, metadata,
                "{label}: the copy path strips this shape's encryption metadata, so the destination cannot \
                 reuse the source ciphertext"
            );
        }
    }

    #[test]
    fn plain_object_metadata_is_not_flagged_as_encrypted() {
        // The metadata-only self-copy shortcut must stay available for unencrypted objects.
        let metadata: HashMap<String, String> = [("content-type", "text/plain"), ("x-amz-meta-stage", "before")]
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();

        assert!(
            !metadata
                .keys()
                .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
        );
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_legacy_managed_metadata_excludes_sealed_keys() {
        let legacy_metadata = HashMap::from([
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
        ]);
        assert!(is_legacy_rustfs_managed_metadata(&legacy_metadata));

        let sealed_metadata = HashMap::from([
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
            (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), "sealed-key".to_string()),
        ]);
        assert!(!is_legacy_rustfs_managed_metadata(&sealed_metadata));
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_normalize_managed_metadata_accepts_minio_only_headers() {
        let metadata = HashMap::from([
            (
                MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
                BASE64_STANDARD.encode(b"encrypted-key"),
            ),
            (MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([0x11u8; 12])),
            (
                MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
            ),
            (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
        ]);

        let normalized = normalize_managed_metadata(&metadata, Some(recode_minio_kms_context));

        assert_eq!(
            normalized.get(INTERNAL_ENCRYPTION_KEY_HEADER),
            Some(&BASE64_STANDARD.encode(b"encrypted-key"))
        );
        assert_eq!(normalized.get(INTERNAL_ENCRYPTION_IV_HEADER), Some(&BASE64_STANDARD.encode([0x11u8; 12])));
        assert_eq!(
            normalized.get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER),
            Some(&MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string())
        );
        assert_eq!(normalized.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER), Some(&"default".to_string()));
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_managed_sse_rio_v2_uses_object_key_metadata_roundtrip() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        if let Some(manager) = rustfs_kms::get_global_kms_service_manager() {
            let _ = manager.stop().await;
        }
        let local_sse_master_key = local_sse_master_key_b64();

        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
            ],
            async {
                let request = EncryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 4096,
                    principal: None,
                };

                let material = sse_encryption(request)
                    .await
                    .expect("managed sse encryption")
                    .expect("managed sse material");
                assert_eq!(material.key_kind, EncryptionKeyKind::Object);

                let metadata = encryption_material_to_metadata(&material).expect("managed SSE metadata should serialize");
                assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER));
                assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));

                let sealing_iv = metadata
                    .get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER)
                    .expect("minio sealing iv should be stored");
                let sealed_key = metadata
                    .get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
                    .expect("minio sealed key should be stored");
                assert_eq!(BASE64_STANDARD.decode(sealing_iv).expect("decode iv").len(), SEALED_KEY_IV_SIZE);
                assert_eq!(BASE64_STANDARD.decode(sealed_key).expect("decode sealed key").len(), SEALED_KEY_SIZE);

                let decrypted = sse_decryption(DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                })
                .await
                .expect("managed sse decryption")
                .expect("managed decryption material");

                assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
                assert_eq!(decrypted.key_bytes, material.key_bytes);

                let resolved = SseObjectEncryptionResolver
                    .resolve_read_material(ReadEncryptionRequest {
                        bucket: "bucket",
                        object: "object",
                        metadata: &metadata,
                        headers: &HeaderMap::new(),
                    })
                    .await
                    .expect("managed resolver")
                    .expect("managed material");
                assert_eq!(resolved.mode, ReadEncryptionMode::Object);
                assert_eq!(resolved.key_bytes, material.key_bytes);
            },
        )
        .await;

        reset_sse_dek_provider();
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_supported_sealed_object_key_cipher_accepts_current_minio_fixture_value() {
        assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_AES_256_GCM));
        assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_CHACHA20_POLY1305));
        assert!(!is_supported_sealed_object_key_cipher(0x02));
    }

    #[cfg(feature = "rio-v2")]
    #[tokio::test]
    async fn test_ssec_rio_v2_uses_sealed_object_key_metadata_roundtrip() {
        let customer_key_bytes = [0x42u8; 32];
        let customer_key = BASE64_STANDARD.encode(customer_key_bytes);
        let customer_key_md5 = md5_base64(customer_key_bytes);

        let material = sse_encryption(EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        })
        .await
        .expect("sse-c encryption")
        .expect("sse-c material");

        assert_eq!(material.key_kind, EncryptionKeyKind::Object);

        let metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
        assert_eq!(
            metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
            Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
        );
        assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));

        let decrypted = sse_decryption(DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        })
        .await
        .expect("sse-c decryption")
        .expect("sse-c decryption material");

        assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
        assert_eq!(decrypted.key_bytes, material.key_bytes);

        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
        headers.insert(
            "x-amz-server-side-encryption-customer-key",
            HeaderValue::from_str(&customer_key).expect("customer key header"),
        );
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            HeaderValue::from_str(&customer_key_md5).expect("customer key MD5 header"),
        );
        let resolved = SseObjectEncryptionResolver
            .resolve_read_material(ReadEncryptionRequest {
                bucket: "bucket",
                object: "object",
                metadata: &metadata,
                headers: &headers,
            })
            .await
            .expect("SSE-C resolver")
            .expect("SSE-C material");
        assert_eq!(resolved.mode, ReadEncryptionMode::Object);
        assert_eq!(resolved.key_bytes, material.key_bytes);
    }

    #[cfg(feature = "rio-v2")]
    #[test]
    fn test_mark_encrypted_multipart_metadata_sets_minio_marker() {
        let mut metadata = HashMap::new();
        mark_encrypted_multipart_metadata(&mut metadata);
        assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER));
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

    fn multipart_ssec_request(key_byte: u8) -> EncryptionRequest<'static> {
        let key_bytes = [key_byte; 32];
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(BASE64_STANDARD.encode(key_bytes)),
            sse_customer_key_md5: Some(md5_base64(key_bytes)),
            content_size: 1,
            principal: None,
        }
    }

    fn multipart_ssec_metadata(key_byte: u8) -> HashMap<String, String> {
        let key_bytes = [key_byte; 32];
        HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), md5_base64(key_bytes)),
        ])
    }

    #[test]
    fn test_validate_multipart_ssec_exact_match() {
        assert!(
            multipart_ssec_request(42)
                .validate_multipart_ssec(&multipart_ssec_metadata(42))
                .is_ok()
        );
    }

    #[test]
    fn test_validate_multipart_ssec_rejects_wrong_key_without_leaking_it() {
        let request = multipart_ssec_request(43);
        let encoded_key = request.sse_customer_key.clone().expect("fixture key");
        let encoded_md5 = request.sse_customer_key_md5.clone().expect("fixture MD5");

        let error = request
            .validate_multipart_ssec(&multipart_ssec_metadata(42))
            .expect_err("wrong key must fail");

        assert_eq!(error.code, S3ErrorCode::InvalidRequest);
        assert!(!error.message.contains(&encoded_key));
        assert!(!error.message.contains(&encoded_md5));
    }

    #[test]
    fn test_validate_multipart_ssec_rejects_missing_or_unexpected_parameters() {
        let no_ssec = EncryptionRequest {
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            ..multipart_ssec_request(42)
        };
        assert_eq!(
            no_ssec
                .validate_multipart_ssec(&multipart_ssec_metadata(42))
                .expect_err("SSE-C session requires all parameters")
                .code,
            S3ErrorCode::InvalidRequest
        );
        assert_eq!(
            multipart_ssec_request(42)
                .validate_multipart_ssec(&HashMap::new())
                .expect_err("plaintext session rejects SSE-C parameters")
                .code,
            S3ErrorCode::InvalidRequest
        );
        let incomplete_session = HashMap::from([(
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            multipart_ssec_request(42).sse_customer_key_md5.expect("fixture MD5"),
        )]);
        assert_eq!(
            multipart_ssec_request(42)
                .validate_multipart_ssec(&incomplete_session)
                .expect_err("incomplete stored SSE-C metadata must fail closed")
                .code,
            S3ErrorCode::InvalidRequest
        );
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
        let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(&context, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // 3. Prepare test data (plaintext)
        let plaintext = b"Hello, World! This is a test message for encryption and decryption.";
        println!("Original plaintext: {:?}", String::from_utf8_lossy(plaintext));
        println!("Plaintext length: {} bytes", plaintext.len());

        // 4. Encrypt with EncryptReader.
        let plaintext_reader = Cursor::new(plaintext.to_vec());
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

        // 5. Decrypt with DecryptReader.
        let encrypted_reader = Cursor::new(encrypted_data);
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
        let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());

        let (data_key, _encrypted_dek) = provider
            .generate_sse_dek(&context, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        // Create 1MB of test data
        let plaintext_size = 1024 * 1024; // 1MB
        let plaintext: Vec<u8> = (0..plaintext_size).map(|i| (i % 256) as u8).collect();
        println!("Testing with {} bytes of data", plaintext.len());

        // Encrypt.
        let plaintext_reader = Cursor::new(plaintext.clone());
        let mut encrypt_reader = EncryptReader::new(plaintext_reader, data_key.plaintext_key, data_key.nonce);

        let mut encrypted_data = Vec::new();
        encrypt_reader
            .read_to_end(&mut encrypted_data)
            .await
            .expect("Failed to encrypt large data");

        println!("Encrypted {} bytes to {} bytes", plaintext.len(), encrypted_data.len());

        // Decrypt.
        let encrypted_reader = Cursor::new(encrypted_data);
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
        let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());

        // Generate two different keys (with different nonces)
        let (data_key1, _) = provider
            .generate_sse_dek(&context, kms_key_id)
            .await
            .expect("Failed to generate DEK 1");

        let (data_key2, _) = provider
            .generate_sse_dek(&context, kms_key_id)
            .await
            .expect("Failed to generate DEK 2");

        // Verify nonces are different
        assert_ne!(data_key1.nonce, data_key2.nonce, "Different keys should have different nonces");

        // Same plaintext
        let plaintext = b"Same plaintext";

        // Encrypt with first key.
        let reader1 = Cursor::new(plaintext.to_vec());
        let mut encrypt_reader1 = EncryptReader::new(reader1, data_key1.plaintext_key, data_key1.nonce);
        let mut encrypted1 = Vec::new();
        encrypt_reader1.read_to_end(&mut encrypted1).await.unwrap();

        // Encrypt with second key.
        let reader2 = Cursor::new(plaintext.to_vec());
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

    #[test]
    fn test_encrypt_dek_writes_json_with_random_nonces() {
        let dek = [0x11u8; 32];
        let cmk = [0x22u8; 32];

        let encrypted_a = TestSseDekProvider::encrypt_dek(dek, cmk).expect("first DEK wrap should succeed");
        let encrypted_b = TestSseDekProvider::encrypt_dek(dek, cmk).expect("second DEK wrap should succeed");

        let envelope_a: super::LocalSseDekEnvelope =
            serde_json::from_str(&encrypted_a).expect("first wrapped DEK should be a JSON envelope");
        let envelope_b: super::LocalSseDekEnvelope =
            serde_json::from_str(&encrypted_b).expect("second wrapped DEK should be a JSON envelope");

        assert_eq!(envelope_a.version, super::LOCAL_SSE_DEK_FORMAT_VERSION);
        assert_eq!(envelope_b.version, super::LOCAL_SSE_DEK_FORMAT_VERSION);
        assert_ne!(envelope_a.nonce, envelope_b.nonce, "each DEK wrap should use a distinct nonce");
        assert_eq!(
            TestSseDekProvider::decrypt_dek(&encrypted_a, cmk).expect("first JSON envelope should decrypt"),
            dek
        );
    }

    #[test]
    fn test_decrypt_dek_accepts_legacy_zero_nonce_payload() {
        let dek = [0x33u8; 32];
        let cmk = [0x44u8; 32];
        let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(cmk));
        let legacy_nonce = Nonce::from([0u8; 12]);
        let ciphertext = cipher
            .encrypt(&legacy_nonce, dek.as_slice())
            .expect("legacy wrap should succeed");
        let legacy_payload = format!("{}:{}", BASE64_STANDARD.encode(legacy_nonce), BASE64_STANDARD.encode(ciphertext));

        let decrypted = TestSseDekProvider::decrypt_dek(&legacy_payload, cmk).expect("legacy payload should remain decryptable");
        assert_eq!(decrypted, dek);
    }

    #[test]
    fn test_decrypt_dek_rejects_unknown_json_version() {
        let envelope = serde_json::json!({
            "version": super::LOCAL_SSE_DEK_FORMAT_VERSION + 1,
            "nonce": BASE64_STANDARD.encode([0u8; 12]),
            "ciphertext": BASE64_STANDARD.encode([0u8; 48]),
        })
        .to_string();

        let error = TestSseDekProvider::decrypt_dek(&envelope, [0x55u8; 32])
            .expect_err("unknown JSON envelope versions must fail closed");
        assert_eq!(error.code, S3ErrorCode::InternalError);
        assert_eq!(error.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        let source = error
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::Io(io_error) if io_error
            .to_string()
            .contains("Unsupported encrypted DEK format version")));
    }

    #[tokio::test]
    async fn test_sse_encryption_fails_closed_without_local_sse_master_key() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
                ("RUSTFS_SSE_S3_MASTER_KEY", None::<&str>),
            ],
            async {
                let err = sse_encryption(EncryptionRequest {
                    bucket: "test-bucket",
                    key: "test-key",
                    server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 1024,
                    principal: None,
                })
                .await
                .expect_err("SSE-S3 should fail closed without a configured local master key");

                assert!(err.message.contains("RUSTFS_SSE_S3_MASTER_KEY"));
                assert_eq!(
                    err.code,
                    S3ErrorCode::InvalidRequest,
                    "missing local SSE master key is a configuration error, not a 500 (rustfs#4844)"
                );
            },
        )
        .await;
        reset_sse_dek_provider();
    }

    #[tokio::test]
    async fn test_sse_encryption_fails_closed_with_invalid_local_sse_master_key() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some("not-base64")),
            ],
            async {
                let err = sse_encryption(EncryptionRequest {
                    bucket: "test-bucket",
                    key: "test-key",
                    server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 1024,
                    principal: None,
                })
                .await
                .expect_err("SSE-S3 should fail closed with an invalid local master key");

                assert!(err.message.contains("valid base64"));
                assert_eq!(
                    err.code,
                    S3ErrorCode::InvalidRequest,
                    "invalid local SSE master key is a configuration error, not a 500 (rustfs#4844)"
                );
            },
        )
        .await;
        reset_sse_dek_provider();
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
        let context = ObjectEncryptionContext::new(bucket.to_string(), key.to_string());

        // 1. Generate DEK and get encrypted DEK
        let (data_key, encrypted_dek) = provider
            .generate_sse_dek(&context, kms_key_id)
            .await
            .expect("Failed to generate DEK");

        let original_plaintext_key = data_key.plaintext_key;
        let original_nonce = data_key.nonce;

        // 2. Simulate storing encrypted_dek and nonce in metadata
        // In real scenario, nonce would be stored separately in metadata

        // 3. Later, decrypt the DEK
        let decrypted_plaintext_key = provider
            .decrypt_sse_dek(&encrypted_dek, kms_key_id, &context)
            .await
            .expect("Failed to decrypt DEK");

        // 4. Verify decrypted key matches original
        assert_eq!(
            decrypted_plaintext_key, original_plaintext_key,
            "Decrypted DEK should match original plaintext key"
        );

        // 5. Use decrypted key to encrypt/decrypt data
        let plaintext = b"Test data with decrypted DEK";

        // Encrypt with original key.
        let reader = Cursor::new(plaintext.to_vec());
        let mut encrypt_reader = EncryptReader::new(reader, original_plaintext_key, original_nonce);
        let mut encrypted_data = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted_data).await.unwrap();

        // Decrypt with recovered key (simulating GET operation).
        let reader = Cursor::new(encrypted_data);
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

    #[tokio::test]
    async fn test_managed_decryption_selects_provider_from_persisted_dek() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();
        let manager = configure_test_global_local_kms().await;

        let local_master_key = [7u8; 32];
        let local_provider = TestSseDekProvider::new_with_key(local_master_key);
        let context = ObjectEncryptionContext::new("bucket".to_string(), "object".to_string());
        let (data_key, encrypted_dek) = local_provider
            .generate_sse_dek(&context, "legacy-local-key")
            .await
            .expect("generate beta.5 local DEK");

        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some(BASE64_STANDARD.encode(local_master_key))),
            ],
            async {
                let metadata = HashMap::from([
                    ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AWS_KMS.to_string()),
                    (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(encrypted_dek)),
                    (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode(data_key.nonce)),
                    (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "legacy-local-key".to_string()),
                ]);

                let material = apply_managed_decryption_material("bucket", "object", &metadata, None)
                    .await
                    .expect("legacy local DEK should not be routed to the running KMS")
                    .expect("managed metadata should produce decryption material");
                assert_eq!(material.key_bytes, data_key.plaintext_key);
                assert_eq!(material.sse_type, SSEType::SseKms);
            },
        )
        .await;

        manager.stop().await.expect("stop test KMS service");
        reset_sse_dek_provider();

        let kms_envelope = br#"{
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AES256.to_string()),
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(kms_envelope)),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([0x14; 12])),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "test-key-id".to_string()),
        ]);
        let error = match apply_managed_decryption_material("bucket", "object", &metadata, None).await {
            Ok(_) => panic!("KMS envelope must not fall back to the local provider"),
            Err(error) => error,
        };
        assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

        reset_sse_dek_provider();
    }

    /// Regression test for "local provider cached → dynamically enable KMS → decrypt KMS envelope".
    ///
    /// Verifies that a `TestSseDekProvider` previously cached in `GLOBAL_SSE_DEK_PROVIDER`
    /// is NEVER selected to unwrap a KMS data-key envelope. The KMS-envelope branch must
    /// read `GLOBAL_KMS_DEK_PROVIDER` (or fall back to `KmsSseDekProvider::new()`), not
    /// the local-provider cache.
    #[tokio::test]
    async fn test_kms_envelope_never_routes_to_cached_local_provider() {
        let _guard = lock_sse_test_state().await;
        reset_sse_dek_provider();

        // 1. Populate GLOBAL_SSE_DEK_PROVIDER with a local provider — the kind
        //    that `get_local_sse_dek_provider` would cache when KMS is absent.
        let local_master_key = [0xAAu8; 32];
        *super::GLOBAL_SSE_DEK_PROVIDER
            .write()
            .expect("write local provider into local cache") = Some(Arc::new(TestSseDekProvider::new_with_key(local_master_key)));

        // 2. Start a KMS service (dynamic enable).
        let manager = configure_test_global_local_kms().await;

        // 3. Construct a KMS JSON envelope — the persisted format of a KMS-wrapped DEK.
        //    is_data_key_envelope() will return true for this payload.
        let kms_envelope = br#"{
            "key_id": "envelope-key",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [10, 20, 30, 40],
            "nonce": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AES256.to_string()),
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode(kms_envelope)),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([0x14; 12])),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "envelope-key".to_string()),
        ]);

        // 4. Decrypt the KMS envelope. Before the fix, this would pick up the cached
        //    TestSseDekProvider from GLOBAL_SSE_DEK_PROVIDER and either panic (wrong
        //    format) or produce garbage. After the fix, it routes to KmsSseDekProvider,
        //    which fails because the envelope contains dummy encrypted bytes that the
        //    test KMS cannot decrypt — but crucially the error code is NOT a local-
        //    provider error.
        let error = match apply_managed_decryption_material("bucket", "object", &metadata, None).await {
            Ok(_) => panic!("dummy KMS envelope must not produce valid decryption material"),
            Err(error) => error,
        };
        // The KMS service was reached (ServiceUnavailable or the KMS's own decrypt
        // failure), not a local-provider format error.
        assert!(
            error.code == S3ErrorCode::ServiceUnavailable || error.code == S3ErrorCode::InternalError,
            "KMS envelope must be routed to KMS provider, not local; got code {:?} msg '{}'",
            error.code,
            error.message,
        );

        manager.stop().await.expect("stop test KMS service");
        reset_sse_dek_provider();
    }

    #[tokio::test]
    async fn test_managed_encryption_preserves_provider_unavailable_error() {
        let _guard = lock_sse_test_state().await;
        let manager = rustfs_kms::init_global_kms_service_manager();
        manager.stop().await.expect("stop global KMS service");
        reset_sse_dek_provider();
        *super::GLOBAL_SSE_DEK_PROVIDER.write().expect("update SSE DEK provider cache") =
            Some(Arc::new(UnavailableSseDekProvider));

        let error = match apply_managed_encryption_material(
            "bucket",
            "object",
            ServerSideEncryption::from_static(ServerSideEncryption::AES256),
            None,
            None,
            0,
            None,
        )
        .await
        {
            Ok(_) => panic!("provider unavailability must fail managed encryption"),
            Err(error) => error,
        };
        assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

        let metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AES256.to_string()),
            (
                INTERNAL_ENCRYPTION_KEY_HEADER.to_string(),
                BASE64_STANDARD.encode(b"local-provider-format"),
            ),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([0x14; 12])),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "test-key-id".to_string()),
        ]);
        let error = match apply_managed_decryption_material("bucket", "object", &metadata, None).await {
            Ok(_) => panic!("provider unavailability must fail managed decryption"),
            Err(error) => error,
        };
        assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

        reset_sse_dek_provider();
    }

    #[tokio::test]
    async fn test_kms_sse_dek_provider_uses_latest_reconfigured_service() {
        use base64::Engine as _;
        use rustfs_kms::config::KmsConfig;
        let _guard = lock_sse_test_state().await;

        let manager = Arc::new(rustfs_kms::KmsServiceManager::new());

        manager
            .reconfigure(KmsConfig::static_kms("first-key".to_string(), BASE64_STANDARD.encode([0x11; 32])))
            .await
            .expect("first KMS reconfigure should succeed");

        let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
            .await
            .expect("provider should initialize");
        let context = ObjectEncryptionContext::new("bucket".to_string(), "object".to_string());
        provider
            .generate_sse_dek(&context, "first-key")
            .await
            .expect("provider should use the initial service");

        manager
            .reconfigure(KmsConfig::static_kms("second-key".to_string(), BASE64_STANDARD.encode([0x22; 32])))
            .await
            .expect("second KMS reconfigure should succeed");

        provider
            .generate_sse_dek(&context, "second-key")
            .await
            .expect("provider should resolve the latest reconfigured service");

        manager.stop().await.expect("kms service should stop cleanly");
        let generate_error = provider
            .generate_sse_dek(&context, "second-key")
            .await
            .expect_err("stopped KMS must reject data-key generation");
        assert_eq!(generate_error.code, S3ErrorCode::ServiceUnavailable);
        let decrypt_error = provider
            .decrypt_sse_dek(b"{}", "second-key", &context)
            .await
            .expect_err("stopped KMS must reject data-key decryption");
        assert_eq!(decrypt_error.code, S3ErrorCode::ServiceUnavailable);
        #[cfg(feature = "rio-v2")]
        {
            let legacy_error = provider
                .decrypt_legacy_sse_dek(b"{}", "second-key", &context)
                .await
                .expect_err("stopped KMS must reject legacy data-key decryption");
            assert_eq!(legacy_error.code, S3ErrorCode::ServiceUnavailable);
        }
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

    #[test]
    fn test_verify_ssec_key_match_returns_invalid_request() {
        let stored = "stored_md5".to_string();
        let err = verify_ssec_key_match("wrong_md5", Some(&stored)).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_verify_ssec_key_match_no_stored_returns_invalid_request() {
        let err = verify_ssec_key_match("any_md5", None).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_ssec_for_read_non_encrypted_object() {
        let metadata = HashMap::new();
        let result = validate_ssec_for_read(&metadata, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ssec_for_read_missing_customer_key() {
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        metadata.insert(
            "x-amz-server-side-encryption-customer-key-md5".to_string(),
            "DWygnHRtgiJ77HCm+1rvHw==".to_string(),
        );

        let err = validate_ssec_for_read(&metadata, None, None).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_ssec_for_read_wrong_key() {
        // Key A is used to "encrypt" the object (stored MD5 is from key A).
        let key_a = [42u8; 32];
        let stored_md5 = md5_base64(key_a);

        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), stored_md5);

        // Key B is a different key; its MD5 won't match stored MD5.
        let key_b = [99u8; 32];
        let key_b_b64 = BASE64_STANDARD.encode(key_b);
        let key_b_md5 = md5_base64(key_b);

        let err = validate_ssec_for_read(&metadata, Some(&key_b_b64), Some(&key_b_md5)).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_ssec_for_read_correct_key() {
        let key_bytes = [42u8; 32];
        let key_b64 = BASE64_STANDARD.encode(key_bytes);
        let key_md5 = md5_base64(key_bytes);

        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.clone());

        let result = validate_ssec_for_read(&metadata, Some(&key_b64), Some(&key_md5));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_ssec_for_read_spoofed_md5() {
        // A client provides the correct stored MD5 in the header but with a
        // DIFFERENT key. The server must recompute MD5 from the key bytes and
        // reject the request because the recomputed MD5 won't match the header.
        let real_key = [42u8; 32];
        let stored_md5 = md5_base64(real_key);

        let mut metadata = HashMap::new();
        metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
        metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), stored_md5.clone());

        // Attacker has a different key but tries to pass the stored MD5 as their header
        let fake_key = [99u8; 32];
        let fake_key_b64 = BASE64_STANDARD.encode(fake_key);

        let err = validate_ssec_for_read(&metadata, Some(&fake_key_b64), Some(&stored_md5)).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_sse_headers_for_read_rejects_kms_on_plain_object() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("aws:kms"));
        headers.insert("x-amz-server-side-encryption-aws-kms-key-id", http::HeaderValue::from_static("test-key"));

        let metadata = HashMap::new();
        let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_validate_sse_headers_for_read_rejects_ssec_on_plain_object() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES256"),
        );
        headers.insert("x-amz-server-side-encryption-customer-key", http::HeaderValue::from_static("test-key"));
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            http::HeaderValue::from_static("test-key-md5"),
        );

        let metadata = HashMap::new();
        let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_sse_headers_for_read_rejects_ssec_on_managed_object() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-server-side-encryption-customer-algorithm",
            http::HeaderValue::from_static("AES256"),
        );
        headers.insert("x-amz-server-side-encryption-customer-key", http::HeaderValue::from_static("test-key"));
        headers.insert(
            "x-amz-server-side-encryption-customer-key-md5",
            http::HeaderValue::from_static("test-key-md5"),
        );

        let metadata = HashMap::from([("x-amz-server-side-encryption".to_string(), "aws:kms".to_string())]);
        let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_validate_sse_headers_for_read_allows_encrypted_object_without_request_headers() {
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
            ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
        ]);
        let headers = HeaderMap::new();
        assert!(validate_sse_headers_for_read(&metadata, &headers).is_ok());
    }

    #[test]
    fn test_validate_sse_headers_for_read_rejects_sse_on_ssec_object() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("aws:kms"));
        headers.insert("x-amz-server-side-encryption-aws-kms-key-id", http::HeaderValue::from_static("test-key"));

        let metadata = HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string())]);
        let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    }

    #[test]
    fn test_map_get_object_reader_error_converts_missing_ssec_headers_to_invalid_request() {
        let err = map_get_object_reader_error(StorageError::other("missing SSE-C algorithm header"));
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message,
            "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object."
        );
    }

    #[test]
    fn test_map_get_object_reader_error_converts_ssec_md5_mismatch_to_invalid_request() {
        let err = map_get_object_reader_error(StorageError::other("SSE-C key MD5 mismatch"));
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
        assert_eq!(
            err.message,
            "The calculated MD5 hash of the key did not match the hash that was provided."
        );
    }

    #[test]
    fn test_map_get_object_reader_error_preserves_typed_service_unavailable() {
        let resolution_error =
            super::EncryptionResolutionError::new(EncryptionResolutionErrorKind::ServiceUnavailable, "KMS unavailable");
        let err = map_get_object_reader_error(StorageError::other(resolution_error));
        assert_eq!(err.code, S3ErrorCode::ServiceUnavailable);
        assert_eq!(err.message, "KMS unavailable");
    }

    #[test]
    fn test_map_get_object_reader_error_redacts_non_ssec_internal_errors() {
        let err = map_get_object_reader_error(StorageError::other("plain io failure"));
        assert_eq!(err.code, S3ErrorCode::InternalError);
        assert_eq!(err.message, ApiError::error_code_to_message(&S3ErrorCode::InternalError));
        let source = err
            .source
            .as_deref()
            .and_then(|source| source.downcast_ref::<StorageError>())
            .expect("API error should retain the storage error source");
        assert!(matches!(source, StorageError::Io(io_error) if io_error.to_string().contains("plain io failure")));
    }

    #[test]
    fn test_validate_ssec_params_returns_invalid_request_on_bad_algorithm() {
        let key = BASE64_STANDARD.encode([42u8; 32]);
        let key_md5 = md5_base64([42u8; 32]);
        let params = SsecParams {
            algorithm: "AES128".to_string(),
            key,
            key_md5,
        };
        let err = validate_ssec_params(params).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn test_validate_ssec_params_returns_invalid_request_on_bad_md5() {
        let key = BASE64_STANDARD.encode([42u8; 32]);
        let params = SsecParams {
            algorithm: "AES256".to_string(),
            key,
            key_md5: BASE64_STANDARD.encode([99u8; 16]),
        };
        let err = validate_ssec_params(params).unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    }

    // ========================================================================
    // Unit tests for issue #2041: no mandatory KMS when encryption not used
    // ========================================================================

    /// When SSE-C params are not present and no managed SSE is requested,
    /// encryption should be skipped (Ok(None)). Ensures we do not require KMS
    /// when the client sends no encryption headers.
    #[tokio::test]
    async fn test_sse_encryption_skip_when_no_ssec_and_no_managed_sse_requested() {
        let request = EncryptionRequest {
            bucket: "test-bucket",
            key: "test-key",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 1024,
            principal: None,
        };
        let result = sse_encryption(request).await;
        match &result {
            Ok(None) => {}
            Ok(Some(_)) => panic!("expected no encryption material when no SSE params provided"),
            Err(e) => {
                assert!(
                    !e.message.contains("No KMS key"),
                    "must not require KMS when no encryption requested; got: {}",
                    e.message
                );
            }
        }
    }

    /// When SSE-C params are partial or invalid, sse_encryption must return an error.
    #[tokio::test]
    async fn test_sse_encryption_errors_on_invalid_ssec_params() {
        let bucket = "test-bucket";
        let key = "test-key";
        let sse_key = BASE64_STANDARD.encode([42u8; 32]);
        let wrong_md5 = BASE64_STANDARD.encode([99u8; 16]);

        let request_wrong_md5 = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(sse_key.clone()),
            sse_customer_key_md5: Some(wrong_md5),
            content_size: 1024,
            principal: None,
        };
        let err = sse_encryption(request_wrong_md5).await.unwrap_err();
        assert_eq!(err.code, S3ErrorCode::InvalidRequest);

        let request_unsupported_algorithm = EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("unsupported-algo".to_string()),
            sse_customer_key: Some(sse_key),
            sse_customer_key_md5: Some(md5_base64([42u8; 32])),
            content_size: 1024,
            principal: None,
        };
        let err = sse_encryption(request_unsupported_algorithm).await.unwrap_err();
        assert!(err.code == S3ErrorCode::InvalidRequest || err.code == S3ErrorCode::InvalidArgument);
    }

    /// When bucket has no SSE-S3/aws:kms setting and request has no SSE headers,
    /// encryption should be skipped (Ok(None)). Ensures no mandatory bucket default SSE.
    #[tokio::test]
    async fn test_sse_prepare_encryption_skip_when_no_params_and_no_bucket_sse() {
        let request = PrepareEncryptionRequest {
            bucket: "test-bucket-no-sse-config",
            key: "test-key",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        };
        let result = sse_prepare_encryption(request).await;
        match &result {
            Ok(None) => {}
            Ok(Some(_)) => panic!("expected no encryption when bucket has no SSE config and no request SSE"),
            Err(e) => {
                assert!(
                    !e.message.contains("No KMS key"),
                    "must not require KMS when no bucket SSE and no request SSE; got: {}",
                    e.message
                );
            }
        }
    }

    // ------------------------------------------------------------------------
    // Per-key KMS authorization on the SSE-KMS data path (rustfs/backlog#1582)
    // ------------------------------------------------------------------------

    struct RecordingKmsKeyAuthorizer {
        allowed: bool,
        calls: std::sync::Mutex<Vec<(KmsAction, String)>>,
    }

    impl RecordingKmsKeyAuthorizer {
        fn new(allowed: bool) -> Arc<Self> {
            Arc::new(Self {
                allowed,
                calls: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<(KmsAction, String)> {
            self.calls.lock().expect("authorizer call log").clone()
        }
    }

    #[async_trait]
    impl KmsKeyAuthorizer for RecordingKmsKeyAuthorizer {
        async fn is_allowed(&self, _principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, ApiError> {
            self.calls
                .lock()
                .expect("authorizer call log")
                .push((action, key_id.to_string()));
            Ok(self.allowed)
        }
    }

    fn enforcing_principal(allowed: bool) -> (SseKmsPrincipal, Arc<RecordingKmsKeyAuthorizer>) {
        let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
        (SseKmsPrincipal::for_test("analyst", true, authorizer.clone()), authorizer)
    }

    fn permissive_principal_with_enforcement_off() -> (SseKmsPrincipal, Arc<RecordingKmsKeyAuthorizer>) {
        let authorizer = RecordingKmsKeyAuthorizer::new(false);
        (SseKmsPrincipal::for_test("analyst", false, authorizer.clone()), authorizer)
    }

    fn sse_kms_write(principal: Option<&SseKmsPrincipal>) -> EncryptionRequest<'_> {
        EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)),
            ssekms_key_id: Some("finance-key".to_string()),
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 128,
            principal,
        }
    }

    fn sse_kms_object_metadata() -> HashMap<String, String> {
        HashMap::from([
            ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AWS_KMS.to_string()),
            ("x-amz-server-side-encryption-aws-kms-key-id".to_string(), "finance-key".to_string()),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "finance-key".to_string()),
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode([7u8; 48])),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([9u8; 12])),
            (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "aws:kms".to_string()),
        ])
    }

    fn sse_s3_object_metadata() -> HashMap<String, String> {
        HashMap::from([
            ("x-amz-server-side-encryption".to_string(), ServerSideEncryption::AES256.to_string()),
            (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode([7u8; 48])),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode([9u8; 12])),
            (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
        ])
    }

    #[test]
    fn kms_authorization_subject_scopes_to_enforced_sse_kms_requests_only() {
        let (principal, _) = enforcing_principal(true);

        assert!(super::kms_authorization_subject(true, Some(&principal), SSEType::SseKms).is_some());
        // Compatibility switch off keeps the pre-enforcement behaviour.
        assert!(super::kms_authorization_subject(false, Some(&principal), SSEType::SseKms).is_none());
        // SSE-S3 and SSE-C are exempt, matching AWS.
        assert!(super::kms_authorization_subject(true, Some(&principal), SSEType::SseS3).is_none());
        assert!(super::kms_authorization_subject(true, Some(&principal), SSEType::SseC).is_none());
        // No principal means an internal caller.
        assert!(super::kms_authorization_subject(true, None, SSEType::SseKms).is_none());
    }

    #[tokio::test]
    async fn sse_kms_write_without_generate_data_key_is_denied() {
        let (principal, authorizer) = enforcing_principal(false);

        let error = sse_encryption(sse_kms_write(Some(&principal)))
            .await
            .expect_err("SSE-KMS write must fail without kms:GenerateDataKey on the key");

        assert_eq!(error.code, S3ErrorCode::AccessDenied);
        assert_eq!(authorizer.calls(), vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]);
    }

    #[tokio::test]
    async fn sse_kms_write_with_generate_data_key_clears_the_gate() {
        let (principal, authorizer) = enforcing_principal(true);

        let outcome = sse_encryption(sse_kms_write(Some(&principal))).await;

        assert!(
            !matches!(&outcome, Err(error) if error.code == S3ErrorCode::AccessDenied),
            "granting kms:GenerateDataKey on the key must clear the authorization gate"
        );
        assert_eq!(authorizer.calls(), vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]);
    }

    #[tokio::test]
    async fn sse_kms_read_without_decrypt_is_denied() {
        let (principal, authorizer) = enforcing_principal(false);
        let metadata = sse_kms_object_metadata();

        let error = sse_decryption(DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        })
        .await
        .expect_err("SSE-KMS read must fail without kms:Decrypt on the key");

        assert_eq!(error.code, S3ErrorCode::AccessDenied);
        assert_eq!(authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);
    }

    #[tokio::test]
    async fn sse_s3_objects_are_exempt_from_kms_key_authorization() {
        let (principal, authorizer) = enforcing_principal(false);
        let metadata = sse_s3_object_metadata();

        let write = sse_encryption(EncryptionRequest {
            server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
            ssekms_key_id: None,
            ..sse_kms_write(Some(&principal))
        })
        .await;
        let read = sse_decryption(DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        })
        .await;

        assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
        assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
        assert!(authorizer.calls().is_empty(), "SSE-S3 must never consult KMS key policy");
    }

    #[tokio::test]
    async fn ssec_requests_are_exempt_from_kms_key_authorization() {
        let (principal, authorizer) = enforcing_principal(false);
        let customer_key = BASE64_STANDARD.encode([42u8; 32]);
        let customer_key_md5 = md5_base64([42u8; 32]);

        let outcome = sse_encryption(EncryptionRequest {
            server_side_encryption: None,
            ssekms_key_id: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key),
            sse_customer_key_md5: Some(customer_key_md5),
            ..sse_kms_write(Some(&principal))
        })
        .await;

        assert!(outcome.is_ok(), "SSE-C must not be gated on KMS key policy");
        assert!(authorizer.calls().is_empty(), "SSE-C never reaches KMS");
    }

    #[tokio::test]
    async fn internal_callers_bypass_kms_key_authorization() {
        let metadata = sse_kms_object_metadata();

        // No principal: replication, lifecycle transition, heal and scanner reach the SSE
        // layer below the S3 authorization boundary and must not be interrupted.
        let write = sse_encryption(sse_kms_write(None)).await;
        let read = sse_decryption(DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        })
        .await;

        assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
        assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
    }

    #[tokio::test]
    async fn enforcement_switch_off_preserves_current_behaviour() {
        let (principal, authorizer) = permissive_principal_with_enforcement_off();
        let metadata = sse_kms_object_metadata();

        let write = sse_encryption(sse_kms_write(Some(&principal))).await;
        let read = sse_decryption(DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        })
        .await;

        assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
        assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
        assert!(
            authorizer.calls().is_empty(),
            "with the switch off the data path must not evaluate KMS key policy at all"
        );
    }

    #[tokio::test]
    async fn copy_source_read_authorization_targets_the_source_key() {
        let (denied, denied_authorizer) = enforcing_principal(false);
        let (allowed, allowed_authorizer) = enforcing_principal(true);
        let metadata = sse_kms_object_metadata();

        let error = authorize_sse_kms_object_read(Some(&denied), &metadata)
            .await
            .expect_err("copying an SSE-KMS source must require kms:Decrypt on the source key");
        assert_eq!(error.code, S3ErrorCode::AccessDenied);
        assert_eq!(denied_authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);

        authorize_sse_kms_object_read(Some(&allowed), &metadata)
            .await
            .expect("kms:Decrypt on the source key must allow the copy");
        assert_eq!(allowed_authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);
    }

    #[tokio::test]
    async fn copy_source_read_authorization_skips_unencrypted_and_sse_s3_sources() {
        let (principal, authorizer) = enforcing_principal(false);

        authorize_sse_kms_object_read(Some(&principal), &HashMap::new())
            .await
            .expect("plaintext sources are not gated");
        authorize_sse_kms_object_read(Some(&principal), &sse_s3_object_metadata())
            .await
            .expect("SSE-S3 sources are not gated");

        assert!(authorizer.calls().is_empty());
    }

    // ========================================================================
    // Data-plane KMS audit attachment
    // ========================================================================

    fn audited_principal(enforced: bool, allowed: bool) -> (SseKmsPrincipal, Arc<super::KmsRequestAudit>) {
        let audit = Arc::new(super::KmsRequestAudit::default());
        let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
        let principal = SseKmsPrincipal::for_test("analyst", enforced, authorizer).with_request_audit(audit.clone());
        (principal, audit)
    }

    fn audit_tag(tags: &[(&'static str, serde_json::Value)], key: &str) -> Option<String> {
        tags.iter()
            .find(|(candidate, _)| *candidate == key)
            .and_then(|(_, value)| value.as_str().map(str::to_string))
    }

    #[tokio::test]
    async fn sse_kms_write_and_read_summarise_key_id_and_outcome_for_the_audit_entry() {
        use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
        let _guard = lock_sse_test_state().await;

        reset_sse_dek_provider();
        let manager = configure_test_global_local_kms().await;
        manager
            .get_encryption_service()
            .await
            .expect("encryption service should exist")
            .create_key(CreateKeyRequest {
                key_name: Some("audit-key".to_string()),
                key_usage: KeyUsage::EncryptDecrypt,
                description: None,
                policy: None,
                tags: HashMap::new(),
                origin: None,
            })
            .await
            .expect("kms test key should be created");
        let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
            .await
            .expect("kms provider should initialize from the configured test manager");
        super::set_sse_dek_provider_for_test(Arc::new(provider));

        let (write_principal, write_audit) = audited_principal(false, true);
        let material = sse_encryption(EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)),
            ssekms_key_id: Some("audit-key".to_string()),
            ssekms_context: Some(HashMap::from([("tenant".to_string(), "acct-4711".to_string())])),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 128,
            principal: Some(&write_principal),
        })
        .await
        .expect("sse-kms encryption should succeed")
        .expect("managed sse-kms material");

        let write_tags = write_audit.audit_tags();
        assert_eq!(audit_tag(&write_tags, "sseType").as_deref(), Some("SSE-KMS"));
        assert_eq!(audit_tag(&write_tags, "kmsKeyId").as_deref(), Some("audit-key"));
        assert_eq!(audit_tag(&write_tags, "kmsOutcome").as_deref(), Some("success"));
        assert_eq!(audit_tag(&write_tags, "kmsErrorClass"), None, "a successful request has no error class");

        // The audit entry reaches every configured target: no encoding of the data
        // key, and no caller-supplied encryption-context value, may appear in it.
        let rendered = format!("{write_tags:?}");
        let encrypted_data_key = material.encrypted_data_key.clone().expect("managed sse wraps a data key");
        for secret in [
            BASE64_STANDARD.encode(&encrypted_data_key),
            BASE64_STANDARD.encode(material.key_bytes),
            format!("{:?}", material.key_bytes),
            format!("{encrypted_data_key:?}"),
            "acct-4711".to_string(),
        ] {
            assert!(!rendered.contains(&secret), "audit tags must not carry key material: {rendered}");
        }

        let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
        let (read_principal, read_audit) = audited_principal(false, true);
        sse_decryption(DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&read_principal),
        })
        .await
        .expect("sse-kms decryption should succeed")
        .expect("managed sse-kms material");

        let read_tags = read_audit.audit_tags();
        assert_eq!(audit_tag(&read_tags, "sseType").as_deref(), Some("SSE-KMS"));
        assert_eq!(audit_tag(&read_tags, "kmsKeyId").as_deref(), Some("audit-key"));
        assert_eq!(audit_tag(&read_tags, "kmsOutcome").as_deref(), Some("success"));
    }

    #[tokio::test]
    async fn sse_c_requests_produce_no_kms_audit_summary() {
        let key = [0x21u8; 32];
        let (principal, audit) = audited_principal(false, true);

        sse_encryption(EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some(SSECustomerAlgorithm::from("AES256".to_string())),
            sse_customer_key: Some(SSECustomerKey::from(BASE64_STANDARD.encode(key))),
            sse_customer_key_md5: Some(SSECustomerKeyMD5::from(md5_base64(key))),
            content_size: 128,
            principal: Some(&principal),
        })
        .await
        .expect("sse-c encryption should succeed")
        .expect("sse-c material");

        assert!(
            audit.audit_tags().is_empty(),
            "SSE-C keys never reach KMS, so the request has no KMS outcome to report"
        );
    }

    #[tokio::test]
    async fn denied_sse_kms_read_is_summarised_as_an_access_denied_failure() {
        let (principal, audit) = audited_principal(true, false);

        authorize_sse_kms_object_read(Some(&principal), &sse_kms_object_metadata())
            .await
            .expect_err("an unauthorized principal must not read the source key");

        let tags = audit.audit_tags();
        assert_eq!(audit_tag(&tags, "sseType").as_deref(), Some("SSE-KMS"));
        assert_eq!(audit_tag(&tags, "kmsKeyId").as_deref(), Some("finance-key"));
        assert_eq!(audit_tag(&tags, "kmsOutcome").as_deref(), Some("failure"));
        assert_eq!(audit_tag(&tags, "kmsErrorClass").as_deref(), Some("access_denied"));
    }

    #[test]
    fn kms_backend_failures_keep_the_kms_audit_error_class() {
        assert_eq!(
            super::kms_data_plane_error_class(&kms_operation_error(rustfs_kms::KmsError::key_not_found("finance-key"))),
            "key_not_found"
        );
        assert_eq!(
            super::kms_data_plane_error_class(&kms_operation_error(rustfs_kms::KmsError::access_denied("nope"))),
            "access_denied"
        );
        // Failures raised by the SSE layer itself never saw a KmsError; they are
        // classified by the boundary they surfaced through.
        assert_eq!(
            super::kms_data_plane_error_class(&ApiError {
                code: S3ErrorCode::AccessDenied,
                message: "Access Denied".to_string(),
                source: None,
            }),
            "access_denied"
        );
        assert_eq!(
            super::kms_data_plane_error_class(&ApiError::from(StorageError::other("no KMS key available"))),
            "sse_internal"
        );
    }

    #[test]
    fn a_request_audit_slot_is_reachable_only_while_its_scope_lives() {
        let scope = super::KmsRequestAuditScope::register("request-under-audit");
        let slot = super::kms_request_audit("request-under-audit").expect("a registered request must resolve its slot");
        slot.record(SSEType::SseKms, Some("finance-key"), None, None);
        assert_eq!(audit_tag(&scope.audit_tags(), "kmsKeyId").as_deref(), Some("finance-key"));

        drop(scope);
        assert!(
            super::kms_request_audit("request-under-audit").is_none(),
            "the slot must not outlive the audit entry it belongs to"
        );
    }

    #[test]
    fn a_request_that_did_no_kms_work_reports_no_tags() {
        let scope = super::KmsRequestAuditScope::register("quiet-request");
        assert!(scope.audit_tags().is_empty());
    }

    /// The canonical seven-field envelope, with `master_key_version` grafted on
    /// when a wrapping version is wanted.
    fn audit_test_envelope(master_key_version: Option<u32>) -> Vec<u8> {
        let mut envelope = serde_json::json!({
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        });
        if let Some(version) = master_key_version {
            envelope
                .as_object_mut()
                .expect("envelope is an object")
                .insert("master_key_version".to_string(), serde_json::json!(version));
        }
        serde_json::to_vec(&envelope).expect("encode envelope")
    }

    #[test]
    fn envelope_master_key_version_reads_only_true_envelopes() {
        // A versioned envelope reports the wrapping version.
        assert_eq!(super::envelope_master_key_version(&audit_test_envelope(Some(3))), Some(3));
        // A pre-versioning envelope has no version to report.
        assert_eq!(super::envelope_master_key_version(&audit_test_envelope(None)), None);
        // Opaque backend ciphertext (Transit, AWS) is not an envelope.
        assert_eq!(super::envelope_master_key_version(b"vault:v2:abcdefgh"), None);
        // JSON that is not the envelope shape must not be probed for a version.
        assert_eq!(super::envelope_master_key_version(br#"{"master_key_version": 9}"#), None);
    }

    #[test]
    fn stored_envelope_master_key_version_reads_both_metadata_families() {
        let envelope = BASE64_STANDARD.encode(audit_test_envelope(Some(2)));

        // RustFS-branded stored key.
        let metadata = HashMap::from([(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), envelope.clone())]);
        assert_eq!(super::stored_envelope_master_key_version(&metadata), Some(2));

        // MinIO-branded stored key reaches the same answer through
        // normalize_managed_metadata — the dual internal metadata key rule.
        let metadata = HashMap::from([(super::MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), envelope)]);
        assert_eq!(super::stored_envelope_master_key_version(&metadata), Some(2));

        assert_eq!(super::stored_envelope_master_key_version(&HashMap::new()), None);
    }

    #[test]
    fn recorded_key_versions_reach_the_audit_tags() {
        let scope = super::KmsRequestAuditScope::register("versioned-request");
        let slot = super::kms_request_audit("versioned-request").expect("a registered request must resolve its slot");
        slot.record(SSEType::SseKms, Some("finance-key"), Some(3), None);
        let tags = scope.audit_tags();
        assert_eq!(audit_tag(&tags, "kmsKeyVersion").as_deref(), Some("3"));
        assert_eq!(audit_tag(&tags, "kmsOutcome").as_deref(), Some("success"));
    }
}
