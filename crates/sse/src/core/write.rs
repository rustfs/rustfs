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

//! Write entry points: resolve the encryption material for a new object or multipart upload.

use crate::core::audit::{envelope_master_key_version, record_managed_kms_outcome};
use crate::core::keying::context::build_object_encryption_context;
use crate::core::keying::material::EncryptionMaterial;
use crate::core::keying::{
    SESSION_KEY_NEEDS_CUSTOMER_KEY, new_key_under_customer_key, new_key_under_data_key, new_session_key_under_customer_key,
};
use crate::core::request::{SseKmsPrincipal, operation_host};
use crate::core::s3::authorization::authorize_sse_kms_key;
use crate::core::s3::headers::reject_kms_context_location_override;
use crate::core::s3::scheme::{
    DEFAULT_SSE_ALGORITHM, WriteScheme, WriteSchemeHeaders, is_managed_sse, managed_sse_type, resolve_write_scheme,
};
use crate::core::s3::ssec::{SsecParams, ssec_invalid_request, validate_ssec_params, verify_ssec_key_match};
use crate::error::{SseError, sse_kms_unavailable_error};
use crate::host::SseHost;
use crate::kms::provider::get_sse_dek_provider;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_policy::policy::action::KmsAction;
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::http::headers::{AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS};
use rustfs_utils::http::object_encryption_keys::SSEType;
use std::collections::HashMap;

/// Request parameters for unified encryption
#[derive(Clone)]
pub struct EncryptionRequest<'a> {
    /// Bucket name
    pub bucket: &'a str,
    /// Object key
    pub key: &'a str,
    /// Server-side encryption algorithm (SSE-S3 or SSE-KMS)
    pub server_side_encryption: Option<String>,
    /// KMS key ID (for SSE-KMS)
    pub ssekms_key_id: Option<String>,
    /// Optional client-provided KMS context for SSE-KMS.
    pub ssekms_context: Option<HashMap<String, String>>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<String>,
    /// SSE-C key (Base64-encoded)
    pub sse_customer_key: Option<String>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<String>,
    /// Content size (for metadata)
    pub content_size: i64,
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    pub principal: Option<&'a SseKmsPrincipal>,
}

impl std::fmt::Debug for EncryptionRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionRequest")
            .field("server_side_encryption", &self.server_side_encryption)
            .field("content_size", &self.content_size)
            .finish_non_exhaustive()
    }
}

impl EncryptionRequest<'_> {
    pub fn validate_complete_multipart_ssec(&self, user_defined: &HashMap<String, String>) -> Result<(), SseError> {
        let request_uses_ssec =
            self.sse_customer_algorithm.is_some() || self.sse_customer_key.is_some() || self.sse_customer_key_md5.is_some();
        if !request_uses_ssec {
            let stored_algorithm = user_defined.get("x-amz-server-side-encryption-customer-algorithm");
            let stored_key_md5 = user_defined.get("x-amz-server-side-encryption-customer-key-md5");
            return match (stored_algorithm, stored_key_md5) {
                (None, None) => Ok(()),
                (Some(algorithm), Some(key_md5))
                    if algorithm == DEFAULT_SSE_ALGORITHM
                        && BASE64_STANDARD
                            .decode_to_vec(key_md5)
                            .is_ok_and(|decoded| decoded.len() == 16) =>
                {
                    if rustfs_utils::http::contains_key_str(user_defined, rustfs_utils::http::SUFFIX_PLAINTEXT_CHECKSUM) {
                        return Err(ssec_invalid_request(
                            "SSE-C multipart uploads with additional checksums require the customer key for completion.",
                        ));
                    }
                    Ok(())
                }
                _ => Err(ssec_invalid_request("The multipart upload contains invalid SSE-C metadata.")),
            };
        }
        self.validate_multipart_ssec(user_defined)
    }

    pub fn validate_multipart_ssec(&self, user_defined: &HashMap<String, String>) -> Result<(), SseError> {
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
        // Multipart session metadata is written by our own CreateMultipartUpload,
        // which always records the MD5 — the MinIO no-stored-MD5 shape cannot occur.
        verify_ssec_key_match(&validated.key_md5, stored_key_md5, false)
    }
}

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
/// if let Some(material) = sse_encryption(host, request).await? {
///     reader = material.wrap_reader(reader);
///     metadata.extend(material.metadata);
/// }
/// ```
pub async fn sse_encryption(host: &dyn SseHost, request: EncryptionRequest<'_>) -> Result<Option<EncryptionMaterial>, SseError> {
    let host = operation_host(host, request.principal);
    let scheme = resolve_write_scheme(
        host,
        request.bucket,
        WriteSchemeHeaders {
            server_side_encryption: request.server_side_encryption,
            ssekms_key_id: request.ssekms_key_id,
            ssekms_context: request.ssekms_context.as_ref(),
            sse_customer_algorithm: request.sse_customer_algorithm,
            sse_customer_key: request.sse_customer_key,
            sse_customer_key_md5: request.sse_customer_key_md5,
        },
    )
    .await?;

    let (server_side_encryption, kms_key_id) = match scheme {
        None => return Ok(None),
        Some(WriteScheme::SseC { algorithm, key, key_md5 }) => {
            return apply_ssec_encryption_material(request.bucket, request.key, algorithm, key, key_md5, request.content_size)
                .await
                .map(Some);
        }
        Some(WriteScheme::SseS3) => (String::from(AMZ_ENCRYPTION_AES), None),
        Some(WriteScheme::SseKms { key_id }) => (String::from(AMZ_ENCRYPTION_KMS), key_id),
    };

    apply_managed_encryption_material(
        host,
        ManagedEncryptionRequest {
            bucket: request.bucket,
            key: request.key,
            server_side_encryption,
            kms_key_id,
            ssekms_context: request.ssekms_context,
            content_size: request.content_size,
            principal: request.principal,
        },
    )
    .await
    .map(Some)
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
    pub server_side_encryption: Option<String>,
    /// KMS key ID (for SSE-KMS)
    pub ssekms_key_id: Option<String>,
    /// Optional client-provided KMS context for SSE-KMS.
    pub ssekms_context: Option<HashMap<String, String>>,
    /// SSE-C algorithm (customer-provided key)
    pub sse_customer_algorithm: Option<String>,
    /// SSE-C key (Base64-encoded)
    pub sse_customer_key: Option<String>,
    /// SSE-C key MD5 (Base64-encoded)
    pub sse_customer_key_md5: Option<String>,
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    ///
    /// This is the only point at which a multipart upload evaluates KMS key permissions:
    /// it is where the session data key is generated. `UploadPart`, `UploadPartCopy` and
    /// `CompleteMultipartUpload` reuse that envelope and are not re-authorized.
    pub principal: Option<&'a SseKmsPrincipal>,
}

pub async fn sse_prepare_encryption(
    host: &dyn SseHost,
    request: PrepareEncryptionRequest<'_>,
) -> Result<Option<EncryptionMaterial>, SseError> {
    let host = operation_host(host, request.principal);
    let scheme = resolve_write_scheme(
        host,
        request.bucket,
        WriteSchemeHeaders {
            server_side_encryption: request.server_side_encryption,
            ssekms_key_id: request.ssekms_key_id,
            ssekms_context: request.ssekms_context.as_ref(),
            sse_customer_algorithm: request.sse_customer_algorithm,
            sse_customer_key: request.sse_customer_key,
            sse_customer_key_md5: request.sse_customer_key_md5,
        },
    )
    .await?;

    let (server_side_encryption, kms_key_id) = match scheme {
        None => return Ok(None),
        Some(WriteScheme::SseC { algorithm, key, key_md5 }) => {
            return apply_ssec_prepare_encryption_material(request.bucket, request.key, algorithm, Some(key), key_md5)
                .await
                .map(Some);
        }
        Some(WriteScheme::SseS3) => (String::from(AMZ_ENCRYPTION_AES), None),
        Some(WriteScheme::SseKms { key_id }) => (String::from(AMZ_ENCRYPTION_KMS), key_id),
    };

    // A session records no plaintext size: the parts decide it.
    apply_managed_encryption_material(
        host,
        ManagedEncryptionRequest {
            bucket: request.bucket,
            key: request.key,
            server_side_encryption,
            kms_key_id,
            ssekms_context: request.ssekms_context,
            content_size: 0,
            principal: request.principal,
        },
    )
    .await
    .map(Some)
}

async fn apply_ssec_prepare_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: String,
    sse_key: Option<String>,
    sse_key_md5: String,
) -> Result<EncryptionMaterial, SseError> {
    // The customer key is validated here only in a mode that seals the session's
    // key under it. Otherwise it is validated per part, against what the session
    // records.
    let customer_key = match sse_key {
        Some(sse_key) if SESSION_KEY_NEEDS_CUSTOMER_KEY => Some(
            validate_ssec_params(SsecParams {
                algorithm: algorithm.clone(),
                key: sse_key,
                key_md5: sse_key_md5.clone(),
            })?
            .key_bytes,
        ),
        _ => None,
    };
    let object_key = new_session_key_under_customer_key(customer_key, bucket, key)?;

    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
        kms_key_id: None,
        algorithm,
        key_bytes: object_key.key_bytes,
        base_nonce: object_key.base_nonce,
        encrypted_data_key: None,
        customer_key_md5: Some(sse_key_md5),
        original_size: None,
        key_kind: object_key.key_kind,
        managed_kms_context: None,
        managed_sealed_key: object_key.sealed_key,
    })
}

async fn apply_ssec_encryption_material(
    bucket: &str,
    key: &str,
    algorithm: String,
    sse_key: String,
    sse_key_md5: String,
    content_size: i64,
) -> Result<EncryptionMaterial, SseError> {
    let params = SsecParams {
        algorithm,
        key: sse_key,
        key_md5: sse_key_md5,
    };

    let validated = validate_ssec_params(params)?;
    let object_key = new_key_under_customer_key(validated.key_bytes, bucket, key)?;

    // Build metadata
    Ok(EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
        kms_key_id: None,
        algorithm: validated.algorithm,
        key_bytes: object_key.key_bytes,
        base_nonce: object_key.base_nonce,
        encrypted_data_key: None,
        customer_key_md5: Some(validated.key_md5),
        original_size: Some(content_size),
        key_kind: object_key.key_kind,
        managed_kms_context: None,
        managed_sealed_key: object_key.sealed_key,
    })
}

/// A managed-SSE (SSE-S3 / SSE-KMS) write, once its effective scheme is known.
pub(crate) struct ManagedEncryptionRequest<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) server_side_encryption: String,
    pub(crate) kms_key_id: Option<String>,
    pub(crate) ssekms_context: Option<HashMap<String, String>>,
    pub(crate) content_size: i64,
    pub(crate) principal: Option<&'a SseKmsPrincipal>,
}

pub(crate) async fn apply_managed_encryption_material(
    host: &dyn SseHost,
    request: ManagedEncryptionRequest<'_>,
) -> Result<EncryptionMaterial, SseError> {
    // Request validation, not a KMS operation: reject before the KMS is called
    // and before an audit outcome is recorded.
    reject_kms_context_location_override(request.bucket, request.ssekms_context.as_ref())?;

    let principal = request.principal;
    let requested_sse_type = managed_sse_type(request.server_side_encryption.as_str());
    let requested_key_id = request.kms_key_id.clone();
    let result = apply_managed_encryption_material_inner(host, request).await;

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

pub(crate) async fn apply_managed_encryption_material_inner(
    host: &dyn SseHost,
    request: ManagedEncryptionRequest<'_>,
) -> Result<EncryptionMaterial, SseError> {
    let ManagedEncryptionRequest {
        bucket,
        key,
        server_side_encryption,
        kms_key_id,
        ssekms_context,
        content_size,
        principal,
    } = request;
    let host = operation_host(host, principal);
    if !is_managed_sse(&server_side_encryption) {
        return Err(SseError::internal(format!(
            "Unsupported server-side encryption: {}",
            server_side_encryption.as_str()
        )));
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
        if let Some(service) = host.encryption_service().await {
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
            // Neither the request nor the bucket default named a key and no
            // service default filled in. Without a service this is the same
            // outage/misconfiguration the provider check below reports, so it
            // must carry the same 503/400 split rather than an untyped
            // internal error; with a running service that has no default key
            // the caller simply has to name one.
            if host.encryption_service().await.is_none() {
                return Err(sse_kms_unavailable_error(host.kms_configured_but_unavailable().await));
            }
            return Err(SseError {
                code: S3ErrorCode::InvalidRequest,
                message: "SSE-KMS requires a KMS key id: the request named none and the KMS service has no default key"
                    .to_string(),
                source: None,
            });
        }
        _ => unreachable!("managed SSE branch only supports SSE-S3 or SSE-KMS"),
    };

    // The key is fully resolved here (request header, then bucket default, then the KMS
    // service default), so this is the first point at which the caller can be held to the
    // key it will actually be encrypted under.
    authorize_sse_kms_key(host, principal, encryption_type, KmsAction::GenerateDataKeyAction, &kms_key_to_use).await?;

    // A node-local master key is the explicit SSE-S3 fallback. Letting it serve
    // an SSE-KMS request would persist an `aws:kms` marker and a KMS key id
    // that never wrapped the data key, so refuse rather than downgrade.
    //
    // The refusal sits after the authorization gate so an unauthorized caller
    // still sees AccessDenied whatever the KMS runtime state is, and it asks
    // the resolved provider rather than a parallel availability signal,
    // because the provider is what actually wraps the DEK.
    let provider = match get_sse_dek_provider(host).await {
        Ok(provider) => {
            if matches!(encryption_type, SSEType::SseKms) && provider.wraps_dek_with_local_master_key() {
                return Err(sse_kms_unavailable_error(host.kms_configured_but_unavailable().await));
            }
            provider
        }
        // With no master key set the local fallback fails with an SSE-S3-worded
        // configuration error. An SSE-KMS request never asked for that provider,
        // so it gets the SSE-KMS refusal instead of a message naming the wrong
        // scheme.
        Err(err) => {
            if matches!(encryption_type, SSEType::SseKms) && host.encryption_service().await.is_none() {
                return Err(sse_kms_unavailable_error(host.kms_configured_but_unavailable().await));
            }
            return Err(err);
        }
    };
    let object_context = build_object_encryption_context(bucket, key, ssekms_context.as_ref());
    let (data_key, encrypted_data_key) = provider.generate_sse_dek(&object_context, &kms_key_to_use).await?;

    let algorithm = server_side_encryption.as_str().to_string();
    let object_key = new_key_under_data_key(&data_key, encryption_type, bucket, key)?;

    Ok(EncryptionMaterial {
        sse_type: encryption_type,
        server_side_encryption,
        kms_key_id: Some(kms_key_to_use),
        algorithm,
        key_bytes: object_key.key_bytes,
        base_nonce: object_key.base_nonce,
        encrypted_data_key: Some(encrypted_data_key),
        customer_key_md5: None,
        original_size: Some(content_size),
        key_kind: object_key.key_kind,
        managed_kms_context: matches!(encryption_type, SSEType::SseKms).then_some(ssekms_context.unwrap_or_default()),
        managed_sealed_key: object_key.sealed_key,
    })
}
