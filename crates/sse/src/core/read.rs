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

//! Read entry points: resolve the decryption material, or the response headers, for a stored object.

use crate::core::audit::{record_managed_kms_outcome, stored_envelope_master_key_version};
use crate::core::keying::context::{build_object_encryption_context, decode_minio_kms_context};
use crate::core::keying::direct::read_stored_ssec_nonce;
#[cfg(not(feature = "rio-v2"))]
use crate::core::keying::material::ManagedSealedKey;
use crate::core::keying::material::{DecryptionMaterial, EncryptionKeyKind};
use crate::core::keying::metadata::{minio_sealed_ssec_metadata, recode_minio_kms_context, stored_ssec_metadata};
#[cfg(feature = "rio-v2")]
use crate::core::keying::object::{
    infer_minio_managed_sse_type, is_legacy_rustfs_managed_metadata, parse_minio_managed_sealed_key, unseal_object_key,
};
use crate::core::request::SseKmsPrincipal;
use crate::core::s3::authorization::authorize_sse_kms_key;
use crate::core::s3::headers::{SseReadResponseHeaders, managed_sse_public_header};
use crate::core::s3::scheme::resolve_stored_kms_key_id;
use crate::core::s3::ssec::{SsecParams, ssec_invalid_request, validate_ssec_params, verify_ssec_key_match};
use crate::error::SseError;
use crate::host::SseHost;
use crate::kms::local::is_local_sse_dek;
#[cfg(feature = "rio-v2")]
use crate::kms::local::is_rustfs_managed_data_key;
#[cfg(feature = "rio-v2")]
use crate::kms::provider::decrypt_unenveloped_sealed_dek;
use crate::kms::provider::{SseDekProvider, bare_data_key_kms_provider, get_local_sse_dek_provider, kms_sse_dek_provider};
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_kms::is_data_key_envelope;
use rustfs_policy::policy::action::KmsAction;
use rustfs_utils::http::headers::{AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS};
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
    INTERNAL_ENCRYPTION_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER, SSEType,
    contains_managed_encryption_metadata, normalize_managed_metadata, stored_managed_encryption_key,
};
use std::collections::HashMap;
use std::sync::Arc;

impl std::fmt::Debug for DecryptionRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecryptionRequest").finish_non_exhaustive()
    }
}

/// Request parameters for unified decryption
pub struct DecryptionRequest<'a> {
    /// Bucket name
    pub bucket: &'a str,
    /// Object key
    pub key: &'a str,
    /// Object metadata containing encryption headers
    pub metadata: &'a HashMap<String, String>,
    /// SSE-C key (Base64-encoded) - required if object was encrypted with SSE-C
    pub sse_customer_key: Option<&'a String>,
    /// SSE-C key MD5 (Base64-encoded) - required if object was encrypted with SSE-C
    pub sse_customer_key_md5: Option<&'a String>,
    /// Caller the SSE-KMS key usage is authorized as. `None` marks an internal caller.
    pub principal: Option<&'a SseKmsPrincipal>,
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
/// if let Some(material) = sse_decryption(host, request).await? {
///     content_size = material.original_size.unwrap_or(actual_size);
/// }
/// ```
pub async fn sse_decryption(host: &dyn SseHost, request: DecryptionRequest<'_>) -> Result<Option<DecryptionMaterial>, SseError> {
    // Check for SSE-C encryption
    if stored_ssec_metadata(request.metadata) {
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
        verify_ssec_key_match(key_md5, stored_md5, minio_sealed_ssec_metadata(request.metadata))?;

        let mut material = apply_ssec_decryption_material(request.bucket, request.key, request.metadata, key, key_md5).await?;
        material.customer_key_md5 = Some(key_md5.clone());
        return Ok(Some(material));
    }

    // Check for managed SSE encryption
    if contains_managed_encryption_metadata(request.metadata) {
        return apply_managed_decryption_material(host, request.bucket, request.key, request.metadata, request.principal).await;
    }

    // No encryption detected
    Ok(None)
}

/// Read-side response classification for an object whose payload the object
/// layer's encryption resolver already decrypted.
///
/// [`sse_decryption`] both unwraps the data key and derives the response
/// headers. The GET path only needs the latter — its stream comes out of the
/// object layer decrypted (see [`SseObjectEncryptionResolver`]) — so calling
/// [`sse_decryption`] there performed a second KMS `Decrypt` per request whose
/// key bytes were discarded. This function reproduces that call's header,
/// validation, authorization and audit behavior from stored metadata alone.
///
/// Contract, mirrored from [`sse_decryption`] item by item:
/// - SSE-C: missing request key/MD5 and stored/request MD5 mismatch fail with
///   the same errors and precedence, and the provided key is fully validated.
/// - Managed SSE: per-key `kms:Decrypt` authorization runs ahead of every other
///   failure mode, and the request's KMS audit summary records the same
///   scheme/key/version/outcome fields the unwrap-based path recorded. The
///   success outcome is honest because a failed unwrap aborts the read in the
///   object layer before response classification is ever reached.
/// - Objects without SSE metadata — and managed metadata whose scheme cannot
///   be established in this build — classify as `None`, exactly where
///   [`sse_decryption`] returned `None`.
pub async fn classify_sse_read_response(
    host: &dyn SseHost,
    request: DecryptionRequest<'_>,
) -> Result<Option<SseReadResponseHeaders>, SseError> {
    if stored_ssec_metadata(request.metadata) {
        let (key, key_md5) = match (request.sse_customer_key, request.sse_customer_key_md5) {
            (Some(k), Some(md5)) => (k, md5),
            _ => {
                return Err(ssec_invalid_request(
                    "The object was stored using a form of Server Side Encryption. \
                     The correct parameters must be provided to retrieve the object.",
                ));
            }
        };

        let stored_md5 = request.metadata.get("x-amz-server-side-encryption-customer-key-md5");
        verify_ssec_key_match(key_md5, stored_md5, minio_sealed_ssec_metadata(request.metadata))?;

        let algorithm = request
            .metadata
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|s| s.as_str())
            .unwrap_or("AES256");
        validate_ssec_params(SsecParams {
            algorithm: algorithm.to_string(),
            key: key.to_string(),
            key_md5: key_md5.to_string(),
        })?;

        return Ok(Some(SseReadResponseHeaders {
            server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
            sse_customer_algorithm: Some(String::from(algorithm)),
            sse_customer_key_md5: Some(key_md5.clone()),
            ssekms_key_id: None,
        }));
    }

    if !contains_managed_encryption_metadata(request.metadata) {
        return Ok(None);
    }

    let sse_type = match request.metadata.get("x-amz-server-side-encryption").map(String::as_str) {
        Some(AMZ_ENCRYPTION_KMS) => SSEType::SseKms,
        Some(_) => SSEType::SseS3,
        #[cfg(feature = "rio-v2")]
        None => match infer_minio_managed_sse_type(request.metadata) {
            Some(sse_type) => sse_type,
            None => return Ok(None),
        },
        #[cfg(not(feature = "rio-v2"))]
        None => return Ok(None),
    };

    // Same key-id resolution chain as the unwrap path, so authorization and the
    // response header name the same key.
    let normalized_metadata = normalize_managed_metadata(request.metadata, Some(recode_minio_kms_context));
    let kms_key_id = resolve_stored_kms_key_id(
        host,
        normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
            .or_else(|| request.metadata.get("x-amz-server-side-encryption-aws-kms-key-id"))
            .cloned(),
    )
    .await;

    // Ahead of every other failure mode, so a denied caller learns nothing
    // about the key beyond "not yours".
    if let Err(error) = authorize_sse_kms_key(host, request.principal, sse_type, KmsAction::DecryptAction, &kms_key_id).await {
        if let Some((sse_type, key_id)) = stored_managed_encryption_key(request.metadata) {
            record_managed_kms_outcome(
                request.principal,
                sse_type,
                Some(&key_id),
                || stored_envelope_master_key_version(request.metadata),
                Err(&error),
            );
        }
        return Err(error);
    }

    record_managed_kms_outcome(
        request.principal,
        sse_type,
        Some(&kms_key_id),
        || stored_envelope_master_key_version(request.metadata),
        Ok(()),
    );

    Ok(Some(SseReadResponseHeaders {
        server_side_encryption: managed_sse_public_header(sse_type).to_string(),
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        // The key id was needed above to authorize the read, but an AES256
        // object's wrapping key is internal: only aws:kms objects advertise it.
        ssekms_key_id: matches!(sse_type, SSEType::SseKms).then_some(kms_key_id),
    }))
}

pub(crate) async fn apply_ssec_decryption_material(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    sse_key: &str,
    sse_key_md5: &str,
) -> Result<DecryptionMaterial, SseError> {
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
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES), // const
        kms_key_id: None,
        algorithm: String::from(algorithm),

        customer_key_md5: None,
        key_bytes,
        base_nonce,
        key_kind,
    })
}

pub(crate) async fn apply_managed_decryption_material(
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    principal: Option<&SseKmsPrincipal>,
) -> Result<Option<DecryptionMaterial>, SseError> {
    let result = apply_managed_decryption_material_inner(host, bucket, key, metadata, principal).await;

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
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    principal: Option<&SseKmsPrincipal>,
) -> Result<Option<DecryptionMaterial>, SseError> {
    #[cfg(not(feature = "rio-v2"))]
    let _ = (bucket, key);
    if !contains_managed_encryption_metadata(metadata) {
        return Ok(None);
    }

    let encryption_type = match metadata.get("x-amz-server-side-encryption").map(String::as_str) {
        Some(AMZ_ENCRYPTION_KMS) => SSEType::SseKms,
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
    let kms_key_id = resolve_stored_kms_key_id(
        host,
        normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
            .or_else(|| metadata.get("x-amz-server-side-encryption-aws-kms-key-id"))
            .cloned(),
    )
    .await;

    // Ahead of every other failure mode below, so a denied caller learns nothing about the
    // key beyond "not yours" — not whether it is disabled, pending deletion, or unreadable.
    authorize_sse_kms_key(host, principal, encryption_type, KmsAction::DecryptAction, &kms_key_id).await?;

    #[cfg(feature = "rio-v2")]
    let minio_sealed_key = parse_minio_managed_sealed_key(metadata, encryption_type)?;
    #[cfg(not(feature = "rio-v2"))]
    let minio_sealed_key: Option<ManagedSealedKey> = None;

    let (encrypted_data_key, iv, algorithm) = if minio_sealed_key.is_some() {
        let encrypted_key_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
            .ok_or_else(|| SseError::internal("Missing encrypted key in metadata"))?;
        let encrypted_data_key = BASE64_STANDARD
            .decode_to_vec(encrypted_key_b64)
            .map_err(|e| SseError::internal(format!("Failed to decode encrypted key: {e}")))?;
        (
            encrypted_data_key,
            Vec::new(),
            normalized_metadata
                .get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER)
                .cloned()
                .unwrap_or_else(|| "AES256".to_string()),
        )
    } else if let Some(service) = host.encryption_service().await {
        // Production mode: use service for metadata parsing
        let parsed = service
            .headers_to_metadata(&normalized_metadata)
            .map_err(|e| SseError::internal(format!("Failed to parse encryption metadata: {e}")))?;

        if parsed.iv.len() != 12 {
            return Err(SseError::internal("Invalid encryption nonce length; expected 12 bytes"));
        }

        (parsed.encrypted_data_key, parsed.iv, parsed.algorithm)
    } else {
        // Test mode: parse metadata manually
        let encrypted_key_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .ok_or_else(|| SseError::internal("Missing encrypted key in metadata"))?;
        let encrypted_data_key = BASE64_STANDARD
            .decode_to_vec(encrypted_key_b64)
            .map_err(|e| SseError::internal(format!("Failed to decode encrypted key: {e}")))?;

        let iv_b64 = normalized_metadata
            .get(INTERNAL_ENCRYPTION_IV_HEADER)
            .ok_or_else(|| SseError::internal("Missing IV in metadata"))?;
        let iv = BASE64_STANDARD
            .decode_to_vec(iv_b64)
            .map_err(|e| SseError::internal(format!("Failed to decode IV: {e}")))?;

        if iv.len() != 12 {
            return Err(SseError::internal("Invalid encryption nonce length; expected 12 bytes"));
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
    //
    // The one shape that cannot name its provider is a backend-native blob
    // stored without an envelope. It is offered to the KMS only when the
    // local provider could not have written those bytes and the active
    // backend is one whose released versions persisted them that way.
    let bare_kms = if is_data_key_envelope(&encrypted_data_key) || is_local_sse_dek(&encrypted_data_key) {
        None
    } else {
        bare_data_key_kms_provider(host).await
    };
    // A MinIO sealed key alone does not mean MinIO wrote the object: RustFS's own
    // writer fills MinIO's metadata slots too, while still storing a RustFS
    // envelope in them, so neither the slot nor the header name distinguishes the
    // two. The data key's own shape does. RustFS envelopes are strictly-parsed
    // JSON; MinIO's builtin-KMS ciphertext is opaque bytes that match neither, so
    // recognizing RustFS positively — and treating only the remainder as MinIO —
    // keeps a RustFS envelope from ever reaching MinIO's decoder.
    #[cfg(feature = "rio-v2")]
    let unenveloped_sealed_dek = if minio_sealed_key.is_some() && !is_rustfs_managed_data_key(&encrypted_data_key) {
        Some(
            decrypt_unenveloped_sealed_dek(
                bare_kms.clone(),
                get_local_sse_dek_provider().await,
                &encrypted_data_key,
                &kms_key_id,
                &object_context,
            )
            .await?,
        )
    } else {
        None
    };
    #[cfg(not(feature = "rio-v2"))]
    let unenveloped_sealed_dek: Option<[u8; 32]> = None;

    let decrypted_data_key = match unenveloped_sealed_dek {
        Some(data_key) => data_key,
        None => {
            let provider: Arc<dyn SseDekProvider> = if is_data_key_envelope(&encrypted_data_key) {
                kms_sse_dek_provider(host).await?
            } else if let Some(kms_provider) = bare_kms {
                kms_provider
            } else {
                get_local_sse_dek_provider().await?
            };
            #[cfg(feature = "rio-v2")]
            let decrypted_data_key = if is_legacy_rustfs_managed_metadata(&normalized_metadata) {
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
            decrypted_data_key?
        }
    };
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
        server_side_encryption: managed_sse_public_header(encryption_type).to_string(),
        kms_key_id: Some(kms_key_id),
        algorithm,
        customer_key_md5: None,

        key_bytes,
        base_nonce,
        key_kind,
    }))
}
