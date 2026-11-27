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
use crate::core::keying::material::DecryptionMaterial;
use crate::core::keying::metadata::stored_ssec_metadata;
use crate::core::keying::stored::{
    StoredManagedKey, open_under_customer_key, open_under_stored_data_key, recognize_stored_managed_key,
};
use crate::core::keying::{SealedEtag, open_sealed_etag, sealed_etag};
use crate::core::request::SseKmsPrincipal;
use crate::core::s3::authorization::authorize_sse_kms_key;
use crate::core::s3::headers::{SseReadResponseHeaders, managed_sse_public_header};
use crate::core::s3::scheme::resolve_stored_kms_key_id;
use crate::core::s3::ssec::{SsecParams, require_matching_ssec_key, stored_ssec_algorithm, validate_ssec_params};
use crate::error::SseError;
use crate::host::SseHost;
use rustfs_policy::policy::action::KmsAction;
use rustfs_utils::http::headers::AMZ_ENCRYPTION_AES;
use rustfs_utils::http::object_encryption_keys::{SSEType, contains_managed_encryption_metadata, stored_managed_encryption_key};
use std::collections::HashMap;

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
        let (key, key_md5) = require_matching_ssec_key(request.metadata, request.sse_customer_key, request.sse_customer_key_md5)?;

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
        let (key, key_md5) = require_matching_ssec_key(request.metadata, request.sse_customer_key, request.sse_customer_key_md5)?;

        let algorithm = stored_ssec_algorithm(request.metadata);
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

    // Same scheme and key resolution as the unwrap path, so authorization and the
    // response header name the same key.
    let Some((stored, kms_key_id)) = resolve_stored_managed_key(host, request.metadata).await else {
        return Ok(None);
    };

    // Ahead of every other failure mode, so a denied caller learns nothing
    // about the key beyond "not yours".
    if let Err(error) =
        authorize_sse_kms_key(host, request.principal, stored.sse_type, KmsAction::DecryptAction, &kms_key_id).await
    {
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
        stored.sse_type,
        Some(&kms_key_id),
        || stored_envelope_master_key_version(request.metadata),
        Ok(()),
    );

    Ok(Some(SseReadResponseHeaders {
        server_side_encryption: managed_sse_public_header(stored.sse_type).to_string(),
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        // The key id was needed above to authorize the read, but an AES256
        // object's wrapping key is internal: only aws:kms objects advertise it.
        ssekms_key_id: matches!(stored.sse_type, SSEType::SseKms).then(|| stored.response_kms_key_id(kms_key_id)),
    }))
}

/// Response headers for a metadata-only read of an object that stores none of
/// the public SSE headers.
///
/// RustFS persists the public scheme headers with every encrypted write, and a
/// metadata-only read answers with them as stored; such an object classifies
/// as `None` here. MinIO persists only its internal sealed-key slots and
/// synthesizes the public headers onto each response, so for a MinIO-written
/// object the scheme is inferred from the slot, the way
/// [`classify_sse_read_response`] infers it for a full read.
///
/// `None` also covers a plaintext object and — in a build without the MinIO
/// read path — every object whose scheme only the sealed-key slot names.
///
/// No key is unwrapped and nothing is authorized here: this reports how the
/// object is stored, exactly as the stored-header path does. For SSE-C the
/// request's customer-key MD5 is echoed because MinIO stores none; it is the
/// value the request declared, which the caller has checked against the
/// request's key but which only an unseal can prove against the object.
pub fn classify_unrecorded_sse_response(
    metadata: &HashMap<String, String>,
    sse_customer_key_md5: Option<&String>,
) -> Option<SseReadResponseHeaders> {
    if metadata.contains_key("x-amz-server-side-encryption")
        || metadata.contains_key("x-amz-server-side-encryption-customer-algorithm")
    {
        return None;
    }

    if stored_ssec_metadata(metadata) {
        return Some(SseReadResponseHeaders {
            server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
            sse_customer_algorithm: Some(String::from(stored_ssec_algorithm(metadata))),
            sse_customer_key_md5: sse_customer_key_md5.cloned(),
            ssekms_key_id: None,
        });
    }

    let stored = recognize_stored_managed_key(metadata)?;
    let ssekms_key_id = if matches!(stored.sse_type, SSEType::SseKms) {
        stored
            .recorded_kms_key_id
            .clone()
            .map(|kms_key_id| stored.response_kms_key_id(kms_key_id))
    } else {
        None
    };
    Some(SseReadResponseHeaders {
        server_side_encryption: managed_sse_public_header(stored.sse_type).to_string(),
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        ssekms_key_id,
    })
}

/// The ETag a client is shown for an object whose stored ETag MinIO sealed.
///
/// MinIO stores the ETag of a single-part SSE object encrypted under the object
/// key and never returns that stored form (`cmd/encryption-v1.go`,
/// `getDecryptedETag`): an SSE-S3 object is answered with the unsealed content
/// MD5, an SSE-KMS or SSE-C object with the last 32 hex characters of the
/// sealed value. Returning the stored form instead changes the ETag every
/// client recorded before the drives moved to RustFS.
///
/// `None` means the stored ETag is what the client sees: it is not sealed, the
/// object is a multipart upload (its ETag is stored as shown), the object does
/// not carry MinIO's sealed object key, or — for SSE-S3 — the object key could
/// not be recovered. MinIO falls back to the stored value in that last case
/// too, and the read itself reports the key failure.
///
/// Only an SSE-S3 object costs a data-key unwrap. SSE-S3 keys carry no per-key
/// authorization, and the unwrap is not recorded in the KMS audit summary: it
/// serves the response of a read that is recorded on its own.
pub async fn sealed_etag_for_response(host: &dyn SseHost, request: DecryptionRequest<'_>, stored_etag: &str) -> Option<String> {
    let sealed = match sealed_etag(request.metadata, stored_etag)? {
        SealedEtag::Shown(etag) => return Some(etag),
        SealedEtag::UnderObjectKey(sealed) => sealed,
    };

    let (stored, kms_key_id) = resolve_stored_managed_key(host, request.metadata).await?;
    let (opened, _) = open_under_stored_data_key(host, request.bucket, request.key, &stored, &kms_key_id)
        .await
        .ok()?;
    open_sealed_etag(opened.key_kind, opened.key_bytes, &sealed)
}

/// Recognize a stored managed-SSE object and name the key it is read under.
///
/// One resolution for every read: authorization, the response headers and the
/// unwrap all name the key this returns. `None` means the object carries no
/// managed-SSE metadata, or carries some whose scheme this build cannot establish.
async fn resolve_stored_managed_key<'m>(
    host: &dyn SseHost,
    metadata: &'m HashMap<String, String>,
) -> Option<(StoredManagedKey<'m>, String)> {
    let stored = recognize_stored_managed_key(metadata)?;
    let kms_key_id = resolve_stored_kms_key_id(host, stored.recorded_kms_key_id.clone()).await;
    Some((stored, kms_key_id))
}

pub(crate) async fn apply_ssec_decryption_material(
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
    sse_key: &str,
    sse_key_md5: &str,
) -> Result<DecryptionMaterial, SseError> {
    // Validate provided key
    let algorithm = stored_ssec_algorithm(metadata);

    let params = SsecParams {
        algorithm: algorithm.to_string(),
        key: sse_key.to_string(),
        key_md5: sse_key_md5.to_string(),
    };

    let validated = validate_ssec_params(params)?;
    let opened = open_under_customer_key(metadata, validated.key_bytes, bucket, key)?;

    Ok(DecryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES), // const
        kms_key_id: None,
        algorithm: String::from(algorithm),

        customer_key_md5: None,
        key_bytes: opened.key_bytes,
        base_nonce: opened.base_nonce,
        key_kind: opened.key_kind,
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
    let Some((stored, kms_key_id)) = resolve_stored_managed_key(host, metadata).await else {
        return Ok(None);
    };

    // Ahead of every other failure mode below, so a denied caller learns nothing about the
    // key beyond "not yours" — not whether it is disabled, pending deletion, or unreadable.
    authorize_sse_kms_key(host, principal, stored.sse_type, KmsAction::DecryptAction, &kms_key_id).await?;

    let (opened, algorithm) = open_under_stored_data_key(host, bucket, key, &stored, &kms_key_id).await?;

    Ok(Some(DecryptionMaterial {
        sse_type: stored.sse_type,
        // Synthesized from the resolved scheme rather than read back from
        // metadata: a MinIO-written object has no stored scheme header, which is
        // exactly why the scheme had to be inferred. MinIO synthesizes the same
        // header onto its own responses.
        server_side_encryption: managed_sse_public_header(stored.sse_type).to_string(),
        kms_key_id: Some(kms_key_id),
        algorithm,
        customer_key_md5: None,

        key_bytes: opened.key_bytes,
        base_nonce: opened.base_nonce,
        key_kind: opened.key_kind,
    }))
}
