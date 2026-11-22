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

//! Reading: recognize how a stored object was keyed, and recover its key.
//!
//! What an object recorded is the only source of truth here. The scheme a request
//! advertises and whether a KMS is running now are write policy and runtime state;
//! neither says how an object written earlier was keyed.

use crate::core::keying::context::{build_object_encryption_context, decode_minio_kms_context};
use crate::core::keying::direct::read_stored_ssec_nonce;
use crate::core::keying::material::EncryptionKeyKind;
#[cfg(not(feature = "rio-v2"))]
use crate::core::keying::material::ManagedSealedKey;
use crate::core::keying::metadata::recode_minio_kms_context;
#[cfg(feature = "rio-v2")]
use crate::core::keying::object::{
    infer_minio_managed_sse_type, is_legacy_rustfs_managed_metadata, parse_minio_managed_sealed_key, unseal_object_key,
};
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
use rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
    INTERNAL_ENCRYPTION_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER, SSEType,
    contains_managed_encryption_metadata, normalize_managed_metadata,
};
use std::collections::HashMap;
use std::sync::Arc;

/// The key a stored object is decrypted with, recovered from what the object recorded.
pub(crate) struct OpenedObjectKey {
    /// Key handed to the stream cipher.
    pub(crate) key_bytes: [u8; 32],
    /// Base nonce the stream cipher derives its block nonces from.
    pub(crate) base_nonce: [u8; 12],
    /// The mode the object was written in.
    pub(crate) key_kind: EncryptionKeyKind,
}

/// The managed scheme a stored object was written under, or `None` when this build
/// cannot establish it.
fn stored_managed_sse_type(metadata: &HashMap<String, String>) -> Option<SSEType> {
    match metadata.get("x-amz-server-side-encryption").map(String::as_str) {
        Some(AMZ_ENCRYPTION_KMS) => Some(SSEType::SseKms),
        Some(_) => Some(SSEType::SseS3),
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
        //
        // An object whose scheme cannot be established stays fail-closed, and
        // deliberately not through an error raised here: the read plan independently
        // classifies the object as encrypted from its markers and refuses to serve
        // it without material, so it never degrades into a plaintext read.
        #[cfg(feature = "rio-v2")]
        None => infer_minio_managed_sse_type(metadata),
        // Without the rio-v2 reader there is no MinIO-format read path to serve
        // such an object with, so it stays on the fail-closed branch.
        #[cfg(not(feature = "rio-v2"))]
        None => None,
    }
}

/// SSE-C: recover the object's key from the customer key.
///
/// A sealed object key recorded in MinIO's slot is unsealed under the customer key;
/// otherwise the customer key is the object's key, under the nonce the object recorded.
pub(crate) fn open_under_customer_key(
    metadata: &HashMap<String, String>,
    customer_key: [u8; 32],
    bucket: &str,
    key: &str,
) -> Result<OpenedObjectKey, SseError> {
    #[cfg(feature = "rio-v2")]
    if let Some(sealed_key) = parse_minio_managed_sealed_key(metadata, SSEType::SseC)? {
        return Ok(OpenedObjectKey {
            key_bytes: unseal_object_key(&sealed_key, customer_key, SSEType::SseC, bucket, key)?,
            base_nonce: [0; 12],
            key_kind: EncryptionKeyKind::Object,
        });
    }

    Ok(OpenedObjectKey {
        key_bytes: customer_key,
        base_nonce: read_stored_ssec_nonce(metadata, bucket, key),
        key_kind: EncryptionKeyKind::Direct,
    })
}

/// A stored managed-SSE object, as recognized from its metadata alone.
pub(crate) struct StoredManagedKey<'a> {
    /// The object's metadata as stored.
    metadata: &'a HashMap<String, String>,
    /// The same metadata with the managed keys normalized.
    normalized_metadata: HashMap<String, String>,
    /// The scheme the object was written under.
    pub(crate) sse_type: SSEType,
    /// The KMS key id the object recorded, if it recorded one.
    pub(crate) recorded_kms_key_id: Option<String>,
}

/// Recognize a stored managed-SSE object.
///
/// `None` means the object carries no managed-SSE metadata, or carries some whose
/// scheme this build cannot establish.
pub(crate) fn recognize_stored_managed_key(metadata: &HashMap<String, String>) -> Option<StoredManagedKey<'_>> {
    if !contains_managed_encryption_metadata(metadata) {
        return None;
    }
    let sse_type = stored_managed_sse_type(metadata)?;

    let normalized_metadata = normalize_managed_metadata(metadata, Some(recode_minio_kms_context));
    let recorded_kms_key_id = normalized_metadata
        .get(INTERNAL_ENCRYPTION_KEY_ID_HEADER)
        .or_else(|| metadata.get("x-amz-server-side-encryption-aws-kms-key-id"))
        .cloned();

    Some(StoredManagedKey {
        metadata,
        normalized_metadata,
        sse_type,
        recorded_kms_key_id,
    })
}

/// Managed SSE: unwrap the stored data key and recover the object's key from it.
///
/// `kms_key_id` is the key the read was authorized against. Returns the object's key
/// together with the algorithm the object recorded.
pub(crate) async fn open_under_stored_data_key(
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    stored: &StoredManagedKey<'_>,
    kms_key_id: &str,
) -> Result<(OpenedObjectKey, String), SseError> {
    let metadata = stored.metadata;
    let normalized_metadata = &stored.normalized_metadata;
    let sse_type = stored.sse_type;

    #[cfg(feature = "rio-v2")]
    let minio_sealed_key = parse_minio_managed_sealed_key(metadata, sse_type)?;
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
            .headers_to_metadata(normalized_metadata)
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

    let kms_context = if matches!(sse_type, SSEType::SseKms) {
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
                get_local_sse_dek_provider(),
                &encrypted_data_key,
                kms_key_id,
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
                get_local_sse_dek_provider()?
            };
            #[cfg(feature = "rio-v2")]
            let decrypted_data_key = if is_legacy_rustfs_managed_metadata(normalized_metadata) {
                provider
                    .decrypt_legacy_sse_dek(&encrypted_data_key, kms_key_id, &object_context)
                    .await
            } else {
                provider
                    .decrypt_sse_dek(&encrypted_data_key, kms_key_id, &object_context)
                    .await
            };
            #[cfg(not(feature = "rio-v2"))]
            let decrypted_data_key = provider
                .decrypt_sse_dek(&encrypted_data_key, kms_key_id, &object_context)
                .await;
            decrypted_data_key?
        }
    };

    #[cfg(feature = "rio-v2")]
    let opened = if let Some(sealed_key) = minio_sealed_key {
        OpenedObjectKey {
            key_bytes: unseal_object_key(&sealed_key, decrypted_data_key, sse_type, bucket, key)?,
            base_nonce: [0u8; 12],
            key_kind: EncryptionKeyKind::Object,
        }
    } else {
        data_key_under_stored_nonce(decrypted_data_key, &iv)
    };
    #[cfg(not(feature = "rio-v2"))]
    let opened = data_key_under_stored_nonce(decrypted_data_key, &iv);

    Ok((opened, algorithm))
}

/// Direct mode: the data key is the object's key, under the nonce stored beside it.
fn data_key_under_stored_nonce(data_key: [u8; 32], iv: &[u8]) -> OpenedObjectKey {
    let mut base_nonce = [0u8; 12];
    base_nonce.copy_from_slice(&iv[..12]);
    OpenedObjectKey {
        key_bytes: data_key,
        base_nonce,
        key_kind: EncryptionKeyKind::Direct,
    }
}
