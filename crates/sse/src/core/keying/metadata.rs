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

//! Encryption metadata as it is stored with the object.

use crate::core::keying::context::encode_minio_kms_context;
use crate::core::keying::material::{EncryptionKeyKind, EncryptionMaterial};
#[cfg(feature = "rio-v2")]
use crate::core::keying::object::MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM;
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_CONTEXT_HEADER, INTERNAL_ENCRYPTION_IV_HEADER,
    INTERNAL_ENCRYPTION_KEY_HEADER, INTERNAL_ENCRYPTION_KEY_ID_HEADER, INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER,
    INTERNAL_ENCRYPTION_TAG_HEADER, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEC_ORIGINAL_SIZE_HEADER, SSEType,
};
use std::borrow::Cow;
use std::collections::HashMap;

/// Two spellings of one encryption metadata key carry different values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConflictingEncryptionMetadata {
    /// The canonical key whose case-variants disagree.
    pub key: &'static str,
}

impl std::fmt::Display for ConflictingEncryptionMetadata {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "conflicting object encryption metadata for {}", self.key)
    }
}

impl std::error::Error for ConflictingEncryptionMetadata {}

/// Fold case-variant spellings of the encryption metadata keys onto their canonical names.
///
/// # Errors
///
/// When two spellings of the same key hold different values: picking either
/// would decrypt with material the object may not have been written under.
pub fn normalize_encryption_metadata_case(
    metadata: &HashMap<String, String>,
) -> Result<Cow<'_, HashMap<String, String>>, ConflictingEncryptionMetadata> {
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
            return Err(ConflictingEncryptionMetadata { key: canonical });
        }
        if !normalized.contains_key(*canonical) {
            normalized.insert((*canonical).to_string(), value.clone());
        }
    }
    Ok(Cow::Owned(normalized))
}

pub fn encryption_material_to_metadata(material: &EncryptionMaterial) -> Result<HashMap<String, String>, SseError> {
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
                metadata.insert(
                    INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(material.base_nonce),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(material.base_nonce),
                );
            }

            #[cfg(feature = "rio-v2")]
            if let Some(sealed) = &material.managed_sealed_key {
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(sealed.iv),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(sealed.sealed_key),
                );
            }
        }
        SSEType::SseS3 | SSEType::SseKms => {
            let encrypted_data_key = material
                .encrypted_data_key
                .as_deref()
                .ok_or_else(|| SseError::internal("managed SSE materials must carry an encrypted data key"))?;
            metadata.insert(
                "x-amz-server-side-encryption".to_string(),
                material.server_side_encryption.as_str().to_string(),
            );

            let internal_key_id = material.kms_key_id.clone().unwrap_or_else(|| "default".to_string());
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
                metadata.insert(
                    INTERNAL_ENCRYPTION_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(encrypted_data_key),
                );
                metadata.insert(
                    INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(material.base_nonce),
                );
                metadata.insert(INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), material.algorithm.as_str().to_string());
            }

            #[cfg(feature = "rio-v2")]
            if let Some(sealed) = &material.managed_sealed_key {
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(sealed.iv),
                );
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
                            BASE64_STANDARD.encode_to_string(sealed.sealed_key),
                        );
                    }
                    SSEType::SseKms => {
                        metadata.insert(
                            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(),
                            BASE64_STANDARD.encode_to_string(sealed.sealed_key),
                        );
                    }
                    SSEType::SseC => {}
                }
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(encrypted_data_key),
                );
            } else if cfg!(feature = "rio-v2") {
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(material.base_nonce),
                );
                metadata.insert(
                    MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
                    MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
                );
                if let Some(kms_key_id) = &material.kms_key_id {
                    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), kms_key_id.to_string());
                }
                let encoded_key = BASE64_STANDARD.encode_to_string(encrypted_data_key);
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
    rustfs_utils::http::metadata_compat::remove_str(metadata, rustfs_utils::http::SUFFIX_OPAQUE_ENCRYPTED_ETAG);
}

pub fn mark_encrypted_multipart_metadata(metadata: &mut HashMap<String, String>) {
    metadata.insert(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER.to_string(), String::new());
}

/// True when the stored metadata marks the object as SSE-C protected.
///
/// RustFS persists the public `x-amz-server-side-encryption-customer-algorithm`
/// key on SSE-C writes. MinIO persists only its internal sealed-key slot and
/// synthesizes the public header onto responses (backlog#1638), so the slot is
/// the only durable evidence on a MinIO-written object. That shape is readable
/// solely through the rio-v2 sealed-object-key path, so it participates in
/// detection only in builds that can actually read it — default builds keep
/// the resolver's fail-closed diagnosis instead of demanding customer keys
/// they could not use.
pub fn stored_ssec_metadata(metadata: &HashMap<String, String>) -> bool {
    metadata.contains_key("x-amz-server-side-encryption-customer-algorithm") || minio_sealed_ssec_metadata(metadata)
}

/// True when the object carries MinIO's SSE-C sealed-key slot — the shape that
/// stores no customer-key MD5 and proves the key through the AEAD unseal
/// instead.
pub(crate) fn minio_sealed_ssec_metadata(metadata: &HashMap<String, String>) -> bool {
    cfg!(feature = "rio-v2") && metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
}

/// Recodes a stored MinIO KMS context value (base64-wrapped JSON) into the
/// plain-JSON form RustFS stores under [`INTERNAL_ENCRYPTION_CONTEXT_HEADER`].
///
/// Injected into the shared [`normalize_managed_metadata`] because the shared
/// crate carries no JSON codec; any decode failure returns `None`, which skips
/// the context mapping exactly like the historical inline `if let Ok` chain.
pub(crate) fn recode_minio_kms_context(value: &str) -> Option<String> {
    let decoded = BASE64_STANDARD.decode_to_vec(value).ok()?;
    let context = serde_json::from_slice::<HashMap<String, String>>(&decoded).ok()?;
    serde_json::to_string(&context).ok()
}
