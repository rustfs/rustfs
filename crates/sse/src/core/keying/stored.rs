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
use crate::core::keying::direct::{read_stored_ssec_nonce, read_wrapped_data_key};
use crate::core::keying::material::EncryptionKeyKind;
use crate::core::keying::metadata::recode_minio_kms_context;
#[cfg(feature = "m-compatible")]
use crate::core::keying::object::{
    infer_minio_managed_sse_type, is_legacy_rustfs_managed_metadata, parse_minio_managed_sealed_key, read_sealed_data_key,
    unseal_object_key,
};
use crate::core::keying::{StoredDataKey, StoredKeyMode};
use crate::error::SseError;
use crate::host::SseHost;
use crate::kms::local::is_local_sse_dek;
#[cfg(feature = "m-compatible")]
use crate::kms::local::is_rustfs_managed_data_key;
#[cfg(feature = "m-compatible")]
use crate::kms::provider::decrypt_unenveloped_sealed_dek;
use crate::kms::provider::{SseDekProvider, bare_data_key_kms_provider, get_local_sse_dek_provider, kms_sse_dek_provider};
use rustfs_kms::is_data_key_envelope;
use rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_KEY_ID_HEADER, SSEType, contains_managed_encryption_metadata, normalize_managed_metadata,
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
        #[cfg(feature = "m-compatible")]
        None => infer_minio_managed_sse_type(metadata),
        // Without the rio-v2 reader there is no MinIO-format read path to serve
        // such an object with, so it stays on the fail-closed branch.
        #[cfg(not(feature = "m-compatible"))]
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
    #[cfg(feature = "m-compatible")]
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

/// Prefix MinIO puts on the key id of every `aws:kms` response
/// (`internal/crypto`, `ARNPrefix`).
const MINIO_KMS_KEY_ARN_PREFIX: &str = "arn:aws:kms:";

impl StoredManagedKey<'_> {
    /// The key id an `aws:kms` response advertises for this object.
    ///
    /// An object that stores its public scheme header is answered with the key
    /// id it recorded. One that stores none was written by MinIO, which
    /// advertises its key ids in ARN form (`cmd/encryption-v1.go`,
    /// `kmsKeyIDFromMetadata`); a client that recorded the id MinIO returned
    /// must see the same value after the drives move to RustFS. `kms_key_id`
    /// stays the bare id everywhere else: authorization and the unwrap name the
    /// key itself.
    pub(crate) fn response_kms_key_id(&self, kms_key_id: String) -> String {
        if self.metadata.contains_key("x-amz-server-side-encryption") || kms_key_id.starts_with(MINIO_KMS_KEY_ARN_PREFIX) {
            return kms_key_id;
        }
        format!("{MINIO_KMS_KEY_ARN_PREFIX}{kms_key_id}")
    }
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
    let record = read_stored_data_key(host, stored).await?;
    let data_key = unwrap_stored_data_key(host, bucket, key, stored, &record, kms_key_id).await?;

    let opened = match record.mode {
        StoredKeyMode::Direct { base_nonce } => OpenedObjectKey {
            key_bytes: data_key,
            base_nonce,
            key_kind: EncryptionKeyKind::Direct,
        },
        #[cfg(feature = "m-compatible")]
        StoredKeyMode::Object { sealed_key } => OpenedObjectKey {
            key_bytes: unseal_object_key(&sealed_key, data_key, stored.sse_type, bucket, key)?,
            base_nonce: [0u8; 12],
            key_kind: EncryptionKeyKind::Object,
        },
    };

    Ok((opened, record.algorithm))
}

/// Read the stored data key in the mode the object recorded.
///
/// This is the recognition order: a mode listed earlier claims the object first, and
/// Direct takes whatever no other mode recognizes. A mode this build cannot read is
/// not listed, so its objects fail in the Direct reader instead of being misread.
async fn read_stored_data_key(host: &dyn SseHost, stored: &StoredManagedKey<'_>) -> Result<StoredDataKey, SseError> {
    #[cfg(feature = "m-compatible")]
    if let Some(sealed_key) = parse_minio_managed_sealed_key(stored.metadata, stored.sse_type)? {
        return read_sealed_data_key(stored.metadata, &stored.normalized_metadata, sealed_key);
    }

    read_wrapped_data_key(host, &stored.normalized_metadata).await
}

/// Unwrap the stored data key with the provider its own bytes identify.
async fn unwrap_stored_data_key(
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    stored: &StoredManagedKey<'_>,
    record: &StoredDataKey,
    kms_key_id: &str,
) -> Result<[u8; 32], SseError> {
    let kms_context = if matches!(stored.sse_type, SSEType::SseKms) {
        decode_minio_kms_context(stored.metadata)?
    } else {
        None
    };
    let object_context = build_object_encryption_context(bucket, key, kms_context.as_ref());
    let wrapped = record.wrapped.as_slice();

    // Persisted wrapping format is the read-side source of truth. The
    // advertised SSE scheme and current KMS availability are write policy
    // and runtime state, neither of which identifies the historical provider.
    //
    // The one shape that cannot name its provider is a backend-native blob
    // stored without an envelope. It is offered to the KMS only when the
    // local provider could not have written those bytes and the active
    // backend is one whose released versions persisted them that way.
    let bare_kms = if is_data_key_envelope(wrapped) || is_local_sse_dek(wrapped) {
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
    #[cfg(feature = "m-compatible")]
    if matches!(record.mode, StoredKeyMode::Object { .. }) && !is_rustfs_managed_data_key(wrapped) {
        return decrypt_unenveloped_sealed_dek(bare_kms, get_local_sse_dek_provider(), wrapped, kms_key_id, &object_context)
            .await;
    }

    let provider: Arc<dyn SseDekProvider> = if is_data_key_envelope(wrapped) {
        kms_sse_dek_provider(host).await?
    } else if let Some(kms_provider) = bare_kms {
        kms_provider
    } else {
        get_local_sse_dek_provider()?
    };

    #[cfg(feature = "m-compatible")]
    if is_legacy_rustfs_managed_metadata(&stored.normalized_metadata) {
        return provider.decrypt_legacy_sse_dek(wrapped, kms_key_id, &object_context).await;
    }

    provider.decrypt_sse_dek(wrapped, kms_key_id, &object_context).await
}
