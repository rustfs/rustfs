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

//! Re-wrap a stored data key onto its master key's current version.

use crate::core::keying::context::{build_object_encryption_context, decode_minio_kms_context};
use crate::core::keying::metadata::recode_minio_kms_context;
use crate::error::SseError;
use crate::host::SseHost;
use crate::kms::provider::kms_sse_dek_provider;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_kms::is_data_key_envelope;
use rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, SSEType,
    contains_managed_encryption_metadata, normalize_managed_metadata,
};
use std::collections::HashMap;

/// Outcome of a single object's DEK rewrap attempt.
// Consumed by the bulk rekey sweep in the follow-up PR; tests exercise it now.
#[allow(dead_code)]
#[derive(Debug)]
pub enum ObjectDekRewrapOutcome {
    /// The object carries no KMS-wrapped RustFS data-key envelope this build
    /// can rewrap: plaintext, SSE-C, or a MinIO-sealed data key.
    NotApplicable,
    /// The envelope is already on the current master key version and format;
    /// the caller must persist nothing, so a sweep re-run converges.
    AlreadyCurrent,
    /// The envelope was rewrapped. `metadata` holds the overrides to merge
    /// into the object's user-defined metadata — every stored copy of the old
    /// envelope, replaced — via `ObjectLayer::put_object_metadata`.
    Rewrapped { metadata: HashMap<String, String> },
}

/// Rewrap the KMS-wrapped data key in an object's stored metadata onto its
/// master key's current version (and current envelope format), without
/// touching the object's data or its plaintext DEK.
///
/// Mirrors the managed decrypt path byte for byte where it matters: the
/// envelope is located through the same normalized-header resolution, and the
/// encryption context is rebuilt with the same helpers, so an envelope the
/// read path can open is exactly an envelope this can rewrap. The KMS backend
/// owns the format and the no-op decision.
///
/// The returned overrides replace every stored copy of the old envelope
/// (RustFS's internal header and the MinIO-compatible slots that the writer
/// fills with the same bytes). A copy left behind would win a read-path
/// fallback and resurrect the old wrapping, so finding no replaceable copy is
/// an error, never a silent success.
pub async fn rewrap_object_encryption_metadata(
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    metadata: &HashMap<String, String>,
) -> Result<ObjectDekRewrapOutcome, SseError> {
    if !contains_managed_encryption_metadata(metadata) {
        return Ok(ObjectDekRewrapOutcome::NotApplicable);
    }

    let encryption_type = match metadata.get("x-amz-server-side-encryption").map(String::as_str) {
        Some(AMZ_ENCRYPTION_KMS) => SSEType::SseKms,
        Some(_) => SSEType::SseS3,
        // MinIO-written objects synthesize the public header on read; their
        // sealed data keys are not RustFS envelopes and fall out below.
        None => SSEType::SseS3,
    };

    let normalized_metadata = normalize_managed_metadata(metadata, Some(recode_minio_kms_context));
    let Some(envelope_b64) = normalized_metadata
        .get(INTERNAL_ENCRYPTION_KEY_HEADER)
        .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
    else {
        return Ok(ObjectDekRewrapOutcome::NotApplicable);
    };
    let encrypted_data_key = BASE64_STANDARD
        .decode_to_vec(envelope_b64)
        .map_err(|e| SseError::internal(format!("Failed to decode encrypted key: {e}")))?;
    // Only RustFS envelopes are rewrappable here; MinIO's builtin-KMS
    // ciphertext is opaque bytes owned by a different root of trust.
    if !is_data_key_envelope(&encrypted_data_key) {
        return Ok(ObjectDekRewrapOutcome::NotApplicable);
    }

    let kms_context = if matches!(encryption_type, SSEType::SseKms) {
        decode_minio_kms_context(metadata)?
    } else {
        None
    };
    let object_context = build_object_encryption_context(bucket, key, kms_context.as_ref());

    // The same provider selection as the managed decrypt path.
    let provider = kms_sse_dek_provider(host).await?;
    let response = provider.rewrap_sse_dek(&encrypted_data_key, &object_context).await?;
    if !response.rewrapped {
        return Ok(ObjectDekRewrapOutcome::AlreadyCurrent);
    }

    // Replace every stored copy of the old envelope, keyed by value and
    // matched case-insensitively: metadata key casing drifts through the
    // storage layer, and an override inserted under a differently-cased name
    // would sit beside the old copy instead of replacing it. The MinIO
    // sealed-key slots can instead hold a sealed *object* key (rio-v2 writer);
    // those bytes differ from the envelope and are untouched — the data key
    // they seal is unchanged by a rewrap.
    const REWRAP_ENVELOPE_HEADERS: [&str; 4] = [
        INTERNAL_ENCRYPTION_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
        MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
    ];
    let old_envelope_b64 = envelope_b64.clone();
    let new_envelope_b64 = BASE64_STANDARD.encode_to_string(&response.ciphertext);
    let mut overrides = HashMap::new();
    for (stored_name, stored_value) in metadata {
        let is_envelope_slot = REWRAP_ENVELOPE_HEADERS
            .iter()
            .any(|header| stored_name.eq_ignore_ascii_case(header));
        if is_envelope_slot && *stored_value == old_envelope_b64 {
            overrides.insert(stored_name.clone(), new_envelope_b64.clone());
        }
    }
    if overrides.is_empty() {
        return Err(SseError::internal(
            "rewrapped a data-key envelope but found no stored metadata copy to replace; refusing a write that would \
             leave the old wrapping live",
        ));
    }

    Ok(ObjectDekRewrapOutcome::Rewrapped { metadata: overrides })
}
