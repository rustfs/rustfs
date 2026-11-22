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

//! The encryption context a data key is bound to.

use crate::core::s3::headers::sse_invalid_argument;
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use rustfs_kms::types::ObjectEncryptionContext;
use rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER;
use rustfs_utils::path::path_join_buf;
use std::collections::HashMap;

pub(crate) fn canonical_kms_bucket_path(bucket: &str, key: &str) -> String {
    path_join_buf(&[bucket, key])
}

pub(crate) fn build_kms_request_context(
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

/// Rejects a client encryption context that would replace the location binding.
///
/// [`build_kms_request_context`] binds the data key to `{bucket: bucket/key}`
/// with `or_insert`, and only the client context is persisted. A client entry
/// keyed by the bucket name therefore travels with the object and replaces the
/// binding on every read, so the envelope would no longer be tied to the
/// object's location. Only the write side rejects it: objects stored with such
/// an entry stay readable because the read side rebuilds the context unchanged.
pub(crate) fn reject_kms_context_location_override(
    bucket: &str,
    ssekms_context: Option<&HashMap<String, String>>,
) -> Result<(), SseError> {
    if ssekms_context.is_some_and(|context| context.contains_key(bucket)) {
        return Err(sse_invalid_argument(&format!(
            "The x-amz-server-side-encryption-context header contains the key \"{bucket}\", which equals the bucket name; \
             that key is reserved for binding the data key to the object's location."
        )));
    }
    Ok(())
}

pub(crate) fn build_object_encryption_context(
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

pub(crate) fn encode_minio_kms_context(context: &HashMap<String, String>) -> Result<String, SseError> {
    let encoded = serde_json::to_vec(context).map_err(|e| SseError::internal(format!("Failed to serialize KMS context: {e}")))?;
    Ok(BASE64_STANDARD.encode_to_string(encoded))
}

pub(crate) fn decode_minio_kms_context(metadata: &HashMap<String, String>) -> Result<Option<HashMap<String, String>>, SseError> {
    let Some(encoded) = metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER) else {
        return Ok(None);
    };
    let decoded = BASE64_STANDARD
        .decode_to_vec(encoded)
        .map_err(|e| SseError::internal(format!("Failed to decode MinIO KMS context: {e}")))?;
    serde_json::from_slice(&decoded)
        .map(Some)
        .map_err(|e| SseError::internal(format!("Failed to parse MinIO KMS context: {e}")))
}
