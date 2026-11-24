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
