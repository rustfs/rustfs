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

//! SSE-C: validating the customer-provided key and matching it against the stored object.

use crate::core::keying::metadata::{minio_sealed_ssec_metadata, stored_ssec_metadata};
use crate::core::s3::scheme::{DEFAULT_SSE_ALGORITHM, SUPPORT_SSE_ALGORITHMS};
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use md5::{Digest as Md5Digest, Md5};
use rustfs_s3_types::S3ErrorCode;
use std::collections::HashMap;
use tracing::error;

pub(crate) fn md5_bytes(input: impl AsRef<[u8]>) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(input.as_ref());
    hasher.finalize().into()
}

pub(crate) fn md5_base64(input: impl AsRef<[u8]>) -> String {
    BASE64_STANDARD.encode_to_string(md5_bytes(input))
}

/// Validated SSE-C parameters
#[derive(Debug, Clone)]
pub(crate) struct ValidatedSsecParams {
    /// Encryption algorithm (always "AES256" for SSE-C)
    pub algorithm: String,
    /// Decoded encryption key bytes (32 bytes for AES-256)
    pub key_bytes: [u8; 32],
    /// Base64-encoded MD5 of the key
    pub key_md5: String,
}

/// SSE-C parameters from client request
#[derive(Debug, Clone)]
pub(crate) struct SsecParams {
    /// Encryption algorithm
    pub algorithm: String,
    /// Base64-encoded encryption key
    pub key: String,
    /// Base64-encoded MD5 of the key
    pub key_md5: String,
}

/// Validate SSE-C parameters from client request
///
/// Validates:
/// 1. Algorithm is "AES256"
/// 2. Key is valid Base64 and exactly 32 bytes
/// 3. MD5 hash matches the key
///
/// # Returns
/// `ValidatedSsecParams` with decoded key bytes
pub(crate) fn validate_ssec_params(params: SsecParams) -> Result<ValidatedSsecParams, SseError> {
    if !SUPPORT_SSE_ALGORITHMS.contains(&params.algorithm.as_str()) {
        return Err(ssec_invalid_request(&format!(
            "Unsupported SSE-C algorithm. Only {DEFAULT_SSE_ALGORITHM} is supported."
        )));
    }

    let key_bytes = BASE64_STANDARD.decode_to_vec(&params.key).map_err(|e| {
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

/// Verify SSE-C key matches the stored metadata.
///
/// Used during GetObject/HeadObject to ensure the client provided the correct key.
/// Returns 400 InvalidRequest on mismatch, consistent with AWS S3 behavior.
pub(crate) fn verify_ssec_key_match(
    provided_md5: &str,
    stored_md5: Option<&String>,
    minio_sealed_ssec: bool,
) -> Result<(), SseError> {
    match stored_md5 {
        Some(stored) if stored == provided_md5 => Ok(()),
        Some(_) => Err(ssec_invalid_request(
            "The provided encryption parameters did not match the ones used originally to encrypt the object.",
        )),
        // MinIO never persists the customer-key MD5, so this early check has
        // nothing to compare against on a migrated object. The key itself is
        // still proven: a wrong key fails the AEAD unseal of the sealed object
        // key. The check exists only for the friendlier early error, so its
        // absence is tolerated for exactly that shape and no other.
        None if minio_sealed_ssec => Ok(()),
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
    sse_customer_key: Option<&String>,
    sse_customer_key_md5: Option<&String>,
) -> Result<(), SseError> {
    if !stored_ssec_metadata(metadata) {
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
    let algorithm = metadata
        .get("x-amz-server-side-encryption-customer-algorithm")
        .cloned()
        .unwrap_or_else(|| DEFAULT_SSE_ALGORITHM.to_string());
    let validated = validate_ssec_params(SsecParams {
        algorithm,
        key: key.to_string(),
        key_md5: key_md5.clone(),
    })?;

    let stored_md5 = metadata.get("x-amz-server-side-encryption-customer-key-md5");
    verify_ssec_key_match(&validated.key_md5, stored_md5, minio_sealed_ssec_metadata(metadata))
}

/// Build an `SseError` with `InvalidRequest` (HTTP 400) for SSE-C related errors.
pub fn ssec_invalid_request(message: &str) -> SseError {
    SseError {
        code: S3ErrorCode::InvalidRequest,
        message: message.to_string(),
        source: None,
    }
}
