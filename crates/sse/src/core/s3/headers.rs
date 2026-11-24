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

//! SSE request headers in, SSE response headers out.

use crate::core::keying::metadata::{normalize_encryption_metadata_case, recode_minio_kms_context, stored_ssec_metadata};
use crate::core::s3::ssec::ssec_invalid_request;
use crate::error::SseError;
use base64_simd::STANDARD as BASE64_STANDARD;
use http::{HeaderMap, HeaderName, HeaderValue};
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::http::headers::{
    AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM,
    AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT,
    SSEC_ALGORITHM_HEADER, SSEC_KEY_MD5_HEADER,
};
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_CONTEXT_HEADER, INTERNAL_ENCRYPTION_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEType,
    normalize_managed_metadata,
};
use std::collections::HashMap;

// The same header names s3s publishes. Owned so this module does not import s3s.
pub(crate) const X_AMZ_SERVER_SIDE_ENCRYPTION: HeaderName = HeaderName::from_static("x-amz-server-side-encryption");

pub(crate) const X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-aws-kms-key-id");

pub(crate) const X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-context");

pub(crate) const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-customer-algorithm");

pub(crate) const X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5: HeaderName =
    HeaderName::from_static("x-amz-server-side-encryption-customer-key-md5");

#[inline]
pub(crate) fn sse_invalid_argument(message: &str) -> SseError {
    SseError {
        code: S3ErrorCode::InvalidArgument,
        message: message.to_string(),
        source: None,
    }
}

/// SSE-C parameters extracted from headers (algorithm, key, key MD5).
pub type SsecParamsFromHeaders = (Option<String>, Option<String>, Option<String>);

/// Extract SSE-C parameters from request headers.
/// Used as fallback when the S3 layer does not populate them in the input struct.
///
/// Returns an error if an SSE-C header is present but cannot be parsed as valid UTF-8,
/// ensuring malformed headers do not bypass validation.
pub fn extract_ssec_params_from_headers(headers: &HeaderMap) -> Result<SsecParamsFromHeaders, SseError> {
    let algorithm = match headers.get("x-amz-server-side-encryption-customer-algorithm") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(s.to_string()))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-algorithm header must be valid UTF-8.")),
    }?;

    let key = match headers.get("x-amz-server-side-encryption-customer-key") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(s.to_string()))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-key header must be valid UTF-8.")),
    }?;

    let key_md5 = match headers.get("x-amz-server-side-encryption-customer-key-md5") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(s.to_string()))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-customer-key-md5 header must be valid UTF-8.")),
    }?;

    Ok((algorithm, key, key_md5))
}

/// Extract x-amz-server-side-encryption from request headers.
/// Used as fallback when the S3 layer does not populate it in the input struct.
///
/// Returns an error if the header is present but cannot be parsed as valid UTF-8,
/// ensuring malformed headers do not bypass validation.
pub fn extract_server_side_encryption_from_headers(headers: &HeaderMap) -> Result<Option<String>, SseError> {
    match headers.get("x-amz-server-side-encryption") {
        None => Ok(None),
        Some(v) => v
            .to_str()
            .map(|s| Some(s.to_string()))
            .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption header must be valid UTF-8.")),
    }
}

pub fn extract_ssekms_context_from_headers(headers: &HeaderMap) -> Result<Option<HashMap<String, String>>, SseError> {
    let Some(v) = headers.get(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT) else {
        return Ok(None);
    };

    let value = v
        .to_str()
        .map_err(|_| sse_invalid_argument("The x-amz-server-side-encryption-context header must be valid UTF-8."))?;
    let decoded = BASE64_STANDARD.decode_to_vec(value).map_err(|_| {
        sse_invalid_argument("The x-amz-server-side-encryption-context header must be valid base64-encoded JSON.")
    })?;

    serde_json::from_slice(&decoded).map(Some).map_err(|_| {
        sse_invalid_argument("The x-amz-server-side-encryption-context header must be a base64-encoded JSON object.")
    })
}

/// Rejects a client encryption context that would replace the location binding.
///
/// The request context binds the data key to `{bucket: bucket/key}` only when the
/// client supplied no entry under that name, and only the client context is
/// persisted. A client entry keyed by the bucket name therefore travels with the
/// object and replaces the binding on every read, so the envelope would no longer
/// be tied to the object's location. Only the write side rejects it: objects stored
/// with such an entry stay readable because the read side rebuilds the context
/// unchanged.
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

#[inline]
pub fn validate_sse_headers_for_write(
    server_side_encryption: Option<&str>,
    ssekms_key_id: Option<&String>,
    ssekms_context: Option<&HashMap<String, String>>,
    sse_customer_algorithm: Option<&String>,
    sse_customer_key: Option<&String>,
    sse_customer_key_md5: Option<&String>,
    require_sse_customer_key: bool,
) -> Result<(), SseError> {
    if let Some(sse) = server_side_encryption {
        let s = sse;
        if s != AMZ_ENCRYPTION_AES && s != AMZ_ENCRYPTION_KMS {
            return Err(sse_invalid_argument(
                "The SSE algorithm specified is not supported. The valid values are AES256 or aws:kms.",
            ));
        }
    }

    let has_ssec_headers = sse_customer_algorithm.is_some() || sse_customer_key.is_some() || sse_customer_key_md5.is_some();
    let has_managed_headers = server_side_encryption.is_some() || ssekms_key_id.is_some() || ssekms_context.is_some();

    if has_ssec_headers {
        if has_managed_headers {
            return Err(sse_invalid_argument(
                "The SSE-C and managed server-side encryption headers cannot be used together.",
            ));
        }

        let has_valid_ssec_headers = if require_sse_customer_key {
            matches!(
                (sse_customer_algorithm, sse_customer_key, sse_customer_key_md5),
                (Some(_), Some(_), Some(_))
            )
        } else {
            matches!((sse_customer_algorithm, sse_customer_key_md5), (Some(_), Some(_)))
        };

        if !has_valid_ssec_headers {
            let message = if require_sse_customer_key {
                "Missing SSE-C parameters. Algorithm, customer key and customer key MD5 are all required."
            } else {
                "Missing SSE-C parameters. Algorithm and customer key MD5 are required."
            };

            return Err(ssec_invalid_request(message));
        }
    }

    if ssekms_key_id.is_some() && server_side_encryption.is_none_or(|sse| sse != AMZ_ENCRYPTION_KMS) {
        return Err(sse_invalid_argument(
            "The SSE-KMS key ID header can only be used when x-amz-server-side-encryption is set to aws:kms.",
        ));
    }

    if ssekms_context.is_some() && server_side_encryption.is_none_or(|sse| sse != AMZ_ENCRYPTION_KMS) {
        return Err(sse_invalid_argument(
            "The SSE-KMS context header can only be used when x-amz-server-side-encryption is set to aws:kms.",
        ));
    }

    Ok(())
}

#[inline]
pub fn validate_sse_headers_for_read(metadata: &HashMap<String, String>, headers: &HeaderMap) -> Result<(), SseError> {
    let has_req_ssec = headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
        || headers.contains_key("x-amz-server-side-encryption-customer-key")
        || headers.contains_key("x-amz-server-side-encryption-customer-key-md5");

    let has_req_sse = headers.contains_key("x-amz-server-side-encryption")
        || headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id")
        || headers.contains_key("x-amz-server-side-encryption-context");

    let is_object_ssec = stored_ssec_metadata(metadata);
    let is_object_sse = metadata.contains_key("x-amz-server-side-encryption");

    if is_object_ssec {
        if has_req_sse {
            return Err(sse_invalid_argument(
                "Server-side encryption headers cannot be used with an object encrypted using SSE-C.",
            ));
        }
        return Ok(());
    }

    if is_object_sse && has_req_ssec {
        return Err(sse_invalid_argument(
            "SSE-C headers cannot be used with an object encrypted using server-side managed encryption.",
        ));
    }

    if has_req_ssec {
        return Err(ssec_invalid_request(
            "The object was stored without SSE-C. The correct SSE-C parameters must not be provided.",
        ));
    }

    if has_req_sse {
        return Err(sse_invalid_argument(
            "The object is not encrypted with server-side encryption. Do not provide server-side encryption headers.",
        ));
    }

    Ok(())
}

/// The public `x-amz-server-side-encryption` value a managed scheme reports.
pub(crate) fn managed_sse_public_header(sse_type: SSEType) -> &'static str {
    match sse_type {
        SSEType::SseKms => AMZ_ENCRYPTION_KMS,
        // SSE-C never reaches the managed path; reporting AES256 keeps this
        // total without inventing a third public value.
        SSEType::SseS3 | SSEType::SseC => AMZ_ENCRYPTION_AES,
    }
}

pub fn build_ssec_read_headers(algorithm: Option<&String>, key: Option<&String>, key_md5: Option<&String>) -> HeaderMap {
    let mut headers = HeaderMap::new();

    if let Some(algorithm) = algorithm
        && let Ok(mut value) = HeaderValue::from_str(algorithm.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, value);
    }

    if let Some(key) = key
        && let Ok(mut value) = HeaderValue::from_str(key.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY, value);
    }

    if let Some(key_md5) = key_md5
        && let Ok(mut value) = HeaderValue::from_str(key_md5.as_str())
    {
        value.set_sensitive(true);
        headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, value);
    }

    headers
}

/// Response headers derived for a decrypted read without unwrapping the data key.
#[derive(Debug)]
pub struct SseReadResponseHeaders {
    pub server_side_encryption: String,
    pub sse_customer_algorithm: Option<String>,
    pub sse_customer_key_md5: Option<String>,
    pub ssekms_key_id: Option<String>,
}

fn invalid_sse_response_metadata() -> SseError {
    SseError {
        code: S3ErrorCode::InternalError,
        message: "Persisted object encryption metadata is invalid.".to_string(),
        source: None,
    }
}

fn insert_sse_response_header(headers: &mut HeaderMap, name: HeaderName, value: &str) -> Result<(), SseError> {
    let value = HeaderValue::from_str(value).map_err(|_| invalid_sse_response_metadata())?;
    headers.insert(name, value);
    Ok(())
}

/// Project response headers after the caller has completed its existing read
/// validation and KMS authorization path.
pub fn project_sse_read_response_headers(
    metadata: &HashMap<String, String>,
    sse_customer_algorithm: Option<&String>,
    sse_customer_key_md5: Option<&String>,
) -> Result<HeaderMap, SseError> {
    if metadata.keys().any(|key| {
        key.eq_ignore_ascii_case(AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
            || key.eq_ignore_ascii_case(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT)
    }) {
        return Err(invalid_sse_response_metadata());
    }

    let metadata = normalize_encryption_metadata_case(metadata).map_err(|_| invalid_sse_response_metadata())?;
    let public_mode = metadata.get("x-amz-server-side-encryption").map(String::as_str);
    let customer_algorithm = metadata.get(SSEC_ALGORITHM_HEADER).map(String::as_str);

    let public_mode = match public_mode {
        Some(AMZ_ENCRYPTION_AES) => Some(SSEType::SseS3),
        Some(AMZ_ENCRYPTION_KMS) => Some(SSEType::SseKms),
        Some(_) => return Err(invalid_sse_response_metadata()),
        None => None,
    };
    if customer_algorithm.is_some_and(|algorithm| algorithm != AMZ_ENCRYPTION_AES) {
        return Err(invalid_sse_response_metadata());
    }

    let mode = if customer_algorithm.is_some() {
        if public_mode == Some(SSEType::SseKms) {
            return Err(invalid_sse_response_metadata());
        }
        Some(SSEType::SseC)
    } else {
        public_mode
    };
    for marker_mode in [
        metadata
            .contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
            .then_some(SSEType::SseC),
        metadata
            .contains_key(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
            .then_some(SSEType::SseS3),
        metadata
            .contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
            .then_some(SSEType::SseKms),
    ]
    .into_iter()
    .flatten()
    {
        if mode != Some(marker_mode) {
            return Err(invalid_sse_response_metadata());
        }
    }
    let Some(mode) = mode else {
        if metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
        {
            return Err(invalid_sse_response_metadata());
        }
        return Ok(HeaderMap::new());
    };

    let mut key_id = None;
    for value in [
        metadata.get("x-amz-server-side-encryption-aws-kms-key-id"),
        metadata.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER),
        metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER),
    ]
    .into_iter()
    .flatten()
    {
        if key_id.is_some_and(|current| current != value) {
            return Err(invalid_sse_response_metadata());
        }
        key_id = Some(value);
    }

    let normalized_managed = normalize_managed_metadata(metadata.as_ref(), Some(recode_minio_kms_context));
    let mut response_headers = HeaderMap::with_capacity(3);
    match mode {
        SSEType::SseS3 => {
            if metadata.contains_key("x-amz-server-side-encryption-aws-kms-key-id") || metadata.contains_key(SSEC_KEY_MD5_HEADER)
            {
                return Err(invalid_sse_response_metadata());
            }
            response_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static(AMZ_ENCRYPTION_AES));
        }
        SSEType::SseKms => {
            if metadata.contains_key(SSEC_KEY_MD5_HEADER) {
                return Err(invalid_sse_response_metadata());
            }
            let key_id = key_id
                .map(String::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(invalid_sse_response_metadata)?;
            response_headers.insert(X_AMZ_SERVER_SIDE_ENCRYPTION, HeaderValue::from_static(AMZ_ENCRYPTION_KMS));
            insert_sse_response_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID, key_id)?;

            if metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
                && !normalized_managed.contains_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER)
            {
                return Err(invalid_sse_response_metadata());
            }
            if let Some(context) = normalized_managed.get(INTERNAL_ENCRYPTION_CONTEXT_HEADER) {
                serde_json::from_str::<HashMap<String, String>>(context).map_err(|_| invalid_sse_response_metadata())?;
                let encoded = BASE64_STANDARD.encode_to_string(context);
                insert_sse_response_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT, &encoded)?;
            }
        }
        SSEType::SseC => {
            if key_id.is_some() || normalized_managed.contains_key(INTERNAL_ENCRYPTION_CONTEXT_HEADER) {
                return Err(invalid_sse_response_metadata());
            }
            let algorithm = sse_customer_algorithm
                .map(|value| value.as_ref())
                .filter(|value| *value == AMZ_ENCRYPTION_AES)
                .ok_or_else(|| {
                    ssec_invalid_request(
                        "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
                    )
                })?;
            let key_md5 = sse_customer_key_md5
                .map(|value| value.as_ref())
                .ok_or_else(|| ssec_invalid_request("Missing SSE-C customer key MD5."))?;
            let stored_md5 = metadata.get(SSEC_KEY_MD5_HEADER).ok_or_else(invalid_sse_response_metadata)?;
            if stored_md5 != key_md5 {
                return Err(ssec_invalid_request(
                    "The provided encryption parameters did not match the ones used originally to encrypt the object.",
                ));
            }
            insert_sse_response_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM, algorithm)?;
            insert_sse_response_header(&mut response_headers, X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5, key_md5)?;
        }
    }
    Ok(response_headers)
}
