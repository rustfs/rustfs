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

//! Which SSE scheme and which key a request resolves to: the request first, then the bucket default.

use crate::core::s3::headers::validate_sse_headers_for_write;
use crate::error::SseError;
use crate::host::SseHost;
use crate::{LOG_COMPONENT_STORAGE, LOG_SUBSYSTEM_SSE};
use rustfs_utils::http::headers::{AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS};
use rustfs_utils::http::object_encryption_keys::SSEType;
use std::collections::HashMap;
use tracing::debug;

pub const DEFAULT_SSE_ALGORITHM: &str = "AES256";

pub(crate) const SUPPORT_SSE_ALGORITHMS: &[&str] = &[DEFAULT_SSE_ALGORITHM];

/// SSE configuration resolved from request and bucket defaults
#[derive(Debug)]
pub(crate) struct SseConfiguration {
    /// Effective server-side encryption algorithm (after considering bucket defaults)
    pub effective_sse: String,
    /// Effective KMS key ID (after considering bucket defaults)
    pub effective_kms_key_id: Option<String>,
}

/// Managed SSE resolved from a bucket default encryption rule on a write path.
///
/// The single mapping shared by every writer: the resolver in this crate, and
/// the server's PUT, COPY and extract paths. Unknown algorithms fall back to AES256 rather
/// than to `None`. Resolving `None` instead lets a same-name copy under a
/// malformed bucket default pass the `copy_changes_encryption` guard and take
/// the metadata-only shortcut while this layer still encrypts: fresh DEK
/// metadata is committed beside the untouched plaintext blocks and the object
/// becomes unreadable. PutBucketEncryption refuses unknown algorithms, so this
/// is reachable only through a configuration stored before that check or
/// through hand-edited bucket metadata (backlog#1826).
pub fn bucket_default_write_sse(sse_algorithm: &str) -> String {
    match sse_algorithm {
        "AES256" => String::from(AMZ_ENCRYPTION_AES),
        "aws:kms" => String::from(AMZ_ENCRYPTION_KMS),
        _ => String::from(AMZ_ENCRYPTION_AES),
    }
}

/// Prepare SSE configuration by resolving request parameters with bucket defaults
///
/// This function:
/// 1. Queries bucket default encryption configuration
/// 2. Resolves effective encryption (request overrides bucket default)
/// 3. Prepares metadata headers for managed SSE
///
/// # Arguments
/// * `bucket` - Bucket name
/// * `server_side_encryption` - SSE algorithm from request (SSE-S3 or SSE-KMS)
/// * `ssekms_key_id` - KMS key ID from request
/// * `sse_customer_algorithm` - SSE-C algorithm from request
///
/// # Returns
/// `SseConfiguration` with resolved encryption parameters and metadata headers
pub(crate) async fn prepare_sse_configuration(
    host: &dyn SseHost,
    bucket: &str,
    server_side_encryption: Option<String>,
    ssekms_key_id: Option<String>,
) -> Result<Option<SseConfiguration>, SseError> {
    if let Some(server_side_encryption) = server_side_encryption.clone()
        && server_side_encryption.as_str() == AMZ_ENCRYPTION_AES
    {
        return Ok(Some(SseConfiguration {
            effective_sse: server_side_encryption,
            effective_kms_key_id: None,
        }));
    }

    if let Some(server_side_encryption) = server_side_encryption.clone()
        && let Some(ssekms_key_id) = ssekms_key_id
    {
        return Ok(Some(SseConfiguration {
            effective_sse: server_side_encryption,
            effective_kms_key_id: Some(ssekms_key_id),
        }));
    }

    // Get bucket default encryption configuration.
    let bucket_encryption = host.bucket_encryption(bucket).await;
    debug!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "bucket_sse_config_lookup",
        bucket = %bucket,
        found = matches!(bucket_encryption, Ok(Some(_))),
        "Bucket SSE configuration lookup completed"
    );

    let Some(bucket_encryption) = bucket_encryption? else {
        // The bucket has no SSE config. If the user explicitly requested
        // aws:kms, we must honor that — return the explicit SSE header so
        // downstream logic can try (and fail if KMS is unavailable).
        return Ok(server_side_encryption.map(|sse| SseConfiguration {
            effective_sse: sse,
            effective_kms_key_id: ssekms_key_id,
        }));
    };

    let default_rule = bucket_encryption.default_rule.as_ref();
    let effective_sse = server_side_encryption.clone().or_else(|| {
        default_rule.map(|sse| {
            debug!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_SSE,
                event = "bucket_sse_default_applied",
                bucket = %bucket,
                algorithm = sse.sse_algorithm.as_str(),
                has_kms_key_id = sse.kms_master_key_id.is_some(),
                "Bucket SSE default resolved"
            );
            bucket_default_write_sse(&sse.sse_algorithm)
        })
    });
    let Some(effective_sse) = effective_sse else {
        return Ok(None);
    };

    debug!(
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_SSE,
        event = "effective_sse_resolved",
        bucket = %bucket,
        requested = ?server_side_encryption,
        effective = ?effective_sse,
        "Resolved effective SSE configuration"
    );

    let effective_kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), ssekms_key_id, || {
        default_rule.and_then(|sse| sse.kms_master_key_id.clone())
    });

    Ok(Some(SseConfiguration {
        effective_sse,
        effective_kms_key_id,
    }))
}

pub(crate) fn resolve_effective_kms_key_id<F>(
    effective_sse: Option<&str>,
    requested_kms_key_id: Option<String>,
    bucket_default_kms_key_id: F,
) -> Option<String>
where
    F: FnOnce() -> Option<String>,
{
    if effective_sse.is_none_or(|sse| sse != AMZ_ENCRYPTION_KMS) {
        return requested_kms_key_id;
    }

    requested_kms_key_id.or_else(bucket_default_kms_key_id)
}

/// The SSE headers of a write request, as the S3 layer received them.
pub(crate) struct WriteSchemeHeaders<'a> {
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) ssekms_context: Option<&'a HashMap<String, String>>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
}

/// The scheme a write resolved to, in S3's terms.
#[derive(Clone)]
pub(crate) enum WriteScheme {
    /// `AES256`: the server chooses the wrapping key and the caller names none.
    SseS3,
    /// `aws:kms`, under the key the request or the bucket default named, if either did.
    SseKms { key_id: Option<String> },
    /// A key the client supplies; only its MD5 is ever stored.
    SseC {
        algorithm: String,
        key: String,
        key_md5: String,
    },
}

impl std::fmt::Debug for WriteScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::SseS3 => "SseS3",
            Self::SseKms { .. } => "SseKms(..)",
            Self::SseC { .. } => "SseC(..)",
        })
    }
}

/// Decide the scheme of a write: validate the headers, then the request first and the
/// bucket default second.
///
/// Every write goes through here, an object and a multipart session alike, so the two
/// cannot resolve the same headers differently.
pub(crate) async fn resolve_write_scheme(
    host: &dyn SseHost,
    bucket: &str,
    headers: WriteSchemeHeaders<'_>,
) -> Result<Option<WriteScheme>, SseError> {
    validate_sse_headers_for_write(
        headers.server_side_encryption.as_deref(),
        headers.ssekms_key_id.as_ref(),
        headers.ssekms_context,
        headers.sse_customer_algorithm.as_ref(),
        headers.sse_customer_key.as_ref(),
        headers.sse_customer_key_md5.as_ref(),
        true,
    )?;

    // Validation admits SSE-C headers only as a complete set, and never beside the
    // managed ones, so their presence alone decides.
    if let (Some(algorithm), Some(key), Some(key_md5)) =
        (headers.sse_customer_algorithm, headers.sse_customer_key, headers.sse_customer_key_md5)
    {
        return Ok(Some(WriteScheme::SseC { algorithm, key, key_md5 }));
    }

    let Some(config) = prepare_sse_configuration(host, bucket, headers.server_side_encryption, headers.ssekms_key_id).await?
    else {
        return Ok(None);
    };

    Ok(match config.effective_sse.as_str() {
        AMZ_ENCRYPTION_AES => {
            // Validation admits a key id only beside `aws:kms`, and the bucket default's
            // key is consulted only for `aws:kms`.
            debug_assert!(config.effective_kms_key_id.is_none(), "SSE-S3 never names a KMS key");
            Some(WriteScheme::SseS3)
        }
        AMZ_ENCRYPTION_KMS => Some(WriteScheme::SseKms {
            key_id: config.effective_kms_key_id,
        }),
        _ => None,
    })
}

/// Scheme a managed-SSE header names, defaulting to SSE-S3 the same way the
/// encryption and decryption paths do.
pub(crate) fn managed_sse_type(server_side_encryption: &str) -> SSEType {
    match server_side_encryption {
        AMZ_ENCRYPTION_KMS => SSEType::SseKms,
        _ => SSEType::SseS3,
    }
}

/// Key id an SSE-KMS read authorizes and reports when the stored metadata
/// carries none (legacy envelopes and MinIO-written objects predate the
/// explicit key-id headers).
///
/// The KMS service's configured default key is the key such envelopes were
/// actually wrapped under, so per-key authorization must target it — the bare
/// literal `default` would silently escape any policy narrowed to real key
/// ids. The literal remains only as the last resort when no service is
/// running, matching what the legacy write path recorded implicitly.
pub(crate) async fn resolve_stored_kms_key_id(host: &dyn SseHost, stored: Option<String>) -> String {
    if let Some(key_id) = stored {
        return key_id;
    }
    if let Some(service) = host.encryption_service().await
        && let Some(key_id) = service.get_default_key_id()
    {
        return key_id.clone();
    }
    "default".to_string()
}

/// Check if the server_side_encryption is a managed SSE type (SSE-S3 or SSE-KMS)
#[inline]
pub(crate) fn is_managed_sse(server_side_encryption: &str) -> bool {
    matches!(server_side_encryption, "AES256" | "aws:kms")
}
