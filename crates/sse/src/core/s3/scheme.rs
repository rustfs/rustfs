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

use crate::error::SseError;
use crate::host::SseHost;
use crate::{LOG_COMPONENT_STORAGE, LOG_SUBSYSTEM_SSE};
use rustfs_utils::http::headers::{AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS};
use rustfs_utils::http::object_encryption_keys::SSEType;
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

#[derive(Clone)]
pub enum SseTypeV2 {
    SseS3(String),
    SseKms(String, Option<String>),
    SseC(String, String, String),
}

impl std::fmt::Debug for SseTypeV2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::SseS3(_) => "SseS3(..)",
            Self::SseKms(_, _) => "SseKms(..)",
            Self::SseC(_, _, _) => "SseC(..)",
        })
    }
}

pub(crate) async fn prepare_sse_configuration_v2(
    host: &dyn SseHost,
    bucket: &str,
    server_side_encryption: Option<String>,
    customer_algorithm: Option<String>,
    customer_key: Option<String>,
    customer_key_md5: Option<String>,
    ssekms_key_id: Option<String>,
) -> Result<Option<SseTypeV2>, SseError> {
    if let Some(customer_algorithm) = customer_algorithm
        && let Some(customer_key_md5) = customer_key_md5
    {
        // if create_multipart_upload request, customer_key is not provided
        let customer_key = customer_key.unwrap_or_default();

        return Ok(Some(SseTypeV2::SseC(customer_algorithm, customer_key, customer_key_md5)));
    }

    let sse_config = prepare_sse_configuration(host, bucket, server_side_encryption, ssekms_key_id).await?;

    if let Some(sse_config) = sse_config {
        return match sse_config.effective_sse.as_str() {
            AMZ_ENCRYPTION_AES => Ok(Some(SseTypeV2::SseS3(sse_config.effective_sse))),
            AMZ_ENCRYPTION_KMS => Ok(Some(SseTypeV2::SseKms(sse_config.effective_sse.clone(), sse_config.effective_kms_key_id))),
            _ => Ok(None),
        };
    }

    Ok(None)
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
