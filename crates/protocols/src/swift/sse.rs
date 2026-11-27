// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Server-side encryption (SSE) and per-key KMS authorization for Swift operations.
//!
//! Enforces two invariants matching the S3 data path:
//! 1. Swift writes honor the mapped bucket's default SSE configuration (SSE-S3 or SSE-KMS)
//!    and fail closed when that configuration exists but cannot be read or encrypted.
//! 2. Swift reads authorize `kms:Decrypt` against the caller's principal before ECStore
//!    unwraps the stored DEK, so denied requests never trigger KMS decryption.

use super::storage_api::SwiftWriteEncryption;
use super::{SwiftError, SwiftResult, get_swift_bucket_sse_config};
use async_trait::async_trait;
use rustfs_credentials::Credentials;
use rustfs_kms::KmsServiceManager;
use rustfs_kms::service::ObjectEncryptionService;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::{Action, KmsAction};
use rustfs_s3_types::S3ErrorCode;
use rustfs_sse::{
    BucketDefaultEncryption, BucketEncryption, EncryptionKeyKind, EncryptionMaterial, EncryptionRequest,
    KMS_KEY_NOT_FOUND_ERROR_CODE, KmsKeyAuthorizer, SseError, SseHost, SseKmsPrincipal, authorize_sse_kms_object_read,
    encryption_material_to_metadata, sse_encryption, strip_managed_encryption_metadata,
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

const LOG_COMPONENT_PROTOCOLS: &str = "protocols";
const LOG_SUBSYSTEM_SWIFT_SSE: &str = "swift_sse";
const EVENT_SWIFT_SSE_STATE: &str = "swift_sse_state";

/// Swift's [`SseHost`] implementation backed by the process's bucket metadata,
/// KMS service manager, and IAM policy evaluator.
pub(crate) struct SwiftSseHost;

static SWIFT_SSE_HOST: SwiftSseHost = SwiftSseHost;

#[async_trait]
impl SseHost for SwiftSseHost {
    async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
        let manager = rustfs_kms::get_global_kms_service_manager()?;
        manager.get_encryption_service().await
    }

    fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
        rustfs_kms::get_global_kms_service_manager()
    }

    async fn bucket_encryption(&self, bucket: &str) -> Result<Option<BucketEncryption>, SseError> {
        match get_swift_bucket_sse_config(bucket).await {
            Ok(Some(config)) => Ok(Some(BucketEncryption {
                default_rule: config
                    .rules
                    .first()
                    .and_then(|rule| rule.apply_server_side_encryption_by_default.as_ref())
                    .map(|sse| BucketDefaultEncryption {
                        sse_algorithm: sse.sse_algorithm.as_str().to_string(),
                        kms_master_key_id: sse.kms_master_key_id.clone(),
                    }),
            })),
            Ok(None) => Ok(None),
            Err(err) => Err(SseError {
                code: S3ErrorCode::ServiceUnavailable,
                message: format!("Failed to load bucket default encryption configuration: {err}"),
                source: None,
            }),
        }
    }

    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        &SwiftIamKmsKeyAuthorizer
    }
}

struct SwiftIamKmsKeyAuthorizer;

#[async_trait]
impl KmsKeyAuthorizer for SwiftIamKmsKeyAuthorizer {
    async fn is_allowed(&self, principal: &SseKmsPrincipal, action: KmsAction, key_id: &str) -> Result<bool, SseError> {
        let iam_store = rustfs_iam::get().map_err(|err| SseError {
            code: S3ErrorCode::ServiceUnavailable,
            message: format!("KMS key authorization requires IAM: {err}"),
            source: None,
        })?;

        Ok(iam_store
            .is_allowed(&Args {
                account: principal.account(),
                groups: principal.groups(),
                action: Action::KmsAction(action),
                // Call-site contract of `Statement::kms_key_scope_matches`: the requested key
                // identifier travels in `object` with `bucket` left empty.
                bucket: "",
                object: key_id,
                conditions: principal.conditions(),
                is_owner: principal.is_owner(),
                claims: principal.claims(),
                deny_only: false,
            })
            .await)
    }
}

/// Build an [`SseKmsPrincipal`] for a Swift request.
///
/// Always returns a client principal (never `None`, which `rustfs-sse` reserves for
/// internal system tasks). Uncredentialed requests (e.g., TempURL) map to an empty
/// account so enforced KMS key policies fail closed with 403 Forbidden.
pub(crate) fn swift_sse_principal(credentials: Option<&Credentials>) -> SseKmsPrincipal {
    match credentials {
        Some(creds) => {
            let is_owner = rustfs_iam::is_root_access_key(&creds.access_key);
            let mut claims = creds.claims.clone().unwrap_or_default();
            claims.insert("principal".to_string(), serde_json::Value::String(creds.access_key.clone()));
            SseKmsPrincipal::new(creds.access_key.clone(), creds.groups.clone(), is_owner, claims, HashMap::new())
        }
        None => SseKmsPrincipal::new(String::new(), None, false, HashMap::new(), HashMap::new()),
    }
}

/// Resolved encryption plan and storage metadata for a Swift object write.
pub(crate) struct PreparedSwiftWriteEncryption {
    pub(crate) write_encryption: SwiftWriteEncryption,
    pub(crate) metadata: HashMap<String, String>,
}

fn material_to_write_encryption(material: &EncryptionMaterial) -> SwiftWriteEncryption {
    match material.key_kind {
        EncryptionKeyKind::Object => SwiftWriteEncryption::singlepart_object_key(material.key_bytes),
        EncryptionKeyKind::Direct => SwiftWriteEncryption::singlepart(material.key_bytes, material.base_nonce),
    }
}

/// Map an [`SseError`] into a sanitized [`SwiftError`].
pub(crate) fn map_sse_error(operation: &str, err: SseError) -> SwiftError {
    match &err.code {
        S3ErrorCode::AccessDenied => SwiftError::Forbidden("Access denied".to_string()),
        S3ErrorCode::ServiceUnavailable => {
            error!(
                event = EVENT_SWIFT_SSE_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SWIFT_SSE,
                operation = %operation,
                code = ?err.code,
                error = %err,
                result = "unavailable",
                "swift sse state changed"
            );
            SwiftError::ServiceUnavailable("Encryption service unavailable".to_string())
        }
        S3ErrorCode::InvalidRequest | S3ErrorCode::InvalidArgument => {
            error!(
                event = EVENT_SWIFT_SSE_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SWIFT_SSE,
                operation = %operation,
                code = ?err.code,
                error = %err,
                result = "invalid_request",
                "swift sse state changed"
            );
            SwiftError::BadRequest("Server-side encryption configuration invalid".to_string())
        }
        S3ErrorCode::Custom(code) if &**code == KMS_KEY_NOT_FOUND_ERROR_CODE => {
            error!(
                event = EVENT_SWIFT_SSE_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SWIFT_SSE,
                operation = %operation,
                code = ?err.code,
                error = %err,
                result = "key_not_found",
                "swift sse state changed"
            );
            SwiftError::BadRequest("Configured KMS key not found".to_string())
        }
        S3ErrorCode::NotImplemented => SwiftError::NotImplemented("Requested encryption mode not implemented".to_string()),
        _ => {
            error!(
                event = EVENT_SWIFT_SSE_STATE,
                component = LOG_COMPONENT_PROTOCOLS,
                subsystem = LOG_SUBSYSTEM_SWIFT_SSE,
                operation = %operation,
                code = ?err.code,
                error = %err,
                result = "failed",
                "swift sse state changed"
            );
            SwiftError::InternalServerError(format!("{operation} encryption operation failed"))
        }
    }
}

async fn bucket_has_default_sse_with_host(host: &dyn SseHost, bucket: &str) -> SwiftResult<bool> {
    let encryption = host
        .bucket_encryption(bucket)
        .await
        .map_err(|err| map_sse_error("Bucket encryption lookup", err))?;
    Ok(encryption.and_then(|cfg| cfg.default_rule).is_some())
}

/// Strip source encryption metadata before writing a copied destination object.
pub(crate) fn strip_source_encryption_metadata(metadata: &mut HashMap<String, String>) {
    strip_managed_encryption_metadata(metadata);
}

/// Resolve bucket default SSE for a Swift write.
///
/// When default encryption applies and `content_size` is unknown (`None`), rejects
/// with `411 Length Required` so `x-rustfs-encryption-original-size` is never stored
/// with an unknown or ciphertext length.
pub(crate) async fn prepare_write_encryption(
    bucket: &str,
    key: &str,
    content_size: Option<u64>,
    credentials: Option<&Credentials>,
) -> SwiftResult<Option<PreparedSwiftWriteEncryption>> {
    let principal = swift_sse_principal(credentials);
    prepare_write_encryption_with_host(&SWIFT_SSE_HOST, bucket, key, content_size, &principal).await
}

async fn prepare_write_encryption_with_host(
    host: &dyn SseHost,
    bucket: &str,
    key: &str,
    content_size: Option<u64>,
    principal: &SseKmsPrincipal,
) -> SwiftResult<Option<PreparedSwiftWriteEncryption>> {
    let bucket_has_default = bucket_has_default_sse_with_host(host, bucket).await?;
    if !bucket_has_default {
        return Ok(None);
    }

    let known_size = content_size.ok_or_else(|| {
        SwiftError::LengthRequired("Content-Length is required when uploading to a bucket with default encryption".to_string())
    })?;
    let size_i64 =
        i64::try_from(known_size).map_err(|_| SwiftError::RequestEntityTooLarge("Object size exceeds maximum".to_string()))?;

    let request = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: size_i64,
        principal: Some(principal),
    };

    let Some(material) = sse_encryption(host, request)
        .await
        .map_err(|err| map_sse_error("Object write", err))?
    else {
        return Ok(None);
    };

    let metadata = encryption_material_to_metadata(&material).map_err(|err| map_sse_error("Object write metadata", err))?;
    Ok(Some(PreparedSwiftWriteEncryption {
        write_encryption: material_to_write_encryption(&material),
        metadata,
    }))
}

/// Authorize `kms:Decrypt` for a Swift object read before opening the decrypting reader.
pub(crate) async fn authorize_object_read(
    credentials: Option<&Credentials>,
    metadata: &HashMap<String, String>,
) -> SwiftResult<()> {
    let principal = swift_sse_principal(credentials);
    authorize_object_read_with_host(&SWIFT_SSE_HOST, &principal, metadata).await
}

async fn authorize_object_read_with_host(
    host: &dyn SseHost,
    principal: &SseKmsPrincipal,
    metadata: &HashMap<String, String>,
) -> SwiftResult<()> {
    authorize_sse_kms_object_read(host, Some(principal), metadata)
        .await
        .map_err(|err| map_sse_error("Object read", err))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct MockAuthorizer {
        allow: bool,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl KmsKeyAuthorizer for MockAuthorizer {
        async fn is_allowed(&self, _principal: &SseKmsPrincipal, _action: KmsAction, _key_id: &str) -> Result<bool, SseError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.allow)
        }
    }

    struct MockHost {
        bucket_encryption: Result<Option<BucketEncryption>, S3ErrorCode>,
        authorizer: MockAuthorizer,
    }

    #[async_trait]
    impl SseHost for MockHost {
        async fn encryption_service(&self) -> Option<Arc<ObjectEncryptionService>> {
            None
        }

        fn kms_service_manager(&self) -> Option<Arc<KmsServiceManager>> {
            None
        }

        async fn bucket_encryption(&self, _bucket: &str) -> Result<Option<BucketEncryption>, SseError> {
            match &self.bucket_encryption {
                Ok(cfg) => Ok(cfg.clone()),
                Err(code) => Err(SseError {
                    code: code.clone(),
                    message: "simulated bucket encryption lookup failure".to_string(),
                    source: None,
                }),
            }
        }

        fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
            &self.authorizer
        }
    }

    fn kms_default_bucket_encryption(key_id: &str) -> BucketEncryption {
        BucketEncryption {
            default_rule: Some(BucketDefaultEncryption {
                sse_algorithm: "aws:kms".to_string(),
                kms_master_key_id: Some(key_id.to_string()),
            }),
        }
    }

    #[tokio::test]
    async fn prepare_write_encryption_returns_none_without_bucket_default() {
        let host = MockHost {
            bucket_encryption: Ok(None),
            authorizer: MockAuthorizer {
                allow: true,
                calls: Arc::new(AtomicUsize::new(0)),
            },
        };
        let principal = swift_sse_principal(None);
        let prepared = prepare_write_encryption_with_host(&host, "bucket", "key", None, &principal)
            .await
            .expect("plaintext bucket should succeed without content-length");
        assert!(prepared.is_none());
    }

    #[tokio::test]
    async fn prepare_write_encryption_fails_closed_when_bucket_config_is_unreadable() {
        let host = MockHost {
            bucket_encryption: Err(S3ErrorCode::ServiceUnavailable),
            authorizer: MockAuthorizer {
                allow: true,
                calls: Arc::new(AtomicUsize::new(0)),
            },
        };
        let principal = swift_sse_principal(None);
        let err = prepare_write_encryption_with_host(&host, "bucket", "key", Some(16), &principal)
            .await
            .err()
            .expect("unreadable bucket encryption must fail closed");
        assert!(matches!(err, SwiftError::ServiceUnavailable(_)));
    }

    #[tokio::test]
    async fn prepare_write_encryption_requires_content_length_when_default_sse_enabled() {
        let host = MockHost {
            bucket_encryption: Ok(Some(kms_default_bucket_encryption("key-1"))),
            authorizer: MockAuthorizer {
                allow: true,
                calls: Arc::new(AtomicUsize::new(0)),
            },
        };
        let principal = swift_sse_principal(None);
        let err = prepare_write_encryption_with_host(&host, "bucket", "key", None, &principal)
            .await
            .err()
            .expect("missing content-length on encrypted bucket must fail closed");
        assert!(matches!(err, SwiftError::LengthRequired(_)));
    }

    #[tokio::test]
    async fn prepare_write_encryption_fails_closed_when_kms_default_has_no_kms_service() {
        let host = MockHost {
            bucket_encryption: Ok(Some(kms_default_bucket_encryption("key-1"))),
            authorizer: MockAuthorizer {
                allow: true,
                calls: Arc::new(AtomicUsize::new(0)),
            },
        };
        let principal = swift_sse_principal(None);
        let err = prepare_write_encryption_with_host(&host, "bucket", "key", Some(32), &principal)
            .await
            .err()
            .expect("SSE-KMS without KMS service must fail closed instead of storing plaintext");
        assert!(matches!(err, SwiftError::BadRequest(_) | SwiftError::ServiceUnavailable(_)));
    }

    #[test]
    fn swift_sse_principal_never_returns_system_exempt_identity() {
        let anon = swift_sse_principal(None);
        assert_eq!(anon.account(), "");
        assert!(!anon.is_owner());

        let creds = Credentials {
            access_key: "keystone:alice".to_string(),
            ..Default::default()
        };
        let user = swift_sse_principal(Some(&creds));
        assert_eq!(user.account(), "keystone:alice");
        assert_eq!(user.claims().get("principal").and_then(|v| v.as_str()), Some("keystone:alice"));
    }

    #[test]
    fn map_sse_error_maps_access_denied_to_forbidden() {
        let err = map_sse_error(
            "Object read",
            SseError {
                code: S3ErrorCode::AccessDenied,
                message: "Access Denied".to_string(),
                source: None,
            },
        );
        assert!(matches!(err, SwiftError::Forbidden(_)));
    }
}
