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

use crate::core::keying::context::{build_kms_request_context, encode_minio_kms_context};
use crate::core::keying::direct::generate_ssec_nonce;
use crate::core::keying::material::{DecryptionMaterial, EncryptionKeyKind, EncryptionMaterial};
use crate::core::keying::metadata::{
    encryption_material_to_metadata, mark_encrypted_multipart_metadata, recode_minio_kms_context,
    strip_managed_encryption_metadata,
};
#[cfg(feature = "rio-v2")]
use crate::core::keying::object::{
    DARE_CIPHER_AES_256_GCM, DARE_CIPHER_CHACHA20_POLY1305, MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM, SEALED_KEY_IV_SIZE,
    SEALED_KEY_SIZE, is_legacy_rustfs_managed_metadata, is_supported_sealed_object_key_cipher,
};
use crate::core::read::{DecryptionRequest, apply_managed_decryption_material, classify_sse_read_response, sse_decryption};
use crate::core::request::SseKmsPrincipal;
use crate::core::rewrap::{ObjectDekRewrapOutcome, rewrap_object_encryption_metadata};
use crate::core::s3::authorization::authorize_sse_kms_object_read;
use crate::core::s3::headers::{
    extract_server_side_encryption_from_headers, extract_ssec_params_from_headers, extract_ssekms_context_from_headers,
    project_sse_read_response_headers, validate_sse_headers_for_read, validate_sse_headers_for_write,
};
use crate::core::s3::scheme::{is_managed_sse, resolve_effective_kms_key_id, resolve_stored_kms_key_id};
use crate::core::s3::ssec::{SsecParams, md5_base64, validate_ssec_for_read, validate_ssec_params, verify_ssec_key_match};
use crate::core::write::ManagedEncryptionRequest;
use crate::core::write::{
    EncryptionRequest, PrepareEncryptionRequest, apply_managed_encryption_material, apply_managed_encryption_material_inner,
    sse_encryption, sse_prepare_encryption,
};
use crate::error::INTERNAL_ERROR_MESSAGE;
use crate::error::{KMS_KEY_NOT_FOUND_ERROR_CODE, SseError};
use crate::host::KmsKeyAuthorizer;
use crate::kms::kms_backed::{KmsSseDekProvider, kms_operation_error};
use crate::kms::local::TestSseDekProvider;
use crate::kms::provider::{SseDekProvider, reset_sse_dek_provider};
use crate::testing::{RecordingKmsKeyAuthorizer, sse_kms_object_metadata};
use rustfs_kms::{DataKey, KmsUnavailableError};
use rustfs_policy::policy::action::KmsAction;
use rustfs_utils::http::headers::SSEC_ALGORITHM_HEADER;
use rustfs_utils::http::object_encryption_keys::{
    INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
    INTERNAL_ENCRYPTION_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER,
    MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, SSEC_ORIGINAL_SIZE_HEADER, SSEType, normalize_managed_metadata,
};

/// The server these tests run in: none.
///
/// No KMS is serving, bucket metadata cannot be read, and no policy subsystem
/// can decide a key permission — the state of a process in which no server
/// component has started. A test that needs a KMS injects a provider; a test
/// that needs an authorization decision attaches hooks to its principal.
struct DetachedHost;

static HOST: DetachedHost = DetachedHost;

struct NoPolicySubsystem;

#[async_trait]
impl KmsKeyAuthorizer for NoPolicySubsystem {
    async fn is_allowed(&self, _principal: &SseKmsPrincipal, _action: KmsAction, _key_id: &str) -> Result<bool, SseError> {
        Err(SseError::internal("KMS key authorization requires IAM: no policy subsystem is running"))
    }
}

#[async_trait]
impl crate::host::SseHost for DetachedHost {
    async fn encryption_service(&self) -> Option<Arc<rustfs_kms::service::ObjectEncryptionService>> {
        None
    }

    fn kms_service_manager(&self) -> Option<Arc<rustfs_kms::KmsServiceManager>> {
        None
    }

    async fn bucket_encryption(&self, _bucket: &str) -> Result<Option<crate::host::BucketEncryption>, SseError> {
        Err(SseError::internal("bucket metadata sys not initialized for this instance"))
    }

    fn kms_key_authorizer(&self) -> &dyn KmsKeyAuthorizer {
        &NoPolicySubsystem
    }
}

/// backlog#1643 PR-B0 acceptance guard: the managed-SSE classifier must
/// have exactly one definition — in the shared encryption-keys module —
/// so the scanner and the S3 layer can never disagree on attribution.
/// This module may only re-export or call it.
#[test]
fn managed_sse_classifier_has_exactly_one_definition() {
    let classifier_fns = [
        "contains_managed_encryption_metadata",
        "normalize_managed_metadata",
        "stored_managed_encryption_key",
    ];

    let sse_src = include_str!("lib.rs");
    let shared_src = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/../utils/src/http/object_encryption_keys.rs"))
        .expect("shared encryption-keys module should be readable");

    for name in classifier_fns {
        // Built at runtime so this test's own source cannot satisfy the scan.
        let definition = format!("fn {name}(");
        assert!(
            !sse_src.contains(&definition),
            "{name} must not be redefined in this crate; call the shared rustfs_utils::http::object_encryption_keys implementation instead"
        );
        assert_eq!(
            shared_src.matches(&definition).count(),
            1,
            "{name} must be defined exactly once, in the shared encryption-keys module"
        );
    }
}

#[test]
fn ssec_read_headers_are_sensitive() {
    let headers = crate::core::s3::headers::build_ssec_read_headers(
        Some(&"AES256".to_string()),
        Some(&"dHJhbnNwb3J0LXNlY3JldA==".to_string()),
        Some(&"bWQ1LXNlY3JldA==".to_string()),
    );

    assert_eq!(headers.len(), 3);
    assert!(headers.values().all(HeaderValue::is_sensitive));
    let debug = format!("{headers:?}");
    assert!(!debug.contains("dHJhbnNwb3J0LXNlY3JldA=="));
    assert!(!debug.contains("bWQ1LXNlY3JldA=="));
}

#[test]
fn parse_simple_sse_cmk_rejects_bad_keys_without_crashing() {
    // Empty / whitespace-only.
    assert!(crate::kms::local::parse_simple_sse_cmk("").is_err());
    assert!(crate::kms::local::parse_simple_sse_cmk("   ").is_err());
    // Not valid base64.
    assert!(crate::kms::local::parse_simple_sse_cmk("@@@not-base64@@@").is_err());
    // Valid base64 but wrong length (16 bytes).
    let short = BASE64_STANDARD.encode_to_string([1u8; 16]);
    assert!(crate::kms::local::parse_simple_sse_cmk(&short).is_err());
    // All-zero 32-byte key is rejected.
    let zero = BASE64_STANDARD.encode_to_string([0u8; 32]);
    assert!(crate::kms::local::parse_simple_sse_cmk(&zero).is_err());
}

#[test]
fn kms_operation_errors_preserve_retryability_classification() {
    let unavailable = kms_operation_error(rustfs_kms::KmsError::backend_error("connection refused"));
    let corrupt = kms_operation_error(rustfs_kms::KmsError::cryptographic_error("decrypt", "authentication failed"));
    let missing = kms_operation_error(rustfs_kms::KmsError::key_not_found("no-such-key"));

    assert_eq!(unavailable.code, S3ErrorCode::ServiceUnavailable);
    assert_eq!(corrupt.code, S3ErrorCode::InternalError);
    assert_eq!(missing.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
    assert_eq!(crate::core::audit::kms_data_plane_error_class(&missing), "key_not_found");
}

/// An unwrap the backend rejects stays an internal error, but says why in
/// words an operator can act on rather than the generic 500 text.
#[test]
fn kms_unwrap_error_keeps_500_but_names_the_envelope_mismatch() {
    let rejected =
        crate::kms::kms_backed::kms_unwrap_error(rustfs_kms::KmsError::cryptographic_error("decrypt", "authentication failed"));
    assert_eq!(rejected.code, S3ErrorCode::InternalError);
    assert!(rejected.message.contains("could not be unwrapped"), "message was {}", rejected.message);
    assert_eq!(crate::core::audit::kms_data_plane_error_class(&rejected), "cryptographic");

    let missing = crate::kms::kms_backed::kms_unwrap_error(rustfs_kms::KmsError::key_not_found("no-such-key"));
    assert_eq!(missing.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()));
    assert_eq!(missing.message, "KMS key not found: no-such-key");
}

#[test]
fn sse_kms_never_falls_back_to_the_local_sse_s3_provider() {
    let unconfigured = crate::core::write::sse_kms_unavailable_error(false);
    assert_eq!(unconfigured.code, S3ErrorCode::InvalidRequest);
    assert!(unconfigured.message.contains("SSE-KMS requires"));

    let stopped = crate::core::write::sse_kms_unavailable_error(true);
    assert_eq!(stopped.code, S3ErrorCode::ServiceUnavailable);
}

fn managed_write(algorithm: &'static str, kms_key_id: Option<&str>) -> EncryptionRequest<'static> {
    EncryptionRequest {
        bucket: "finance",
        key: "ledger.csv",
        server_side_encryption: Some(String::from(algorithm)),
        ssekms_key_id: kms_key_id.map(str::to_string),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 128,
        principal: None,
    }
}

/// A bucket default naming a KMS key, on a node with no KMS, used to write
/// the object under the local master key while stamping `aws:kms` and that
/// never-consulted key id into the metadata (backlog#2368 B4).
#[tokio::test]
async fn sse_kms_write_is_refused_when_only_a_local_master_key_is_available() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(BASE64_STANDARD.encode_to_string([9u8; 32]))),
        ],
        async {
            let error = sse_encryption(
                &HOST,
                managed_write(AMZ_ENCRYPTION_KMS, Some("arn:aws:kms:us-east-1:123:key/nonexistent")),
            )
            .await
            .expect_err("SSE-KMS must not be served by the local master key");

            assert_eq!(error.code, S3ErrorCode::InvalidRequest);
            assert!(error.message.contains("SSE-KMS requires"), "message was {}", error.message);
        },
    )
    .await;

    reset_sse_dek_provider();
}

/// Without a master key the local fallback fails with an SSE-S3-worded
/// configuration error. An SSE-KMS request must not be told to set
/// `RUSTFS_SSE_S3_MASTER_KEY`.
#[tokio::test]
async fn sse_kms_refusal_names_sse_kms_rather_than_the_sse_s3_master_key() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
            ("RUSTFS_SSE_S3_MASTER_KEY", None::<String>),
        ],
        async {
            let error = sse_encryption(&HOST, managed_write(AMZ_ENCRYPTION_KMS, Some("finance-key")))
                .await
                .expect_err("SSE-KMS must be refused when no KMS is configured");

            assert_eq!(error.code, S3ErrorCode::InvalidRequest);
            assert!(error.message.contains("SSE-KMS requires"), "message was {}", error.message);
            assert!(
                !error.message.contains("RUSTFS_SSE_S3_MASTER_KEY"),
                "an SSE-KMS refusal must not name the SSE-S3 master key: {}",
                error.message
            );
        },
    )
    .await;

    reset_sse_dek_provider();
}

/// A bare `aws:kms` request (no key id, no bucket default) on a node with
/// no KMS has no key to resolve. It must get the same configuration
/// refusal as the keyed form, whether or not the SSE-S3 master key is
/// set, rather than an untyped internal error (backlog#2368 B4).
#[tokio::test]
async fn sse_kms_write_without_a_key_id_is_refused_like_the_keyed_form() {
    let _guard = lock_sse_test_state().await;

    for master_key in [None, Some(BASE64_STANDARD.encode_to_string([9u8; 32]))] {
        reset_sse_dek_provider();
        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
                ("RUSTFS_SSE_S3_MASTER_KEY", master_key.clone()),
            ],
            async {
                // Entered directly: `sse_encryption` consults the bucket
                // default first, which needs a bucket metadata store.
                let error = apply_managed_encryption_material(
                    &HOST,
                    ManagedEncryptionRequest {
                        bucket: "finance",
                        key: "ledger.csv",
                        server_side_encryption: String::from(AMZ_ENCRYPTION_KMS),
                        kms_key_id: None,
                        ssekms_context: None,
                        content_size: 128,
                        principal: None,
                    },
                )
                .await
                .expect_err("SSE-KMS without a key id must be refused when no KMS is running");

                assert_eq!(
                    error.code,
                    S3ErrorCode::InvalidRequest,
                    "master_key={master_key:?}: message was {}",
                    error.message
                );
                assert!(error.message.contains("SSE-KMS requires"), "message was {}", error.message);
                assert!(
                    !error.message.contains("RUSTFS_SSE_S3_MASTER_KEY"),
                    "an SSE-KMS refusal must not name the SSE-S3 master key: {}",
                    error.message
                );
            },
        )
        .await;
    }

    reset_sse_dek_provider();
}

/// The SSE-S3 local fallback itself is unchanged: refusing SSE-KMS must not
/// take the documented no-KMS deployment down with it.
#[tokio::test]
async fn sse_s3_write_still_uses_the_local_master_key_fallback() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(BASE64_STANDARD.encode_to_string([9u8; 32]))),
        ],
        async {
            let material = sse_encryption(&HOST, managed_write(AMZ_ENCRYPTION_AES, None))
                .await
                .expect("SSE-S3 keeps its local master key fallback")
                .expect("managed sse-s3 material");

            assert_eq!(material.sse_type, SSEType::SseS3);
            assert_eq!(material.algorithm, AMZ_ENCRYPTION_AES);

            // No object may claim aws:kms while its DEK is wrapped locally.
            let metadata = encryption_material_to_metadata(&material).expect("sse-s3 metadata should serialize");
            assert!(
                !metadata.iter().any(|(_, value)| value == AMZ_ENCRYPTION_KMS),
                "local-master-key material must never be stamped aws:kms: {metadata:?}"
            );
        },
    )
    .await;

    reset_sse_dek_provider();
}

#[test]
fn parse_simple_sse_cmk_accepts_valid_32_byte_key() {
    let mut key = [0u8; 32];
    key[0] = 7;
    let encoded = BASE64_STANDARD.encode_to_string(key);
    let got = crate::kms::local::parse_simple_sse_cmk(&encoded).expect("valid 32-byte key must parse");
    assert_eq!(got, key);
}
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use async_trait::async_trait;
use base64_simd::STANDARD as BASE64_STANDARD;
use http::{HeaderMap, HeaderValue};
use rustfs_kms::types::ObjectEncryptionContext;
use rustfs_s3_types::S3ErrorCode;
use rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT;
use rustfs_utils::http::headers::{AMZ_ENCRYPTION_AES, AMZ_ENCRYPTION_KMS};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use temp_env::async_with_vars;
use tokio::sync::Mutex;

static SSE_TEST_LOCK: OnceLock<Mutex<()>> = OnceLock::new();
static SSE_TEST_KMS_KEY_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();

pub(crate) async fn lock_sse_test_state() -> tokio::sync::MutexGuard<'static, ()> {
    SSE_TEST_LOCK.get_or_init(|| Mutex::new(())).lock().await
}

async fn configure_test_global_local_kms() -> Arc<rustfs_kms::KmsServiceManager> {
    let key_dir = SSE_TEST_KMS_KEY_DIR.get_or_init(|| tempfile::TempDir::new().expect("create KMS key directory"));
    let manager = rustfs_kms::init_global_kms_service_manager();
    manager
        .reconfigure(rustfs_kms::KmsConfig::local(key_dir.path().to_path_buf()).with_insecure_development_defaults())
        .await
        .expect("configure test KMS service");
    manager
}

#[tokio::test]
async fn resolve_stored_kms_key_id_prefers_the_stored_id() {
    assert_eq!(resolve_stored_kms_key_id(&HOST, Some("app-key".to_string())).await, "app-key");
}

#[test]
fn normalize_encryption_metadata_case_accepts_lowercase_minio_internal_keys() {
    let lowercase_key = MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_ascii_lowercase();
    let metadata = HashMap::from([(lowercase_key, "sealed-key".to_string())]);

    let normalized =
        crate::core::keying::metadata::normalize_encryption_metadata_case(&metadata).expect("metadata aliases should normalize");

    assert_eq!(
        normalized.get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER),
        Some(&"sealed-key".to_string())
    );
}

struct UnavailableSseDekProvider;

#[async_trait::async_trait]
impl SseDekProvider for UnavailableSseDekProvider {
    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        Err(SseError::internal(KmsUnavailableError))
    }

    async fn decrypt_sse_dek(
        &self,
        _encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        Err(SseError::internal(KmsUnavailableError))
    }
}

fn local_sse_master_key_b64() -> String {
    BASE64_STANDARD.encode_to_string([0x24u8; 32])
}

#[test]
fn test_extract_ssec_params_from_headers() {
    let mut headers = http::HeaderMap::new();
    let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
    assert!(algo.is_none());
    assert!(key.is_none());
    assert!(md5.is_none());

    headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
    let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
    assert_eq!(algo.as_deref(), Some("AES256"));
    assert!(key.is_none());
    assert!(md5.is_none());

    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_static("pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs="),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_static("DWygnHRtgiJ77HCm+1rvHw=="),
    );
    let (algo, key, md5) = extract_ssec_params_from_headers(&headers).unwrap();
    assert_eq!(algo.as_deref(), Some("AES256"));
    assert!(key.is_some());
    assert!(md5.is_some());
}

#[test]
fn test_extract_ssec_params_from_headers_rejects_invalid_utf8() {
    let mut headers = http::HeaderMap::new();
    // Header value with invalid UTF-8; to_str() will fail
    let invalid_utf8 = HeaderValue::from_bytes(b"invalid-\x80-utf8").unwrap();
    headers.insert("x-amz-server-side-encryption-customer-algorithm", invalid_utf8);
    let result = extract_ssec_params_from_headers(&headers);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_extract_server_side_encryption_from_headers_rejects_invalid_utf8() {
    let mut headers = http::HeaderMap::new();
    let invalid_utf8 = HeaderValue::from_bytes(b"aes:kms-\x80-invalid").unwrap();
    headers.insert("x-amz-server-side-encryption", invalid_utf8);
    let result = extract_server_side_encryption_from_headers(&headers);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_extract_ssekms_context_from_headers_decodes_base64_json() {
    let mut headers = http::HeaderMap::new();
    let encoded = BASE64_STANDARD
        .encode_to_string(serde_json::to_vec(&HashMap::from([("tenant".to_string(), "alpha".to_string())])).unwrap());
    headers.insert(AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT, HeaderValue::from_str(&encoded).unwrap());

    let context = extract_ssekms_context_from_headers(&headers)
        .expect("kms context header should parse")
        .expect("kms context should exist");
    assert_eq!(context.get("tenant").map(String::as_str), Some("alpha"));
}

#[test]
fn test_validate_sse_headers_for_write_rejects_algorithm_without_key() {
    let algorithm = "AES256".to_string();
    let result = validate_sse_headers_for_write(
        None,
        None,
        None,
        Some(&algorithm),
        None,
        None,
        true, // PutObject requires all three
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_sse_headers_for_write_rejects_algorithm_and_key_without_md5() {
    let algorithm = "AES256".to_string();
    let key = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=".to_string();
    let result = validate_sse_headers_for_write(
        None,
        None,
        None,
        Some(&algorithm),
        Some(&key),
        None,
        true, // PutObject requires all three
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_sse_headers_for_write_rejects_invalid_sse_algorithm() {
    let bad_sse = String::from("aes:kms");
    let result = validate_sse_headers_for_write(Some(&bad_sse), None, None, None, None, None, true);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_validate_sse_headers_for_write_rejects_ssec_with_managed_sse() {
    let algorithm = "AES256".to_string();
    let key = "pO3upElrwuEXSoFwCfnZPdSsmt/xWeFa0N9KgDijwVs=".to_string();
    let key_md5 = "DWygnHRtgiJ77HCm+1rvHw==".to_string();
    let server_side_encryption = String::from(AMZ_ENCRYPTION_AES);
    let result = validate_sse_headers_for_write(
        Some(&server_side_encryption),
        None,
        None,
        Some(&algorithm),
        Some(&key),
        Some(&key_md5),
        true,
    );
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_is_managed_sse() {
    assert!(is_managed_sse(&String::from("AES256")));
    assert!(is_managed_sse(&String::from("aws:kms")));
}

#[test]
fn test_generate_ssec_nonce() {
    let nonce1 = generate_ssec_nonce("bucket1", "key1");
    let nonce2 = generate_ssec_nonce("bucket1", "key1");
    let nonce3 = generate_ssec_nonce("bucket1", "key2");

    // Same inputs should produce same nonce
    assert_eq!(nonce1, nonce2);

    // Different inputs should produce different nonce
    assert_ne!(nonce1, nonce3);

    // Nonce should be exactly 12 bytes
    assert_eq!(nonce1.len(), 12);
}

#[test]
fn test_validate_ssec_params_success() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let key_md5 = md5_base64([42u8; 32]);

    let params = SsecParams {
        algorithm: "AES256".to_string(),
        key,
        key_md5,
    };

    let result = validate_ssec_params(params);
    assert!(result.is_ok());
    let validated = result.unwrap();
    assert_eq!(validated.key_bytes, [42u8; 32]);
}

#[test]
fn test_validate_ssec_params_wrong_algorithm() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let key_md5 = md5_base64([42u8; 32]);

    let params = SsecParams {
        algorithm: "AES128".to_string(), // Wrong algorithm
        key,
        key_md5,
    };

    let result = validate_ssec_params(params);
    assert!(result.is_err());
}

#[test]
fn test_validate_ssec_params_wrong_key_length() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 16]); // Only 16 bytes
    let key_md5 = md5_base64([42u8; 16]);

    let params = SsecParams {
        algorithm: "AES256".to_string(),
        key,
        key_md5,
    };

    let result = validate_ssec_params(params);
    assert!(result.is_err());
}

#[test]
fn test_validate_ssec_params_wrong_md5() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let key_md5 = BASE64_STANDARD.encode_to_string([99u8; 16]); // Wrong MD5

    let params = SsecParams {
        algorithm: "AES256".to_string(),
        key,
        key_md5,
    };

    let result = validate_ssec_params(params);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_sse_encryption_rejects_partial_ssec_headers() {
    let bucket = "test-bucket";
    let key = "test-key";
    let sse_key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let sse_key_md5 = md5_base64([42u8; 32]);
    let content_size = 1024;

    let request_missing_md5 = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(sse_key.clone()),
        sse_customer_key_md5: None,
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request_missing_md5).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);

    let request_missing_key = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: None,
        sse_customer_key_md5: Some(sse_key_md5.clone()),
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request_missing_key).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);

    let request_missing_algorithm = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: Some(sse_key),
        sse_customer_key_md5: Some(sse_key_md5),
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request_missing_algorithm).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn test_sse_prepare_encryption_rejects_partial_ssec_headers() {
    let bucket = "test-bucket";
    let key = "test-key";
    let sse_key_md5 = md5_base64([42u8; 32]);

    let request_missing_algorithm = PrepareEncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: Some(sse_key_md5),
        principal: None,
    };

    let err = sse_prepare_encryption(&HOST, request_missing_algorithm).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_sse_headers_for_write_allows_aws_kms_without_key_id() {
    let server_side_encryption: String = "aws:kms".to_string();

    let result = validate_sse_headers_for_write(Some(&server_side_encryption), None, None, None, None, None, true);

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_sse_prepare_encryption_rejects_ssec_headers_without_customer_key() {
    let bucket = "test-bucket";
    let key = "test-key";
    let sse_key_md5 = md5_base64([42u8; 32]);

    let request = PrepareEncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: None,
        sse_customer_key_md5: Some(sse_key_md5),
        principal: None,
    };

    let error = sse_prepare_encryption(&HOST, request)
        .await
        .expect_err("multipart preparation must require possession of the customer key");
    assert_eq!(error.code, S3ErrorCode::InvalidRequest);
}

// ------------------------------------------------------------------------
// SSE-C Direct (default, non-`rio-v2` build) random-nonce persistence
// ------------------------------------------------------------------------

#[cfg(not(feature = "rio-v2"))]
async fn ssec_direct_put_metadata(
    bucket: &str,
    key: &str,
    customer_key: &str,
    customer_key_md5: &str,
) -> HashMap<String, String> {
    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.to_string()),
            sse_customer_key_md5: Some(customer_key_md5.to_string()),
            content_size: 128,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");
    assert_eq!(material.key_kind, EncryptionKeyKind::Direct);
    encryption_material_to_metadata(&material).expect("sse-c metadata should serialize")
}

// (a) Overwriting the same bucket/key under the same SSE-C key must persist a DIFFERENT
// random IV each time, and each stored IV must be read back on decrypt. This is the core
// of the fix: no (key, nonce) reuse across overwrites.
#[cfg(not(feature = "rio-v2"))]
#[tokio::test]
async fn test_ssec_direct_random_nonce_is_unique_per_put_and_read_back() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let metadata_one = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;
    let metadata_two = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

    let iv_one = metadata_one
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("first put persists a random IV");
    let iv_two = metadata_two
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("second put persists a random IV");

    assert_ne!(iv_one, iv_two, "overwriting the same object must not reuse the nonce");

    // Dual-key persistence for MinIO interop: both headers carry the same value.
    assert_eq!(metadata_one.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_one));
    assert_eq!(metadata_two.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER), Some(iv_two));

    for (metadata, iv) in [(&metadata_one, iv_one), (&metadata_two, iv_two)] {
        let decrypted =
            crate::core::read::apply_ssec_decryption_material(bucket, key, metadata, &customer_key, &customer_key_md5)
                .await
                .expect("sse-c decryption material");
        assert_eq!(
            BASE64_STANDARD.encode_to_string(decrypted.base_nonce),
            *iv,
            "decrypt must read the persisted random nonce back"
        );
        assert_eq!(decrypted.key_bytes, customer_key_bytes);
    }
}

// (b) Legacy compatibility: objects encrypted before this change carry no IV header. Decrypt
// must fall back to the deterministic `generate_ssec_nonce(bucket, key)` they were encrypted
// with, so previously stored objects keep decrypting.
#[cfg(not(feature = "rio-v2"))]
#[tokio::test]
async fn test_ssec_direct_legacy_object_without_iv_falls_back_to_deterministic_nonce() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(!metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));

    let decrypted = crate::core::read::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
        .await
        .expect("sse-c decryption material");

    assert_eq!(
        decrypted.base_nonce,
        generate_ssec_nonce(bucket, key),
        "legacy objects must decrypt via the deterministic nonce fallback"
    );
}

// (c) Byte-level round trip: data encrypted with the material's key + persisted nonce must
// decrypt with the key + nonce resolved from the stored metadata.
#[cfg(not(feature = "rio-v2"))]
#[tokio::test]
async fn test_ssec_direct_persisted_nonce_round_trips_plaintext() {
    use aes_gcm::{
        Aes256Gcm, Nonce,
        aead::{Aead, KeyInit},
    };

    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x51u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);
    let plaintext = b"attack at dawn - sse-c round trip".to_vec();

    let metadata = ssec_direct_put_metadata(bucket, key, &customer_key, &customer_key_md5).await;

    // Encrypt with the key + nonce that were persisted at PUT time.
    let enc_iv = BASE64_STANDARD
        .decode_to_vec(metadata.get(INTERNAL_ENCRYPTION_IV_HEADER).expect("persisted IV"))
        .expect("valid base64 IV");
    let cipher = Aes256Gcm::new_from_slice(&customer_key_bytes).expect("cipher");
    let ciphertext = cipher
        .encrypt(&Nonce::try_from(enc_iv.as_slice()).expect("nonce"), plaintext.as_ref())
        .expect("encrypt");

    // Resolve key + nonce purely from stored metadata (as decrypt does in production).
    let decrypted_material =
        crate::core::read::apply_ssec_decryption_material(bucket, key, &metadata, &customer_key, &customer_key_md5)
            .await
            .expect("sse-c decryption material");
    let dec_cipher = Aes256Gcm::new_from_slice(&decrypted_material.key_bytes).expect("cipher");
    let recovered = dec_cipher
        .decrypt(
            &Nonce::try_from(decrypted_material.base_nonce.as_slice()).expect("nonce"),
            ciphertext.as_ref(),
        )
        .expect("decrypt with resolved key + nonce");

    assert_eq!(recovered, plaintext);
}

// (d) Multipart: the random nonce generated at CreateMultipartUpload is persisted in the
// session metadata, and every part resolves the SAME nonce via `sse_decryption` on that
// session metadata. Parts must never diverge, and the value must not be deterministic.
#[cfg(not(feature = "rio-v2"))]
#[tokio::test]
async fn test_ssec_direct_multipart_all_parts_share_one_persisted_nonce() {
    let bucket = "bucket";
    let key = "object";
    let customer_key_bytes = [0x33u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_prepare_encryption(
        &HOST,
        PrepareEncryptionRequest {
            bucket,
            key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            principal: None,
        },
    )
    .await
    .expect("prepare ssec")
    .expect("prepare material");
    assert_eq!(material.key_kind, EncryptionKeyKind::Direct);

    let session_metadata = encryption_material_to_metadata(&material).expect("session metadata should serialize");
    let session_iv = session_metadata
        .get(INTERNAL_ENCRYPTION_IV_HEADER)
        .expect("multipart session persists a random IV")
        .clone();
    // Random, not the deterministic bucket/key derivation.
    assert_ne!(
        BASE64_STANDARD.decode_to_vec(&session_iv).expect("valid IV")[..],
        generate_ssec_nonce(bucket, key)[..]
    );

    let resolve_part_nonce = |part_number: usize| {
        let session_metadata = session_metadata.clone();
        let customer_key = customer_key.clone();
        let customer_key_md5 = customer_key_md5.clone();
        async move {
            let _ = part_number;
            sse_decryption(
                &HOST,
                DecryptionRequest {
                    bucket,
                    key,
                    metadata: &session_metadata,
                    sse_customer_key: Some(&customer_key),
                    sse_customer_key_md5: Some(&customer_key_md5),
                    principal: None,
                },
            )
            .await
            .expect("part decryption")
            .expect("part material")
            .base_nonce
        }
    };

    let part_one_nonce = resolve_part_nonce(1).await;
    let part_two_nonce = resolve_part_nonce(2).await;

    assert_eq!(part_one_nonce, part_two_nonce, "all parts of one upload must share the persisted nonce");
    assert_eq!(BASE64_STANDARD.encode_to_string(part_one_nonce), session_iv);
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_sse_prepare_encryption_ssec_with_customer_key_stores_sealed_key_metadata() {
    let bucket = "test-bucket";
    let key = "test-key";
    let customer_key_bytes = [0x24u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let sse_key_md5 = md5_base64(customer_key_bytes);

    let request = PrepareEncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(customer_key),
        sse_customer_key_md5: Some(sse_key_md5),
        principal: None,
    };

    let material = sse_prepare_encryption(&HOST, request)
        .await
        .expect("prepare should accept full ssec headers")
        .expect("ssec metadata should be generated");
    assert_eq!(material.key_kind, EncryptionKeyKind::Object);

    let metadata = encryption_material_to_metadata(&material).expect("ssec metadata should serialize");
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));
}

#[test]
fn test_encryption_material_to_metadata_persists_ssec_original_size() {
    let metadata = encryption_material_to_metadata(&EncryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
        kms_key_id: None,
        algorithm: "AES256".to_string(),
        key_bytes: [0u8; 32],
        base_nonce: [0u8; 12],
        encrypted_data_key: None,
        customer_key_md5: Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
        original_size: Some(1024),
        key_kind: EncryptionKeyKind::Direct,
        managed_kms_context: None,
        managed_sealed_key: None,
    })
    .expect("ssec original-size metadata should serialize");

    assert_eq!(metadata.get(SSEC_ORIGINAL_SIZE_HEADER).map(String::as_str), Some("1024"));
}

fn material_variant(sse_type: SSEType, key_kind: EncryptionKeyKind) -> EncryptionMaterial {
    let (server_side_encryption, kms_key_id, encrypted_data_key, customer_key_md5, managed_kms_context) = match sse_type {
        SSEType::SseC => (
            String::from(AMZ_ENCRYPTION_AES),
            None,
            None,
            Some("d41d8cd98f00b204e9800998ecf8427e".to_string()),
            None,
        ),
        SSEType::SseS3 => (String::from(AMZ_ENCRYPTION_AES), None, Some(vec![0u8; 32]), None, None),
        SSEType::SseKms => (
            String::from(AMZ_ENCRYPTION_KMS),
            Some("kms-key-1".to_string()),
            Some(vec![0u8; 32]),
            None,
            Some(HashMap::from([("app".to_string(), "test".to_string())])),
        ),
    };
    EncryptionMaterial {
        sse_type,
        server_side_encryption,
        kms_key_id,
        algorithm: "AES256".to_string(),
        key_bytes: [0u8; 32],
        base_nonce: [0u8; 12],
        encrypted_data_key,
        customer_key_md5,
        original_size: Some(1024),
        key_kind,
        managed_kms_context,
        managed_sealed_key: None,
    }
}

#[test]
fn native_request_debug_withholds_encryption_customer_key() {
    let mut request = managed_write(AMZ_ENCRYPTION_AES, None);
    request.sse_customer_key = Some("fixture-key-must-stay-private".to_owned());
    let diagnostic = format!("{request:?}");
    assert!(!diagnostic.contains("sse_customer_key"), "customer keys must not be debug fields");
}

#[test]
fn native_request_debug_withholds_decryption_customer_key() {
    let metadata = HashMap::new();
    let key = "fixture-key-must-stay-private".to_owned();
    let request = DecryptionRequest {
        bucket: "bucket",
        key: "object",
        metadata: &metadata,
        sse_customer_key: Some(&key),
        sse_customer_key_md5: None,
        principal: None,
    };
    let diagnostic = format!("{request:?}");
    assert!(!diagnostic.contains("sse_customer_key"), "customer keys must not be debug fields");
}

#[test]
fn native_material_debug_withholds_encryption_keys() {
    let material = material_variant(SSEType::SseC, EncryptionKeyKind::Direct);
    let diagnostic = format!("{material:?}");
    assert!(
        !diagnostic.contains("key_bytes")
            && !diagnostic.contains("encrypted_data_key")
            && !diagnostic.contains("managed_sealed_key"),
        "encryption keys must not be debug fields"
    );
}

#[test]
fn native_material_debug_withholds_decryption_keys() {
    let material = DecryptionMaterial {
        sse_type: SSEType::SseC,
        server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
        kms_key_id: None,
        algorithm: "AES256".to_owned(),
        customer_key_md5: None,
        key_bytes: [0; 32],
        base_nonce: [0; 12],
        key_kind: EncryptionKeyKind::Direct,
    };
    let diagnostic = format!("{material:?}");
    assert!(!diagnostic.contains("key_bytes"), "decryption keys must not be debug fields");
}

#[test]
fn native_material_debug_withholds_customer_key() {
    let material = crate::core::s3::scheme::SseTypeV2::SseC(
        "AES256".to_owned(),
        "fixture-key-must-stay-private".to_owned(),
        "fixture-md5".to_owned(),
    );
    let diagnostic = format!("{material:?}");
    assert!(
        !diagnostic.contains("fixture-key-must-stay-private"),
        "customer keys must not appear in diagnostics"
    );
}

/// Reconciliation contract with the replication boundary (backlog#1783):
/// every metadata key this module persists must either be remapped by the
/// SSE-C transport table or be caught by the replication strip predicate.
/// A new stored key that is in neither turns this test red before it can
/// silently leak through outbound replication metadata.
#[test]
fn test_encryption_metadata_keys_reconcile_with_replication_transport_and_strip() {
    use rustfs_utils::http::object_encryption_keys::{is_replication_stripped_encryption_key, ssec_replication_transport_header};

    let variants = [
        (SSEType::SseC, EncryptionKeyKind::Direct),
        (SSEType::SseS3, EncryptionKeyKind::Direct),
        (SSEType::SseKms, EncryptionKeyKind::Direct),
    ];

    for (sse_type, key_kind) in variants {
        let metadata =
            encryption_material_to_metadata(&material_variant(sse_type, key_kind)).expect("encryption material should serialize");
        assert!(!metadata.is_empty());

        for key in metadata.keys() {
            assert!(
                ssec_replication_transport_header(key).is_some() || is_replication_stripped_encryption_key(key),
                "stored key {key} ({sse_type:?}/{key_kind:?}) is neither transport-mapped nor stripped for replication"
            );
            if !matches!(sse_type, SSEType::SseC) {
                // Managed SSE never takes the transport mapping; every key
                // must be structurally stripped so envelopes cannot leave
                // the source site.
                assert!(
                    is_replication_stripped_encryption_key(key),
                    "managed-SSE stored key {key} ({sse_type:?}) escapes the replication strip predicate"
                );
            }
        }
    }
}

#[tokio::test]
async fn test_sse_encryption_rejects_kms_key_with_invalid_algorithm() {
    let bucket = "test-bucket";
    let key = "test-key";
    let content_size = 1024;

    let request = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: Some("AES256".to_string()),
        ssekms_key_id: Some("test-key".to_string()),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn test_sse_encryption_rejects_kms_key_without_algorithm() {
    let bucket = "test-bucket";
    let key = "test-key";
    let content_size = 1024;

    let request = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: Some("test-key".to_string()),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[tokio::test]
async fn test_sse_encryption_rejects_conflict_between_kms_and_ssec() {
    let bucket = "test-bucket";
    let key = "test-key";
    let content_size = 1024;
    let sse_key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let sse_key_md5 = md5_base64([42u8; 32]);

    let request = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: Some("aws:kms".to_string()),
        ssekms_key_id: Some("test-key".to_string()),
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(sse_key),
        sse_customer_key_md5: Some(sse_key_md5),
        content_size,
        principal: None,
    };

    let err = sse_encryption(&HOST, request).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_resolve_effective_kms_key_id_ignores_bucket_default_for_explicit_sse_s3() {
    let effective_sse = String::from(AMZ_ENCRYPTION_AES);

    let kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), None, || Some("bucket-default".to_string()));

    assert_eq!(kms_key_id, None);
}

#[test]
fn test_resolve_effective_kms_key_id_uses_bucket_default_for_sse_kms() {
    let effective_sse = String::from(AMZ_ENCRYPTION_KMS);

    let kms_key_id = resolve_effective_kms_key_id(Some(&effective_sse), None, || Some("bucket-default".to_string()));

    assert_eq!(kms_key_id.as_deref(), Some("bucket-default"));
}

/// One recorded rewrap call: (envelope bytes, bucket, object key, context).
type RecordedRewrapCall = (Vec<u8>, String, String, HashMap<String, String>);

/// Test double for the rewrap seam: records what it was asked to rewrap
/// and answers with a canned response.
struct RewrapProbeProvider {
    rewrapped: bool,
    new_ciphertext: Vec<u8>,
    calls: std::sync::Mutex<Vec<RecordedRewrapCall>>,
}

#[async_trait]
impl SseDekProvider for RewrapProbeProvider {
    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        unreachable!("rewrap tests never generate keys")
    }

    async fn decrypt_sse_dek(
        &self,
        _encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        unreachable!("rewrap tests never decrypt keys")
    }

    async fn rewrap_sse_dek(
        &self,
        encrypted_dek: &[u8],
        context: &ObjectEncryptionContext,
    ) -> Result<rustfs_kms::types::RewrapDataKeyResponse, SseError> {
        self.calls.lock().expect("probe lock").push((
            encrypted_dek.to_vec(),
            context.bucket.clone(),
            context.object_key.clone(),
            context.encryption_context.clone(),
        ));
        Ok(rustfs_kms::types::RewrapDataKeyResponse {
            ciphertext: if self.rewrapped {
                self.new_ciphertext.clone()
            } else {
                encrypted_dek.to_vec()
            },
            key_id: "probe-key".to_string(),
            source_key_version: Some(1),
            destination_key_version: Some(2),
            rewrapped: self.rewrapped,
        })
    }
}

/// A minimal but well-formed data-key envelope, the shape
/// `is_data_key_envelope` recognizes.
fn probe_envelope_json() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "key_id": "dek-id",
        "master_key_id": "master-key",
        "key_spec": "AES_256",
        "encrypted_key": [1, 2, 3, 4],
        "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        "encryption_context": {"bucket": "bucket/dir/object"},
        "created_at": "2024-01-01T00:00:00+00:00"
    }))
    .expect("serialize probe envelope")
}

/// The adapter must replace every stored copy of the old envelope — under
/// whatever key casing the storage layer preserved — and leave slots
/// holding different bytes (a sealed object key) untouched. Its context
/// must be rebuilt exactly as the managed decrypt path rebuilds it.
#[tokio::test]
async fn rewrap_object_metadata_replaces_every_stored_envelope_copy() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    let envelope = probe_envelope_json();
    let envelope_b64 = BASE64_STANDARD.encode_to_string(&envelope);
    let client_context = HashMap::from([("tenant".to_string(), "alpha".to_string())]);
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
        // Mixed casing on the internal header, exactly as the storage layer
        // can hand it back.
        ("X-Rustfs-Encryption-Key".to_string(), envelope_b64.clone()),
        (MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), envelope_b64.clone()),
        // A sealed object key: different bytes, must not be rewritten.
        (
            MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(b"sealed-object-key-not-the-envelope"),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
            encode_minio_kms_context(&client_context).expect("encode context"),
        ),
    ]);

    let new_ciphertext = b"rewrapped-envelope-bytes".to_vec();
    let provider = Arc::new(RewrapProbeProvider {
        rewrapped: true,
        new_ciphertext: new_ciphertext.clone(),
        calls: std::sync::Mutex::new(Vec::new()),
    });
    crate::kms::provider::set_sse_dek_provider_for_test(provider.clone());

    let outcome = rewrap_object_encryption_metadata(&HOST, "bucket", "dir/object", &metadata)
        .await
        .expect("rewrap must succeed");
    let ObjectDekRewrapOutcome::Rewrapped { metadata: overrides } = outcome else {
        panic!("expected a rewrapped outcome, got {outcome:?}");
    };

    let new_b64 = BASE64_STANDARD.encode_to_string(&new_ciphertext);
    assert_eq!(
        overrides,
        HashMap::from([
            ("X-Rustfs-Encryption-Key".to_string(), new_b64.clone()),
            (MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(), new_b64),
        ]),
        "every envelope copy must be replaced under its stored name and nothing else touched"
    );

    let calls = provider.calls.lock().expect("probe lock");
    let (sent_envelope, bucket, object_key, sent_context) = calls.first().expect("the provider must be called");
    assert_eq!(*sent_envelope, envelope, "the decoded stored envelope must reach the provider");
    assert_eq!(bucket, "bucket");
    assert_eq!(object_key, "dir/object");
    assert_eq!(
        *sent_context,
        build_kms_request_context("bucket", "dir/object", Some(&client_context)),
        "the context must be rebuilt exactly as the managed decrypt path rebuilds it"
    );

    reset_sse_dek_provider();
}

/// `rewrapped: false` from the backend means nothing to persist; the
/// adapter must answer AlreadyCurrent so a sweep re-run converges.
#[tokio::test]
async fn rewrap_object_metadata_converges_when_already_current() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    let envelope_b64 = BASE64_STANDARD.encode_to_string(probe_envelope_json());
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), envelope_b64),
    ]);
    let provider = Arc::new(RewrapProbeProvider {
        rewrapped: false,
        new_ciphertext: Vec::new(),
        calls: std::sync::Mutex::new(Vec::new()),
    });
    crate::kms::provider::set_sse_dek_provider_for_test(provider.clone());

    let outcome = rewrap_object_encryption_metadata(&HOST, "bucket", "object", &metadata)
        .await
        .expect("rewrap must succeed");
    assert!(matches!(outcome, ObjectDekRewrapOutcome::AlreadyCurrent), "got {outcome:?}");
    assert_eq!(provider.calls.lock().expect("probe lock").len(), 1);

    reset_sse_dek_provider();
}

/// The KMS-backed provider's rewrap threads through the encryption
/// service to the backend. The Local test backend has no rewrap support,
/// so the capability refusal coming back proves the whole chain is wired —
/// a stub that silently succeeded would return Ok here.
#[tokio::test]
async fn kms_provider_rewrap_reaches_the_backend_through_the_service() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    let manager = configure_test_global_local_kms().await;
    let provider = KmsSseDekProvider::new_with_service_manager(manager)
        .await
        .expect("kms provider should initialize from the configured test manager");

    let context = crate::core::keying::context::build_object_encryption_context("bucket", "object", None);
    let error = provider
        .rewrap_sse_dek(b"{}", &context)
        .await
        .expect_err("the Local backend must refuse rewrap through the full chain");
    assert!(
        error.to_string().contains("rewrap") || format!("{:?}", error.source).contains("rewrap_data_key"),
        "the refusal must come from the backend capability gate: {error:?}"
    );

    reset_sse_dek_provider();
}

/// A write whose resolved key — from the request header or a bucket
/// default rule — is unknown to the KMS must come back as the client
/// error S3 uses for it, all the way from the backend lookup. Answering
/// 500 here made a bucket default pointing at a deleted or mistyped key
/// look like a server outage (rustfs/backlog#2330, KMS-312).
#[tokio::test]
async fn kms_provider_reports_an_unknown_key_as_kms_not_found() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    let manager = configure_test_global_local_kms().await;
    let provider = KmsSseDekProvider::new_with_service_manager(manager)
        .await
        .expect("kms provider should initialize from the configured test manager");

    let context = crate::core::keying::context::build_object_encryption_context("bucket", "object", None);
    let error = provider
        .generate_sse_dek(&context, "no-such-key")
        .await
        .expect_err("the Local backend must refuse a key it does not hold");
    assert_eq!(error.code, S3ErrorCode::Custom(KMS_KEY_NOT_FOUND_ERROR_CODE.into()), "got {error:?}");
    assert!(error.message.contains("no-such-key"), "the missing key must be named: {error:?}");
    assert_eq!(crate::core::audit::kms_data_plane_error_class(&error), "key_not_found");

    reset_sse_dek_provider();
}

/// Objects without a rewrappable envelope — plaintext, SSE-C, or a
/// MinIO-sealed opaque data key — are reported NotApplicable without any
/// provider call.
#[tokio::test]
async fn rewrap_object_metadata_skips_objects_without_a_rustfs_envelope() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    let provider = Arc::new(RewrapProbeProvider {
        rewrapped: true,
        new_ciphertext: b"never-used".to_vec(),
        calls: std::sync::Mutex::new(Vec::new()),
    });
    crate::kms::provider::set_sse_dek_provider_for_test(provider.clone());

    // Plaintext object.
    let outcome = rewrap_object_encryption_metadata(&HOST, "bucket", "object", &HashMap::new())
        .await
        .expect("plaintext objects must not error");
    assert!(matches!(outcome, ObjectDekRewrapOutcome::NotApplicable), "got {outcome:?}");

    // SSE-C object: customer-key encryption never reaches KMS.
    let ssec = HashMap::from([
        ("X-Amz-Server-Side-Encryption-Customer-Algorithm".to_string(), "AES256".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([1u8; 12])),
    ]);
    let outcome = rewrap_object_encryption_metadata(&HOST, "bucket", "object", &ssec)
        .await
        .expect("SSE-C objects must not error");
    assert!(matches!(outcome, ObjectDekRewrapOutcome::NotApplicable), "got {outcome:?}");

    // MinIO builtin-KMS ciphertext: opaque bytes, not a RustFS envelope.
    let minio = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
        (
            MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(b"opaque-minio-sealed-bytes"),
        ),
    ]);
    let outcome = rewrap_object_encryption_metadata(&HOST, "bucket", "object", &minio)
        .await
        .expect("MinIO-sealed objects must not error");
    assert!(matches!(outcome, ObjectDekRewrapOutcome::NotApplicable), "got {outcome:?}");

    assert!(
        provider.calls.lock().expect("probe lock").is_empty(),
        "no provider call may happen for non-rewrappable objects"
    );

    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_sse_encryption_persists_aws_kms_header_for_kms_objects() {
    let material = EncryptionMaterial {
        sse_type: SSEType::SseKms,
        server_side_encryption: String::from(AMZ_ENCRYPTION_KMS),
        kms_key_id: Some("test-key".to_string()),
        algorithm: AMZ_ENCRYPTION_KMS.to_string(),
        key_bytes: [7u8; 32],
        base_nonce: [9u8; 12],
        encrypted_data_key: Some(vec![1, 2, 3, 4]),
        customer_key_md5: None,
        original_size: Some(1024),
        key_kind: EncryptionKeyKind::Direct,
        managed_kms_context: None,
        managed_sealed_key: None,
    };
    // Only an aws:kms object names its key in write responses.
    assert_eq!(material.response_kms_key_id().as_deref(), Some("test-key"));
    let metadata = encryption_material_to_metadata(&material).expect("managed SSE metadata should serialize");

    assert_eq!(metadata.get("x-amz-server-side-encryption").map(String::as_str), Some("aws:kms"));
    assert_eq!(
        metadata
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .map(String::as_str),
        Some("test-key")
    );
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_sse_kms_roundtrip_persists_and_uses_minio_context() {
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
    let _guard = lock_sse_test_state().await;

    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;
    manager
        .get_encryption_service()
        .await
        .expect("encryption service should exist")
        .create_key(CreateKeyRequest {
            key_name: Some("kms-test".to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        })
        .await
        .expect("kms test key should be created");

    let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
        .await
        .expect("kms provider should initialize from the configured test manager");
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));

    let client_context = HashMap::from([("tenant".to_string(), "alpha".to_string())]);
    let request = EncryptionRequest {
        bucket: "bucket",
        key: "dir/object",
        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
        ssekms_key_id: Some("kms-test".to_string()),
        ssekms_context: Some(client_context.clone()),
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 4096,
        principal: None,
    };

    let material = sse_encryption(&HOST, request)
        .await
        .expect("sse-kms encryption")
        .expect("managed sse-kms material");
    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
    let encoded_context = metadata
        .get(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
        .expect("minio kms context header should exist");
    let decoded_context: HashMap<String, String> =
        serde_json::from_slice(&BASE64_STANDARD.decode_to_vec(encoded_context).expect("decode base64 context"))
            .expect("decode json context");
    assert_eq!(decoded_context, client_context);

    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("sse-kms decryption should succeed")
    .expect("sse-kms material should exist");
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    let mut wrong_metadata = metadata.clone();
    wrong_metadata.insert(
        MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(),
        BASE64_STANDARD
            .encode_to_string(serde_json::to_vec(&HashMap::from([("tenant".to_string(), "beta".to_string())])).unwrap()),
    );
    let err = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata: &wrong_metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect_err("mismatched kms context should fail");
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
    assert_eq!(
        err.message,
        "Encryption context mismatch: Context mismatch for key 'tenant': expected 'alpha', got 'beta'"
    );
    assert_eq!(crate::core::audit::kms_data_plane_error_class(&err), "context_mismatch");

    manager.stop().await.expect("kms service should stop cleanly");
    reset_sse_dek_provider();
}

#[cfg(feature = "rio-v2")]
#[test]
fn test_encryption_material_to_metadata_persists_minio_managed_headers() {
    let encoded_nonce = BASE64_STANDARD.encode_to_string([9u8; 12]);
    let encoded_key = BASE64_STANDARD.encode_to_string([1u8, 2, 3, 4]);
    let metadata = encryption_material_to_metadata(&EncryptionMaterial {
        sse_type: SSEType::SseKms,
        server_side_encryption: String::from(AMZ_ENCRYPTION_KMS),
        kms_key_id: Some("test-key".to_string()),
        algorithm: AMZ_ENCRYPTION_KMS.to_string(),
        key_bytes: [7u8; 32],
        base_nonce: [9u8; 12],
        encrypted_data_key: Some(vec![1, 2, 3, 4]),
        customer_key_md5: None,
        original_size: Some(1024),
        key_kind: EncryptionKeyKind::Direct,
        managed_kms_context: None,
        managed_sealed_key: None,
    })
    .expect("managed SSE metadata should serialize");

    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER).map(String::as_str),
        Some(encoded_nonce.as_str())
    );
    assert_eq!(
        metadata
            .get(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER)
            .map(String::as_str),
        Some(encoded_key.as_str())
    );
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER).map(String::as_str),
        Some("test-key")
    );
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
        Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
    );
}

#[tokio::test]
async fn test_sse_encryption_omits_kms_header_for_sse_s3_objects() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    let local_sse_master_key = local_sse_master_key_b64();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
        ],
        async {
            let request = EncryptionRequest {
                bucket: "test-bucket",
                key: "test-key",
                server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                content_size: 1024,
                principal: None,
            };

            let material = sse_encryption(&HOST, request)
                .await
                .expect("sse-s3 encryption should succeed");
            let material = material.expect("managed sse-s3 encryption should return material");
            let metadata = encryption_material_to_metadata(&material).expect("managed SSE-S3 metadata should serialize");

            assert_eq!(material.kms_key_id.as_deref(), Some("default"));
            // The wrapping key stays internal: no write response may
            // advertise it for an AES256 object.
            assert_eq!(material.response_kms_key_id(), None);
            assert_eq!(metadata.get("x-amz-server-side-encryption").map(String::as_str), Some("AES256"));
            assert!(!metadata.contains_key("x-amz-server-side-encryption-aws-kms-key-id"));
            assert_eq!(metadata.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER).map(String::as_str), Some("default"));
        },
    )
    .await;

    reset_sse_dek_provider();
}

#[test]
fn test_strip_managed_encryption_metadata() {
    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption".to_string(), "aws:kms".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), "source-key-md5".to_string());
    metadata.insert(SSEC_ORIGINAL_SIZE_HEADER.to_string(), "123".to_string());
    metadata.insert("x-rustfs-encryption-key".to_string(), "encrypted_key".to_string());
    metadata.insert(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), "sealed".to_string());
    metadata.insert("content-type".to_string(), "text/plain".to_string());

    strip_managed_encryption_metadata(&mut metadata);

    assert!(!metadata.contains_key("x-amz-server-side-encryption"));
    assert!(!metadata.contains_key("x-amz-server-side-encryption-customer-algorithm"));
    assert!(!metadata.contains_key("x-amz-server-side-encryption-customer-key-md5"));
    assert!(!metadata.contains_key(SSEC_ORIGINAL_SIZE_HEADER));
    assert!(!metadata.contains_key("x-rustfs-encryption-key"));
    assert!(!metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER));
    assert!(metadata.contains_key("content-type"));
}

/// A copy destination must not inherit any key that `is_object_encryption_marker`
/// accepts as proof the payload is encrypted.
///
/// Guards this chain: `ObjectInfo::is_encrypted` is keyed on that predicate, so a
/// single leftover marker makes a plaintext destination report itself encrypted.
/// `object_api::readers` then takes its encrypted branch and demands read material,
/// but the material itself was stripped, so `sse_decryption` reports no encryption
/// and the read fails closed on unresolvable encryption metadata — a destination
/// that CopyObject wrote successfully becomes permanently unreadable.
#[test]
fn test_strip_managed_encryption_metadata_clears_encryption_markers() {
    let mut metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
        ("x-amz-server-side-encryption-aws-kms-key-id".to_string(), "source-key".to_string()),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "source-key".to_string()),
        (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "wrapped-dek".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "base-nonce".to_string()),
        ("content-type".to_string(), "text/plain".to_string()),
    ]);

    strip_managed_encryption_metadata(&mut metadata);

    let inherited: Vec<&str> = metadata
        .keys()
        .filter(|key| rustfs_utils::http::is_object_encryption_marker(key))
        .map(String::as_str)
        .collect();
    assert!(inherited.is_empty(), "copy destination inherited encryption markers: {inherited:?}");
    assert!(metadata.contains_key("content-type"));
}

/// A same-name CopyObject is normally serviced as a metadata-only update that reuses the
/// stored ciphertext, while the handler rebuilds the destination metadata around a *fresh*
/// DEK. `ObjectInfo::is_encrypted` — i.e. `is_object_encryption_marker` — is the guard that
/// keeps that shortcut away from encrypted objects (see the `metadata_only` decision in
/// `rustfs/src/app/object_usecase.rs`). Any encryption flavour whose headers
/// `strip_managed_encryption_metadata` drops must therefore also be *detected* there — a
/// flavour that is stripped but not detected would resurrect "new DEK + old ciphertext",
/// which leaves the object permanently undecryptable.
#[test]
fn every_strippable_encryption_shape_is_detected_as_encrypted() {
    let shapes: [(&str, Vec<(&str, &str)>); 4] = [
        (
            "sse-s3",
            vec![
                ("x-amz-server-side-encryption", "AES256"),
                (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER, "sealed"),
                (MINIO_INTERNAL_ENCRYPTION_IV_HEADER, "iv"),
            ],
        ),
        (
            "sse-kms",
            vec![
                ("x-amz-server-side-encryption", "aws:kms"),
                (MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER, "sealed"),
                (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER, "key-1"),
            ],
        ),
        (
            "legacy rustfs managed",
            vec![
                ("x-amz-server-side-encryption", "AES256"),
                (INTERNAL_ENCRYPTION_KEY_HEADER, "wrapped"),
                (INTERNAL_ENCRYPTION_IV_HEADER, "iv"),
            ],
        ),
        (
            "sse-c",
            vec![
                (SSEC_ALGORITHM_HEADER, "AES256"),
                (MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER, "sealed"),
            ],
        ),
    ];

    for (label, entries) in shapes {
        let metadata: HashMap<String, String> = entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();

        assert!(
            metadata
                .keys()
                .any(|key| rustfs_utils::http::is_object_encryption_marker(key)),
            "{label}: an encrypted object must be detected, otherwise a same-name copy would re-key it \
                 without rewriting the ciphertext"
        );

        let mut stripped = metadata.clone();
        strip_managed_encryption_metadata(&mut stripped);
        assert_ne!(
            stripped, metadata,
            "{label}: the copy path strips this shape's encryption metadata, so the destination cannot \
                 reuse the source ciphertext"
        );
    }
}

#[test]
fn plain_object_metadata_is_not_flagged_as_encrypted() {
    // The metadata-only self-copy shortcut must stay available for unencrypted objects.
    let metadata: HashMap<String, String> = [("content-type", "text/plain"), ("x-amz-meta-stage", "before")]
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();

    assert!(
        !metadata
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key))
    );
}

#[cfg(feature = "rio-v2")]
#[test]
fn test_legacy_managed_metadata_excludes_sealed_keys() {
    let legacy_metadata = HashMap::from([
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
    ]);
    assert!(is_legacy_rustfs_managed_metadata(&legacy_metadata));

    let sealed_metadata = HashMap::from([
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "encrypted-dek".to_string()),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "nonce".to_string()),
        (MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(), "sealed-key".to_string()),
    ]);
    assert!(!is_legacy_rustfs_managed_metadata(&sealed_metadata));
}

#[cfg(feature = "rio-v2")]
#[test]
fn test_normalize_managed_metadata_accepts_minio_only_headers() {
    let metadata = HashMap::from([
        (
            MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(b"encrypted-key"),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string([0x11u8; 12]),
        ),
        (
            MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(),
            MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string(),
        ),
        (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
    ]);

    let normalized = normalize_managed_metadata(&metadata, Some(recode_minio_kms_context));

    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_KEY_HEADER),
        Some(&BASE64_STANDARD.encode_to_string(b"encrypted-key"))
    );
    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_IV_HEADER),
        Some(&BASE64_STANDARD.encode_to_string([0x11u8; 12]))
    );
    assert_eq!(
        normalized.get(INTERNAL_ENCRYPTION_ALGORITHM_HEADER),
        Some(&MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM.to_string())
    );
    assert_eq!(normalized.get(INTERNAL_ENCRYPTION_KEY_ID_HEADER), Some(&"default".to_string()));
}

#[cfg(feature = "rio-v2")]
#[test]
fn test_supported_sealed_object_key_cipher_accepts_current_minio_fixture_value() {
    assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_AES_256_GCM));
    assert!(is_supported_sealed_object_key_cipher(DARE_CIPHER_CHACHA20_POLY1305));
    assert!(!is_supported_sealed_object_key_cipher(0x02));
}

/// Rewrites rio-v2 SSE-C metadata into the shape MinIO actually persists:
/// only the internal sealed-key slot, IV, and seal algorithm — the public
/// scheme, customer-algorithm, and customer-key-MD5 keys are synthesized
/// onto responses by MinIO and never hit disk (backlog#1638).
#[cfg(feature = "rio-v2")]
fn strip_to_minio_ssec_shape(metadata: &mut HashMap<String, String>) {
    metadata.remove("x-amz-server-side-encryption");
    metadata.remove("x-amz-server-side-encryption-customer-algorithm");
    metadata.remove("x-amz-server-side-encryption-customer-key-md5");
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_minio_shaped_ssec_metadata_decrypts_with_the_right_key_only() {
    let customer_key_bytes = [0x42u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");

    let mut metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    strip_to_minio_ssec_shape(&mut metadata);
    assert!(
        !metadata.contains_key("x-amz-server-side-encryption-customer-key-md5"),
        "the MinIO shape stores no customer-key MD5"
    );

    // Correct key: detected via the sealed slot, proven by the unseal.
    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped sse-c decryption")
    .expect("minio-shaped sse-c material");
    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    // Wrong key with a self-consistent MD5: the missing stored MD5 cannot
    // catch it, so the AEAD unseal must — the relaxation is not a bypass.
    let wrong_key_bytes = [0x43u8; 32];
    let wrong_key = BASE64_STANDARD.encode_to_string(wrong_key_bytes);
    let wrong_key_md5 = md5_base64(wrong_key_bytes);
    let result = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&wrong_key),
            sse_customer_key_md5: Some(&wrong_key_md5),
            principal: None,
        },
    )
    .await;
    assert!(result.is_err(), "a wrong customer key must fail the sealed-key unseal");

    // No key at all: detected as SSE-C, so the caller is asked for the
    // parameters instead of getting an opaque unreadable-object error.
    let result = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await;
    let error = result.expect_err("missing SSE-C parameters must fail closed");
    assert!(error.message.contains("must be provided"), "got: {}", error.message);
}

#[cfg(feature = "rio-v2")]
#[test]
fn test_mark_encrypted_multipart_metadata_sets_minio_marker() {
    let mut metadata = HashMap::new();
    mark_encrypted_multipart_metadata(&mut metadata);
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER));
}

#[test]
fn test_verify_ssec_key_match_tolerates_missing_md5_only_for_minio_sealed_ssec() {
    // MinIO SSE-C objects store no customer-key MD5; the AEAD unseal is the
    // real key proof, so the early check stands down for that shape only.
    assert!(verify_ssec_key_match("provided_md5", None, true).is_ok());
    // A stored MD5 always wins, even on the MinIO shape: a mismatch is a
    // mismatch.
    let stored = "stored_md5".to_string();
    assert!(verify_ssec_key_match("provided_md5", Some(&stored), true).is_err());
}

#[test]
fn test_verify_ssec_key_match_success() {
    let md5 = "test_md5".to_string();
    let result = verify_ssec_key_match("test_md5", Some(&md5), false);
    assert!(result.is_ok());
}

#[test]
fn test_verify_ssec_key_match_mismatch() {
    let md5 = "stored_md5".to_string();
    let result = verify_ssec_key_match("provided_md5", Some(&md5), false);
    assert!(result.is_err());
}

#[test]
fn test_verify_ssec_key_match_no_stored() {
    let result = verify_ssec_key_match("provided_md5", None, false);
    assert!(result.is_err());
}

fn multipart_ssec_request(key_byte: u8) -> EncryptionRequest<'static> {
    let key_bytes = [key_byte; 32];
    EncryptionRequest {
        bucket: "bucket",
        key: "object",
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(BASE64_STANDARD.encode_to_string(key_bytes)),
        sse_customer_key_md5: Some(md5_base64(key_bytes)),
        content_size: 1,
        principal: None,
    }
}

fn multipart_ssec_metadata(key_byte: u8) -> HashMap<String, String> {
    let key_bytes = [key_byte; 32];
    HashMap::from([
        ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
        ("x-amz-server-side-encryption-customer-key-md5".to_string(), md5_base64(key_bytes)),
    ])
}

#[test]
fn test_validate_multipart_ssec_exact_match() {
    assert!(
        multipart_ssec_request(42)
            .validate_multipart_ssec(&multipart_ssec_metadata(42))
            .is_ok()
    );
}

#[test]
fn test_validate_complete_multipart_ssec_allows_omitted_parameters() {
    let no_ssec = EncryptionRequest {
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ..multipart_ssec_request(42)
    };

    assert!(no_ssec.validate_complete_multipart_ssec(&multipart_ssec_metadata(42)).is_ok());
    assert!(no_ssec.validate_complete_multipart_ssec(&HashMap::new()).is_ok());

    for invalid_metadata in [
        HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string())]),
        HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES128".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), md5_base64([42u8; 32])),
        ]),
        HashMap::from([
            ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
            ("x-amz-server-side-encryption-customer-key-md5".to_string(), "invalid".to_string()),
        ]),
    ] {
        assert_eq!(
            no_ssec
                .validate_complete_multipart_ssec(&invalid_metadata)
                .expect_err("corrupt SSE-C session metadata must fail closed")
                .code,
            S3ErrorCode::InvalidRequest
        );
    }
}

#[test]
fn ssec_checksum_completion_requires_key_for_both_metadata_aliases() {
    let no_key = EncryptionRequest {
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ..multipart_ssec_request(42)
    };
    for prefix in ["x-rustfs-internal-", "x-minio-internal-"] {
        let mut metadata = multipart_ssec_metadata(42);
        metadata.insert(format!("{prefix}plaintext-checksum"), "true".to_string());
        assert_eq!(
            no_key
                .validate_complete_multipart_ssec(&metadata)
                .expect_err("no key cannot compare or reveal plaintext checksums")
                .code,
            S3ErrorCode::InvalidRequest
        );
        multipart_ssec_request(42)
            .validate_complete_multipart_ssec(&metadata)
            .expect("correct key");
        assert!(
            multipart_ssec_request(43)
                .validate_complete_multipart_ssec(&metadata)
                .is_err()
        );
    }
}

#[test]
fn test_validate_complete_multipart_ssec_still_validates_present_parameters() {
    let partial = EncryptionRequest {
        sse_customer_key: None,
        ..multipart_ssec_request(42)
    };
    assert_eq!(
        partial
            .validate_complete_multipart_ssec(&multipart_ssec_metadata(42))
            .expect_err("partial SSE-C parameters must fail")
            .code,
        S3ErrorCode::InvalidRequest
    );
    assert_eq!(
        multipart_ssec_request(43)
            .validate_complete_multipart_ssec(&multipart_ssec_metadata(42))
            .expect_err("wrong SSE-C parameters must fail")
            .code,
        S3ErrorCode::InvalidRequest
    );
}

#[test]
fn test_validate_multipart_ssec_rejects_wrong_key_without_leaking_it() {
    let request = multipart_ssec_request(43);
    let encoded_key = request.sse_customer_key.clone().expect("fixture key");
    let encoded_md5 = request.sse_customer_key_md5.clone().expect("fixture MD5");

    let error = request
        .validate_multipart_ssec(&multipart_ssec_metadata(42))
        .expect_err("wrong key must fail");

    assert_eq!(error.code, S3ErrorCode::InvalidRequest);
    assert!(!error.message.contains(&encoded_key));
    assert!(!error.message.contains(&encoded_md5));
}

#[test]
fn test_validate_multipart_ssec_rejects_missing_or_unexpected_parameters() {
    let no_ssec = EncryptionRequest {
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ..multipart_ssec_request(42)
    };
    assert_eq!(
        no_ssec
            .validate_multipart_ssec(&multipart_ssec_metadata(42))
            .expect_err("SSE-C session requires all parameters")
            .code,
        S3ErrorCode::InvalidRequest
    );
    assert_eq!(
        multipart_ssec_request(42)
            .validate_multipart_ssec(&HashMap::new())
            .expect_err("plaintext session rejects SSE-C parameters")
            .code,
        S3ErrorCode::InvalidRequest
    );
    let incomplete_session = HashMap::from([(
        "x-amz-server-side-encryption-customer-key-md5".to_string(),
        multipart_ssec_request(42).sse_customer_key_md5.expect("fixture MD5"),
    )]);
    assert_eq!(
        multipart_ssec_request(42)
            .validate_multipart_ssec(&incomplete_session)
            .expect_err("incomplete stored SSE-C metadata must fail closed")
            .code,
        S3ErrorCode::InvalidRequest
    );
}

// ============================================================================
// Integration Tests - Encrypt/Decrypt with SimpleSseDekProvider
// ============================================================================

#[test]
fn test_encrypt_dek_writes_json_with_random_nonces() {
    let dek = [0x11u8; 32];
    let cmk = [0x22u8; 32];

    let encrypted_a = TestSseDekProvider::encrypt_dek(dek, cmk).expect("first DEK wrap should succeed");
    let encrypted_b = TestSseDekProvider::encrypt_dek(dek, cmk).expect("second DEK wrap should succeed");

    let envelope_a: crate::kms::local::LocalSseDekEnvelope =
        serde_json::from_str(&encrypted_a).expect("first wrapped DEK should be a JSON envelope");
    let envelope_b: crate::kms::local::LocalSseDekEnvelope =
        serde_json::from_str(&encrypted_b).expect("second wrapped DEK should be a JSON envelope");

    assert_eq!(envelope_a.version, crate::kms::local::LOCAL_SSE_DEK_FORMAT_VERSION);
    assert_eq!(envelope_b.version, crate::kms::local::LOCAL_SSE_DEK_FORMAT_VERSION);
    assert_ne!(envelope_a.nonce, envelope_b.nonce, "each DEK wrap should use a distinct nonce");
    assert_eq!(
        TestSseDekProvider::decrypt_dek(&encrypted_a, cmk).expect("first JSON envelope should decrypt"),
        dek
    );
}

#[test]
fn test_decrypt_dek_accepts_legacy_zero_nonce_payload() {
    let dek = [0x33u8; 32];
    let cmk = [0x44u8; 32];
    let cipher = Aes256Gcm::new(&Key::<Aes256Gcm>::from(cmk));
    let legacy_nonce = Nonce::from([0u8; 12]);
    let ciphertext = cipher
        .encrypt(&legacy_nonce, dek.as_slice())
        .expect("legacy wrap should succeed");
    let legacy_payload = format!(
        "{}:{}",
        BASE64_STANDARD.encode_to_string(legacy_nonce),
        BASE64_STANDARD.encode_to_string(ciphertext)
    );

    let decrypted = TestSseDekProvider::decrypt_dek(&legacy_payload, cmk).expect("legacy payload should remain decryptable");
    assert_eq!(decrypted, dek);
}

#[test]
fn test_decrypt_dek_rejects_unknown_json_version() {
    let envelope = serde_json::json!({
        "version": crate::kms::local::LOCAL_SSE_DEK_FORMAT_VERSION + 1,
        "nonce": BASE64_STANDARD.encode_to_string([0u8; 12]),
        "ciphertext": BASE64_STANDARD.encode_to_string([0u8; 48]),
    })
    .to_string();

    let error =
        TestSseDekProvider::decrypt_dek(&envelope, [0x55u8; 32]).expect_err("unknown JSON envelope versions must fail closed");
    assert_eq!(error.code, S3ErrorCode::InternalError);
    assert_eq!(error.message, INTERNAL_ERROR_MESSAGE);
    let source = error.source.as_deref().expect("the error should retain its source");
    assert!(source.to_string().contains("Unsupported encrypted DEK format version"));
}

#[tokio::test]
async fn test_sse_encryption_fails_closed_without_local_sse_master_key() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
            ("RUSTFS_SSE_S3_MASTER_KEY", None::<&str>),
        ],
        async {
            let err = sse_encryption(
                &HOST,
                EncryptionRequest {
                    bucket: "test-bucket",
                    key: "test-key",
                    server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 1024,
                    principal: None,
                },
            )
            .await
            .expect_err("SSE-S3 should fail closed without a configured local master key");

            assert!(err.message.contains("RUSTFS_SSE_S3_MASTER_KEY"));
            assert_eq!(
                err.code,
                S3ErrorCode::InvalidRequest,
                "missing local SSE master key is a configuration error, not a 500 (rustfs#4844)"
            );
        },
    )
    .await;
    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_sse_encryption_fails_closed_with_invalid_local_sse_master_key() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some("not-base64")),
        ],
        async {
            let err = sse_encryption(
                &HOST,
                EncryptionRequest {
                    bucket: "test-bucket",
                    key: "test-key",
                    server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    content_size: 1024,
                    principal: None,
                },
            )
            .await
            .expect_err("SSE-S3 should fail closed with an invalid local master key");

            assert!(err.message.contains("valid base64"));
            assert_eq!(
                err.code,
                S3ErrorCode::InvalidRequest,
                "invalid local SSE master key is a configuration error, not a 500 (rustfs#4844)"
            );
        },
    )
    .await;
    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_managed_decryption_selects_provider_from_persisted_dek() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;

    let local_master_key = [7u8; 32];
    let local_provider = TestSseDekProvider::new_with_key(local_master_key);
    let context = ObjectEncryptionContext::new("bucket".to_string(), "object".to_string());
    let (data_key, encrypted_dek) = local_provider
        .generate_sse_dek(&context, "legacy-local-key")
        .await
        .expect("generate beta.5 local DEK");

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(BASE64_STANDARD.encode_to_string(local_master_key))),
        ],
        async {
            let metadata = HashMap::from([
                ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_KMS.to_string()),
                (
                    INTERNAL_ENCRYPTION_KEY_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(encrypted_dek),
                ),
                (
                    INTERNAL_ENCRYPTION_IV_HEADER.to_string(),
                    BASE64_STANDARD.encode_to_string(data_key.nonce),
                ),
                (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "legacy-local-key".to_string()),
            ]);

            let material = apply_managed_decryption_material(&HOST, "bucket", "object", &metadata, None)
                .await
                .expect("legacy local DEK should not be routed to the running KMS")
                .expect("managed metadata should produce decryption material");
            assert_eq!(material.key_bytes, data_key.plaintext_key);
            assert_eq!(material.sse_type, SSEType::SseKms);
        },
    )
    .await;

    manager.stop().await.expect("stop test KMS service");
    reset_sse_dek_provider();

    let kms_envelope = br#"{
            "key_id": "test-key-id",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [1, 2, 3, 4],
            "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_AES.to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(kms_envelope)),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([0x14; 12])),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "test-key-id".to_string()),
    ]);
    let error = match apply_managed_decryption_material(&HOST, "bucket", "object", &metadata, None).await {
        Ok(_) => panic!("KMS envelope must not fall back to the local provider"),
        Err(error) => error,
    };
    assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

    reset_sse_dek_provider();
}

/// A loopback stand-in for AWS KMS, so the real AWS backend — SDK client,
/// service manager and all — sits behind the SSE entry points under test.
///
/// It issues data keys the way AWS does: a binary blob that is not UTF-8
/// and carries nothing RustFS could recognize, decryptable only with the
/// encryption context it was generated under.
struct FakeAwsKms {
    endpoint: String,
    state: Arc<FakeAwsKmsState>,
    server: tokio::task::JoinHandle<()>,
}

/// Plaintext data key and the encryption context it was generated under.
type FakeAwsDataKey = ([u8; 32], serde_json::Value);

#[derive(Default)]
struct FakeAwsKmsState {
    data_keys: std::sync::Mutex<HashMap<Vec<u8>, FakeAwsDataKey>>,
    decrypt_calls: std::sync::atomic::AtomicUsize,
}

impl Drop for FakeAwsKms {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl FakeAwsKms {
    const KEY_ARN: &'static str = "arn:aws:kms:us-east-1:111122223333:key/test-key";

    async fn start() -> Self {
        let state = Arc::new(FakeAwsKmsState::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake AWS KMS listener");
        let endpoint = format!("http://{}", listener.local_addr().expect("fake AWS KMS address"));
        let app = axum::Router::new().fallback(Self::handle).with_state(state.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve fake AWS KMS");
        });
        Self { endpoint, state, server }
    }

    fn decrypt_calls(&self) -> usize {
        self.state.decrypt_calls.load(std::sync::atomic::Ordering::SeqCst)
    }

    async fn handle(
        axum::extract::State(state): axum::extract::State<Arc<FakeAwsKmsState>>,
        headers: axum::http::HeaderMap,
        body: axum::body::Bytes,
    ) -> axum::response::Response {
        let operation = headers
            .get("x-amz-target")
            .and_then(|target| target.to_str().ok())
            .and_then(|target| target.rsplit('.').next())
            .unwrap_or_default();
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let context = request
            .get("EncryptionContext")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let reply = match operation {
            "ListKeys" => Ok(serde_json::json!({ "Keys": [], "Truncated": false })),
            "DescribeKey" => Ok(serde_json::json!({
                "KeyMetadata": {
                    "KeyId": "test-key",
                    "Arn": Self::KEY_ARN,
                    "CreationDate": 1_700_000_000,
                    "Enabled": true,
                    "KeyUsage": "ENCRYPT_DECRYPT",
                    "KeyState": "Enabled",
                    "Origin": "AWS_KMS",
                    "KeyManager": "CUSTOMER",
                }
            })),
            "GenerateDataKey" => {
                let mut plaintext = [0u8; 32];
                rand::Rng::fill_bytes(&mut rand::rng(), &mut plaintext);
                // A continuation byte straight after ASCII is never valid
                // UTF-8, like the real blob in practice.
                let mut blob = vec![0x01, 0x02, 0x02, 0x00, 0x78];
                let mut tail = [0u8; 48];
                rand::Rng::fill_bytes(&mut rand::rng(), &mut tail);
                blob.extend(tail.map(|byte| 0x80 | (byte & 0x3f)));
                state
                    .data_keys
                    .lock()
                    .expect("fake AWS KMS state")
                    .insert(blob.clone(), (plaintext, context));
                Ok(serde_json::json!({
                    "KeyId": Self::KEY_ARN,
                    "Plaintext": BASE64_STANDARD.encode_to_string(plaintext),
                    "CiphertextBlob": BASE64_STANDARD.encode_to_string(&blob),
                }))
            }
            "Decrypt" => {
                state.decrypt_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let blob = request
                    .get("CiphertextBlob")
                    .and_then(serde_json::Value::as_str)
                    .and_then(|blob| BASE64_STANDARD.decode_to_vec(blob).ok())
                    .unwrap_or_default();
                match state.data_keys.lock().expect("fake AWS KMS state").get(&blob) {
                    Some((plaintext, sealed_context)) if *sealed_context == context => Ok(serde_json::json!({
                        "KeyId": Self::KEY_ARN,
                        "Plaintext": BASE64_STANDARD.encode_to_string(plaintext),
                        "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                    })),
                    _ => Err("InvalidCiphertextException"),
                }
            }
            _ => Err("UnsupportedOperationException"),
        };

        let (status, body) = match reply {
            Ok(body) => (axum::http::StatusCode::OK, body),
            Err(code) => (
                axum::http::StatusCode::BAD_REQUEST,
                serde_json::json!({ "__type": code, "message": code }),
            ),
        };
        axum::response::Response::builder()
            .status(status)
            .header("content-type", "application/x-amz-json-1.1")
            .body(axum::body::Body::from(body.to_string()))
            .expect("fake AWS KMS response")
    }
}

/// Run `test` with the global KMS service on the AWS backend, pointed at a
/// [`FakeAwsKms`], and no SSE provider injected — the wiring a deployment
/// with `RUSTFS_KMS_BACKEND=aws` and no local master key has.
async fn with_aws_kms_backend<F, Fut>(test: F)
where
    F: FnOnce(Arc<FakeAwsKms>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    let fake = Arc::new(FakeAwsKms::start().await);

    async_with_vars(
        [
            ("AWS_ACCESS_KEY_ID", Some("AKIDTEST")),
            ("AWS_SECRET_ACCESS_KEY", Some("secret")),
            ("AWS_SESSION_TOKEN", None),
            ("AWS_PROFILE", None),
            ("AWS_EC2_METADATA_DISABLED", Some("true")),
            ("HTTP_PROXY", None),
            ("HTTPS_PROXY", None),
            ("http_proxy", None),
            ("https_proxy", None),
            ("RUSTFS_SSE_S3_MASTER_KEY", None),
            ("__RUSTFS_SSE_SIMPLE_CMK", None),
        ],
        async {
            let mut config = rustfs_kms::KmsConfig::aws(Some("us-east-1".to_string())).with_insecure_development_defaults();
            let rustfs_kms::BackendConfig::Aws(aws) = &mut config.backend_config else {
                panic!("the AWS constructor must create an AWS backend configuration");
            };
            aws.endpoint_url = Some(fake.endpoint.clone());
            let manager = rustfs_kms::init_global_kms_service_manager();
            manager.reconfigure(config).await.expect("start the AWS KMS backend");
            let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
                .await
                .expect("kms provider should initialize from the AWS-backed manager");
            crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));

            test(fake.clone()).await;

            manager.stop().await.expect("stop the AWS KMS backend");
        },
    )
    .await;

    reset_sse_dek_provider();
}

fn aws_sse_kms_write() -> EncryptionRequest<'static> {
    EncryptionRequest {
        bucket: "bucket",
        key: "dir/object",
        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
        ssekms_key_id: Some("test-key".to_string()),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 4096,
        principal: None,
    }
}

async fn read_managed_object(
    metadata: &HashMap<String, String>,
) -> Result<crate::core::keying::material::DecryptionMaterial, SseError> {
    sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "dir/object",
            metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .map(|material| material.expect("an SSE-KMS object must yield decryption material"))
}

/// An SSE-KMS object written through the AWS backend must be readable.
///
/// It was not: the AWS data key reached storage as a bare blob, the read
/// path took the unrecognized bytes for a locally wrapped key, and every
/// GET failed asking for `RUSTFS_SSE_S3_MASTER_KEY` without AWS ever being
/// asked to decrypt.
#[tokio::test]
async fn sse_kms_object_written_through_the_aws_backend_reads_back() {
    with_aws_kms_backend(|aws| async move {
        let material = sse_encryption(&HOST, aws_sse_kms_write())
            .await
            .expect("sse-kms encryption")
            .expect("managed sse-kms material");
        let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
        // The rio-v2 writer keeps the wrapped data key in MinIO's slot only.
        let stored_key = metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
            .expect("stored data key");
        let stored_key = BASE64_STANDARD.decode_to_vec(stored_key).expect("stored data key is base64");
        assert!(
            rustfs_kms::is_data_key_envelope(&stored_key),
            "the stored data key must identify itself as KMS-wrapped"
        );

        let decrypted = read_managed_object(&metadata)
            .await
            .expect("an object written through the AWS backend must decrypt");

        assert_eq!(decrypted.key_bytes, material.key_bytes);
        assert_eq!(decrypted.sse_type, SSEType::SseKms);
        assert_eq!(aws.decrypt_calls(), 1, "the read must be served by AWS Decrypt");
    })
    .await;
}

/// Objects written by releases through 1.0.5 keep the AWS blob bare in
/// their metadata. They are intact and must decrypt as they are, without
/// being rewritten. A `rio-v2` build stored the same bare blob beside a
/// sealed object key, which is the shape MinIO's own objects have, so the
/// route has to hold there too.
#[tokio::test]
async fn sse_kms_object_with_a_bare_aws_data_key_reads_back() {
    with_aws_kms_backend(|aws| async move {
        let material = sse_encryption(&HOST, aws_sse_kms_write())
            .await
            .expect("sse-kms encryption")
            .expect("managed sse-kms material");
        let mut metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");

        // Rebuild what those releases persisted: the same headers, with
        // the blob AWS issued in place of the envelope around it.
        let envelope_b64 = metadata
            .get(INTERNAL_ENCRYPTION_KEY_HEADER)
            .or_else(|| metadata.get(MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER))
            .expect("stored data key")
            .clone();
        let envelope: serde_json::Value = serde_json::from_slice(
            &BASE64_STANDARD
                .decode_to_vec(&envelope_b64)
                .expect("stored data key is base64"),
        )
        .expect("stored data key is an envelope");
        let bare: Vec<u8> = serde_json::from_value(envelope["encrypted_key"].clone()).expect("envelope carries the AWS blob");
        let bare_b64 = BASE64_STANDARD.encode_to_string(&bare);
        let mut replaced = 0;
        for value in metadata.values_mut().filter(|value| **value == envelope_b64) {
            *value = bare_b64.clone();
            replaced += 1;
        }
        assert!(replaced > 0, "the stored data key must have been replaced");
        assert!(!rustfs_kms::is_data_key_envelope(&bare));

        let decrypted = read_managed_object(&metadata)
            .await
            .expect("an object carrying a bare AWS data key must decrypt");

        assert_eq!(decrypted.key_bytes, material.key_bytes);
        assert_eq!(aws.decrypt_calls(), 1, "the read must be served by AWS Decrypt");
    })
    .await;
}

/// The bare-blob route exists for AWS alone. No other backend claims an
/// unrecognized data key, and without a KMS it still belongs to the local
/// provider, which is the one that can say what is wrong with it.
#[tokio::test]
async fn bare_data_key_stays_with_the_local_provider_without_an_aws_backend() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    let local_kms = KmsSseDekProvider::new_with_service_manager(configure_test_global_local_kms().await)
        .await
        .expect("kms provider should initialize from the Local-backed manager");
    assert!(!local_kms.reads_bare_data_key_ciphertext().await);
    assert!(
        !TestSseDekProvider::new_with_key([9u8; 32])
            .reads_bare_data_key_ciphertext()
            .await
    );

    async_with_vars([("RUSTFS_SSE_S3_MASTER_KEY", None::<&str>), ("__RUSTFS_SSE_SIMPLE_CMK", None)], async {
        let metadata = HashMap::from([
            ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_KMS.to_string()),
            (
                INTERNAL_ENCRYPTION_KEY_HEADER.to_string(),
                BASE64_STANDARD.encode_to_string([0x01, 0x02, 0x02, 0x00, 0x78, 0x80, 0x81]),
            ),
            (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([0x14; 12])),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "test-key".to_string()),
        ]);

        let error = read_managed_object(&metadata)
            .await
            .expect_err("a bare blob has no owner without a KMS");

        assert!(
            error.message.contains("RUSTFS_SSE_S3_MASTER_KEY"),
            "the local provider must still own the diagnosis: {}",
            error.message
        );
    })
    .await;

    reset_sse_dek_provider();
}

/// A KMS stand-in that owns bare data keys and records what it is asked to unwrap.
#[cfg(feature = "rio-v2")]
struct BareKmsProvider {
    data_key: [u8; 32],
    unwrapped: std::sync::Mutex<Vec<Vec<u8>>>,
}

#[cfg(feature = "rio-v2")]
#[async_trait::async_trait]
impl SseDekProvider for BareKmsProvider {
    async fn generate_sse_dek(
        &self,
        _context: &ObjectEncryptionContext,
        _kms_key_id: &str,
    ) -> Result<(DataKey, Vec<u8>), SseError> {
        Err(SseError::internal("not exercised"))
    }

    async fn decrypt_sse_dek(
        &self,
        encrypted_dek: &[u8],
        _kms_key_id: &str,
        _context: &ObjectEncryptionContext,
    ) -> Result<[u8; 32], SseError> {
        self.unwrapped.lock().expect("unwrap log").push(encrypted_dek.to_vec());
        Ok(self.data_key)
    }

    async fn reads_bare_data_key_ciphertext(&self) -> bool {
        true
    }
}

/// Seal a data key the way MinIO's builtin KMS does, in its raw layout:
/// `sealed || iv[16] || nonce[12]`, AES-256-GCM under `HMAC-SHA256(master, iv)`.
#[cfg(feature = "rio-v2")]
fn seal_minio_kms_data_key(data_key: &[u8; 32], master_key: &[u8; 32], context: &ObjectEncryptionContext) -> Vec<u8> {
    use aes_gcm::{Aes256Gcm, KeyInit, aead::Aead};
    use hmac::Mac;
    let (iv, nonce) = ([0x21u8; 16], [0x42u8; 12]);
    let mut mac = crate::core::keying::object::HmacSha256::new_from_slice(master_key).expect("hmac key");
    mac.update(&iv);
    let sealing_key: [u8; 32] = mac.finalize().into_bytes().into();
    let aad = crate::kms::minio::minio_kms_associated_data(context).expect("associated data");
    let mut sealed = Aes256Gcm::new_from_slice(&sealing_key)
        .expect("sealing key")
        .encrypt(
            &aes_gcm::Nonce::from(nonce),
            aes_gcm::aead::Payload {
                msg: data_key,
                aad: &aad,
            },
        )
        .expect("seal");
    sealed.extend_from_slice(&iv);
    sealed.extend_from_slice(&nonce);
    sealed
}

/// A sealed object key with no RustFS envelope comes from MinIO or from a
/// `rio-v2` build on a backend that stored its ciphertext bare. Opening it
/// as MinIO's is an authenticated proof, so the genuine MinIO object keeps
/// its route and only the remainder reaches the KMS.
#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn unenveloped_sealed_data_key_goes_to_minio_first_and_the_kms_otherwise() {
    let context = ObjectEncryptionContext::new("bucket".to_string(), "dir/object".to_string());
    let master_key = [0x5Au8; 32];
    let minio_data_key = [0x11u8; 32];
    let minio_ciphertext = seal_minio_kms_data_key(&minio_data_key, &master_key, &context);
    let aws_blob = vec![
        0x01, 0x02, 0x02, 0x00, 0x78, 0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89,
    ];
    let kms = || {
        Arc::new(BareKmsProvider {
            data_key: [0x22u8; 32],
            unwrapped: std::sync::Mutex::new(Vec::new()),
        })
    };
    let local = || -> Result<Arc<dyn SseDekProvider>, SseError> { Ok(Arc::new(TestSseDekProvider::new_with_key(master_key))) };
    let no_local = || -> Result<Arc<dyn SseDekProvider>, SseError> {
        Err(SseError {
            code: S3ErrorCode::InvalidRequest,
            message: "no local master key".to_string(),
            source: None,
        })
    };

    // MinIO's own object on a deployment whose backend also reads bare keys.
    let provider = kms();
    let data_key =
        crate::kms::provider::decrypt_unenveloped_sealed_dek(Some(provider.clone()), local(), &minio_ciphertext, "key", &context)
            .await
            .expect("a MinIO-sealed data key must open under the local master key");
    assert_eq!(data_key, minio_data_key);
    assert!(
        provider.unwrapped.lock().expect("unwrap log").is_empty(),
        "a MinIO-sealed data key must never be sent to the KMS"
    );

    // A bare backend blob: with a local master key that cannot open it, and with none at all.
    for local in [local(), no_local()] {
        let provider = kms();
        let data_key =
            crate::kms::provider::decrypt_unenveloped_sealed_dek(Some(provider.clone()), local, &aws_blob, "key", &context)
                .await
                .expect("a bare backend blob must be unwrapped by the KMS");
        assert_eq!(data_key, [0x22u8; 32]);
        assert_eq!(*provider.unwrapped.lock().expect("unwrap log"), vec![aws_blob.clone()]);
    }

    // Without a backend that reads bare keys, the MinIO route is the only one.
    let data_key = crate::kms::provider::decrypt_unenveloped_sealed_dek(None, local(), &minio_ciphertext, "key", &context)
        .await
        .expect("the MinIO route is unchanged");
    assert_eq!(data_key, minio_data_key);
    crate::kms::provider::decrypt_unenveloped_sealed_dek(None, local(), &aws_blob, "key", &context)
        .await
        .expect_err("bytes MinIO did not seal have no owner without such a backend");
    let error = crate::kms::provider::decrypt_unenveloped_sealed_dek(None, no_local(), &minio_ciphertext, "key", &context)
        .await
        .expect_err("a missing local master key is still reported as such");
    assert_eq!((error.code, error.message.as_str()), (S3ErrorCode::InvalidRequest, "no local master key"));
}

#[test]
fn local_sse_dek_recognition_covers_exactly_the_local_formats() {
    let wrapped = TestSseDekProvider::encrypt_dek([7u8; 32], [9u8; 32]).expect("local wrap");
    assert!(crate::kms::local::is_local_sse_dek(wrapped.as_bytes()));
    assert!(crate::kms::local::is_local_sse_dek(b"bm9uY2U=:Y2lwaGVydGV4dA=="));
    // A damaged local envelope is still the local provider's to diagnose.
    assert!(crate::kms::local::is_local_sse_dek(br#"  {"version": 9"#));

    // An AWS blob: binary, and not UTF-8.
    assert!(!crate::kms::local::is_local_sse_dek(&[0x01, 0x02, 0x02, 0x00, 0x78, 0x80, 0x81]));
    // Text that merely contains a colon is not the legacy pair.
    assert!(!crate::kms::local::is_local_sse_dek(b"vault:v1:Y2lwaGVydGV4dA=="));
    assert!(!crate::kms::local::is_local_sse_dek(b"not base64:Y2lwaGVydGV4dA=="));
    assert!(!crate::kms::local::is_local_sse_dek(b""));
}

/// Regression test for "local provider cached → dynamically enable KMS → decrypt KMS envelope".
///
/// Verifies that a `TestSseDekProvider` previously cached in `GLOBAL_SSE_DEK_PROVIDER`
/// is NEVER selected to unwrap a KMS data-key envelope. The KMS-envelope branch must
/// read `GLOBAL_KMS_DEK_PROVIDER` (or fall back to `KmsSseDekProvider::new()`), not
/// the local-provider cache.
#[tokio::test]
async fn test_kms_envelope_never_routes_to_cached_local_provider() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();

    // 1. Populate GLOBAL_SSE_DEK_PROVIDER with a local provider — the kind
    //    that `get_local_sse_dek_provider` would cache when KMS is absent.
    let local_master_key = [0xAAu8; 32];
    *crate::kms::provider::GLOBAL_SSE_DEK_PROVIDER
        .write()
        .expect("write local provider into local cache") = Some(Arc::new(TestSseDekProvider::new_with_key(local_master_key)));

    // 2. Start a KMS service (dynamic enable).
    let manager = configure_test_global_local_kms().await;

    // 3. Construct a KMS JSON envelope — the persisted format of a KMS-wrapped DEK.
    //    is_data_key_envelope() will return true for this payload.
    let kms_envelope = br#"{
            "key_id": "envelope-key",
            "master_key_id": "master-key-id",
            "key_spec": "AES_256",
            "encrypted_key": [10, 20, 30, 40],
            "nonce": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            "encryption_context": {},
            "created_at": "2024-01-01T00:00:00+00:00"
        }"#;
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_AES.to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string(kms_envelope)),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([0x14; 12])),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "envelope-key".to_string()),
    ]);

    // 4. Decrypt the KMS envelope. Before the fix, this would pick up the cached
    //    TestSseDekProvider from GLOBAL_SSE_DEK_PROVIDER and either panic (wrong
    //    format) or produce garbage. After the fix, it routes to KmsSseDekProvider,
    //    which fails because the envelope contains dummy encrypted bytes that the
    //    test KMS cannot decrypt — but crucially the error code is NOT a local-
    //    provider error.
    let error = match apply_managed_decryption_material(&HOST, "bucket", "object", &metadata, None).await {
        Ok(_) => panic!("dummy KMS envelope must not produce valid decryption material"),
        Err(error) => error,
    };
    // The KMS service was reached (ServiceUnavailable or the KMS's own decrypt
    // failure), not a local-provider format error.
    assert!(
        error.code == S3ErrorCode::ServiceUnavailable || error.code == S3ErrorCode::InternalError,
        "KMS envelope must be routed to KMS provider, not local; got code {:?} msg '{}'",
        error.code,
        error.message,
    );

    manager.stop().await.expect("stop test KMS service");
    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_managed_encryption_preserves_provider_unavailable_error() {
    let _guard = lock_sse_test_state().await;
    let manager = rustfs_kms::init_global_kms_service_manager();
    manager.stop().await.expect("stop global KMS service");
    reset_sse_dek_provider();
    *crate::kms::provider::GLOBAL_SSE_DEK_PROVIDER
        .write()
        .expect("update SSE DEK provider cache") = Some(Arc::new(UnavailableSseDekProvider));

    let error = match apply_managed_encryption_material(
        &HOST,
        ManagedEncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: String::from(AMZ_ENCRYPTION_AES),
            kms_key_id: None,
            ssekms_context: None,
            content_size: 0,
            principal: None,
        },
    )
    .await
    {
        Ok(_) => panic!("provider unavailability must fail managed encryption"),
        Err(error) => error,
    };
    assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_AES.to_string()),
        (
            INTERNAL_ENCRYPTION_KEY_HEADER.to_string(),
            BASE64_STANDARD.encode_to_string(b"local-provider-format"),
        ),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([0x14; 12])),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "test-key-id".to_string()),
    ]);
    let error = match apply_managed_decryption_material(&HOST, "bucket", "object", &metadata, None).await {
        Ok(_) => panic!("provider unavailability must fail managed decryption"),
        Err(error) => error,
    };
    assert_eq!(error.code, S3ErrorCode::ServiceUnavailable);

    reset_sse_dek_provider();
}

#[tokio::test]
async fn test_kms_sse_dek_provider_uses_latest_reconfigured_service() {
    use rustfs_kms::config::KmsConfig;
    let _guard = lock_sse_test_state().await;

    let manager = Arc::new(rustfs_kms::KmsServiceManager::new());

    manager
        .reconfigure(KmsConfig::static_kms(
            "first-key".to_string(),
            BASE64_STANDARD.encode_to_string([0x11; 32]),
        ))
        .await
        .expect("first KMS reconfigure should succeed");

    let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
        .await
        .expect("provider should initialize");
    let context = ObjectEncryptionContext::new("bucket".to_string(), "object".to_string());
    provider
        .generate_sse_dek(&context, "first-key")
        .await
        .expect("provider should use the initial service");

    manager
        .reconfigure(KmsConfig::static_kms(
            "second-key".to_string(),
            BASE64_STANDARD.encode_to_string([0x22; 32]),
        ))
        .await
        .expect("second KMS reconfigure should succeed");

    provider
        .generate_sse_dek(&context, "second-key")
        .await
        .expect("provider should resolve the latest reconfigured service");

    manager.stop().await.expect("kms service should stop cleanly");
    let generate_error = provider
        .generate_sse_dek(&context, "second-key")
        .await
        .expect_err("stopped KMS must reject data-key generation");
    assert_eq!(generate_error.code, S3ErrorCode::ServiceUnavailable);
    let decrypt_error = provider
        .decrypt_sse_dek(b"{}", "second-key", &context)
        .await
        .expect_err("stopped KMS must reject data-key decryption");
    assert_eq!(decrypt_error.code, S3ErrorCode::ServiceUnavailable);
    #[cfg(feature = "rio-v2")]
    {
        let legacy_error = provider
            .decrypt_legacy_sse_dek(b"{}", "second-key", &context)
            .await
            .expect_err("stopped KMS must reject legacy data-key decryption");
        assert_eq!(legacy_error.code, S3ErrorCode::ServiceUnavailable);
    }
}

#[test]
fn test_encryption_type_enum() {
    // Test EncryptionType enum
    assert_eq!(SSEType::SseS3, SSEType::SseS3);
    assert_eq!(SSEType::SseKms, SSEType::SseKms);
    assert_eq!(SSEType::SseC, SSEType::SseC);
    assert_ne!(SSEType::SseS3, SSEType::SseKms);

    // Test Debug format
    let debug_str = format!("{:?}", SSEType::SseKms);
    assert!(debug_str.contains("SseKms"));
}

#[test]
fn test_verify_ssec_key_match_returns_invalid_request() {
    let stored = "stored_md5".to_string();
    let err = verify_ssec_key_match("wrong_md5", Some(&stored), false).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_verify_ssec_key_match_no_stored_returns_invalid_request() {
    let err = verify_ssec_key_match("any_md5", None, false).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_ssec_for_read_non_encrypted_object() {
    let metadata = HashMap::new();
    let result = validate_ssec_for_read(&metadata, None, None);
    assert!(result.is_ok());
}

#[test]
fn test_validate_ssec_for_read_missing_customer_key() {
    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    metadata.insert(
        "x-amz-server-side-encryption-customer-key-md5".to_string(),
        "DWygnHRtgiJ77HCm+1rvHw==".to_string(),
    );

    let err = validate_ssec_for_read(&metadata, None, None).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_ssec_for_read_wrong_key() {
    // Key A is used to "encrypt" the object (stored MD5 is from key A).
    let key_a = [42u8; 32];
    let stored_md5 = md5_base64(key_a);

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), stored_md5);

    // Key B is a different key; its MD5 won't match stored MD5.
    let key_b = [99u8; 32];
    let key_b_b64 = BASE64_STANDARD.encode_to_string(key_b);
    let key_b_md5 = md5_base64(key_b);

    let err = validate_ssec_for_read(&metadata, Some(&key_b_b64), Some(&key_b_md5)).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_ssec_for_read_correct_key() {
    let key_bytes = [42u8; 32];
    let key_b64 = BASE64_STANDARD.encode_to_string(key_bytes);
    let key_md5 = md5_base64(key_bytes);

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.clone());

    let result = validate_ssec_for_read(&metadata, Some(&key_b64), Some(&key_md5));
    assert!(result.is_ok());
}

#[test]
fn test_validate_ssec_for_read_spoofed_md5() {
    // A client provides the correct stored MD5 in the header but with a
    // DIFFERENT key. The server must recompute MD5 from the key bytes and
    // reject the request because the recomputed MD5 won't match the header.
    let real_key = [42u8; 32];
    let stored_md5 = md5_base64(real_key);

    let mut metadata = HashMap::new();
    metadata.insert("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string());
    metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), stored_md5.clone());

    // Attacker has a different key but tries to pass the stored MD5 as their header
    let fake_key = [99u8; 32];
    let fake_key_b64 = BASE64_STANDARD.encode_to_string(fake_key);

    let err = validate_ssec_for_read(&metadata, Some(&fake_key_b64), Some(&stored_md5)).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_sse_headers_for_read_rejects_kms_on_plain_object() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("aws:kms"));
    headers.insert("x-amz-server-side-encryption-aws-kms-key-id", http::HeaderValue::from_static("test-key"));

    let metadata = HashMap::new();
    let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_validate_sse_headers_for_read_rejects_ssec_on_plain_object() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        http::HeaderValue::from_static("AES256"),
    );
    headers.insert("x-amz-server-side-encryption-customer-key", http::HeaderValue::from_static("test-key"));
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        http::HeaderValue::from_static("test-key-md5"),
    );

    let metadata = HashMap::new();
    let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_sse_headers_for_read_rejects_ssec_on_managed_object() {
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-server-side-encryption-customer-algorithm",
        http::HeaderValue::from_static("AES256"),
    );
    headers.insert("x-amz-server-side-encryption-customer-key", http::HeaderValue::from_static("test-key"));
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        http::HeaderValue::from_static("test-key-md5"),
    );

    let metadata = HashMap::from([("x-amz-server-side-encryption".to_string(), "aws:kms".to_string())]);
    let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_validate_sse_headers_for_read_allows_encrypted_object_without_request_headers() {
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
        ("x-rustfs-encryption-key".to_string(), "encrypted-key".to_string()),
    ]);
    let headers = HeaderMap::new();
    assert!(validate_sse_headers_for_read(&metadata, &headers).is_ok());
}

#[test]
fn test_validate_sse_headers_for_read_rejects_sse_on_ssec_object() {
    let mut headers = HeaderMap::new();
    headers.insert("x-amz-server-side-encryption", http::HeaderValue::from_static("aws:kms"));
    headers.insert("x-amz-server-side-encryption-aws-kms-key-id", http::HeaderValue::from_static("test-key"));

    let metadata = HashMap::from([("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string())]);
    let err = validate_sse_headers_for_read(&metadata, &headers).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
}

#[test]
fn test_validate_ssec_params_returns_invalid_request_on_bad_algorithm() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let key_md5 = md5_base64([42u8; 32]);
    let params = SsecParams {
        algorithm: "AES128".to_string(),
        key,
        key_md5,
    };
    let err = validate_ssec_params(params).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn test_validate_ssec_params_returns_invalid_request_on_bad_md5() {
    let key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let params = SsecParams {
        algorithm: "AES256".to_string(),
        key,
        key_md5: BASE64_STANDARD.encode_to_string([99u8; 16]),
    };
    let err = validate_ssec_params(params).unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);
}

// ========================================================================
// Unit tests for issue #2041: no mandatory KMS when encryption not used
// ========================================================================

/// When SSE-C params are not present and no managed SSE is requested,
/// encryption should be skipped (Ok(None)). Ensures we do not require KMS
/// when the client sends no encryption headers.
#[tokio::test]
async fn test_sse_encryption_skip_when_no_ssec_and_no_managed_sse_requested() {
    let request = EncryptionRequest {
        bucket: "test-bucket",
        key: "test-key",
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 1024,
        principal: None,
    };
    let result = sse_encryption(&HOST, request).await;
    match &result {
        Ok(None) => {}
        Ok(Some(_)) => panic!("expected no encryption material when no SSE params provided"),
        Err(e) => {
            assert!(
                !e.message.contains("No KMS key"),
                "must not require KMS when no encryption requested; got: {}",
                e.message
            );
        }
    }
}

/// When SSE-C params are partial or invalid, sse_encryption must return an error.
#[tokio::test]
async fn test_sse_encryption_errors_on_invalid_ssec_params() {
    let bucket = "test-bucket";
    let key = "test-key";
    let sse_key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let wrong_md5 = BASE64_STANDARD.encode_to_string([99u8; 16]);

    let request_wrong_md5 = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("AES256".to_string()),
        sse_customer_key: Some(sse_key.clone()),
        sse_customer_key_md5: Some(wrong_md5),
        content_size: 1024,
        principal: None,
    };
    let err = sse_encryption(&HOST, request_wrong_md5).await.unwrap_err();
    assert_eq!(err.code, S3ErrorCode::InvalidRequest);

    let request_unsupported_algorithm = EncryptionRequest {
        bucket,
        key,
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: Some("unsupported-algo".to_string()),
        sse_customer_key: Some(sse_key),
        sse_customer_key_md5: Some(md5_base64([42u8; 32])),
        content_size: 1024,
        principal: None,
    };
    let err = sse_encryption(&HOST, request_unsupported_algorithm).await.unwrap_err();
    assert!(err.code == S3ErrorCode::InvalidRequest || err.code == S3ErrorCode::InvalidArgument);
}

/// When bucket has no SSE-S3/aws:kms setting and request has no SSE headers,
/// encryption should be skipped (Ok(None)). Ensures no mandatory bucket default SSE.
#[tokio::test]
async fn test_sse_prepare_encryption_skip_when_no_params_and_no_bucket_sse() {
    let request = PrepareEncryptionRequest {
        bucket: "test-bucket-no-sse-config",
        key: "test-key",
        server_side_encryption: None,
        ssekms_key_id: None,
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        principal: None,
    };
    let result = sse_prepare_encryption(&HOST, request).await;
    match &result {
        Ok(None) => {}
        Ok(Some(_)) => panic!("expected no encryption when bucket has no SSE config and no request SSE"),
        Err(e) => {
            assert!(
                !e.message.contains("No KMS key"),
                "must not require KMS when no bucket SSE and no request SSE; got: {}",
                e.message
            );
        }
    }
}

// ------------------------------------------------------------------------
// Per-key KMS authorization on the SSE-KMS data path (rustfs/backlog#1582)
// ------------------------------------------------------------------------

fn enforcing_principal(allowed: bool) -> (SseKmsPrincipal, Arc<RecordingKmsKeyAuthorizer>) {
    let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
    (SseKmsPrincipal::for_test("analyst", true, authorizer.clone()), authorizer)
}

fn permissive_principal_with_enforcement_off() -> (SseKmsPrincipal, Arc<RecordingKmsKeyAuthorizer>) {
    let authorizer = RecordingKmsKeyAuthorizer::new(false);
    (SseKmsPrincipal::for_test("analyst", false, authorizer.clone()), authorizer)
}

fn sse_kms_write(principal: Option<&SseKmsPrincipal>) -> EncryptionRequest<'_> {
    EncryptionRequest {
        bucket: "finance",
        key: "ledger.csv",
        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
        ssekms_key_id: Some("finance-key".to_string()),
        ssekms_context: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 128,
        principal,
    }
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_managed_sse_rio_v2_uses_object_key_metadata_roundtrip() {
    let _guard = lock_sse_test_state().await;
    reset_sse_dek_provider();
    if let Some(manager) = rustfs_kms::get_global_kms_service_manager() {
        let _ = manager.stop().await;
    }
    let local_sse_master_key = local_sse_master_key_b64();

    async_with_vars(
        [
            ("__RUSTFS_SSE_SIMPLE_CMK", None::<&str>),
            ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key.as_str())),
        ],
        async {
            let request = EncryptionRequest {
                bucket: "bucket",
                key: "object",
                server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                content_size: 4096,
                principal: None,
            };

            let material = sse_encryption(&HOST, request)
                .await
                .expect("managed sse encryption")
                .expect("managed sse material");
            assert_eq!(material.key_kind, EncryptionKeyKind::Object);

            let metadata = encryption_material_to_metadata(&material).expect("managed SSE metadata should serialize");
            assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER));
            assert!(!metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));

            let sealing_iv = metadata
                .get(MINIO_INTERNAL_ENCRYPTION_IV_HEADER)
                .expect("minio sealing iv should be stored");
            let sealed_key = metadata
                .get(MINIO_INTERNAL_ENCRYPTION_S3_SEALED_KEY_HEADER)
                .expect("minio sealed key should be stored");
            assert_eq!(BASE64_STANDARD.decode_to_vec(sealing_iv).expect("decode iv").len(), SEALED_KEY_IV_SIZE);
            assert_eq!(
                BASE64_STANDARD.decode_to_vec(sealed_key).expect("decode sealed key").len(),
                SEALED_KEY_SIZE
            );

            let decrypted = sse_decryption(
                &HOST,
                DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                },
            )
            .await
            .expect("managed sse decryption")
            .expect("managed decryption material");

            assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
            assert_eq!(decrypted.key_bytes, material.key_bytes);
        },
    )
    .await;

    reset_sse_dek_provider();
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_ssec_rio_v2_uses_sealed_object_key_metadata_roundtrip() {
    let customer_key_bytes = [0x42u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");

    assert_eq!(material.key_kind, EncryptionKeyKind::Object);

    let metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    assert_eq!(
        metadata.get(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER).map(String::as_str),
        Some(MINIO_INTERNAL_ENCRYPTION_SEAL_ALGORITHM)
    );
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_IV_HEADER));
    assert!(metadata.contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER));

    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("sse-c decryption")
    .expect("sse-c decryption material");

    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);
}

#[cfg(feature = "rio-v2")]
#[tokio::test]
async fn test_minio_shaped_ssec_metadata_resolves_and_classifies() {
    let customer_key_bytes = [0x51u8; 32];
    let customer_key = BASE64_STANDARD.encode_to_string(customer_key_bytes);
    let customer_key_md5 = md5_base64(customer_key_bytes);

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key.clone()),
            sse_customer_key_md5: Some(customer_key_md5.clone()),
            content_size: 4096,
            principal: None,
        },
    )
    .await
    .expect("sse-c encryption")
    .expect("sse-c material");
    let mut metadata = encryption_material_to_metadata(&material).expect("sse-c metadata should serialize");
    strip_to_minio_ssec_shape(&mut metadata);

    let mut headers = HeaderMap::new();
    headers.insert("x-amz-server-side-encryption-customer-algorithm", HeaderValue::from_static("AES256"));
    headers.insert(
        "x-amz-server-side-encryption-customer-key",
        HeaderValue::from_str(&customer_key).expect("customer key header"),
    );
    headers.insert(
        "x-amz-server-side-encryption-customer-key-md5",
        HeaderValue::from_str(&customer_key_md5).expect("customer key MD5 header"),
    );
    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped SSE-C decryption")
    .expect("minio-shaped SSE-C material");
    assert_eq!(decrypted.key_kind, EncryptionKeyKind::Object);
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    // HEAD-path key validation: no stored MD5 to compare against, so a
    // self-consistent key passes here and the unseal remains the proof.
    crate::core::s3::ssec::validate_ssec_for_read(&metadata, Some(&customer_key), Some(&customer_key_md5))
        .expect("self-consistent SSE-C parameters validate");
    crate::core::s3::ssec::validate_ssec_for_read(&metadata, None, None).expect_err("missing SSE-C parameters must be rejected");

    // Response classification reports SSE-C, not managed SSE.
    let classified = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &metadata,
            sse_customer_key: Some(&customer_key),
            sse_customer_key_md5: Some(&customer_key_md5),
            principal: None,
        },
    )
    .await
    .expect("minio-shaped sse-c classification")
    .expect("minio-shaped sse-c response headers");
    assert!(classified.sse_customer_algorithm.is_some());
    assert!(classified.ssekms_key_id.is_none());
}

fn sse_s3_object_metadata() -> HashMap<String, String> {
    HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_AES.to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string([7u8; 48])),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([9u8; 12])),
        (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
    ])
}

#[test]
fn kms_authorization_subject_scopes_to_enforced_sse_kms_requests_only() {
    let (principal, _) = enforcing_principal(true);

    assert!(crate::core::s3::authorization::kms_authorization_subject(true, Some(&principal), SSEType::SseKms).is_some());
    // Compatibility switch off keeps the pre-enforcement behaviour.
    assert!(crate::core::s3::authorization::kms_authorization_subject(false, Some(&principal), SSEType::SseKms).is_none());
    // SSE-S3 and SSE-C are exempt, matching AWS.
    assert!(crate::core::s3::authorization::kms_authorization_subject(true, Some(&principal), SSEType::SseS3).is_none());
    assert!(crate::core::s3::authorization::kms_authorization_subject(true, Some(&principal), SSEType::SseC).is_none());
    // No principal means an internal caller.
    assert!(crate::core::s3::authorization::kms_authorization_subject(true, None, SSEType::SseKms).is_none());
}

#[tokio::test]
async fn sse_kms_write_without_generate_data_key_is_denied() {
    let (principal, authorizer) = enforcing_principal(false);

    let error = sse_encryption(&HOST, sse_kms_write(Some(&principal)))
        .await
        .expect_err("SSE-KMS write must fail without kms:GenerateDataKey on the key");

    assert_eq!(error.code, S3ErrorCode::AccessDenied);
    assert_eq!(authorizer.calls(), vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]);
}

#[tokio::test]
async fn sse_kms_write_with_generate_data_key_clears_the_gate() {
    let (principal, authorizer) = enforcing_principal(true);

    let outcome = sse_encryption(&HOST, sse_kms_write(Some(&principal))).await;

    assert!(
        !matches!(&outcome, Err(error) if error.code == S3ErrorCode::AccessDenied),
        "granting kms:GenerateDataKey on the key must clear the authorization gate"
    );
    assert_eq!(authorizer.calls(), vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]);
}

#[tokio::test]
async fn sse_kms_read_without_decrypt_is_denied() {
    let (principal, authorizer) = enforcing_principal(false);
    let metadata = sse_kms_object_metadata();

    let error = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        },
    )
    .await
    .expect_err("SSE-KMS read must fail without kms:Decrypt on the key");

    assert_eq!(error.code, S3ErrorCode::AccessDenied);
    assert_eq!(authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);
}

#[tokio::test]
async fn sse_s3_objects_are_exempt_from_kms_key_authorization() {
    let (principal, authorizer) = enforcing_principal(false);
    let metadata = sse_s3_object_metadata();

    let write = sse_encryption(
        &HOST,
        EncryptionRequest {
            server_side_encryption: Some(String::from(AMZ_ENCRYPTION_AES)),
            ssekms_key_id: None,
            ..sse_kms_write(Some(&principal))
        },
    )
    .await;
    let read = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        },
    )
    .await;

    assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
    assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
    assert!(authorizer.calls().is_empty(), "SSE-S3 must never consult KMS key policy");
}

#[tokio::test]
async fn ssec_requests_are_exempt_from_kms_key_authorization() {
    let (principal, authorizer) = enforcing_principal(false);
    let customer_key = BASE64_STANDARD.encode_to_string([42u8; 32]);
    let customer_key_md5 = md5_base64([42u8; 32]);

    let outcome = sse_encryption(
        &HOST,
        EncryptionRequest {
            server_side_encryption: None,
            ssekms_key_id: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(customer_key),
            sse_customer_key_md5: Some(customer_key_md5),
            ..sse_kms_write(Some(&principal))
        },
    )
    .await;

    assert!(outcome.is_ok(), "SSE-C must not be gated on KMS key policy");
    assert!(authorizer.calls().is_empty(), "SSE-C never reaches KMS");
}

#[tokio::test]
async fn internal_callers_bypass_kms_key_authorization() {
    let metadata = sse_kms_object_metadata();

    // No principal: replication, lifecycle transition, heal and scanner reach the SSE
    // layer below the S3 authorization boundary and must not be interrupted.
    let write = sse_encryption(&HOST, sse_kms_write(None)).await;
    let read = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await;

    assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
    assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
}

#[tokio::test]
async fn enforcement_switch_off_preserves_current_behaviour() {
    let (principal, authorizer) = permissive_principal_with_enforcement_off();
    let metadata = sse_kms_object_metadata();

    let write = sse_encryption(&HOST, sse_kms_write(Some(&principal))).await;
    let read = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        },
    )
    .await;

    assert!(!matches!(&write, Err(error) if error.code == S3ErrorCode::AccessDenied));
    assert!(!matches!(&read, Err(error) if error.code == S3ErrorCode::AccessDenied));
    assert!(
        authorizer.calls().is_empty(),
        "with the switch off the data path must not evaluate KMS key policy at all"
    );
}

#[tokio::test]
async fn multipart_write_authorization_uses_stored_key_and_current_principal() {
    let (denied, denied_authorizer) = enforcing_principal(false);
    let (allowed, allowed_authorizer) = enforcing_principal(true);
    let metadata = sse_kms_object_metadata();
    let error = crate::core::s3::authorization::authorize_sse_kms_multipart_write(&HOST, Some(&denied), &metadata)
        .await
        .expect_err("upload ID must not grant its creator's KMS permission");
    assert_eq!(error.code, S3ErrorCode::AccessDenied);
    assert_eq!(
        denied_authorizer.calls(),
        vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]
    );
    crate::core::s3::authorization::authorize_sse_kms_multipart_write(&HOST, Some(&allowed), &metadata)
        .await
        .expect("authorized writer");
    assert_eq!(
        allowed_authorizer.calls(),
        vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string())]
    );
    for metadata in [HashMap::new(), sse_s3_object_metadata(), multipart_ssec_metadata(42)] {
        crate::core::s3::authorization::authorize_sse_kms_multipart_write(&HOST, Some(&denied), &metadata)
            .await
            .expect("non-KMS upload");
    }
    assert_eq!(denied_authorizer.calls().len(), 1);
    let (off, off_authorizer) = permissive_principal_with_enforcement_off();
    crate::core::s3::authorization::authorize_sse_kms_multipart_write(&HOST, Some(&off), &metadata)
        .await
        .expect("enforcement off");
    crate::core::s3::authorization::authorize_sse_kms_multipart_write(&HOST, None, &metadata)
        .await
        .expect("trusted internal writer");
    assert!(off_authorizer.calls().is_empty());
}

#[tokio::test]
async fn copy_source_read_authorization_targets_the_source_key() {
    let (denied, denied_authorizer) = enforcing_principal(false);
    let (allowed, allowed_authorizer) = enforcing_principal(true);
    let metadata = sse_kms_object_metadata();

    let error = authorize_sse_kms_object_read(&HOST, Some(&denied), &metadata)
        .await
        .expect_err("copying an SSE-KMS source must require kms:Decrypt on the source key");
    assert_eq!(error.code, S3ErrorCode::AccessDenied);
    assert_eq!(denied_authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);

    authorize_sse_kms_object_read(&HOST, Some(&allowed), &metadata)
        .await
        .expect("kms:Decrypt on the source key must allow the copy");
    assert_eq!(allowed_authorizer.calls(), vec![(KmsAction::DecryptAction, "finance-key".to_string())]);
}

#[tokio::test]
async fn copy_source_read_authorization_skips_unencrypted_and_sse_s3_sources() {
    let (principal, authorizer) = enforcing_principal(false);

    authorize_sse_kms_object_read(&HOST, Some(&principal), &HashMap::new())
        .await
        .expect("plaintext sources are not gated");
    authorize_sse_kms_object_read(&HOST, Some(&principal), &sse_s3_object_metadata())
        .await
        .expect("SSE-S3 sources are not gated");

    assert!(authorizer.calls().is_empty());
}

// ========================================================================
// Data-plane KMS audit attachment
// ========================================================================

fn audited_principal(enforced: bool, allowed: bool) -> (SseKmsPrincipal, Arc<crate::core::audit::KmsRequestAudit>) {
    let audit = Arc::new(crate::core::audit::KmsRequestAudit::default());
    let authorizer = RecordingKmsKeyAuthorizer::new(allowed);
    let principal = SseKmsPrincipal::for_test("analyst", enforced, authorizer).with_request_audit(audit.clone());
    (principal, audit)
}

fn audit_tag(tags: &[(&'static str, serde_json::Value)], key: &str) -> Option<String> {
    tags.iter()
        .find(|(candidate, _)| *candidate == key)
        .and_then(|(_, value)| value.as_str().map(str::to_string))
}

#[tokio::test]
async fn sse_kms_write_and_read_summarise_key_id_and_outcome_for_the_audit_entry() {
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
    let _guard = lock_sse_test_state().await;

    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;
    manager
        .get_encryption_service()
        .await
        .expect("encryption service should exist")
        .create_key(CreateKeyRequest {
            key_name: Some("audit-key".to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        })
        .await
        .expect("kms test key should be created");
    let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
        .await
        .expect("kms provider should initialize from the configured test manager");
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));

    let (write_principal, write_audit) = audited_principal(false, true);
    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
            ssekms_key_id: Some("audit-key".to_string()),
            ssekms_context: Some(HashMap::from([("tenant".to_string(), "acct-4711".to_string())])),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 128,
            principal: Some(&write_principal),
        },
    )
    .await
    .expect("sse-kms encryption should succeed")
    .expect("managed sse-kms material");

    let write_tags = write_audit.audit_tags();
    assert_eq!(audit_tag(&write_tags, "sseType").as_deref(), Some("SSE-KMS"));
    assert_eq!(audit_tag(&write_tags, "kmsKeyId").as_deref(), Some("audit-key"));
    assert_eq!(audit_tag(&write_tags, "kmsOutcome").as_deref(), Some("success"));
    assert_eq!(audit_tag(&write_tags, "kmsErrorClass"), None, "a successful request has no error class");

    // The audit entry reaches every configured target: no encoding of the data
    // key, and no caller-supplied encryption-context value, may appear in it.
    let rendered = format!("{write_tags:?}");
    let encrypted_data_key = material.encrypted_data_key.clone().expect("managed sse wraps a data key");
    for secret in [
        BASE64_STANDARD.encode_to_string(&encrypted_data_key),
        BASE64_STANDARD.encode_to_string(material.key_bytes),
        format!("{:?}", material.key_bytes),
        format!("{encrypted_data_key:?}"),
        "acct-4711".to_string(),
    ] {
        assert!(!rendered.contains(&secret), "audit tags must not carry key material: {rendered}");
    }

    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");
    let (read_principal, read_audit) = audited_principal(false, true);
    sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&read_principal),
        },
    )
    .await
    .expect("sse-kms decryption should succeed")
    .expect("managed sse-kms material");

    let read_tags = read_audit.audit_tags();
    assert_eq!(audit_tag(&read_tags, "sseType").as_deref(), Some("SSE-KMS"));
    assert_eq!(audit_tag(&read_tags, "kmsKeyId").as_deref(), Some("audit-key"));
    assert_eq!(audit_tag(&read_tags, "kmsOutcome").as_deref(), Some("success"));
}

async fn install_test_kms_provider(key_name: &str) {
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage};

    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;
    manager
        .get_encryption_service()
        .await
        .expect("encryption service should exist")
        .create_key(CreateKeyRequest {
            key_name: Some(key_name.to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        })
        .await
        .expect("kms test key should be created");
    let provider = KmsSseDekProvider::new_with_service_manager(manager)
        .await
        .expect("kms provider should initialize from the configured test manager");
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));
}

fn sse_kms_request<'a>(bucket: &'a str, key: &'a str, context: HashMap<String, String>) -> EncryptionRequest<'a> {
    EncryptionRequest {
        bucket,
        key,
        server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
        ssekms_key_id: Some("binding-key".to_string()),
        ssekms_context: Some(context),
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        content_size: 64,
        principal: None,
    }
}

#[tokio::test]
async fn sse_kms_write_rejects_a_context_key_equal_to_the_bucket_name() {
    let _guard = lock_sse_test_state().await;
    install_test_kms_provider("binding-key").await;
    let context = HashMap::from([("finance".to_string(), "finance/elsewhere".to_string())]);

    let err = sse_encryption(&HOST, sse_kms_request("finance", "ledger.csv", context.clone()))
        .await
        .expect_err("a context entry under the bucket name must be rejected");
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);
    assert!(
        err.message.contains("\"finance\""),
        "the message must name the offending key: {}",
        err.message
    );

    // CreateMultipartUpload generates the session data key through the
    // prepare path; it must refuse the same context.
    let err = sse_prepare_encryption(
        &HOST,
        PrepareEncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
            ssekms_key_id: Some("binding-key".to_string()),
            ssekms_context: Some(context),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect_err("the multipart prepare path must reject the same context");
    assert_eq!(err.code, S3ErrorCode::InvalidArgument);

    reset_sse_dek_provider();
}

#[tokio::test]
async fn sse_kms_write_keeps_the_location_binding_when_the_context_names_another_bucket() {
    let _guard = lock_sse_test_state().await;
    install_test_kms_provider("binding-key").await;
    let context = HashMap::from([
        ("archive".to_string(), "archive/ledger.csv".to_string()),
        ("tenant".to_string(), "acct-4711".to_string()),
    ]);

    let material = sse_encryption(&HOST, sse_kms_request("finance", "ledger.csv", context))
        .await
        .expect("a key naming a different bucket is an ordinary context entry")
        .expect("managed sse-kms material");
    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");

    let read = |key: &'static str| {
        let metadata = metadata.clone();
        async move {
            sse_decryption(
                &HOST,
                DecryptionRequest {
                    bucket: "finance",
                    key,
                    metadata: &metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                },
            )
            .await
        }
    };
    let decrypted = read("ledger.csv")
        .await
        .expect("the object decrypts at its own location")
        .expect("managed sse-kms material");
    assert_eq!(decrypted.key_bytes, material.key_bytes);
    read("relocated.csv")
        .await
        .expect_err("the data key stays bound to the object's location");

    reset_sse_dek_provider();
}

#[tokio::test]
async fn sse_kms_objects_stored_with_a_bucket_keyed_context_stay_readable() {
    let _guard = lock_sse_test_state().await;
    install_test_kms_provider("binding-key").await;
    let context = HashMap::from([("finance".to_string(), "finance/elsewhere".to_string())]);

    // Objects written before the write-side rejection reached the inner
    // routine without validation; reproduce that stored shape directly.
    let material = apply_managed_encryption_material_inner(
        &HOST,
        ManagedEncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: String::from(AMZ_ENCRYPTION_KMS),
            kms_key_id: Some("binding-key".to_string()),
            ssekms_context: Some(context),
            content_size: 64,
            principal: None,
        },
    )
    .await
    .expect("the inner routine does not validate the client context");
    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");

    let decrypted = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("an existing object must stay readable after the write-side rejection")
    .expect("managed sse-kms material");
    assert_eq!(decrypted.key_bytes, material.key_bytes);

    reset_sse_dek_provider();
}

#[tokio::test]
async fn sse_c_requests_produce_no_kms_audit_summary() {
    let key = [0x21u8; 32];
    let (principal, audit) = audited_principal(false, true);

    sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: Some("AES256".to_string()),
            sse_customer_key: Some(BASE64_STANDARD.encode_to_string(key)),
            sse_customer_key_md5: Some(md5_base64(key)),
            content_size: 128,
            principal: Some(&principal),
        },
    )
    .await
    .expect("sse-c encryption should succeed")
    .expect("sse-c material");

    assert!(
        audit.audit_tags().is_empty(),
        "SSE-C keys never reach KMS, so the request has no KMS outcome to report"
    );
}

#[tokio::test]
async fn denied_sse_kms_read_is_summarised_as_an_access_denied_failure() {
    let (principal, audit) = audited_principal(true, false);

    authorize_sse_kms_object_read(&HOST, Some(&principal), &sse_kms_object_metadata())
        .await
        .expect_err("an unauthorized principal must not read the source key");

    let tags = audit.audit_tags();
    assert_eq!(audit_tag(&tags, "sseType").as_deref(), Some("SSE-KMS"));
    assert_eq!(audit_tag(&tags, "kmsKeyId").as_deref(), Some("finance-key"));
    assert_eq!(audit_tag(&tags, "kmsOutcome").as_deref(), Some("failure"));
    assert_eq!(audit_tag(&tags, "kmsErrorClass").as_deref(), Some("access_denied"));
}

// ========================================================================
// Read-side response classification (single-decrypt GET path)
// ========================================================================

/// An AES256 object is read under the same key-id resolution as aws:kms
/// (authorization needs it), but the response must not advertise that
/// internal wrapping key.
#[tokio::test]
async fn classification_withholds_the_wrapping_key_for_sse_s3_reads() {
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption".to_string(), AMZ_ENCRYPTION_AES.to_string()),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "service-default".to_string()),
        (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), BASE64_STANDARD.encode_to_string([1u8; 16])),
        (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), BASE64_STANDARD.encode_to_string([2u8; 12])),
    ]);
    let headers = crate::core::read::classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("sse-s3 classification should succeed")
    .expect("managed metadata should classify");
    assert_eq!(headers.server_side_encryption.as_str(), AMZ_ENCRYPTION_AES);
    assert_eq!(headers.ssekms_key_id, None);
}

#[tokio::test]
async fn classification_reproduces_managed_read_headers_and_audit_without_a_kms_unwrap() {
    use rustfs_kms::types::{CreateKeyRequest, KeyUsage};
    let _guard = lock_sse_test_state().await;

    reset_sse_dek_provider();
    let manager = configure_test_global_local_kms().await;
    manager
        .get_encryption_service()
        .await
        .expect("encryption service should exist")
        .create_key(CreateKeyRequest {
            key_name: Some("classify-key".to_string()),
            key_usage: KeyUsage::EncryptDecrypt,
            description: None,
            policy: None,
            tags: HashMap::new(),
            origin: None,
        })
        .await
        .expect("kms test key should be created");
    let provider = KmsSseDekProvider::new_with_service_manager(manager.clone())
        .await
        .expect("kms provider should initialize from the configured test manager");
    crate::kms::provider::set_sse_dek_provider_for_test(Arc::new(provider));

    let material = sse_encryption(
        &HOST,
        EncryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            server_side_encryption: Some(String::from(AMZ_ENCRYPTION_KMS)),
            ssekms_key_id: Some("classify-key".to_string()),
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            content_size: 128,
            principal: None,
        },
    )
    .await
    .expect("sse-kms encryption should succeed")
    .expect("managed sse-kms material");
    let metadata = encryption_material_to_metadata(&material).expect("kms metadata should serialize");

    // Header parity against the unwrap-based path, on the same metadata.
    let unwrapped = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("sse-kms decryption should succeed")
    .expect("managed sse-kms material");

    // Classification must not need a data-key unwrap at all: drop the
    // provider before classifying, so any KMS round trip would fail loudly.
    reset_sse_dek_provider();

    let (read_principal, read_audit) = audited_principal(false, true);
    let headers = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&read_principal),
        },
    )
    .await
    .expect("classification should succeed without a dek provider")
    .expect("managed metadata must classify as encrypted");

    assert_eq!(headers.server_side_encryption, unwrapped.server_side_encryption);
    assert_eq!(headers.ssekms_key_id, unwrapped.kms_key_id);
    assert_eq!(headers.sse_customer_algorithm, None);
    assert_eq!(headers.sse_customer_key_md5, None);

    // Audit parity: the same summary fields the unwrap-based read recorded.
    let read_tags = read_audit.audit_tags();
    assert_eq!(audit_tag(&read_tags, "sseType").as_deref(), Some("SSE-KMS"));
    assert_eq!(audit_tag(&read_tags, "kmsKeyId").as_deref(), Some("classify-key"));
    assert_eq!(audit_tag(&read_tags, "kmsOutcome").as_deref(), Some("success"));
    assert_eq!(audit_tag(&read_tags, "kmsErrorClass"), None);
}

#[tokio::test]
async fn classification_denies_an_unauthorized_principal_with_the_same_audit_summary() {
    let (principal, audit) = audited_principal(true, false);

    let error = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &sse_kms_object_metadata(),
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: Some(&principal),
        },
    )
    .await
    .expect_err("an unauthorized principal must not classify against the key");
    assert_eq!(error.code, S3ErrorCode::AccessDenied);

    let tags = audit.audit_tags();
    assert_eq!(audit_tag(&tags, "sseType").as_deref(), Some("SSE-KMS"));
    assert_eq!(audit_tag(&tags, "kmsKeyId").as_deref(), Some("finance-key"));
    assert_eq!(audit_tag(&tags, "kmsOutcome").as_deref(), Some("failure"));
    assert_eq!(audit_tag(&tags, "kmsErrorClass").as_deref(), Some("access_denied"));
}

#[test]
fn read_response_projection_uses_shared_managed_metadata_compatibility() {
    let context_json = r#"{"tenant":"s3-select"}"#;
    let context = BASE64_STANDARD.encode_to_string(context_json);
    let key_id = "arn:aws:kms:us-east-1:123456789012:key/select";
    let kms_metadata = HashMap::from([
        (
            rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
            rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
        ),
        (
            rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(),
            key_id.to_string(),
        ),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), key_id.to_string()),
        (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), key_id.to_string()),
        (MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), context.clone()),
    ]);

    let headers = project_sse_read_response_headers(&kms_metadata, None, None)
        .expect("valid SSE-KMS metadata should project response headers");
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION)
            .expect("SSE-KMS algorithm"),
        rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS
    );
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION_AWS_KMS_KEY_ID)
            .expect("SSE-KMS key ID"),
        key_id
    );
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION_CONTEXT)
            .expect("SSE-KMS context"),
        context.as_str()
    );

    let s3_metadata = HashMap::from([
        (
            rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
            rustfs_utils::http::headers::AMZ_ENCRYPTION_AES.to_string(),
        ),
        (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "default".to_string()),
        (MINIO_INTERNAL_ENCRYPTION_KMS_KEY_ID_HEADER.to_string(), "default".to_string()),
        (
            rustfs_utils::http::object_encryption_keys::INTERNAL_ENCRYPTION_CONTEXT_HEADER.to_string(),
            r#"{"bucket":"select"}"#.to_string(),
        ),
        ("x-amz-meta-private".to_string(), "private".to_string()),
    ]);
    let headers = project_sse_read_response_headers(&s3_metadata, None, None)
        .expect("valid SSE-S3 metadata should project response headers");
    assert_eq!(headers.len(), 1);
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION)
            .expect("SSE-S3 algorithm"),
        rustfs_utils::http::headers::AMZ_ENCRYPTION_AES
    );

    let plaintext = project_sse_read_response_headers(&HashMap::new(), None, None)
        .expect("plaintext metadata should not produce SSE headers");
    assert!(plaintext.is_empty());
}

#[test]
fn read_response_projection_returns_only_validated_ssec_values() {
    let key_md5 = "customer-key-md5";
    let metadata = HashMap::from([
        (
            rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.to_string(),
            rustfs_utils::http::headers::AMZ_ENCRYPTION_AES.to_string(),
        ),
        (
            rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5.to_string(),
            key_md5.to_string(),
        ),
    ]);
    let algorithm = String::from(rustfs_utils::http::headers::AMZ_ENCRYPTION_AES);
    let key_md5 = String::from(key_md5);

    let headers = project_sse_read_response_headers(&metadata, Some(&algorithm), Some(&key_md5))
        .expect("validated SSE-C values should project response headers");
    assert_eq!(headers.len(), 2);
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM)
            .expect("SSE-C algorithm"),
        rustfs_utils::http::headers::AMZ_ENCRYPTION_AES
    );
    assert_eq!(
        headers
            .get(crate::core::s3::headers::X_AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5)
            .expect("SSE-C key MD5"),
        key_md5.as_str()
    );
    assert!(
        headers
            .get(rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY)
            .is_none()
    );

    let missing_algorithm = project_sse_read_response_headers(&metadata, None, Some(&key_md5))
        .expect_err("missing SSE-C algorithm must fail before streaming");
    assert_eq!(missing_algorithm.code, S3ErrorCode::InvalidRequest);
}

#[test]
fn read_response_projection_rejects_unvalidated_or_sensitive_metadata() {
    let invalid_context = "not-base64";
    let persisted_context = "must-not-leak-context";
    let persisted_key = "must-not-leak-key";
    let corrupt_metadata = [
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_ascii_lowercase(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_AES.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_ascii_uppercase(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
            ),
        ]),
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_AES.to_string(),
            ),
        ]),
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(),
                "key-1".to_string(),
            ),
            (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "key-2".to_string()),
        ]),
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(),
                "key-1".to_string(),
            ),
            (MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), invalid_context.to_string()),
        ]),
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(),
                "key-1".to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT.to_string(),
                persisted_context.to_string(),
            ),
        ]),
        HashMap::from([
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_ALGORITHM.to_string(),
                rustfs_utils::http::headers::AMZ_ENCRYPTION_AES.to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY_MD5.to_string(),
                "customer-key-md5".to_string(),
            ),
            (
                rustfs_utils::http::headers::AMZ_SERVER_SIDE_ENCRYPTION_CUSTOMER_KEY.to_ascii_uppercase(),
                persisted_key.to_string(),
            ),
        ]),
    ];

    for metadata in corrupt_metadata {
        let error =
            project_sse_read_response_headers(&metadata, None, None).expect_err("corrupt encryption metadata must fail closed");
        assert_eq!(error.code, S3ErrorCode::InternalError);
        for sensitive in [invalid_context, persisted_context, persisted_key] {
            assert!(!error.to_string().contains(sensitive));
        }
    }

    let marker_only_kms = HashMap::from([(MINIO_INTERNAL_ENCRYPTION_KMS_SEALED_KEY_HEADER.to_string(), "sealed".to_string())]);
    let marker_error = project_sse_read_response_headers(&marker_only_kms, None, None)
        .expect_err("marker-only KMS metadata must not bypass the existing authorization classifier");
    assert_eq!(marker_error.code, S3ErrorCode::InternalError);
}

#[tokio::test]
async fn classification_matches_ssec_validation_and_headers() {
    let key = [0x42u8; 32];
    let key_b64 = BASE64_STANDARD.encode_to_string(key);
    let key_md5 = md5_base64(key);
    let metadata = HashMap::from([
        ("x-amz-server-side-encryption-customer-algorithm".to_string(), "AES256".to_string()),
        ("x-amz-server-side-encryption-customer-key-md5".to_string(), key_md5.clone()),
    ]);

    let headers = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: Some(&key_b64.clone()),
            sse_customer_key_md5: Some(&key_md5.clone()),
            principal: None,
        },
    )
    .await
    .expect("a matching customer key should classify")
    .expect("SSE-C metadata must classify as encrypted");
    assert_eq!(headers.server_side_encryption.as_str(), AMZ_ENCRYPTION_AES);
    assert_eq!(headers.sse_customer_algorithm.as_deref(), Some("AES256"));
    assert_eq!(headers.sse_customer_key_md5.as_deref(), Some(key_md5.as_str()));
    assert_eq!(headers.ssekms_key_id, None);

    // Same error and precedence as `sse_decryption` when the key is absent…
    let missing = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect_err("an SSE-C object must not classify without the customer key");
    assert_eq!(missing.code, S3ErrorCode::InvalidRequest);

    // …and when the provided key does not match the stored digest.
    let other_key = [0x43u8; 32];
    let mismatch = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: Some(&BASE64_STANDARD.encode_to_string(other_key)),
            sse_customer_key_md5: Some(&md5_base64(other_key)),
            principal: None,
        },
    )
    .await
    .expect_err("a mismatched customer key must not classify");
    let parity = sse_decryption(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &metadata,
            sse_customer_key: Some(&BASE64_STANDARD.encode_to_string(other_key)),
            sse_customer_key_md5: Some(&md5_base64(other_key)),
            principal: None,
        },
    )
    .await
    .expect_err("the unwrap-based path rejects the same mismatch");
    assert_eq!(mismatch.code, parity.code);
}

#[tokio::test]
async fn classification_reports_plaintext_objects_as_unencrypted() {
    let result = classify_sse_read_response(
        &HOST,
        DecryptionRequest {
            bucket: "finance",
            key: "ledger.csv",
            metadata: &HashMap::new(),
            sse_customer_key: None,
            sse_customer_key_md5: None,
            principal: None,
        },
    )
    .await
    .expect("plaintext metadata should classify cleanly");
    assert!(result.is_none(), "objects without SSE metadata carry no SSE response headers");
}

#[test]
fn kms_backend_failures_keep_the_kms_audit_error_class() {
    assert_eq!(
        crate::core::audit::kms_data_plane_error_class(&kms_operation_error(rustfs_kms::KmsError::key_not_found("finance-key"))),
        "key_not_found"
    );
    assert_eq!(
        crate::core::audit::kms_data_plane_error_class(&kms_operation_error(rustfs_kms::KmsError::access_denied("nope"))),
        "access_denied"
    );
    // Failures raised by the SSE layer itself never saw a KmsError; they are
    // classified by the boundary they surfaced through.
    assert_eq!(
        crate::core::audit::kms_data_plane_error_class(&SseError {
            code: S3ErrorCode::AccessDenied,
            message: "Access Denied".to_string(),
            source: None,
        }),
        "access_denied"
    );
    assert_eq!(
        crate::core::audit::kms_data_plane_error_class(&SseError::internal("no KMS key available")),
        "sse_internal"
    );
}

#[test]
fn a_request_audit_slot_is_reachable_only_while_its_scope_lives() {
    let scope = crate::core::audit::KmsRequestAuditScope::register("request-under-audit");
    let slot = crate::core::audit::kms_request_audit("request-under-audit").expect("a registered request must resolve its slot");
    slot.record(SSEType::SseKms, Some("finance-key"), None, None);
    assert_eq!(audit_tag(&scope.audit_tags(), "kmsKeyId").as_deref(), Some("finance-key"));

    drop(scope);
    assert!(
        crate::core::audit::kms_request_audit("request-under-audit").is_none(),
        "the slot must not outlive the audit entry it belongs to"
    );
}

#[test]
fn a_request_that_did_no_kms_work_reports_no_tags() {
    let scope = crate::core::audit::KmsRequestAuditScope::register("quiet-request");
    assert!(scope.audit_tags().is_empty());
}

/// The canonical seven-field envelope, with `master_key_version` grafted on
/// when a wrapping version is wanted.
fn audit_test_envelope(master_key_version: Option<u32>) -> Vec<u8> {
    let mut envelope = serde_json::json!({
        "key_id": "test-key-id",
        "master_key_id": "master-key-id",
        "key_spec": "AES_256",
        "encrypted_key": [1, 2, 3, 4],
        "nonce": [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        "encryption_context": {},
        "created_at": "2024-01-01T00:00:00+00:00"
    });
    if let Some(version) = master_key_version {
        envelope
            .as_object_mut()
            .expect("envelope is an object")
            .insert("master_key_version".to_string(), serde_json::json!(version));
    }
    serde_json::to_vec(&envelope).expect("encode envelope")
}

#[test]
fn envelope_master_key_version_reads_only_true_envelopes() {
    // A versioned envelope reports the wrapping version.
    assert_eq!(crate::core::audit::envelope_master_key_version(&audit_test_envelope(Some(3))), Some(3));
    // A pre-versioning envelope has no version to report.
    assert_eq!(crate::core::audit::envelope_master_key_version(&audit_test_envelope(None)), None);
    // Opaque backend ciphertext (Transit, AWS) is not an envelope.
    assert_eq!(crate::core::audit::envelope_master_key_version(b"vault:v2:abcdefgh"), None);
    // JSON that is not the envelope shape must not be probed for a version.
    assert_eq!(crate::core::audit::envelope_master_key_version(br#"{"master_key_version": 9}"#), None);
}

#[test]
fn stored_envelope_master_key_version_reads_both_metadata_families() {
    let envelope = BASE64_STANDARD.encode_to_string(audit_test_envelope(Some(2)));

    // RustFS-branded stored key.
    let metadata = HashMap::from([(INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), envelope.clone())]);
    assert_eq!(crate::core::audit::stored_envelope_master_key_version(&metadata), Some(2));

    // MinIO-branded stored key reaches the same answer through
    // normalize_managed_metadata — the dual internal metadata key rule.
    let metadata = HashMap::from([(
        rustfs_utils::http::object_encryption_keys::MINIO_INTERNAL_ENCRYPTION_KMS_DATA_KEY_HEADER.to_string(),
        envelope,
    )]);
    assert_eq!(crate::core::audit::stored_envelope_master_key_version(&metadata), Some(2));

    assert_eq!(crate::core::audit::stored_envelope_master_key_version(&HashMap::new()), None);
}

#[test]
fn recorded_key_versions_reach_the_audit_tags() {
    let scope = crate::core::audit::KmsRequestAuditScope::register("versioned-request");
    let slot = crate::core::audit::kms_request_audit("versioned-request").expect("a registered request must resolve its slot");
    slot.record(SSEType::SseKms, Some("finance-key"), Some(3), None);
    let tags = scope.audit_tags();
    assert_eq!(audit_tag(&tags, "kmsKeyVersion").as_deref(), Some("3"));
    assert_eq!(audit_tag(&tags, "kmsOutcome").as_deref(), Some("success"));
}
