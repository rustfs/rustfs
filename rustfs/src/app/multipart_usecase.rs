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

//! Multipart application use-case contracts.

use super::storage_api::multipart_usecase::ECStore;
use super::storage_api::multipart_usecase::access::{
    TableDataPlaneListAccess, apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard,
    has_bypass_governance_header, replication_request_authorized, retained_table_data_plane_publication_guards,
};
use super::storage_api::multipart_usecase::bucket::quota::checker::QuotaChecker;
use super::storage_api::multipart_usecase::bucket::{
    lifecycle::{bucket_lifecycle_audit::LcEventSrc, bucket_lifecycle_ops::enqueue_transition_immediate},
    metadata_sys,
    quota::QuotaOperation,
    replication::{must_replicate_object, schedule_object_replication},
    versioning_sys::BucketVersioningSys,
};
use super::storage_api::multipart_usecase::compression::{is_disk_compressible, is_multipart_disk_compression_enabled};
#[cfg(test)]
use super::storage_api::multipart_usecase::contract::http::HTTPPreconditions;
use super::storage_api::multipart_usecase::contract::multipart::{
    CompletePart, MAX_MULTIPART_PART_NUMBER, MultipartOperations as _, MultipartUploadResult,
};
#[cfg(test)]
use super::storage_api::multipart_usecase::contract::object::ObjectIO as _;
use super::storage_api::multipart_usecase::contract::object::ObjectOperations as _;
use super::storage_api::multipart_usecase::contract::range::HTTPRangeSpec;
use super::storage_api::multipart_usecase::data_usage::{
    quota_object_size, record_bucket_object_version_write_memory, record_bucket_object_write_memory,
    record_bucket_object_write_unknown_previous_memory,
};
use super::storage_api::multipart_usecase::error::{
    StorageError, is_err_invalid_upload_id, is_err_object_not_found, is_err_version_not_found,
};
use super::storage_api::multipart_usecase::helper::{OperationHelper, run_owned_mutation};
#[cfg(test)]
use super::storage_api::multipart_usecase::io::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader, wrap_reader};
use super::storage_api::multipart_usecase::io::{HashReader, WriteEncryption, WritePlan, compression_metadata_value};
use super::storage_api::multipart_usecase::object_utils::{to_s3s_etag, to_s3s_xml_etag};
use super::storage_api::multipart_usecase::options::{
    copy_src_opts, extract_metadata_from_mime, get_complete_multipart_upload_opts_with_replication_authorization,
    get_content_sha256_with_query, get_opts, has_replication_retention_update, namespace_reserved_user_metadata,
    parse_copy_source_range, put_opts_with_replication_authorization, validate_archive_content_encoding,
};
use super::storage_api::multipart_usecase::request_context::spawn_traced_join;
use super::storage_api::multipart_usecase::s3_api::multipart::{
    ListMultipartUploadsParams, build_list_multipart_uploads_output, build_list_parts_output,
    parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
};
use super::storage_api::multipart_usecase::set_disk::is_valid_storage_class;
use super::storage_api::multipart_usecase::sse::{
    DecryptionRequest, EncryptionKeyKind, EncryptionRequest, PrepareEncryptionRequest, SseKmsPrincipal,
    apply_bucket_default_lock_retention, authorize_sse_kms_multipart_write, authorize_sse_kms_object_read,
    build_ssec_read_headers, encryption_material_to_metadata, extract_server_side_encryption_from_headers,
    extract_ssec_params_from_headers, extract_ssekms_context_from_headers, get_buffer_size_opt_in,
    load_bucket_object_lock_config_state, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    multipart_completion_authorization, sse_decryption, sse_prepare_encryption,
};
use super::storage_api::multipart_usecase::{
    StorageObjectInfo as ObjectInfo, StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader,
    validate_new_object_key,
};
use super::trailer_adapter::trailer_source;
use crate::app::object::{
    ConcurrencyManager, ForegroundWriteAdmission, get_concurrency_manager, put_object_body_read_timeout,
    reject_non_wildcard_write_if_none_match, reject_oversize_single_upload, reject_veeam_sosapi_mutation,
};
use crate::app::object_data_cache::{
    ObjectDataCacheAdapter, invalidate_object_data_cache_after_complete_multipart_success,
    invalidate_object_data_cache_before_mutation,
};
use crate::app::object_usecase::{
    apply_quota_admission, build_put_like_object_lock_metadata, map_quota_check_outcome, s3s_body_error_to_io,
    validate_existing_object_lock_for_write,
};
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_object_data_cache_for_context, current_object_store_handle_for_context,
};
use crate::app::table_list_isolation;
use crate::auth::{
    VerifiedPresignedRequest, VerifiedSigV4Request, parse_presigned_multipart_max_total_object_size,
    reject_presigned_multipart_max_total_object_size_for_other_operation,
    reject_presigned_put_max_content_length_for_other_operation,
};
use crate::capacity::record_capacity_write;
use crate::error::ApiError;
use crate::table_catalog;
#[cfg(test)]
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Uri};
use metrics::counter;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
#[cfg(test)]
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, insert_header};
use rustfs_utils::http::{
    SUFFIX_MAX_TOTAL_OBJECT_SIZE, SUFFIX_PLAINTEXT_CHECKSUM, SUFFIX_REPLICATION_GENERATION,
    SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    SUFFIX_SOURCE_REPLICATION_REQUEST, contains_key_str, get_consistent_str, get_header, get_source_scheme,
    headers::{AMZ_CHECKSUM_TYPE, AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS},
    insert_str,
};
use s3s::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};
use urlencoding::encode;
use uuid::Uuid;

mod abort;
mod complete;
mod create;
mod list;
#[cfg(test)]
mod test_support;
mod upload_part;
mod upload_part_copy;

#[cfg(test)]
fn merge_part_encryption_metadata(
    metadata: &HashMap<String, String>,
    part_metadata: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut merged = metadata.clone();
    merged.extend(part_metadata.clone());
    merged
}

#[cfg(test)]
fn multipart_plaintext_size(parts: &[rustfs_filemeta::ObjectPartInfo], fallback: i64) -> i64 {
    let total: i64 = parts
        .iter()
        .map(|part| {
            if part.actual_size > 0 {
                part.actual_size
            } else {
                part.size as i64
            }
        })
        .sum();

    if total > 0 { total } else { fallback }
}

#[cfg(test)]
fn multipart_part_numbers(parts: &[rustfs_filemeta::ObjectPartInfo]) -> Vec<usize> {
    parts.iter().map(|part| part.number).collect()
}

fn multipart_max_total_object_size(metadata: &HashMap<String, String>) -> S3Result<Option<u64>> {
    if !contains_key_str(metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE) {
        return Ok(None);
    }

    let value = get_consistent_str(metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE).ok_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "multipart size capability metadata is missing or inconsistent".to_string(),
        )
    })?;
    value.parse::<u64>().map(Some).map_err(|_| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, "multipart size capability metadata is invalid".to_string())
    })
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

fn internal_object_info_lookup_opts(mut opts: ObjectOptions) -> ObjectOptions {
    opts.http_preconditions = None;
    opts
}

#[derive(Clone, Default)]
pub struct DefaultMultipartUsecase {
    context: Option<Arc<AppContext>>,
    #[cfg(test)]
    concurrency_manager: Option<Arc<ConcurrencyManager>>,
}

impl DefaultMultipartUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self {
            context: None,
            concurrency_manager: None,
        }
    }

    pub fn from_global() -> Self {
        Self {
            context: current_app_context(),
            #[cfg(test)]
            concurrency_manager: None,
        }
    }

    /// Build the use-case bound to an explicit application context
    /// (backlog#1052 S6): the per-server request path passes its own context
    /// so the use-case resolves that server's store; `None` falls back to the
    /// ambient default.
    pub fn with_context(context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>) -> Self {
        Self {
            context,
            #[cfg(test)]
            concurrency_manager: None,
        }
    }

    #[cfg(test)]
    fn with_context_and_concurrency_manager(
        context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>,
        concurrency_manager: Arc<ConcurrencyManager>,
    ) -> Self {
        Self {
            context,
            concurrency_manager: Some(concurrency_manager),
        }
    }

    fn bucket_metadata_sys(&self) -> Option<Arc<RwLock<metadata_sys::BucketMetadataSys>>> {
        self.context.as_ref().and_then(|context| context.bucket_metadata().handle())
    }

    fn object_store(&self) -> Option<Arc<ECStore>> {
        current_object_store_handle_for_context(self.context.as_deref())
    }

    fn object_data_cache(&self) -> Arc<ObjectDataCacheAdapter> {
        current_object_data_cache_for_context(self.context.as_deref())
    }

    fn concurrency_manager(&self) -> &ConcurrencyManager {
        #[cfg(test)]
        if let Some(concurrency_manager) = self.concurrency_manager.as_deref() {
            return concurrency_manager;
        }

        get_concurrency_manager()
    }
}

/// Maps a storage error from an operation that addresses an existing multipart
/// upload (UploadPart, UploadPartCopy, ListParts, CompleteMultipartUpload and
/// AbortMultipartUpload) to an S3 error.
///
/// A malformed upload ID cannot name an existing upload, so it is reported as
/// `NoSuchUpload` (404), which the AWS S3 error reference describes as "The
/// upload ID might be invalid, or the multipart upload might have been aborted
/// or completed".
fn multipart_upload_api_error(err: StorageError) -> S3Error {
    if matches!(err, StorageError::MalformedUploadID(_)) {
        return S3Error::new(S3ErrorCode::NoSuchUpload);
    }
    ApiError::from(err).into()
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use crate::app::storage_api::s3::dto::{CompleteMultipartUploadInput, CreateMultipartUploadInput, ServerSideEncryption};

    #[test]
    fn internal_object_info_lookup_opts_drops_http_preconditions() {
        let opts = ObjectOptions {
            version_id: Some(Uuid::new_v4().to_string()),
            no_lock: true,
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                if_match: Some("\"etag\"".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let lookup_opts = internal_object_info_lookup_opts(opts);

        assert!(lookup_opts.http_preconditions.is_none());
        assert!(lookup_opts.no_lock);
        assert!(lookup_opts.version_id.is_some());
    }

    #[test]
    fn merge_part_encryption_metadata_keeps_source_metadata_unchanged() {
        let multipart_metadata = HashMap::from([
            ("x-rustfs-encryption-iv".to_string(), "base-nonce".to_string()),
            ("x-rustfs-encryption-key".to_string(), "base-key".to_string()),
        ]);
        let part_metadata = HashMap::from([
            ("x-rustfs-encryption-iv".to_string(), "part-nonce".to_string()),
            ("x-rustfs-encryption-original-size".to_string(), "1024".to_string()),
        ]);

        let merged = merge_part_encryption_metadata(&multipart_metadata, &part_metadata);

        assert_eq!(multipart_metadata.get("x-rustfs-encryption-iv").map(String::as_str), Some("base-nonce"));
        assert_eq!(merged.get("x-rustfs-encryption-iv").map(String::as_str), Some("part-nonce"));
        assert_eq!(merged.get("x-rustfs-encryption-key").map(String::as_str), Some("base-key"));
    }

    #[tokio::test]
    async fn managed_multipart_roundtrip_preserves_session_nonce_between_parts() {
        let local_sse_master_key = base64_simd::STANDARD.encode_to_string([0x24u8; 32]);
        async_with_vars(
            [
                ("__RUSTFS_SSE_SIMPLE_CMK", None::<String>),
                ("RUSTFS_SSE_S3_MASTER_KEY", Some(local_sse_master_key)),
            ],
            async {
                let prepare_request = PrepareEncryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                    ssekms_key_id: None,
                    ssekms_context: None,
                    sse_customer_algorithm: None,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                };
                let session_material = sse_prepare_encryption(prepare_request)
                    .await
                    .expect("prepare multipart encryption")
                    .expect("managed multipart session material");
                let mut session_metadata =
                    encryption_material_to_metadata(&session_material).expect("multipart session metadata should be generated");
                mark_encrypted_multipart_metadata(&mut session_metadata);

                let part_one_plaintext = vec![0x31; rustfs_rio::DEFAULT_ENCRYPTION_BLOCK_SIZE + 23];
                let part_two_plaintext = vec![0x32; rustfs_rio::DEFAULT_ENCRYPTION_BLOCK_SIZE * 2 + 7];

                let part_one_material = sse_decryption(DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &session_metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                })
                .await
                .expect("decrypt session one")
                .expect("part one material");
                let mut encrypted_one = Vec::new();
                #[cfg(feature = "rio-v2")]
                let mut part_one_reader = match part_one_material.key_kind {
                    EncryptionKeyKind::Object => EncryptReader::new_multipart_with_object_key(
                        Cursor::new(part_one_plaintext.clone()),
                        part_one_material.key_bytes,
                        1,
                    ),
                    EncryptionKeyKind::Direct => EncryptReader::new_multipart(
                        Cursor::new(part_one_plaintext.clone()),
                        part_one_material.key_bytes,
                        part_one_material.base_nonce,
                        1,
                    ),
                };
                #[cfg(not(feature = "rio-v2"))]
                let mut part_one_reader = EncryptReader::new_multipart(
                    Cursor::new(part_one_plaintext.clone()),
                    part_one_material.key_bytes,
                    part_one_material.base_nonce,
                    1,
                );
                part_one_reader
                    .read_to_end(&mut encrypted_one)
                    .await
                    .expect("read encrypted part one");

                let part_two_material = sse_decryption(DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &session_metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                })
                .await
                .expect("decrypt session two")
                .expect("part two material");
                let mut encrypted_two = Vec::new();
                #[cfg(feature = "rio-v2")]
                let mut part_two_reader = match part_two_material.key_kind {
                    EncryptionKeyKind::Object => EncryptReader::new_multipart_with_object_key(
                        Cursor::new(part_two_plaintext.clone()),
                        part_two_material.key_bytes,
                        2,
                    ),
                    EncryptionKeyKind::Direct => EncryptReader::new_multipart(
                        Cursor::new(part_two_plaintext.clone()),
                        part_two_material.key_bytes,
                        part_two_material.base_nonce,
                        2,
                    ),
                };
                #[cfg(not(feature = "rio-v2"))]
                let mut part_two_reader = EncryptReader::new_multipart(
                    Cursor::new(part_two_plaintext.clone()),
                    part_two_material.key_bytes,
                    part_two_material.base_nonce,
                    2,
                );
                part_two_reader
                    .read_to_end(&mut encrypted_two)
                    .await
                    .expect("read encrypted part two");

                if session_material.key_kind == EncryptionKeyKind::Object {
                    assert!(session_metadata.contains_key("X-Minio-Internal-Encrypted-Multipart"));
                    assert!(session_metadata.contains_key("X-Minio-Internal-Server-Side-Encryption-S3-Sealed-Key"));
                } else {
                    assert!(session_metadata.contains_key("x-rustfs-encryption-iv"));
                }

                let parts = vec![
                    ObjectPartInfo {
                        number: 1,
                        size: encrypted_one.len(),
                        actual_size: part_one_plaintext.len() as i64,
                        ..Default::default()
                    },
                    ObjectPartInfo {
                        number: 2,
                        size: encrypted_two.len(),
                        actual_size: part_two_plaintext.len() as i64,
                        ..Default::default()
                    },
                ];

                let mut encrypted_stream = Vec::with_capacity(encrypted_one.len() + encrypted_two.len());
                encrypted_stream.extend_from_slice(&encrypted_one);
                encrypted_stream.extend_from_slice(&encrypted_two);

                let decryption_material = sse_decryption(DecryptionRequest {
                    bucket: "bucket",
                    key: "object",
                    metadata: &session_metadata,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                    principal: None,
                })
                .await
                .expect("decrypt multipart")
                .expect("managed decryption material");

                let plaintext_size = multipart_plaintext_size(&parts, -1);
                #[cfg(feature = "rio-v2")]
                let decrypted_stream = match decryption_material.key_kind {
                    EncryptionKeyKind::Object => boxed_reader(DecryptReader::new_multipart_with_object_key(
                        wrap_reader(Cursor::new(encrypted_stream)),
                        decryption_material.key_bytes,
                        multipart_part_numbers(&parts),
                    )),
                    EncryptionKeyKind::Direct => boxed_reader(DecryptReader::new_multipart(
                        wrap_reader(Cursor::new(encrypted_stream)),
                        decryption_material.key_bytes,
                        decryption_material.base_nonce,
                        multipart_part_numbers(&parts),
                    )),
                };
                #[cfg(not(feature = "rio-v2"))]
                let decrypted_stream = boxed_reader(DecryptReader::new_multipart(
                    wrap_reader(Cursor::new(encrypted_stream)),
                    decryption_material.key_bytes,
                    decryption_material.base_nonce,
                    multipart_part_numbers(&parts),
                ));
                let mut decrypted_reader = HardLimitReader::new(decrypted_stream, plaintext_size);

                let mut decrypted = Vec::new();
                decrypted_reader
                    .read_to_end(&mut decrypted)
                    .await
                    .expect("read decrypted multipart data");

                let mut expected = part_one_plaintext;
                expected.extend_from_slice(&part_two_plaintext);

                assert_eq!(plaintext_size, expected.len() as i64);
                assert_eq!(decrypted, expected);
            },
        )
        .await;
    }

    #[tokio::test]
    async fn multipart_upload_rejects_reserved_directory_markers() {
        for key in [
            "__XLDIR__",
            "prefix__XLDIR__/object",
            "object__XLDIR__",
            "parent/xl.meta",
            "parent/uuid/xl.meta.bkp",
            "parent/uuid/delete-prepared",
            "parent/uuid/delete-quorum-committed",
        ] {
            let create = CreateMultipartUploadInput::builder()
                .bucket("bucket".to_owned())
                .key(key.to_owned())
                .build()
                .expect("valid create request DTO");
            let error = make_usecase()
                .execute_create_multipart_upload(build_request(create, Method::POST))
                .await
                .expect_err("reserved key must be rejected before accessing storage");
            assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);

            let complete = CompleteMultipartUploadInput::builder()
                .bucket("bucket".to_owned())
                .key(key.to_owned())
                .upload_id("upload".to_owned())
                .build()
                .expect("valid complete request DTO");
            let error = Box::pin(make_usecase().execute_complete_multipart_upload(build_request(complete, Method::POST)))
                .await
                .expect_err("uploads created before the fix must not publish reserved keys");
            assert_eq!(error.code(), &S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn multipart_max_total_object_size_reads_compatible_internal_metadata() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE, "104857600".to_string());
        assert_eq!(multipart_max_total_object_size(&metadata).unwrap(), Some(104_857_600));

        metadata.insert("x-minio-internal-max-total-object-size".to_string(), "1".to_string());
        assert!(multipart_max_total_object_size(&metadata).is_err());
    }

    #[test]
    fn multipart_upload_api_error_reports_malformed_upload_id_as_no_such_upload() {
        let malformed = multipart_upload_api_error(StorageError::MalformedUploadID("bucket/object-not-an-id".to_string()));
        assert_eq!(malformed.code(), &S3ErrorCode::NoSuchUpload);

        let unknown = multipart_upload_api_error(StorageError::InvalidUploadID(
            "bucket".to_string(),
            "object".to_string(),
            "dXBsb2FkLWlk".to_string(),
        ));
        assert_eq!(unknown.code(), &S3ErrorCode::NoSuchUpload);

        let other = multipart_upload_api_error(StorageError::ObjectNotFound("bucket".to_string(), "object".to_string()));
        assert_eq!(other.code(), &S3ErrorCode::NoSuchKey);
    }

    // Lives in the app layer: the handlers under test are app use cases; the SSE
    // helpers it borrows are infra test support (allowed downward dependency).
    #[test]
    #[serial_test::serial]
    fn multipart_handlers_reject_another_callers_denied_kms_upload() {
        crate::app::gating_test_env::run_large_stack_test("mpu-kms-current-caller", || async {
            use crate::app::storage_api::s3::S3ErrorCode;
            use crate::app::storage_api::s3::dto::{
                CompleteMultipartUploadInput, CompletedMultipartUpload, CompletedPart, CopySource, StreamingBlob,
                UploadPartCopyInput, UploadPartInput,
            };
            use crate::app::storage_api::test::ReqInfo;
            use crate::app::storage_api::test::sse::{
                RecordingKmsKeyAuthorizer, TestAuthorizationHooks, sse_kms_object_metadata,
            };
            use crate::storage::storage_api::contract::multipart::MultipartOperations as _;
            use rustfs_policy::policy::action::KmsAction;
            use std::sync::Arc;
            let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("mpu-kms-current-caller", 16_384).await;
            let usecase = DefaultMultipartUsecase::from_global();
            let key = "kms-owned-upload";
            let metadata = sse_kms_object_metadata();
            let upload = store
                .new_multipart_upload(
                    &bucket,
                    key,
                    &crate::storage::ObjectOptions {
                        user_defined: metadata,
                        ..Default::default()
                    },
                )
                .await
                .expect("seed authorized upload envelope");
            let authorizer = RecordingKmsKeyAuthorizer::new(false);
            fn denied_request<T>(
                input: T,
                authorizer: Arc<RecordingKmsKeyAuthorizer>,
            ) -> crate::app::storage_api::s3::S3Request<T> {
                let mut req = crate::app::storage_api::s3::S3Request {
                    input,
                    method: http::Method::PUT,
                    uri: http::Uri::from_static("/"),
                    headers: http::HeaderMap::new(),
                    extensions: http::Extensions::new(),
                    credentials: None,
                    region: None,
                    service: None,
                    trailing_headers: None,
                };
                req.extensions.insert(ReqInfo {
                    cred: Some(rustfs_credentials::Credentials {
                        access_key: "denied-writer".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                req.extensions.insert(TestAuthorizationHooks {
                    enforced: true,
                    authorizer,
                });
                req
            }
            let mut part = UploadPartInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload.upload_id.clone())
                .part_number(1)
                .build()
                .expect("part");
            part.body = Some(StreamingBlob::from(bytes::Bytes::from_static(b"abc")));
            part.content_length = Some(3);
            // Keep typed request construction separate: each real handler must
            // consult the destination envelope before encryption or commit.
            let error = usecase
                .execute_upload_part(denied_request(part, authorizer.clone()))
                .await
                .expect_err("denied part");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            let copy = UploadPartCopyInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload.upload_id.clone())
                .part_number(1)
                .copy_source(CopySource::Bucket {
                    bucket: bucket.clone().into(),
                    key: "source".into(),
                    version_id: None,
                })
                .build()
                .expect("copy");
            let copy_req = denied_request(copy, authorizer.clone());
            let error = usecase
                .execute_upload_part_copy(copy_req)
                .await
                .expect_err("denied destination copy");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            let complete = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload.upload_id.clone())
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(crate::app::storage_api::s3::dto::ETag::Strong("etag".to_string())),
                        ..Default::default()
                    }]),
                }))
                .build()
                .expect("complete");
            let complete_req = denied_request(complete, authorizer.clone());
            let error = usecase
                .execute_complete_multipart_upload(complete_req)
                .await
                .expect_err("denied completion");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            assert_eq!(authorizer.calls(), vec![(KmsAction::GenerateDataKeyAction, "finance-key".to_string()); 3]);
            assert!(
                store
                    .get_multipart_info(&bucket, key, &upload.upload_id, &crate::storage::ObjectOptions::default())
                    .await
                    .is_ok(),
                "denials must retain the original upload"
            );
            let mut reader = crate::storage::PutObjReader::from_vec(b"abc".to_vec());
            let uploaded_part = store
                .put_object_part(&bucket, key, &upload.upload_id, 1, &mut reader, &crate::storage::ObjectOptions::default())
                .await
                .expect("trusted owner stages part");
            store
                .clone()
                .complete_multipart_upload(
                    &bucket,
                    key,
                    &upload.upload_id,
                    vec![crate::storage::storage_api::contract::multipart::CompletePart {
                        part_num: 1,
                        etag: uploaded_part.etag,
                        ..Default::default()
                    }],
                    &crate::storage::ObjectOptions::default(),
                )
                .await
                .expect("trusted owner completes upload");
            let replay = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload.upload_id)
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(crate::app::storage_api::s3::dto::ETag::Strong("guessed-etag".to_string())),
                        ..Default::default()
                    }]),
                }))
                .build()
                .expect("replay");
            let error = usecase
                .execute_complete_multipart_upload(denied_request(replay, authorizer.clone()))
                .await
                .expect_err("replay must authorize before comparing guessed part metadata");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            assert_eq!(authorizer.calls().len(), 4);
        });
    }
}
