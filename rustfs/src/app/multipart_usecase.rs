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
    apply_bucket_generation_guard, apply_copy_source_bucket_generation_guard, has_bypass_governance_header,
    replication_request_authorized, retained_table_data_plane_publication_guards,
};
use super::storage_api::multipart_usecase::bucket::quota::checker::QuotaChecker;
use super::storage_api::multipart_usecase::bucket::{
    etag::bucket_uses_xxh3_etag,
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
use super::storage_api::multipart_usecase::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use super::storage_api::multipart_usecase::helper::{OperationHelper, run_owned_mutation};
#[cfg(test)]
use super::storage_api::multipart_usecase::io::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader, wrap_reader};
use super::storage_api::multipart_usecase::io::{
    EtagAlgorithm, HashReader, WriteEncryption, WritePlan, compression_metadata_value,
};
use super::storage_api::multipart_usecase::options::{
    copy_src_opts, extract_metadata_from_mime, get_complete_multipart_upload_opts_with_replication_authorization,
    get_content_sha256_with_query, get_opts, has_replication_retention_update, namespace_reserved_user_metadata,
    parse_copy_source_range, put_opts_with_replication_authorization, validate_archive_content_encoding,
};
use super::storage_api::multipart_usecase::request_context::spawn_traced_join;
use super::storage_api::multipart_usecase::s3_api::multipart::{
    ListMultipartUploadsParams, parse_list_multipart_uploads_params, parse_list_parts_params, parse_upload_part_number,
};
use super::storage_api::multipart_usecase::set_disk::is_valid_storage_class;
use super::storage_api::multipart_usecase::sse::{
    DecryptionRequest, EncryptionKeyKind, EncryptionRequest, PrepareEncryptionRequest, SseKmsPrincipal, SseKmsPrincipalExt,
    apply_bucket_default_lock_retention, authorize_sse_kms_multipart_write, authorize_sse_kms_object_read,
    build_ssec_read_headers, encryption_material_to_metadata, extract_server_side_encryption_from_headers,
    extract_ssec_params_from_headers, extract_ssekms_context_from_headers, get_buffer_size_opt_in,
    load_bucket_object_lock_config_state, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    multipart_completion_authorization, requested_sse_kms_key_id, sse_decryption, sse_prepare_encryption,
};
use super::storage_api::multipart_usecase::{
    StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader, validate_new_object_key,
};
use crate::app::object::{
    ConcurrencyManager, ForegroundWriteAdmission, get_concurrency_manager, put_object_body_read_timeout,
    reject_non_wildcard_write_if_none_match, reject_oversize_single_upload, reject_veeam_sosapi_mutation,
};
use crate::app::object_data_cache::{
    ObjectDataCacheAdapter, invalidate_object_data_cache_after_complete_multipart_success,
    invalidate_object_data_cache_before_mutation,
};
use crate::app::object_usecase::{
    apply_quota_admission, map_quota_check_outcome, s3s_body_error_to_io, validate_existing_object_lock_for_write,
};
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_object_data_cache_for_context, current_object_store_handle_for_context,
};
use crate::app::table_list_isolation;
#[cfg(test)]
use crate::auth::VerifiedPresignedRequest;
use crate::auth::{
    parse_presigned_multipart_max_total_object_size, reject_presigned_multipart_max_total_object_size_for_other_operation,
    reject_presigned_put_max_content_length_for_other_operation,
};
use crate::capacity::record_capacity_write;
use crate::error::ApiError;
use crate::table_catalog;
#[cfg(test)]
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Uri};
use metrics::counter;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
#[cfg(test)]
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, insert_header};
use rustfs_utils::http::{
    SUFFIX_ETAG_ALGORITHM, SUFFIX_MAX_TOTAL_OBJECT_SIZE, SUFFIX_PLAINTEXT_CHECKSUM, SUFFIX_REPLICATION_GENERATION,
    SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    SUFFIX_SOURCE_REPLICATION_REQUEST, contains_key_str, get_consistent_str, get_header, get_source_scheme,
    headers::{AMZ_CHECKSUM_TYPE, AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS},
    insert_str,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{instrument, warn};
use urlencoding::encode;
use uuid::Uuid;

mod abort;
pub(crate) use abort::{AbortMultipartUploadTarget, reject_abort_capabilities};
mod complete;
mod create;
mod create_values;
pub(crate) use create_values::{CreateMultipartUploadFields, CreateMultipartUploadResult};
mod list;
pub(crate) use list::ListPartsInfo;
pub(crate) use list::{ListPartsFields, ListPartsTarget};
#[cfg(test)]
mod test_support;
mod upload_part;
mod upload_part_copy;
pub(crate) use upload_part_copy::{UploadPartCopyWriteInput, UploadPartCopyWriteOutput};
mod upload_values;
pub(crate) use upload_values::{UploadPartTarget, UploadPartWriteInput, UploadPartWriteOutput};

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

/// ETag value pinned by CreateMultipartUpload for a bucket with XXH3 ETags.
const ETAG_ALGORITHM_XXH3: &str = "xxh3";

/// The ETag algorithm pinned for this multipart upload at its creation;
/// uploads without the pin (MD5 buckets, older uploads) keep MD5.
fn multipart_etag_algorithm(metadata: &HashMap<String, String>) -> EtagAlgorithm {
    match get_consistent_str(metadata, SUFFIX_ETAG_ALGORITHM) {
        Some(value) if value.eq_ignore_ascii_case(ETAG_ALGORITHM_XXH3) => EtagAlgorithm::Xxh3,
        _ => EtagAlgorithm::Md5,
    }
}

fn multipart_max_total_object_size(metadata: &HashMap<String, String>) -> S3Result<Option<u64>> {
    if !contains_key_str(metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE) {
        return Ok(None);
    }

    let value = get_consistent_str(metadata, SUFFIX_MAX_TOTAL_OBJECT_SIZE).ok_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            "multipart size capability metadata is missing or inconsistent",
        )
    })?;
    value
        .parse::<u64>()
        .map(Some)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidRequest, "multipart size capability metadata is invalid"))
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
        return S3Error::with_default_message(S3ErrorCode::NoSuchUpload);
    }
    ApiError::from(err).into()
}

#[cfg(test)]
fn completion_part(number: i32, stored: Option<&str>) -> rustfs_gateway_types::dto::CompletedPart {
    rustfs_gateway_types::dto::CompletedPart {
        part_number: number,
        e_tag: stored.map(crate::app::dto_values::stored_etag),
        ..Default::default()
    }
}

#[cfg(test)]
fn completion_input(
    bucket: &str,
    key: &str,
    upload_id: &str,
    parts: Vec<rustfs_gateway_types::dto::CompletedPart>,
) -> rustfs_gateway_types::dto::CompleteMultipartUploadInput {
    use rustfs_gateway_types::{BucketName, ObjectKey, UploadIdClaim, dto};
    dto::CompleteMultipartUploadInput {
        bucket: BucketName::new(bucket).expect("test bucket"),
        key: ObjectKey::new(key).expect("test key"),
        upload_id: UploadIdClaim::from_wire(upload_id),
        multipart_upload: dto::CompletedMultipartUpload { parts },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use rustfs_gateway_types::dto::ServerSideEncryption;

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
                    server_side_encryption: Some(ServerSideEncryption::AES256.as_str().to_owned()),
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
            // Same facts `from_legacy` carried. Metadata, tagging, storage class, SSE, and object lock stay absent.
            let create = super::create_values::CreateMultipartUploadFields {
                bucket: "bucket".to_owned(),
                key: key.to_owned(),
                metadata: None,
                tagging: None,
                storage_class: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                ssekms_key_id: None,
                object_lock_legal_hold_status: None,
                object_lock_mode: None,
                object_lock_retain_until_date: None,
            };
            let error = match make_usecase()
                .execute_create_multipart_upload(owned_envelope(Method::POST), create, None)
                .await
            {
                Err(error) => error,
                Ok(_) => panic!("reserved key must be rejected before accessing storage"),
            };
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);

            let complete = completion_input("bucket", key, "upload", vec![]);
            let error = Box::pin(make_usecase().execute_complete_multipart_upload(owned_envelope(Method::POST), complete))
                .await
                .expect_err("uploads created before the fix must not publish reserved keys");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::InvalidArgument);
        }
    }

    #[test]
    fn multipart_etag_algorithm_reads_the_pinned_value() {
        let mut metadata = HashMap::new();
        assert_eq!(multipart_etag_algorithm(&metadata), EtagAlgorithm::Md5);

        insert_str(&mut metadata, SUFFIX_ETAG_ALGORITHM, ETAG_ALGORITHM_XXH3.to_string());
        assert_eq!(multipart_etag_algorithm(&metadata), EtagAlgorithm::Xxh3);
        assert_eq!(
            metadata.get("x-minio-internal-etag-algorithm").map(String::as_str),
            Some("xxh3"),
            "the pin is written under both internal prefixes"
        );

        // Conflicting copies fail closed to MD5.
        metadata.insert("x-minio-internal-etag-algorithm".to_string(), "md5".to_string());
        assert_eq!(multipart_etag_algorithm(&metadata), EtagAlgorithm::Md5);
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
        let expected = S3Error::with_default_message(S3ErrorCode::NoSuchUpload);
        assert_eq!(
            malformed.message(),
            Some(
                "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed."
            )
        );
        assert_eq!(malformed.message(), expected.message());
        assert_eq!(malformed.status_code(), Some(http::StatusCode::NOT_FOUND));
        assert_eq!(malformed.status_code(), expected.status_code());

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
            use crate::app::storage_api::test::ReqInfo;
            use crate::app::storage_api::test::sse::{
                RecordingKmsKeyAuthorizer, TestAuthorizationHooks, sse_kms_object_metadata,
            };
            use crate::storage::storage_api::contract::multipart::MultipartOperations as _;
            use rustfs_gateway_stream::Body;
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
            fn denied_envelope(authorizer: Arc<RecordingKmsKeyAuthorizer>) -> crate::app::RequestEnvelope {
                let mut extensions = http::Extensions::new();
                extensions.insert(ReqInfo {
                    cred: Some(rustfs_credentials::Credentials {
                        access_key: "denied-writer".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                });
                extensions.insert(TestAuthorizationHooks {
                    enforced: true,
                    authorizer,
                });
                crate::app::RequestEnvelope::from_parts(
                    crate::app::RequestHead {
                        method: http::Method::PUT,
                        uri: http::Uri::from_static("/"),
                        headers: http::HeaderMap::new(),
                        credentials: None,
                        region: None,
                        service: None,
                    },
                    extensions,
                )
            }
            // Keep typed request construction separate: each real handler must
            // consult the destination envelope before encryption or commit.
            let part_input = UploadPartWriteInput {
                target: UploadPartTarget::Legacy {
                    bucket: bucket.clone(),
                    key: key.to_string(),
                    upload_id: upload.upload_id.clone(),
                },
                part_number: 1,
                content_length: Some(3),
                body: Some(Body::from(bytes::Bytes::from_static(b"abc"))),
                trailing_headers: None,
                frame_trailers: None,
                content_md5: None,
                checksum_algorithm: None,
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_crc64nvme: None,
                checksum_sha1: None,
                checksum_sha256: None,
                sse_customer_algorithm: None,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            };
            let error = usecase
                .execute_upload_part_with_audit(denied_envelope(authorizer.clone()), part_input)
                .await
                .err()
                .expect("denied part");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
            let error = usecase
                .execute_upload_part_copy_with_audit(
                    denied_envelope(authorizer.clone()),
                    UploadPartCopyWriteInput {
                        target: UploadPartTarget::Legacy {
                            bucket: bucket.clone(),
                            key: key.to_string(),
                            upload_id: upload.upload_id.clone(),
                        },
                        part_number: 1,
                        copy_source: rustfs_gateway_types::CopySourceValue::Bucket {
                            bucket: bucket.clone(),
                            key: "source".to_owned(),
                            version_id: None,
                        },
                        copy_source_range: None,
                        copy_source_if_match: None,
                        copy_source_if_none_match: None,
                        copy_source_dates: Default::default(),
                        sse_customer_algorithm: None,
                        sse_customer_key: None,
                        sse_customer_key_md5: None,
                        copy_source_sse_customer_algorithm: None,
                        copy_source_sse_customer_key: None,
                        copy_source_sse_customer_key_md5: None,
                    },
                )
                .await
                .err()
                .expect("denied destination copy");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
            let complete = completion_input(&bucket, key, &upload.upload_id, vec![completion_part(1, Some("etag"))]);
            let error = Box::pin(usecase.execute_complete_multipart_upload(denied_envelope(authorizer.clone()), complete))
                .await
                .expect_err("denied completion");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
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
            let replay = completion_input(&bucket, key, &upload.upload_id, vec![completion_part(1, Some("guessed-etag"))]);
            let error = Box::pin(usecase.execute_complete_multipart_upload(denied_envelope(authorizer.clone()), replay))
                .await
                .expect_err("replay must authorize before comparing guessed part metadata");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::AccessDenied);
            assert_eq!(authorizer.calls().len(), 4);
        });
    }

    /// The SSE fields one multipart response carries, each of which becomes a response header.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct SseResponseFields {
        server_side_encryption: Option<String>,
        ssekms_key_id: Option<String>,
        sse_customer_algorithm: Option<String>,
        sse_customer_key_md5: Option<String>,
    }

    const SSE_HEADER_PREFIX: &str = "x-amz-server-side-encryption";

    /// Runs one upload through CreateMultipartUpload, UploadPart, UploadPartCopy and
    /// CompleteMultipartUpload and returns the SSE fields of each response, followed by the
    /// SSE header names of the encoded completion response.
    async fn multipart_sse_responses(
        bucket: &str,
        key: &str,
        requested_sse: Option<&str>,
        requested_kms_key_id: Option<&str>,
        customer_key: Option<(&str, &str)>,
    ) -> (Vec<(&'static str, SseResponseFields)>, Vec<String>) {
        use crate::app::storage_api::test::contract::object::ObjectIO as _;
        use rustfs_gateway::{Limits, MetaView, OperationCodec, TargetKind, WireRequest};
        use rustfs_gateway_stream::Body;

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        let usecase = DefaultMultipartUsecase::from_global();
        let source_key = format!("{key}-source");
        store
            .put_object(
                bucket,
                &source_key,
                &mut crate::storage::PutObjReader::from_vec(b"copied".to_vec()),
                &ObjectOptions::default(),
            )
            .await
            .expect("write plaintext copy source");
        let customer_algorithm = customer_key.map(|_| "AES256".to_string());
        let customer_key_md5 = customer_key.map(|(_, md5)| md5.to_string());
        let customer_key = customer_key.map(|(key, _)| key.to_string());

        let create = usecase
            .execute_create_multipart_upload(
                owned_envelope(Method::POST),
                CreateMultipartUploadFields {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    metadata: None,
                    tagging: None,
                    storage_class: None,
                    server_side_encryption: requested_sse.map(ToOwned::to_owned),
                    sse_customer_algorithm: customer_algorithm.clone(),
                    sse_customer_key: customer_key.clone(),
                    sse_customer_key_md5: customer_key_md5.clone(),
                    ssekms_key_id: requested_kms_key_id.map(ToOwned::to_owned),
                    object_lock_legal_hold_status: None,
                    object_lock_mode: None,
                    object_lock_retain_until_date: None,
                },
                None,
            )
            .await
            .expect("create multipart upload");
        let upload_id = create.upload_id;

        let part = usecase
            .execute_upload_part_with_audit(
                owned_envelope(Method::PUT),
                UploadPartWriteInput {
                    target: UploadPartTarget::Legacy {
                        bucket: bucket.to_string(),
                        key: key.to_string(),
                        upload_id: upload_id.clone(),
                    },
                    part_number: 1,
                    content_length: Some(3),
                    body: Some(Body::from(bytes::Bytes::from_static(b"abc"))),
                    trailing_headers: None,
                    frame_trailers: None,
                    content_md5: None,
                    checksum_algorithm: None,
                    checksum_crc32: None,
                    checksum_crc32c: None,
                    checksum_crc64nvme: None,
                    checksum_sha1: None,
                    checksum_sha256: None,
                    sse_customer_algorithm: customer_algorithm.clone(),
                    sse_customer_key: customer_key.clone(),
                    sse_customer_key_md5: customer_key_md5.clone(),
                },
            )
            .await
            .expect("upload part");

        let copy = Box::pin(usecase.execute_upload_part_copy_with_audit(
            owned_envelope(Method::PUT),
            UploadPartCopyWriteInput {
                target: UploadPartTarget::Legacy {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    upload_id: upload_id.clone(),
                },
                part_number: 2,
                copy_source: rustfs_gateway_types::CopySourceValue::Bucket {
                    bucket: bucket.into(),
                    key: source_key,
                    version_id: None,
                },
                copy_source_range: None,
                copy_source_if_match: None,
                copy_source_if_none_match: None,
                copy_source_dates: Default::default(),
                sse_customer_algorithm: customer_algorithm.clone(),
                sse_customer_key: customer_key.clone(),
                sse_customer_key_md5: customer_key_md5.clone(),
                copy_source_sse_customer_algorithm: None,
                copy_source_sse_customer_key: None,
                copy_source_sse_customer_key_md5: None,
            },
        ))
        .await
        .expect("copy part");

        let mut complete_input = completion_input(bucket, key, &upload_id, vec![completion_part(1, part.e_tag.as_deref())]);
        complete_input.sse_customer_algorithm = customer_algorithm;
        complete_input.sse_customer_key = customer_key.map(rustfs_gateway_types::SseCustomerKey::new);
        complete_input.sse_customer_key_md5 = customer_key_md5;
        let complete = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), complete_input))
            .await
            .expect("complete multipart upload");

        let request = http::Request::builder()
            .method("POST")
            .uri("http://localhost/bucket/object")
            .header("host", "localhost")
            .body(bytes::Bytes::new())
            .expect("wire request");
        let wire = WireRequest::accept(request, &Limits::default()).expect("accepted wire request");
        let view = MetaView::of(&wire, TargetKind::Object)
            .expect("object view")
            .with_rustfs_response_layout();
        let encoded = rustfs_gateway::dto::CompleteMultipartUpload::encode(complete.clone(), &view, 200)
            .expect("completion response encodes");
        let mut completion_sse_headers: Vec<String> = encoded
            .headers
            .keys()
            .map(|name| name.as_str().to_owned())
            .filter(|name| name.starts_with(SSE_HEADER_PREFIX))
            .collect();
        completion_sse_headers.sort();

        let responses = vec![
            (
                "CreateMultipartUpload",
                SseResponseFields {
                    server_side_encryption: create.server_side_encryption,
                    ssekms_key_id: create.ssekms_key_id,
                    sse_customer_algorithm: create.sse_customer_algorithm,
                    sse_customer_key_md5: create.sse_customer_key_md5,
                },
            ),
            (
                "UploadPart",
                SseResponseFields {
                    server_side_encryption: part.server_side_encryption,
                    ssekms_key_id: part.ssekms_key_id,
                    sse_customer_algorithm: part.sse_customer_algorithm,
                    sse_customer_key_md5: part.sse_customer_key_md5,
                },
            ),
            (
                "UploadPartCopy",
                SseResponseFields {
                    server_side_encryption: copy.server_side_encryption,
                    ssekms_key_id: copy.ssekms_key_id,
                    sse_customer_algorithm: copy.sse_customer_algorithm,
                    sse_customer_key_md5: copy.sse_customer_key_md5,
                },
            ),
            (
                "CompleteMultipartUpload",
                SseResponseFields {
                    server_side_encryption: complete.server_side_encryption.map(|value| value.as_str().to_owned()),
                    ssekms_key_id: complete.ssekms_key_id,
                    sse_customer_algorithm: complete.sse_customer_algorithm,
                    sse_customer_key_md5: complete.sse_customer_key_md5,
                },
            ),
        ];
        (responses, completion_sse_headers)
    }

    fn every_multipart_response(expected: impl Fn() -> SseResponseFields) -> Vec<(&'static str, SseResponseFields)> {
        [
            "CreateMultipartUpload",
            "UploadPart",
            "UploadPartCopy",
            "CompleteMultipartUpload",
        ]
        .into_iter()
        .map(|operation| (operation, expected()))
        .collect()
    }

    // `x-amz-server-side-encryption` names the SSE-S3 or SSE-KMS algorithm. An SSE-C upload
    // answers with the customer-key headers only, as PutObject, GetObject and HeadObject do.
    #[test]
    #[serial_test::serial]
    fn ssec_multipart_responses_carry_only_the_customer_key_headers() {
        crate::app::gating_test_env::run_large_stack_test("mpu-ssec-response-headers", || async {
            let (_store, bucket) =
                crate::app::gating_test_env::durable_quota_test_bucket("mpu-ssec-response-headers", 16_384).await;
            let customer_key = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";
            let customer_key_md5 = "SaaDheEjzuynJH8eW6AEpQ==";

            let (responses, completion_sse_headers) =
                multipart_sse_responses(&bucket, "customer-encrypted", None, None, Some((customer_key, customer_key_md5))).await;

            assert_eq!(
                responses,
                every_multipart_response(|| SseResponseFields {
                    sse_customer_algorithm: Some("AES256".to_string()),
                    sse_customer_key_md5: Some(customer_key_md5.to_string()),
                    ..Default::default()
                })
            );
            assert_eq!(
                completion_sse_headers,
                [
                    "x-amz-server-side-encryption-customer-algorithm",
                    "x-amz-server-side-encryption-customer-key-md5",
                ]
            );
        });
    }

    #[test]
    #[serial_test::serial]
    fn managed_sse_multipart_responses_name_the_managed_algorithm() {
        crate::app::gating_test_env::run_large_stack_test("mpu-managed-sse-response-headers", || async {
            use rustfs_kms::types::{CreateKeyRequest, KeyUsage};

            let (_store, bucket) =
                crate::app::gating_test_env::durable_quota_test_bucket("mpu-managed-sse-response-headers", 16_384).await;
            let key_dir = tempfile::TempDir::new().expect("local KMS key directory");
            let manager = crate::app::gating_test_env::shared_gating_ambient().await.kms().handle();
            let local_kms = || rustfs_kms::KmsConfig::local(key_dir.path().to_path_buf()).with_insecure_development_defaults();
            manager.reconfigure(local_kms()).await.expect("configure local KMS");
            let kms_key = manager
                .get_encryption_service()
                .await
                .expect("local KMS encryption service")
                .create_key(CreateKeyRequest {
                    key_name: Some("multipart-response-key".to_string()),
                    key_usage: KeyUsage::EncryptDecrypt,
                    description: None,
                    policy: None,
                    tags: HashMap::new(),
                    origin: None,
                })
                .await
                .expect("create multipart KMS key");
            // SSE-S3 names no key, so it wraps its data key under the service default.
            manager
                .reconfigure(local_kms().with_default_key(kms_key.key_id.clone()))
                .await
                .expect("select the default KMS key");

            let (sse_s3, sse_s3_completion_headers) =
                multipart_sse_responses(&bucket, "sse-s3-encrypted", Some(ServerSideEncryption::AES256.as_str()), None, None)
                    .await;
            assert_eq!(
                sse_s3,
                every_multipart_response(|| SseResponseFields {
                    server_side_encryption: Some("AES256".to_string()),
                    ..Default::default()
                })
            );
            assert_eq!(sse_s3_completion_headers, ["x-amz-server-side-encryption"]);

            let (sse_kms, sse_kms_completion_headers) = multipart_sse_responses(
                &bucket,
                "sse-kms-encrypted",
                Some(ServerSideEncryption::AWS_KMS.as_str()),
                Some(&kms_key.key_id),
                None,
            )
            .await;
            assert_eq!(
                sse_kms,
                every_multipart_response(|| SseResponseFields {
                    server_side_encryption: Some("aws:kms".to_string()),
                    ssekms_key_id: Some(kms_key.key_id.clone()),
                    ..Default::default()
                })
            );
            assert_eq!(
                sse_kms_completion_headers,
                ["x-amz-server-side-encryption", "x-amz-server-side-encryption-aws-kms-key-id"]
            );

            // A plain key name sent behind an AWS-style ARN prefix opens a session under
            // that key, and every later response names the key by its id.
            let (arn_prefixed, _) = multipart_sse_responses(
                &bucket,
                "sse-kms-arn-prefixed-key-name",
                Some(ServerSideEncryption::AWS_KMS.as_str()),
                Some(&format!("arn:aws:kms:{}", kms_key.key_id)),
                None,
            )
            .await;
            assert_eq!(
                arn_prefixed,
                every_multipart_response(|| SseResponseFields {
                    server_side_encryption: Some("aws:kms".to_string()),
                    ssekms_key_id: Some(kms_key.key_id.clone()),
                    ..Default::default()
                })
            );
            manager.stop().await.expect("stop local KMS");
        });
    }
}
