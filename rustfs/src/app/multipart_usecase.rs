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
    replication_request_authorized,
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
use super::storage_api::multipart_usecase::contract::multipart::{CompletePart, MultipartOperations as _, MultipartUploadResult};
use super::storage_api::multipart_usecase::contract::object::{ObjectIO as _, ObjectOperations as _};
use super::storage_api::multipart_usecase::contract::range::HTTPRangeSpec;
use super::storage_api::multipart_usecase::data_usage::{
    quota_object_size, record_bucket_object_version_write_memory, record_bucket_object_write_memory,
};
use super::storage_api::multipart_usecase::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use super::storage_api::multipart_usecase::helper::OperationHelper;
#[cfg(test)]
use super::storage_api::multipart_usecase::io::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader, wrap_reader};
use super::storage_api::multipart_usecase::io::{HashReader, WriteEncryption, WritePlan, compression_metadata_value};
use super::storage_api::multipart_usecase::object_utils::to_s3s_etag;
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
    apply_bucket_default_lock_retention, authorize_sse_kms_object_read, build_ssec_read_headers, encryption_material_to_metadata,
    extract_server_side_encryption_from_headers, extract_ssec_params_from_headers, extract_ssekms_context_from_headers,
    get_buffer_size_opt_in, load_bucket_object_lock_config_state, map_get_object_reader_error, mark_encrypted_multipart_metadata,
    sse_decryption, sse_prepare_encryption,
};
use super::storage_api::multipart_usecase::{
    StorageObjectInfo as ObjectInfo, StorageObjectOptions as ObjectOptions, StoragePutObjReader as PutObjReader,
};
use crate::app::object_data_cache::{
    ObjectDataCacheAdapter, invalidate_object_data_cache_after_complete_multipart_success,
    invalidate_object_data_cache_before_mutation,
};
use crate::app::object_usecase::{
    acquire_copy_bucket_lifecycle_locks, apply_quota_admission, build_put_like_object_lock_metadata, map_quota_check_outcome,
    validate_existing_object_lock_for_write,
};
use crate::app::runtime_sources::{
    AppContext, current_app_context, current_object_data_cache_for_context, current_object_store_handle_for_context,
};
use crate::capacity::record_capacity_write;
use crate::error::ApiError;
use crate::table_catalog;
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, Uri};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
#[cfg(test)]
use rustfs_utils::http::insert_header;
use rustfs_utils::http::{
    SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP,
    SUFFIX_SOURCE_REPLICATION_REQUEST, contains_key_str, get_header, get_source_scheme,
    headers::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING, AMZ_STORAGE_CLASS},
    insert_str,
};
use s3s::dto::{
    AbortMultipartUploadInput, AbortMultipartUploadOutput, ChecksumAlgorithm, ChecksumType, CompleteMultipartUploadInput,
    CompleteMultipartUploadOutput, CompletedPart, CopyPartResult, CopySource, CreateMultipartUploadInput,
    CreateMultipartUploadOutput, ETag, ListMultipartUploadsInput, ListMultipartUploadsOutput, ListPartsInput, ListPartsOutput,
    ServerSideEncryption, StreamingBlob, Timestamp, UploadPartCopyInput, UploadPartCopyOutput, UploadPartInput, UploadPartOutput,
};
use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::io::StreamReader;
use tracing::{instrument, warn};
use urlencoding::encode;
use uuid::Uuid;

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

/// Returns InvalidRange error if CopySourceRange end exceeds the source object size.
/// Used by execute_upload_part_copy to reject out-of-bounds ranges per S3 spec.
fn validate_copy_source_range_not_exceeds(range_spec: &HTTPRangeSpec, object_size: i64) -> S3Result<()> {
    if range_spec.end >= object_size {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRange,
            "The requested range is not satisfiable".to_string(),
        ));
    }
    Ok(())
}

fn validate_complete_multipart_parts(parts: &[CompletePart]) -> S3Result<()> {
    if parts.windows(2).any(|window| window[0].part_num >= window[1].part_num) {
        return Err(s3_error!(InvalidPartOrder, "Part numbers must be strictly increasing"));
    }

    Ok(())
}

fn normalize_complete_multipart_parts(parts: Vec<CompletePart>) -> S3Result<Vec<CompletePart>> {
    // For duplicate part numbers, keep the last occurrence from the request.
    // This matches retry/resend semantics where later uploads override earlier ones.
    let mut seen = HashSet::with_capacity(parts.len());
    let mut deduped_reversed = Vec::with_capacity(parts.len());
    for part in parts.into_iter().rev() {
        if seen.insert(part.part_num) {
            deduped_reversed.push(part);
        }
    }
    deduped_reversed.reverse();

    validate_complete_multipart_parts(&deduped_reversed)?;
    Ok(deduped_reversed)
}

fn complete_part_from_s3(value: CompletedPart) -> CompletePart {
    CompletePart {
        part_num: value
            .part_number
            .and_then(|part_num| usize::try_from(part_num).ok())
            .unwrap_or_default(),
        etag: value.e_tag.map(|v| v.value().to_owned()),
        checksum_crc32: value.checksum_crc32,
        checksum_crc32c: value.checksum_crc32c,
        checksum_sha1: value.checksum_sha1,
        checksum_sha256: value.checksum_sha256,
        checksum_crc64nvme: value.checksum_crc64nvme,
    }
}

fn create_multipart_upload_metadata(
    input_metadata: Option<HashMap<String, String>>,
    headers: &HeaderMap,
    tagging: Option<String>,
    storage_class: Option<&s3s::dto::StorageClass>,
) -> HashMap<String, String> {
    let mut metadata = input_metadata.unwrap_or_default();
    namespace_reserved_user_metadata(&mut metadata);
    extract_metadata_from_mime(headers, &mut metadata);

    if let Some(tags) = tagging {
        metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
    }
    if let Some(storage_class) = storage_class {
        metadata.insert(AMZ_STORAGE_CLASS.to_owned(), storage_class.as_str().to_owned());
    }

    metadata
}

/// A multipart session advertises disk compression only when the staged-rollout
/// switch (`RUSTFS_COMPRESSION_MULTIPART_ENABLED`) is on, the object key/headers
/// qualify, AND the session is not an SSE-C ciphertext-passthrough replication
/// session, which must preserve source bytes verbatim.
///
/// The rollout switch defaults to off so a rolling upgrade never creates new
/// compressed multipart objects while pre-fix nodes (whose decompressor is not
/// resumable) may still serve reads. Enable it once the fleet has converged on a
/// fixed build; the default flips per the `multipart-compression-default-off-window`
/// entry in docs/architecture/compat-cleanup-register.md.
///
/// Each part is compressed as an independent stream; the GET path decodes across part
/// boundaries (see `ReadTransform::Compressed`), so the session may advertise
/// object-level compression again.
///
/// Unlike single PUT there is no `MIN_DISK_COMPRESSIBLE_SIZE` floor here: the total
/// object size is unknown at CreateMultipartUpload time, so tiny multipart objects pay
/// the (harmless) framing overhead. This is a deliberate trade-off, not a bug.
fn should_advertise_session_compression(multipart_enabled: bool, ciphertext_passthrough: bool, disk_compressible: bool) -> bool {
    multipart_enabled && !ciphertext_passthrough && disk_compressible
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

fn has_complete_multipart_object_lock_headers(headers: &HeaderMap) -> bool {
    headers.contains_key(X_AMZ_OBJECT_LOCK_MODE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD)
        || has_bypass_governance_header(headers)
}

fn internal_object_info_lookup_opts(mut opts: ObjectOptions) -> ObjectOptions {
    opts.http_preconditions = None;
    opts
}

fn quota_accounting_object_size(info: &ObjectInfo, fail_closed: bool) -> S3Result<u64> {
    match quota_object_size(info) {
        Ok(size) => Ok(size),
        Err(err) if fail_closed => Err(ApiError::from(err).into()),
        Err(_) => Ok(info.size.max(0) as u64),
    }
}

fn encode_s3_path(path: &str) -> String {
    path.split('/')
        .map(|part| encode(part).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn extract_request_scheme(headers: &HeaderMap, uri: &Uri) -> String {
    get_source_scheme(headers)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
        })
        .or_else(|| uri.scheme_str().map(str::to_owned))
        .unwrap_or_else(|| "http".to_string())
        .to_ascii_lowercase()
}

fn extract_request_host(headers: &HeaderMap, uri: &Uri) -> Option<String> {
    headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| uri.authority().map(|authority| authority.as_str().to_string()))
}

fn decoded_content_length_from_headers(headers: &HeaderMap) -> S3Result<Option<i64>> {
    let Some(val) = headers.get(AMZ_DECODED_CONTENT_LENGTH) else {
        return Ok(None);
    };

    match atoi::atoi::<i64>(val.as_bytes()) {
        Some(x) => Ok(Some(x)),
        None => Err(s3_error!(UnexpectedContent)),
    }
}

fn request_uses_aws_chunked(headers: &HeaderMap) -> bool {
    let has_aws_chunked = |header_name: &str| {
        headers
            .get(header_name)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.split(',').any(|part| part.trim().eq_ignore_ascii_case("aws-chunked")))
    };

    has_aws_chunked("content-encoding") || has_aws_chunked("transfer-encoding")
}

fn resolve_upload_part_size(headers: &HeaderMap, content_length: Option<i64>) -> S3Result<Option<i64>> {
    let decoded_content_length = decoded_content_length_from_headers(headers)?;
    let size = match (request_uses_aws_chunked(headers), decoded_content_length, content_length) {
        (true, Some(decoded), _) => Some(decoded),
        (_, _, Some(length)) => Some(length),
        (_, Some(decoded), None) => Some(decoded),
        _ => None,
    };

    if size == Some(-1) {
        return Err(s3_error!(UnexpectedContent));
    }

    Ok(size)
}

fn build_complete_multipart_location(headers: &HeaderMap, uri: &Uri, bucket: &str, key: &str) -> String {
    let object_path = format!("/{}/{}", encode(bucket), encode_s3_path(key));

    match extract_request_host(headers, uri) {
        Some(host) => {
            let scheme = extract_request_scheme(headers, uri);
            format!("{scheme}://{host}{object_path}")
        }
        None => object_path,
    }
}

#[derive(Clone, Default)]
pub struct DefaultMultipartUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultMultipartUsecase {
    #[cfg(test)]
    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: current_app_context(),
        }
    }

    /// Build the use-case bound to an explicit application context
    /// (backlog#1052 S6): the per-server request path passes its own context
    /// so the use-case resolves that server's store; `None` falls back to the
    /// ambient default.
    pub fn with_context(context: Option<std::sync::Arc<crate::runtime_sources::AppContext>>) -> Self {
        Self { context }
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

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        record_s3_op(S3Operation::AbortMultipartUpload);
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let AbortMultipartUploadInput {
            bucket, key, upload_id, ..
        } = req.input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // Special handling for abort_multipart_upload: Per AWS S3 API specification, this operation
        // should return NoSuchUpload (404) when the upload_id doesn't exist, even if the format
        // appears invalid. This differs from other multipart operations (upload_part, list_parts,
        // complete_multipart_upload) which return InvalidArgument for malformed upload_ids.
        // The lenient validation matches AWS S3 behavior where format validation is relaxed for
        // abort operations to avoid leaking information about upload_id format requirements.
        match store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), &opts)
            .await
        {
            Ok(_) => {
                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() }))
            }
            Err(err) => {
                // Convert MalformedUploadID to NoSuchUpload for S3 API compatibility
                if matches!(err, StorageError::MalformedUploadID(_)) {
                    return Err(S3Error::new(S3ErrorCode::NoSuchUpload));
                }
                Err(ApiError::from(err).into())
            }
        }
    }

    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let mut helper = OperationHelper::new(
            &req,
            EventName::ObjectCreatedCompleteMultipartUpload,
            S3Operation::CompleteMultipartUpload,
        );
        let replication_authorized = replication_request_authorized(&req);
        let input = req.input.clone();
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            if_match,
            if_none_match,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = input;

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        if if_match.is_some() || if_none_match.is_some() {
            let Some(store) = self.object_store() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
            };

            match store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
                Ok(info) => {
                    if !info.delete_marker {
                        if let Some(ifmatch) = if_match
                            && let Some(strong_etag) = ifmatch.into_etag()
                            && info
                                .etag
                                .as_ref()
                                .is_some_and(|etag| ETag::Strong(etag.clone()) != strong_etag)
                        {
                            return Err(s3_error!(PreconditionFailed));
                        }
                        if let Some(ifnonematch) = if_none_match
                            && let Some(strong_etag) = ifnonematch.into_etag()
                            && info
                                .etag
                                .as_ref()
                                .is_some_and(|etag| ETag::Strong(etag.clone()) == strong_etag)
                        {
                            return Err(s3_error!(PreconditionFailed));
                        }
                    }
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(ApiError::from(err).into());
                    }

                    if if_match.is_some() && (is_err_object_not_found(&err) || is_err_version_not_found(&err)) {
                        return Err(ApiError::from(err).into());
                    }
                }
            }
        }

        let Some(multipart_upload) = multipart_upload else { return Err(s3_error!(InvalidPart)) };
        let Some(parts) = multipart_upload.parts.filter(|parts| !parts.is_empty()) else {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "You must specify at least one part".to_string(),
            ));
        };

        let mut opts = get_complete_multipart_upload_opts_with_replication_authorization(&req.headers, replication_authorized)
            .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;
        let version_suspended = BucketVersioningSys::prefix_suspended(&bucket, &key).await;
        opts.versioned = versioned;
        opts.version_suspended = version_suspended;
        let capacity_scope_token = Uuid::new_v4();
        opts.capacity_scope_token = Some(capacity_scope_token);

        let uploaded_parts_vec = parts.into_iter().map(complete_part_from_s3).collect::<Vec<_>>();

        let uploaded_parts = normalize_complete_multipart_parts(uploaded_parts_vec)?;

        if has_complete_multipart_object_lock_headers(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "CompleteMultipartUpload does not accept object lock or governance bypass headers.".to_string(),
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let current_opts = internal_object_info_lookup_opts(
            get_opts(&bucket, &key, None, None, &req.headers)
                .await
                .map_err(ApiError::from)?,
        );
        let previous_current_sizes = match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => {
                validate_existing_object_lock_for_write(&existing_obj_info, &current_opts)?;
                let physical_size = existing_obj_info.size.max(0) as u64;
                let logical_size = quota_object_size(&existing_obj_info);
                Some((physical_size, logical_size))
            }
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
                None
            }
        };

        let multipart_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &opts)
            .await
            .map_err(ApiError::from)?;
        // A ciphertext-passthrough session stores encrypted parts verbatim and
        // completes without the customer key (the replication client has none),
        // so the SSE-C completion check must be skipped for it.
        if !contains_key_str(&multipart_info.user_defined, SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT) {
            EncryptionRequest {
                bucket: &bucket,
                key: &key,
                server_side_encryption: None,
                ssekms_key_id: None,
                ssekms_context: None,
                sse_customer_algorithm,
                sse_customer_key,
                sse_customer_key_md5,
                content_size: 0,
                principal: None,
            }
            .validate_multipart_ssec(&multipart_info.user_defined)?;
        }
        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let server_side_encryption = multipart_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|s| ServerSideEncryption::from(s.clone()));
        let ssekms_key_id = match server_side_encryption.as_ref() {
            Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS => multipart_info
                .user_defined
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .cloned(),
            _ => None,
        };

        let quota_metadata_sys = self.bucket_metadata_sys();
        let quota_tracking = quota_metadata_sys.is_some();
        let mut quota_enabled = false;
        if let Some(metadata_sys) = quota_metadata_sys.as_ref() {
            let quota_checker = QuotaChecker::new(metadata_sys.clone());
            let check_result =
                map_quota_check_outcome(&bucket, quota_checker.check_quota(&bucket, QuotaOperation::PutObject, 0).await)?;
            quota_enabled = check_result.quota_limit.is_some();
            apply_quota_admission(&mut opts, &check_result)?;
        }

        let previous_current_size = match previous_current_sizes {
            Some((_, Ok(logical_size))) if quota_enabled => Some(logical_size),
            Some((_, Err(err))) if quota_enabled => return Err(ApiError::from(err).into()),
            Some((physical_size, _)) => Some(physical_size),
            None => None,
        };

        let complete_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let opts = opts.clone();
            async move {
                let obj_info = store
                    .clone()
                    .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, &opts)
                    .await
                    .map_err(ApiError::from)?;
                let _ = invalidate_object_data_cache_after_complete_multipart_success(&cache_adapter, &bucket, &key).await;
                record_capacity_write(Some(capacity_scope_token)).await;

                if quota_tracking {
                    let committed_size = quota_accounting_object_size(&obj_info, quota_enabled)?;

                    if versioned {
                        record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size).await;
                    } else {
                        record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                    }
                }

                enqueue_transition_immediate(&obj_info, LcEventSrc::S3CompleteMultipartUpload).await;

                let mt2 = obj_info.user_defined.clone();
                let dsc = must_replicate_object(
                    &bucket,
                    &key,
                    &mt2,
                    "".to_string(),
                    opts.delete_marker_replication_status(),
                    opts.clone(),
                )
                .await;

                if dsc.replicate_any() {
                    warn!("need multipart replication");
                    schedule_object_replication(obj_info.clone(), store, dsc).await;
                }

                rustfs_scanner::record_dirty_usage_bucket(&bucket);
                Ok::<_, S3Error>(obj_info)
            }
        });
        let obj_info = complete_commit.await.map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("complete multipart upload commit owner task failed: {err}"),
            )
        })??;

        let mpu_version = if versioned {
            obj_info.version_id.map(|v| v.to_string())
        } else {
            None
        };
        let mpu_version_for_event = mpu_version.clone();
        // checksum: stored (decrypted) values take precedence over the request input;
        // additional algorithms (XXHash3/64/128, SHA-512, MD5), which have no typed
        // CompleteMultipartUploadOutput field, are echoed as raw response headers (#1261).
        let (checksums, is_multipart) = obj_info
            .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
            .map_err(ApiError::from)?;

        let classified = crate::app::object_usecase::classify_response_checksums(checksums, is_multipart);
        let checksum_crc32 = classified.crc32.or(input.checksum_crc32);
        let checksum_crc32c = classified.crc32c.or(input.checksum_crc32c);
        let checksum_sha1 = classified.sha1.or(input.checksum_sha1);
        let checksum_sha256 = classified.sha256.or(input.checksum_sha256);
        let checksum_crc64nvme = classified.crc64nvme.or(input.checksum_crc64nvme);
        let checksum_type = classified.checksum_type.or(input.checksum_type);
        let complete_extra_checksum_headers = classified.extra;

        let location = build_complete_multipart_location(&req.headers, &req.uri, &bucket, &key);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(location),
            server_side_encryption: server_side_encryption.clone(),
            ssekms_key_id: ssekms_key_id.clone(),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            version_id: mpu_version,
            ..Default::default()
        };
        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }

        let mut response = S3Response::new(output);
        crate::app::object_usecase::inject_additional_checksum_headers(&mut response.headers, &complete_extra_checksum_headers);
        if let Some(algorithm) = multipart_info
            .user_defined
            .get("x-amz-server-side-encryption-customer-algorithm")
        {
            let value = HeaderValue::from_str(algorithm)
                .map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C algorithm metadata"))?;
            response
                .headers
                .insert("x-amz-server-side-encryption-customer-algorithm", value);
        }
        if let Some(key_md5) = multipart_info
            .user_defined
            .get("x-amz-server-side-encryption-customer-key-md5")
        {
            let value =
                HeaderValue::from_str(key_md5).map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C key metadata"))?;
            response
                .headers
                .insert("x-amz-server-side-encryption-customer-key-md5", value);
        }
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let helper =
            OperationHelper::new(&req, EventName::ObjectCreatedCreateMultipartUpload, S3Operation::CreateMultipartUpload)
                .suppress_event();
        let replication_authorized = replication_request_authorized(&req);
        let CreateMultipartUploadInput {
            bucket,
            key,
            tagging,
            version_id,
            storage_class,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            metadata: input_metadata,
            ..
        } = req.input.clone();

        let server_side_encryption = server_side_encryption.or(extract_server_side_encryption_from_headers(&req.headers)?);
        let ssekms_key_id = ssekms_key_id.or_else(|| {
            req.headers
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .and_then(|value| value.to_str().ok())
                .map(ToOwned::to_owned)
        });

        // Validate storage class if provided
        if let Some(ref storage_class) = storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_archive_content_encoding(
            &key,
            req.headers.get("content-type").and_then(|value| value.to_str().ok()),
            req.headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;

        let mut metadata = create_multipart_upload_metadata(input_metadata, &req.headers, tagging, storage_class.as_ref());

        let has_explicit_object_lock_retention = object_lock_mode.is_some()
            || object_lock_retain_until_date.is_some()
            || has_replication_retention_update(&req.headers, replication_authorized);
        let object_lock_config_state = load_bucket_object_lock_config_state(&bucket).await?;
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
            &bucket,
            &object_lock_config_state,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )? {
            metadata.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(
            &bucket,
            &object_lock_config_state,
            &mut metadata,
            has_explicit_object_lock_retention,
        )?;
        let (header_sse_customer_algorithm, header_sse_customer_key, header_sse_customer_key_md5) =
            extract_ssec_params_from_headers(&req.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(header_sse_customer_algorithm);
        let sse_customer_key = sse_customer_key.or(header_sse_customer_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(header_sse_customer_key_md5);

        // The session data key is generated here, so this is where a multipart upload is held
        // to the KMS key it names. Parts and the completion reuse the resulting envelope.
        let session_principal = SseKmsPrincipal::from_request(&req);
        let encryption_request = PrepareEncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            principal: session_principal.as_ref(),
        };

        // SSE-C ciphertext passthrough: parts are already encrypted, so no
        // session DEK is prepared; a session marker tells UploadPart to store
        // the ciphertext verbatim instead of recovering encryption material.
        let ciphertext_passthrough = replication_authorized
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_REQUEST).as_deref() == Some("true")
            && rustfs_utils::http::ssec_transport_to_stored_metadata(&req.headers).is_some();
        if ciphertext_passthrough && let Some(metadata_sys) = self.bucket_metadata_sys() {
            let check_result = map_quota_check_outcome(
                &bucket,
                QuotaChecker::new(metadata_sys)
                    .check_quota(&bucket, QuotaOperation::PutObject, 0)
                    .await,
            )?;
            if check_result.quota_limit.is_some() {
                return Err(S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    "SSE-C ciphertext replication is unavailable for quota-enabled buckets".to_string(),
                ));
            }
        }
        if ciphertext_passthrough {
            insert_str(&mut metadata, SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT, "true".to_string());
        }

        let prepared_material = if ciphertext_passthrough {
            None
        } else {
            sse_prepare_encryption(encryption_request).await?
        };
        let (effective_sse, effective_kms_key_id) = match prepared_material {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let ssekms_key_id = material.kms_key_id.clone();

                let mut encryption_metadata = encryption_material_to_metadata(&material)?;
                if material.key_kind == EncryptionKeyKind::Object {
                    mark_encrypted_multipart_metadata(&mut encryption_metadata);
                }
                metadata.extend(encryption_metadata);

                (server_side_encryption, ssekms_key_id)
            }
            None => (None, None),
        };

        if should_advertise_session_compression(
            is_multipart_disk_compression_enabled(),
            ciphertext_passthrough,
            is_disk_compressible(&req.headers, &key),
        ) {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_COMPRESSION,
                compression_metadata_value(CompressionAlgorithm::default()),
            );
        }

        let mt2 = metadata.clone();
        let mut opts: ObjectOptions =
            put_opts_with_replication_authorization(&bucket, &key, version_id, &req.headers, metadata, replication_authorized)
                .await
                .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;

        let dsc =
            must_replicate_object(&bucket, &key, &mt2, "".to_string(), opts.delete_marker_replication_status(), opts.clone())
                .await;
        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let current_opts: ObjectOptions = get_opts(&bucket, &key, opts.version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => validate_existing_object_lock_for_write(&existing_obj_info, &opts)?,
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
            }
        }

        let checksum_type = rustfs_rio::ChecksumType::from_header(&req.headers);
        if checksum_type.is(rustfs_rio::ChecksumType::INVALID) {
            return Err(s3_error!(InvalidArgument, "Invalid checksum type"));
        } else if checksum_type.is_set() && !checksum_type.is(rustfs_rio::ChecksumType::TRAILING) {
            opts.want_checksum = Some(rustfs_rio::Checksum {
                checksum_type,
                ..Default::default()
            });
        }

        let MultipartUploadResult {
            upload_id,
            checksum_algo,
            checksum_type,
        } = store
            .new_multipart_upload(&bucket, &key, &opts)
            .await
            .map_err(ApiError::from)?;

        let output = CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            server_side_encryption: effective_sse,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: effective_kms_key_id,
            checksum_algorithm: checksum_algo.map(ChecksumAlgorithm::from),
            checksum_type: checksum_type.map(ChecksumType::from),
            ..Default::default()
        };

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let input = req.input;
        let UploadPartInput {
            body,
            bucket,
            key,
            upload_id,
            part_number,
            content_length,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            // content_md5,
            ..
        } = input;

        let part_id = parse_upload_part_number(part_number)?;

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let mut size = resolve_upload_part_size(&req.headers, content_length)?;
        let mut body_stream = body.ok_or_else(|| s3_error!(IncompleteBody))?;

        if size.is_none() {
            let mut total = 0i64;
            let mut buffer = bytes::BytesMut::new();
            while let Some(chunk) = body_stream.next().await {
                let chunk = chunk.map_err(|e| ApiError::from(StorageError::other(e.to_string())))?;
                total += chunk.len() as i64;
                buffer.extend_from_slice(&chunk);
            }

            if total <= 0 {
                return Err(s3_error!(UnexpectedContent));
            }

            size = Some(total);
            let combined = buffer.freeze();
            let stream = futures::stream::once(async move { Ok::<Bytes, std::io::Error>(combined) });
            body_stream = StreamingBlob::wrap(stream);
        }

        // Get multipart info early to check if managed encryption will be applied
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let fi = store
            .get_multipart_info(&bucket, &key, &upload_id, &opts)
            .await
            .map_err(ApiError::from)?;

        let mut size = size.ok_or_else(|| s3_error!(UnexpectedContent))?;
        let ingress_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(std::time::Instant::now);

        // Apply adaptive buffer sizing based on part size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on part size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body_stream.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let is_disk_compressed = rustfs_utils::http::contains_key_str(&fi.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        let actual_size = size;

        let mut md5hex = if let Some(base64_md5) = input.content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256_with_query(&req.headers, req.uri.query());

        let mut write_plan = WritePlan::new();
        let mut reader = if is_disk_compressed {
            let algorithm = CompressionAlgorithm::default();
            let mut hrd = HashReader::from_stream(body, size, actual_size, md5hex.take(), sha256hex.take(), false)
                .map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            size = HashReader::SIZE_PRESERVE_LAYER;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            HashReader::from_stream(body, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?
        };

        if let Err(err) = reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), size < 0) {
            return Err(ApiError::from(err).into());
        }
        opts.want_checksum = reader.checksum();

        // An SSE-C passthrough session stores ciphertext parts verbatim: no
        // material recovery, no validation against the (absent) customer key.
        let preserve_ciphertext = contains_key_str(&fi.user_defined, SUFFIX_REPLICATION_PRESERVE_CIPHERTEXT);
        let has_ssec = !preserve_ciphertext
            && fi
                .user_defined
                .contains_key("x-amz-server-side-encryption-customer-algorithm");
        let (server_side_encryption, ssekms_key_id) = if has_ssec || preserve_ciphertext {
            (None, None)
        } else {
            let sse = fi
                .user_defined
                .get("x-amz-server-side-encryption")
                .map(|s| {
                    ServerSideEncryption::from_str(s)
                        .map_err(|e| ApiError::from(StorageError::other(format!("Invalid server-side encryption: {e}"))))
                })
                .transpose()?;
            let key_id = match sse.as_ref() {
                Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS => fi
                    .user_defined
                    .get("x-amz-server-side-encryption-aws-kms-key-id")
                    .map(|s| s.to_string()),
                _ => None,
            };
            (sse, key_id)
        };
        if !preserve_ciphertext {
            EncryptionRequest {
                bucket: &bucket,
                key: &key,
                server_side_encryption: server_side_encryption.clone(),
                ssekms_key_id: ssekms_key_id.clone(),
                ssekms_context: None,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key: sse_customer_key.clone(),
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
                principal: None,
            }
            .validate_multipart_ssec(&fi.user_defined)?;
        }
        let (requested_sse, requested_kms_key_id) = if has_ssec {
            let ssec_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &fi.user_defined,
                sse_customer_key: sse_customer_key.as_ref(),
                sse_customer_key_md5: sse_customer_key_md5.as_ref(),
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing SSE-C session material")))?;
            let ssec_write = match ssec_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(ssec_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(ssec_material.key_bytes, ssec_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(ssec_write);
            (Some(ssec_material.server_side_encryption), ssec_material.kms_key_id)
        } else if let Some(server_side_encryption) = server_side_encryption {
            // Reuses the envelope the create-multipart-upload call was authorized for; the
            // KMS key was pinned into the session metadata then and cannot change here.
            let managed_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &fi.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(managed_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(managed_material.key_bytes, managed_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(managed_write);
            (Some(server_side_encryption), ssekms_key_id)
        } else {
            (None, None)
        };

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        let mut reader = PutObjReader::new(reader);

        if let Some(stage_start) = ingress_stage_start {
            rustfs_io_metrics::record_put_object_stage_duration(
                "multipart_ingress_prepare",
                stage_start.elapsed().as_secs_f64() * 1000.0,
            );
        }

        let info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;

        let mut checksum_crc32 = input.checksum_crc32;
        let mut checksum_crc32c = input.checksum_crc32c;
        let mut checksum_sha1 = input.checksum_sha1;
        let mut checksum_sha256 = input.checksum_sha256;
        let mut checksum_crc64nvme = input.checksum_crc64nvme;

        if let Some(alg) = &input.checksum_algorithm
            && let Some(Some(checksum_str)) = req.trailing_headers.as_ref().map(|trailer| {
                let key = match alg.as_str() {
                    ChecksumAlgorithm::CRC32 => rustfs_rio::ChecksumType::CRC32.key(),
                    ChecksumAlgorithm::CRC32C => rustfs_rio::ChecksumType::CRC32C.key(),
                    ChecksumAlgorithm::SHA1 => rustfs_rio::ChecksumType::SHA1.key(),
                    ChecksumAlgorithm::SHA256 => rustfs_rio::ChecksumType::SHA256.key(),
                    ChecksumAlgorithm::CRC64NVME => rustfs_rio::ChecksumType::CRC64_NVME.key(),
                    _ => return None,
                };
                trailer.read(|headers| {
                    headers
                        .get(key.unwrap_or_default())
                        .and_then(|value| value.to_str().ok().map(|s| s.to_string()))
                })
            })
        {
            match alg.as_str() {
                ChecksumAlgorithm::CRC32 => checksum_crc32 = checksum_str,
                ChecksumAlgorithm::CRC32C => checksum_crc32c = checksum_str,
                ChecksumAlgorithm::SHA1 => checksum_sha1 = checksum_str,
                ChecksumAlgorithm::SHA256 => checksum_sha256 = checksum_str,
                ChecksumAlgorithm::CRC64NVME => checksum_crc64nvme = checksum_str,
                _ => (),
            }
        }

        // XXHash3/64/128 and SHA-512 have no typed UploadPartOutput field; echo the
        // server-computed part checksum as a raw response header (#1261).
        let upload_part_extra_checksum_headers = crate::app::object_usecase::additional_checksum_echo_pairs(&opts.want_checksum);

        let output = UploadPartOutput {
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            ..Default::default()
        };

        let mut response = S3Response::new(output);
        crate::app::object_usecase::inject_additional_checksum_headers(
            &mut response.headers,
            &upload_part_extra_checksum_headers,
        );
        Ok(response)
    }

    pub async fn execute_list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let ListMultipartUploadsInput {
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
            ..
        } = req.input;

        let ListMultipartUploadsParams {
            prefix,
            key_marker,
            max_uploads,
        } = parse_list_multipart_uploads_params(prefix, key_marker, max_uploads)?;
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // `apply_bucket_generation_guard` tolerates a missing guard (only the S3
        // access layer installs one), so resolve the current generation rather
        // than failing the request. Listing is filtered by this value, so a
        // stale one simply hides foreign-incarnation uploads, as intended.
        let expected_incarnation_id = match opts.expected_bucket_incarnation_id {
            Some(incarnation_id) => incarnation_id,
            None => store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?,
        };

        let result = store
            .list_multipart_uploads_for_bucket_incarnation(
                &bucket,
                &prefix,
                key_marker,
                upload_id_marker,
                delimiter,
                max_uploads,
                expected_incarnation_id,
            )
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(build_list_multipart_uploads_output(bucket, prefix, result)))
    }

    pub async fn execute_list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let mut opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut opts)?;
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        let params = parse_list_parts_params(part_number_marker, max_parts)?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let res = store
            .list_object_parts(&bucket, &key, &upload_id, params.part_number_marker, params.max_parts, &opts)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(build_list_parts_output(res)))
    }

    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        // Captured before `req.input` is destructured below.
        let copy_principal = SseKmsPrincipal::from_request(&req);
        let source_bucket = match &req.input.copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket { bucket, .. } => bucket.to_string(),
        };
        let mut source_generation_opts = ObjectOptions::default();
        apply_copy_source_bucket_generation_guard(&req, &source_bucket, &mut source_generation_opts)?;
        let expected_source_incarnation_id = source_generation_opts.expected_bucket_incarnation_id;
        let mut destination_generation_opts = ObjectOptions::default();
        apply_bucket_generation_guard(&req, &req.input.bucket, &mut destination_generation_opts)?;
        let expected_destination_incarnation_id = destination_generation_opts.expected_bucket_incarnation_id;
        let UploadPartCopyInput {
            bucket,
            key,
            copy_source,
            copy_source_range,
            part_number,
            upload_id,
            copy_source_if_match,
            copy_source_if_none_match,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            ..
        } = req.input;

        let (src_bucket, src_key, src_version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                bucket: ref src_bucket,
                key: ref src_key,
                version_id,
            } => (src_bucket.to_string(), src_key.to_string(), version_id.map(|v| v.to_string())),
        };

        let rs = if let Some(range_str) = copy_source_range {
            Some(parse_copy_source_range(&range_str)?)
        } else {
            None
        };

        let part_id = parse_upload_part_number(part_number)?;

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let (source_bucket_lifecycle_guard, destination_bucket_lifecycle_guard_storage) =
            acquire_copy_bucket_lifecycle_locks(store.as_ref(), &src_bucket, &bucket).await?;
        let current_source_incarnation_id = store
            .bucket_incarnation_id_from_disk(&src_bucket)
            .await
            .map_err(ApiError::from)?;
        if expected_source_incarnation_id.is_some_and(|expected| expected != current_source_incarnation_id) {
            return Err(ApiError::from(StorageError::BucketNotFound(src_bucket.clone())).into());
        }
        let current_destination_incarnation_id = if src_bucket == bucket {
            current_source_incarnation_id
        } else {
            store.bucket_incarnation_id_from_disk(&bucket).await.map_err(ApiError::from)?
        };
        if expected_destination_incarnation_id.is_some_and(|expected| expected != current_destination_incarnation_id) {
            return Err(ApiError::from(StorageError::BucketNotFound(bucket.clone())).into());
        }
        let destination_bucket_lifecycle_guard = destination_bucket_lifecycle_guard_storage
            .as_ref()
            .unwrap_or(&source_bucket_lifecycle_guard);
        if source_bucket_lifecycle_guard.is_lock_lost() || destination_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_bucket_generation",
                bucket: bucket.clone(),
                object: key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }
        let mut dst_opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(current_destination_incarnation_id),
            ..Default::default()
        };
        if src_bucket != bucket {
            dst_opts.add_bucket_lifecycle_lock_guard(&source_bucket_lifecycle_guard);
        }
        dst_opts.add_bucket_lifecycle_lock_guard(destination_bucket_lifecycle_guard);

        let mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &dst_opts)
            .await
            .map_err(ApiError::from)?;
        EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: None,
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key: sse_customer_key.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: 0,
            principal: None,
        }
        .validate_multipart_ssec(&mp_info.user_defined)?;

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;
        src_opts.version_id = src_version_id.clone();

        let h = build_ssec_read_headers(
            copy_source_sse_customer_algorithm.as_ref(),
            copy_source_sse_customer_key.as_ref(),
            copy_source_sse_customer_key_md5.as_ref(),
        );
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            expected_bucket_incarnation_id: Some(current_source_incarnation_id),
            ..Default::default()
        };
        if source_bucket_lifecycle_guard.is_lock_lost() {
            return Err(ApiError::from(StorageError::NamespaceLockQuorumUnavailable {
                mode: "copy_source_bucket_generation",
                bucket: src_bucket.clone(),
                object: src_key.clone(),
                required: 1,
                achieved: 0,
            })
            .into());
        }

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        let src_info = src_reader.object_info;

        // Same shape as CopyObject: the part copy reads the source plaintext, and the source
        // read resolves its material inside the object layer, which carries no request identity.
        authorize_sse_kms_object_read(copy_principal.as_ref(), &src_info.user_defined).await?;

        let src_stream = src_reader.stream;
        let resolved_src_version_id = src_info.version_id.map(|version_id| {
            if version_id == Uuid::nil() {
                "null".to_string()
            } else {
                version_id.to_string()
            }
        });

        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    return Err(s3_error!(PreconditionFailed));
                }
            } else {
                return Err(s3_error!(PreconditionFailed));
            }
        }

        if let Some(if_none_match) = copy_source_if_none_match
            && let Some(ref etag) = src_info.etag
            && let Some(strong_etag) = if_none_match.into_etag()
            && ETag::Strong(etag.clone()) == strong_etag
        {
            return Err(s3_error!(PreconditionFailed));
        }

        let (_start_offset, length) = if let Some(ref range_spec) = rs {
            // Copy-source ranges are expressed over the logical plaintext object.
            // Encrypted (and compressed) objects have a larger or smaller physical
            // representation, so validating against `size` rejects valid later parts.
            let validation_size = src_info.get_actual_size().unwrap_or(src_info.size);

            validate_copy_source_range_not_exceeds(range_spec, validation_size)?;

            range_spec
                .get_offset_length(validation_size)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRange, e.to_string()))?
        } else {
            (0, src_info.size)
        };

        let is_disk_compressed =
            rustfs_utils::http::contains_key_str(&mp_info.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        let actual_size = length;

        let mut write_plan = WritePlan::new();
        let mut reader = if is_disk_compressed {
            let algorithm = CompressionAlgorithm::default();
            let hrd = HashReader::from_stream(src_stream, length, actual_size, None, None, false).map_err(ApiError::from)?;
            write_plan = write_plan.with_compression(algorithm);
            hrd
        } else {
            HashReader::from_stream(src_stream, length, actual_size, None, None, false).map_err(ApiError::from)?
        };

        let server_side_encryption = mp_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|s| {
                ServerSideEncryption::from_str(s)
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid server-side encryption: {e}"))))
            })
            .transpose()?;
        let has_ssec = mp_info
            .user_defined
            .contains_key("x-amz-server-side-encryption-customer-algorithm");
        let ssekms_key_id = match server_side_encryption.as_ref() {
            Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS => mp_info
                .user_defined
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(|s| s.to_string()),
            _ => None,
        };
        let (requested_sse, requested_kms_key_id, dst_user_defined) = if has_ssec {
            let ssec_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &mp_info.user_defined,
                sse_customer_key: sse_customer_key.as_ref(),
                sse_customer_key_md5: sse_customer_key_md5.as_ref(),
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing SSE-C session material")))?;
            let ssec_write = match ssec_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(ssec_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(ssec_material.key_bytes, ssec_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(ssec_write);
            (
                Some(ssec_material.server_side_encryption),
                ssec_material.kms_key_id,
                mp_info.user_defined.clone(),
            )
        } else if let Some(server_side_encryption) = server_side_encryption {
            // Destination side of the part copy: reuses the session envelope authorized at
            // create-multipart-upload time. The source side is authorized above.
            let managed_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &mp_info.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
                principal: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                EncryptionKeyKind::Object => WriteEncryption::multipart_object_key(managed_material.key_bytes, part_id as u32),
                EncryptionKeyKind::Direct => {
                    WriteEncryption::multipart(managed_material.key_bytes, managed_material.base_nonce, part_id)
                }
            };
            write_plan = write_plan.with_encryption(managed_write);
            (Some(server_side_encryption), ssekms_key_id, mp_info.user_defined.clone())
        } else {
            (None, None, mp_info.user_defined.clone())
        };

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        if let Some(checksum_algorithm) = mp_info
            .user_defined
            .get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)
            .filter(|checksum_algorithm| !checksum_algorithm.is_empty())
        {
            let checksum_type = rustfs_rio::ChecksumType::from_string_with_obj_type(
                checksum_algorithm,
                mp_info
                    .user_defined
                    .get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE)
                    .map(String::as_str)
                    .unwrap_or_default(),
            );
            if !checksum_type.is_set() {
                return Err(ApiError::from(StorageError::other(format!(
                    "Invalid multipart checksum type: {checksum_algorithm}"
                )))
                .into());
            }
            reader.add_calculated_checksum(checksum_type).map_err(ApiError::from)?;
        }

        let mut reader = PutObjReader::new(reader);

        dst_opts.user_defined = dst_user_defined;

        let part_info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        let copy_checksums = reader.as_hash_reader().content_crc();
        let checksum_value = |checksum_type: rustfs_rio::ChecksumType| copy_checksums.get(&checksum_type.to_string()).cloned();

        let copy_part_result = CopyPartResult {
            checksum_crc32: checksum_value(rustfs_rio::ChecksumType::CRC32),
            checksum_crc32c: checksum_value(rustfs_rio::ChecksumType::CRC32C),
            checksum_sha1: checksum_value(rustfs_rio::ChecksumType::SHA1),
            checksum_sha256: checksum_value(rustfs_rio::ChecksumType::SHA256),
            checksum_crc64nvme: checksum_value(rustfs_rio::ChecksumType::CRC64_NVME),
            e_tag: part_info.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: part_info.last_mod.map(Timestamp::from),
            ..Default::default()
        };

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            copy_source_version_id: resolved_src_version_id,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Extensions, HeaderMap, Method, Uri, header::HeaderValue};
    use rustfs_filemeta::ObjectPartInfo;
    use rustfs_utils::http::{
        AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
    };
    use s3s::dto::{CompletedMultipartUpload, StorageClass};
    use std::{collections::HashMap, io::Cursor};
    use temp_env::async_with_vars;
    use tokio::io::AsyncReadExt;

    fn s3_op_total(op: S3Operation) -> u64 {
        rustfs_io_metrics::s3_op_metrics_snapshot()
            .into_iter()
            .find(|snapshot| snapshot.op == op.as_str())
            .map(|snapshot| snapshot.total)
            .unwrap_or_default()
    }

    fn build_request<T>(input: T, method: Method) -> S3Request<T> {
        S3Request {
            input,
            method,
            uri: Uri::from_static("/"),
            headers: HeaderMap::new(),
            extensions: Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn make_usecase() -> DefaultMultipartUsecase {
        DefaultMultipartUsecase::without_context()
    }

    #[test]
    fn session_compression_is_advertised_only_for_non_passthrough_compressible_uploads() {
        // (multipart_enabled, ciphertext_passthrough, disk_compressible, expected)
        let cases = [
            (true, false, false, false),
            (true, false, true, true),
            (true, true, false, false),
            (true, true, true, false),
            // The staged-rollout switch keeps multipart compression dark by
            // default regardless of the other gates.
            (false, false, true, false),
            (false, false, false, false),
            (false, true, true, false),
            (false, true, false, false),
        ];

        for (multipart_enabled, ciphertext_passthrough, disk_compressible, expected) in cases {
            assert_eq!(
                should_advertise_session_compression(multipart_enabled, ciphertext_passthrough, disk_compressible),
                expected,
                "multipart_enabled={multipart_enabled} ciphertext_passthrough={ciphertext_passthrough} disk_compressible={disk_compressible}"
            );
        }
    }

    #[test]
    fn quota_accounting_uses_logical_size_when_available() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "8192".to_string());
        let info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        assert_eq!(quota_accounting_object_size(&info, true).expect("logical size should resolve"), 8192);
        assert_eq!(quota_accounting_object_size(&info, false).expect("logical size should resolve"), 8192);

        let mut poisoned_metadata = HashMap::new();
        insert_str(&mut poisoned_metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut poisoned_metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1".to_string());
        let poisoned = ObjectInfo {
            size: 17,
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 4096,
                actual_size: 4096,
                ..Default::default()
            }]),
            user_defined: Arc::new(poisoned_metadata),
            ..Default::default()
        };
        assert_eq!(
            quota_accounting_object_size(&poisoned, true).expect("persisted part size must be charged"),
            4096
        );
    }

    #[test]
    fn quota_accounting_fails_closed_only_when_quota_is_configured() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "-1".to_string());
        let info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let err = quota_accounting_object_size(&info, true).expect_err("invalid logical size must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(quota_accounting_object_size(&info, false).expect("physical fallback should resolve"), 128);
    }

    #[test]
    fn test_build_complete_multipart_location_uses_forwarded_proto_and_encodes_key() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::HOST, HeaderValue::from_static("storage.example.com:9000"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));

        let location = build_complete_multipart_location(
            &headers,
            &Uri::from_static("/bucket/object?uploadId=1"),
            "bucket",
            "dir/file name.txt",
        );

        assert_eq!(location, "https://storage.example.com:9000/bucket/dir/file%20name.txt");
    }

    #[test]
    fn test_build_complete_multipart_location_falls_back_to_uri_authority_and_scheme() {
        let location = build_complete_multipart_location(
            &HeaderMap::new(),
            &"https://gateway.example.com:9443/complete".parse::<Uri>().unwrap(),
            "bucket",
            "object.txt",
        );

        assert_eq!(location, "https://gateway.example.com:9443/bucket/object.txt");
    }

    #[test]
    fn test_build_complete_multipart_location_returns_path_without_host() {
        let location = build_complete_multipart_location(&HeaderMap::new(), &Uri::from_static("/"), "bucket", "nested/object");

        assert_eq!(location, "/bucket/nested/object");
    }

    #[test]
    fn resolve_upload_part_size_uses_decoded_length_for_aws_chunked() {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("aws-chunked"));
        headers.insert(AMZ_DECODED_CONTENT_LENGTH, HeaderValue::from_static("5242880"));

        let size = resolve_upload_part_size(&headers, Some(5242962)).expect("decoded size should parse");

        assert_eq!(size, Some(5242880));
    }

    #[test]
    fn resolve_upload_part_size_preserves_regular_content_length() {
        let headers = HeaderMap::new();

        let size = resolve_upload_part_size(&headers, Some(5242880)).expect("regular size should parse");

        assert_eq!(size, Some(5242880));
    }

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
    async fn execute_abort_multipart_upload_returns_internal_error_when_store_uninitialized() {
        let input = AbortMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::DELETE);
        let before = s3_op_total(S3Operation::AbortMultipartUpload);

        let err = make_usecase().execute_abort_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(s3_op_total(S3Operation::AbortMultipartUpload), before + 1);
    }

    #[tokio::test]
    async fn execute_create_multipart_upload_rejects_invalid_storage_class() {
        let input = CreateMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .storage_class(Some(StorageClass::from("invalid".to_string())))
            .build()
            .unwrap();
        let req = build_request(input, Method::POST);

        let err = make_usecase().execute_create_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[test]
    fn create_multipart_upload_metadata_persists_dto_storage_class_without_raw_header() {
        let metadata = create_multipart_upload_metadata(
            None,
            &HeaderMap::new(),
            None,
            Some(&StorageClass::from_static("REDUCED_REDUNDANCY")),
        );

        assert_eq!(metadata.get(AMZ_STORAGE_CLASS), Some(&"REDUCED_REDUNDANCY".to_string()));
    }

    #[test]
    fn create_multipart_upload_metadata_keeps_user_and_system_namespaces_separate() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", HeaderValue::from_static("application/octet-stream"));
        headers.insert(AMZ_STORAGE_CLASS, HeaderValue::from_static("STANDARD"));
        let input_metadata = HashMap::from([
            ("content-type".to_string(), "user-content-type".to_string()),
            (AMZ_STORAGE_CLASS.to_string(), "user-storage-class".to_string()),
        ]);

        let metadata = create_multipart_upload_metadata(
            Some(input_metadata),
            &headers,
            Some("project=rustfs".to_string()),
            Some(&StorageClass::from_static("REDUCED_REDUNDANCY")),
        );

        assert_eq!(metadata.get("content-type"), Some(&"application/octet-stream".to_string()));
        assert_eq!(metadata.get("x-amz-meta-content-type"), Some(&"user-content-type".to_string()));
        assert_eq!(metadata.get(AMZ_STORAGE_CLASS), Some(&"REDUCED_REDUNDANCY".to_string()));
        assert_eq!(metadata.get("x-amz-meta-x-amz-storage-class"), Some(&"user-storage-class".to_string()));
        assert_eq!(metadata.get(AMZ_OBJECT_TAGGING), Some(&"project=rustfs".to_string()));
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_missing_parts_payload() {
        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::POST);

        let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_missing_parts_list() {
        use s3s::xml::Deserialize as _;

        let mut deserializer = s3s::xml::Deserializer::new(b"<CompleteMultipartUpload/>");
        let multipart_upload =
            CompletedMultipartUpload::deserialize(&mut deserializer).expect("empty multipart XML should decode");
        deserializer
            .expect_eof()
            .expect("empty multipart XML should be fully consumed");
        assert!(multipart_upload.parts.is_none());

        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .multipart_upload(Some(multipart_upload))
            .build()
            .expect("complete multipart input should build");
        let req = build_request(input, Method::POST);

        let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
            .await
            .expect_err("missing parts list must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn compressed_complete_records_logical_quota_usage_and_overwrite_delta() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("compressed-complete-quota", 16_384).await;
        let object = "object";

        let usecase = DefaultMultipartUsecase::from_global();
        let metadata_sys = usecase
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        let quota_checker = QuotaChecker::new(metadata_sys);

        for (actual_size, payload_byte) in [(8192_i64, 0x61), (4096_i64, 0x62)] {
            let mut create_opts = ObjectOptions::default();
            insert_str(&mut create_opts.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
            let upload = store
                .new_multipart_upload(&bucket, object, &create_opts)
                .await
                .expect("create compressed multipart upload");
            let payload = vec![payload_byte; 128];
            let mut part_reader = PutObjReader::new(
                HashReader::from_stream(Cursor::new(payload), 128, actual_size, None, None, false)
                    .expect("construct compressed part reader"),
            );
            let staged_part = store
                .put_object_part(&bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
                .await
                .expect("write compressed multipart part");
            let staged_etag = staged_part.etag.expect("staged compressed part should have an ETag");
            let input = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .upload_id(upload.upload_id)
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(to_s3s_etag(&staged_etag)),
                        ..Default::default()
                    }]),
                }))
                .build()
                .expect("complete multipart input should build");
            usecase
                .execute_complete_multipart_upload(build_request(input, Method::POST))
                .await
                .expect("compressed multipart completion should succeed");

            let quota = quota_checker
                .check_quota(&bucket, QuotaOperation::PutObject, 0)
                .await
                .expect("read live quota usage");
            assert_eq!(quota.current_usage, Some(actual_size as u64));
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn create_multipart_rejects_ciphertext_replication_before_parts_are_staged() {
        let (_store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("ciphertext-multipart-quota", 4096).await;
        let usecase = DefaultMultipartUsecase::from_global();
        let input = CreateMultipartUploadInput::builder()
            .bucket(bucket)
            .key("object".to_string())
            .build()
            .expect("create multipart request should build");
        let mut request = build_request(input, Method::POST);
        insert_header(&mut request.headers, SUFFIX_SOURCE_REPLICATION_REQUEST, "true");
        request
            .headers
            .insert(rustfs_utils::http::REPLICATION_SSEC_ALGORITHM_HEADER, HeaderValue::from_static("AES256"));
        request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });

        let err = usecase
            .execute_create_multipart_upload(request)
            .await
            .expect_err("quota-enabled ciphertext multipart replication should fail before upload creation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_completions_share_durable_bucket_quota_reservations() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-complete-quota", 6000).await;

        let usecase = DefaultMultipartUsecase::from_global();

        let mut inputs = Vec::new();
        for object in ["first", "second"] {
            let upload = store
                .new_multipart_upload(&bucket, object, &ObjectOptions::default())
                .await
                .expect("create concurrent multipart upload");
            let mut reader = PutObjReader::from_vec(vec![0x71; 4096]);
            let part = store
                .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
                .expect("stage concurrent multipart part");
            inputs.push(
                CompleteMultipartUploadInput::builder()
                    .bucket(bucket.clone())
                    .key(object.to_string())
                    .upload_id(upload.upload_id)
                    .multipart_upload(Some(CompletedMultipartUpload {
                        parts: Some(vec![CompletedPart {
                            part_number: Some(1),
                            e_tag: part.etag.map(|etag| to_s3s_etag(&etag)),
                            ..Default::default()
                        }]),
                    }))
                    .build()
                    .expect("build concurrent completion input"),
            );
        }

        let first_usecase = usecase.clone();
        let first = first_usecase.execute_complete_multipart_upload(build_request(inputs.remove(0), Method::POST));
        let second = usecase.execute_complete_multipart_upload(build_request(inputs.remove(0), Method::POST));
        let (first, second) = tokio::join!(first, second);

        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let denied = first.err().or_else(|| second.err()).expect("one completion must be denied");
        assert_eq!(denied.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn multipart_completion_rejects_rotated_quota_capability_before_rename() {
        use crate::app::storage_api::test::set_disk::{MultipartCommitBarrier, MultipartCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("rotated-proof-mpu-quota", 4096).await;
        let object = "object";
        let upload = store
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart upload");
        let mut reader = PutObjReader::from_vec(vec![0x78; 4096]);
        let part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("stage multipart part");
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::BeforeQuotaRename);
        let complete_store = Arc::clone(&store);
        let complete_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(
                    &complete_bucket,
                    object,
                    &upload_id,
                    vec![CompletePart {
                        part_num: 1,
                        etag: part.etag,
                        ..Default::default()
                    }],
                    &ObjectOptions::default(),
                )
                .await
        });
        barrier.wait_until_paused().await;
        assert!(
            crate::storage::storage_api::ecstore_notification::rotate_cross_pool_fence_fleet_proof_for_test(),
            "the gating environment must have a current fleet proof"
        );
        barrier.release();

        let err = complete
            .await
            .expect("completion task should not panic")
            .expect_err("a replaced fleet proof must fence multipart rename");
        assert!(matches!(
            err,
            StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_reservation",
                ..
            }
        ));
        store
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await
            .expect("proof rotation must preserve the multipart upload for retry");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_completion_has_zero_quota_growth() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("data-movement-mpu-quota", 0).await;
        let object = "object";
        let mut movement_opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        let upload = store
            .new_multipart_upload(&bucket, object, &movement_opts)
            .await
            .expect("create data-movement multipart upload");
        let mut reader = PutObjReader::from_vec(vec![0x7a; 4096]);
        let part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &movement_opts)
            .await
            .expect("stage data-movement multipart part");
        movement_opts.preserve_etag = Some("movement-etag".to_string());
        let completed = store
            .complete_multipart_upload(
                &bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: part.etag,
                    ..Default::default()
                }],
                &movement_opts,
            )
            .await
            .expect("moving an already-accounted multipart object between pools must have zero quota growth");
        assert_eq!(completed.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rejected_empty_parts_preserve_existing_object_and_staging() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;

        let bucket = format!("empty-complete-{}", Uuid::new_v4());
        let object = "existing-object";
        let existing_payload = b"existing object must survive";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multipart regression bucket");

        let mut existing_reader = PutObjReader::from_vec(existing_payload.to_vec());
        let existing_info = store
            .put_object(&bucket, object, &mut existing_reader, &ObjectOptions::default())
            .await
            .expect("write existing object");

        let upload = store
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart staging upload");
        let mut part_reader = PutObjReader::from_vec(b"staged part".to_vec());
        let staged_part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("write staged multipart part");

        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(upload.upload_id.clone())
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(Vec::new()) }))
            .build()
            .expect("complete multipart input should build");
        let err = DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(build_request(input, Method::POST))
            .await
            .expect_err("empty parts list must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        let current_info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("existing object should remain readable");
        assert_eq!(current_info.size, existing_info.size);
        assert_eq!(current_info.etag, existing_info.etag);

        let staged = store
            .list_object_parts(&bucket, object, &upload.upload_id, None, 1000, &ObjectOptions::default())
            .await
            .expect("multipart staging should remain available");
        assert_eq!(staged.parts.len(), 1);
        assert_eq!(staged.parts[0].part_num, 1);
        assert_eq!(staged.parts[0].etag, staged_part.etag);

        let staged_etag = staged_part.etag.expect("staged part should have an ETag");
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .upload_id(upload.upload_id)
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(to_s3s_etag(&staged_etag)),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("complete multipart input should build");
        let completed = DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(build_request(input, Method::POST))
            .await
            .expect("staged part should remain completable after empty request rejection");
        assert!(completed.output.e_tag.is_some());
        let completed_info = store
            .get_object_info(
                completed
                    .output
                    .bucket
                    .as_deref()
                    .expect("complete response should include bucket"),
                object,
                &ObjectOptions::default(),
            )
            .await
            .expect("completed object should be readable");
        assert_eq!(completed_info.size, b"staged part".len() as i64);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_allows_duplicate_part_numbers_by_using_last_occurrence() {
        let multipart_upload = CompletedMultipartUpload {
            parts: Some(vec![
                CompletedPart {
                    part_number: Some(1),
                    ..Default::default()
                },
                CompletedPart {
                    part_number: Some(1),
                    ..Default::default()
                },
            ]),
        };
        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .multipart_upload(Some(multipart_upload))
            .build()
            .unwrap();
        let req = build_request(input, Method::POST);

        let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
            .await
            .unwrap_err();
        assert_ne!(err.code(), &S3ErrorCode::InvalidPartOrder);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_out_of_order_parts() {
        let multipart_upload = CompletedMultipartUpload {
            parts: Some(vec![
                CompletedPart {
                    part_number: Some(2),
                    ..Default::default()
                },
                CompletedPart {
                    part_number: Some(1),
                    ..Default::default()
                },
            ]),
        };
        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .multipart_upload(Some(multipart_upload))
            .build()
            .unwrap();
        let req = build_request(input, Method::POST);

        let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidPartOrder);
    }

    #[test]
    fn normalize_complete_multipart_parts_keeps_last_duplicate_part() {
        let input = vec![
            CompletePart {
                part_num: 1,
                etag: Some("old".to_string()),
                ..Default::default()
            },
            CompletePart {
                part_num: 1,
                etag: Some("new".to_string()),
                ..Default::default()
            },
        ];

        let normalized = normalize_complete_multipart_parts(input).expect("normalization should succeed");
        assert_eq!(normalized.len(), 1);
        assert_eq!(normalized[0].part_num, 1);
        assert_eq!(normalized[0].etag.as_deref(), Some("new"));
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_object_lock_headers() {
        let multipart_upload = CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                ..Default::default()
            }]),
        };

        for (header_name, header_value) in [
            (AMZ_OBJECT_LOCK_MODE_LOWER, "GOVERNANCE"),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER, "2030-01-01T00:00:00Z"),
            (AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, "ON"),
            ("x-amz-bypass-governance-retention", "true"),
        ] {
            let input = CompleteMultipartUploadInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .multipart_upload(Some(multipart_upload.clone()))
                .build()
                .unwrap();
            let mut req = build_request(input, Method::POST);
            req.headers.insert(header_name, HeaderValue::from_str(header_value).unwrap());

            let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
                .await
                .unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest, "header {header_name} should be rejected");
        }
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_returns_internal_error_when_store_uninitialized() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_key_marker_before_store_lookup() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .prefix(Some("prefix/".to_string()))
            .key_marker(Some("other/key".to_string()))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("Invalid key marker"));
    }

    #[tokio::test]
    async fn execute_list_multipart_uploads_rejects_invalid_max_uploads_before_store_lookup() {
        let input = ListMultipartUploadsInput::builder()
            .bucket("bucket".to_string())
            .max_uploads(Some(0))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);
        let expected = "max-uploads must be between 1 and 1000";

        let err = make_usecase().execute_list_multipart_uploads(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some(expected));
    }

    #[tokio::test]
    async fn execute_list_parts_returns_internal_error_when_store_uninitialized() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_list_parts_rejects_negative_part_number_marker_before_store_lookup() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .part_number_marker(Some(-1))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("part-number-marker must be non-negative"));
    }

    #[tokio::test]
    async fn execute_list_parts_rejects_invalid_max_parts_before_store_lookup() {
        let input = ListPartsInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .max_parts(Some(1001))
            .build()
            .unwrap();
        let req = build_request(input, Method::GET);

        let err = make_usecase().execute_list_parts(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
        assert_eq!(err.message(), Some("max-parts must be between 1 and 1000"));
    }

    #[tokio::test]
    async fn execute_upload_part_copy_returns_internal_error_when_store_uninitialized() {
        let input = UploadPartCopyInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .copy_source(CopySource::Bucket {
                bucket: "src-bucket".into(),
                key: "src-object".into(),
                version_id: None,
            })
            .part_number(1)
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::PUT);

        let err = Box::pin(make_usecase().execute_upload_part_copy(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_upload_part_copy_rejects_invalid_part_number_before_store_lookup() {
        for part_number in [-1, 0, 10001] {
            let input = UploadPartCopyInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .copy_source(CopySource::Bucket {
                    bucket: "src-bucket".into(),
                    key: "src-object".into(),
                    version_id: None,
                })
                .part_number(part_number)
                .upload_id("upload-id".to_string())
                .build()
                .unwrap();
            let req = build_request(input, Method::PUT);

            let err = Box::pin(make_usecase().execute_upload_part_copy(req)).await.unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(err.message(), Some("partNumber must be between 1 and 10000"));
        }
    }

    #[test]
    fn test_validate_copy_source_range_not_exceeds_returns_invalid_range_when_range_exceeds() {
        use super::validate_copy_source_range_not_exceeds;

        let range_exceeds = HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: 21,
        };
        let err = validate_copy_source_range_not_exceeds(&range_exceeds, 5).unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRange);
        assert!(err.to_string().contains("not satisfiable"));
    }

    #[test]
    fn test_validate_copy_source_range_not_exceeds_ok_when_range_valid() {
        use super::validate_copy_source_range_not_exceeds;

        let range_valid = HTTPRangeSpec {
            is_suffix_length: false,
            start: 0,
            end: 4,
        };
        assert!(validate_copy_source_range_not_exceeds(&range_valid, 5).is_ok());
    }

    #[test]
    fn test_validate_copy_source_range_not_exceeds_ok_for_suffix_range() {
        use super::validate_copy_source_range_not_exceeds;

        let range_suffix = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };
        assert!(validate_copy_source_range_not_exceeds(&range_suffix, 5).is_ok());
    }

    #[tokio::test]
    async fn execute_upload_part_rejects_missing_body() {
        let input = UploadPartInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .part_number(1)
            .build()
            .unwrap();
        let req = build_request(input, Method::PUT);

        let err = make_usecase().execute_upload_part(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn execute_upload_part_rejects_invalid_part_number_before_body_lookup() {
        for part_number in [-1, 0, 10001] {
            let input = UploadPartInput::builder()
                .bucket("bucket".to_string())
                .key("object".to_string())
                .upload_id("upload-id".to_string())
                .part_number(part_number)
                .build()
                .unwrap();
            let req = build_request(input, Method::PUT);

            let err = make_usecase().execute_upload_part(req).await.unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(err.message(), Some("partNumber must be between 1 and 10000"));
        }
    }
}
