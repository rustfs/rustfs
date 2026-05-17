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

use crate::app::context::{AppContext, get_global_app_context};
use crate::app::object_usecase::{build_put_like_object_lock_metadata, validate_existing_object_lock_for_write};
use crate::capacity::record_capacity_write;
use crate::error::ApiError;
use crate::storage::access::has_bypass_governance_header;
use crate::storage::helper::OperationHelper;
use crate::storage::options::{
    copy_src_opts, extract_metadata_from_mime, get_complete_multipart_upload_opts, get_content_sha256_with_query, get_opts,
    parse_copy_source_range, put_opts, validate_archive_content_encoding,
};
use crate::storage::s3_api::multipart::{
    ListMultipartUploadsParams, build_list_multipart_uploads_output, build_list_parts_output,
    parse_list_multipart_uploads_params, parse_list_parts_params,
};
use crate::storage::sse::{
    build_ssec_read_headers, encryption_material_to_metadata, extract_ssekms_context_from_headers, map_get_object_reader_error,
    mark_encrypted_multipart_metadata,
};
use crate::storage::*;
use bytes::Bytes;
use futures::StreamExt;
use http::{HeaderMap, Uri};
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    lifecycle::{bucket_lifecycle_audit::LcEventSrc, bucket_lifecycle_ops::enqueue_transition_immediate},
    metadata_sys,
    quota::QuotaOperation,
    replication::{get_must_replicate_options, must_replicate, schedule_replication},
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::is_compressible;
use rustfs_ecstore::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::new_object_layer_fn;
#[cfg(test)]
use rustfs_ecstore::rio::{DecryptReader, EncryptReader, HardLimitReader, boxed_reader, wrap_reader};
use rustfs_ecstore::rio::{HashReader, WritePlan};
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{CompletePart, HTTPRangeSpec, MultipartUploadResult, ObjectIO, ObjectOptions, PutObjReader};
use rustfs_ecstore::store_api::{MultipartOperations, ObjectOperations};
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_s3_common::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::{
    AMZ_CHECKSUM_TYPE, get_source_scheme,
    headers::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING},
};
use s3s::dto::*;
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

fn has_complete_multipart_object_lock_headers(headers: &HeaderMap) -> bool {
    headers.contains_key(X_AMZ_OBJECT_LOCK_MODE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD)
        || has_bypass_governance_header(headers)
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
            context: get_global_app_context(),
        }
    }

    fn bucket_metadata_sys(&self) -> Option<Arc<RwLock<metadata_sys::BucketMetadataSys>>> {
        self.context.as_ref().and_then(|context| context.bucket_metadata().handle())
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket, key, upload_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts = &ObjectOptions::default();

        // Special handling for abort_multipart_upload: Per AWS S3 API specification, this operation
        // should return NoSuchUpload (404) when the upload_id doesn't exist, even if the format
        // appears invalid. This differs from other multipart operations (upload_part, list_parts,
        // complete_multipart_upload) which return InvalidArgument for malformed upload_ids.
        // The lenient validation matches AWS S3 behavior where format validation is relaxed for
        // abort operations to avoid leaking information about upload_id format requirements.
        match store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), upload_id.as_str(), opts)
            .await
        {
            Ok(_) => Ok(S3Response::new(AbortMultipartUploadOutput { ..Default::default() })),
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
    pub async fn execute_complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let mut helper = OperationHelper::new(
            &req,
            EventName::ObjectCreatedCompleteMultipartUpload,
            S3Operation::CompleteMultipartUpload,
        );
        let input = req.input;
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            if_match,
            if_none_match,
            ..
        } = input;

        if if_match.is_some() || if_none_match.is_some() {
            let Some(store) = new_object_layer_fn() else {
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

        let mut opts = get_complete_multipart_upload_opts(&req.headers).map_err(ApiError::from)?;
        let capacity_scope_token = Uuid::new_v4();
        opts.capacity_scope_token = Some(capacity_scope_token);

        let uploaded_parts_vec = multipart_upload
            .parts
            .unwrap_or_default()
            .into_iter()
            .map(CompletePart::from)
            .collect::<Vec<_>>();

        let uploaded_parts = normalize_complete_multipart_parts(uploaded_parts_vec)?;

        if has_complete_multipart_object_lock_headers(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "CompleteMultipartUpload does not accept object lock or governance bypass headers.".to_string(),
            ));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let current_opts = get_opts(&bucket, &key, None, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => validate_existing_object_lock_for_write(&existing_obj_info)?,
            Err(err) => {
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
            }
        }

        let multipart_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

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

        let obj_info = store
            .clone()
            .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, &opts)
            .await
            .map_err(ApiError::from)?;
        record_capacity_write(Some(capacity_scope_token)).await;

        // check quota after completing multipart upload
        if let Some(metadata_sys) = self.bucket_metadata_sys() {
            let quota_checker = QuotaChecker::new(metadata_sys);

            match quota_checker
                .check_quota(&bucket, QuotaOperation::PutObject, obj_info.size as u64)
                .await
            {
                Ok(check_result) => {
                    if !check_result.allowed {
                        // Quota exceeded, delete the completed object
                        let _ = store.delete_object(&bucket, &key, ObjectOptions::default()).await;
                        return Err(S3Error::with_message(
                            S3ErrorCode::InvalidRequest,
                            format!(
                                "Bucket quota exceeded. Current usage: {} bytes, limit: {} bytes",
                                check_result.current_usage.unwrap_or(0),
                                check_result.quota_limit.unwrap_or(0)
                            ),
                        ));
                    }
                    // Update quota tracking after successful multipart upload
                    rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;
                }
                Err(e) => {
                    warn!("Quota check failed for bucket {}: {}, allowing operation", bucket, e);
                }
            }
        }

        enqueue_transition_immediate(&obj_info, LcEventSrc::S3CompleteMultipartUpload).await;

        let raw_mpu_version = obj_info.version_id.map(|v| v.to_string());
        let mpu_version = if BucketVersioningSys::prefix_enabled(&bucket, &key).await {
            raw_mpu_version.clone()
        } else {
            None
        };
        let mpu_version_for_event = mpu_version.clone();
        let mut checksum_crc32 = input.checksum_crc32;
        let mut checksum_crc32c = input.checksum_crc32c;
        let mut checksum_sha1 = input.checksum_sha1;
        let mut checksum_sha256 = input.checksum_sha256;
        let mut checksum_crc64nvme = input.checksum_crc64nvme;
        let mut checksum_type = input.checksum_type;

        // checksum
        let (checksums, _is_multipart) = obj_info
            .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
            .map_err(ApiError::from)?;

        for (key, checksum) in checksums {
            if key == AMZ_CHECKSUM_TYPE {
                checksum_type = Some(ChecksumType::from(checksum));
                continue;
            }

            match rustfs_rio::ChecksumType::from_string(key.as_str()) {
                rustfs_rio::ChecksumType::CRC32 => checksum_crc32 = Some(checksum),
                rustfs_rio::ChecksumType::CRC32C => checksum_crc32c = Some(checksum),
                rustfs_rio::ChecksumType::SHA1 => checksum_sha1 = Some(checksum),
                rustfs_rio::ChecksumType::SHA256 => checksum_sha256 = Some(checksum),
                rustfs_rio::ChecksumType::CRC64_NVME => checksum_crc64nvme = Some(checksum),
                _ => (),
            }
        }

        let location = build_complete_multipart_location(&req.headers, &req.uri, &bucket, &key);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(location.clone()),
            server_side_encryption: server_side_encryption.clone(),
            ssekms_key_id: ssekms_key_id.clone(),
            checksum_crc32: checksum_crc32.clone(),
            checksum_crc32c: checksum_crc32c.clone(),
            checksum_sha1: checksum_sha1.clone(),
            checksum_sha256: checksum_sha256.clone(),
            checksum_crc64nvme: checksum_crc64nvme.clone(),
            checksum_type: checksum_type.clone(),
            version_id: mpu_version,
            ..Default::default()
        };
        let mt2 = HashMap::new();
        let replicate_options =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts.clone());
        let dsc = must_replicate(&bucket, &key, replicate_options).await;

        if dsc.replicate_any() {
            warn!("need multipart replication");
            schedule_replication(obj_info.clone(), store, dsc, ReplicationType::Object).await;
        }

        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let helper =
            OperationHelper::new(&req, EventName::ObjectCreatedCreateMultipartUpload, S3Operation::CreateMultipartUpload)
                .suppress_event();
        let CreateMultipartUploadInput {
            bucket,
            key,
            tagging,
            version_id,
            storage_class,
            server_side_encryption,
            sse_customer_algorithm,
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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_archive_content_encoding(
            &key,
            req.headers.get("content-type").and_then(|value| value.to_str().ok()),
            req.headers.get("content-encoding").and_then(|value| value.to_str().ok()),
        )?;

        let mut metadata = input_metadata.unwrap_or_default();
        extract_metadata_from_mime(&req.headers, &mut metadata);

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let has_explicit_object_lock_retention = object_lock_mode.is_some() || object_lock_retain_until_date.is_some();
        if let Some(object_lock_metadata) = build_put_like_object_lock_metadata(
            &bucket,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
        )
        .await?
        {
            metadata.extend(object_lock_metadata);
        }
        apply_bucket_default_lock_retention(&bucket, &mut metadata, has_explicit_object_lock_retention).await?;

        let encryption_request = PrepareEncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            ssekms_context: extract_ssekms_context_from_headers(&req.headers)?,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
        };

        let (effective_sse, effective_kms_key_id) = match sse_prepare_encryption(encryption_request).await? {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let ssekms_key_id = material.kms_key_id.clone();

                let mut encryption_metadata = encryption_material_to_metadata(&material);
                if material.key_kind == crate::storage::sse::EncryptionKeyKind::Object {
                    mark_encrypted_multipart_metadata(&mut encryption_metadata);
                }
                metadata.extend(encryption_metadata);

                (server_side_encryption, ssekms_key_id)
            }
            None => (None, None),
        };

        if is_compressible(&req.headers, &key) {
            rustfs_utils::http::insert_str(
                &mut metadata,
                rustfs_utils::http::SUFFIX_COMPRESSION,
                rustfs_ecstore::rio::compression_metadata_value(CompressionAlgorithm::default()),
            );
        }

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

        let current_opts: ObjectOptions = get_opts(&bucket, &key, opts.version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        match store.get_object_info(&bucket, &key, &current_opts).await {
            Ok(existing_obj_info) => validate_existing_object_lock_for_write(&existing_obj_info)?,
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
    pub async fn execute_upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
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

        let part_id = part_number as usize;

        let mut size = content_length;
        let mut body_stream = body.ok_or_else(|| s3_error!(IncompleteBody))?;

        if size.is_none() {
            if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH)
                && let Some(x) = atoi::atoi::<i64>(val.as_bytes())
            {
                size = Some(x);
            }

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
        }

        // Get multipart info early to check if managed encryption will be applied
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts = ObjectOptions::default();
        let fi = store
            .get_multipart_info(&bucket, &key, &upload_id, &opts)
            .await
            .map_err(ApiError::from)?;

        let mut size = size.ok_or_else(|| s3_error!(UnexpectedContent))?;

        // Apply adaptive buffer sizing based on part size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on part size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body_stream.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let is_compressible = rustfs_utils::http::contains_key_str(&fi.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

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
        let mut reader = if is_compressible {
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

        let has_ssec = sse_customer_algorithm.is_some();
        // When SSE-C headers are present, skip managed-encryption metadata to avoid
        // false conflict: the bucket default SSE config stored in multipart metadata
        // should not block a legitimate SSE-C upload part.
        let (server_side_encryption, ssekms_key_id) = if has_ssec {
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
        }
        .check_upload_part_customer_key_md5(&fi.user_defined, sse_customer_key_md5.clone())?;
        let (requested_sse, requested_kms_key_id) = if has_ssec {
            let encryption_request = EncryptionRequest {
                bucket: &bucket,
                key: &key,
                server_side_encryption,
                ssekms_key_id,
                ssekms_context: None,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key,
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
            };

            match sse_encryption(encryption_request).await? {
                Some(material) => {
                    let requested_sse = Some(material.server_side_encryption.clone());
                    let requested_kms_key_id = material.kms_key_id.clone();
                    write_plan = write_plan.with_encryption(material.write_encryption(Some(part_id)));
                    (requested_sse, requested_kms_key_id)
                }
                None => (None, None),
            }
        } else if let Some(server_side_encryption) = server_side_encryption {
            let managed_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &fi.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                crate::storage::sse::EncryptionKeyKind::Object => {
                    rustfs_ecstore::rio::WriteEncryption::multipart_object_key(managed_material.key_bytes, part_id as u32)
                }
                crate::storage::sse::EncryptionKeyKind::Direct => rustfs_ecstore::rio::WriteEncryption::multipart(
                    managed_material.key_bytes,
                    managed_material.base_nonce,
                    part_id,
                ),
            };
            write_plan = write_plan.with_encryption(managed_write);
            (Some(server_side_encryption), ssekms_key_id)
        } else {
            (None, None)
        };

        reader = write_plan.apply(reader, actual_size).map_err(ApiError::from)?;

        let mut reader = PutObjReader::new(reader);

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

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let result = store
            .list_multipart_uploads(&bucket, &prefix, delimiter, key_marker, upload_id_marker, max_uploads)
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(build_list_multipart_uploads_output(bucket, prefix, result)))
    }

    pub async fn execute_list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        let params = parse_list_parts_params(part_number_marker, max_parts)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let res = store
            .list_object_parts(
                &bucket,
                &key,
                &upload_id,
                params.part_number_marker,
                params.max_parts,
                &ObjectOptions::default(),
            )
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(build_list_parts_output(res)))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
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

        let part_id = part_number as usize;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

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
            ..Default::default()
        };

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        let src_info = src_reader.object_info;

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
            let validation_size = match src_info.is_compressed_ok() {
                Ok((_, true)) => src_info.get_actual_size().unwrap_or(src_info.size),
                _ => src_info.size,
            };

            validate_copy_source_range_not_exceeds(range_spec, validation_size)?;

            range_spec
                .get_offset_length(validation_size)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRange, e.to_string()))?
        } else {
            (0, src_info.size)
        };

        let h = build_ssec_read_headers(
            copy_source_sse_customer_algorithm.as_ref(),
            copy_source_sse_customer_key.as_ref(),
            copy_source_sse_customer_key_md5.as_ref(),
        );
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(map_get_object_reader_error)?;
        let src_stream = src_reader.stream;

        let is_compressible = rustfs_utils::http::contains_key_str(&mp_info.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        let actual_size = length;

        let mut write_plan = WritePlan::new();
        let mut reader = if is_compressible {
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
        let has_ssec = sse_customer_algorithm.is_some();
        let ssekms_key_id = match server_side_encryption.as_ref() {
            Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS => mp_info
                .user_defined
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(|s| s.to_string()),
            _ => None,
        };
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
        }
        .check_upload_part_customer_key_md5(&mp_info.user_defined, sse_customer_key_md5.clone())?;

        let (requested_sse, requested_kms_key_id, dst_user_defined) = if has_ssec {
            let encryption_request = EncryptionRequest {
                bucket: &bucket,
                key: &key,
                server_side_encryption,
                ssekms_key_id,
                ssekms_context: None,
                sse_customer_algorithm: sse_customer_algorithm.clone(),
                sse_customer_key,
                sse_customer_key_md5: sse_customer_key_md5.clone(),
                content_size: actual_size,
            };

            match sse_encryption(encryption_request).await? {
                Some(material) => {
                    let requested_sse = Some(material.server_side_encryption.clone());
                    let requested_kms_key_id = material.kms_key_id.clone();
                    write_plan = write_plan.with_encryption(material.write_encryption(Some(part_id)));
                    (requested_sse, requested_kms_key_id, mp_info.user_defined.clone())
                }
                None => (None, None, mp_info.user_defined.clone()),
            }
        } else if let Some(server_side_encryption) = server_side_encryption {
            let managed_material = sse_decryption(DecryptionRequest {
                bucket: &bucket,
                key: &key,
                metadata: &mp_info.user_defined,
                sse_customer_key: None,
                sse_customer_key_md5: None,
            })
            .await?
            .ok_or_else(|| ApiError::from(StorageError::other("Missing managed SSE session material")))?;
            let managed_write = match managed_material.key_kind {
                crate::storage::sse::EncryptionKeyKind::Object => {
                    rustfs_ecstore::rio::WriteEncryption::multipart_object_key(managed_material.key_bytes, part_id as u32)
                }
                crate::storage::sse::EncryptionKeyKind::Direct => rustfs_ecstore::rio::WriteEncryption::multipart(
                    managed_material.key_bytes,
                    managed_material.base_nonce,
                    part_id,
                ),
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

        let dst_opts = ObjectOptions {
            user_defined: dst_user_defined,
            ..Default::default()
        };

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
        };

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            copy_source_version_id: src_version_id,
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
    use std::{collections::HashMap, io::Cursor};
    use tokio::io::AsyncReadExt;

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
        let prepare_request = PrepareEncryptionRequest {
            bucket: "bucket",
            key: "object",
            server_side_encryption: Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
            ssekms_key_id: None,
            ssekms_context: None,
            sse_customer_algorithm: None,
            sse_customer_key_md5: None,
        };
        let session_material = sse_prepare_encryption(prepare_request)
            .await
            .expect("prepare multipart encryption")
            .expect("managed multipart session material");
        let mut session_metadata = encryption_material_to_metadata(&session_material);
        mark_encrypted_multipart_metadata(&mut session_metadata);

        let part_one_plaintext = vec![0x31; rustfs_rio::DEFAULT_ENCRYPTION_BLOCK_SIZE + 23];
        let part_two_plaintext = vec![0x32; rustfs_rio::DEFAULT_ENCRYPTION_BLOCK_SIZE * 2 + 7];

        let part_one_material = sse_decryption(DecryptionRequest {
            bucket: "bucket",
            key: "object",
            metadata: &session_metadata,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        })
        .await
        .expect("decrypt session one")
        .expect("part one material");
        let mut encrypted_one = Vec::new();
        #[cfg(feature = "rio-v2")]
        let mut part_one_reader = match part_one_material.key_kind {
            crate::storage::sse::EncryptionKeyKind::Object => EncryptReader::new_multipart_with_object_key(
                Cursor::new(part_one_plaintext.clone()),
                part_one_material.key_bytes,
                1,
            ),
            crate::storage::sse::EncryptionKeyKind::Direct => EncryptReader::new_multipart(
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
        })
        .await
        .expect("decrypt session two")
        .expect("part two material");
        let mut encrypted_two = Vec::new();
        #[cfg(feature = "rio-v2")]
        let mut part_two_reader = match part_two_material.key_kind {
            crate::storage::sse::EncryptionKeyKind::Object => EncryptReader::new_multipart_with_object_key(
                Cursor::new(part_two_plaintext.clone()),
                part_two_material.key_bytes,
                2,
            ),
            crate::storage::sse::EncryptionKeyKind::Direct => EncryptReader::new_multipart(
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

        if session_material.key_kind == crate::storage::sse::EncryptionKeyKind::Object {
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
        })
        .await
        .expect("decrypt multipart")
        .expect("managed decryption material");

        let plaintext_size = multipart_plaintext_size(&parts, -1);
        #[cfg(feature = "rio-v2")]
        let decrypted_stream = match decryption_material.key_kind {
            crate::storage::sse::EncryptionKeyKind::Object => boxed_reader(DecryptReader::new_multipart_with_object_key(
                wrap_reader(Cursor::new(encrypted_stream)),
                decryption_material.key_bytes,
                multipart_part_numbers(&parts),
            )),
            crate::storage::sse::EncryptionKeyKind::Direct => boxed_reader(DecryptReader::new_multipart(
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
    }

    #[tokio::test]
    #[ignore = "requires isolated global object layer state"]
    async fn execute_abort_multipart_upload_returns_internal_error_when_store_uninitialized() {
        let input = AbortMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let req = build_request(input, Method::DELETE);

        let err = make_usecase().execute_abort_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
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
    #[ignore = "requires isolated global object layer state"]
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
        assert_eq!(err.code(), &S3ErrorCode::NotImplemented);
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
    #[ignore = "requires isolated global object layer state"]
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
    #[ignore = "requires isolated global object layer state"]
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
}
