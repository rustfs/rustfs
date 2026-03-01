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
use crate::error::ApiError;
use crate::storage::concurrency::get_concurrency_manager;
use crate::storage::ecfs::RUSTFS_OWNER;
use crate::storage::entity;
use crate::storage::helper::OperationHelper;
use crate::storage::options::{
    copy_src_opts, extract_metadata, get_complete_multipart_upload_opts, get_content_sha256, parse_copy_source_range, put_opts,
};
use crate::storage::*;
use bytes::Bytes;
use futures::StreamExt;
use rustfs_config::RUSTFS_REGION;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    metadata_sys,
    quota::QuotaOperation,
    replication::{get_must_replicate_options, must_replicate, schedule_replication},
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::is_compressible;
use rustfs_ecstore::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::{MAX_PARTS_COUNT, is_valid_storage_class};
use rustfs_ecstore::store_api::{CompletePart, HTTPRangeSpec, MultipartUploadResult, ObjectIO, ObjectOptions, PutObjReader};
use rustfs_ecstore::store_api::{MultipartOperations, ObjectOperations};
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_rio::{CompressReader, HashReader, Reader, WarpReader};
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::{
    AMZ_CHECKSUM_TYPE,
    headers::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER},
};
use s3s::dto::*;
use s3s::region::Region;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::io::StreamReader;
use tracing::{info, instrument, warn};

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

    fn global_region(&self) -> Option<Region> {
        self.context.as_ref().and_then(|context| context.region().get())
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper =
            OperationHelper::new(&req, EventName::ObjectCreatedCompleteMultipartUpload, "s3:CompleteMultipartUpload");
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

        let opts = &get_complete_multipart_upload_opts(&req.headers).map_err(ApiError::from)?;

        let uploaded_parts_vec = multipart_upload
            .parts
            .unwrap_or_default()
            .into_iter()
            .map(CompletePart::from)
            .collect::<Vec<_>>();

        // is part number sorted?
        if !uploaded_parts_vec.is_sorted_by_key(|p| p.part_num) {
            return Err(s3_error!(InvalidPart, "Part numbers must be sorted"));
        }

        // Handle duplicate part numbers: according to S3 specification, when the same part number
        // is uploaded multiple times, the last uploaded part (in the list order) should be used.
        // This can happen in concurrent upload scenarios where a part is re-uploaded before completion.
        // We deduplicate by keeping the last occurrence of each part number using a HashMap.
        let mut part_map: HashMap<usize, CompletePart> = HashMap::new();
        for part in uploaded_parts_vec {
            part_map.insert(part.part_num, part);
        }

        // Reconstruct the parts list in sorted order, keeping only the last occurrence of each part number
        let mut uploaded_parts: Vec<CompletePart> = part_map.into_values().collect();
        uploaded_parts.sort_by_key(|p| p.part_num);

        // TODO: check object lock

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // TDD: Get multipart info to extract encryption configuration before completing
        info!(
            "TDD: Attempting to get multipart info for bucket={}, key={}, upload_id={}",
            bucket, key, upload_id
        );

        let multipart_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        info!("TDD: Got multipart info successfully");
        info!("TDD: Multipart info metadata: {:?}", multipart_info.user_defined);

        // TDD: Extract encryption information from multipart upload metadata
        let server_side_encryption = multipart_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|s| ServerSideEncryption::from(s.clone()));
        info!(
            "TDD: Raw encryption from metadata: {:?} -> parsed: {:?}",
            multipart_info.user_defined.get("x-amz-server-side-encryption"),
            server_side_encryption
        );

        let ssekms_key_id = multipart_info
            .user_defined
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .cloned();

        info!(
            "TDD: Extracted encryption info - SSE: {:?}, KMS Key: {:?}",
            server_side_encryption, ssekms_key_id
        );

        let obj_info = store
            .clone()
            .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, opts)
            .await
            .map_err(ApiError::from)?;

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

        // Invalidate cache for the completed multipart object
        let manager = get_concurrency_manager();
        let mpu_bucket = bucket.clone();
        let mpu_key = key.clone();
        let mpu_version = obj_info.version_id.map(|v| v.to_string());
        let mpu_version_clone = mpu_version.clone();
        let mpu_version_for_event = mpu_version.clone();
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&mpu_bucket, &mpu_key, mpu_version_clone.as_deref())
                .await;
        });

        info!(
            "TDD: Creating output with SSE: {:?}, KMS Key: {:?}",
            server_side_encryption, ssekms_key_id
        );

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

        let region = self
            .global_region()
            .map(|region| region.to_string())
            .unwrap_or_else(|| RUSTFS_REGION.to_string());
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(region.clone()),
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
        let helper_output = entity::CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(region),
            server_side_encryption,
            ssekms_key_id,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            ..Default::default()
        };
        info!(
            "TDD: Created output: SSE={:?}, KMS={:?}",
            output.server_side_encryption, output.ssekms_key_id
        );

        let mt2 = HashMap::new();
        let replicate_options =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts.clone());

        let dsc = must_replicate(&bucket, &key, replicate_options).await;

        if dsc.replicate_any() {
            warn!("need multipart replication");
            schedule_replication(obj_info.clone(), store, dsc, ReplicationType::Object).await;
        }
        info!(
            "TDD: About to return S3Response with output: SSE={:?}, KMS={:?}",
            output.server_side_encryption, output.ssekms_key_id
        );

        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }

        let helper_result = Ok(S3Response::new(helper_output));
        let _ = helper.complete(&helper_result);
        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, "s3:CreateMultipartUpload");
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
            ..
        } = req.input.clone();

        // Validate storage class if provided
        if let Some(ref storage_class) = storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut metadata = extract_metadata(&req.headers);

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        let encryption_request = PrepareEncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
        };

        let (effective_sse, effective_kms_key_id) = match sse_prepare_encryption(encryption_request).await? {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let ssekms_key_id = material.kms_key_id.clone();

                metadata.extend(material.metadata);

                (server_side_encryption, ssekms_key_id)
            }
            None => (None, None),
        };

        if is_compressible(&req.headers, &key) {
            metadata.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                CompressionAlgorithm::default().to_string(),
            );
        }

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

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
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
        let mut fi = store
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

        let is_compressible = fi
            .user_defined
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}compression").as_str());

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let actual_size = size;

        let mut md5hex = if let Some(base64_md5) = input.content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256(&req.headers);

        if is_compressible {
            let mut hrd = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(err).into());
            }

            let compress_reader = CompressReader::new(hrd, CompressionAlgorithm::default());
            reader = Box::new(compress_reader);
            size = HashReader::SIZE_PRESERVE_LAYER;
            md5hex = None;
            sha256hex = None;
        }

        let mut reader = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

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
            let key_id = fi
                .user_defined
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .map(|s| s.to_string());
            (sse, key_id)
        };
        let part_key = fi.user_defined.get("x-rustfs-encryption-key").cloned();
        let part_nonce = fi.user_defined.get("x-rustfs-encryption-iv").cloned();
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: Some(part_id),
            part_key,
            part_nonce,
        };

        encryption_request.check_upload_part_customer_key_md5(&fi.user_defined, sse_customer_key_md5.clone())?;

        let (requested_sse, requested_kms_key_id) = match sse_encryption(encryption_request).await? {
            Some(material) => {
                let requested_sse = Some(material.server_side_encryption.clone());
                let requested_kms_key_id = material.kms_key_id.clone();

                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                fi.user_defined.extend(material.metadata);

                (requested_sse, requested_kms_key_id)
            }
            None => (None, None),
        };

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
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let ListMultipartUploadsInput {
            bucket,
            prefix,
            delimiter,
            key_marker,
            upload_id_marker,
            max_uploads,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let prefix = prefix.unwrap_or_default();
        let max_uploads = max_uploads.map(|x| x as usize).unwrap_or(MAX_PARTS_COUNT);

        if let Some(key_marker) = &key_marker
            && !key_marker.starts_with(prefix.as_str())
        {
            return Err(s3_error!(NotImplemented, "Invalid key marker"));
        }

        let result = store
            .list_multipart_uploads(&bucket, &prefix, delimiter, key_marker, upload_id_marker, max_uploads)
            .await
            .map_err(ApiError::from)?;

        let output = ListMultipartUploadsOutput {
            bucket: Some(bucket),
            prefix: Some(prefix),
            delimiter: result.delimiter,
            key_marker: result.key_marker,
            upload_id_marker: result.upload_id_marker,
            max_uploads: Some(result.max_uploads as i32),
            is_truncated: Some(result.is_truncated),
            uploads: Some(
                result
                    .uploads
                    .into_iter()
                    .map(|u| MultipartUpload {
                        key: Some(u.object),
                        upload_id: Some(u.upload_id),
                        initiated: u.initiated.map(Timestamp::from),
                        ..Default::default()
                    })
                    .collect(),
            ),
            common_prefixes: Some(
                result
                    .common_prefixes
                    .into_iter()
                    .map(|c| CommonPrefix { prefix: Some(c) })
                    .collect(),
            ),
            ..Default::default()
        };

        Ok(S3Response::new(output))
    }

    pub async fn execute_list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let part_number_marker = part_number_marker.map(|x| x as usize);
        let max_parts = match max_parts {
            Some(parts) => {
                if !(1..=1000).contains(&parts) {
                    return Err(s3_error!(InvalidArgument, "max-parts must be between 1 and 1000"));
                }
                parts as usize
            }
            None => 1000,
        };

        let res = store
            .list_object_parts(&bucket, &key, &upload_id, part_number_marker, max_parts, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let output = ListPartsOutput {
            bucket: Some(res.bucket),
            key: Some(res.object),
            upload_id: Some(res.upload_id),
            parts: Some(
                res.parts
                    .into_iter()
                    .map(|p| Part {
                        e_tag: p.etag.map(|etag| to_s3s_etag(&etag)),
                        last_modified: p.last_mod.map(Timestamp::from),
                        part_number: Some(p.part_num as i32),
                        size: Some(p.size as i64),
                        ..Default::default()
                    })
                    .collect(),
            ),
            owner: Some(RUSTFS_OWNER.to_owned()),
            initiator: Some(Initiator {
                id: RUSTFS_OWNER.id.clone(),
                display_name: RUSTFS_OWNER.display_name.clone(),
            }),
            is_truncated: Some(res.is_truncated),
            next_part_number_marker: res.next_part_number_marker.try_into().ok(),
            max_parts: res.max_parts.try_into().ok(),
            part_number_marker: res.part_number_marker.try_into().ok(),
            storage_class: if res.storage_class.is_empty() {
                None
            } else {
                Some(res.storage_class.into())
            },
            ..Default::default()
        };
        Ok(S3Response::new(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            ..
        } = req.input;

        let (src_bucket, src_key, src_version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
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

        let mut mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;
        src_opts.version_id = src_version_id.clone();

        let h = http::HeaderMap::new();
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        let mut src_info = src_reader.object_info;

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

        let h = http::HeaderMap::new();
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(ApiError::from)?;
        let src_stream = src_reader.stream;

        let is_compressible = mp_info
            .user_defined
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}compression").as_str());

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(src_stream));

        let src_decryption_request = DecryptionRequest {
            bucket: &src_bucket,
            key: &src_key,
            metadata: &src_info.user_defined,
            sse_customer_key: copy_source_sse_customer_key.as_ref(),
            sse_customer_key_md5: copy_source_sse_customer_key_md5.as_ref(),
            part_number: None,
            parts: &src_info.parts,
        };

        if let Some(material) = sse_decryption(src_decryption_request).await? {
            reader = material.wrap_single_reader(reader);
            if let Some(original) = material.original_size {
                src_info.actual_size = original;
            }
        }

        let actual_size = length;
        let mut size = length;

        if is_compressible {
            let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;
            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            size = HashReader::SIZE_PRESERVE_LAYER;
        }

        let mut reader = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;

        let server_side_encryption = mp_info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|s| {
                ServerSideEncryption::from_str(s)
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid server-side encryption: {e}"))))
            })
            .transpose()?;
        let ssekms_key_id = mp_info
            .user_defined
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .map(|s| s.to_string());
        let part_key = mp_info.user_defined.get("x-rustfs-encryption-key").cloned();
        let part_nonce = mp_info.user_defined.get("x-rustfs-encryption-iv").cloned();
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: Some(part_id),
            part_key,
            part_nonce,
        };

        encryption_request.check_upload_part_customer_key_md5(&mp_info.user_defined, sse_customer_key_md5.clone())?;

        let (requested_sse, requested_kms_key_id) = match sse_encryption(encryption_request).await? {
            Some(material) => {
                let requested_sse = Some(material.server_side_encryption.clone());
                let requested_kms_key_id = material.kms_key_id.clone();

                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                mp_info.user_defined.extend(material.metadata);

                (requested_sse, requested_kms_key_id)
            }
            None => (None, None),
        };

        let mut reader = PutObjReader::new(reader);

        let dst_opts = ObjectOptions {
            user_defined: mp_info.user_defined.clone(),
            ..Default::default()
        };

        let part_info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        let copy_part_result = CopyPartResult {
            e_tag: part_info.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: part_info.last_mod.map(Timestamp::from),
            ..Default::default()
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
    use http::{Extensions, HeaderMap, Method, Uri};

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

    #[tokio::test]
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

        let err = make_usecase().execute_complete_multipart_upload(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
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

        let err = make_usecase().execute_upload_part_copy(req).await.unwrap_err();
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
