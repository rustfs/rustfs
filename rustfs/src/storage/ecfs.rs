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

use crate::auth::get_condition_values;
use crate::config::workload_profiles::get_global_buffer_config;
use crate::error::ApiError;
use crate::server::RemoteAddr;
use crate::storage::concurrency::{
    CachedGetObject, ConcurrencyManager, GetObjectGuard, get_concurrency_aware_buffer_size, get_concurrency_manager,
};
use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use crate::storage::helper::OperationHelper;
use crate::storage::options::{filter_object_metadata, get_content_sha256};
use crate::storage::readers::InMemoryAsyncReader;
use crate::storage::s3_api::acl::{build_get_bucket_acl_output, build_get_object_acl_output};
use crate::storage::s3_api::bucket::{
    build_list_buckets_output, build_list_object_versions_output, build_list_objects_output, build_list_objects_v2_output,
    parse_list_object_versions_params, parse_list_objects_v2_params,
};
use crate::storage::s3_api::multipart::{
    build_list_multipart_uploads_output, build_list_parts_output, parse_list_multipart_uploads_params, parse_list_parts_params,
};
use crate::storage::s3_api::response::{
    access_denied_error, map_abort_multipart_upload_error, not_initialized_error, s3_response,
};
use crate::storage::sse::{
    DecryptionRequest, EncryptionRequest, PrepareEncryptionRequest, check_encryption_metadata, sse_decryption, sse_encryption,
    sse_prepare_encryption, strip_managed_encryption_metadata,
};
use crate::storage::{
    access::{ReqInfo, authorize_request, has_bypass_governance_header},
    ecfs_extend::RFC1123,
    options::{
        copy_dst_opts, copy_src_opts, del_opts, extract_metadata, get_complete_multipart_upload_opts, get_opts,
        parse_copy_source_range, put_opts,
    },
};
use crate::storage::{
    check_preconditions, get_buffer_size_opt_in, get_validated_store, has_replication_rules, parse_object_lock_legal_hold,
    parse_object_lock_retention, process_lambda_configurations, process_queue_configurations, process_topic_configurations,
    validate_bucket_object_lock_enabled, validate_list_object_unordered_with_delimiter, validate_object_key,
    wrap_response_with_cors,
};
use crate::storage::{entity, parse_part_number_i32_to_usize};
// base64 imports moved to sse module
use bytes::Bytes;
use datafusion::arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::WriterBuilder as JsonWriterBuilder, json::writer::JsonArray,
};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use metrics::{counter, histogram};
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::{
    bucket::{
        lifecycle::{
            bucket_lifecycle_ops::{RestoreRequestOps, post_restore_opts, validate_transition_tier},
            lifecycle::{self, Lifecycle, TransitionOptions},
        },
        metadata::{
            BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_NOTIFICATION_CONFIG, BUCKET_POLICY_CONFIG,
            BUCKET_REPLICATION_CONFIG, BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG,
        },
        metadata_sys,
        metadata_sys::get_replication_config,
        object_lock::objectlock_sys::{BucketObjectLockSys, check_object_lock_for_deletion, check_retention_for_modification},
        policy_sys::PolicySys,
        quota::QuotaOperation,
        replication::{
            DeletedObjectReplicationInfo, ObjectOpts, ReplicationConfigurationExt, check_replicate_delete,
            get_must_replicate_options, must_replicate, schedule_replication, schedule_replication_delete,
        },
        tagging::{decode_tags, encode_tags},
        utils::serialize,
        versioning::VersioningApi,
        versioning_sys::BucketVersioningSys,
    },
    client::object_api_utils::to_s3s_etag,
    compress::{MIN_COMPRESSIBLE_SIZE, is_compressible},
    disk::{error::DiskError, error_reduce::is_all_buckets_not_found},
    error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    set_disk::is_valid_storage_class,
    store_api::{
        BucketOptions,
        CompletePart,
        DeleteBucketOptions,
        HTTPRangeSpec,
        MakeBucketOptions,
        MultipartUploadResult,
        ObjectIO,
        ObjectInfo,
        ObjectOptions,
        ObjectToDelete,
        PutObjReader,
        StorageAPI,
        // RESERVED_METADATA_PREFIX,
    },
};
use rustfs_filemeta::REPLICATE_INCOMING_DELETE;
use rustfs_filemeta::{ReplicationStatusType, ReplicationType, VersionPurgeStatusType};
use rustfs_filemeta::{RestoreStatusOps, parse_restore_obj_status};
// KMS imports moved to sse module
use rustfs_notify::{EventArgsBuilder, notifier_global};
use rustfs_policy::policy::{
    action::{Action, S3Action},
    {BucketPolicy, BucketPolicyArgs, Validator},
};
use rustfs_rio::{CompressReader, EtagReader, HashReader, Reader, WarpReader};
use rustfs_s3select_api::{
    object_store::bytes_stream,
    query::{Context, Query},
};
use rustfs_s3select_query::get_global_db;
use rustfs_targets::{
    EventName,
    arn::{ARN, TargetIDError},
};
use rustfs_utils::http::RUSTFS_FORCE_DELETE;
use rustfs_utils::string::parse_bool;
use rustfs_utils::{
    CompressionAlgorithm, extract_params_header, extract_resp_elements, get_request_host, get_request_port,
    get_request_user_agent,
    http::{
        AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE,
        headers::{
            AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE,
            AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
            AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE, AMZ_TAG_COUNT, RESERVED_METADATA_PREFIX,
            RESERVED_METADATA_PREFIX_LOWER,
        },
    },
    path::{is_dir_object, path_join_buf},
};
use rustfs_zip::CompressionFormat;
use s3s::header::{X_AMZ_RESTORE, X_AMZ_RESTORE_OUTPUT_PATH};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use std::convert::Infallible;
use std::ops::Add;
use std::{collections::HashMap, fmt::Debug, path::Path, str::FromStr, sync::Arc};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_tar::Archive;
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

macro_rules! try_ {
    ($result:expr) => {
        match $result {
            Ok(val) => val,
            Err(err) => {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)));
            }
        }
    };
}

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ListObjectUnorderedQuery {
    #[serde(rename = "allow-unordered")]
    pub(crate) allow_unordered: Option<String>,
}

impl FS {
    pub fn new() -> Self {
        // let store: ECStore = ECStore::new(address, endpoint_pools).await?;
        Self {}
    }

    #[instrument(level = "debug", skip(self, req))]
    #[allow(dead_code)]
    async fn put_object_extract(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, "s3:PutObject").suppress_event();
        let input = req.input;

        let PutObjectInput {
            body,
            bucket,
            key,
            version_id,
            content_length,
            content_md5,
            ..
        } = input;

        let event_version_id = version_id;
        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let size = match content_length {
            Some(c) => c,
            None => {
                if let Some(val) = req.headers.get(AMZ_DECODED_CONTENT_LENGTH) {
                    match atoi::atoi::<i64>(val.as_bytes()) {
                        Some(x) => x,
                        None => return Err(s3_error!(UnexpectedContent)),
                    }
                } else {
                    return Err(s3_error!(UnexpectedContent));
                }
            }
        };

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        let Some(ext) = Path::new(&key).extension().and_then(|s| s.to_str()) else {
            return Err(s3_error!(InvalidArgument, "key extension not found"));
        };

        let ext = ext.to_owned();

        let md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let sha256hex = get_content_sha256(&req.headers);
        let actual_size = size;

        let reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let mut hreader = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) = hreader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
            return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
        }

        // TODO: support zip
        let decoder = CompressionFormat::from_extension(&ext).get_decoder(hreader).map_err(|e| {
            error!("get_decoder err {:?}", e);
            s3_error!(InvalidArgument, "get_decoder err")
        })?;

        let mut ar = Archive::new(decoder);
        let mut entries = ar.entries().map_err(|e| {
            error!("get entries err {:?}", e);
            s3_error!(InvalidArgument, "get entries err")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let prefix = req
            .headers
            .get("X-Amz-Meta-Rustfs-Snowball-Prefix")
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or_default();
        let version_id = match event_version_id {
            Some(v) => v.to_string(),
            None => String::new(),
        };
        while let Some(entry) = entries.next().await {
            let f = match entry {
                Ok(f) => f,
                Err(e) => {
                    error!("Failed to read archive entry: {}", e);
                    return Err(s3_error!(InvalidArgument, "Failed to read archive entry: {:?}", e));
                }
            };

            if f.header().entry_type().is_dir() {
                continue;
            }

            if let Ok(fpath) = f.path() {
                let mut fpath = fpath.to_string_lossy().to_string();

                if !prefix.is_empty() {
                    fpath = format!("{prefix}/{fpath}");
                }

                let mut size = f.header().size().unwrap_or_default() as i64;

                debug!("Extracting file: {}, size: {} bytes", fpath, size);

                let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(f));

                let mut metadata = HashMap::new();

                let actual_size = size;

                if is_compressible(&HeaderMap::new(), &fpath) && size > MIN_COMPRESSIBLE_SIZE as i64 {
                    metadata.insert(
                        format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                        CompressionAlgorithm::default().to_string(),
                    );
                    metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

                    let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;

                    reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
                    size = HashReader::SIZE_PRESERVE_LAYER;
                }

                let hrd = HashReader::new(reader, size, actual_size, None, None, false).map_err(ApiError::from)?;
                let mut reader = PutObjReader::new(hrd);

                let _obj_info = store
                    .put_object(&bucket, &fpath, &mut reader, &ObjectOptions::default())
                    .await
                    .map_err(ApiError::from)?;

                // Invalidate cache for the written object to prevent stale data
                let manager = get_concurrency_manager();
                let fpath_clone = fpath.clone();
                let bucket_clone = bucket.clone();
                tokio::spawn(async move {
                    manager.invalidate_cache_versioned(&bucket_clone, &fpath_clone, None).await;
                });

                let e_tag = _obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

                // // store.put_object(bucket, object, data, opts);

                let output = PutObjectOutput {
                    e_tag,
                    ..Default::default()
                };

                let event_args = rustfs_notify::EventArgs {
                    event_name: EventName::ObjectCreatedPut,
                    bucket_name: bucket.clone(),
                    object: _obj_info.clone(),
                    req_params: extract_params_header(&req.headers),
                    resp_elements: extract_resp_elements(&s3_response(output.clone())),
                    version_id: version_id.clone(),
                    host: get_request_host(&req.headers),
                    port: get_request_port(&req.headers),
                    user_agent: get_request_user_agent(&req.headers),
                };

                // Asynchronous call will not block the response of the current request
                tokio::spawn(async move {
                    notifier_global::notify(event_args).await;
                });
            }
        }

        // match decompress(
        //     body,
        //     CompressionFormat::from_extension(&ext),
        //     |entry: tokio_tar::Entry<tokio_tar::Archive<Box<dyn AsyncRead + Send + Unpin + 'static>>>| async move {
        //         let path = entry.path().unwrap();
        //         debug!("Extracted: {}", path.display());
        //         Ok(())
        //     },
        // )
        // .await
        // {
        //     Ok(_) => info!("Decompression completed successfully"),
        //     Err(e) => error!("Decompression failed: {}", e),
        // }

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

        warn!(
            "put object extract checksum_crc32={checksum_crc32:?}, checksum_crc32c={checksum_crc32c:?}, checksum_sha1={checksum_sha1:?}, checksum_sha256={checksum_sha256:?}, checksum_crc64nvme={checksum_crc64nvme:?}",
        );

        // TODO: etag
        let output = PutObjectOutput {
            // e_tag: hreader.try_resolve_etag().map(|v| ETag::Strong(v)),
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            ..Default::default()
        };
        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    /// Auxiliary functions: parse version ID
    ///
    /// # Arguments
    /// * `version_id` - An optional string representing the version ID to be parsed.
    ///
    /// # Returns
    /// * `S3Result<Option<Uuid>>` - A result containing an optional UUID if parsing is successful, or an S3 error if parsing fails.
    fn parse_version_id(&self, version_id: Option<String>) -> S3Result<Option<Uuid>> {
        if let Some(vid) = version_id {
            let uuid = Uuid::parse_str(&vid).map_err(|e| {
                error!("Invalid version ID: {}", e);
                s3_error!(InvalidArgument, "Invalid version ID")
            })?;
            Ok(Some(uuid))
        } else {
            Ok(None)
        }
    }
}

#[async_trait::async_trait]
impl S3 for FS {
    #[instrument(level = "debug", skip(self))]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let AbortMultipartUploadInput {
            bucket, key, upload_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
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
            Ok(_) => Ok(s3_response(AbortMultipartUploadOutput { ..Default::default() })),
            Err(err) => Err(map_abort_multipart_upload_error(err)),
        }
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
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
                return Err(not_initialized_error());
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
        use std::collections::HashMap;
        let mut part_map: HashMap<usize, CompletePart> = HashMap::new();
        for part in uploaded_parts_vec {
            part_map.insert(part.part_num, part);
        }

        // Reconstruct the parts list in sorted order, keeping only the last occurrence of each part number
        let mut uploaded_parts: Vec<CompletePart> = part_map.into_values().collect();
        uploaded_parts.sort_by_key(|p| p.part_num);

        // TODO: check object lock

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
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
        let mut quota_usage_calculated = false;
        if let Some(metadata_sys) = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys.get() {
            let quota_checker = QuotaChecker::new(metadata_sys.clone());

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
                    // Track if usage was actually calculated (not just returned as None/0)
                    quota_usage_calculated = check_result.current_usage.is_some();
                }
                Err(e) => {
                    warn!("Quota check failed for bucket {}: {}, allowing operation", bucket, e);
                }
            }
        }

        // Only increment usage cache when quota checking actually calculated real usage.
        // This prevents cache corruption: when quotas are disabled, the cache remains unset.
        // When quotas are later enabled, the cache will miss and recalculate from backend.
        if quota_usage_calculated {
            rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;
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

        let region = rustfs_ecstore::global::get_global_region().unwrap_or_else(|| "us-east-1".to_string());
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(region.clone()),
            server_side_encryption: server_side_encryption.clone(), // TDD: Return encryption info
            ssekms_key_id: ssekms_key_id.clone(),                   // TDD: Return KMS key ID if present
            checksum_crc32: checksum_crc32.clone(),
            checksum_crc32c: checksum_crc32c.clone(),
            checksum_sha1: checksum_sha1.clone(),
            checksum_sha256: checksum_sha256.clone(),
            checksum_crc64nvme: checksum_crc64nvme.clone(),
            checksum_type: checksum_type.clone(),
            version_id: mpu_version,
            ..Default::default()
        };
        info!(
            "TDD: Created output: SSE={:?}, KMS={:?}",
            output.server_side_encryption, output.ssekms_key_id
        );

        let helper_output = entity::CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_etag(&etag)),
            location: Some(region),
            server_side_encryption, // TDD: Return encryption info
            ssekms_key_id,          // TDD: Return KMS key ID if present
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
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
        info!(
            "TDD: About to return S3Response with output: SSE={:?}, KMS={:?}",
            output.server_side_encryption, output.ssekms_key_id
        );

        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }

        let helper_result = Ok(s3_response(helper_output));
        let _ = helper.complete(&helper_result);
        Ok(s3_response(output))
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedCopy, "s3:CopyObject");
        let CopyObjectInput {
            copy_source,
            bucket,
            key,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            metadata_directive,
            metadata,
            copy_source_if_match,
            copy_source_if_none_match,
            content_type,
            ..
        } = req.input.clone();
        let (src_bucket, src_key, version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                ref bucket,
                ref key,
                version_id,
            } => (bucket.to_string(), key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Validate both source and destination keys
        validate_object_key(&src_key, "COPY (source)")?;
        validate_object_key(&key, "COPY (dest)")?;

        // AWS S3 allows self-copy when metadata directive is REPLACE (used to update metadata in-place).
        // Reject only when the directive is not REPLACE.
        if metadata_directive.as_ref().map(|d| d.as_str()) != Some(MetadataDirective::REPLACE)
            && src_bucket == bucket
            && src_key == key
        {
            error!("Rejected self-copy operation: bucket={}, key={}", bucket, key);
            return Err(s3_error!(
                InvalidRequest,
                "Cannot copy an object to itself. Source and destination must be different."
            ));
        }

        // warn!("copy_object {}/{}, to {}/{}", &src_bucket, &src_key, &bucket, &key);

        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;

        src_opts.version_id = version_id.clone();

        let mut get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        let dst_opts = copy_dst_opts(&bucket, &key, version_id, &req.headers, HashMap::new())
            .await
            .map_err(ApiError::from)?;

        let cp_src_dst_same = path_join_buf(&[&src_bucket, &src_key]) == path_join_buf(&[&bucket, &key]);

        if cp_src_dst_same {
            get_opts.no_lock = true;
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let h = HeaderMap::new();

        let gr = store
            .get_object_reader(&src_bucket, &src_key, None, h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        let mut src_info = gr.object_info.clone();

        // Validate copy source conditions
        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    // Weak ETag or Any (*) in If-Match should fail per RFC 9110
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

        if cp_src_dst_same {
            src_info.metadata_only = true;
        }

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(gr.stream));

        // Apply unified SSE decryption for source object if encrypted
        // Note: SSE-C for copy source is handled via copy_source_sse_customer_* headers
        let copy_source_sse_customer_key = req.input.copy_source_sse_customer_key.as_ref();
        let copy_source_sse_customer_key_md5 = req.input.copy_source_sse_customer_key_md5.as_ref();
        let decryption_request = DecryptionRequest {
            bucket: &src_bucket,
            key: &src_key,
            metadata: &src_info.user_defined,
            sse_customer_key: copy_source_sse_customer_key,
            sse_customer_key_md5: copy_source_sse_customer_key_md5,
            part_number: None,
            parts: &src_info.parts,
        };

        if let Some(material) = sse_decryption(decryption_request).await? {
            reader = material.wrap_single_reader(reader);
            if let Some(original) = material.original_size {
                src_info.actual_size = original;
            }
        }

        strip_managed_encryption_metadata(&mut src_info.user_defined);

        let actual_size = src_info.get_actual_size().map_err(ApiError::from)?;

        let mut length = actual_size;

        let mut compress_metadata = HashMap::new();

        if is_compressible(&req.headers, &key) && actual_size > MIN_COMPRESSIBLE_SIZE as i64 {
            compress_metadata.insert(
                format!("{RESERVED_METADATA_PREFIX_LOWER}compression"),
                CompressionAlgorithm::default().to_string(),
            );
            compress_metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), actual_size.to_string());

            let hrd = EtagReader::new(reader, None);

            // let hrd = HashReader::new(reader, length, actual_size, None, false).map_err(ApiError::from)?;

            reader = Box::new(CompressReader::new(hrd, CompressionAlgorithm::default()));
            length = HashReader::SIZE_PRESERVE_LAYER;
        } else {
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX}compression"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX}actual-size"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression-size"));
            src_info
                .user_defined
                .remove(&format!("{RESERVED_METADATA_PREFIX}compression-size"));
        }

        // Handle MetadataDirective REPLACE: replace user metadata while preserving system metadata.
        // System metadata (compression, encryption) is added after this block to ensure
        // it's not cleared by the REPLACE operation.
        if metadata_directive.as_ref().map(|d| d.as_str()) == Some(MetadataDirective::REPLACE) {
            src_info.user_defined.clear();
            if let Some(metadata) = metadata {
                src_info.user_defined.extend(metadata);
            }
            if let Some(ct) = content_type {
                src_info.content_type = Some(ct.clone());
                src_info.user_defined.insert("content-type".to_string(), ct);
            }
        }

        let mut reader = HashReader::new(reader, length, actual_size, None, None, false).map_err(ApiError::from)?;

        // Apply unified SSE encryption for destination object
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: None,
            part_key: None,
            part_nonce: None,
        };

        let (requested_sse, requested_kms_key_id) = match sse_encryption(encryption_request).await? {
            Some(material) => {
                let requested_sse = Some(material.server_side_encryption.clone());
                let requested_kms_key_id = material.kms_key_id.clone();

                // Apply encryption wrapper
                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                // Merge encryption metadata
                src_info.user_defined.extend(material.metadata);

                (requested_sse, requested_kms_key_id)
            }
            None => (None, None),
        };

        src_info.put_object_reader = Some(PutObjReader::new(reader));

        // check quota

        for (k, v) in compress_metadata {
            src_info.user_defined.insert(k, v);
        }

        // check quota for copy operation
        let mut quota_usage_calculated = false;
        if let Some(metadata_sys) = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys.get() {
            let quota_checker = QuotaChecker::new(metadata_sys.clone());

            match quota_checker
                .check_quota(&bucket, QuotaOperation::CopyObject, src_info.size as u64)
                .await
            {
                Ok(check_result) => {
                    if !check_result.allowed {
                        return Err(S3Error::with_message(
                            S3ErrorCode::InvalidRequest,
                            format!(
                                "Bucket quota exceeded. Current usage: {} bytes, limit: {} bytes",
                                check_result.current_usage.unwrap_or(0),
                                check_result.quota_limit.unwrap_or(0)
                            ),
                        ));
                    }
                    // Track if usage was actually calculated (not just returned as None/0)
                    quota_usage_calculated = check_result.current_usage.is_some();
                }
                Err(e) => {
                    warn!("Quota check failed for bucket {}: {}, allowing operation", bucket, e);
                }
            }
        }

        let oi = store
            .copy_object(&src_bucket, &src_key, &bucket, &key, &mut src_info, &src_opts, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        // Only increment usage cache when quota checking actually calculated real usage.
        // This prevents cache corruption: when quotas are disabled, the cache remains unset.
        // When quotas are later enabled, the cache will miss and recalculate from backend.
        if quota_usage_calculated {
            rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, oi.size as u64).await;
        }

        // Invalidate cache for the destination object to prevent stale data
        let manager = get_concurrency_manager();
        let dest_bucket = bucket.clone();
        let dest_key = key.clone();
        let dest_version = oi.version_id.map(|v| v.to_string());
        let dest_version_clone = dest_version.clone();
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&dest_bucket, &dest_key, dest_version_clone.as_deref())
                .await;
        });

        // warn!("copy_object oi {:?}", &oi);
        let object_info = oi.clone();
        let copy_object_result = CopyObjectResult {
            e_tag: oi.etag.map(|etag| to_s3s_etag(&etag)),
            last_modified: oi.mod_time.map(Timestamp::from),
            ..Default::default()
        };

        let output = CopyObjectOutput {
            copy_object_result: Some(copy_object_result),
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            version_id: dest_version,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketCreated, "s3:CreateBucket");
        let CreateBucketInput {
            bucket,
            object_lock_enabled_for_bucket,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        counter!("rustfs_create_bucket_total").increment(1);

        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    force_create: false, // TODO: force support
                    lock_enabled: object_lock_enabled_for_bucket.is_some_and(|v| v),
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let output = CreateBucketOutput::default();

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
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

        // mc cp step 3

        // debug!("create_multipart_upload meta {:?}", &metadata);

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let mut metadata = extract_metadata(&req.headers);

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags);
        }

        // Prepare SSE configuration for multipart upload
        // Apply encryption using unified SSE API
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

                // Merge encryption metadata
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
            server_side_encryption: effective_sse, // TDD: Return effective encryption config
            sse_customer_algorithm,
            ssekms_key_id: effective_kms_key_id, // TDD: Return effective KMS key ID
            checksum_algorithm: checksum_algo.map(ChecksumAlgorithm::from),
            checksum_type: checksum_type.map(ChecksumType::from),
            ..Default::default()
        };

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    /// Delete a bucket
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, mut req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let helper = OperationHelper::new(&req, EventName::BucketRemoved, "s3:DeleteBucket");
        let input = req.input.clone();
        // TODO: DeleteBucketInput doesn't have force parameter?
        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        // get value from header, support mc style
        let force_str = req
            .headers
            .get(RUSTFS_FORCE_DELETE)
            .map(|v| v.to_str().unwrap_or_default())
            .unwrap_or(
                req.headers
                    .get("x-minio-force-delete")
                    .map(|v| v.to_str().unwrap_or_default())
                    .unwrap_or_default(),
            );

        let force = parse_bool(force_str).unwrap_or_default();

        if force {
            authorize_request(&mut req, Action::S3Action(S3Action::ForceDeleteBucketAction)).await?;
        }

        store
            .delete_bucket(
                &input.bucket,
                &DeleteBucketOptions {
                    force,
                    ..Default::default()
                },
            )
            .await
            .map_err(ApiError::from)?;

        let result = Ok(s3_response(DeleteBucketOutput {}));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let DeleteBucketCorsInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_CORS_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(DeleteBucketCorsOutput {}))
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let DeleteBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        metadata_sys::delete(&bucket, BUCKET_SSECONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(DeleteBucketEncryptionOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let DeleteBucketLifecycleInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_LIFECYCLE_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(DeleteBucketLifecycleOutput::default()))
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let DeleteBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        metadata_sys::delete(&bucket, BUCKET_POLICY_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(DeleteBucketPolicyOutput {}))
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let DeleteBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        metadata_sys::delete(&bucket, BUCKET_REPLICATION_CONFIG)
            .await
            .map_err(ApiError::from)?;

        // TODO: remove targets
        error!("delete bucket");

        Ok(s3_response(DeleteBucketReplicationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let DeleteBucketTaggingInput { bucket, .. } = req.input;

        metadata_sys::delete(&bucket, BUCKET_TAGGING_CONFIG)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(DeleteBucketTaggingOutput {}))
    }

    /// Delete an object
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, mut req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, "s3:DeleteObject");
        let DeleteObjectInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "DELETE")?;

        let replica = req
            .headers
            .get(AMZ_BUCKET_REPLICATION_STATUS)
            .map(|v| v.to_str().unwrap_or_default() == ReplicationStatusType::Replica.as_str())
            .unwrap_or_default();

        if replica {
            authorize_request(&mut req, Action::S3Action(S3Action::ReplicateDeleteAction)).await?;
        }

        let metadata = extract_metadata(&req.headers);
        // Clone version_id before it's moved
        let version_id_clone = version_id.clone();

        let mut opts: ObjectOptions = del_opts(&bucket, &key, version_id, &req.headers, metadata)
            .await
            .map_err(ApiError::from)?;

        let lock_cfg = BucketObjectLockSys::get(&bucket).await;
        if lock_cfg.is_some() && opts.delete_prefix {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("force-delete is forbidden on Object Locking enabled buckets".into()),
                "force-delete is forbidden on Object Locking enabled buckets",
            ));
        }

        // let mut vid = opts.version_id.clone();

        if replica {
            opts.set_replica_status(ReplicationStatusType::Replica);

            // if opts.version_purge_status().is_empty() {
            //     vid = None;
            // }
        }

        let is_force_delete = opts.delete_prefix;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        // Check Object Lock retention before deletion
        // TODO: Future optimization (separate PR) - If performance becomes critical under high delete load:
        // 1. Integrate OptimizedFileCache (file_cache.rs) into the read_version() path
        // 2. Or add a lightweight get_object_lock_info() that only fetches retention metadata
        // 3. Or use combined get-and-delete in storage layer with retention check callback
        // Note: The project has OptimizedFileCache with moka, but get_object_info doesn't use it yet
        let get_opts: ObjectOptions = get_opts(&bucket, &key, version_id_clone, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        match store.get_object_info(&bucket, &key, &get_opts).await {
            Ok(obj_info) => {
                // Check for bypass governance retention header (permission already verified in access.rs)
                let bypass_governance = has_bypass_governance_header(&req.headers);

                if let Some(block_reason) = check_object_lock_for_deletion(&bucket, &obj_info, bypass_governance).await {
                    return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
                }
            }
            Err(err) => {
                // If object not found, allow deletion to proceed (will return 204 No Content)
                if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                    return Err(ApiError::from(err).into());
                }
            }
        }

        let obj_info = {
            match store.delete_object(&bucket, &key, opts).await {
                Ok(obj) => obj,
                Err(err) => {
                    if is_err_bucket_not_found(&err) {
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
                    }

                    if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                        // TODO: send event

                        return Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
                    }

                    return Err(ApiError::from(err).into());
                }
            }
        };

        // Fast in-memory update for immediate quota consistency
        rustfs_ecstore::data_usage::decrement_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        // Invalidate cache for the deleted object
        let manager = get_concurrency_manager();
        let del_bucket = bucket.clone();
        let del_key = key.clone();
        let del_version = obj_info.version_id.map(|v| v.to_string());
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&del_bucket, &del_key, del_version.as_deref())
                .await;
        });

        if is_force_delete
            && !replica
            && let Ok((rcfg, _)) = metadata_sys::get_replication_config(&bucket).await
        {
            let tgt_arns = rcfg.filter_target_arns(&ObjectOpts {
                name: key.clone(),
                ..Default::default()
            });
            for arn in tgt_arns {
                schedule_replication_delete(DeletedObjectReplicationInfo {
                    delete_object: rustfs_ecstore::store_api::DeletedObject {
                        object_name: key.clone(),
                        force_delete: true,
                        ..Default::default()
                    },
                    bucket: bucket.clone(),
                    target_arn: arn,
                    event_type: REPLICATE_INCOMING_DELETE.to_string(),
                    ..Default::default()
                })
                .await;
            }
        }

        if obj_info.name.is_empty() {
            return Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
        }

        if obj_info.replication_status == ReplicationStatusType::Pending
            || obj_info.version_purge_status == VersionPurgeStatusType::Pending
        {
            schedule_replication_delete(DeletedObjectReplicationInfo {
                delete_object: rustfs_ecstore::store_api::DeletedObject {
                    delete_marker: obj_info.delete_marker,
                    delete_marker_version_id: if obj_info.delete_marker { obj_info.version_id } else { None },
                    object_name: key.clone(),
                    version_id: if obj_info.delete_marker { None } else { obj_info.version_id },
                    delete_marker_mtime: obj_info.mod_time,
                    replication_state: Some(obj_info.replication_state()),
                    ..Default::default()
                },
                bucket: bucket.clone(),
                event_type: REPLICATE_INCOMING_DELETE.to_string(),
                ..Default::default()
            })
            .await;
        }

        let delete_marker = obj_info.delete_marker;
        let version_id = obj_info.version_id;

        let output = DeleteObjectOutput {
            delete_marker: Some(delete_marker),
            version_id: version_id.map(|v| v.to_string()),
            ..Default::default()
        };

        let event_name = if delete_marker {
            EventName::ObjectRemovedDeleteMarkerCreated
        } else {
            EventName::ObjectRemovedDelete
        };

        helper = helper.event_name(event_name);
        helper = helper
            .object(obj_info)
            .version_id(version_id.map(|v| v.to_string()).unwrap_or_default());

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedDeleteTagging, "s3:DeleteObjectTagging");
        let DeleteObjectTaggingInput {
            bucket,
            key: object,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(not_initialized_error());
        };

        // Support versioned objects
        let version_id_for_parse = version_id.clone();
        let opts = ObjectOptions {
            version_id: self.parse_version_id(version_id_for_parse)?.map(Into::into),
            ..Default::default()
        };

        // TODO: Replicate (keep the original TODO, if further replication logic is needed)
        store.delete_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            error!("Failed to delete object tags: {}", e);
            ApiError::from(e)
        })?;

        // Invalidate cache for the deleted tagged object
        let manager = get_concurrency_manager();
        let version_id_clone = version_id.clone();
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&bucket, &object, version_id_clone.as_deref())
                .await;
            debug!(
                "Cache invalidated for deleted tagged object: bucket={}, object={}, version_id={:?}",
                bucket, object, version_id_clone
            );
        });

        // Add metrics
        counter!("rustfs.delete_object_tagging.success").increment(1);

        let version_id_resp = version_id.clone().unwrap_or_default();
        helper = helper.version_id(version_id_resp);

        let result = Ok(s3_response(DeleteObjectTaggingOutput { version_id }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "delete").record(duration.as_secs_f64());
        result
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, mut req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let helper = OperationHelper::new(&req, EventName::ObjectRemovedDelete, "s3:DeleteObjects").suppress_event();
        let (bucket, delete) = {
            let bucket = req.input.bucket.clone();
            let delete = req.input.delete.clone();
            (bucket, delete)
        };

        if delete.objects.is_empty() || delete.objects.len() > 1000 {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                "No objects to delete or too many objects to delete".to_string(),
            ));
        }

        let replicate_deletes = has_replication_rules(
            &bucket,
            &delete
                .objects
                .iter()
                .map(|v| ObjectToDelete {
                    object_name: v.key.clone(),
                    ..Default::default()
                })
                .collect::<Vec<ObjectToDelete>>(),
        )
        .await;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let version_cfg = BucketVersioningSys::get(&bucket).await.unwrap_or_default();

        // Check for bypass governance retention header (permission already verified in access.rs)
        let bypass_governance = has_bypass_governance_header(&req.headers);

        #[derive(Default, Clone)]
        struct DeleteResult {
            delete_object: Option<rustfs_ecstore::store_api::DeletedObject>,
            error: Option<Error>,
        }

        let mut delete_results = vec![DeleteResult::default(); delete.objects.len()];

        let mut object_to_delete = Vec::new();
        let mut object_to_delete_index = HashMap::new();
        let mut object_sizes = HashMap::new();
        for (idx, obj_id) in delete.objects.iter().enumerate() {
            // Per S3 API spec, "null" string means non-versioned object
            // Filter out "null" version_id to treat as unversioned
            let version_id = obj_id.version_id.clone().filter(|v| v != "null");
            if let Some(ref vid) = version_id {
                let _vid = match Uuid::parse_str(vid) {
                    Ok(v) => v,
                    Err(err) => {
                        delete_results[idx].error = Some(Error {
                            code: Some("NoSuchVersion".to_string()),
                            key: Some(obj_id.key.clone()),
                            message: Some(err.to_string()),
                            version_id: Some(vid.clone()),
                        });

                        continue;
                    }
                };
            };

            {
                let req_info = req.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
                req_info.bucket = Some(bucket.clone());
                req_info.object = Some(obj_id.key.clone());
                req_info.version_id = version_id.clone();
            }

            let auth_res = authorize_request(&mut req, Action::S3Action(S3Action::DeleteObjectAction)).await;
            if let Err(e) = auth_res {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(e.to_string()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            let mut object = ObjectToDelete {
                object_name: obj_id.key.clone(),
                version_id: version_id
                    .clone()
                    .map(|v| Uuid::parse_str(&v).expect("version_id validated as UUID earlier")),
                ..Default::default()
            };

            let metadata = extract_metadata(&req.headers);

            let opts: ObjectOptions = del_opts(
                &bucket,
                &object.object_name,
                object.version_id.map(|f| f.to_string()),
                &req.headers,
                metadata,
            )
            .await
            .map_err(ApiError::from)?;

            // Get object info to collect size for quota tracking
            let (goi, gerr) = match store.get_object_info(&bucket, &object.object_name, &opts).await {
                Ok(res) => (res, None),
                Err(e) => (ObjectInfo::default(), Some(e.to_string())),
            };

            // Check Object Lock retention before deletion
            // NOTE: Unlike single DeleteObject, this reuses the get_object_info result from quota
            // tracking above, so no additional storage operation is required for the retention check.
            if gerr.is_none()
                && let Some(block_reason) = check_object_lock_for_deletion(&bucket, &goi, bypass_governance).await
            {
                delete_results[idx].error = Some(Error {
                    code: Some("AccessDenied".to_string()),
                    key: Some(obj_id.key.clone()),
                    message: Some(block_reason.error_message()),
                    version_id: version_id.clone(),
                });
                continue;
            }

            // Store object size for quota tracking
            object_sizes.insert(object.object_name.clone(), goi.size);

            if is_dir_object(&object.object_name) && object.version_id.is_none() {
                object.version_id = Some(Uuid::nil());
            }

            if replicate_deletes {
                let dsc = check_replicate_delete(
                    &bucket,
                    &ObjectToDelete {
                        object_name: object.object_name.clone(),
                        version_id: object.version_id,
                        ..Default::default()
                    },
                    &goi,
                    &opts,
                    gerr.clone(),
                )
                .await;
                if dsc.replicate_any() {
                    if object.version_id.is_some() {
                        object.version_purge_status = Some(VersionPurgeStatusType::Pending);
                        object.version_purge_statuses = dsc.pending_status();
                    } else {
                        object.delete_marker_replication_status = dsc.pending_status();
                    }
                    object.replicate_decision_str = Some(dsc.to_string());
                }
            }

            // TODO: Retention
            object_to_delete_index.insert(object.object_name.clone(), idx);
            object_to_delete.push(object);
        }

        let (mut dobjs, errs) = {
            store
                .delete_objects(
                    &bucket,
                    object_to_delete.clone(),
                    ObjectOptions {
                        version_suspended: version_cfg.suspended(),
                        ..Default::default()
                    },
                )
                .await
        };

        // Invalidate cache for successfully deleted objects
        let manager = get_concurrency_manager();
        let bucket_clone = bucket.clone();
        let deleted_objects = dobjs.clone();
        tokio::spawn(async move {
            for dobj in deleted_objects {
                manager
                    .invalidate_cache_versioned(
                        &bucket_clone,
                        &dobj.object_name,
                        dobj.version_id.map(|v| v.to_string()).as_deref(),
                    )
                    .await;
            }
        });

        if is_all_buckets_not_found(
            &errs
                .iter()
                .map(|v| v.as_ref().map(|v| v.clone().into()))
                .collect::<Vec<Option<DiskError>>>() as &[Option<DiskError>],
        ) {
            let result = Err(S3Error::with_message(S3ErrorCode::NoSuchBucket, "Bucket not found".to_string()));
            let _ = helper.complete(&result);
            return result;
        }

        for (i, err) in errs.iter().enumerate() {
            let obj = dobjs[i].clone();

            // let replication_state = obj.replication_state.clone().unwrap_or_default();

            // let obj_to_del = ObjectToDelete {
            //     object_name: decode_dir_object(dobjs[i].object_name.as_str()),
            //     version_id: obj.version_id,
            //     delete_marker_replication_status: replication_state.replication_status_internal.clone(),
            //     version_purge_status: Some(obj.version_purge_status()),
            //     version_purge_statuses: replication_state.version_purge_status_internal.clone(),
            //     replicate_decision_str: Some(replication_state.replicate_decision_str.clone()),
            // };

            let Some(didx) = object_to_delete_index.get(&obj.object_name) else {
                continue;
            };

            if err.is_none()
                || err
                    .clone()
                    .is_some_and(|v| is_err_object_not_found(&v) || is_err_version_not_found(&v))
            {
                if replicate_deletes {
                    dobjs[i].replication_state = Some(object_to_delete[i].replication_state());
                }
                delete_results[*didx].delete_object = Some(dobjs[i].clone());
                // Update quota tracking for successfully deleted objects
                if let Some(&size) = object_sizes.get(&obj.object_name) {
                    rustfs_ecstore::data_usage::decrement_bucket_usage_memory(&bucket, size as u64).await;
                }
                continue;
            }

            if let Some(err) = err.clone() {
                delete_results[*didx].error = Some(Error {
                    code: Some(err.to_string()),
                    key: Some(object_to_delete[i].object_name.clone()),
                    message: Some(err.to_string()),
                    version_id: object_to_delete[i].version_id.map(|v| v.to_string()),
                });
            }
        }

        let deleted = delete_results
            .iter()
            .filter_map(|v| v.delete_object.clone())
            .map(|v| DeletedObject {
                delete_marker: { if v.delete_marker { Some(true) } else { None } },
                delete_marker_version_id: v.delete_marker_version_id.map(|v| v.to_string()),
                key: Some(v.object_name.clone()),
                version_id: if is_dir_object(v.object_name.as_str()) && v.version_id == Some(Uuid::nil()) {
                    None
                } else {
                    v.version_id.map(|v| v.to_string())
                },
            })
            .collect();

        let errors = delete_results.iter().filter_map(|v| v.error.clone()).collect::<Vec<Error>>();

        let output = DeleteObjectsOutput {
            deleted: Some(deleted),
            errors: Some(errors),
            ..Default::default()
        };

        for dobjs in delete_results.iter() {
            if let Some(dobj) = &dobjs.delete_object
                && replicate_deletes
                && (dobj.delete_marker_replication_status() == ReplicationStatusType::Pending
                    || dobj.version_purge_status() == VersionPurgeStatusType::Pending)
            {
                let mut dobj = dobj.clone();
                if is_dir_object(dobj.object_name.as_str()) && dobj.version_id.is_none() {
                    dobj.version_id = Some(Uuid::nil());
                }

                let deleted_object = DeletedObjectReplicationInfo {
                    delete_object: dobj,
                    bucket: bucket.clone(),
                    event_type: REPLICATE_INCOMING_DELETE.to_string(),
                    ..Default::default()
                };
                schedule_replication_delete(deleted_object).await;
            }
        }

        let req_headers = req.headers.clone();
        tokio::spawn(async move {
            for res in delete_results {
                if let Some(dobj) = res.delete_object {
                    let event_name = if dobj.delete_marker {
                        EventName::ObjectRemovedDeleteMarkerCreated
                    } else {
                        EventName::ObjectRemovedDelete
                    };
                    let event_args = EventArgsBuilder::new(
                        event_name,
                        bucket.clone(),
                        ObjectInfo {
                            name: dobj.object_name.clone(),
                            bucket: bucket.clone(),
                            ..Default::default()
                        },
                    )
                    .version_id(dobj.version_id.map(|v| v.to_string()).unwrap_or_default())
                    .req_params(extract_params_header(&req_headers))
                    .resp_elements(extract_resp_elements(&s3_response(DeleteObjectsOutput::default())))
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notifier_global::notify(event_args).await;
                }
            }
        });

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(build_get_bucket_acl_output()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        let bucket = req.input.bucket.clone();
        // check bucket exists.
        let _bucket = self
            .head_bucket(req.map_input(|input| HeadBucketInput {
                bucket: input.bucket,
                expected_bucket_owner: None,
            }))
            .await?;

        let cors_configuration = match metadata_sys::get_cors_config(&bucket).await {
            Ok((config, _)) => config,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::NoSuchCORSConfiguration,
                        "The CORS configuration does not exist".to_string(),
                    ));
                }
                warn!("get_cors_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(s3_response(GetBucketCorsOutput {
            cors_rules: Some(cors_configuration.cors_rules),
        }))
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        let GetBucketEncryptionInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let server_side_encryption_configuration = match metadata_sys::get_sse_config(&bucket).await {
            Ok((cfg, _)) => Some(cfg),
            Err(err) => {
                // if BucketMetadataError::BucketLifecycleNotFound.is(&err) {
                //     return Err(s3_error!(ErrNoSuchBucketSSEConfig));
                // }
                warn!("get_sse_config err {:?}", err);
                None
            }
        };

        Ok(s3_response(GetBucketEncryptionOutput {
            server_side_encryption_configuration,
        }))
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        let GetBucketLifecycleConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let rules = match metadata_sys::get_lifecycle_config(&bucket).await {
            Ok((cfg, _)) => cfg.rules,
            Err(_err) => {
                // Return NoSuchLifecycleConfiguration error as expected by S3 clients
                // This fixes issue #990 where Ansible S3 roles fail with KeyError: 'Rules'
                return Err(s3_error!(NoSuchLifecycleConfiguration));
            }
        };

        Ok(s3_response(GetBucketLifecycleConfigurationOutput {
            rules: Some(rules),
            ..Default::default()
        }))
    }

    /// Get bucket location
    #[instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // mc get  1
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(region) = rustfs_ecstore::global::get_global_region() {
            return Ok(s3_response(GetBucketLocationOutput {
                location_constraint: Some(BucketLocationConstraint::from(region)),
            }));
        }

        let output = GetBucketLocationOutput::default();
        Ok(s3_response(output))
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        let GetBucketNotificationConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let has_notification_config = metadata_sys::get_notification_config(&bucket).await.unwrap_or_else(|err| {
            warn!("get_notification_config err {:?}", err);
            None
        });

        // TODO: valid target list

        if let Some(NotificationConfiguration {
            event_bridge_configuration,
            lambda_function_configurations,
            queue_configurations,
            topic_configurations,
        }) = has_notification_config
        {
            Ok(s3_response(GetBucketNotificationConfigurationOutput {
                event_bridge_configuration,
                lambda_function_configurations,
                queue_configurations,
                topic_configurations,
            }))
        } else {
            Ok(s3_response(GetBucketNotificationConfigurationOutput::default()))
        }
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        let GetBucketPolicyInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // Return the raw policy JSON as originally stored to preserve exact format.
        // This ensures GET returns the same document that was PUT, matching S3 behavior.
        let (policy_str, _) = match metadata_sys::get_bucket_policy_raw(&bucket).await {
            Ok(res) => res,
            Err(err) => {
                if StorageError::ConfigNotFound == err {
                    return Err(s3_error!(NoSuchBucketPolicy));
                }
                return Err(S3Error::with_message(S3ErrorCode::InternalError, err.to_string()));
            }
        };

        Ok(s3_response(GetBucketPolicyOutput {
            policy: Some(policy_str),
        }))
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        let GetBucketPolicyStatusInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
        let conditions = get_condition_values(&req.headers, &rustfs_credentials::Credentials::default(), None, None, remote_addr);

        let read_only = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::ListBucketAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let write_only = PolicySys::is_allowed(&BucketPolicyArgs {
            bucket: &bucket,
            action: Action::S3Action(S3Action::PutObjectAction),
            is_owner: false,
            account: "",
            groups: &None,
            conditions: &conditions,
            object: "",
        })
        .await;

        let is_public = read_only && write_only;

        let output = GetBucketPolicyStatusOutput {
            policy_status: Some(PolicyStatus {
                is_public: Some(is_public),
            }),
        };

        Ok(s3_response(output))
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        let GetBucketReplicationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let rcfg = match get_replication_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                error!("get_replication_config err {:?}", err);
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ReplicationConfigurationNotFoundError,
                        "replication not found".to_string(),
                    ));
                }
                return Err(ApiError::from(err).into());
            }
        };

        if rcfg.is_none() {
            return Err(S3Error::with_message(
                S3ErrorCode::ReplicationConfigurationNotFoundError,
                "replication not found".to_string(),
            ));
        }

        // Ok(s3_response(GetBucketReplicationOutput {
        //     replication_configuration: rcfg,
        // }))

        if rcfg.is_some() {
            Ok(s3_response(GetBucketReplicationOutput {
                replication_configuration: rcfg,
            }))
        } else {
            let rep = ReplicationConfiguration {
                role: "".to_string(),
                rules: vec![],
            };
            Ok(s3_response(GetBucketReplicationOutput {
                replication_configuration: Some(rep),
            }))
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        let bucket = req.input.bucket.clone();
        // check bucket exists.
        let _bucket = self
            .head_bucket(req.map_input(|input| HeadBucketInput {
                bucket: input.bucket,
                expected_bucket_owner: None,
            }))
            .await?;

        let Tagging { tag_set } = match metadata_sys::get_tagging_config(&bucket).await {
            Ok((tags, _)) => tags,
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(S3ErrorCode::NoSuchTagSet, "The TagSet does not exist".to_string()));
                }
                warn!("get_tagging_config err {:?}", &err);
                return Err(ApiError::from(err).into());
            }
        };

        Ok(s3_response(GetBucketTaggingOutput { tag_set }))
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        let GetBucketVersioningInput { bucket, .. } = req.input;
        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let VersioningConfiguration { status, .. } = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;

        Ok(s3_response(GetBucketVersioningOutput {
            status,
            ..Default::default()
        }))
    }

    /// Get bucket notification
    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let request_start = std::time::Instant::now();

        // Track this request for concurrency-aware optimizations
        let _request_guard = ConcurrencyManager::track_request();
        let concurrent_requests = GetObjectGuard::concurrent_requests();

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge};
            counter!("rustfs.get.object.requests.total").increment(1);
            gauge!("rustfs.concurrent.get.object.requests").set(concurrent_requests as f64);
        }

        debug!("GetObject request started with {} concurrent requests", concurrent_requests);

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGet, "s3:GetObject");
        // mc get 3

        let GetObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "GET")?;

        // Try to get from cache for small, frequently accessed objects
        let manager = get_concurrency_manager();
        // Generate cache key with version support: "{bucket}/{key}" or "{bucket}/{key}?versionId={vid}"
        let cache_key = ConcurrencyManager::make_cache_key(&bucket, &key, version_id.as_deref());

        // Only attempt cache lookup if caching is enabled and for objects without range/part requests
        if manager.is_cache_enabled()
            && part_number.is_none()
            && range.is_none()
            && let Some(cached) = manager.get_cached_object(&cache_key).await
        {
            let cache_serve_duration = request_start.elapsed();

            debug!("Serving object from response cache: {} (latency: {:?})", cache_key, cache_serve_duration);

            #[cfg(feature = "metrics")]
            {
                use metrics::{counter, histogram};
                counter!("rustfs.get.object.cache.served.total").increment(1);
                histogram!("rustfs.get.object.cache.serve.duration.seconds").record(cache_serve_duration.as_secs_f64());
                histogram!("rustfs.get.object.cache.size.bytes").record(cached.body.len() as f64);
            }

            // Build response from cached data with full metadata
            let body_data = cached.body.clone();
            let body = Some(StreamingBlob::wrap::<_, Infallible>(futures::stream::once(async move { Ok(body_data) })));

            // Parse last_modified from RFC3339 string if available
            let last_modified = cached
                .last_modified
                .as_ref()
                .and_then(|s| match OffsetDateTime::parse(s, &Rfc3339) {
                    Ok(dt) => Some(Timestamp::from(dt)),
                    Err(e) => {
                        warn!("Failed to parse cached last_modified '{}': {}", s, e);
                        None
                    }
                });

            // Parse content_type
            let content_type = cached.content_type.as_ref().and_then(|ct| ContentType::from_str(ct).ok());

            let output = GetObjectOutput {
                body,
                content_length: Some(cached.content_length),
                accept_ranges: Some("bytes".to_string()),
                e_tag: cached.e_tag.as_ref().map(|etag| to_s3s_etag(etag)),
                last_modified,
                content_type,
                cache_control: cached.cache_control.clone(),
                content_disposition: cached.content_disposition.clone(),
                content_encoding: cached.content_encoding.clone(),
                content_language: cached.content_language.clone(),
                version_id: cached.version_id.clone(),
                delete_marker: Some(cached.delete_marker),
                tag_count: cached.tag_count,
                metadata: if cached.user_metadata.is_empty() {
                    None
                } else {
                    Some(cached.user_metadata.clone())
                },
                ..Default::default()
            };

            // CRITICAL: Build ObjectInfo for event notification before calling complete().
            // This ensures S3 bucket notifications (s3:GetObject events) include proper
            // object metadata for event-driven workflows (Lambda, SNS, SQS).
            let event_info = ObjectInfo {
                bucket: bucket.clone(),
                name: key.clone(),
                storage_class: cached.storage_class.clone(),
                mod_time: cached
                    .last_modified
                    .as_ref()
                    .and_then(|s| OffsetDateTime::parse(s, &Rfc3339).ok()),
                size: cached.content_length,
                actual_size: cached.content_length,
                is_dir: false,
                user_defined: cached.user_metadata.clone(),
                version_id: cached.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok()),
                delete_marker: cached.delete_marker,
                content_type: cached.content_type.clone(),
                content_encoding: cached.content_encoding.clone(),
                etag: cached.e_tag.clone(),
                ..Default::default()
            };

            // Set object info and version_id on helper for proper event notification
            let version_id_str = req.input.version_id.clone().unwrap_or_default();
            helper = helper.object(event_info).version_id(version_id_str);

            // Call helper.complete() for cache hits to ensure
            // S3 bucket notifications (s3:GetObject events) are triggered.
            // This ensures event-driven workflows (Lambda, SNS) work correctly
            // for both cache hits and misses.
            let result = Ok(s3_response(output));
            let _ = helper.complete(&result);
            return result;
        }

        // TODO: getObjectInArchiveFileHandler object = xxx.zip/xxx/xxx.xxx

        // let range = HTTPRangeSpec::nil();

        let h = HeaderMap::new();

        let part_number = part_number.map(|v| v as usize);

        if let Some(part_num) = part_number
            && part_num == 0
        {
            return Err(s3_error!(InvalidArgument, "Invalid part number: part number must be greater than 0"));
        }

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let store = get_validated_store(&bucket).await?;

        // ============================================
        // Adaptive I/O Strategy with Disk Permit
        // ============================================
        //
        // Acquire disk read permit and calculate adaptive I/O strategy
        // based on the wait time. Longer wait times indicate higher system
        // load, which triggers more conservative I/O parameters.
        let permit_wait_start = std::time::Instant::now();
        let _disk_permit = manager.acquire_disk_read_permit().await;
        let permit_wait_duration = permit_wait_start.elapsed();

        // Calculate adaptive I/O strategy from permit wait time
        // This adjusts buffer sizes, read-ahead, and caching behavior based on load
        // Use 256KB as the base buffer size for strategy calculation
        let base_buffer_size = get_global_buffer_config().base_config.default_unknown;
        let io_strategy = manager.calculate_io_strategy(permit_wait_duration, base_buffer_size);

        // Record detailed I/O metrics for monitoring
        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, gauge, histogram};
            // Record permit wait time histogram
            histogram!("rustfs.disk.permit.wait.duration.seconds").record(permit_wait_duration.as_secs_f64());
            // Record current load level as gauge (0=Low, 1=Medium, 2=High, 3=Critical)
            let load_level_value = match io_strategy.load_level {
                crate::storage::concurrency::IoLoadLevel::Low => 0.0,
                crate::storage::concurrency::IoLoadLevel::Medium => 1.0,
                crate::storage::concurrency::IoLoadLevel::High => 2.0,
                crate::storage::concurrency::IoLoadLevel::Critical => 3.0,
            };
            gauge!("rustfs.io.load.level").set(load_level_value);
            // Record buffer multiplier as gauge
            gauge!("rustfs.io.buffer.multiplier").set(io_strategy.buffer_multiplier);
            // Count strategy selections by load level
            counter!("rustfs.io.strategy.selected", "level" => format!("{:?}", io_strategy.load_level)).increment(1);
        }

        // Log strategy details at debug level for troubleshooting
        debug!(
            wait_ms = permit_wait_duration.as_millis() as u64,
            load_level = ?io_strategy.load_level,
            buffer_size = io_strategy.buffer_size,
            readahead = io_strategy.enable_readahead,
            cache_wb = io_strategy.cache_writeback_enabled,
            "Adaptive I/O strategy calculated"
        );

        let reader = store
            .get_object_reader(bucket.as_str(), key.as_str(), rs.clone(), h, &opts)
            .await
            .map_err(ApiError::from)?;

        let info = reader.object_info;

        check_preconditions(&req.headers, &info)?;

        debug!(object_size = info.size, part_count = info.parts.len(), "GET object metadata snapshot");
        for part in &info.parts {
            debug!(
                part_number = part.number,
                part_size = part.size,
                part_actual_size = part.actual_size,
                "GET object part details"
            );
        }
        let event_info = info.clone();
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("parse content-type err {} {:?}", content_type, err);
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        let mut rs = rs;

        if let Some(part_number) = part_number
            && rs.is_none()
        {
            rs = HTTPRangeSpec::from_object_info(&info, part_number);
        }

        let mut content_length = info.get_actual_size().map_err(ApiError::from)?;

        let content_range = if let Some(rs) = &rs {
            let total_size = content_length;
            let (start, length) = rs.get_offset_length(total_size).map_err(ApiError::from)?;
            content_length = length;
            Some(format!("bytes {}-{}/{}", start, start as i64 + length - 1, total_size))
        } else {
            None
        };

        // ============================================
        // Apply Unified SSE Decryption
        // ============================================
        // Apply decryption if object is encrypted

        // Apply unified SSE decryption using apply_decryption API
        let mut final_stream = reader.stream;
        let mut response_content_length = content_length;

        debug!(
            "GET object metadata check: parts={}, provided_sse_key={:?}",
            info.parts.len(),
            req.input.sse_customer_key.is_some()
        );

        let decryption_request = DecryptionRequest {
            bucket: &bucket,
            key: &key,
            metadata: &info.user_defined,
            sse_customer_key: req.input.sse_customer_key.as_ref(),
            sse_customer_key_md5: req.input.sse_customer_key_md5.as_ref(),
            part_number: None,
            parts: &info.parts,
        };

        let (server_side_encryption, sse_customer_algorithm, sse_customer_key_md5, ssekms_key_id, encryption_applied) =
            match sse_decryption(decryption_request).await? {
                Some(material) => {
                    let server_side_encryption = Some(material.server_side_encryption.clone());
                    let sse_customer_algorithm = Some(material.algorithm.clone());
                    let sse_customer_key_md5 = material.customer_key_md5.clone();
                    let ssekms_key_id = material.kms_key_id.clone();

                    // Apply unified SSE decryption (handles single-part, multipart, and hard limit)
                    let (decrypted_stream, plaintext_size) = material
                        .wrap_reader(final_stream, content_length)
                        .await
                        .map_err(ApiError::from)?;

                    final_stream = decrypted_stream;
                    response_content_length = plaintext_size;

                    (server_side_encryption, sse_customer_algorithm, sse_customer_key_md5, ssekms_key_id, true)
                }
                None => (None, None, None, None, false),
            };

        // Calculate concurrency-aware buffer size for optimal performance
        // This adapts based on the number of concurrent GetObject requests
        // AND the adaptive I/O strategy from permit wait time
        let base_buffer_size = get_buffer_size_opt_in(response_content_length);
        let optimal_buffer_size = if io_strategy.buffer_size > 0 {
            // Use adaptive I/O strategy buffer size (derived from permit wait time)
            io_strategy.buffer_size.min(base_buffer_size)
        } else {
            // Fallback to concurrency-aware sizing
            get_concurrency_aware_buffer_size(response_content_length, base_buffer_size)
        };

        debug!(
            "GetObject buffer sizing: file_size={}, base={}, optimal={}, concurrent_requests={}, io_strategy={:?}",
            response_content_length, base_buffer_size, optimal_buffer_size, concurrent_requests, io_strategy.load_level
        );

        // Cache writeback logic for small, non-encrypted, non-range objects
        // Only cache when:
        // 1. Cache is enabled (RUSTFS_OBJECT_CACHE_ENABLE=true)
        // 2. No part/range request (full object)
        // 3. Object size is known and within cache threshold (10MB)
        // 4. Not encrypted (SSE-C or managed encryption)
        // 5. I/O strategy allows cache writeback (disabled under critical load)
        let should_cache = manager.is_cache_enabled()
            && io_strategy.cache_writeback_enabled
            && part_number.is_none()
            && rs.is_none()
            && !encryption_applied
            && response_content_length > 0
            && (response_content_length as usize) <= manager.max_object_size();

        let body = if should_cache {
            // Read entire object into memory for caching
            debug!(
                "Reading object into memory for caching: key={} size={}",
                cache_key, response_content_length
            );

            // Read the stream into a Vec<u8>
            let mut buf = Vec::with_capacity(response_content_length as usize);
            if let Err(e) = tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                error!("Failed to read object into memory for caching: {}", e);
                return Err(ApiError::from(StorageError::other(format!("Failed to read object for caching: {e}"))).into());
            }

            // Verify we read the expected amount
            if buf.len() != response_content_length as usize {
                warn!(
                    "Object size mismatch during cache read: expected={} actual={}",
                    response_content_length,
                    buf.len()
                );
            }

            // Build CachedGetObject with full metadata for cache writeback
            let last_modified_str = info.mod_time.and_then(|t| match t.format(&Rfc3339) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!("Failed to format last_modified for cache writeback: {}", e);
                    None
                }
            });

            let cached_response = CachedGetObject::new(Bytes::from(buf.clone()), response_content_length)
                .with_content_type(info.content_type.clone().unwrap_or_default())
                .with_e_tag(info.etag.clone().unwrap_or_default())
                .with_last_modified(last_modified_str.unwrap_or_default());

            // Cache the object in background to avoid blocking the response
            let cache_key_clone = cache_key.clone();
            tokio::spawn(async move {
                let manager = get_concurrency_manager();
                manager.put_cached_object(cache_key_clone.clone(), cached_response).await;
                debug!("Object cached successfully with metadata: {}", cache_key_clone);
            });

            #[cfg(feature = "metrics")]
            {
                use metrics::counter;
                counter!("rustfs.object.cache.writeback.total").increment(1);
            }

            // Create response from the in-memory data
            let mem_reader = InMemoryAsyncReader::new(buf);
            Some(StreamingBlob::wrap(bytes_stream(
                ReaderStream::with_capacity(Box::new(mem_reader), optimal_buffer_size),
                response_content_length as usize,
            )))
        } else if encryption_applied {
            // For encrypted objects, don't use bytes_stream to limit the stream
            // because DecryptReader needs to read all encrypted data to produce decrypted output
            info!(
                "SSE decryption: Using unlimited stream for decryption with buffer size {}",
                optimal_buffer_size
            );
            Some(StreamingBlob::wrap(ReaderStream::with_capacity(final_stream, optimal_buffer_size)))
        } else {
            let seekable_object_size_threshold = rustfs_config::DEFAULT_OBJECT_SEEK_SUPPORT_THRESHOLD;

            let should_provide_seek_support = response_content_length > 0
                && response_content_length <= seekable_object_size_threshold as i64
                && part_number.is_none()
                && rs.is_none();

            if should_provide_seek_support {
                debug!(
                    "Reading small object into memory for seek support: key={} size={}",
                    cache_key, response_content_length
                );

                // Read the stream into memory
                let mut buf = Vec::with_capacity(response_content_length as usize);
                match tokio::io::AsyncReadExt::read_to_end(&mut final_stream, &mut buf).await {
                    Ok(_) => {
                        // Verify we read the expected amount
                        if buf.len() != response_content_length as usize {
                            warn!(
                                "Object size mismatch during seek support read: expected={} actual={}",
                                response_content_length,
                                buf.len()
                            );
                        }

                        // Create seekable in-memory reader (similar to MinIO SDK's bytes.Reader)
                        let mem_reader = InMemoryAsyncReader::new(buf);
                        Some(StreamingBlob::wrap(bytes_stream(
                            ReaderStream::with_capacity(Box::new(mem_reader), optimal_buffer_size),
                            response_content_length as usize,
                        )))
                    }
                    Err(e) => {
                        error!("Failed to read object into memory for seek support: {}", e);
                        // Fallback to streaming if read fails
                        Some(StreamingBlob::wrap(bytes_stream(
                            ReaderStream::with_capacity(final_stream, optimal_buffer_size),
                            response_content_length as usize,
                        )))
                    }
                }
            } else {
                // Standard streaming path for large objects or range/part requests
                Some(StreamingBlob::wrap(bytes_stream(
                    ReaderStream::with_capacity(final_stream, optimal_buffer_size),
                    response_content_length as usize,
                )))
            }
        };

        let mut checksum_crc32 = None;
        let mut checksum_crc32c = None;
        let mut checksum_sha1 = None;
        let mut checksum_sha256 = None;
        let mut checksum_crc64nvme = None;
        let mut checksum_type = None;

        // checksum
        if let Some(checksum_mode) = req.headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (checksums, _is_multipart) =
                info.decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                    .map_err(|e| {
                        error!("decrypt_checksums error: {}", e);
                        ApiError::from(e)
                    })?;

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
        }

        let versioned = BucketVersioningSys::prefix_enabled(&bucket, &key).await;

        // Get version_id from object info
        // If versioning is enabled and version_id exists in object info, return it
        // If version_id is Uuid::nil(), return "null" string (AWS S3 convention)
        let output_version_id = if versioned {
            info.version_id.map(|vid| {
                if vid == Uuid::nil() {
                    "null".to_string()
                } else {
                    vid.to_string()
                }
            })
        } else {
            None
        };

        let output = GetObjectOutput {
            body,
            content_length: Some(response_content_length),
            last_modified,
            content_type,
            content_encoding: info.content_encoding.clone(),
            accept_ranges: Some("bytes".to_string()),
            content_range,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata: filter_object_metadata(&info.user_defined),
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            version_id: output_version_id,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(event_info).version_id(version_id);

        let total_duration = request_start.elapsed();

        #[cfg(feature = "metrics")]
        {
            use metrics::{counter, histogram};
            counter!("rustfs.get.object.requests.completed").increment(1);
            histogram!("rustfs.get.object.total.duration.seconds").record(total_duration.as_secs_f64());
            histogram!("rustfs.get.object.response.size.bytes").record(response_content_length as f64);

            // Record buffer size that was used
            histogram!("get.object.buffer.size.bytes").record(optimal_buffer_size as f64);
        }

        debug!(
            "GetObject completed: key={} size={} duration={:?} buffer={}",
            cache_key, response_content_length, total_duration, optimal_buffer_size
        );

        let response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        let GetObjectAclInput { bucket, key, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        Ok(s3_response(build_get_object_acl_output()))
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedAttributes, "s3:GetObjectAttributes");
        let GetObjectAttributesInput { bucket, key, .. } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        if let Err(e) = store
            .get_object_reader(&bucket, &key, None, HeaderMap::new(), &ObjectOptions::default())
            .await
        {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        let output = GetObjectAttributesOutput {
            delete_marker: None,
            object_parts: None,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper
            .object(ObjectInfo {
                name: key.clone(),
                bucket,
                ..Default::default()
            })
            .version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGetLegalHold, "s3:GetObjectLegalHold");
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // check object lock
        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let legal_hold = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .map(|v| v.as_str().to_string());

        let status = if let Some(v) = legal_hold {
            v
        } else {
            ObjectLockLegalHoldStatus::OFF.to_string()
        };

        let output = GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from(status)),
            }),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        let GetObjectLockConfigurationInput { bucket, .. } = req.input;

        let object_lock_configuration = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ObjectLockConfigurationNotFoundError,
                        "Object Lock configuration does not exist for this bucket".to_string(),
                    ));
                }
                warn!("get_object_lock_config err {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "Failed to load Object Lock configuration".to_string(),
                ));
            }
        };

        // warn!("object_lock_configuration {:?}", &object_lock_configuration);

        Ok(s3_response(GetObjectLockConfigurationOutput {
            object_lock_configuration,
        }))
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGetRetention, "s3:GetObjectRetention");
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        // check object lock
        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let mode = object_info
            .user_defined
            .get("x-amz-object-lock-mode")
            .map(|v| ObjectLockRetentionMode::from(v.as_str().to_string()));

        let retain_until_date = object_info
            .user_defined
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| OffsetDateTime::parse(v.as_str(), &Rfc3339).ok())
            .map(Timestamp::from);

        let output = GetObjectRetentionOutput {
            retention: Some(ObjectLockRetention { mode, retain_until_date }),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let GetObjectTaggingInput { bucket, key: object, .. } = req.input;

        info!("Starting get_object_tagging for bucket: {}, object: {}", bucket, object);

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(not_initialized_error());
        };

        // Support versioned objects
        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: self.parse_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        let tags = store.get_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            if is_err_object_not_found(&e) {
                error!("Object not found: {}", e);
                return s3_error!(NoSuchKey);
            }
            error!("Failed to get object tags: {}", e);
            ApiError::from(e).into()
        })?;

        let tag_set = decode_tags(tags.as_str());
        debug!("Decoded tag set: {:?}", tag_set);

        // Add metrics
        counter!("rustfs.get_object_tagging.success").increment(1);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "put").record(duration.as_secs_f64());
        Ok(s3_response(GetObjectTaggingOutput {
            tag_set,
            version_id: req.input.version_id.clone(),
        }))
    }

    #[instrument(level = "debug", skip(self, _req))]
    async fn get_object_torrent(&self, _req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        // Torrent functionality is not implemented in RustFS
        // Per S3 API test expectations, return 404 NoSuchKey (not 501 Not Implemented)
        // This allows clients to gracefully handle the absence of torrent support
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let input = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&input.bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;
        // mc cp step 2 GetBucketInfo

        Ok(s3_response(HeadBucketOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedHead, "s3:HeadObject");
        // mc get 2
        let HeadObjectInput {
            bucket,
            key,
            version_id,
            part_number,
            range,
            if_none_match,
            if_match,
            if_modified_since,
            if_unmodified_since,
            ..
        } = req.input.clone();

        // Validate object key
        validate_object_key(&key, "HEAD")?;
        // Parse part number from Option<i32> to Option<usize> with validation
        let part_number: Option<usize> = parse_part_number_i32_to_usize(part_number, "HEAD")?;

        let rs = range.map(|v| match v {
            Range::Int { first, last } => HTTPRangeSpec {
                is_suffix_length: false,
                start: first as i64,
                end: if let Some(last) = last { last as i64 } else { -1 },
            },
            Range::Suffix { length } => HTTPRangeSpec {
                is_suffix_length: true,
                start: length as i64,
                end: -1,
            },
        });

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };
        // Modification Points: Explicitly handles get_object_info errors, distinguishing between object absence and other errors
        let info = match store.get_object_info(&bucket, &key, &opts).await {
            Ok(info) => info,
            Err(err) => {
                // If the error indicates the object or its version was not found, return 404 (NoSuchKey)
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!("Failed to probe children for prefix (bucket: {}, key: {}): {}", bucket, key, e);
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                // Other errors, such as insufficient permissions, still return the original error
                return Err(ApiError::from(err).into());
            }
        };
        if info.delete_marker {
            if opts.version_id.is_none() {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
        }
        if let Some(match_etag) = if_none_match
            && let Some(strong_etag) = match_etag.into_etag()
            && info
                .etag
                .as_ref()
                .is_some_and(|etag| ETag::Strong(etag.clone()) == strong_etag)
        {
            return Err(S3Error::new(S3ErrorCode::NotModified));
        }
        if let Some(modified_since) = if_modified_since {
            // obj_time < givenTime + 1s
            if info.mod_time.is_some_and(|mod_time| {
                let give_time: OffsetDateTime = modified_since.into();
                mod_time < give_time.add(time::Duration::seconds(1))
            }) {
                return Err(S3Error::new(S3ErrorCode::NotModified));
            }
        }
        if let Some(match_etag) = if_match {
            if let Some(strong_etag) = match_etag.into_etag()
                && info
                    .etag
                    .as_ref()
                    .is_some_and(|etag| ETag::Strong(etag.clone()) != strong_etag)
            {
                return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
            }
        } else if let Some(unmodified_since) = if_unmodified_since
            && info.mod_time.is_some_and(|mod_time| {
                let give_time: OffsetDateTime = unmodified_since.into();
                mod_time > give_time.add(time::Duration::seconds(1))
            })
        {
            return Err(S3Error::new(S3ErrorCode::PreconditionFailed));
        }
        let event_info = info.clone();
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!("parse content-type err {} {:?}", &content_type, err);
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        // TODO: range download

        let content_length = info.get_actual_size().map_err(|e| {
            error!("get_actual_size error: {}", e);
            ApiError::from(e)
        })?;

        let metadata_map = info.user_defined.clone();
        let server_side_encryption = metadata_map
            .get("x-amz-server-side-encryption")
            .map(|v| ServerSideEncryption::from(v.clone()));
        let sse_customer_algorithm = metadata_map
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| SSECustomerAlgorithm::from(v.clone()));
        let sse_customer_key_md5 = metadata_map.get("x-amz-server-side-encryption-customer-key-md5").cloned();
        let sse_kms_key_id = metadata_map.get("x-amz-server-side-encryption-aws-kms-key-id").cloned();
        // Prefer explicit storage_class from object info; fall back to persisted metadata header.
        let storage_class = info
            .storage_class
            .clone()
            .or_else(|| metadata_map.get("x-amz-storage-class").cloned())
            .filter(|s| !s.is_empty())
            .map(StorageClass::from);
        let mut checksum_crc32 = None;
        let mut checksum_crc32c = None;
        let mut checksum_sha1 = None;
        let mut checksum_sha256 = None;
        let mut checksum_crc64nvme = None;
        let mut checksum_type = None;

        // checksum
        if let Some(checksum_mode) = req.headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (checksums, _is_multipart) = info
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
        }
        // Extract standard HTTP headers from user_defined metadata
        // Note: These headers are stored with lowercase keys by extract_metadata_from_mime
        let cache_control = metadata_map.get("cache-control").cloned();
        let content_disposition = metadata_map.get("content-disposition").cloned();
        let content_language = metadata_map.get("content-language").cloned();
        let expires = info.expires.map(Timestamp::from);

        // Calculate tag count from user_tags already in ObjectInfo
        // This avoids an additional API call since user_tags is already populated by get_object_info
        let tag_count = if !info.user_tags.is_empty() {
            let tag_set = decode_tags(&info.user_tags);
            tag_set.len()
        } else {
            0
        };
        let output = HeadObjectOutput {
            content_length: Some(content_length),
            content_type,
            content_encoding: info.content_encoding.clone(),
            cache_control,
            content_disposition,
            content_language,
            expires,
            last_modified,
            e_tag: info.etag.map(|etag| to_s3s_etag(&etag)),
            metadata: filter_object_metadata(&metadata_map),
            version_id: info.version_id.map(|v| v.to_string()),
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: sse_kms_key_id,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            storage_class,
            // metadata: object_metadata,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(event_info).version_id(version_id);

        // NOTE ON CORS:
        // Bucket-level CORS headers are intentionally applied only for object retrieval
        // operations (GET/HEAD) via `wrap_response_with_cors`. Other S3 operations that
        // interact with objects (PUT/POST/DELETE/LIST, etc.) rely on the system-level
        // CORS layer instead. In case both are applicable, this bucket-level CORS logic
        // takes precedence for these read operations.
        let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;

        if let Some(content_disposition) = metadata_map.get("content-disposition") {
            // Only set Content-Disposition from metadata if it has not already been set
            if response.headers.get(http::header::CONTENT_DISPOSITION).is_none() {
                match HeaderValue::from_str(content_disposition) {
                    Ok(header_value) => {
                        response.headers.insert(http::header::CONTENT_DISPOSITION, header_value);
                    }
                    Err(err) => {
                        warn!(
                            "Failed to parse content-disposition metadata value `{}` into HeaderValue: {}",
                            content_disposition, err
                        );
                    }
                }
            }
        }

        if !response.headers.contains_key(http::header::CONTENT_TYPE)
            && let Some(content_type) = metadata_map.get("content-type")
            && let Ok(header_value) = HeaderValue::from_str(content_type)
        {
            response.headers.insert(http::header::CONTENT_TYPE, header_value);
        }

        // Add x-amz-tagging-count header if object has tags
        // Per S3 API spec, this header should be present in HEAD object response when tags exist
        if tag_count > 0 {
            let header_name = http::HeaderName::from_static(AMZ_TAG_COUNT);
            if let Ok(header_value) = tag_count.to_string().parse::<HeaderValue>() {
                response.headers.insert(header_name, header_value);
            } else {
                warn!("Failed to parse x-amz-tagging-count header value, skipping");
            }
        }
        if let Some(retain_date) = metadata_map
            .get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(retain_date)
        {
            response.headers.insert(header_name, header_value);
        }
        if let Some(mode) = metadata_map
            .get(AMZ_OBJECT_LOCK_MODE_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_MODE))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_MODE_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(mode)
        {
            response.headers.insert(header_name, header_value);
        }
        if let Some(legal_hold) = metadata_map
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .or_else(|| metadata_map.get(AMZ_OBJECT_LOCK_LEGAL_HOLD))
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(legal_hold)
        {
            response.headers.insert(header_name, header_value);
        }

        if let Some(amz_restore) = metadata_map.get(X_AMZ_RESTORE.as_str()) {
            let Ok(restore_status) = parse_restore_obj_status(amz_restore) else {
                return Err(S3Error::with_message(S3ErrorCode::Custom("ErrMeta".into()), "parse amz_restore failed."));
            };
            if let Ok(header_value) = HeaderValue::from_str(restore_status.to_string2().as_str()) {
                response.headers.insert(X_AMZ_RESTORE, header_value);
            }
        }
        if let Some(amz_restore_request_date) = metadata_map.get(AMZ_RESTORE_REQUEST_DATE)
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_RESTORE_REQUEST_DATE.as_bytes())
        {
            let Ok(amz_restore_request_date) = OffsetDateTime::parse(amz_restore_request_date, &Rfc3339) else {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("ErrMeta".into()),
                    "parse amz_restore_request_date failed.",
                ));
            };
            let Ok(amz_restore_request_date) = amz_restore_request_date.format(&RFC1123) else {
                return Err(S3Error::with_message(
                    S3ErrorCode::Custom("ErrMeta".into()),
                    "format amz_restore_request_date failed.",
                ));
            };
            if let Ok(header_value) = HeaderValue::from_str(&amz_restore_request_date) {
                response.headers.insert(header_name, header_value);
            }
        }
        if let Some(amz_restore_expiry_days) = metadata_map.get(AMZ_RESTORE_EXPIRY_DAYS)
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_RESTORE_EXPIRY_DAYS.as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(amz_restore_expiry_days)
        {
            response.headers.insert(header_name, header_value);
        }

        let result = Ok(response);
        let _ = helper.complete(&result);

        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // mc ls

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let mut req = req;

        if req.credentials.as_ref().is_none_or(|cred| cred.access_key.is_empty()) {
            return Err(access_denied_error());
        }

        let bucket_infos = if let Err(e) = authorize_request(&mut req, Action::S3Action(S3Action::ListAllMyBucketsAction)).await {
            if e.code() != &S3ErrorCode::AccessDenied {
                return Err(e);
            }

            let mut list_bucket_infos = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

            list_bucket_infos = futures::stream::iter(list_bucket_infos)
                .filter_map(|info| async {
                    let mut req_clone = req.clone();
                    let req_info = req_clone.extensions.get_mut::<ReqInfo>().expect("ReqInfo not found");
                    req_info.bucket = Some(info.name.clone());

                    if authorize_request(&mut req_clone, Action::S3Action(S3Action::ListBucketAction))
                        .await
                        .is_ok()
                        || authorize_request(&mut req_clone, Action::S3Action(S3Action::GetBucketLocationAction))
                            .await
                            .is_ok()
                    {
                        Some(info)
                    } else {
                        None
                    }
                })
                .collect()
                .await;

            if list_bucket_infos.is_empty() {
                return Err(access_denied_error());
            }
            list_bucket_infos
        } else {
            store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?
        };

        let output = build_list_buckets_output(&bucket_infos);
        Ok(s3_response(output))
    }

    async fn list_multipart_uploads(
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

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let parsed = parse_list_multipart_uploads_params(prefix, key_marker, max_uploads)?;

        let result = store
            .list_multipart_uploads(
                &bucket,
                &parsed.prefix,
                delimiter,
                parsed.key_marker,
                upload_id_marker,
                parsed.max_uploads,
            )
            .await
            .map_err(ApiError::from)?;

        let output = build_list_multipart_uploads_output(bucket, parsed.prefix, result);

        Ok(s3_response(output))
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        let ListObjectVersionsInput {
            bucket,
            delimiter,
            key_marker,
            version_id_marker,
            max_keys,
            prefix,
            ..
        } = req.input;

        let parsed = parse_list_object_versions_params(prefix, delimiter, key_marker, version_id_marker, max_keys)?;

        let store = get_validated_store(&bucket).await?;

        let object_infos = store
            .list_object_versions(
                &bucket,
                &parsed.prefix,
                parsed.key_marker,
                parsed.version_id_marker,
                parsed.delimiter.clone(),
                parsed.max_keys,
            )
            .await
            .map_err(ApiError::from)?;

        let output = build_list_object_versions_output(object_infos, bucket, parsed.prefix, parsed.delimiter, parsed.max_keys);

        Ok(s3_response(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        // Capture the original marker from the request before conversion
        // S3 API requires the marker field to be echoed back in the response
        let request_marker = req.input.marker.clone();

        let v2_resp = self
            .list_objects_v2(req.map_input(|v1| {
                let mut v2: ListObjectsV2Input = v1.into();
                v2.fetch_owner = Some(true);
                v2
            }))
            .await?;

        Ok(v2_resp.map_output(move |v2| build_list_objects_output(v2, request_marker.clone())))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        // warn!("list_objects_v2 req {:?}", &req.input);
        let ListObjectsV2Input {
            bucket,
            continuation_token,
            delimiter,
            encoding_type,
            fetch_owner,
            max_keys,
            prefix,
            start_after,
            ..
        } = req.input;

        let parsed = parse_list_objects_v2_params(prefix, delimiter, max_keys, continuation_token, start_after)?;
        validate_list_object_unordered_with_delimiter(parsed.delimiter.as_ref(), req.uri.query())?;

        let store = get_validated_store(&bucket).await?;
        let fetch_owner = fetch_owner.unwrap_or_default();

        let incl_deleted = req
            .headers
            .get(rustfs_utils::http::headers::RUSTFS_INCLUDE_DELETED)
            .is_some_and(|v| v.to_str().unwrap_or_default() == "true");

        let object_infos = store
            .list_objects_v2(
                &bucket,
                &parsed.prefix,
                parsed.decoded_continuation_token,
                parsed.delimiter.clone(),
                parsed.max_keys,
                fetch_owner,
                parsed.start_after_for_query,
                incl_deleted,
            )
            .await
            .map_err(ApiError::from)?;

        let output = build_list_objects_v2_output(
            object_infos,
            fetch_owner,
            parsed.max_keys,
            bucket,
            parsed.prefix,
            parsed.delimiter,
            encoding_type,
            parsed.response_continuation_token,
            parsed.response_start_after,
        );

        Ok(s3_response(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        let ListPartsInput {
            bucket,
            key,
            upload_id,
            part_number_marker,
            max_parts,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let parsed = parse_list_parts_params(part_number_marker, max_parts)?;

        let res = store
            .list_object_parts(
                &bucket,
                &key,
                &upload_id,
                parsed.part_number_marker,
                parsed.max_parts,
                &ObjectOptions::default(),
            )
            .await
            .map_err(ApiError::from)?;

        let output = build_list_parts_output(res);
        Ok(s3_response(output))
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let PutBucketAclInput {
            bucket,
            acl,
            access_control_policy,
            ..
        } = req.input;

        // TODO:checkRequestAuthType

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if let Some(canned_acl) = acl {
            if canned_acl.as_str() != BucketCannedACL::PRIVATE {
                return Err(s3_error!(NotImplemented));
            }
        } else {
            let is_full_control = access_control_policy.is_some_and(|v| {
                v.grants.is_some_and(|gs| {
                    //
                    !gs.is_empty()
                        && gs.first().is_some_and(|g| {
                            g.to_owned()
                                .permission
                                .is_some_and(|p| p.as_str() == Permission::FULL_CONTROL)
                        })
                })
            });

            if !is_full_control {
                return Err(s3_error!(NotImplemented));
            }
        }
        Ok(s3_response(PutBucketAclOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let PutBucketCorsInput {
            bucket,
            cors_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = try_!(serialize(&cors_configuration));

        metadata_sys::update(&bucket, BUCKET_CORS_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(PutBucketCorsOutput::default()))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let PutBucketEncryptionInput {
            bucket,
            server_side_encryption_configuration,
            ..
        } = req.input;

        info!("sse_config {:?}", &server_side_encryption_configuration);

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // TODO: check kms

        let data = try_!(serialize(&server_side_encryption_configuration));
        metadata_sys::update(&bucket, BUCKET_SSECONFIG, data)
            .await
            .map_err(ApiError::from)?;
        Ok(s3_response(PutBucketEncryptionOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = lifecycle_configuration else { return Err(s3_error!(InvalidArgument)) };

        let rcfg = metadata_sys::get_object_lock_config(&bucket).await;
        if let Ok(rcfg) = rcfg
            && let Err(err) = input_cfg.validate(&rcfg.0).await
        {
            //return Err(S3Error::with_message(S3ErrorCode::Custom("BucketLockValidateFailed".into()), err.to_string()));
            return Err(S3Error::with_message(S3ErrorCode::Custom("ValidateFailed".into()), err.to_string()));
        }

        if let Err(err) = validate_transition_tier(&input_cfg).await {
            //warn!("lifecycle_configuration add failed, err: {:?}", err);
            return Err(S3Error::with_message(S3ErrorCode::Custom("CustomError".into()), err.to_string()));
        }

        let data = try_!(serialize(&input_cfg));
        metadata_sys::update(&bucket, BUCKET_LIFECYCLE_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(PutBucketLifecycleConfigurationOutput::default()))
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let PutBucketNotificationConfigurationInput {
            bucket,
            notification_configuration,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        //  Verify that the bucket exists
        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        //  Persist the new notification configuration
        let data = try_!(serialize(&notification_configuration));
        metadata_sys::update(&bucket, BUCKET_NOTIFICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // Determine region (BucketInfo has no region field) -> use global region or default
        let region = rustfs_ecstore::global::get_global_region().unwrap_or_else(|| req.region.clone().unwrap_or_default());

        // Purge old rules and resolve new rules in parallel
        let clear_rules = notifier_global::clear_bucket_notification_rules(&bucket);
        let parse_rules = async {
            let mut event_rules = Vec::new();

            process_queue_configurations(&mut event_rules, notification_configuration.queue_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_topic_configurations(&mut event_rules, notification_configuration.topic_configurations.clone(), |arn_str| {
                ARN::parse(arn_str)
                    .map(|arn| arn.target_id)
                    .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
            })?;
            process_lambda_configurations(
                &mut event_rules,
                notification_configuration.lambda_function_configurations.clone(),
                |arn_str| {
                    ARN::parse(arn_str)
                        .map(|arn| arn.target_id)
                        .map_err(|e| TargetIDError::InvalidFormat(e.to_string()))
                },
            )?;

            Ok::<_, TargetIDError>(event_rules)
        };

        let (clear_result, event_rules_result) = tokio::join!(clear_rules, parse_rules);

        clear_result.map_err(|e| s3_error!(InternalError, "Failed to clear rules: {e}"))?;
        let event_rules =
            event_rules_result.map_err(|e| s3_error!(InvalidArgument, "Invalid ARN in notification configuration: {e}"))?;
        warn!("notify event rules: {:?}", &event_rules);

        // Add a new notification rule
        notifier_global::add_event_specific_rules(&bucket, &region, &event_rules)
            .await
            .map_err(|e| s3_error!(InternalError, "Failed to add rules: {e}"))?;

        Ok(s3_response(PutBucketNotificationConfigurationOutput {}))
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let PutBucketPolicyInput { bucket, policy, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // warn!("input policy {}", &policy);

        let cfg: BucketPolicy =
            serde_json::from_str(&policy).map_err(|e| s3_error!(InvalidArgument, "parse policy failed {:?}", e))?;

        if let Err(err) = cfg.is_valid() {
            warn!("put_bucket_policy err input {:?}, {:?}", &policy, err);
            return Err(s3_error!(MalformedPolicy));
        }

        // Preserve the original JSON text so GetBucketPolicy can return byte-for-byte content.
        // s3-tests expects exact string round-trip equality.
        let data = policy.into_bytes();

        metadata_sys::update(&bucket, BUCKET_POLICY_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(PutBucketPolicyOutput {}))
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let PutBucketReplicationInput {
            bucket,
            replication_configuration,
            ..
        } = req.input;
        warn!("put bucket replication");

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // TODO: check enable, versioning enable
        let data = try_!(serialize(&replication_configuration));

        metadata_sys::update(&bucket, BUCKET_REPLICATION_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(PutBucketReplicationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let PutBucketTaggingInput { bucket, tagging, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let data = try_!(serialize(&tagging));

        metadata_sys::update(&bucket, BUCKET_TAGGING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        Ok(s3_response(Default::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let PutBucketVersioningInput {
            bucket,
            versioning_configuration,
            ..
        } = req.input;

        // TODO: check other sys
        // check site replication enable
        // check bucket object lock enable
        // check replication suspended

        let data = try_!(serialize(&versioning_configuration));

        metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // TODO: globalSiteReplicationSys.BucketMetaHook

        Ok(s3_response(PutBucketVersioningOutput {}))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        crate::storage::objects::GLOBAL_OBJECTS.put_object(req).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        let PutObjectAclInput {
            bucket,
            key,
            acl,
            access_control_policy,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        if let Err(e) = store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, format!("{e}")));
        }

        if let Some(canned_acl) = acl {
            if canned_acl.as_str() != BucketCannedACL::PRIVATE {
                return Err(s3_error!(NotImplemented));
            }
        } else {
            let is_full_control = access_control_policy.is_some_and(|v| {
                v.grants.is_some_and(|gs| {
                    //
                    !gs.is_empty()
                        && gs.first().is_some_and(|g| {
                            g.to_owned()
                                .permission
                                .is_some_and(|p| p.as_str() == Permission::FULL_CONTROL)
                        })
                })
            });

            if !is_full_control {
                return Err(s3_error!(NotImplemented));
            }
        }
        Ok(s3_response(PutObjectAclOutput::default()))
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutLegalHold, "s3:PutObjectLegalHold");
        let PutObjectLegalHoldInput {
            bucket,
            key,
            legal_hold,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        // check object lock
        validate_bucket_object_lock_enabled(&bucket).await?;
        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let eval_metadata = parse_object_lock_legal_hold(legal_hold)?;

        let popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        let info = store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(info).version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let _ = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok(_) => {}
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::InvalidBucketState,
                        "Object Lock configuration cannot be enabled on existing buckets".to_string(),
                    ));
                }
                warn!("get_object_lock_config err {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "Failed to get bucket ObjectLockConfiguration".to_string(),
                ));
            }
        };

        let data = try_!(serialize(&input_cfg));

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // When Object Lock is enabled, automatically enable versioning if not already enabled
        // This matches AWS S3 and MinIO behavior
        let versioning_config = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let enable_versioning_config = VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            };
            let versioning_data = try_!(serialize(&enable_versioning_config));
            metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, versioning_data)
                .await
                .map_err(ApiError::from)?;
        }

        Ok(s3_response(PutObjectLockConfigurationOutput::default()))
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutRetention, "s3:PutObjectRetention");
        let PutObjectRetentionInput {
            bucket,
            key,
            retention,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        // check object lock
        validate_bucket_object_lock_enabled(&bucket).await?;

        // Extract new retain_until_date for validation
        let new_retain_until = retention
            .as_ref()
            .and_then(|r| r.retain_until_date.as_ref())
            .map(|d| OffsetDateTime::from(d.clone()));

        // Check if object already has retention and if modification is allowed
        // This follows AWS S3 and MinIO behavior:
        // - COMPLIANCE mode: can only extend retention period, never shorten
        // - GOVERNANCE mode: requires bypass header to modify/shorten retention
        //
        // TODO: Known race condition (fix in future PR)
        // There's a TOCTOU (time-of-check-time-of-use) window between retention check here
        // and the actual update at put_object_metadata. In theory:
        //   Thread A: reads GOVERNANCE mode, checks bypass header
        //   Thread B: updates retention to COMPLIANCE mode
        //   Thread A: modifies retention, bypassing what is now COMPLIANCE mode
        // This violates S3 spec that COMPLIANCE cannot be modified even with bypass.
        // Fix options:
        // 1. Pass expected retention mode to storage layer, verify before update
        // 2. Use optimistic concurrency with version/etag checks
        // 3. Perform check within the same lock scope as update in storage layer
        // Current mitigation: Storage layer has fast_lock_manager which provides some protection
        let check_opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        if let Ok(existing_obj_info) = store.get_object_info(&bucket, &key, &check_opts).await {
            let bypass_governance = has_bypass_governance_header(&req.headers);
            if let Some(block_reason) =
                check_retention_for_modification(&existing_obj_info.user_defined, new_retain_until, bypass_governance)
            {
                return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
            }
        }

        let eval_metadata = parse_object_lock_retention(retention)?;

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.eval_metadata = Some(eval_metadata);

        let object_info = store.put_object_metadata(&bucket, &key, &opts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(s3_response(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutTagging, "s3:PutObjectTagging");
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        if tagging.tag_set.len() > 10 {
            // TOTO: Note that Amazon S3 limits the maximum number of tags to 10 tags per object.
            // Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-tagging.html
            // Reference: https://docs.aws.amazon.com/zh_cn/AmazonS3/latest/API/API_PutObjectTagging.html
            // https://github.com/minio/mint/blob/master/run/core/aws-sdk-go-v2/main.go#L1647
            error!("Tag set exceeds maximum of 10 tags: {}", tagging.tag_set.len());
            return Err(s3_error!(InvalidTag, "Cannot have more than 10 tags per object"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let mut tag_keys = std::collections::HashSet::with_capacity(tagging.tag_set.len());
        for tag in &tagging.tag_set {
            let key = tag.key.as_ref().filter(|k| !k.is_empty()).ok_or_else(|| {
                error!("Empty tag key");
                s3_error!(InvalidTag, "Tag key cannot be empty")
            })?;

            if key.len() > 128 {
                error!("Tag key too long: {} bytes", key.len());
                return Err(s3_error!(InvalidTag, "Tag key is too long, maximum allowed length is 128 characters"));
            }

            // allow to set the value of a tag to an empty string, but cannot set it to a null value.
            // Reference：https://docs.aws.amazon.com/zh_cn/AWSEC2/latest/UserGuide/Using_Tags.html#tag-restrictions
            let value = tag.value.as_ref().ok_or_else(|| {
                error!("Null tag value");
                s3_error!(InvalidTag, "Tag value cannot be null")
            })?;

            if value.len() > 256 {
                error!("Tag value too long: {} bytes", value.len());
                return Err(s3_error!(InvalidTag, "Tag value is too long, maximum allowed length is 256 characters"));
            }

            if !tag_keys.insert(key) {
                error!("Duplicate tag key: {}", key);
                return Err(s3_error!(InvalidTag, "Cannot provide multiple Tags with the same key"));
            }
        }

        let tags = encode_tags(tagging.tag_set);
        debug!("Encoded tags: {}", tags);

        // TODO: getOpts, Replicate
        // Support versioned objects
        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: self.parse_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        store.put_object_tags(&bucket, &object, &tags, &opts).await.map_err(|e| {
            error!("Failed to put object tags: {}", e);
            counter!("rustfs.put_object_tagging.failure").increment(1);
            ApiError::from(e)
        })?;

        // Invalidate cache for the tagged object
        let manager = get_concurrency_manager();
        let version_id = req.input.version_id.clone();
        let cache_key = ConcurrencyManager::make_cache_key(&bucket, &object, version_id.clone().as_deref());
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&bucket, &object, version_id.as_deref())
                .await;
            debug!("Cache invalidated for tagged object: {}", cache_key);
        });

        // Add metrics
        counter!("rustfs.put_object_tagging.success").increment(1);

        let version_id_resp = req.input.version_id.clone().unwrap_or_default();
        helper = helper.version_id(version_id_resp);

        let result = Ok(s3_response(PutObjectTaggingOutput {
            version_id: req.input.version_id.clone(),
        }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "put").record(duration.as_secs_f64());
        result
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        let RestoreObjectInput {
            bucket,
            key: object,
            restore_request: rreq,
            version_id,
            ..
        } = req.input.clone();

        let rreq = rreq.ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::Custom("ErrValidRestoreObject".into()), "restore request is required")
        })?;

        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        let version_id_str = version_id.clone().unwrap_or_default();
        let opts = post_restore_opts(&version_id_str, &bucket, &object)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrPostRestoreOpts".into()), "restore object failed."))?;

        let mut obj_info = store
            .get_object_info(&bucket, &object, &opts)
            .await
            .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrInvalidObjectState".into()), "restore object failed."))?;

        // Check if object is in a transitioned state
        if obj_info.transitioned_object.status != lifecycle::TRANSITION_COMPLETE {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrInvalidTransitionedState".into()),
                "restore object failed.",
            ));
        }

        // Validate restore request
        if let Err(e) = rreq.validate(store.clone()) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrValidRestoreObject".into()),
                format!("Restore object validation failed: {}", e),
            ));
        }

        // Check if restore is already in progress
        if obj_info.restore_ongoing && (rreq.type_.as_ref().is_none_or(|t| t.as_str() != "SELECT")) {
            return Err(S3Error::with_message(
                S3ErrorCode::Custom("ErrObjectRestoreAlreadyInProgress".into()),
                "restore object failed.",
            ));
        }

        let mut already_restored = false;
        if let Some(restore_expires) = obj_info.restore_expires
            && !obj_info.restore_ongoing
            && restore_expires.unix_timestamp() != 0
        {
            already_restored = true;
        }

        let restore_expiry = lifecycle::expected_expiry_time(OffsetDateTime::now_utc(), *rreq.days.as_ref().unwrap_or(&1));
        let mut metadata = obj_info.user_defined.clone();

        let mut header = HeaderMap::new();

        let obj_info_ = obj_info.clone();
        if rreq.type_.as_ref().is_none_or(|t| t.as_str() != "SELECT") {
            obj_info.metadata_only = true;
            metadata.insert(AMZ_RESTORE_EXPIRY_DAYS.to_string(), rreq.days.unwrap_or(1).to_string());
            metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), OffsetDateTime::now_utc().format(&Rfc3339).unwrap());
            if already_restored {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(false),
                        restore_expiry_date: Some(Timestamp::from(restore_expiry)),
                    }
                    .to_string(),
                );
            } else {
                metadata.insert(
                    X_AMZ_RESTORE.as_str().to_string(),
                    RestoreStatus {
                        is_restore_in_progress: Some(true),
                        restore_expiry_date: Some(Timestamp::from(OffsetDateTime::now_utc())),
                    }
                    .to_string(),
                );
            }
            obj_info.user_defined = metadata;

            store
                .clone()
                .copy_object(
                    &bucket,
                    &object,
                    &bucket,
                    &object,
                    &mut obj_info,
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        ..Default::default()
                    },
                    &ObjectOptions {
                        version_id: obj_info_.version_id.map(|v| v.to_string()),
                        mod_time: obj_info_.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| S3Error::with_message(S3ErrorCode::Custom("ErrCopyObject".into()), "restore object failed."))?;

            if already_restored {
                let output = RestoreObjectOutput {
                    request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
                    restore_output_path: None,
                };
                return Ok(s3_response(output));
            }
        }

        // Handle output location for SELECT requests
        if let Some(output_location) = &rreq.output_location
            && let Some(s3) = &output_location.s3
            && !s3.bucket_name.is_empty()
        {
            let restore_object = Uuid::new_v4().to_string();
            header.insert(
                X_AMZ_RESTORE_OUTPUT_PATH,
                format!("{}{}{}", s3.bucket_name, s3.prefix, restore_object).parse().unwrap(),
            );
        }

        // Spawn restoration task in the background
        let store_clone = store.clone();
        let bucket_clone = bucket.clone();
        let object_clone = object.clone();
        let rreq_clone = rreq.clone();
        let version_id_clone = version_id.clone();

        tokio::spawn(async move {
            let opts = ObjectOptions {
                transition: TransitionOptions {
                    restore_request: rreq_clone,
                    restore_expiry,
                    ..Default::default()
                },
                version_id: version_id_clone,
                ..Default::default()
            };

            if let Err(err) = store_clone
                .restore_transitioned_object(&bucket_clone, &object_clone, &opts)
                .await
            {
                warn!(
                    "unable to restore transitioned bucket/object {}/{}: {}",
                    bucket_clone,
                    object_clone,
                    err.to_string()
                );
                // Note: Errors from background tasks cannot be returned to client
                // Consider adding to monitoring/metrics system
            } else {
                info!("successfully restored transitioned object: {}/{}", bucket_clone, object_clone);
            }
        });

        let output = RestoreObjectOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
            restore_output_path: None,
        };

        Ok(S3Response::with_headers(output, header))
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        info!("handle select_object_content");

        let input = Arc::new(req.input);
        info!("{:?}", input);

        let db = get_global_db((*input).clone(), false).await.map_err(|e| {
            error!("get global db failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;
        let query = Query::new(Context { input: input.clone() }, input.request.expression.clone());
        let result = db
            .execute(&query)
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?;

        let results = result.result().chunk_result().await.unwrap().to_vec();

        let mut buffer = Vec::new();
        if input.request.output_serialization.csv.is_some() {
            let mut csv_writer = CsvWriterBuilder::new().with_header(false).build(&mut buffer);
            for batch in results {
                csv_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "can't encode output to csv. e: {}", e.to_string()))?;
            }
        } else if input.request.output_serialization.json.is_some() {
            let mut json_writer = JsonWriterBuilder::new()
                .with_explicit_nulls(true)
                .build::<_, JsonArray>(&mut buffer);
            for batch in results {
                json_writer
                    .write(&batch)
                    .map_err(|e| s3_error!(InternalError, "can't encode output to json. e: {}", e.to_string()))?;
            }
            json_writer
                .finish()
                .map_err(|e| s3_error!(InternalError, "writer output into json error, e: {}", e.to_string()))?;
        } else {
            return Err(s3_error!(
                InvalidArgument,
                "Unsupported output format. Supported formats are CSV and JSON"
            ));
        }

        let (tx, rx) = mpsc::channel::<S3Result<SelectObjectContentEvent>>(2);
        let stream = ReceiverStream::new(rx);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Cont(ContinuationEvent::default())))
                .await;
            let _ = tx
                .send(Ok(SelectObjectContentEvent::Records(RecordsEvent {
                    payload: Some(Bytes::from(buffer)),
                })))
                .await;
            let _ = tx.send(Ok(SelectObjectContentEvent::End(EndEvent::default()))).await;

            drop(tx);
        });

        Ok(s3_response(SelectObjectContentOutput {
            payload: Some(SelectObjectContentEventStream::new(stream)),
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
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

        // let upload_id =

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
            return Err(not_initialized_error());
        };

        let opts = ObjectOptions::default();
        let mut fi = store
            .get_multipart_info(&bucket, &key, &upload_id, &opts)
            .await
            .map_err(ApiError::from)?;

        // Check if managed encryption will be applied
        let will_apply_managed_encryption = check_encryption_metadata(&fi.user_defined);

        // If managed encryption will be applied, and we have Content-Length, buffer the entire body
        // This is necessary because encryption changes the data size, which causes Content-Length mismatches
        if will_apply_managed_encryption && size.is_some() {
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

        let mut size = size.ok_or_else(|| s3_error!(UnexpectedContent))?;

        // Apply adaptive buffer sizing based on part size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on part size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body_stream.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        // mc cp step 4

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
                return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
            }

            let compress_reader = CompressReader::new(hrd, CompressionAlgorithm::default());
            reader = Box::new(compress_reader);
            size = HashReader::SIZE_PRESERVE_LAYER;
            md5hex = None;
            sha256hex = None;
        }

        let mut reader = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if let Err(err) = reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), size < 0) {
            return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
        }

        // Apply unified SSE encryption for upload_part
        // Note: For SSE-C, the key is provided with each part upload
        // For managed SSE, the encryption material was generated in create_multipart_upload
        let server_side_encryption = fi
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|s| {
                ServerSideEncryption::from_str(s)
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid server-side encryption: {e}"))))
            })
            .transpose()?;
        let ssekms_key_id = fi
            .user_defined
            .get("x-amz-server-side-encryption-aws-kms-key-id")
            .map(|s| s.to_string());
        let part_key = fi.user_defined.get("x-rustfs-encryption-key").cloned();
        let part_nonce = fi.user_defined.get("x-rustfs-encryption-iv").cloned();
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption, // Managed SSE handled below
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

                // Apply encryption wrapper
                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                // Merge encryption metadata
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

        Ok(s3_response(output))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
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
            ..
        } = req.input;

        // Parse source bucket, object and version from copy_source
        let (src_bucket, src_key, src_version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                bucket: ref src_bucket,
                key: ref src_key,
                version_id,
            } => (src_bucket.to_string(), src_key.to_string(), version_id.map(|v| v.to_string())),
        };

        // Parse range if provided (format: "bytes=start-end")
        let rs = if let Some(range_str) = copy_source_range {
            Some(parse_copy_source_range(&range_str)?)
        } else {
            None
        };

        let part_id = part_number as usize;

        // Note: In a real implementation, you would properly validate access
        // For now, we'll skip the detailed authorization check
        let Some(store) = new_object_layer_fn() else {
            return Err(not_initialized_error());
        };

        // Check if multipart upload exists and get its info
        let mut mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &ObjectOptions::default())
            .await
            .map_err(ApiError::from)?;

        // Set up source options
        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;
        src_opts.version_id = src_version_id.clone();

        // Get source object info to validate conditions
        let h = HeaderMap::new();
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

        // Validate copy conditions (simplified for now)
        if let Some(if_match) = copy_source_if_match {
            if let Some(ref etag) = src_info.etag {
                if let Some(strong_etag) = if_match.into_etag() {
                    if ETag::Strong(etag.clone()) != strong_etag {
                        return Err(s3_error!(PreconditionFailed));
                    }
                } else {
                    // Weak ETag in If-Match should fail
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
        // Weak ETag in If-None-Match is ignored (doesn't match)

        // TODO: Implement proper time comparison for if_modified_since and if_unmodified_since
        // For now, we'll skip these conditions

        // Calculate actual range and length
        // Note: These values are used implicitly through the range specification (rs)
        // passed to get_object_reader, which handles the offset and length internally
        let (_start_offset, length) = if let Some(ref range_spec) = rs {
            // For range validation, use the actual logical size of the file
            // For compressed files, this means using the uncompressed size
            let validation_size = match src_info.is_compressed_ok() {
                Ok((_, true)) => {
                    // For compressed files, use actual uncompressed size for range validation
                    src_info.get_actual_size().unwrap_or(src_info.size)
                }
                _ => {
                    // For non-compressed files, use the stored size
                    src_info.size
                }
            };

            range_spec
                .get_offset_length(validation_size)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRange, e.to_string()))?
        } else {
            (0, src_info.size)
        };

        // Create a new reader from the source data with the correct range
        // We need to re-read from the source with the correct range specification
        let h = HeaderMap::new();
        let get_opts = ObjectOptions {
            version_id: src_opts.version_id.clone(),
            versioned: src_opts.versioned,
            version_suspended: src_opts.version_suspended,
            ..Default::default()
        };

        // Get the source object reader once with the validated range
        let src_reader = store
            .get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(ApiError::from)?;

        // Use the same reader for streaming
        let src_stream = src_reader.stream;

        // Check if compression is enabled for this multipart upload
        let is_compressible = mp_info
            .user_defined
            .contains_key(format!("{RESERVED_METADATA_PREFIX_LOWER}compression").as_str());

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(src_stream));

        // Apply unified SSE decryption for source object if encrypted
        // Note: SSE-C for copy source is handled via copy_source_sse_customer_* headers
        let copy_source_sse_customer_key = req.input.copy_source_sse_customer_key.as_ref();
        let copy_source_sse_customer_key_md5 = req.input.copy_source_sse_customer_key_md5.as_ref();
        let src_decryption_request = DecryptionRequest {
            bucket: &src_bucket,
            key: &src_key,
            metadata: &src_info.user_defined,
            sse_customer_key: copy_source_sse_customer_key,
            sse_customer_key_md5: copy_source_sse_customer_key_md5,
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

        // Apply unified SSE encryption for upload_part
        // Note: For SSE-C, the key is provided with each part upload
        // For managed SSE, the encryption material was generated in create_multipart_upload
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
            server_side_encryption, // Managed SSE handled below
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

                // Apply encryption wrapper
                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                // Merge encryption metadata
                mp_info.user_defined.extend(material.metadata);

                (requested_sse, requested_kms_key_id)
            }
            None => (None, None),
        };

        let mut reader = PutObjReader::new(reader);

        // Set up destination options (inherit from multipart upload)
        let dst_opts = ObjectOptions {
            user_defined: mp_info.user_defined.clone(),
            ..Default::default()
        };

        // Write the copied data as a new part
        let part_info = store
            .put_object_part(&bucket, &key, &upload_id, part_id, &mut reader, &dst_opts)
            .await
            .map_err(ApiError::from)?;

        // Create response
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

        Ok(s3_response(output))
    }
}
