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

//! Object application use-case contracts.
#![allow(dead_code)]

use crate::app::context::{AppContext, get_global_app_context};
use crate::config::workload_profiles::get_global_buffer_config;
use crate::error::ApiError;
use crate::storage::access::{ReqInfo, authorize_request, has_bypass_governance_header};
use crate::storage::concurrency::{
    CachedGetObject, ConcurrencyManager, GetObjectGuard, get_concurrency_aware_buffer_size, get_concurrency_manager,
};
use crate::storage::ecfs::*;
use crate::storage::head_prefix::{head_prefix_not_found_message, probe_prefix_has_children};
use crate::storage::helper::OperationHelper;
use crate::storage::options::{
    copy_dst_opts, copy_src_opts, del_opts, extract_metadata, extract_metadata_from_mime_with_object_name,
    filter_object_metadata, get_content_sha256, get_opts, put_opts,
};
use crate::storage::*;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use datafusion::arrow::{
    csv::WriterBuilder as CsvWriterBuilder, json::WriterBuilder as JsonWriterBuilder, json::writer::JsonArray,
};
use futures::StreamExt;
use http::{HeaderMap, HeaderValue, StatusCode};
use metrics::{counter, histogram};
use rustfs_ecstore::StorageAPI;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    lifecycle::{
        bucket_lifecycle_ops::{RestoreRequestOps, post_restore_opts},
        lifecycle::{self, TransitionOptions},
    },
    metadata::{BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG},
    metadata_sys,
    object_lock::objectlock_sys::{BucketObjectLockSys, check_object_lock_for_deletion, check_retention_for_modification},
    quota::QuotaOperation,
    replication::{
        DeletedObjectReplicationInfo, check_replicate_delete, get_must_replicate_options, must_replicate, schedule_replication,
        schedule_replication_delete,
    },
    tagging::{decode_tags, encode_tags},
    utils::serialize,
    versioning::VersioningApi,
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::disk::{error::DiskError, error_reduce::is_all_buckets_not_found};
use rustfs_ecstore::error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{
    BucketOptions, HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOptions, ObjectToDelete, PutObjReader,
};
use rustfs_filemeta::{
    REPLICATE_INCOMING_DELETE, ReplicationStatusType, ReplicationType, RestoreStatusOps, VersionPurgeStatusType,
    parse_restore_obj_status,
};
use rustfs_notify::{EventArgsBuilder, notifier_global};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_rio::{CompressReader, DecryptReader, EncryptReader, EtagReader, HardLimitReader, HashReader, Reader, WarpReader};
use rustfs_s3select_api::{
    object_store::bytes_stream,
    query::{Context, Query},
};
use rustfs_s3select_query::get_global_db;
use rustfs_targets::EventName;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE, RESERVED_METADATA_PREFIX,
    headers::{
        AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE,
        AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
        AMZ_OBJECT_TAGGING, AMZ_RESTORE_EXPIRY_DAYS, AMZ_RESTORE_REQUEST_DATE, AMZ_TAG_COUNT, RESERVED_METADATA_PREFIX_LOWER,
    },
};
use rustfs_utils::path::{is_dir_object, path_join_buf};
use rustfs_utils::{
    CompressionAlgorithm, extract_params_header, extract_resp_elements, get_request_host, get_request_user_agent,
};
use s3s::dto::*;
use s3s::header::{X_AMZ_RESTORE, X_AMZ_RESTORE_OUTPUT_PATH};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::convert::Infallible;
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

pub type ObjectUsecaseResult<T> = Result<T, ApiError>;

fn normalize_delete_objects_version_id(version_id: Option<String>) -> Result<(Option<String>, Option<Uuid>), String> {
    let version_id = version_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    match version_id {
        Some(id) => {
            if id.eq_ignore_ascii_case("null") {
                Ok((Some("null".to_string()), Some(Uuid::nil())))
            } else {
                let uuid = Uuid::parse_str(&id).map_err(|e| e.to_string())?;
                Ok((Some(id), Some(uuid)))
            }
        }
        None => Ok((None, None)),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectRequest {
    pub bucket: String,
    pub key: String,
    pub content_length: Option<i64>,
    pub content_type: Option<String>,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectResponse {
    pub etag: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GetObjectResponse {
    pub etag: Option<String>,
    pub content_length: i64,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectRequest {
    pub bucket: String,
    pub key: String,
    pub version_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteObjectResponse {
    pub delete_marker: Option<bool>,
    pub version_id: Option<String>,
}

#[async_trait::async_trait]
pub trait ObjectUsecase: Send + Sync {
    async fn put_object(&self, req: PutObjectRequest) -> ObjectUsecaseResult<PutObjectResponse>;

    async fn get_object(&self, req: GetObjectRequest) -> ObjectUsecaseResult<GetObjectResponse>;

    async fn delete_object(&self, req: DeleteObjectRequest) -> ObjectUsecaseResult<DeleteObjectResponse>;
}

#[derive(Clone, Default)]
pub struct DefaultObjectUsecase {
    context: Option<Arc<AppContext>>,
}

impl DefaultObjectUsecase {
    pub fn new(context: Arc<AppContext>) -> Self {
        Self { context: Some(context) }
    }

    pub fn without_context() -> Self {
        Self { context: None }
    }

    pub fn from_global() -> Self {
        Self {
            context: get_global_app_context(),
        }
    }

    pub fn context(&self) -> Option<Arc<AppContext>> {
        self.context.clone()
    }

    #[instrument(level = "debug", skip(self, fs, req))]
    pub async fn execute_put_object(&self, fs: &FS, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, "s3:PutObject");
        if req
            .headers
            .get("X-Amz-Meta-Snowball-Auto-Extract")
            .is_some_and(|v| v.to_str().unwrap_or_default() == "true")
        {
            return fs.put_object_extract(req).await;
        }

        let input = req.input;

        // Save SSE-C parameters before moving input
        if let Some(ref storage_class) = input.storage_class
            && !is_valid_storage_class(storage_class.as_str())
        {
            return Err(s3_error!(InvalidStorageClass));
        }
        let PutObjectInput {
            body,
            bucket,
            key,
            acl,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write_acp,
            content_length,
            content_type,
            tagging,
            metadata,
            version_id,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            content_md5,
            ..
        } = input;

        // Validate object key
        validate_object_key(&key, "PUT")?;

        if let Some(acl) = &acl
            && let Ok((config, _)) = metadata_sys::get_public_access_block_config(&bucket).await
            && config.block_public_acls.unwrap_or(false)
            && is_public_canned_acl(acl.as_str())
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        // check quota for put operation
        if let Some(size) = content_length
            && let Some(metadata_sys) = rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys.get()
        {
            let quota_checker = QuotaChecker::new(metadata_sys.clone());

            match quota_checker
                .check_quota(&bucket, QuotaOperation::PutObject, size as u64)
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
                }
                Err(e) => {
                    warn!("Quota check failed for bucket {}: {}, allowing operation", bucket, e);
                }
            }
        }

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let mut size = match content_length {
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

        if size == -1 {
            return Err(s3_error!(UnexpectedContent));
        }

        // Apply adaptive buffer sizing based on file size for optimal streaming performance.
        // Uses workload profile configuration (enabled by default) to select appropriate buffer size.
        // Buffer sizes range from 32KB to 4MB depending on file size and configured workload profile.
        let buffer_size = get_buffer_size_opt_in(size);
        let body = tokio::io::BufReader::with_capacity(
            buffer_size,
            StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))),
        );

        // let body = Box::new(StreamReader::new(body.map(|f| f.map_err(|e| std::io::Error::other(e.to_string())))));

        // let mut reader = PutObjReader::new(body, content_length as usize);

        let store = get_validated_store(&bucket).await?;

        // TDD: Get bucket default encryption configuration
        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        debug!("TDD: bucket_sse_config={:?}", bucket_sse_config);

        // TDD: Determine effective encryption configuration (request overrides bucket default)
        let original_sse = server_side_encryption.clone();
        let effective_sse = server_side_encryption.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                debug!("TDD: Processing bucket SSE config: {:?}", config);
                config.rules.first().and_then(|rule| {
                    debug!("TDD: Processing SSE rule: {:?}", rule);
                    rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                        debug!("TDD: Found SSE default: {:?}", sse);
                        match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256), // fallback to AES256
                        }
                    })
                })
            })
        });
        debug!("TDD: effective_sse={:?} (original={:?})", effective_sse, original_sse);

        let mut effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });

        let mut metadata = metadata.unwrap_or_default();
        let owner = req
            .extensions
            .get::<ReqInfo>()
            .and_then(|info| info.cred.as_ref())
            .map(|cred| owner_from_access_key(&cred.access_key))
            .unwrap_or_else(default_owner);
        let bucket_owner = default_owner();
        let mut stored_acl = stored_acl_from_grant_headers(
            &owner,
            grant_read.map(|v| v.to_string()),
            None,
            grant_read_acp.map(|v| v.to_string()),
            grant_write_acp.map(|v| v.to_string()),
            grant_full_control.map(|v| v.to_string()),
        )?;

        if stored_acl.is_none()
            && let Some(canned) = acl.as_ref()
        {
            stored_acl = Some(stored_acl_from_canned_object(canned.as_str(), &bucket_owner, &owner));
        }

        let stored_acl =
            stored_acl.unwrap_or_else(|| stored_acl_from_canned_object(ObjectCannedACL::PRIVATE, &bucket_owner, &owner));
        let acl_data = serialize_acl(&stored_acl)?;
        metadata.insert(INTERNAL_ACL_METADATA_KEY.to_string(), String::from_utf8_lossy(&acl_data).to_string());

        if let Some(content_type) = content_type {
            metadata.insert("content-type".to_string(), content_type.to_string());
        }

        extract_metadata_from_mime_with_object_name(&req.headers, &mut metadata, true, Some(&key));

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_string());
        }

        // TDD: Store effective SSE information in metadata for GET responses
        if let Some(sse_alg) = &sse_customer_algorithm {
            metadata.insert(
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                sse_alg.as_str().to_string(),
            );
        }
        if let Some(sse_md5) = &sse_customer_key_md5 {
            metadata.insert("x-amz-server-side-encryption-customer-key-md5".to_string(), sse_md5.clone());
        }
        if let Some(sse) = &effective_sse {
            metadata.insert("x-amz-server-side-encryption".to_string(), sse.as_str().to_string());
        }

        if let Some(kms_key_id) = &effective_kms_key_id {
            metadata.insert("x-amz-server-side-encryption-aws-kms-key-id".to_string(), kms_key_id.clone());
        }

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id.clone(), &req.headers, metadata.clone())
            .await
            .map_err(ApiError::from)?;

        let mut reader: Box<dyn Reader> = Box::new(WarpReader::new(body));

        let actual_size = size;

        let mut md5hex = if let Some(base64_md5) = content_md5 {
            let md5 = base64_simd::STANDARD
                .decode_to_vec(base64_md5.as_bytes())
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
            Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
        } else {
            None
        };

        let mut sha256hex = get_content_sha256(&req.headers);

        if is_compressible(&req.headers, &key) && size > MIN_COMPRESSIBLE_SIZE as i64 {
            let algorithm = CompressionAlgorithm::default();
            metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}compression"), algorithm.to_string());

            metadata.insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

            let mut hrd = HashReader::new(reader, size as i64, size as i64, md5hex, sha256hex, false).map_err(ApiError::from)?;

            if let Err(err) = hrd.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
            }

            opts.want_checksum = hrd.checksum();
            opts.user_defined
                .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}compression"), algorithm.to_string());
            opts.user_defined
                .insert(format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size",), size.to_string());

            reader = Box::new(CompressReader::new(hrd, algorithm));
            size = HashReader::SIZE_PRESERVE_LAYER;
            md5hex = None;
            sha256hex = None;
        }

        let mut reader = HashReader::new(reader, size, actual_size, md5hex, sha256hex, false).map_err(ApiError::from)?;

        if size >= 0 {
            if let Err(err) = reader.add_checksum_from_s3s(&req.headers, req.trailing_headers.clone(), false) {
                return Err(ApiError::from(StorageError::other(format!("add_checksum error={err:?}"))).into());
            }

            opts.want_checksum = reader.checksum();
        }

        // Apply SSE-C encryption if customer provided key
        if let (Some(_), Some(sse_key), Some(sse_key_md5_provided)) =
            (&sse_customer_algorithm, &sse_customer_key, &sse_customer_key_md5)
        {
            // Decode the base64 key
            let key_bytes = BASE64_STANDARD
                .decode(sse_key)
                .map_err(|e| ApiError::from(StorageError::other(format!("Invalid SSE-C key: {e}"))))?;

            // Verify key length (should be 32 bytes for AES-256)
            if key_bytes.len() != 32 {
                return Err(ApiError::from(StorageError::other("SSE-C key must be 32 bytes")).into());
            }

            // Convert Vec<u8> to [u8; 32]
            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(&key_bytes[..32]);

            // Verify MD5 hash of the key matches what the client claims
            let computed_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);
            if computed_md5 != *sse_key_md5_provided {
                return Err(ApiError::from(StorageError::other("SSE-C key MD5 mismatch")).into());
            }

            // Store original size for later retrieval during decryption
            let original_size = if size >= 0 { size } else { actual_size };
            metadata.insert(
                "x-amz-server-side-encryption-customer-original-size".to_string(),
                original_size.to_string(),
            );

            // Generate a deterministic nonce from object key for consistency
            let mut nonce = [0u8; 12];
            let nonce_source = format!("{bucket}-{key}");
            let nonce_hash = md5::compute(nonce_source.as_bytes());
            nonce.copy_from_slice(&nonce_hash.0[..12]);

            // Apply encryption
            let encrypt_reader = EncryptReader::new(reader, key_array, nonce);
            reader = HashReader::new(Box::new(encrypt_reader), -1, actual_size, None, None, false).map_err(ApiError::from)?;
        }

        // Apply managed SSE (SSE-S3 or SSE-KMS) when requested
        if sse_customer_algorithm.is_none()
            && let Some(sse_alg) = &effective_sse
            && is_managed_sse(sse_alg)
        {
            let material =
                create_managed_encryption_material(&bucket, &key, sse_alg, effective_kms_key_id.clone(), actual_size).await?;

            let ManagedEncryptionMaterial {
                data_key,
                headers,
                kms_key_id: kms_key_used,
            } = material;

            let key_bytes = data_key.plaintext_key;
            let nonce = data_key.nonce;

            metadata.extend(headers);
            effective_kms_key_id = Some(kms_key_used.clone());

            let encrypt_reader = EncryptReader::new(reader, key_bytes, nonce);
            reader = HashReader::new(Box::new(encrypt_reader), -1, actual_size, None, None, false).map_err(ApiError::from)?;
        }

        let mut reader = PutObjReader::new(reader);

        let mt2 = metadata.clone();

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts.clone());

        let dsc = must_replicate(&bucket, &key, repoptions).await;

        if dsc.replicate_any() {
            let k = format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-timestamp");
            opts.user_defined.insert(k, jiff::Zoned::now().to_string());
            let k = format!("{}{}", RESERVED_METADATA_PREFIX_LOWER, "replication-status");
            opts.user_defined.insert(k, dsc.pending_status().unwrap_or_default());
        }

        let obj_info = store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;

        // Fast in-memory update for immediate quota consistency
        rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        // Invalidate cache for the written object to prevent stale data
        let manager = get_concurrency_manager();
        let put_bucket = bucket.clone();
        let put_key = key.clone();
        let put_version = obj_info.version_id.map(|v| v.to_string());

        helper = helper.object(obj_info.clone());
        if let Some(version_id) = &put_version {
            helper = helper.version_id(version_id.clone());
        }

        let put_version_clone = put_version.clone();
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&put_bucket, &put_key, put_version_clone.as_deref())
                .await;
        });

        let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts);

        let dsc = must_replicate(&bucket, &key, repoptions).await;

        if dsc.replicate_any() {
            schedule_replication(obj_info, store, dsc, ReplicationType::Object).await;
        }

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

        let output = PutObjectOutput {
            e_tag,
            server_side_encryption: effective_sse, // TDD: Return effective encryption config
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            ssekms_key_id: effective_kms_key_id, // TDD: Return effective KMS key ID
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            version_id: put_version,
            ..Default::default()
        };

        // TODO fix response for POST Policy (multipart/form-data) ，wait s3s crate update,fix issue #1564
        // // If it is a POST Policy(multipart/form-data) path, the PutObjectInput carries the success_action_* field
        // // Here, the response is uniformly rewritten, with the default being 204, redirect prioritizing 303, and status supporting 200/201/204
        // if input.success_action_status.is_some() || input.success_action_redirect.is_some() {
        //     let mut form_fields = HashMap::<String, String>::new();
        //     if let Some(v) = &input.success_action_status {
        //         form_fields.insert("success_action_status".to_string(), v.to_string());
        //     }
        //     if let Some(v) = &input.success_action_redirect {
        //         form_fields.insert("success_action_redirect".to_string(), v.to_string());
        //     }
        //
        //     // obj_info.etag has been converted to e_tag (s3s etag) above, so try to pass the original string here
        //     let etag_str = e_tag.as_ref().map(|v| v.as_str());
        //
        //     // Returns using POST semantics: 204/303/201/200
        //     let resp = build_post_object_success_response(&form_fields, &bucket, &key, etag_str, None)?;
        //
        //     // Keep helper event complete (note: (StatusCode, Body) is returned here instead of PutObjectOutput)
        //     let result = Ok(resp);
        //     let _ = helper.complete(&result);
        //     return result;
        // }

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    pub async fn execute_put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutObjectAclInput {
            bucket,
            key,
            acl,
            access_control_policy,
            grant_full_control,
            grant_read,
            grant_read_acp,
            grant_write,
            grant_write_acp,
            version_id,
            ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let info = store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        let bucket_owner = default_owner();
        let existing_owner = info
            .user_defined
            .get(INTERNAL_ACL_METADATA_KEY)
            .and_then(|acl| serde_json::from_str::<StoredAcl>(acl).ok())
            .map(|acl| acl.owner)
            .unwrap_or_else(|| bucket_owner.clone());

        let mut stored_acl = access_control_policy
            .as_ref()
            .map(|policy| stored_acl_from_policy(policy, &existing_owner))
            .transpose()?;

        if stored_acl.is_none() {
            stored_acl = stored_acl_from_grant_headers(
                &existing_owner,
                grant_read.map(|v| v.to_string()),
                grant_write.map(|v| v.to_string()),
                grant_read_acp.map(|v| v.to_string()),
                grant_write_acp.map(|v| v.to_string()),
                grant_full_control.map(|v| v.to_string()),
            )?;
        }

        if stored_acl.is_none()
            && let Some(canned) = acl
        {
            stored_acl = Some(stored_acl_from_canned_object(canned.as_str(), &bucket_owner, &existing_owner));
        }

        let stored_acl =
            stored_acl.unwrap_or_else(|| stored_acl_from_canned_object(ObjectCannedACL::PRIVATE, &bucket_owner, &existing_owner));

        if let Ok((config, _)) = metadata_sys::get_public_access_block_config(&bucket).await
            && config.block_public_acls.unwrap_or(false)
            && stored_acl.grants.iter().any(is_public_grant)
        {
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        let acl_data = serialize_acl(&stored_acl)?;
        let mut eval_metadata = HashMap::new();
        eval_metadata.insert(INTERNAL_ACL_METADATA_KEY.to_string(), String::from_utf8_lossy(&acl_data).to_string());

        let popts = ObjectOptions {
            mod_time: info.mod_time,
            version_id: opts.version_id,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        Ok(S3Response::new(PutObjectAclOutput::default()))
    }

    pub async fn execute_put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutLegalHold, "s3:PutObjectLegalHold");
        let PutObjectLegalHoldInput {
            bucket,
            key,
            legal_hold,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        match metadata_sys::get_object_lock_config(&bucket).await {
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

        let data = serialize(&input_cfg).map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)))?;

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // When Object Lock is enabled, automatically enable versioning if not already enabled.
        // This matches AWS S3 and MinIO behavior.
        let versioning_config = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let enable_versioning_config = VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            };
            let versioning_data = serialize(&enable_versioning_config)
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{}", err)))?;
            metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, versioning_data)
                .await
                .map_err(ApiError::from)?;
        }

        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    pub async fn execute_put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutRetention, "s3:PutObjectRetention");
        let PutObjectRetentionInput {
            bucket,
            key,
            retention,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let new_retain_until = retention
            .as_ref()
            .and_then(|r| r.retain_until_date.as_ref())
            .map(|d| OffsetDateTime::from(d.clone()));

        // TODO(security): Known TOCTOU race condition (fix in future PR).
        //
        // There is a time-of-check-time-of-use (TOCTOU) window between the retention
        // check below (using get_object_info + check_retention_for_modification) and
        // the actual update performed later in put_object_metadata.
        //
        // In theory:
        //   * Thread A reads retention mode = GOVERNANCE and checks the bypass header.
        //   * Thread B updates retention to COMPLIANCE mode.
        //   * Thread A then proceeds to modify retention, still assuming GOVERNANCE,
        //     and effectively bypasses what is now COMPLIANCE mode.
        //
        // This would violate the S3 spec, which states that COMPLIANCE-mode retention
        // cannot be modified even with a bypass header.
        //
        // Possible fixes (to be implemented in a future change):
        //   1. Pass the expected retention mode down to the storage layer and verify
        //      it has not changed immediately before the update.
        //   2. Use optimistic concurrency (e.g., version/etag) so that the update
        //      fails if the object changed between check and update.
        //   3. Perform the retention check inside the same lock/transaction scope as
        //      the metadata update within the storage layer.
        //
        // Current mitigation: the storage layer provides a fast_lock_manager, which
        // offers some protection, but it does not fully eliminate this race.
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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_put_object_tagging(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPutTagging, "s3:PutObjectTagging");
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        if tagging.tag_set.len() > 10 {
            error!("Tag set exceeds maximum of 10 tags: {}", tagging.tag_set.len());
            return Err(s3_error!(InvalidTag, "Cannot have more than 10 tags per object"));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        store.put_object_tags(&bucket, &object, &tags, &opts).await.map_err(|e| {
            error!("Failed to put object tags: {}", e);
            counter!("rustfs.put_object_tagging.failure").increment(1);
            ApiError::from(e)
        })?;

        let manager = get_concurrency_manager();
        let version_id = req.input.version_id.clone();
        let cache_key = ConcurrencyManager::make_cache_key(&bucket, &object, version_id.clone().as_deref());
        tokio::spawn(async move {
            manager
                .invalidate_cache_versioned(&bucket, &object, version_id.as_deref())
                .await;
            debug!("Cache invalidated for tagged object: {}", cache_key);
        });

        counter!("rustfs.put_object_tagging.success").increment(1);

        let version_id_resp = req.input.version_id.clone().unwrap_or_default();
        helper = helper.version_id(version_id_resp);

        let result = Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: req.input.version_id.clone(),
        }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "put").record(duration.as_secs_f64());
        result
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    pub async fn execute_get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            let result = Ok(S3Response::new(output));
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

        // Apply SSE-C decryption if customer provided key and object was encrypted with SSE-C
        let mut final_stream = reader.stream;
        let stored_sse_algorithm = info.user_defined.get("x-amz-server-side-encryption-customer-algorithm");
        let stored_sse_key_md5 = info.user_defined.get("x-amz-server-side-encryption-customer-key-md5");
        let mut managed_encryption_applied = false;
        let mut managed_original_size: Option<i64> = None;

        debug!(
            "GET object metadata check: stored_sse_algorithm={:?}, stored_sse_key_md5={:?}, provided_sse_key={:?}",
            stored_sse_algorithm,
            stored_sse_key_md5,
            req.input.sse_customer_key.is_some()
        );

        if stored_sse_algorithm.is_some() {
            // Object was encrypted with SSE-C, so customer must provide matching key
            if let (Some(sse_key), Some(sse_key_md5_provided)) = (&req.input.sse_customer_key, &req.input.sse_customer_key_md5) {
                // For true multipart objects (more than 1 part), SSE-C decryption is currently not fully implemented
                // Each part needs to be decrypted individually, which requires storage layer changes
                // Note: Single part objects also have info.parts.len() == 1, but they are not true multipart uploads
                if info.parts.len() > 1 {
                    warn!(
                        "SSE-C multipart object detected with {} parts. Currently, multipart SSE-C upload parts are not encrypted during upload_part, so no decryption is needed during GET.",
                        info.parts.len()
                    );

                    // Verify that the provided key MD5 matches the stored MD5 for security
                    if let Some(stored_md5) = stored_sse_key_md5 {
                        debug!("SSE-C MD5 comparison: provided='{}', stored='{}'", sse_key_md5_provided, stored_md5);
                        if sse_key_md5_provided != stored_md5 {
                            error!("SSE-C key MD5 mismatch: provided='{}', stored='{}'", sse_key_md5_provided, stored_md5);
                            return Err(
                                ApiError::from(StorageError::other("SSE-C key does not match object encryption key")).into()
                            );
                        }
                    } else {
                        return Err(ApiError::from(StorageError::other(
                            "Object encrypted with SSE-C but stored key MD5 not found",
                        ))
                        .into());
                    }

                    // Since upload_part currently doesn't encrypt the data (SSE-C code is commented out),
                    // we don't need to decrypt it either. Just return the data as-is.
                    // TODO: Implement proper multipart SSE-C encryption/decryption
                } else {
                    // Verify that the provided key MD5 matches the stored MD5
                    if let Some(stored_md5) = stored_sse_key_md5 {
                        debug!("SSE-C MD5 comparison: provided='{}', stored='{}'", sse_key_md5_provided, stored_md5);
                        if sse_key_md5_provided != stored_md5 {
                            error!("SSE-C key MD5 mismatch: provided='{}', stored='{}'", sse_key_md5_provided, stored_md5);
                            return Err(
                                ApiError::from(StorageError::other("SSE-C key does not match object encryption key")).into()
                            );
                        }
                    } else {
                        return Err(ApiError::from(StorageError::other(
                            "Object encrypted with SSE-C but stored key MD5 not found",
                        ))
                        .into());
                    }

                    // Decode the base64 key
                    let key_bytes = BASE64_STANDARD
                        .decode(sse_key)
                        .map_err(|e| ApiError::from(StorageError::other(format!("Invalid SSE-C key: {e}"))))?;

                    // Verify key length (should be 32 bytes for AES-256)
                    if key_bytes.len() != 32 {
                        return Err(ApiError::from(StorageError::other("SSE-C key must be 32 bytes")).into());
                    }

                    // Convert Vec<u8> to [u8; 32]
                    let mut key_array = [0u8; 32];
                    key_array.copy_from_slice(&key_bytes[..32]);

                    // Verify MD5 hash of the key matches what the client claims
                    let computed_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);
                    if computed_md5 != *sse_key_md5_provided {
                        return Err(ApiError::from(StorageError::other("SSE-C key MD5 mismatch")).into());
                    }

                    // Generate the same deterministic nonce from object key
                    let mut nonce = [0u8; 12];
                    let nonce_source = format!("{bucket}-{key}");
                    let nonce_hash = md5::compute(nonce_source.as_bytes());
                    nonce.copy_from_slice(&nonce_hash.0[..12]);

                    // Apply decryption
                    // We need to wrap the stream in a Reader first since DecryptReader expects a Reader
                    let warp_reader = WarpReader::new(final_stream);
                    let decrypt_reader = DecryptReader::new(warp_reader, key_array, nonce);
                    final_stream = Box::new(decrypt_reader);
                }
            } else {
                return Err(
                    ApiError::from(StorageError::other("Object encrypted with SSE-C but no customer key provided")).into(),
                );
            }
        }

        if stored_sse_algorithm.is_none()
            && let Some((key_bytes, nonce, original_size)) =
                decrypt_managed_encryption_key(&bucket, &key, &info.user_defined).await?
        {
            if info.parts.len() > 1 {
                let (reader, plain_size) = decrypt_multipart_managed_stream(final_stream, &info.parts, key_bytes, nonce)
                    .await
                    .map_err(ApiError::from)?;
                final_stream = reader;
                managed_original_size = Some(plain_size);
            } else {
                let warp_reader = WarpReader::new(final_stream);
                let decrypt_reader = DecryptReader::new(warp_reader, key_bytes, nonce);
                final_stream = Box::new(decrypt_reader);
                managed_original_size = original_size;
            }
            managed_encryption_applied = true;
        }

        // For SSE-C encrypted objects, use the original size instead of encrypted size
        let response_content_length = if stored_sse_algorithm.is_some() {
            if let Some(original_size_str) = info.user_defined.get("x-amz-server-side-encryption-customer-original-size") {
                let original_size = original_size_str.parse::<i64>().unwrap_or(content_length);
                info!(
                    "SSE-C decryption: using original size {} instead of encrypted size {}",
                    original_size, content_length
                );
                original_size
            } else {
                debug!("SSE-C decryption: no original size found, using content_length {}", content_length);
                content_length
            }
        } else if managed_encryption_applied {
            managed_original_size.unwrap_or(content_length)
        } else {
            content_length
        };

        info!("Final response_content_length: {}", response_content_length);

        if stored_sse_algorithm.is_some() || managed_encryption_applied {
            let limit_reader = HardLimitReader::new(Box::new(WarpReader::new(final_stream)), response_content_length);
            final_stream = Box::new(limit_reader);
        }

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
            && !managed_encryption_applied
            && stored_sse_algorithm.is_none()
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
        } else if stored_sse_algorithm.is_some() || managed_encryption_applied {
            // For SSE-C encrypted objects, don't use bytes_stream to limit the stream
            // because DecryptReader needs to read all encrypted data to produce decrypted output
            info!(
                "Managed SSE: Using unlimited stream for decryption with buffer size {}",
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

        // Extract SSE information from metadata for response
        let server_side_encryption = info
            .user_defined
            .get("x-amz-server-side-encryption")
            .map(|v| ServerSideEncryption::from(v.clone()));
        let sse_customer_algorithm = info
            .user_defined
            .get("x-amz-server-side-encryption-customer-algorithm")
            .map(|v| SSECustomerAlgorithm::from(v.clone()));
        let sse_customer_key_md5 = info
            .user_defined
            .get("x-amz-server-side-encryption-customer-key-md5")
            .cloned();
        let ssekms_key_id = info.user_defined.get("x-amz-server-side-encryption-aws-kms-key-id").cloned();

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

    pub async fn execute_get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let GetObjectAclInput {
            bucket, key, version_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let info = store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        let bucket_owner = default_owner();
        let object_owner = info
            .user_defined
            .get(INTERNAL_ACL_METADATA_KEY)
            .and_then(|acl| serde_json::from_str::<StoredAcl>(acl).ok())
            .map(|acl| acl.owner)
            .unwrap_or_else(default_owner);

        let stored_acl = info
            .user_defined
            .get(INTERNAL_ACL_METADATA_KEY)
            .map(|acl| parse_acl_json_or_canned_object(acl, &bucket_owner, &object_owner))
            .unwrap_or_else(|| stored_acl_from_canned_object(ObjectCannedACL::PRIVATE, &bucket_owner, &object_owner));

        let mut sorted_grants = stored_acl.grants.clone();
        sorted_grants.sort_by_key(|grant| grant.grantee.grantee_type != "Group");
        let grants = sorted_grants.iter().map(stored_grant_to_dto).collect();

        Ok(S3Response::new(GetObjectAclOutput {
            grants: Some(grants),
            owner: Some(stored_owner_to_dto(&stored_acl.owner)),
            ..Default::default()
        }))
    }

    pub async fn execute_get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedAttributes, "s3:GetObjectAttributes");
        let GetObjectAttributesInput { bucket, key, .. } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    pub async fn execute_get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGetLegalHold, "s3:GetObjectLegalHold");
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn execute_get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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

        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration,
        }))
    }

    pub async fn execute_get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedGetRetention, "s3:GetObjectRetention");
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_get_object_tagging(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let start_time = std::time::Instant::now();
        let GetObjectTaggingInput { bucket, key: object, .. } = req.input;

        info!("Starting get_object_tagging for bucket: {}, object: {}", bucket, object);

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
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

        counter!("rustfs.get_object_tagging.success").increment(1);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "get").record(duration.as_secs_f64());
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: req.input.version_id.clone(),
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        let effective_sse = requested_sse.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| match sse.sse_algorithm.as_str() {
                            "AES256" => Some(ServerSideEncryption::from_static(ServerSideEncryption::AES256)),
                            "aws:kms" => Some(ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS)),
                            _ => None,
                        })
                })
            })
        });
        let mut effective_kms_key_id = requested_kms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });

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

        if let Some((key_bytes, nonce, original_size_opt)) =
            decrypt_managed_encryption_key(&src_bucket, &src_key, &src_info.user_defined).await?
        {
            reader = Box::new(DecryptReader::new(reader, key_bytes, nonce));
            if let Some(original) = original_size_opt {
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

        if let Some(ref sse_alg) = effective_sse
            && is_managed_sse(sse_alg)
        {
            let material =
                create_managed_encryption_material(&bucket, &key, sse_alg, effective_kms_key_id.clone(), actual_size).await?;

            let ManagedEncryptionMaterial {
                data_key,
                headers,
                kms_key_id: kms_key_used,
            } = material;

            let key_bytes = data_key.plaintext_key;
            let nonce = data_key.nonce;

            src_info.user_defined.extend(headers.into_iter());
            effective_kms_key_id = Some(kms_key_used.clone());

            let encrypt_reader = EncryptReader::new(reader, key_bytes, nonce);
            reader = HashReader::new(Box::new(encrypt_reader), -1, actual_size, None, None, false).map_err(ApiError::from)?;
        }

        // Apply SSE-C encryption if customer-provided key is specified
        if let (Some(sse_alg), Some(sse_key), Some(sse_md5)) = (&sse_customer_algorithm, &sse_customer_key, &sse_customer_key_md5)
            && sse_alg.as_str() == "AES256"
        {
            let key_bytes = BASE64_STANDARD.decode(sse_key.as_str()).map_err(|e| {
                error!("Failed to decode SSE-C key: {}", e);
                ApiError::from(StorageError::other("Invalid SSE-C key"))
            })?;

            if key_bytes.len() != 32 {
                return Err(ApiError::from(StorageError::other("SSE-C key must be 32 bytes")).into());
            }

            let computed_md5 = BASE64_STANDARD.encode(md5::compute(&key_bytes).0);
            if computed_md5 != sse_md5.as_str() {
                return Err(ApiError::from(StorageError::other("SSE-C key MD5 mismatch")).into());
            }

            // Store original size before encryption
            src_info
                .user_defined
                .insert("x-amz-server-side-encryption-customer-original-size".to_string(), actual_size.to_string());

            let key_array: [u8; 32] = key_bytes
                .try_into()
                .map_err(|_| ApiError::from(StorageError::other("SSE-C key must be 32 bytes")))?;
            // Generate deterministic nonce from bucket-key
            let nonce_source = format!("{bucket}-{key}");
            let nonce_hash = md5::compute(nonce_source.as_bytes());
            let nonce: [u8; 12] = nonce_hash.0[..12]
                .try_into()
                .map_err(|_| ApiError::from(StorageError::other("Failed to derive SSE-C nonce")))?;

            let encrypt_reader = EncryptReader::new(reader, key_array, nonce);
            reader = HashReader::new(Box::new(encrypt_reader), -1, actual_size, None, None, false).map_err(ApiError::from)?;
        }

        src_info.put_object_reader = Some(PutObjReader::new(reader));

        // check quota

        for (k, v) in compress_metadata {
            src_info.user_defined.insert(k, v);
        }

        // Store SSE-C metadata for GET responses
        if let Some(ref sse_alg) = sse_customer_algorithm {
            src_info.user_defined.insert(
                "x-amz-server-side-encryption-customer-algorithm".to_string(),
                sse_alg.as_str().to_string(),
            );
        }
        if let Some(ref sse_md5) = sse_customer_key_md5 {
            src_info
                .user_defined
                .insert("x-amz-server-side-encryption-customer-key-md5".to_string(), sse_md5.clone());
        }

        // check quota for copy operation
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

        // Update quota tracking after successful copy
        if rustfs_ecstore::bucket::metadata_sys::GLOBAL_BucketMetadataSys.get().is_some() {
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
            server_side_encryption: effective_sse,
            ssekms_key_id: effective_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            version_id: dest_version,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_objects(
        &self,
        mut req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_cfg = BucketVersioningSys::get(&bucket).await.unwrap_or_default();
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
            let raw_version_id = obj_id.version_id.clone();
            let (version_id, version_uuid) = match normalize_delete_objects_version_id(raw_version_id.clone()) {
                Ok(parsed) => parsed,
                Err(err) => {
                    delete_results[idx].error = Some(Error {
                        code: Some("NoSuchVersion".to_string()),
                        key: Some(obj_id.key.clone()),
                        message: Some(err),
                        version_id: raw_version_id,
                    });
                    continue;
                }
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
                version_id: version_uuid,
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

            let (goi, gerr) = match store.get_object_info(&bucket, &object.object_name, &opts).await {
                Ok(res) => (res, None),
                Err(e) => (ObjectInfo::default(), Some(e.to_string())),
            };

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

            object_to_delete_index.insert(object.object_name.clone(), idx);
            object_to_delete.push(object);
        }

        let (mut dobjs, errs) = store
            .delete_objects(
                &bucket,
                object_to_delete.clone(),
                ObjectOptions {
                    version_suspended: version_cfg.suspended(),
                    ..Default::default()
                },
            )
            .await;

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

        for dobjs in &delete_results {
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
                    .resp_elements(extract_resp_elements(&S3Response::new(DeleteObjectsOutput::default())))
                    .host(get_request_host(&req_headers))
                    .user_agent(get_request_user_agent(&req_headers))
                    .build();

                    notifier_global::notify(event_args).await;
                }
            }
        });

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_object(&self, mut req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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

        if obj_info.name.is_empty() {
            return Ok(S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT));
        }

        if obj_info.replication_status == ReplicationStatusType::Replica
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

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_for_parse = version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id_for_parse)?.map(Into::into),
            ..Default::default()
        };

        store.delete_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            error!("Failed to delete object tags: {}", e);
            ApiError::from(e)
        })?;

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

        counter!("rustfs.delete_object_tagging.success").increment(1);

        let version_id_resp = version_id.clone().unwrap_or_default();
        helper = helper.version_id(version_id_resp);

        let result = Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "delete").record(duration.as_secs_f64());
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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
            let request_date = OffsetDateTime::now_utc().format(&Rfc3339).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InternalError, format!("format restore request date failed: {}", e))
            })?;
            metadata.insert(AMZ_RESTORE_REQUEST_DATE.to_string(), request_date);
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
                return Ok(S3Response::new(output));
            }
        }

        // Handle output location for SELECT requests
        if let Some(output_location) = &rreq.output_location
            && let Some(s3) = &output_location.s3
            && !s3.bucket_name.is_empty()
        {
            let restore_object = Uuid::new_v4().to_string();
            if let Ok(header_value) = format!("{}{}{}", s3.bucket_name, s3.prefix, restore_object).parse() {
                header.insert(X_AMZ_RESTORE_OUTPUT_PATH, header_value);
            }
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

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

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

        let results = result
            .result()
            .chunk_result()
            .await
            .map_err(|e| s3_error!(InternalError, "{}", e.to_string()))?
            .to_vec();

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

        Ok(S3Response::new(SelectObjectContentOutput {
            payload: Some(SelectObjectContentEventStream::new(stream)),
        }))
    }
}

#[async_trait::async_trait]
impl ObjectUsecase for DefaultObjectUsecase {
    async fn put_object(&self, req: PutObjectRequest) -> ObjectUsecaseResult<PutObjectResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultObjectUsecase::put_object DTO path is not implemented yet",
        )))
    }

    async fn get_object(&self, req: GetObjectRequest) -> ObjectUsecaseResult<GetObjectResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultObjectUsecase::get_object DTO path is not implemented yet",
        )))
    }

    async fn delete_object(&self, req: DeleteObjectRequest) -> ObjectUsecaseResult<DeleteObjectResponse> {
        let _ = req;
        Err(ApiError::from(StorageError::other(
            "DefaultObjectUsecase::delete_object DTO path is not implemented yet",
        )))
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

    #[tokio::test]
    async fn execute_put_object_rejects_invalid_storage_class() {
        let input = PutObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .storage_class(Some(StorageClass::from_static("INVALID-STORAGE-CLASS")))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();
        let fs = FS::new();

        let err = usecase.execute_put_object(&fs, req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidStorageClass);
    }

    #[tokio::test]
    async fn execute_get_object_rejects_zero_part_number() {
        let input = GetObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(0))
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_copy_object_rejects_self_copy_without_replace_directive() {
        let input = CopyObjectInput::builder()
            .copy_source(CopySource::Bucket {
                bucket: "test-bucket".into(),
                key: "test-key".into(),
                version_id: None,
            })
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_copy_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    async fn execute_delete_object_rejects_invalid_object_key() {
        let input = DeleteObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("bad\0key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_rejects_empty_object_list() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_delete_objects_returns_internal_error_when_store_uninitialized() {
        let input = DeleteObjectsInput::builder()
            .bucket("test-bucket".to_string())
            .delete(Delete {
                objects: vec![ObjectIdentifier {
                    key: "test-key".to_string(),
                    version_id: None,
                    ..Default::default()
                }],
                quiet: None,
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_objects(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_delete_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = DeleteObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::DELETE);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_delete_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_acl_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectAclInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_attributes_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectAttributesInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_attributes(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_legal_hold_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectLegalHoldInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_legal_hold(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_retention_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectRetentionInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_retention(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_get_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = GetObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::GET);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_get_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_acl_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectAclInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_acl(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_legal_hold_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectLegalHoldInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_legal_hold(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_lock_configuration_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectLockConfigurationInput::builder()
            .bucket("test-bucket".to_string())
            .object_lock_configuration(Some(ObjectLockConfiguration {
                object_lock_enabled: Some(ObjectLockEnabled::from_static(ObjectLockEnabled::ENABLED)),
                rule: None,
            }))
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_lock_configuration(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_retention_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectRetentionInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_retention(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_put_object_tagging_returns_internal_error_when_store_uninitialized() {
        let input = PutObjectTaggingInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .tagging(Tagging {
                tag_set: vec![Tag {
                    key: Some("k".to_string()),
                    value: Some("v".to_string()),
                }],
            })
            .build()
            .unwrap();

        let req = build_request(input, Method::PUT);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_put_object_tagging(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_head_object_rejects_range_with_part_number() {
        let input = HeadObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .part_number(Some(1))
            .range(Some(Range::Int { first: 0, last: Some(1) }))
            .build()
            .unwrap();

        let req = build_request(input, Method::HEAD);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_head_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_restore_object_rejects_missing_restore_request() {
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        match err.code() {
            S3ErrorCode::Custom(code) => assert_eq!(code, "ErrValidRestoreObject"),
            code => panic!("unexpected error code: {:?}", code),
        }
    }

    #[tokio::test]
    async fn execute_restore_object_returns_internal_error_when_store_uninitialized() {
        let restore_request = RestoreRequest {
            days: Some(1),
            description: None,
            glacier_job_parameters: None,
            output_location: None,
            select_parameters: None,
            tier: None,
            type_: None,
        };
        let input = RestoreObjectInput::builder()
            .bucket("test-bucket".to_string())
            .key("test-key".to_string())
            .restore_request(Some(restore_request))
            .build()
            .unwrap();

        let req = build_request(input, Method::POST);
        let usecase = DefaultObjectUsecase::without_context();

        let err = usecase.execute_restore_object(req).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
