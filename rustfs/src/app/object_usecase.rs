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
use crate::storage::access::ReqInfo;
use crate::storage::concurrency::{
    CachedGetObject, ConcurrencyManager, GetObjectGuard, get_concurrency_aware_buffer_size, get_concurrency_manager,
};
use crate::storage::ecfs::*;
use crate::storage::helper::OperationHelper;
use crate::storage::options::{
    extract_metadata_from_mime_with_object_name, filter_object_metadata, get_content_sha256, get_opts, put_opts,
};
use crate::storage::*;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
use bytes::Bytes;
use futures::StreamExt;
use http::HeaderMap;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::{
    metadata_sys,
    quota::QuotaOperation,
    replication::{get_must_replicate_options, must_replicate, schedule_replication},
    versioning_sys::BucketVersioningSys,
};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{HTTPRangeSpec, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader};
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_rio::{CompressReader, DecryptReader, EncryptReader, HardLimitReader, HashReader, Reader, WarpReader};
use rustfs_s3select_api::object_store::bytes_stream;
use rustfs_targets::EventName;
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::{
    AMZ_CHECKSUM_MODE, AMZ_CHECKSUM_TYPE,
    headers::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER},
};
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::convert::Infallible;
use std::str::FromStr;
use std::sync::Arc;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio_util::io::{ReaderStream, StreamReader};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

pub type ObjectUsecaseResult<T> = Result<T, ApiError>;

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
}
