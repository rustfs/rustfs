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

use crate::error::ApiError;
use crate::storage::concurrency::get_concurrency_manager;
use crate::storage::helper::OperationHelper;
use crate::storage::objects::Objects;
use crate::storage::options::{extract_metadata_from_mime_with_object_name, get_content_sha256, put_opts};
use crate::storage::sse::{EncryptionRequest, sse_encryption};
use crate::storage::{apply_lock_retention, get_buffer_size_opt_in, get_validated_store, validate_object_key};
use futures_util::StreamExt;
use http::HeaderMap;
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::quota::QuotaOperation;
use rustfs_ecstore::bucket::quota::checker::QuotaChecker;
use rustfs_ecstore::bucket::replication::{get_must_replicate_options, must_replicate, schedule_replication};
use rustfs_ecstore::client::object_api_utils::to_s3s_etag;
use rustfs_ecstore::compress::{MIN_COMPRESSIBLE_SIZE, is_compressible};
use rustfs_ecstore::error::{StorageError, is_err_object_not_found, is_err_version_not_found};
use rustfs_ecstore::set_disk::is_valid_storage_class;
use rustfs_ecstore::store_api::{ObjectIO, ObjectOptions, PutObjReader};
use rustfs_ecstore::{StorageAPI, new_object_layer_fn};
use rustfs_filemeta::{ReplicationStatusType, ReplicationType};
use rustfs_notify::notifier_global;
use rustfs_rio::{CompressReader, HashReader, Reader, WarpReader};
use rustfs_targets::EventName;
use rustfs_utils::http::{AMZ_DECODED_CONTENT_LENGTH, AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER};
use rustfs_utils::{
    CompressionAlgorithm, extract_params_header, extract_resp_elements, get_request_host, get_request_port,
    get_request_user_agent,
};
use rustfs_zip::CompressionFormat;
use s3s::dto::{ChecksumAlgorithm, ETag, PutObjectInput, PutObjectOutput};
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use std::collections::HashMap;
use std::path::Path;
use tokio_tar::Archive;
use tokio_util::io::StreamReader;
use tracing::{debug, error, instrument, warn};

impl Objects {
    #[instrument(level = "debug", skip(self, req))]
    pub async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectCreatedPut, "s3:PutObject");
        if req
            .headers
            .get("X-Amz-Meta-Snowball-Auto-Extract")
            .is_some_and(|v| v.to_str().unwrap_or_default() == "true")
        {
            return self.put_object_extract(req).await;
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
            if_match,
            if_none_match,
            ..
        } = input;

        // Validate object key
        validate_object_key(&key, "PUT")?;

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
                        if let Some(rematching) = if_none_match
                            && let Some(strong_etag) = rematching.into_etag()
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

        // check quota for put operation
        let mut quota_usage_calculated = false;
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
                    // Track if usage was actually calculated (not just returned as None/0)
                    quota_usage_calculated = check_result.current_usage.is_some();
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

        let mut metadata = metadata.unwrap_or_default();

        let object_lock_configuration = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    None
                } else {
                    warn!("get_object_lock_config err {:?}", err);
                    return Err(S3Error::with_message(
                        S3ErrorCode::InternalError,
                        "Failed to load Object Lock configuration".to_string(),
                    ));
                }
            }
        };

        apply_lock_retention(object_lock_configuration, &mut metadata);

        if let Some(content_type) = content_type {
            metadata.insert("content-type".to_string(), content_type.to_string());
        }

        extract_metadata_from_mime_with_object_name(&req.headers, &mut metadata, true, Some(&key));

        if let Some(tags) = tagging {
            metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_string());
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

        // Apply encryption using unified SSE API
        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption,
            ssekms_key_id,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: None,
            part_key: None,
            part_nonce: None,
        };

        let (effective_sse, effective_kms_key_id) = match sse_encryption(encryption_request).await? {
            Some(material) => {
                let server_side_encryption = Some(material.server_side_encryption.clone());
                let ssekms_key_id = material.kms_key_id.clone();

                // Apply encryption wrapper
                let encrypted_reader = material.wrap_reader(reader);
                reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                    .map_err(ApiError::from)?;

                // Merge encryption metadata
                metadata.extend(material.metadata);

                (server_side_encryption, ssekms_key_id)
            }
            None => (None, None),
        };

        let mut reader = PutObjReader::new(reader);

        let mt2 = metadata.clone();
        opts.user_defined.extend(metadata);

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

        // Only increment usage cache when quota checking actually calculated real usage.
        // This prevents cache corruption: when quotas are disabled, the cache remains unset.
        // When quotas are later enabled, the cache will miss and recalculate from backend.
        if quota_usage_calculated {
            rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;
        }

        // Invalidate cache for the written object to prevent stale data
        let manager = get_concurrency_manager();
        let put_bucket = bucket.clone();
        let put_key = key.clone();
        let mut put_version = obj_info.version_id.map(|v| v.to_string());
        if opts.version_suspended && obj_info.version_id.is_none_or(|v| v.is_nil()) {
            put_version = Some("null".to_string());
        }

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
            sse_customer_algorithm,
            sse_customer_key_md5,
            ssekms_key_id: effective_kms_key_id, // TDD: Return effective KMS key ID
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            version_id: put_version,
            ..Default::default()
        };

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

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
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
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
                    resp_elements: extract_resp_elements(&S3Response::new(output.clone())),
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
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            ..Default::default()
        };
        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }
}
