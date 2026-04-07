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

use super::*;
use bytes::Buf;
use futures::{Stream, StreamExt};
use rustfs_ecstore::config::GLOBAL_STORAGE_CLASS;
use rustfs_io_core::{BytesPool, PooledBuffer};
use rustfs_object_io::put::{
    PutObjectChecksums, PutObjectIngressKind, PutObjectLegacyHashStagePlan, PutObjectLegacyHashValues, PutObjectTransformStage,
    apply_trailing_checksums, build_put_object_ingress_source, build_put_object_legacy_hash_stage,
    build_put_object_plain_hash_stage, plan_put_object_body_with_transforms, resolve_put_transformed_fallback_reason,
};
use rustfs_rio::{BlockReadable, BoxReadBlockFuture, EtagResolvable, HashReaderDetector, TryGetIndex};
use rustfs_utils::http::headers::AMZ_TRAILER;

const DEFAULT_SMALL_PUT_EAGER_MAX_BYTES: i64 = 1024 * 1024;
const ENV_RUSTFS_PUT_SMALL_EAGER_MAX_BYTES: &str = "RUSTFS_PUT_SMALL_EAGER_MAX_BYTES";
const ENV_RUSTFS_PUT_FORCE_DISABLE_SMALL_EAGER: &str = "RUSTFS_PUT_FORCE_DISABLE_SMALL_EAGER";
const SLOW_PUT_PHASE_DEBUG_THRESHOLD_MS: u64 = 100;
const SLOW_PUT_PHASE_WARN_THRESHOLD_MS: u64 = 1_000;
const SLOW_PUT_PHASE_ERROR_THRESHOLD_MS: u64 = 5_000;

fn resolved_checksum_bytes(checksums: &PutObjectChecksums) -> Option<bytes::Bytes> {
    [
        (rustfs_rio::ChecksumType::CRC32, checksums.crc32.as_deref()),
        (rustfs_rio::ChecksumType::CRC32C, checksums.crc32c.as_deref()),
        (rustfs_rio::ChecksumType::SHA1, checksums.sha1.as_deref()),
        (rustfs_rio::ChecksumType::SHA256, checksums.sha256.as_deref()),
        (rustfs_rio::ChecksumType::CRC64_NVME, checksums.crc64nvme.as_deref()),
    ]
    .into_iter()
    .find_map(|(checksum_type, value)| {
        value.and_then(|value| rustfs_rio::Checksum::new_with_type(checksum_type, value).map(|checksum| checksum.to_bytes(&[])))
    })
}

fn clamp_small_put_eager_max_bytes(inline_object_limit_bytes: Option<usize>) -> i64 {
    inline_object_limit_bytes
        .unwrap_or(DEFAULT_SMALL_PUT_EAGER_MAX_BYTES as usize)
        .min(DEFAULT_SMALL_PUT_EAGER_MAX_BYTES as usize) as i64
}

fn env_flag_enabled(name: &str) -> bool {
    rustfs_utils::get_env_bool(name, false)
}

fn env_non_negative_i64(name: &str) -> Option<i64> {
    rustfs_utils::get_env_opt_i64(name).filter(|value| *value >= 0)
}

fn topology_aware_small_put_eager_max_bytes(store: &rustfs_ecstore::store::ECStore, versioned: bool) -> i64 {
    let Some(first_pool) = store.pools.first() else {
        return DEFAULT_SMALL_PUT_EAGER_MAX_BYTES;
    };

    let data_shards = first_pool
        .set_drive_count
        .saturating_sub(first_pool.default_parity_count)
        .max(1);

    let inline_object_limit = GLOBAL_STORAGE_CLASS
        .get()
        .map(|config| config.inline_object_limit_bytes(data_shards, versioned));

    clamp_small_put_eager_max_bytes(inline_object_limit)
}

fn resolved_small_put_eager_max_bytes(default_max_bytes: i64) -> i64 {
    if env_flag_enabled(ENV_RUSTFS_PUT_FORCE_DISABLE_SMALL_EAGER) {
        return 0;
    }

    env_non_negative_i64(ENV_RUSTFS_PUT_SMALL_EAGER_MAX_BYTES)
        .map(|value| value.min(DEFAULT_SMALL_PUT_EAGER_MAX_BYTES).min(default_max_bytes))
        .unwrap_or(default_max_bytes)
}

fn should_use_small_put_eager_path(size: i64, eager_max_bytes: i64, compression_enabled: bool, encryption_enabled: bool) -> bool {
    size > 0 && size <= eager_max_bytes && !compression_enabled && !encryption_enabled
}

fn request_uses_trailing_checksum(headers: &HeaderMap, trailing_headers: &Option<s3s::TrailingHeaders>) -> bool {
    trailing_headers.is_some()
        || headers.contains_key(AMZ_TRAILER)
        || matches!(
            rustfs_rio::get_content_checksum(headers),
            Ok(Some(checksum)) if checksum.checksum_type.trailing()
        )
}

fn put_path_label(small_eager: bool, reduced_copy: bool, compressed: bool) -> &'static str {
    if small_eager {
        "small_eager"
    } else if compressed {
        "compressed"
    } else if reduced_copy {
        "reduced_copy"
    } else {
        "legacy_plain"
    }
}

#[allow(clippy::too_many_arguments)]
fn log_put_flow_phase(
    bucket: &str,
    key: &str,
    phase: &str,
    elapsed: std::time::Duration,
    object_size: i64,
    small_eager: bool,
    reduced_copy: bool,
    compressed: bool,
    encrypted: bool,
) {
    let duration_ms = elapsed.as_millis() as u64;
    if duration_ms < SLOW_PUT_PHASE_DEBUG_THRESHOLD_MS {
        return;
    }

    let put_path = put_path_label(small_eager, reduced_copy, compressed);
    if duration_ms >= SLOW_PUT_PHASE_ERROR_THRESHOLD_MS {
        error!(
            phase,
            duration_ms, object_size, put_path, compressed, encrypted, bucket, key, "Small PUT phase is critically slow"
        );
    } else if duration_ms >= SLOW_PUT_PHASE_WARN_THRESHOLD_MS {
        warn!(
            phase,
            duration_ms, object_size, put_path, compressed, encrypted, bucket, key, "Small PUT phase is slow"
        );
    } else {
        debug!(
            phase,
            duration_ms, object_size, put_path, compressed, encrypted, bucket, key, "Small PUT phase exceeded debug threshold"
        );
    }
}

struct PooledBufferReader {
    buffer: PooledBuffer,
    position: usize,
}

impl PooledBufferReader {
    fn new(buffer: PooledBuffer) -> Self {
        Self { buffer, position: 0 }
    }
}

impl tokio::io::AsyncRead for PooledBufferReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let remaining = &self.buffer[self.position..];
        if remaining.is_empty() {
            return std::task::Poll::Ready(Ok(()));
        }

        let to_copy = remaining.len().min(buf.remaining());
        buf.put_slice(&remaining[..to_copy]);
        self.position += to_copy;
        std::task::Poll::Ready(Ok(()))
    }
}

impl BlockReadable for PooledBufferReader {
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        Box::pin(async move {
            let remaining = &self.buffer[self.position..];
            if remaining.is_empty() {
                return Ok(0);
            }

            let to_copy = remaining.len().min(buf.len());
            buf[..to_copy].copy_from_slice(&remaining[..to_copy]);
            self.position += to_copy;
            Ok(to_copy)
        })
    }
}

impl EtagResolvable for PooledBufferReader {}

impl HashReaderDetector for PooledBufferReader {}

impl TryGetIndex for PooledBufferReader {}

async fn read_small_put_body_eager<S, B, E>(body: S, size: i64, pool: std::sync::Arc<BytesPool>) -> S3Result<PooledBuffer>
where
    S: Stream<Item = Result<B, E>>,
    B: Buf,
    E: std::fmt::Display,
{
    let expected_len = usize::try_from(size).map_err(|_| s3_error!(InvalidRequest, "Object size overflow"))?;
    let mut data = pool.acquire_buffer(expected_len).await;
    let mut body = Box::pin(body);

    while let Some(result) = body.next().await {
        let mut chunk = result.map_err(|err| S3Error::with_message(S3ErrorCode::IncompleteBody, err.to_string()))?;
        let chunk_len = chunk.remaining();
        if chunk_len == 0 {
            continue;
        }

        let new_len = data
            .len()
            .checked_add(chunk_len)
            .ok_or_else(|| s3_error!(InvalidRequest, "Object size overflow"))?;
        if new_len > expected_len {
            return Err(s3_error!(IncompleteBody));
        }

        let start = data.len();
        data.resize(new_len, 0);
        chunk.copy_to_slice(&mut data[start..new_len]);

        if data.len() == expected_len {
            return Ok(data);
        }
    }

    if data.len() != expected_len {
        return Err(s3_error!(IncompleteBody));
    }

    Ok(data)
}

async fn build_small_put_eager_hash_stage<S, B, E>(
    body: S,
    size: i64,
    pool: std::sync::Arc<BytesPool>,
    hash_values: PutObjectLegacyHashValues,
    headers: &HeaderMap,
    trailing_headers: Option<s3s::TrailingHeaders>,
) -> S3Result<rustfs_object_io::put::PutObjectHashStage>
where
    S: Stream<Item = Result<B, E>>,
    B: Buf,
    E: std::fmt::Display,
{
    let data = read_small_put_body_eager(body, size, pool).await?;
    build_put_object_legacy_hash_stage(
        Box::new(PooledBufferReader::new(data)),
        hash_values,
        PutObjectLegacyHashStagePlan {
            size,
            actual_size: size,
            apply_s3_checksum: true,
            ignore_s3_checksum_value: false,
        },
        headers,
        trailing_headers,
    )
    .map_err(ApiError::from)
    .map_err(Into::into)
}

impl DefaultObjectUsecase {
    pub(super) async fn run_put_object_flow(
        input: PutObjectInput,
        request_context: PutObjectRequestContext,
        request_method_name: &'static str,
        resolved_size: i64,
    ) -> S3Result<PutObjectFlowResult> {
        let start_time = std::time::Instant::now();

        let PutObjectInput {
            body,
            bucket,
            cache_control,
            key,
            content_length: _content_length,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            tagging,
            metadata,
            version_id,
            server_side_encryption,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ssekms_key_id,
            content_md5,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            storage_class,
            website_redirect_location,
            ..
        } = input;

        let (h_algo, h_key, h_md5) = extract_ssec_params_from_headers(&request_context.headers)?;
        let sse_customer_algorithm = sse_customer_algorithm.or(h_algo);
        let sse_customer_key = sse_customer_key.or(h_key);
        let sse_customer_key_md5 = sse_customer_key_md5.or(h_md5);

        let server_side_encryption =
            server_side_encryption.or(extract_server_side_encryption_from_headers(&request_context.headers)?);

        validate_object_key(&key, request_method_name)?;

        let Some(body) = body else { return Err(s3_error!(IncompleteBody)) };

        let mut size = resolved_size;
        let mut transform_stage = PutObjectTransformStage::default();
        let mut plain_reduced_copy_stage = false;
        let mut small_object_eager_stage = false;
        let bytes_pool = get_concurrency_manager().bytes_pool();

        let store = get_validated_store_adapter(&bucket).await?;

        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();

        let mut effective_sse = server_side_encryption.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .map(|sse| match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                        })
                })
            })
        });

        let mut effective_kms_key_id = ssekms_key_id.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                config.rules.first().and_then(|rule| {
                    rule.apply_server_side_encryption_by_default
                        .as_ref()
                        .and_then(|sse| sse.kms_master_key_id.clone())
                })
            })
        });

        validate_sse_headers_for_write(
            effective_sse.as_ref(),
            effective_kms_key_id.as_ref(),
            sse_customer_algorithm.as_ref(),
            sse_customer_key.as_ref(),
            sse_customer_key_md5.as_ref(),
            true,
        )?;

        let encryption_enabled_for_put = effective_sse.is_some()
            || effective_kms_key_id.is_some()
            || sse_customer_algorithm.is_some()
            || sse_customer_key.is_some()
            || sse_customer_key_md5.is_some();

        let body_plan = plan_put_object_body_with_transforms(
            size,
            &request_context.headers,
            &key,
            get_buffer_size_opt_in(size),
            encryption_enabled_for_put,
        );
        if body_plan.ingress.kind == PutObjectIngressKind::ReducedCopyCandidate {
            rustfs_io_metrics::record_put_object_attempted_fast_path(size);
            debug!(
                encryption_enabled = encryption_enabled_for_put,
                compressed = body_plan.should_compress(),
                "Zero-copy write enabled for {} byte object (bucket={}, key={})",
                size,
                bucket,
                key
            );
        } else if let Some(reason) = resolve_put_transformed_fallback_reason(
            body_plan.ingress.kind,
            body_plan.should_compress(),
            encryption_enabled_for_put,
        ) {
            rustfs_io_metrics::record_io_fallback(rustfs_io_metrics::IoStage::PutTransform, reason);
            rustfs_io_metrics::record_put_fallback(size, reason);
        }

        let mut metadata = metadata.unwrap_or_default();
        apply_put_request_metadata(
            &mut metadata,
            &request_context.headers,
            &key,
            cache_control,
            content_disposition,
            content_encoding,
            content_language,
            content_type,
            expires,
            website_redirect_location,
            tagging,
            storage_class.clone(),
        )?;

        let mut opts: ObjectOptions = put_opts(&bucket, &key, version_id.clone(), &request_context.headers, metadata.clone())
            .await
            .map_err(ApiError::from)?;
        apply_put_request_object_lock_opts(
            &bucket,
            object_lock_legal_hold_status,
            object_lock_mode,
            object_lock_retain_until_date,
            &mut opts,
        )
        .await?;
        let eager_max_bytes =
            resolved_small_put_eager_max_bytes(topology_aware_small_put_eager_max_bytes(&store, opts.versioned));
        let can_use_small_put_eager =
            !request_uses_trailing_checksum(&request_context.headers, &request_context.trailing_headers);

        let current_opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &request_context.headers)
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

        let actual_size = size;
        let mut hash_values = PutObjectLegacyHashValues {
            md5hex: if let Some(base64_md5) = content_md5 {
                let md5 = base64_simd::STANDARD
                    .decode_to_vec(base64_md5.as_bytes())
                    .map_err(|e| ApiError::from(StorageError::other(format!("Invalid content MD5: {e}"))))?;
                Some(hex_simd::encode_to_string(&md5, hex_simd::AsciiCase::Lower))
            } else {
                None
            },
            sha256hex: get_content_sha256_with_query(&request_context.headers, request_context.uri_query.as_deref()),
        };

        let reader_stage_start = std::time::Instant::now();
        let stage = if can_use_small_put_eager
            && should_use_small_put_eager_path(size, eager_max_bytes, body_plan.should_compress(), encryption_enabled_for_put)
        {
            small_object_eager_stage = true;
            debug!(
                "Plain PUT is using the eager small-object path (bucket={}, key={}, size={}, eager_max={})",
                bucket, key, size, eager_max_bytes
            );
            build_small_put_eager_hash_stage(
                body,
                size,
                bytes_pool.clone(),
                hash_values,
                &request_context.headers,
                request_context.trailing_headers.clone(),
            )
            .await?
        } else if body_plan.should_compress() {
            transform_stage.mark_compression();
            let algorithm = CompressionAlgorithm::default();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, algorithm.to_string());
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

            let ingress_source = build_put_object_ingress_source(body, body_plan);
            let stage = build_put_object_plain_hash_stage(
                ingress_source,
                std::mem::take(&mut hash_values),
                PutObjectLegacyHashStagePlan {
                    size,
                    actual_size: size,
                    apply_s3_checksum: true,
                    ignore_s3_checksum_value: false,
                },
                &request_context.headers,
                request_context.trailing_headers.clone(),
            )
            .map_err(ApiError::from)?;

            if stage.ingress_kind == PutObjectIngressKind::ReducedCopyCandidate {
                plain_reduced_copy_stage = true;
            }
            opts.want_checksum = stage.want_checksum;
            insert_str(&mut opts.user_defined, SUFFIX_COMPRESSION, algorithm.to_string());
            insert_str(&mut opts.user_defined, SUFFIX_ACTUAL_SIZE, size.to_string());

            let reader: Box<dyn Reader> = Box::new(CompressReader::new(stage.reader, algorithm));
            size = HashReader::SIZE_PRESERVE_LAYER;
            hash_values.clear_for_transformed_body();
            build_put_object_legacy_hash_stage(
                reader,
                hash_values,
                PutObjectLegacyHashStagePlan {
                    size,
                    actual_size,
                    apply_s3_checksum: size >= 0,
                    ignore_s3_checksum_value: false,
                },
                &request_context.headers,
                request_context.trailing_headers.clone(),
            )
            .map_err(ApiError::from)?
        } else {
            let ingress_source = build_put_object_ingress_source(body, body_plan);
            let stage = build_put_object_plain_hash_stage(
                ingress_source,
                hash_values,
                PutObjectLegacyHashStagePlan {
                    size,
                    actual_size,
                    apply_s3_checksum: size >= 0,
                    ignore_s3_checksum_value: false,
                },
                &request_context.headers,
                request_context.trailing_headers.clone(),
            )
            .map_err(ApiError::from)?;

            if stage.ingress_kind == PutObjectIngressKind::ReducedCopyCandidate {
                plain_reduced_copy_stage = true;
                debug!(
                    "Plain PUT is using the reduced-copy Reader + BlockReadable hash path (bucket={}, key={})",
                    bucket, key
                );
            }

            stage
        };
        log_put_flow_phase(
            &bucket,
            &key,
            "build_hash_stage",
            reader_stage_start.elapsed(),
            actual_size,
            small_object_eager_stage,
            plain_reduced_copy_stage,
            transform_stage.compression_applied(),
            false,
        );
        let mut reader = stage.reader;
        if stage.want_checksum.is_some() {
            opts.want_checksum = stage.want_checksum;
        }

        let encryption_request = EncryptionRequest {
            bucket: &bucket,
            key: &key,
            server_side_encryption: effective_sse.clone(),
            ssekms_key_id: effective_kms_key_id.clone(),
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key,
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            content_size: actual_size,
            part_number: None,
            part_key: None,
            part_nonce: None,
        };

        if let Some(material) = sse_encryption(encryption_request).await? {
            transform_stage.mark_encryption();
            effective_sse = Some(material.server_side_encryption.clone());
            effective_kms_key_id = material.kms_key_id.clone();

            let encrypted_reader = material.wrap_reader(reader);
            reader = HashReader::new(encrypted_reader, HashReader::SIZE_PRESERVE_LAYER, actual_size, None, None, false)
                .map_err(ApiError::from)?;

            let encryption_metadata = material.metadata;
            metadata.extend(encryption_metadata.clone());
            opts.user_defined.extend(encryption_metadata);
        }

        let mut reader = ChunkNativePutData::new(reader);

        let mt2 = metadata.clone();
        opts.user_defined.extend(metadata);

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts.clone());

        let dsc = must_replicate(&bucket, &key, repoptions).await;

        if dsc.replicate_any() {
            insert_str(&mut opts.user_defined, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(
                &mut opts.user_defined,
                SUFFIX_REPLICATION_STATUS,
                dsc.pending_status().unwrap_or_default(),
            );
        }

        let store_put_start = std::time::Instant::now();
        let obj_info = store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;
        log_put_flow_phase(
            &bucket,
            &key,
            "store_put_object",
            store_put_start.elapsed(),
            actual_size,
            small_object_eager_stage,
            plain_reduced_copy_stage,
            transform_stage.compression_applied(),
            transform_stage.encryption_applied(),
        );

        maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;

        rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        let raw_version = obj_info.version_id.map(|v| v.to_string());

        let put_version = if bucket_prefix_versioning_enabled(&bucket, &key).await {
            raw_version.clone()
        } else {
            None
        };

        let e_tag = obj_info.etag.clone().map(|etag| to_s3s_etag(&etag));

        let repoptions =
            get_must_replicate_options(&mt2, "".to_string(), ReplicationStatusType::Empty, ReplicationType::Object, opts);

        let dsc = must_replicate(&bucket, &key, repoptions).await;
        let expiration = resolve_put_object_expiration(&bucket, &obj_info).await;

        if dsc.replicate_any() {
            schedule_replication(obj_info.clone(), store.clone(), dsc, ReplicationType::Object).await;
        }

        let mut checksums = PutObjectChecksums {
            crc32: input.checksum_crc32,
            crc32c: input.checksum_crc32c,
            sha1: input.checksum_sha1,
            sha256: input.checksum_sha256,
            crc64nvme: input.checksum_crc64nvme,
        };
        apply_trailing_checksums(
            input.checksum_algorithm.as_ref().map(|a| a.as_str()),
            &request_context.trailing_headers,
            &mut checksums,
        );
        checksums.merge_from_map(&reader.content_crc());
        if let Some(checksum_bytes) = resolved_checksum_bytes(&checksums)
            && obj_info
                .checksum
                .as_ref()
                .is_none_or(|stored| rustfs_rio::read_checksums(stored.as_ref(), 0).0.is_empty())
        {
            let checksum_update_opts = ObjectOptions {
                version_id: raw_version.clone(),
                resolved_checksum: Some(checksum_bytes),
                ..Default::default()
            };
            let _ = store
                .put_object_metadata(&bucket, &key, &checksum_update_opts)
                .await
                .map_err(ApiError::from)?;
        }

        let output = PutObjectOutput {
            e_tag,
            server_side_encryption: effective_sse,
            sse_customer_algorithm: sse_customer_algorithm.clone(),
            sse_customer_key_md5: sse_customer_key_md5.clone(),
            ssekms_key_id: effective_kms_key_id,
            expiration,
            checksum_crc32: checksums.crc32,
            checksum_crc32c: checksums.crc32c,
            checksum_sha1: checksums.sha1,
            checksum_sha256: checksums.sha256,
            checksum_crc64nvme: checksums.crc64nvme,
            version_id: put_version,
            ..Default::default()
        };

        let manager = get_capacity_manager();
        manager.record_write_operation().await;

        {
            let duration_ms = start_time.elapsed().as_millis() as f64;
            let fast_path_selected = plain_reduced_copy_stage || small_object_eager_stage;
            rustfs_io_metrics::record_put_object(duration_ms, size, fast_path_selected);
            let io_path = if fast_path_selected {
                rustfs_io_metrics::IoPath::Fast
            } else {
                rustfs_io_metrics::IoPath::Legacy
            };
            rustfs_io_metrics::record_io_path_selected("put", io_path);
            rustfs_io_metrics::record_put_path_selected(actual_size, io_path);
            let effective_copy_mode = transform_stage.effective_copy_mode();
            rustfs_io_metrics::record_io_copy_mode("put", effective_copy_mode, actual_size.max(0) as usize);
            rustfs_io_metrics::record_put_copy_mode(actual_size, effective_copy_mode);
            if let Some(transform_kind) = transform_stage.metric_kind() {
                rustfs_io_metrics::record_put_transform_selected(transform_kind, io_path, actual_size.max(0) as usize);
            }
        }

        Ok(PutObjectFlowResult {
            output,
            helper_object: obj_info,
            helper_version_id: raw_version,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::{StreamExt, stream};
    use rustfs_io_core::BytesPool;
    use serial_test::serial;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[test]
    fn small_put_eager_path_only_targets_plain_small_objects() {
        assert!(should_use_small_put_eager_path(
            64 * 1024,
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            false,
            false
        ));
        assert!(should_use_small_put_eager_path(
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            false,
            false
        ));
        assert!(!should_use_small_put_eager_path(
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES + 1,
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            false,
            false
        ));
        assert!(!should_use_small_put_eager_path(
            64 * 1024,
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            true,
            false
        ));
        assert!(!should_use_small_put_eager_path(
            64 * 1024,
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES,
            false,
            true
        ));
        assert!(!should_use_small_put_eager_path(0, DEFAULT_SMALL_PUT_EAGER_MAX_BYTES, false, false));
    }

    #[test]
    fn clamp_small_put_eager_max_bytes_caps_inline_budget() {
        assert_eq!(
            clamp_small_put_eager_max_bytes(Some(rustfs_object_io::put::PUT_REDUCED_COPY_MIN_SIZE_BYTES as usize * 2)),
            DEFAULT_SMALL_PUT_EAGER_MAX_BYTES
        );
        assert_eq!(clamp_small_put_eager_max_bytes(Some(128 * 1024)), 128 * 1024);
        assert_eq!(clamp_small_put_eager_max_bytes(None), DEFAULT_SMALL_PUT_EAGER_MAX_BYTES);
    }

    #[test]
    #[serial]
    fn resolved_small_put_eager_max_bytes_honors_disable_env() {
        temp_env::with_var(ENV_RUSTFS_PUT_FORCE_DISABLE_SMALL_EAGER, Some("true"), || {
            assert_eq!(resolved_small_put_eager_max_bytes(256 * 1024), 0);
        });
    }

    #[test]
    #[serial]
    fn resolved_small_put_eager_max_bytes_narrows_default_budget() {
        temp_env::with_var(ENV_RUSTFS_PUT_SMALL_EAGER_MAX_BYTES, Some("4096"), || {
            assert_eq!(resolved_small_put_eager_max_bytes(256 * 1024), 4096);
        });
    }

    #[test]
    #[serial]
    fn resolved_small_put_eager_max_bytes_ignores_invalid_override() {
        temp_env::with_var(ENV_RUSTFS_PUT_SMALL_EAGER_MAX_BYTES, Some("invalid"), || {
            assert_eq!(resolved_small_put_eager_max_bytes(256 * 1024), 256 * 1024);
        });
    }

    #[tokio::test]
    async fn read_small_put_body_eager_requires_exact_content_length() {
        let body = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"abc")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"def")),
        ]);

        let pool = Arc::new(BytesPool::new_tiered());
        let data = read_small_put_body_eager(body, 6, pool)
            .await
            .expect("eager read should succeed");
        assert_eq!(data.as_ref(), b"abcdef");
    }

    #[tokio::test]
    async fn read_small_put_body_eager_rejects_length_mismatch() {
        let body = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(b"abc"))]);
        let pool = Arc::new(BytesPool::new_tiered());

        let err = read_small_put_body_eager(body, 4, pool)
            .await
            .expect_err("short eager read should fail");
        assert_eq!(err.code(), &S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn read_small_put_body_eager_rejects_overlong_body() {
        let body = stream::iter(vec![
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"abc")),
            Ok::<Bytes, std::io::Error>(Bytes::from_static(b"def")),
        ]);
        let pool = Arc::new(BytesPool::new_tiered());

        let err = read_small_put_body_eager(body, 5, pool)
            .await
            .expect_err("overlong eager read should fail");
        assert_eq!(err.code(), &S3ErrorCode::IncompleteBody);
    }

    #[tokio::test]
    async fn read_small_put_body_eager_returns_buffer_to_pool_after_drop() {
        let body = stream::iter(vec![Ok::<Bytes, std::io::Error>(Bytes::from_static(b"abc"))]);
        let pool = Arc::new(BytesPool::new_tiered());

        let data = read_small_put_body_eager(body, 3, pool.clone())
            .await
            .expect("pooled eager read should succeed");
        assert_eq!(pool.available_buffers(), 0);

        drop(data);
        assert_eq!(pool.available_buffers(), 1);
    }

    #[tokio::test]
    async fn read_small_put_body_eager_returns_after_expected_bytes_without_waiting_for_eof() {
        let body = stream::once(async { Ok::<Bytes, std::io::Error>(Bytes::from_static(b"abc")) }).chain(stream::pending());
        let pool = Arc::new(BytesPool::new_tiered());

        let data = timeout(Duration::from_millis(50), read_small_put_body_eager(body, 3, pool))
            .await
            .expect("eager read should not wait for stream termination")
            .expect("eager read should succeed once content-length bytes are read");

        assert_eq!(data.as_ref(), b"abc");
    }
}
