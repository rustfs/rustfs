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
use rustfs_object_io::put::{
    PutObjectChecksums, PutObjectIngressKind, PutObjectLegacyHashStagePlan, PutObjectLegacyHashValues, PutObjectTransformStage,
    apply_trailing_checksums, build_put_object_ingress_source, build_put_object_legacy_hash_stage,
    build_put_object_plain_hash_stage, plan_put_object_body_with_transforms, resolve_put_transformed_fallback_reason,
};

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

        let store = get_validated_store_adapter(&bucket).await?;

        let bucket_sse_config = metadata_sys::get_sse_config(&bucket).await.ok();
        debug!("TDD: bucket_sse_config={:?}", bucket_sse_config);

        let original_sse = server_side_encryption.clone();
        let mut effective_sse = server_side_encryption.or_else(|| {
            bucket_sse_config.as_ref().and_then(|(config, _timestamp)| {
                debug!("TDD: Processing bucket SSE config: {:?}", config);
                config.rules.first().and_then(|rule| {
                    debug!("TDD: Processing SSE rule: {:?}", rule);
                    rule.apply_server_side_encryption_by_default.as_ref().map(|sse| {
                        debug!("TDD: Found SSE default: {:?}", sse);
                        match sse.sse_algorithm.as_str() {
                            "AES256" => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
                            "aws:kms" => ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                            _ => ServerSideEncryption::from_static(ServerSideEncryption::AES256),
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
        }

        let ingress_source = build_put_object_ingress_source(body, body_plan);

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

        let stage = if body_plan.should_compress() {
            transform_stage.mark_compression();
            let algorithm = CompressionAlgorithm::default();
            insert_str(&mut metadata, SUFFIX_COMPRESSION, algorithm.to_string());
            insert_str(&mut metadata, SUFFIX_ACTUAL_SIZE, size.to_string());

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

        let obj_info = store
            .put_object(&bucket, &key, &mut reader, &opts)
            .await
            .map_err(ApiError::from)?;

        maybe_enqueue_transition_immediate(&obj_info, LcEventSrc::S3PutObject).await;

        rustfs_ecstore::data_usage::increment_bucket_usage_memory(&bucket, obj_info.size as u64).await;

        let raw_version = obj_info.version_id.map(|v| v.to_string());

        Self::spawn_cache_invalidation(bucket.clone(), key.clone(), raw_version.clone());

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
            schedule_replication(obj_info.clone(), store, dsc, ReplicationType::Object).await;
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
            rustfs_io_metrics::record_put_object(duration_ms, size, body_plan.ingress.enable_zero_copy);
            let io_path = if plain_reduced_copy_stage {
                rustfs_io_metrics::IoPath::Fast
            } else {
                rustfs_io_metrics::IoPath::Legacy
            };
            rustfs_io_metrics::record_io_path_selected("put", io_path);
            let effective_copy_mode = transform_stage.effective_copy_mode();
            rustfs_io_metrics::record_io_copy_mode("put", effective_copy_mode, actual_size.max(0) as usize);
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
