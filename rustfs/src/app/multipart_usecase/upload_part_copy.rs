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

//! UploadPartCopy.

use super::*;
use crate::app::storage_api::s3::dto::{
    CopyPartResult, CopySource, ETag, ServerSideEncryption, Timestamp, UploadPartCopyInput, UploadPartCopyOutput,
};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

fn normalize_copy_source_version_id(version_id: Option<&str>) -> S3Result<Option<String>> {
    match version_id {
        Some(v) => {
            let trimmed = v.trim();
            if trimmed.eq_ignore_ascii_case("null") {
                Ok(Some(Uuid::nil().to_string()))
            } else if Uuid::parse_str(trimmed).is_ok() {
                Ok(Some(trimmed.to_string()))
            } else {
                Err(S3Error::with_message(
                    S3ErrorCode::InvalidArgument,
                    "Invalid version id specified in copy source",
                ))
            }
        }
        None => Ok(None),
    }
}

fn format_copy_source_version_id(version_id: Option<Uuid>, src_versioned: bool, src_version_suspended: bool) -> Option<String> {
    if src_versioned || src_version_suspended {
        Some(match version_id {
            Some(vid) if vid != Uuid::nil() => vid.to_string(),
            _ => "null".to_string(),
        })
    } else {
        None
    }
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

impl DefaultMultipartUsecase {
    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_upload_part_copy(
        &self,
        req: S3Request<UploadPartCopyInput>,
    ) -> S3Result<S3Response<UploadPartCopyOutput>> {
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedSigV4Request>().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            &req.headers,
            req.uri.query(),
            req.extensions.get::<VerifiedPresignedRequest>().is_some(),
        )?;
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

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        let (src_bucket, src_key, raw_src_version_id) = match copy_source {
            CopySource::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySource::Bucket {
                bucket: ref src_bucket,
                key: ref src_key,
                version_id,
            } => (src_bucket.to_string(), src_key.to_string(), version_id.map(|v| v.to_string())),
        };
        let src_version_id = normalize_copy_source_version_id(raw_src_version_id.as_deref())?;

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

        let (
            source_bucket_lifecycle_guard,
            current_source_incarnation_id,
            destination_bucket_lifecycle_guard_storage,
            current_destination_incarnation_id,
        ) = store
            .acquire_copy_bucket_incarnation_fences(
                &src_bucket,
                expected_source_incarnation_id,
                &bucket,
                expected_destination_incarnation_id,
            )
            .await
            .map_err(ApiError::from)?;
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
            source_bucket_lifecycle_guard.attach_to_object_options(&mut dst_opts);
        }
        destination_bucket_lifecycle_guard.attach_to_object_options(&mut dst_opts);

        let mp_info = store
            .get_multipart_info(&bucket, &key, &upload_id, &dst_opts)
            .await
            .map_err(multipart_upload_api_error)?;
        authorize_sse_kms_multipart_write(copy_principal.as_ref(), &mp_info.user_defined).await?;
        let destination_size_limit = multipart_max_total_object_size(&mp_info.user_defined)?;
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

        let src_versioned = BucketVersioningSys::prefix_enabled(&src_bucket, &src_key).await;
        let src_version_suspended = BucketVersioningSys::prefix_suspended(&src_bucket, &src_key).await;
        let mut src_opts = copy_src_opts(&src_bucket, &src_key, &req.headers).map_err(ApiError::from)?;
        src_opts.version_id = src_version_id.clone();
        src_opts.versioned = src_versioned;
        src_opts.version_suspended = src_version_suspended;

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

        let prepared_source = store
            .prepare_get_object_reader(&src_bucket, &src_key, rs.clone(), h, &get_opts)
            .await
            .map_err(map_get_object_reader_error)?;

        let src_info = prepared_source.object_info();
        if src_info.delete_marker {
            let error = if src_opts.version_id.is_none() {
                StorageError::ObjectNotFound(src_bucket.clone(), src_key.clone())
            } else {
                StorageError::MethodNotAllowed
            };
            return Err(map_get_object_reader_error(error).into());
        }

        // Same shape as CopyObject: the part copy reads the source plaintext, and the source
        // read resolves its material inside the object layer, which carries no request identity.
        authorize_sse_kms_object_read(copy_principal.as_ref(), &src_info.user_defined).await?;

        let resolved_src_version_id = format_copy_source_version_id(src_info.version_id, src_versioned, src_version_suspended);

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

        let source_actual_size = src_info.get_actual_size().ok().filter(|size| *size >= 0);
        let source_logical_size = match source_actual_size {
            Some(size) => size,
            None if destination_size_limit.is_some() => {
                return Err(S3Error::new(S3ErrorCode::UnexpectedContent));
            }
            None => src_info.size,
        };

        let (_start_offset, length) = if let Some(ref range_spec) = rs {
            // Copy-source ranges are expressed over the logical plaintext object.
            // Encrypted (and compressed) objects have a larger or smaller physical
            // representation, so validating against `size` rejects valid later parts.
            validate_copy_source_range_not_exceeds(range_spec, source_logical_size)?;

            range_spec
                .get_offset_length(source_logical_size)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRange, e.to_string()))?
        } else {
            (0, source_logical_size)
        };

        // Resolve the range under the prepared source's read lock, then try the
        // shared budget without waiting while holding namespace/lifecycle locks.
        // Keep both the budget and source cancellation alive through the write.
        let admission_length = source_actual_size.map_or(-1, |_| length);
        let _copy_admission = match self.concurrency_manager().try_admit_multipart_part_copy(admission_length) {
            ForegroundWriteAdmission::Disabled => None,
            ForegroundWriteAdmission::Admitted(permit) => Some(permit),
            ForegroundWriteAdmission::Rejected => {
                return Err(S3Error::with_message(
                    S3ErrorCode::SlowDown,
                    "foreground write concurrency limit reached, please reduce your request rate",
                ));
            }
        };
        let (src_reader, _source_cancellation) = prepared_source
            .into_reader_for_copy()
            .await
            .map_err(map_get_object_reader_error)?;
        let src_stream = src_reader.stream;

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

        let server_side_encryption = mp_info.user_defined.get("x-amz-server-side-encryption").cloned();
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
            // Reuse the destination envelope after authorizing this caller against
            // its pinned key above. The source read is separately authorized.
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
            e_tag: part_info.etag.map(|etag| to_s3s_xml_etag(&etag)),
            last_modified: part_info.last_mod.map(Timestamp::from),
            ..Default::default()
        };

        let output = UploadPartCopyOutput {
            copy_part_result: Some(copy_part_result),
            copy_source_version_id: resolved_src_version_id,
            server_side_encryption: requested_sse.map(ServerSideEncryption::from),
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
    use super::super::test_support::*;
    use super::*;

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

    #[test]
    fn upload_part_copy_normalizes_and_formats_source_version_id() {
        assert_eq!(normalize_copy_source_version_id(None).expect("missing version id"), None);
        assert_eq!(
            normalize_copy_source_version_id(Some(" null ")).expect("null version id"),
            Some(Uuid::nil().to_string())
        );
        let valid = Uuid::new_v4();
        assert_eq!(
            normalize_copy_source_version_id(Some(&format!(" {valid} "))).expect("valid uuid"),
            Some(valid.to_string())
        );
        let err = normalize_copy_source_version_id(Some("not-a-uuid")).expect_err("invalid uuid should fail");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);

        assert_eq!(format_copy_source_version_id(Some(Uuid::nil()), false, false), None);
        assert_eq!(format_copy_source_version_id(None, true, false), Some("null".to_string()));
        assert_eq!(format_copy_source_version_id(Some(Uuid::nil()), false, true), Some("null".to_string()));
        assert_eq!(format_copy_source_version_id(Some(valid), true, false), Some(valid.to_string()));
    }

    mod copy_admission_tests;
}
