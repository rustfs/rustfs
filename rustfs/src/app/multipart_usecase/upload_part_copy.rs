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

//! UploadPartCopy through one owned storage implementation and its legacy adapter.
//! Gateway and protocol clients supply request facts. Storage owns the multipart upload.

use super::*;
use crate::app::{
    RequestEnvelope,
    object_usecase::{CopySourceCondition, CopySourceDates},
};
use rustfs_gateway_types::CopySourceValue;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};

// No Debug: source and destination customer encryption keys are bearer material.
pub(crate) struct UploadPartCopyWriteInput {
    pub(crate) target: UploadPartTarget,
    pub(crate) part_number: i32,
    pub(crate) copy_source: CopySourceValue,
    pub(crate) copy_source_range: Option<String>,
    pub(crate) copy_source_if_match: Option<CopySourceCondition>,
    pub(crate) copy_source_if_none_match: Option<CopySourceCondition>,
    pub(crate) copy_source_dates: CopySourceDates,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) copy_source_sse_customer_algorithm: Option<String>,
    pub(crate) copy_source_sse_customer_key: Option<String>,
    pub(crate) copy_source_sse_customer_key_md5: Option<String>,
}

pub(crate) struct UploadPartCopyWriteOutput {
    pub(crate) e_tag: Option<String>,
    pub(crate) last_modified: Option<time::OffsetDateTime>,
    pub(crate) copy_source_version_id: Option<String>,
    pub(crate) server_side_encryption: Option<String>,
    pub(crate) ssekms_key_id: Option<String>,
    pub(crate) sse_customer_algorithm: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
    pub(crate) checksum_crc32: Option<String>,
    pub(crate) checksum_crc32c: Option<String>,
    pub(crate) checksum_sha1: Option<String>,
    pub(crate) checksum_sha256: Option<String>,
    pub(crate) checksum_crc64nvme: Option<String>,
}

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

fn validate_upload_part_copy_source_key(src_key: &str) -> S3Result<()> {
    crate::app::storage_api::multipart_usecase::validate_object_key(src_key, "UPLOAD_PART_COPY (source)")?;
    if src_key.starts_with('/') {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            "Object name contains forward slash as prefix",
        ));
    }
    Ok(())
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
    /// Copies plaintext into an existing upload without changing its recorded owner.
    /// Records the gateway audit. The protocol client records its own projected response.
    pub(crate) async fn execute_upload_part_copy_with_audit(
        &self,
        envelope: RequestEnvelope,
        input: UploadPartCopyWriteInput,
    ) -> S3Result<UploadPartCopyWriteOutput> {
        RequestEnvelope::audit_only_operation(envelope, S3Operation::UploadPartCopy, http::StatusCode::OK, |envelope| {
            Box::pin(self.execute_upload_part_copy_owned(envelope, input))
        })
        .await
    }

    #[instrument(level = "debug", skip(self, envelope, input))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub(crate) async fn execute_upload_part_copy_owned(
        &self,
        envelope: RequestEnvelope,
        input: UploadPartCopyWriteInput,
    ) -> S3Result<UploadPartCopyWriteOutput> {
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            envelope.headers(),
            envelope.uri().query(),
            envelope.verified_sigv4().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            envelope.headers(),
            envelope.uri().query(),
            envelope.verified_presigned().is_some(),
        )?;
        let copy_principal = SseKmsPrincipal::from_request(&envelope);
        let source_bucket = match &input.copy_source {
            CopySourceValue::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySourceValue::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySourceValue::Bucket { bucket, .. } => bucket.to_string(),
        };
        let mut source_generation_opts = ObjectOptions::default();
        apply_copy_source_bucket_generation_guard(&envelope, &source_bucket, &mut source_generation_opts)?;
        let expected_source_incarnation_id = source_generation_opts.expected_bucket_incarnation_id;
        let (bucket, key) = input.target.resource();
        let (bucket, key) = (bucket.to_owned(), key.to_owned());
        let mut destination_generation_opts = ObjectOptions::default();
        apply_bucket_generation_guard(&envelope, &bucket, &mut destination_generation_opts)?;
        let expected_destination_incarnation_id = destination_generation_opts.expected_bucket_incarnation_id;
        let UploadPartCopyWriteInput {
            target,
            copy_source,
            copy_source_range,
            part_number,
            copy_source_if_match,
            copy_source_if_none_match,
            mut copy_source_dates,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key,
            copy_source_sse_customer_key_md5,
            ..
        } = input;

        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        let (src_bucket, src_key, raw_src_version_id) = match copy_source {
            CopySourceValue::AccessPoint { .. } => return Err(s3_error!(NotImplemented)),
            CopySourceValue::Outpost { .. } => return Err(s3_error!(NotImplemented)),
            CopySourceValue::Bucket {
                bucket: src_bucket,
                key: src_key,
                version_id,
            } => (src_bucket, src_key, version_id),
        };
        let src_version_id = normalize_copy_source_version_id(raw_src_version_id.as_deref())?;
        validate_upload_part_copy_source_key(&src_key)?;

        let headers = envelope.headers();
        let copy_source_if_match = CopySourceCondition::with_header(copy_source_if_match, headers, "x-amz-copy-source-if-match")?;
        let copy_source_if_none_match =
            CopySourceCondition::with_header(copy_source_if_none_match, headers, "x-amz-copy-source-if-none-match")?;
        copy_source_dates.merge_headers(headers)?;
        let copy_source_range = match copy_source_range {
            Some(range) => Some(range),
            None => crate::app::object::shared::optional_header_string(headers, "x-amz-copy-source-range")?,
        };
        let rs = copy_source_range.as_deref().map(parse_copy_source_range).transpose()?;

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
            multipart_copy_source_locked: true,
            expected_bucket_incarnation_id: Some(current_destination_incarnation_id),
            ..Default::default()
        };
        if src_bucket != bucket {
            source_bucket_lifecycle_guard.attach_to_object_options(&mut dst_opts);
        }
        destination_bucket_lifecycle_guard.attach_to_object_options(&mut dst_opts);

        let (upload_id, mp_info) = match target {
            UploadPartTarget::Legacy { upload_id, .. } => {
                let info = store
                    .get_multipart_info(&bucket, &key, &upload_id, &dst_opts)
                    .await
                    .map_err(multipart_upload_api_error)?;
                (upload_id, info)
            }
            UploadPartTarget::Gateway {
                bucket: bound_bucket,
                key: bound_key,
                upload_id,
            } => {
                let (id, upload) = rustfs_gateway_types::resolve_upload_async(&upload_id, &bound_bucket, &bound_key, |id| {
                    let store = &store;
                    let opts = &dst_opts;
                    let bucket = &bucket;
                    let key = &key;
                    async move { super::upload_part::lookup_recorded_part_upload(store, bucket, key, id, opts).await }
                })
                .await?
                .map_err(|rejection| S3Error::with_message(S3ErrorCode::NoSuchUpload, rejection.reason()))?;
                (id.id().to_owned(), upload.info)
            }
        };
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
        .validate_multipart_ssec(&mp_info.user_defined)
        .map_err(ApiError::from)?;

        let src_versioned = BucketVersioningSys::prefix_enabled(&src_bucket, &src_key).await;
        let src_version_suspended = BucketVersioningSys::prefix_suspended(&src_bucket, &src_key).await;
        let mut src_opts = copy_src_opts(&src_bucket, &src_key, envelope.headers()).map_err(ApiError::from)?;
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

        if !copy_source_dates.permits_copy(
            copy_source_if_match.as_ref(),
            copy_source_if_none_match.as_ref(),
            src_info.etag.as_deref(),
            src_info.mod_time,
        ) {
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
            Some(sse) if sse.as_str() == rustfs_utils::http::headers::AMZ_ENCRYPTION_KMS => mp_info
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
            // An SSE-C part answers with its customer-key headers only.
            (None, None, mp_info.user_defined.clone())
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

        Ok(UploadPartCopyWriteOutput {
            e_tag: part_info.etag,
            last_modified: part_info.last_mod,
            copy_source_version_id: resolved_src_version_id,
            server_side_encryption: requested_sse,
            ssekms_key_id: requested_kms_key_id,
            sse_customer_algorithm,
            sse_customer_key_md5,
            checksum_crc32: checksum_value(rustfs_rio::ChecksumType::CRC32),
            checksum_crc32c: checksum_value(rustfs_rio::ChecksumType::CRC32C),
            checksum_sha1: checksum_value(rustfs_rio::ChecksumType::SHA1),
            checksum_sha256: checksum_value(rustfs_rio::ChecksumType::SHA256),
            checksum_crc64nvme: checksum_value(rustfs_rio::ChecksumType::CRC64_NVME),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::app::multipart_usecase::UploadPartTarget;

    fn copy_part_input(
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: i32,
        copy_source: CopySourceValue,
    ) -> UploadPartCopyWriteInput {
        UploadPartCopyWriteInput {
            target: UploadPartTarget::Legacy {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                upload_id: upload_id.to_owned(),
            },
            part_number,
            copy_source,
            copy_source_range: None,
            copy_source_if_match: None,
            copy_source_if_none_match: None,
            copy_source_dates: Default::default(),
            sse_customer_algorithm: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
            copy_source_sse_customer_algorithm: None,
            copy_source_sse_customer_key: None,
            copy_source_sse_customer_key_md5: None,
        }
    }

    #[tokio::test]
    async fn execute_upload_part_copy_returns_internal_error_when_store_uninitialized() {
        let input = copy_part_input(
            "bucket",
            "object",
            "upload-id",
            1,
            CopySourceValue::Bucket {
                bucket: "src-bucket".into(),
                key: "src-object".into(),
                version_id: None,
            },
        );
        let err = match Box::pin(make_usecase().execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input)).await {
            Err(error) => error,
            Ok(_) => panic!("uninitialized store must be an internal error"),
        };
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }

    #[tokio::test]
    async fn execute_upload_part_copy_rejects_invalid_part_number_before_store_lookup() {
        for part_number in [-1, 0, 10001] {
            let input = copy_part_input(
                "bucket",
                "object",
                "upload-id",
                part_number,
                CopySourceValue::Bucket {
                    bucket: "src-bucket".into(),
                    key: "src-object".into(),
                    version_id: None,
                },
            );
            let err = match Box::pin(make_usecase().execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input)).await
            {
                Err(error) => error,
                Ok(_) => panic!("invalid part number must be rejected"),
            };
            assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
            assert_eq!(err.message(), Some("partNumber must be between 1 and 10000"));
        }
    }

    #[tokio::test]
    async fn execute_upload_part_copy_rejects_leading_slash_and_invalid_source_keys_before_store_lookup() {
        for invalid_src_key in [
            "/tables/table-uuid/data/00000-0-rows.parquet",
            "/.rustfs-table/warehouses/default/metadata.json",
            "//tables/table-uuid/data/00000-0-rows.parquet",
            "/",
            "",
            "a/../b",
            "a//b",
            "a\0b",
        ] {
            let input = copy_part_input(
                "bucket",
                "object",
                "upload-id",
                1,
                CopySourceValue::Bucket {
                    bucket: "src-bucket".into(),
                    key: invalid_src_key.into(),
                    version_id: None,
                },
            );
            let err = match Box::pin(make_usecase().execute_upload_part_copy_with_audit(owned_envelope(Method::PUT), input)).await
            {
                Err(error) => error,
                Ok(_) => panic!("invalid copy source key {invalid_src_key:?} must be rejected"),
            };
            assert_eq!(
                err.code(),
                &S3ErrorCode::InvalidArgument,
                "invalid copy source key {invalid_src_key:?} must fail with InvalidArgument"
            );
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
