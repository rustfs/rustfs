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

//! CompleteMultipartUpload.

use super::*;

fn validate_complete_multipart_parts(parts: &[CompletePart]) -> rustfs_s3_types::S3Result<()> {
    if let Some(part) = parts
        .iter()
        .find(|part| !(1..=MAX_MULTIPART_PART_NUMBER as usize).contains(&part.part_num))
    {
        return Err(rustfs_s3_types::S3Error::with_message(
            rustfs_s3_types::S3ErrorCode::InvalidPart,
            format!("Part number {} must be between 1 and {MAX_MULTIPART_PART_NUMBER}", part.part_num),
        ));
    }

    if parts.windows(2).any(|window| window[0].part_num >= window[1].part_num) {
        return Err(rustfs_s3_types::s3_error!(InvalidPartOrder, "Part numbers must be strictly increasing"));
    }

    Ok(())
}

fn normalize_complete_multipart_parts(parts: Vec<CompletePart>) -> rustfs_s3_types::S3Result<Vec<CompletePart>> {
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

fn complete_part_from_s3(value: CompletedPart) -> CompletePart {
    CompletePart {
        part_num: value
            .part_number
            .and_then(|part_num| usize::try_from(part_num).ok())
            .unwrap_or_default(),
        etag: value.e_tag.map(|v| v.value().to_owned()),
        checksum_crc32: value.checksum_crc32,
        checksum_crc32c: value.checksum_crc32c,
        checksum_sha1: value.checksum_sha1,
        checksum_sha256: value.checksum_sha256,
        checksum_crc64nvme: value.checksum_crc64nvme,
    }
}

fn has_complete_multipart_object_lock_headers(headers: &HeaderMap) -> bool {
    headers.contains_key(X_AMZ_OBJECT_LOCK_MODE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE)
        || headers.contains_key(X_AMZ_OBJECT_LOCK_LEGAL_HOLD)
        || has_bypass_governance_header(headers)
}

/// Reject a CompleteMultipartUpload whose `x-amz-checksum-type` contradicts the
/// type the upload was created with, the way AWS does (`InvalidRequest`).
///
/// The storage layer already refuses the combination -- `complete_multipart_upload`
/// compares `opts.want_checksum` against the algorithm/type pair recorded under
/// `x-rustfs-multipart-checksum*` at CreateMultipartUpload -- but it refuses with a
/// generic error that reaches the caller as `500 InternalError`, telling them to
/// retry a request that can only ever fail. The recorded type is already in hand
/// here, so the contradiction is answered as the client error it is.
///
/// Uploads created without a checksum algorithm record no type. There is nothing
/// to contradict in that case, so the header is left alone rather than newly
/// rejected.
fn validate_complete_multipart_checksum_type(headers: &HeaderMap, upload_metadata: &HashMap<String, String>) -> S3Result<()> {
    let Some(requested) = headers
        .get(AMZ_CHECKSUM_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    let Some(recorded) = upload_metadata
        .get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE)
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    if requested != recorded {
        // Routed through `ApiError` rather than the s3s error macro so this
        // validation does not widen the direct s3s surface the s3gate migration
        // is shrinking (scripts/check_s3s_footprint.sh); the response is identical.
        return Err(ApiError::invalid_request(format!(
            "The upload was created with checksum type {recorded}. The complete request must use the same checksum type, got {requested}."
        ))
        .into());
    }

    Ok(())
}

fn quota_accounting_object_size(info: &ObjectInfo, fail_closed: bool) -> S3Result<u64> {
    match quota_object_size(info) {
        Ok(size) => Ok(size),
        Err(err) if fail_closed => Err(ApiError::from(err).into()),
        Err(_) => Ok(info.size.max(0) as u64),
    }
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

impl DefaultMultipartUsecase {
    #[instrument(level = "debug", skip(self, req))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let owner = self.clone();
        run_owned_mutation(async move { Box::pin(owner.execute_complete_multipart_upload_owned(req)).await }).await
    }

    async fn execute_complete_multipart_upload_owned(
        &self,
        mut req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        validate_new_object_key(&req.input.key, "CompleteMultipartUpload")?;
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
        let mut helper = OperationHelper::new(
            &req,
            EventName::ObjectCreatedCompleteMultipartUpload,
            S3Operation::CompleteMultipartUpload,
        );
        let replication_authorized = replication_request_authorized(&req);
        let input = std::mem::take(&mut req.input);
        let CompleteMultipartUploadInput {
            multipart_upload,
            bucket,
            key,
            upload_id,
            if_match,
            if_none_match,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = input;

        let customer_key_provided = sse_customer_key.is_some();
        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        reject_non_wildcard_write_if_none_match(if_none_match.as_ref(), &req.headers)?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        if if_match.is_some() || if_none_match.is_some() {
            let Some(store) = self.object_store() else {
                return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
            };

            match store.get_object_info(&bucket, &key, &ObjectOptions::default()).await {
                Ok(info) => {
                    if info.delete_marker {
                        if if_match.is_some() {
                            return Err(ApiError::from(StorageError::ObjectNotFound(bucket.clone(), key.clone())).into());
                        }
                    } else {
                        if let Some(ifmatch) = &if_match
                            && !ifmatch.is_any()
                            && ifmatch
                                .as_etag()
                                .and_then(ETag::as_strong)
                                .is_none_or(|expected| info.etag.as_deref() != Some(expected))
                        {
                            return Err(s3_error!(PreconditionFailed));
                        }
                        // The request guard permits only the wildcard here.
                        if if_none_match.is_some() {
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
        let Some(parts) = multipart_upload.parts.filter(|parts| !parts.is_empty()) else {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "You must specify at least one part".to_string(),
            ));
        };

        let mut opts = get_complete_multipart_upload_opts_with_replication_authorization(&req.headers, replication_authorized)
            .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        let (versioned, version_suspended) = BucketVersioningSys::write_state(&bucket, &key)
            .await
            .map_err(ApiError::from)?;
        opts.versioned = versioned;
        opts.version_suspended = version_suspended;
        let capacity_scope_token = Uuid::new_v4();
        opts.capacity_scope_token = Some(capacity_scope_token);

        let uploaded_parts_vec = parts.into_iter().map(complete_part_from_s3).collect::<Vec<_>>();

        let uploaded_parts = normalize_complete_multipart_parts(uploaded_parts_vec)?;

        if has_complete_multipart_object_lock_headers(&req.headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "CompleteMultipartUpload does not accept object lock or governance bypass headers.".to_string(),
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // A lost Complete response retries the same upload id after staging is
        // gone. `get_multipart_info` is NoSuchUpload in that case; completion
        // below replays the committed object instead of failing the retry.
        let multipart_info = match store.get_multipart_info(&bucket, &key, &upload_id, &opts).await {
            Ok(info) => Some(info),
            Err(err) if is_err_invalid_upload_id(&err) => None,
            Err(err) => return Err(multipart_upload_api_error(err)),
        };
        let completion_principal = SseKmsPrincipal::from_request(&req);
        let completion_authorization = multipart_completion_authorization(
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
                principal: completion_principal.as_ref(),
            },
            replication_authorized,
        );
        opts.multipart_completion_authorization = Some(completion_authorization.clone());
        if let Some(info) = multipart_info.as_ref() {
            completion_authorization
                .authorize(&info.user_defined)
                .await
                .map_err(map_get_object_reader_error)?;
            validate_complete_multipart_checksum_type(&req.headers, &info.user_defined)?;
        }

        let quota_metadata_sys = self.bucket_metadata_sys();
        let quota_tracking = quota_metadata_sys.is_some();
        let mut quota_enabled = false;
        if let Some(metadata_sys) = quota_metadata_sys.as_ref() {
            let quota_checker = QuotaChecker::new(metadata_sys.clone());
            let check_result =
                map_quota_check_outcome(&bucket, quota_checker.check_quota(&bucket, QuotaOperation::PutObject, 0).await)?;
            quota_enabled = check_result.quota_limit.is_some();
            apply_quota_admission(&mut opts, &check_result)?;
        }

        let object_lock_config_state = Box::pin(load_bucket_object_lock_config_state(&bucket)).await?;
        let prelookup_required = quota_enabled
            || (!super::super::object::shared::put_like_write_creates_new_version(&opts)
                && super::super::object::shared::object_lock_checks_required_for_state(&object_lock_config_state));
        let prelookup_previous_current_size: Option<Option<u64>> = if prelookup_required {
            let current_opts = internal_object_info_lookup_opts(
                get_opts(&bucket, &key, None, None, &req.headers)
                    .await
                    .map_err(ApiError::from)?,
            );
            match store.get_object_info(&bucket, &key, &current_opts).await {
                Ok(existing_obj_info) => {
                    validate_existing_object_lock_for_write(&object_lock_config_state, &existing_obj_info, &current_opts)?;
                    Some(Some(if quota_enabled {
                        quota_object_size(&existing_obj_info).map_err(ApiError::from)?
                    } else {
                        existing_obj_info.size.max(0) as u64
                    }))
                }
                Err(err) => {
                    if !is_err_object_not_found(&err) && !is_err_version_not_found(&err) {
                        return Err(ApiError::from(err).into());
                    }
                    Some(None)
                }
            }
        } else {
            None
        };

        let cache_adapter = self.object_data_cache();
        let _ = invalidate_object_data_cache_before_mutation(&cache_adapter, &bucket, &key).await;

        let upload_user_defined = multipart_info.as_ref().map(|info| info.user_defined.clone());
        let table_data_plane_publication_guards = retained_table_data_plane_publication_guards(&req);

        // Replication admission is decided at the object-creation commit, not
        // at multipart-session creation. Configuration and matching targets
        // can change while parts are uploaded, so persist the exact decision's
        // generation and PENDING set atomically with the completed object and
        // reuse the same decision for scheduling below. A retry whose upload
        // is already gone must not re-evaluate or rewrite that decision.
        let completion_replication_decision = if let Some(user_defined) = upload_user_defined.as_ref() {
            let completion_replication_decision = must_replicate_object(
                &bucket,
                &key,
                user_defined,
                "".to_string(),
                opts.delete_marker_replication_status(),
                opts.clone(),
            )
            .await;
            let mut completion_replication_metadata = HashMap::new();
            if completion_replication_decision.replicate_any() {
                insert_str(
                    &mut completion_replication_metadata,
                    SUFFIX_REPLICATION_GENERATION,
                    Uuid::new_v4().to_string(),
                );
                insert_str(
                    &mut completion_replication_metadata,
                    SUFFIX_REPLICATION_TIMESTAMP,
                    jiff::Zoned::now().to_string(),
                );
                insert_str(
                    &mut completion_replication_metadata,
                    SUFFIX_REPLICATION_STATUS,
                    completion_replication_decision.pending_status().unwrap_or_default(),
                );
            }
            // `Some(empty)` deliberately means that completion re-evaluated the
            // session as not admitted; storage removes stale Create-MPU admission
            // metadata in the same final-object commit.
            opts.eval_metadata = Some(completion_replication_metadata);
            Some(completion_replication_decision)
        } else {
            None
        };

        let complete_commit = spawn_traced_join({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let key = key.clone();
            let upload_id = upload_id.clone();
            let opts = opts.clone();
            let table_data_plane_publication_guards = table_data_plane_publication_guards;
            async move {
                let _table_data_plane_publication_guards = table_data_plane_publication_guards;
                let obj_info = store
                    .clone()
                    .complete_multipart_upload(&bucket, &key, &upload_id, uploaded_parts, &opts)
                    .await
                    .map_err(map_get_object_reader_error)?;
                let _ = invalidate_object_data_cache_after_complete_multipart_success(&cache_adapter, &bucket, &key).await;
                // Staging reclaim on a replay can free bytes; the dirty mark is
                // not a usage counter. Quota, replication, lifecycle, and the
                // object-created scanner signal belong to the commit that
                // published the object.
                record_capacity_write(Some(capacity_scope_token)).await;

                if !obj_info.multipart_completion_replayed {
                    if quota_tracking {
                        let committed_size = quota_accounting_object_size(&obj_info, quota_enabled)?;
                        match prelookup_previous_current_size.or_else(|| {
                            super::super::object::previous_current_size_from_backfill(obj_info.backfilled_old_current_size)
                        }) {
                            Some(previous_current_size) => {
                                if versioned {
                                    record_bucket_object_version_write_memory(&bucket, previous_current_size, committed_size)
                                        .await;
                                } else {
                                    record_bucket_object_write_memory(&bucket, previous_current_size, committed_size).await;
                                }
                            }
                            None => {
                                record_bucket_object_write_unknown_previous_memory(&bucket, committed_size, versioned).await;
                            }
                        }
                    }

                    enqueue_transition_immediate(&obj_info, LcEventSrc::S3CompleteMultipartUpload).await;

                    if let Some(decision) = completion_replication_decision
                        && decision.replicate_any()
                    {
                        warn!("need multipart replication");
                        schedule_object_replication(obj_info.clone(), store, decision).await;
                    }

                    rustfs_scanner::record_dirty_usage_object_from_producer(
                        &bucket,
                        &key,
                        rustfs_scanner::SegmentInvalidationProducerIdentity::CompleteMultipartUpload,
                    );
                }
                Ok::<_, S3Error>(obj_info)
            }
        });
        let obj_info = complete_commit.await.map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("complete multipart upload commit owner task failed: {err}"),
            )
        })??;

        #[cfg(test)]
        super::super::storage_api::multipart_usecase::helper::pause_committed_mutation_for_test(&bucket).await;

        // Storage has authorized the actual source or committed replay candidate
        // before comparing checksums/parts, recovery publication or replay effects.
        let encryption_metadata = upload_user_defined.as_ref().unwrap_or(obj_info.user_defined.as_ref());
        let server_side_encryption = encryption_metadata
            .get("x-amz-server-side-encryption")
            .map(|s| ServerSideEncryption::from(s.clone()));
        let ssekms_key_id = match server_side_encryption.as_ref() {
            Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS => encryption_metadata
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .cloned(),
            _ => None,
        };
        let ssec_algorithm = encryption_metadata
            .get("x-amz-server-side-encryption-customer-algorithm")
            .cloned();
        let ssec_key_md5 = encryption_metadata
            .get("x-amz-server-side-encryption-customer-key-md5")
            .cloned();
        let replayed = obj_info.multipart_completion_replayed;

        let mpu_version = if versioned {
            obj_info.version_id.map(|v| v.to_string())
        } else {
            None
        };
        let mpu_version_for_event = mpu_version.clone();
        // checksum: stored (decrypted) values take precedence over the request input;
        // additional algorithms (XXHash3/64/128, SHA-512, MD5), which have no typed
        // CompleteMultipartUploadOutput field, are echoed as raw response headers (#1261).
        // Keyless completion remains supported, but plaintext checksums are
        // returned only after verifying that key (backlog#3050). The customer-key
        // MD5 is a fingerprint of the key, not of the plaintext, and the S3
        // conformance suite expects it on a keyless completion, so it is echoed
        // unconditionally. Apply this equally to a new commit and a replay.
        let disclose_checksums = (ssec_algorithm.is_none() && ssec_key_md5.is_none()) || customer_key_provided;
        let classified = if disclose_checksums {
            let (checksums, is_multipart) = obj_info
                .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                .map_err(ApiError::from)?;
            crate::app::object_usecase::classify_response_checksums(checksums, is_multipart)
        } else {
            Default::default()
        };
        let checksum_crc32 = if disclose_checksums {
            classified.crc32.or(input.checksum_crc32)
        } else {
            None
        };
        let checksum_crc32c = if disclose_checksums {
            classified.crc32c.or(input.checksum_crc32c)
        } else {
            None
        };
        let checksum_sha1 = if disclose_checksums {
            classified.sha1.or(input.checksum_sha1)
        } else {
            None
        };
        let checksum_sha256 = if disclose_checksums {
            classified.sha256.or(input.checksum_sha256)
        } else {
            None
        };
        let checksum_crc64nvme = if disclose_checksums {
            classified.crc64nvme.or(input.checksum_crc64nvme)
        } else {
            None
        };
        let checksum_type = if disclose_checksums {
            classified.checksum_type.or(input.checksum_type)
        } else {
            None
        };
        let complete_extra_checksum_headers = classified.extra;

        let location = build_complete_multipart_location(&req.headers, &req.uri, &bucket, &key);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket.clone()),
            key: Some(key.clone()),
            e_tag: obj_info.etag.clone().map(|etag| to_s3s_xml_etag(&etag)),
            location: Some(location),
            server_side_encryption,
            ssekms_key_id,
            checksum_crc32,
            checksum_crc32c,
            checksum_sha1,
            checksum_sha256,
            checksum_crc64nvme,
            checksum_type,
            version_id: mpu_version,
            ..Default::default()
        };
        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }

        let mut response = S3Response::new(output);
        crate::app::object_usecase::inject_additional_checksum_headers(&mut response.headers, &complete_extra_checksum_headers);
        if let Some(algorithm) = ssec_algorithm.as_deref() {
            let value = HeaderValue::from_str(algorithm)
                .map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C algorithm metadata"))?;
            response
                .headers
                .insert("x-amz-server-side-encryption-customer-algorithm", value);
        }
        if let Some(key_md5) = ssec_key_md5.as_deref() {
            let value =
                HeaderValue::from_str(key_md5).map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C key metadata"))?;
            response
                .headers
                .insert("x-amz-server-side-encryption-customer-key-md5", value);
        }
        if replayed {
            // The object-created event was emitted by the commit that published
            // the version. The retry is still a successful API call.
            helper = helper.suppress_event();
        }
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;

    fn upload_metadata_with_checksum_type(recorded: &str) -> HashMap<String, String> {
        HashMap::from([(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_string(), recorded.to_string())])
    }

    fn headers_with_checksum_type(requested: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(AMZ_CHECKSUM_TYPE, HeaderValue::from_str(requested).expect("header value"));
        headers
    }

    /// A CompleteMultipartUpload that restates the type the upload was created
    /// with is the normal AWS SDK request shape and must pass through.
    #[test]
    fn complete_multipart_checksum_type_matching_the_upload_is_accepted() {
        for kind in ["FULL_OBJECT", "COMPOSITE"] {
            validate_complete_multipart_checksum_type(
                &headers_with_checksum_type(kind),
                &upload_metadata_with_checksum_type(kind),
            )
            .unwrap_or_else(|err| panic!("{kind} must be accepted, got {err:?}"));
        }
    }

    /// Contradicting the recorded type is a client error, not an internal one:
    /// before this check the storage layer refused it as a generic error and the
    /// caller saw `500 InternalError`.
    #[test]
    fn complete_multipart_checksum_type_contradicting_the_upload_is_rejected() {
        for (requested, recorded) in [("COMPOSITE", "FULL_OBJECT"), ("FULL_OBJECT", "COMPOSITE")] {
            let err = validate_complete_multipart_checksum_type(
                &headers_with_checksum_type(requested),
                &upload_metadata_with_checksum_type(recorded),
            )
            .expect_err("contradicting checksum type must be rejected");
            assert_eq!(*err.code(), S3ErrorCode::InvalidRequest, "must be a client error");
            let message = err.message().unwrap_or_default().to_string();
            assert!(
                message.contains(requested) && message.contains(recorded),
                "message must name both types, got {message}"
            );
        }
    }

    /// No header, an empty header, and an upload that recorded no checksum type
    /// all leave the request untouched -- the check only resolves contradictions.
    #[test]
    fn complete_multipart_checksum_type_without_both_sides_is_accepted() {
        validate_complete_multipart_checksum_type(&HeaderMap::new(), &upload_metadata_with_checksum_type("FULL_OBJECT"))
            .expect("absent header is not a contradiction");
        validate_complete_multipart_checksum_type(
            &headers_with_checksum_type(""),
            &upload_metadata_with_checksum_type("FULL_OBJECT"),
        )
        .expect("empty header is not a contradiction");
        validate_complete_multipart_checksum_type(&headers_with_checksum_type("FULL_OBJECT"), &HashMap::new())
            .expect("upload without a recorded checksum type is not a contradiction");
    }

    #[test]
    fn quota_accounting_uses_logical_size_when_available() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "8192".to_string());
        let info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        assert_eq!(quota_accounting_object_size(&info, true).expect("logical size should resolve"), 8192);
        assert_eq!(quota_accounting_object_size(&info, false).expect("logical size should resolve"), 8192);

        let mut poisoned_metadata = HashMap::new();
        insert_str(&mut poisoned_metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut poisoned_metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "1".to_string());
        let poisoned = ObjectInfo {
            size: 17,
            parts: Arc::new(vec![rustfs_filemeta::ObjectPartInfo {
                size: 4096,
                actual_size: 4096,
                ..Default::default()
            }]),
            user_defined: Arc::new(poisoned_metadata),
            ..Default::default()
        };
        assert_eq!(
            quota_accounting_object_size(&poisoned, true).expect("persisted part size must be charged"),
            4096
        );
    }

    #[test]
    fn quota_accounting_fails_closed_only_when_quota_is_configured() {
        let mut metadata = HashMap::new();
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
        insert_str(&mut metadata, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "-1".to_string());
        let info = ObjectInfo {
            size: 128,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let err = quota_accounting_object_size(&info, true).expect_err("invalid logical size must fail closed");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert_eq!(quota_accounting_object_size(&info, false).expect("physical fallback should resolve"), 128);
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
    async fn execute_complete_multipart_upload_rejects_missing_parts_list() {
        use crate::app::storage_api::s3::xml::Deserialize as _;

        let mut deserializer = crate::app::storage_api::s3::xml::Deserializer::new(b"<CompleteMultipartUpload/>");
        let multipart_upload =
            CompletedMultipartUpload::deserialize(&mut deserializer).expect("empty multipart XML should decode");
        deserializer
            .expect_eof()
            .expect("empty multipart XML should be fully consumed");
        assert!(multipart_upload.parts.is_none());

        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .multipart_upload(Some(multipart_upload))
            .build()
            .expect("complete multipart input should build");
        let req = build_request(input, Method::POST);

        let err = Box::pin(make_usecase().execute_complete_multipart_upload(req))
            .await
            .expect_err("missing parts list must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn compressed_complete_records_logical_quota_usage_and_overwrite_delta() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("compressed-complete-quota", 16_384).await;
        let object = "object";

        let usecase = DefaultMultipartUsecase::from_global();
        let metadata_sys = usecase
            .bucket_metadata_sys()
            .expect("test app context should expose bucket metadata");
        let quota_checker = QuotaChecker::new(metadata_sys);

        for (actual_size, payload_byte) in [(8192_i64, 0x61), (4096_i64, 0x62)] {
            let mut create_opts = ObjectOptions::default();
            insert_str(&mut create_opts.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION, "S2".to_string());
            let upload = store
                .new_multipart_upload(&bucket, object, &create_opts)
                .await
                .expect("create compressed multipart upload");
            let payload = vec![payload_byte; 128];
            let mut part_reader = PutObjReader::new(
                HashReader::from_stream(Cursor::new(payload), 128, actual_size, None, None, false)
                    .expect("construct compressed part reader"),
            );
            let staged_part = store
                .put_object_part(&bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
                .await
                .expect("write compressed multipart part");
            let staged_etag = staged_part.etag.expect("staged compressed part should have an ETag");
            let input = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .upload_id(upload.upload_id)
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: Some(to_s3s_etag(&staged_etag)),
                        ..Default::default()
                    }]),
                }))
                .build()
                .expect("complete multipart input should build");
            usecase
                .execute_complete_multipart_upload(build_request(input, Method::POST))
                .await
                .expect("compressed multipart completion should succeed");

            let quota = quota_checker
                .check_quota(&bucket, QuotaOperation::PutObject, 0)
                .await
                .expect("read live quota usage");
            assert_eq!(quota.current_usage, Some(actual_size as u64));
        }
    }

    #[test]
    #[serial_test::serial]
    fn authorized_replica_only_multipart_complete_preserves_persisted_anti_cascade_state() {
        crate::app::gating_test_env::run_large_stack_test(
            "authorized-replica-only-multipart-complete",
            authorized_replica_only_multipart_complete_preserves_persisted_anti_cascade_state_inner,
        );
    }

    async fn authorized_replica_only_multipart_complete_preserves_persisted_anti_cascade_state_inner() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("replica-only-complete", 16_384).await;
        let object = "object";
        let usecase = DefaultMultipartUsecase::from_global();

        let create_input = CreateMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("create multipart input should build");
        let mut create_request = build_request(create_input, Method::POST);
        create_request
            .headers
            .insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
        create_request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });
        let upload_id = usecase
            .execute_create_multipart_upload(create_request)
            .await
            .expect("authorized REPLICA-only create should succeed")
            .output
            .upload_id
            .expect("create response should contain upload id");

        let mut reader = PutObjReader::from_vec(b"authorized replica-only multipart body".to_vec());
        let part = store
            .put_object_part(&bucket, object, &upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("replica part should be staged");
        let complete_input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(upload_id)
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: part.etag.map(|etag| to_s3s_etag(&etag)),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("complete multipart input should build");
        let mut complete_request = build_request(complete_input, Method::POST);
        complete_request
            .headers
            .insert(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
        complete_request.extensions.insert(crate::storage::access::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });
        usecase
            .execute_complete_multipart_upload(complete_request)
            .await
            .expect("authorized REPLICA-only completion should succeed without the source-request marker");

        let persisted = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("completed replica should be readable from storage");
        assert_eq!(
            persisted
                .user_defined
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(AMZ_BUCKET_REPLICATION_STATUS))
                .map(|(_, value)| value.as_str()),
            Some("REPLICA"),
            "the committed xl.meta must retain REPLICA to prevent scanner cascades"
        );
    }

    #[test]
    #[serial_test::serial]
    fn ssec_completion_and_replay_require_key_before_disclosing_plaintext_checksums() {
        crate::app::gating_test_env::run_large_stack_test("ssec-completion-checksums", || async {
            let (_store, bucket) =
                crate::app::gating_test_env::durable_quota_test_bucket("ssec-completion-checksums", 16_384).await;
            let fs = crate::storage::ecfs::FS::new();
            crate::app::storage_api::s3::S3::put_bucket_versioning(
                &fs,
                build_request(
                    crate::app::storage_api::s3::dto::PutBucketVersioningInput::builder()
                        .bucket(bucket.clone())
                        .versioning_configuration(crate::app::storage_api::s3::dto::VersioningConfiguration {
                            status: Some(crate::app::storage_api::s3::dto::BucketVersioningStatus::from_static("Enabled")),
                            ..Default::default()
                        })
                        .build()
                        .expect("bucket versioning input"),
                    Method::PUT,
                ),
            )
            .await
            .expect("enable versioning for encrypted multipart listing");
            let usecase = DefaultMultipartUsecase::from_global();
            let key = "customer-encrypted";
            let customer_key = "KioqKioqKioqKioqKioqKioqKioqKioqKioqKioqKio=";
            let customer_md5 = "SaaDheEjzuynJH8eW6AEpQ==";
            let plaintext_sha256 = "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=";
            let mut create_input = CreateMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .build()
                .expect("create");
            create_input.sse_customer_algorithm = Some("AES256".to_string());
            create_input.sse_customer_key = Some(customer_key.to_string());
            create_input.sse_customer_key_md5 = Some(customer_md5.to_string());
            create_input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
            let mut create = build_request(create_input, Method::POST);
            create
                .headers
                .insert("x-amz-checksum-algorithm", HeaderValue::from_static("SHA256"));
            create
                .headers
                .insert("x-amz-checksum-type", HeaderValue::from_static("COMPOSITE"));
            let upload_id = usecase
                .execute_create_multipart_upload(create)
                .await
                .expect("SSE-C create")
                .output
                .upload_id
                .expect("upload id");
            let mut part_input = UploadPartInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload_id.clone())
                .part_number(1)
                .build()
                .expect("part");
            part_input.body = Some(StreamingBlob::from(Bytes::from_static(b"abc")));
            part_input.content_length = Some(3);
            part_input.sse_customer_algorithm = Some("AES256".to_string());
            part_input.sse_customer_key = Some(customer_key.to_string());
            part_input.sse_customer_key_md5 = Some(customer_md5.to_string());
            part_input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
            part_input.checksum_sha256 = Some(plaintext_sha256.to_string());
            let mut part = build_request(part_input, Method::PUT);
            part.headers
                .insert("x-amz-checksum-sha256", HeaderValue::from_static(plaintext_sha256));
            let part = usecase.execute_upload_part(part).await.expect("SSE-C part").output;
            let complete = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(key.to_string())
                .upload_id(upload_id)
                .multipart_upload(Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: part.e_tag,
                        checksum_sha256: Some(plaintext_sha256.to_string()),
                        ..Default::default()
                    }]),
                }))
                .build()
                .expect("complete");
            // Reject keyless completion before checksum comparison, then enforce
            // the same key boundary on a successful commit and its replay.
            for (attempt, keyed) in [false, true, false, true].into_iter().enumerate() {
                let mut input = complete.clone();
                if keyed {
                    input.sse_customer_algorithm = Some("AES256".to_string());
                    input.sse_customer_key = Some(customer_key.to_string());
                    input.sse_customer_key_md5 = Some(customer_md5.to_string());
                }
                if attempt == 2 {
                    let part = &mut input
                        .multipart_upload
                        .as_mut()
                        .expect("parts")
                        .parts
                        .as_mut()
                        .expect("part list")[0];
                    part.e_tag = Some(ETag::Strong("guessed-etag".to_string()));
                    part.checksum_sha256 = Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_string());
                }
                let result = usecase
                    .execute_complete_multipart_upload(build_request(input, Method::POST))
                    .await;
                if keyed {
                    let response = result.expect("verified key completes and replays");
                    assert!(response.output.checksum_sha256.is_some(), "verified key retains checksum response");
                    assert_eq!(
                        response
                            .headers
                            .get("x-amz-server-side-encryption-customer-key-md5")
                            .expect("key MD5"),
                        customer_md5
                    );
                    let etag = response.output.e_tag.expect("completed encrypted object ETag");
                    let version_id = response.output.version_id.expect("completed object version");
                    assert_ne!(version_id, "null");
                    assert_multipart_etag_listings(&fs, &bucket, key, &etag, &version_id).await;
                    let head = crate::app::storage_api::s3::S3::head_object(
                        &fs,
                        build_request(
                            crate::app::storage_api::s3::dto::HeadObjectInput {
                                bucket: bucket.clone(),
                                key: key.to_string(),
                                version_id: Some(version_id.clone()),
                                sse_customer_algorithm: Some("AES256".to_string()),
                                sse_customer_key: Some(customer_key.to_string()),
                                sse_customer_key_md5: Some(customer_md5.to_string()),
                                ..Default::default()
                            },
                            Method::HEAD,
                        ),
                    )
                    .await
                    .expect("authorized HEAD of completed encrypted version");
                    assert_eq!(head.output.e_tag, Some(etag));
                    assert_eq!(head.output.version_id, Some(version_id));
                } else {
                    assert_eq!(
                        result
                            .expect_err("keyless checksum completion must fail before disclosure")
                            .code(),
                        &S3ErrorCode::InvalidRequest
                    );
                }
            }
        });
    }

    async fn assert_multipart_etag_listings(
        fs: &crate::storage::ecfs::FS,
        bucket: &str,
        key: &str,
        etag: &ETag,
        version_id: &str,
    ) {
        use crate::app::storage_api::s3::dto::{ListObjectVersionsInput, ListObjectsInput, ListObjectsV2Input};
        use crate::app::storage_api::s3::xml::{Serialize, Serializer};

        fn assert_xml_etag<T: Serialize>(output: &T, etag: &ETag) {
            let mut bytes = Vec::new();
            output
                .serialize(&mut Serializer::new(&mut bytes))
                .expect("serialize listing XML");
            let xml = String::from_utf8(bytes).expect("listing XML is UTF-8");
            assert!(
                xml.contains(&format!("<ETag>\"{}\"</ETag>", etag.value())),
                "listing must include the completed ETag: {xml}"
            );
        }

        // Listings carry neither the SSE-C key nor a per-object decryption request.
        let versions = crate::app::storage_api::s3::S3::list_object_versions(
            fs,
            build_request(
                ListObjectVersionsInput {
                    bucket: bucket.to_string(),
                    prefix: Some(key.to_string()),
                    ..Default::default()
                },
                Method::GET,
            ),
        )
        .await
        .expect("keyless version listing")
        .output;
        let entries = versions.versions.as_ref().expect("completed version must be listed");
        assert_eq!(entries.len(), 1, "completion replay must preserve a single version");
        assert_eq!(entries[0].key.as_deref(), Some(key));
        assert_eq!(entries[0].version_id.as_deref(), Some(version_id));
        assert_eq!(entries[0].e_tag.as_ref(), Some(etag));
        assert_xml_etag(&versions, etag);

        let v1 = crate::app::storage_api::s3::S3::list_objects(
            fs,
            build_request(
                ListObjectsInput {
                    bucket: bucket.to_string(),
                    prefix: Some(key.to_string()),
                    ..Default::default()
                },
                Method::GET,
            ),
        )
        .await
        .expect("keyless V1 listing")
        .output;
        let v2 = crate::app::storage_api::s3::S3::list_objects_v2(
            fs,
            build_request(
                ListObjectsV2Input {
                    bucket: bucket.to_string(),
                    prefix: Some(key.to_string()),
                    ..Default::default()
                },
                Method::GET,
            ),
        )
        .await
        .expect("keyless V2 listing")
        .output;
        for contents in [&v1.contents, &v2.contents] {
            let entries = contents.as_ref().expect("completed object must be listed");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key.as_deref(), Some(key));
            assert_eq!(entries[0].e_tag.as_ref(), Some(etag));
        }
        assert_xml_etag(&v1, etag);
        assert_xml_etag(&v2, etag);
    }

    #[test]
    #[serial_test::serial]
    fn kms_multipart_completion_and_replay_preserve_etag_in_versioned_listings() {
        crate::app::gating_test_env::run_large_stack_test("kms-multipart-listing-etag", || async {
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            use rustfs_kms::types::{CreateKeyRequest, KeyUsage};

            let ambient = crate::app::gating_test_env::shared_gating_ambient().await;
            let store = ambient.object_store();
            let key_dir = tempfile::TempDir::new().expect("local KMS key directory");
            let manager = ambient.kms().handle();
            manager
                .reconfigure(rustfs_kms::KmsConfig::local(key_dir.path().to_path_buf()).with_insecure_development_defaults())
                .await
                .expect("configure local KMS");
            let kms_key = manager
                .get_encryption_service()
                .await
                .expect("local KMS encryption service")
                .create_key(CreateKeyRequest {
                    key_name: Some("multipart-listing-key".to_string()),
                    key_usage: KeyUsage::EncryptDecrypt,
                    description: None,
                    policy: None,
                    tags: HashMap::new(),
                    origin: None,
                })
                .await
                .expect("create multipart KMS key");
            let bucket = format!("kms-mpu-etag-{}", Uuid::new_v4().simple());
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        versioning_enabled: true,
                        lock_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create versioned Object Lock bucket");
            let fs = crate::storage::ecfs::FS::new();
            let key = "kms-encrypted";
            crate::app::storage_api::s3::S3::put_bucket_encryption(
                &fs,
                build_request(
                    crate::app::storage_api::s3::dto::PutBucketEncryptionInput::builder()
                        .bucket(bucket.clone())
                        .server_side_encryption_configuration(
                            crate::app::storage_api::s3::dto::ServerSideEncryptionConfiguration {
                                rules: vec![crate::app::storage_api::s3::dto::ServerSideEncryptionRule {
                                    apply_server_side_encryption_by_default: Some(
                                        crate::app::storage_api::s3::dto::ServerSideEncryptionByDefault {
                                            sse_algorithm: ServerSideEncryption::from_static(ServerSideEncryption::AWS_KMS),
                                            kms_master_key_id: Some(kms_key.key_id.clone()),
                                        },
                                    ),
                                    ..Default::default()
                                }],
                            },
                        )
                        .build()
                        .expect("bucket encryption input"),
                    Method::PUT,
                ),
            )
            .await
            .expect("configure bucket-default SSE-KMS");
            let upload_id = crate::app::storage_api::s3::S3::create_multipart_upload(
                &fs,
                build_request(
                    CreateMultipartUploadInput {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                        ..Default::default()
                    },
                    Method::POST,
                ),
            )
            .await
            .expect("create KMS multipart upload")
            .output
            .upload_id
            .expect("KMS upload id");
            let part = crate::app::storage_api::s3::S3::upload_part(
                &fs,
                build_request(
                    UploadPartInput {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                        upload_id: upload_id.clone(),
                        part_number: 1,
                        content_length: Some(3),
                        body: Some(StreamingBlob::from(Bytes::from_static(b"abc"))),
                        ..Default::default()
                    },
                    Method::PUT,
                ),
            )
            .await
            .expect("upload encrypted KMS part")
            .output;
            let complete = CompleteMultipartUploadInput {
                bucket: bucket.clone(),
                key: key.to_string(),
                upload_id,
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(vec![CompletedPart {
                        part_number: Some(1),
                        e_tag: part.e_tag,
                        ..Default::default()
                    }]),
                }),
                ..Default::default()
            };
            for _ in 0..2 {
                let response = crate::app::storage_api::s3::S3::complete_multipart_upload(
                    &fs,
                    build_request(complete.clone(), Method::POST),
                )
                .await
                .expect("complete and replay KMS multipart upload")
                .output;
                let etag = response.e_tag.expect("KMS completed ETag");
                let version_id = response.version_id.expect("KMS completed version");
                assert_ne!(version_id, "null");
                assert_multipart_etag_listings(&fs, &bucket, key, &etag, &version_id).await;
                let head = crate::app::storage_api::s3::S3::head_object(
                    &fs,
                    build_request(
                        crate::app::storage_api::s3::dto::HeadObjectInput {
                            bucket: bucket.clone(),
                            key: key.to_string(),
                            version_id: Some(version_id.clone()),
                            ..Default::default()
                        },
                        Method::HEAD,
                    ),
                )
                .await
                .expect("HEAD of completed KMS version")
                .output;
                assert_eq!(head.e_tag, Some(etag));
                assert_eq!(head.version_id, Some(version_id));
                assert_eq!(head.server_side_encryption.as_ref().map(|value| value.as_str()), Some("aws:kms"));
                assert_eq!(head.ssekms_key_id.as_deref(), Some(kms_key.key_id.as_str()));
            }
            manager.stop().await.expect("stop local KMS");
        });
    }

    #[test]
    #[serial_test::serial]
    fn conditional_multipart_upload_if_none_match_returns_precondition_failed_when_object_exists() {
        crate::app::gating_test_env::run_large_stack_test(
            "conditional-mpu-if-none-match",
            conditional_multipart_upload_if_none_match_returns_precondition_failed_when_object_exists_inner,
        );
    }

    async fn conditional_multipart_upload_if_none_match_returns_precondition_failed_when_object_exists_inner() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use crate::app::storage_api::test::contract::object::ObjectOperations as _;

        let (store, bucket) =
            crate::app::gating_test_env::durable_quota_test_bucket("conditional-mpu-if-none-match", 16_384).await;
        let object = "conditional-object";
        let usecase = DefaultMultipartUsecase::from_global();
        let initial_body = b"initial multipart body";

        let create_input = CreateMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .build()
            .expect("create multipart input should build");
        let mut initial_create_req = build_request(create_input.clone(), Method::POST);
        initial_create_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let first_upload_id = usecase
            .execute_create_multipart_upload(initial_create_req)
            .await
            .expect("initial conditional create multipart upload should succeed")
            .output
            .upload_id
            .expect("create response should contain upload id");

        let mut reader = PutObjReader::from_vec(initial_body.to_vec());
        let first_part = store
            .put_object_part(&bucket, object, &first_upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("initial multipart part should be staged");
        let first_complete_input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(first_upload_id)
            .if_none_match(Some(crate::app::storage_api::s3::dto::ETagCondition::Any))
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: first_part.etag.map(|etag| to_s3s_etag(&etag)),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("initial complete multipart input should build");
        let mut first_complete_req = build_request(first_complete_input, Method::POST);
        first_complete_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        usecase
            .execute_complete_multipart_upload(first_complete_req)
            .await
            .expect("initial conditional complete multipart upload should succeed");

        let committed_info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("committed object info should be readable");
        let committed_etag = committed_info.etag.clone().expect("committed object should have an ETag");
        assert_eq!(committed_info.size, initial_body.len() as i64);

        let mut second_create_req = build_request(create_input.clone(), Method::POST);
        second_create_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let create_err = usecase
            .execute_create_multipart_upload(second_create_req)
            .await
            .expect_err("create multipart upload with If-None-Match: * on existing key must fail");
        assert_eq!(create_err.code(), &S3ErrorCode::PreconditionFailed);

        let second_upload_id = usecase
            .execute_create_multipart_upload(build_request(create_input, Method::POST))
            .await
            .expect("unconditional create multipart upload should succeed")
            .output
            .upload_id
            .expect("second create response should contain upload id");
        let mut second_reader = PutObjReader::from_vec(b"replacement multipart body".to_vec());
        let second_part = store
            .put_object_part(&bucket, object, &second_upload_id, 1, &mut second_reader, &ObjectOptions::default())
            .await
            .expect("second multipart part should be staged");
        let completed_parts = CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: second_part.etag.map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        };

        // DTO-only If-None-Match: * exercises the early precheck in execute_complete_multipart_upload.
        let dto_only_complete_input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(second_upload_id.clone())
            .if_none_match(Some(crate::app::storage_api::s3::dto::ETagCondition::Any))
            .multipart_upload(Some(completed_parts.clone()))
            .build()
            .expect("DTO-only complete multipart input should build");
        let dto_complete_err = usecase
            .execute_complete_multipart_upload(build_request(dto_only_complete_input, Method::POST))
            .await
            .expect_err("DTO-only If-None-Match: * on existing key must fail");
        assert_eq!(dto_complete_err.code(), &S3ErrorCode::PreconditionFailed);

        for condition in [ETag::Weak(committed_etag.clone()), ETag::Strong("different-etag".to_string())] {
            let input = CompleteMultipartUploadInput::builder()
                .bucket(bucket.clone())
                .key(object.to_string())
                .upload_id(second_upload_id.clone())
                .if_match(Some(crate::app::storage_api::s3::dto::ETagCondition::ETag(condition)))
                .multipart_upload(Some(completed_parts.clone()))
                .build()
                .expect("If-Match complete input should build");
            let err = usecase
                .execute_complete_multipart_upload(build_request(input, Method::POST))
                .await
                .expect_err("weak or nonmatching If-Match must reject the overwrite");
            assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
        }

        // Write requests reject ETag-valued If-None-Match before checking object state.
        let weak_if_none_match_input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(second_upload_id.clone())
            .if_none_match(Some(crate::app::storage_api::s3::dto::ETagCondition::ETag(ETag::Weak(
                committed_etag.clone(),
            ))))
            .multipart_upload(Some(completed_parts.clone()))
            .build()
            .expect("weak If-None-Match complete multipart input should build");
        let weak_complete_err = usecase
            .execute_complete_multipart_upload(build_request(weak_if_none_match_input, Method::POST))
            .await
            .expect_err("weak If-None-Match ETag must be rejected as unsupported");
        assert_eq!(weak_complete_err.code(), &S3ErrorCode::NotImplemented);

        let second_complete_input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(second_upload_id)
            .if_none_match(Some(crate::app::storage_api::s3::dto::ETagCondition::Any))
            .multipart_upload(Some(completed_parts))
            .build()
            .expect("second complete multipart input should build");
        let mut second_complete_req = build_request(second_complete_input, Method::POST);
        second_complete_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let complete_err = usecase
            .execute_complete_multipart_upload(second_complete_req)
            .await
            .expect_err("complete multipart upload with If-None-Match: * on existing key must fail");
        assert_eq!(complete_err.code(), &S3ErrorCode::PreconditionFailed);

        let after_info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("original object must remain readable after rejected conditional completions");
        assert_eq!(after_info.etag.as_deref(), Some(committed_etag.as_str()));
        assert_eq!(after_info.size, initial_body.len() as i64);

        // Two-pool regression: when Pool 0 holds an incomplete multipart upload and Pool 1
        // holds the committed object, conditional new_multipart_upload and complete_multipart_upload
        // must still reject If-None-Match: * with PreconditionFailed.
        let (_multi_temp_dir, _pool_paths, multi_store) = crate::app::gating_test_env::isolated_multi_pool_ecstore().await;
        let multi_bucket = format!("multi-pool-cond-mpu-{}", Uuid::new_v4().simple());
        multi_store
            .make_bucket(&multi_bucket, &MakeBucketOptions::default())
            .await
            .expect("create multi-pool test bucket");
        let multi_object = "cross-pool-conditional-object";
        let staged_pool_0 = multi_store.pools[0]
            .new_multipart_upload(&multi_bucket, multi_object, &ObjectOptions::default())
            .await
            .expect("stage incomplete upload in pool 0");
        let mut pool0_part_reader = PutObjReader::from_vec(b"pool-0-staged-part".to_vec());
        let pool0_part = multi_store.pools[0]
            .put_object_part(
                &multi_bucket,
                multi_object,
                &staged_pool_0.upload_id,
                1,
                &mut pool0_part_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("write staged part in pool 0");
        let mut pool1_reader = PutObjReader::from_vec(b"pool-1-committed-object".to_vec());
        multi_store.pools[1]
            .put_object(&multi_bucket, multi_object, &mut pool1_reader, &ObjectOptions::default())
            .await
            .expect("commit object in pool 1");

        let multi_cond_opts = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let multi_create_err = multi_store
            .new_multipart_upload(&multi_bucket, multi_object, &multi_cond_opts)
            .await
            .expect_err("multi-pool new_multipart_upload with If-None-Match: * must fail when object is in pool 1");
        assert_eq!(multi_create_err, StorageError::PreconditionFailed);

        let multi_complete_err = Arc::clone(&multi_store)
            .complete_multipart_upload(
                &multi_bucket,
                multi_object,
                &staged_pool_0.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: pool0_part.etag,
                    ..Default::default()
                }],
                &multi_cond_opts,
            )
            .await
            .expect_err("multi-pool complete_multipart_upload with If-None-Match: * must fail when object is in pool 1");
        assert_eq!(multi_complete_err, StorageError::PreconditionFailed);
        metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;

        // Delete-marker regression: when the current version in a versioned bucket is a delete
        // marker, If-Match must fail with NoSuchKey while If-None-Match: * succeeds.
        let versioned_bucket = format!("versioned-cond-mpu-{}", Uuid::new_v4().simple());
        store
            .make_bucket(
                &versioned_bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create versioned bucket");
        let mut v_reader = PutObjReader::from_vec(b"v1-body".to_vec());
        store
            .put_object(
                &versioned_bucket,
                object,
                &mut v_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write initial versioned object");
        let marker = store
            .delete_object(
                &versioned_bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete versioned object to create delete marker");
        assert!(marker.delete_marker);

        let dm_create_input = CreateMultipartUploadInput::builder()
            .bucket(versioned_bucket.clone())
            .key(object.to_string())
            .build()
            .expect("delete-marker create input");
        let mut dm_create_req = build_request(dm_create_input, Method::POST);
        dm_create_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        let dm_upload_id = usecase
            .execute_create_multipart_upload(dm_create_req)
            .await
            .expect("If-None-Match: * create must succeed past a delete marker")
            .output
            .upload_id
            .expect("upload id");
        let mut dm_part_reader = PutObjReader::from_vec(b"after-delete-marker".to_vec());
        let dm_part = store
            .put_object_part(
                &versioned_bucket,
                object,
                &dm_upload_id,
                1,
                &mut dm_part_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("stage part past delete marker");
        let dm_parts = CompletedMultipartUpload {
            parts: Some(vec![CompletedPart {
                part_number: Some(1),
                e_tag: dm_part.etag.map(|etag| to_s3s_etag(&etag)),
                ..Default::default()
            }]),
        };

        let dm_if_match_input = CompleteMultipartUploadInput::builder()
            .bucket(versioned_bucket.clone())
            .key(object.to_string())
            .upload_id(dm_upload_id.clone())
            .if_match(Some(crate::app::storage_api::s3::dto::ETagCondition::Any))
            .multipart_upload(Some(dm_parts.clone()))
            .build()
            .expect("delete-marker If-Match complete input");
        let mut dm_if_match_req = build_request(dm_if_match_input, Method::POST);
        dm_if_match_req
            .headers
            .insert(http::header::IF_MATCH, HeaderValue::from_static("*"));
        let dm_if_match_err = usecase
            .execute_complete_multipart_upload(dm_if_match_req)
            .await
            .expect_err("If-Match against a current delete marker must fail with NoSuchKey");
        assert_eq!(dm_if_match_err.code(), &S3ErrorCode::NoSuchKey);

        let dm_if_none_match_input = CompleteMultipartUploadInput::builder()
            .bucket(versioned_bucket)
            .key(object.to_string())
            .upload_id(dm_upload_id)
            .if_none_match(Some(crate::app::storage_api::s3::dto::ETagCondition::Any))
            .multipart_upload(Some(dm_parts))
            .build()
            .expect("delete-marker If-None-Match complete input");
        let mut dm_if_none_match_req = build_request(dm_if_none_match_input, Method::POST);
        dm_if_none_match_req
            .headers
            .insert(http::header::IF_NONE_MATCH, HeaderValue::from_static("*"));
        usecase
            .execute_complete_multipart_upload(dm_if_none_match_req)
            .await
            .expect("If-None-Match: * completion must succeed past a delete marker");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn concurrent_completions_share_durable_bucket_quota_reservations() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("concurrent-complete-quota", 6000).await;

        let usecase = DefaultMultipartUsecase::from_global();

        let mut inputs = Vec::new();
        for object in ["first", "second"] {
            let upload = store
                .new_multipart_upload(&bucket, object, &ObjectOptions::default())
                .await
                .expect("create concurrent multipart upload");
            let mut reader = PutObjReader::from_vec(vec![0x71; 4096]);
            let part = store
                .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
                .await
                .expect("stage concurrent multipart part");
            inputs.push(
                CompleteMultipartUploadInput::builder()
                    .bucket(bucket.clone())
                    .key(object.to_string())
                    .upload_id(upload.upload_id)
                    .multipart_upload(Some(CompletedMultipartUpload {
                        parts: Some(vec![CompletedPart {
                            part_number: Some(1),
                            e_tag: part.etag.map(|etag| to_s3s_etag(&etag)),
                            ..Default::default()
                        }]),
                    }))
                    .build()
                    .expect("build concurrent completion input"),
            );
        }

        let first_usecase = usecase.clone();
        let first = first_usecase.execute_complete_multipart_upload(build_request(inputs.remove(0), Method::POST));
        let second = usecase.execute_complete_multipart_upload(build_request(inputs.remove(0), Method::POST));
        let (first, second) = tokio::join!(first, second);

        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        let denied = first.err().or_else(|| second.err()).expect("one completion must be denied");
        assert_eq!(denied.code(), &S3ErrorCode::InvalidRequest);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn multipart_completion_commits_after_quota_fleet_proof_loss_before_rename() {
        use crate::app::storage_api::test::set_disk::{MultipartCommitBarrier, MultipartCommitPause};

        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("rotated-proof-mpu-quota", 4096).await;
        let object = "object";
        let upload = store
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart upload");
        let mut reader = PutObjReader::from_vec(vec![0x78; 4096]);
        let part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("stage multipart part");
        let barrier = MultipartCommitBarrier::install(&bucket, object, MultipartCommitPause::BeforeQuotaRename);
        let complete_store = Arc::clone(&store);
        let complete_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let complete = tokio::spawn(async move {
            complete_store
                .complete_multipart_upload(
                    &complete_bucket,
                    object,
                    &upload_id,
                    vec![CompletePart {
                        part_num: 1,
                        etag: part.etag,
                        ..Default::default()
                    }],
                    &ObjectOptions::default(),
                )
                .await
        });
        barrier.wait_until_paused().await;
        let _without_proof = crate::storage::storage_api::ecstore_notification::without_cross_pool_fence_fleet_proof_for_test();
        assert!(crate::storage::storage_api::ecstore_notification::acquire_cross_pool_fence_fleet_proof().is_none());
        barrier.release();

        complete
            .await
            .expect("completion task should not panic")
            .expect("fleet proof loss must not invalidate multipart quota reservation and disk fences");
        let stored = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("completed multipart object must remain readable after fleet proof loss");
        assert_eq!(stored.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn data_movement_multipart_completion_has_zero_quota_growth() {
        let (store, bucket) = crate::app::gating_test_env::durable_quota_test_bucket("data-movement-mpu-quota", 0).await;
        let object = "object";
        let mut movement_opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        let upload = store
            .new_multipart_upload(&bucket, object, &movement_opts)
            .await
            .expect("create data-movement multipart upload");
        let mut reader = PutObjReader::from_vec(vec![0x7a; 4096]);
        let part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &movement_opts)
            .await
            .expect("stage data-movement multipart part");
        movement_opts.preserve_etag = Some("movement-etag".to_string());
        let completed = store
            .complete_multipart_upload(
                &bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: 1,
                    etag: part.etag,
                    ..Default::default()
                }],
                &movement_opts,
            )
            .await
            .expect("moving an already-accounted multipart object between pools must have zero quota growth");
        assert_eq!(completed.size, 4096);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_multipart_completion_emits_once_and_replay_does_not_emit() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("mpu-owned-event-{}", Uuid::new_v4());
        let object = "object";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");
        let upload = store
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("upload");
        let mut reader = PutObjReader::from_vec(b"part".to_vec());
        let part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut reader, &ObjectOptions::default())
            .await
            .expect("part");
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_owned())
            .upload_id(upload.upload_id)
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: part.etag.map(|etag| to_s3s_etag(&etag)),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("complete input");
        let probe = crate::app::storage_api::multipart_usecase::helper::MutationCompletionProbe::install(&bucket);
        let (entered, resume) = probe.pause_after_commit();
        let request = |input| {
            let mut request = build_request(input, Method::POST);
            request.uri = format!("/{bucket}/{object}").parse().expect("complete URI");
            request
        };
        let first = request(input.clone());
        let completion = tokio::spawn(async move {
            DefaultMultipartUsecase::from_global()
                .execute_complete_multipart_upload(first)
                .await
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), entered.wait())
            .await
            .expect("committed MPU");
        completion.abort();
        assert!(completion.await.expect_err("cancel waiter").is_cancelled());
        resume.wait().await;
        probe.wait_for(1, 1).await;
        assert_eq!(
            store
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("committed object")
                .size,
            4
        );
        DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(request(input))
            .await
            .expect("idempotent replay");
        let audits = probe.audits.lock().expect("audits");
        assert_eq!(audits.len(), 2);
        assert!(audits.iter().all(|entry| entry.api.status.as_deref() == Some("success")));
        let events = probe.events.lock().expect("events");
        assert_eq!(events.len(), 1, "replay must not emit another creation event");
        assert_eq!(events[0].event_name, EventName::ObjectCreatedCompleteMultipartUpload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rejected_empty_parts_preserve_existing_object_and_staging() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;

        let bucket = format!("empty-complete-{}", Uuid::new_v4());
        let object = "existing-object";
        let existing_payload = b"existing object must survive";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multipart regression bucket");

        let mut existing_reader = PutObjReader::from_vec(existing_payload.to_vec());
        let existing_info = store
            .put_object(&bucket, object, &mut existing_reader, &ObjectOptions::default())
            .await
            .expect("write existing object");

        let upload = store
            .new_multipart_upload(&bucket, object, &ObjectOptions::default())
            .await
            .expect("create multipart staging upload");
        let mut part_reader = PutObjReader::from_vec(b"staged part".to_vec());
        let staged_part = store
            .put_object_part(&bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("write staged multipart part");

        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(object.to_string())
            .upload_id(upload.upload_id.clone())
            .multipart_upload(Some(CompletedMultipartUpload { parts: Some(Vec::new()) }))
            .build()
            .expect("complete multipart input should build");
        let err = DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(build_request(input, Method::POST))
            .await
            .expect_err("empty parts list must be rejected");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        let current_info = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("existing object should remain readable");
        assert_eq!(current_info.size, existing_info.size);
        assert_eq!(current_info.etag, existing_info.etag);

        let staged = store
            .list_object_parts(&bucket, object, &upload.upload_id, None, 1000, &ObjectOptions::default())
            .await
            .expect("multipart staging should remain available");
        assert_eq!(staged.parts.len(), 1);
        assert_eq!(staged.parts[0].part_num, 1);
        assert_eq!(staged.parts[0].etag, staged_part.etag);

        let staged_etag = staged_part.etag.expect("staged part should have an ETag");
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket)
            .key(object.to_string())
            .upload_id(upload.upload_id)
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(to_s3s_etag(&staged_etag)),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("complete multipart input should build");
        let completed = DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(build_request(input, Method::POST))
            .await
            .expect("staged part should remain completable after empty request rejection");
        assert!(completed.output.e_tag.is_some());
        let completed_info = store
            .get_object_info(
                completed
                    .output
                    .bucket
                    .as_deref()
                    .expect("complete response should include bucket"),
                object,
                &ObjectOptions::default(),
            )
            .await
            .expect("completed object should be readable");
        assert_eq!(completed_info.size, b"staged part".len() as i64);
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
    fn validate_complete_multipart_parts_enforces_part_number_range() {
        let result: rustfs_s3_types::S3Result<()> = validate_complete_multipart_parts(&[CompletePart {
            part_num: MAX_MULTIPART_PART_NUMBER as usize,
            ..Default::default()
        }]);
        result.expect("part number 10000 must remain valid");

        for part_num in [0, MAX_MULTIPART_PART_NUMBER as usize + 1] {
            let err = validate_complete_multipart_parts(&[CompletePart {
                part_num,
                ..Default::default()
            }])
            .expect_err("out-of-range complete part number must be rejected");
            assert_eq!(err.code(), &rustfs_s3_types::S3ErrorCode::InvalidPart);
            let expected = format!("Part number {part_num} must be between 1 and {MAX_MULTIPART_PART_NUMBER}");
            assert_eq!(err.message(), Some(expected.as_str()));
        }
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

        let result: rustfs_s3_types::S3Result<Vec<CompletePart>> = normalize_complete_multipart_parts(input);
        let normalized = result.expect("normalization should succeed");
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
}
