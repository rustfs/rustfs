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
use crate::app::RequestEnvelope;
use crate::app::dto_values::stored_etag;
use crate::app::object::quota_accounting_object_size;
use crate::app::storage_api::multipart_usecase::EC_BLOCK_SIZE_RESPONSE_HEADER;
use http::HeaderValue;
use http::StatusCode;
use rustfs_gateway_types::dto::{
    ChecksumType, CompleteMultipartUploadInput, CompleteMultipartUploadOutput, CompletedPart, ServerSideEncryption,
};
use rustfs_gateway_types::{ETag, RecordedUpload, resolve_upload_async};
use rustfs_s3_client::constants::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use rustfs_utils::http::headers::{SSEC_ALGORITHM_HEADER, SSEC_KEY_MD5_HEADER};

fn validate_complete_multipart_parts(parts: &[CompletePart]) -> S3Result<()> {
    if let Some(part) = parts
        .iter()
        .find(|part| !(1..=MAX_MULTIPART_PART_NUMBER as usize).contains(&part.part_num))
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidPart,
            format!("Part number {} must be between 1 and {MAX_MULTIPART_PART_NUMBER}", part.part_num),
        ));
    }

    if parts.windows(2).any(|window| window[0].part_num >= window[1].part_num) {
        return Err(s3_error!(InvalidPartOrder, "Part numbers must be strictly increasing"));
    }

    Ok(())
}

fn normalize_complete_multipart_parts(parts: Vec<CompletePart>) -> S3Result<Vec<CompletePart>> {
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

/// A part as storage completes it: a number outside `usize` is part `0`, which the range check
/// refuses, and the entity tag is the opaque tag the request named.
fn complete_part(value: CompletedPart) -> CompletePart {
    CompletePart {
        part_num: usize::try_from(value.part_number).unwrap_or_default(),
        etag: value.e_tag.map(|etag| etag.opaque_tag().to_owned()),
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

/// Whether `If-Match`, as the header spells it, admits the current object tagged `stored`: `*`
/// always, a strong tag only when it is the stored one, a weak or unreadable tag never.
fn if_match_admits(condition: &str, stored: Option<&str>) -> bool {
    match ETag::parse_http_header(condition) {
        Ok(tag) if tag.is_any() => true,
        Ok(tag) if !tag.is_weak() => stored == Some(tag.opaque_tag()),
        _ => false,
    }
}

/// The value of the request's `x-amz-checksum-<algorithm>` header, as the legacy decoder read it
/// into its per-algorithm input member: what the answer echoes when storage records no checksum.
fn requested_checksum(headers: &HeaderMap, algorithm: rustfs_rio::ChecksumType) -> Option<String> {
    let name = algorithm.key()?;
    headers.get(name).and_then(|value| value.to_str().ok()).map(str::to_owned)
}

/// Storage-observed ownership and optional live metadata; never publication authority.
struct CompletionUpload {
    bucket: String,
    key: String,
    user_defined: Option<HashMap<String, String>>,
}

impl RecordedUpload for CompletionUpload {
    fn bucket(&self) -> &str {
        &self.bucket
    }
    fn key(&self) -> &str {
        &self.key
    }
}

impl DefaultMultipartUsecase {
    /// Completes a multipart upload using owned request facts.
    ///
    /// Callers must finish shared request authorization before invoking this method.
    /// Once the owned task starts, dropping the caller's future does not cancel it.
    /// Storage rechecks the upload under its commit locks and determines whether a retry can replay a retained completion.
    ///
    /// # Errors
    ///
    /// Returns the S3 error from request, encryption, quota, object-lock, or storage checks when completion is rejected.
    #[instrument(level = "debug", skip(self, envelope, input))]
    #[hotpath::measure(impl_type = "MultipartUsecase")]
    pub async fn execute_complete_multipart_upload(
        &self,
        envelope: RequestEnvelope,
        input: CompleteMultipartUploadInput,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let mut input = input;
        let multipart_upload = Some(std::mem::take(&mut input.multipart_upload));
        self.execute_complete_multipart_upload_parts(envelope, input, multipart_upload)
            .await
    }

    pub(crate) async fn execute_complete_multipart_upload_parts(
        &self,
        envelope: RequestEnvelope,
        input: CompleteMultipartUploadInput,
        multipart_upload: Option<rustfs_gateway_types::dto::CompletedMultipartUpload>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let owner = self.clone();
        run_owned_mutation(async move {
            Box::pin(owner.execute_complete_multipart_upload_owned(envelope, input, multipart_upload)).await
        })
        .await
    }

    async fn execute_complete_multipart_upload_owned(
        &self,
        envelope: RequestEnvelope,
        input: CompleteMultipartUploadInput,
        multipart_upload: Option<rustfs_gateway_types::dto::CompletedMultipartUpload>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        let headers = envelope.headers();
        validate_new_object_key(input.key.as_str(), "CompleteMultipartUpload")?;
        reject_presigned_multipart_max_total_object_size_for_other_operation(
            headers,
            envelope.uri().query(),
            envelope.verified_sigv4().is_some(),
        )?;
        reject_presigned_put_max_content_length_for_other_operation(
            headers,
            envelope.uri().query(),
            envelope.verified_presigned().is_some(),
        )?;
        let mut helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectCreatedCompleteMultipartUpload,
            S3Operation::CompleteMultipartUpload,
        );
        let replication_authorized = replication_request_authorized(&envelope);
        let CompleteMultipartUploadInput {
            bucket: bucket_name,
            key: object_key,
            upload_id,
            checksum_type: requested_checksum_type,
            if_match,
            if_none_match,
            sse_customer_algorithm,
            sse_customer_key,
            sse_customer_key_md5,
            ..
        } = input;
        let bucket = bucket_name.as_str().to_owned();
        let key = object_key.as_str().to_owned();
        let sse_customer_key = sse_customer_key.map(|key| key.expose_secret().to_owned());

        let customer_key_provided = sse_customer_key.is_some();
        reject_veeam_sosapi_mutation(|| self.object_store(), &bucket, &key).await?;
        // The input member and the header are one value on the wire; both are checked, as the
        // legacy guard checks both.
        let specific_if_none_match = if_none_match.as_deref().is_some_and(|condition| condition.trim() != "*");
        reject_non_wildcard_write_if_none_match(specific_if_none_match, headers)?;
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
                        if let Some(condition) = if_match.as_deref()
                            && !if_match_admits(condition, info.etag.as_deref())
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
        if multipart_upload.parts.is_empty() {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "You must specify at least one part".to_string(),
            ));
        }

        let mut opts = get_complete_multipart_upload_opts_with_replication_authorization(headers, replication_authorized)
            .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&envelope, &bucket, &mut opts)?;
        let (versioned, version_suspended) = BucketVersioningSys::write_state(&bucket, &key)
            .await
            .map_err(ApiError::from)?;
        opts.versioned = versioned;
        opts.version_suspended = version_suspended;
        let capacity_scope_token = Uuid::new_v4();
        opts.capacity_scope_token = Some(capacity_scope_token);

        let uploaded_parts_vec = multipart_upload.parts.into_iter().map(complete_part).collect::<Vec<_>>();

        let uploaded_parts = normalize_complete_multipart_parts(uploaded_parts_vec)?;

        if has_complete_multipart_object_lock_headers(headers) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "CompleteMultipartUpload does not accept object lock or governance bypass headers.".to_string(),
            ));
        }

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let lookup_store = &store;
        let lookup_opts = &opts;
        let (bucket_ref, key_ref) = (bucket.as_str(), key.as_str());
        let mut absent_upload_id = None;
        let absent = &mut absent_upload_id;
        let resolved = resolve_upload_async(&upload_id, &bucket_name, &object_key, |id| async move {
            let binding = lookup_store
                .lookup_multipart_completion_binding(bucket_ref, key_ref, id, lookup_opts)
                .await
                .map_err(multipart_upload_api_error)?;
            Ok::<_, S3Error>(match binding {
                Some(binding) => Some(CompletionUpload {
                    bucket: binding.bucket().to_owned(),
                    key: binding.object().to_owned(),
                    user_defined: binding.active_info().map(|info| info.user_defined.clone()),
                }),
                None => {
                    // Preserve quota and object-lock refusal order without claiming ownership.
                    // Final storage completion still resolves and checks the original request.
                    *absent = Some(id.to_owned());
                    None
                }
            })
        })
        .await?;
        let (upload_id, upload_user_defined) = match resolved {
            Ok((id, upload)) => (id.id().to_owned(), upload.user_defined),
            Err(_) => match absent_upload_id {
                Some(id) => (id, None),
                None => {
                    return Err(S3Error::with_message(
                        S3ErrorCode::NoSuchUpload,
                        "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
                    ));
                }
            },
        };

        let completion_principal = SseKmsPrincipal::from_request(&envelope);
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
        if let Some(user_defined) = upload_user_defined.as_ref() {
            completion_authorization
                .authorize(user_defined)
                .await
                .map_err(map_get_object_reader_error)?;
            validate_complete_multipart_checksum_type(headers, user_defined)?;
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
            let current_opts =
                internal_object_info_lookup_opts(get_opts(&bucket, &key, None, None, headers).await.map_err(ApiError::from)?);
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

        let table_data_plane_publication_guards = retained_table_data_plane_publication_guards(&envelope);

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
                    jiff::ZonedDateTime::now().to_string(),
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

                    enqueue_transition_immediate(
                        &obj_info,
                        LcEventSrc::S3CompleteMultipartUpload,
                        opts.expected_bucket_incarnation_id,
                    )
                    .await;

                    if let Some(decision) = completion_replication_decision
                        && decision.replicate_any()
                    {
                        warn!("need multipart replication");
                        schedule_object_replication(obj_info.clone(), store, decision, opts.expected_bucket_incarnation_id).await;
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
        let ssec_algorithm = encryption_metadata
            .get("x-amz-server-side-encryption-customer-algorithm")
            .cloned();
        // SSE-C metadata also records `x-amz-server-side-encryption: AES256`, but
        // that header names a managed algorithm; an SSE-C upload answers with its
        // customer-key headers only.
        let server_side_encryption = encryption_metadata
            .get("x-amz-server-side-encryption")
            .filter(|_| ssec_algorithm.is_none())
            .map(|s| ServerSideEncryption::custom(s.clone()));
        let ssekms_key_id = match server_side_encryption.as_ref() {
            Some(sse) if sse.as_str() == ServerSideEncryption::AWS_KMS.as_str() => encryption_metadata
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .cloned(),
            _ => None,
        };
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
        // checksum: stored (decrypted) values take precedence over the request's;
        // additional algorithms (XXHash3/64/128, SHA-512, MD5) are echoed as
        // response headers, not as body members (#1261).
        // Keyless completion remains supported, but plaintext checksums are
        // returned only after verifying that key (backlog#3050). The customer-key
        // MD5 is a fingerprint of the key, not of the plaintext, and the S3
        // conformance suite expects it on a keyless completion, so it is echoed
        // unconditionally. Apply this equally to a new commit and a replay.
        let disclose_checksums = (ssec_algorithm.is_none() && ssec_key_md5.is_none()) || customer_key_provided;
        let classified = if disclose_checksums {
            let (checksums, is_multipart) = obj_info
                .decrypt_checksums(opts.part_number.unwrap_or(0), headers)
                .map_err(ApiError::from)?;
            crate::app::object_usecase::classify_response_checksums(checksums, is_multipart)
        } else {
            Default::default()
        };
        let checksum_crc32 = if disclose_checksums {
            classified
                .crc32
                .or_else(|| requested_checksum(headers, rustfs_rio::ChecksumType::CRC32))
        } else {
            None
        };
        let checksum_crc32c = if disclose_checksums {
            classified
                .crc32c
                .or_else(|| requested_checksum(headers, rustfs_rio::ChecksumType::CRC32C))
        } else {
            None
        };
        let checksum_sha1 = if disclose_checksums {
            classified
                .sha1
                .or_else(|| requested_checksum(headers, rustfs_rio::ChecksumType::SHA1))
        } else {
            None
        };
        let checksum_sha256 = if disclose_checksums {
            classified
                .sha256
                .or_else(|| requested_checksum(headers, rustfs_rio::ChecksumType::SHA256))
        } else {
            None
        };
        let checksum_crc64nvme = if disclose_checksums {
            classified
                .crc64nvme
                .or_else(|| requested_checksum(headers, rustfs_rio::ChecksumType::CRC64_NVME))
        } else {
            None
        };
        let checksum_type = if disclose_checksums {
            classified.checksum_type.map(ChecksumType::custom).or(requested_checksum_type)
        } else {
            None
        };
        let complete_extra_checksum_headers = classified.extra;

        let effective_ec_block_size = obj_info.effective_ec_block_size();
        let mut answer_headers = HeaderMap::new();
        crate::app::object_usecase::inject_additional_checksum_headers(&mut answer_headers, &complete_extra_checksum_headers);
        if let Some(block_size) = effective_ec_block_size
            && let Ok(value) = HeaderValue::from_str(&block_size.to_string())
        {
            answer_headers.insert(EC_BLOCK_SIZE_RESPONSE_HEADER, value);
        }
        if let Some(algorithm) = ssec_algorithm.as_deref() {
            let value = HeaderValue::from_str(algorithm)
                .map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C algorithm metadata"))?;
            answer_headers.insert(SSEC_ALGORITHM_HEADER, value);
        }
        if let Some(key_md5) = ssec_key_md5.as_deref() {
            let value =
                HeaderValue::from_str(key_md5).map_err(|_| s3_error!(InternalError, "Invalid stored SSE-C key metadata"))?;
            answer_headers.insert(SSEC_KEY_MD5_HEADER, value);
        }
        let header = |name: &str| {
            answer_headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned)
        };
        let location = build_complete_multipart_location(headers, envelope.uri(), &bucket, &key);
        let output = CompleteMultipartUploadOutput {
            bucket: Some(bucket_name),
            key: Some(object_key),
            e_tag: obj_info.etag.as_deref().map(stored_etag),
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
            sse_customer_algorithm: header(SSEC_ALGORITHM_HEADER),
            sse_customer_key_md5: header(SSEC_KEY_MD5_HEADER),
            checksum_header_md5: header("x-amz-checksum-md5"),
            checksum_header_sha512: header("x-amz-checksum-sha512"),
            checksum_header_xxhash3: header("x-amz-checksum-xxhash3"),
            checksum_header_xxhash64: header("x-amz-checksum-xxhash64"),
            checksum_header_xxhash128: header("x-amz-checksum-xxhash128"),
            ..Default::default()
        };
        // Set object info for event notification
        helper = helper.object(obj_info);
        if let Some(version_id) = &mpu_version_for_event {
            helper = helper.version_id(version_id.clone());
        }
        if replayed {
            // The object-created event was emitted by the commit that published
            // the version. The retry is still a successful API call.
            helper = helper.suppress_event();
        }
        let result = Ok(output);
        let _ = helper.complete_owned(&result, StatusCode::OK, &answer_headers);
        result
    }
}

#[cfg(test)]
#[cfg(test)]
impl DefaultMultipartUsecase {
    /// CompleteMultipartUpload for an s3s request: its facts in an envelope and its input across
    /// the compatibility seam, as the tests of the legacy request shape reach the owned use case.
    pub(crate) async fn complete_for_s3s_request(
        &self,
        req: crate::app::storage_api::s3::S3Request<crate::app::storage_api::s3::dto::CompleteMultipartUploadInput>,
    ) -> S3Result<CompleteMultipartUploadOutput> {
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

        let envelope = RequestEnvelope::from_s3s(&req);
        let mut s3s_input = req.input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let mut input = complete_multipart_upload::input_from_s3s(s3s_input).map_err(crate::app::dto_values::seam_error)?;
        for (part, tag) in input.multipart_upload.parts.iter_mut().zip(tags) {
            part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
        }
        let parts = std::mem::take(&mut input.multipart_upload);
        let parts = has_upload.then_some(parts);
        match parts {
            Some(parts) => {
                let mut input = input;
                input.multipart_upload = parts;
                self.execute_complete_multipart_upload(envelope, input).await
            }
            None => self.execute_complete_multipart_upload_parts(envelope, input, None).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::*;
    use super::*;
    use crate::app::object::DefaultObjectUsecase;
    use crate::app::storage_api::object_usecase::StorageObjectInfo as ObjectInfo;
    use crate::app::storage_api::s3::dto::{
        ChecksumAlgorithm, CompleteMultipartUploadInput, CompletedPart, CreateMultipartUploadInput, ETag, StreamingBlob,
        UploadPartInput,
    };
    use rustfs_gateway_types::dto::HeadObjectInput;
    use rustfs_gateway_types::{BucketName, ObjectKey, SseCustomerKey};
    fn completion_with_header(name: impl http::header::IntoHeaderName, value: HeaderValue) -> RequestEnvelope {
        let mut headers = HeaderMap::new();
        headers.insert(name, value);
        RequestEnvelope::from_parts(
            crate::app::RequestHead {
                method: Method::POST,
                uri: Uri::from_static("/"),
                headers,
                credentials: None,
                region: None,
                service: None,
            },
            Extensions::new(),
        )
    }
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
        assert_eq!(err.status_code(), Some(StatusCode::INTERNAL_SERVER_ERROR));
        assert_eq!(err.message(), Some("part missing or corrupt"));
        assert!(matches!(
            err.source().and_then(|source| source.downcast_ref::<StorageError>()),
            Some(StorageError::PartMissingOrCorrupt)
        ));
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
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

        let input = CompleteMultipartUploadInput::builder()
            .bucket("bucket".to_string())
            .key("object".to_string())
            .upload_id("upload-id".to_string())
            .build()
            .unwrap();
        let mut s3s_input = input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let err = match complete_multipart_upload::input_from_s3s(s3s_input) {
            Err(error) => crate::app::dto_values::seam_error(error),
            Ok(mut owned) => {
                for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                    part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                }
                let taken = std::mem::take(&mut owned.multipart_upload);
                let parts = has_upload.then_some(taken);
                let result = match parts {
                    Some(parts) => {
                        owned.multipart_upload = parts;
                        Box::pin(make_usecase().execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                    }
                    None => {
                        Box::pin(make_usecase().execute_complete_multipart_upload_parts(
                            owned_envelope(Method::POST),
                            owned,
                            None,
                        ))
                        .await
                    }
                };
                result.unwrap_err()
            }
        };
        assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_missing_parts_list() {
        use crate::app::storage_api::s3::xml::Deserialize as _;
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

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
        let mut s3s_input = input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let err = match complete_multipart_upload::input_from_s3s(s3s_input) {
            Err(error) => crate::app::dto_values::seam_error(error),
            Ok(mut owned) => {
                for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                    part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                }
                let taken = std::mem::take(&mut owned.multipart_upload);
                let parts = has_upload.then_some(taken);
                let result = match parts {
                    Some(parts) => {
                        owned.multipart_upload = parts;
                        Box::pin(make_usecase().execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                    }
                    None => {
                        Box::pin(make_usecase().execute_complete_multipart_upload_parts(
                            owned_envelope(Method::POST),
                            owned,
                            None,
                        ))
                        .await
                    }
                };
                result.expect_err("missing parts list must be rejected")
            }
        };
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
            let input = completion_input(&bucket, object, &upload.upload_id, vec![completion_part(1, Some(&staged_etag))]);
            Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input))
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
        create_request.extensions.insert(crate::storage::req_info::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });
        let upload_id = usecase
            .execute_create_multipart_upload_from_s3s(create_request)
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
        let input = completion_input(&bucket, object, &upload_id, vec![completion_part(1, part.etag.as_deref())]);
        let mut envelope = completion_with_header(AMZ_BUCKET_REPLICATION_STATUS, HeaderValue::from_static("REPLICA"));
        envelope.install_req_info(crate::storage::req_info::ReqInfo {
            replication_request_authorized: true,
            ..Default::default()
        });
        usecase
            .execute_complete_multipart_upload(envelope, input)
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
            crate::app::bucket_usecase::DefaultBucketUsecase::from_global()
                .execute_put_bucket_versioning(
                    super::super::test_support::owned_envelope(Method::PUT),
                    &bucket,
                    Ok(rustfs_gateway_types::persistence::PersistedVersioningConfiguration {
                        status: Some("Enabled".to_owned()),
                        ..Default::default()
                    }),
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
                .execute_create_multipart_upload_from_s3s(create)
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
            let part = usecase.execute_upload_part(part).await.expect("SSE-C part");
            // Reject keyless completion before checksum comparison, then enforce
            // the same key boundary on a successful commit and its replay.
            for (attempt, keyed) in [false, true, false, true].into_iter().enumerate() {
                let mut completed_part = completion_part(1, part.e_tag.as_deref());
                completed_part.checksum_sha256 = Some(plaintext_sha256.to_owned());
                if attempt == 2 {
                    completed_part.e_tag = Some(rustfs_gateway_types::ETag::from_stored("guessed-etag".to_owned()));
                    completed_part.checksum_sha256 = Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".to_owned());
                }
                let mut input = completion_input(&bucket, key, &upload_id, vec![completed_part]);
                if keyed {
                    input.sse_customer_algorithm = Some("AES256".to_owned());
                    input.sse_customer_key = Some(SseCustomerKey::new(customer_key.to_owned()));
                    input.sse_customer_key_md5 = Some(customer_md5.to_owned());
                }
                let result = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input)).await;
                if keyed {
                    let response = result.expect("verified key completes and replays");
                    assert!(response.checksum_sha256.is_some(), "verified key retains checksum response");
                    assert_eq!(response.sse_customer_key_md5.as_deref(), Some(customer_md5));
                    {
                        use rustfs_gateway::{Limits, MetaView, OperationCodec, TargetKind, WireRequest};
                        let request = http::Request::builder()
                            .method("POST")
                            .uri("http://localhost/bucket/object")
                            .header("host", "localhost")
                            .body(bytes::Bytes::new())
                            .expect("wire request");
                        let wire = WireRequest::accept(request, &Limits::default()).expect("accepted wire request");
                        let view = MetaView::of(&wire, TargetKind::Object).expect("object view");
                        let encoded = rustfs_gateway::dto::CompleteMultipartUpload::encode(response.clone(), &view, 200)
                            .expect("completed output encodes");
                        assert_eq!(encoded.status.as_u16(), 200);
                        assert_eq!(encoded.headers.get(SSEC_ALGORITHM_HEADER).and_then(|v| v.to_str().ok()), Some("AES256"));
                        assert!(
                            encoded
                                .headers
                                .get(SSEC_KEY_MD5_HEADER)
                                .is_some_and(|v| v.as_bytes() == customer_md5.as_bytes())
                        );
                        let rustfs_gateway::ResponseBody::Complete(body) = encoded.body else {
                            panic!("completion must encode a complete XML document");
                        };
                        let xml = std::str::from_utf8(&body).expect("UTF-8 completion XML");
                        assert!(xml.contains(&format!(
                            "<ChecksumSHA256>{}</ChecksumSHA256>",
                            response.checksum_sha256.as_deref().expect("completed checksum")
                        )));
                    }
                    let etag = response.e_tag.expect("completed encrypted object ETag");
                    let version_id = response.version_id.expect("completed object version");
                    assert_ne!(version_id, "null");
                    assert_multipart_etag_listings(&bucket, key, &etag, &version_id).await;
                    let head = crate::app::test_seams::head_object_gateway(
                        &DefaultObjectUsecase::from_global(),
                        owned_envelope(Method::HEAD),
                        HeadObjectInput {
                            bucket: BucketName::new(bucket.clone()).expect("test bucket name"),
                            key: ObjectKey::new(key).expect("test object key"),
                            version_id: Some(version_id.clone()),
                            sse_customer_algorithm: Some("AES256".to_owned()),
                            sse_customer_key: Some(SseCustomerKey::new(customer_key.to_string())),
                            sse_customer_key_md5: Some(customer_md5.to_string()),
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("authorized HEAD of completed encrypted version")
                    .output;
                    let head_etag = head.etag.expect("completed version has an ETag");
                    assert_eq!(head_etag.opaque, etag.opaque_tag());
                    assert_eq!(head_etag.weak, etag.is_weak());
                    assert_eq!(head.dto.version_id, Some(version_id));
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

    async fn assert_multipart_etag_listings(bucket: &str, key: &str, etag: &rustfs_gateway_types::ETag, version_id: &str) {
        use crate::app::storage_api::s3::dto::{ListObjectVersionsInput, ListObjectsInput, ListObjectsV2Input};
        use crate::app::storage_api::s3::xml::{Serialize, Serializer};
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{list_object_versions, list_objects};

        fn assert_listing_etag(actual: Option<&ETag>, expected: &rustfs_gateway_types::ETag) {
            let actual = actual.expect("listing must include the completed ETag");
            assert_eq!(actual.value(), expected.opaque_tag());
            assert_eq!(actual.as_weak().is_some(), expected.is_weak());
        }

        fn assert_xml_etag<T: Serialize>(output: &T, etag: &rustfs_gateway_types::ETag) {
            let mut bytes = Vec::new();
            output
                .serialize(&mut Serializer::new(&mut bytes))
                .expect("serialize listing XML");
            let xml = String::from_utf8(bytes).expect("listing XML is UTF-8");
            assert!(
                xml.contains(&format!("<ETag>\"{}\"</ETag>", etag.opaque_tag())),
                "listing must include the completed ETag: {xml}"
            );
        }

        let bucket_usecase = crate::app::bucket_usecase::DefaultBucketUsecase::from_global();
        // Listings carry neither the SSE-C key nor a per-object decryption request.
        let versions_input = ListObjectVersionsInput {
            bucket: bucket.to_string(),
            prefix: Some(key.to_string()),
            ..Default::default()
        };
        let versions_env = super::super::test_support::owned_envelope(Method::GET);
        let versions_in = list_object_versions::input_from_s3s(versions_input).expect("convert ListObjectVersionsInput");
        let versions = list_object_versions::output_to_s3s(
            bucket_usecase
                .execute_list_object_versions(versions_env, versions_in)
                .await
                .expect("keyless version listing"),
        )
        .expect("convert ListObjectVersionsOutput");
        let entries = versions.versions.as_ref().expect("completed version must be listed");
        assert_eq!(entries.len(), 1, "completion replay must preserve a single version");
        assert_eq!(entries[0].key.as_deref(), Some(key));
        assert_eq!(entries[0].version_id.as_deref(), Some(version_id));
        assert_listing_etag(entries[0].e_tag.as_ref(), etag);
        assert_xml_etag(&versions, etag);

        let v1_input = ListObjectsInput {
            bucket: bucket.to_string(),
            prefix: Some(key.to_string()),
            ..Default::default()
        };
        let v1_env = super::super::test_support::owned_envelope(Method::GET);
        let v1_in = list_objects::input_from_s3s(v1_input).expect("convert ListObjectsInput");
        let v1 = list_objects::output_to_s3s(
            bucket_usecase
                .execute_list_objects(v1_env, v1_in)
                .await
                .expect("keyless V1 listing"),
        )
        .expect("convert ListObjectsOutput");
        let v2 = crate::app::test_seams::list_objects_v2_method_only(
            &bucket_usecase,
            ListObjectsV2Input {
                bucket: bucket.to_string(),
                prefix: Some(key.to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("keyless V2 listing")
        .output;
        for contents in [&v1.contents, &v2.contents] {
            let entries = contents.as_ref().expect("completed object must be listed");
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].key.as_deref(), Some(key));
            assert_listing_etag(entries[0].e_tag.as_ref(), etag);
        }
        assert_xml_etag(&v1, etag);
        assert_xml_etag(&v2, etag);
    }

    #[test]
    #[serial_test::serial]
    fn kms_multipart_completion_and_replay_preserve_etag_in_versioned_listings() {
        crate::app::gating_test_env::run_large_stack_test("kms-multipart-listing-etag", || async {
            use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
            use rustfs_gateway_types::dto as gateway_dto;
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
            let key = "kms-encrypted";
            let enc_cfg = gateway_dto::ServerSideEncryptionConfiguration {
                rules: vec![gateway_dto::ServerSideEncryptionRule {
                    apply_server_side_encryption_by_default: Some(gateway_dto::ServerSideEncryptionByDefault {
                        sse_algorithm: gateway_dto::SseAlgorithm::AWS_KMS,
                        kms_master_key_id: Some(kms_key.key_id.clone()),
                    }),
                    ..Default::default()
                }],
            };
            let xml =
                rustfs_gateway_types::persistence::serialize_bucket_encryption_dto(&enc_cfg).expect("encode bucket SSE-KMS");
            let prepared = rustfs_gateway_types::persistence::parse_bucket_encryption(&xml).expect("parse bucket SSE-KMS");
            crate::app::bucket_usecase::DefaultBucketUsecase::from_global()
                .execute_put_bucket_encryption(owned_envelope(Method::PUT), &bucket, Ok(prepared))
                .await
                .expect("configure bucket-default SSE-KMS");
            let usecase = DefaultMultipartUsecase::from_global();
            let created = usecase
                .execute_create_multipart_upload(
                    owned_envelope(Method::POST),
                    super::super::CreateMultipartUploadFields::from_legacy(CreateMultipartUploadInput {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                        ..Default::default()
                    }),
                    None,
                )
                .await
                .expect("create KMS multipart upload");
            let upload_id = created.upload_id;
            assert!(!upload_id.is_empty(), "KMS upload id");
            let input = UploadPartInput {
                bucket: bucket.clone(),
                key: key.to_string(),
                upload_id: upload_id.clone(),
                part_number: 1,
                content_length: Some(3),
                body: Some(StreamingBlob::from(Bytes::from_static(b"abc"))),
                ..Default::default()
            };
            let output = usecase
                .execute_upload_part_with_audit(
                    owned_envelope(Method::PUT),
                    super::super::UploadPartWriteInput::from_legacy(input, None),
                )
                .await
                .expect("upload encrypted KMS part");
            for _ in 0..2 {
                let complete = completion_input(&bucket, key, &upload_id, vec![completion_part(1, output.e_tag.as_deref())]);
                let response = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), complete))
                    .await
                    .expect("complete and replay KMS multipart upload");
                let etag = response.e_tag.expect("KMS completed ETag");
                let version_id = response.version_id.expect("KMS completed version");
                assert_ne!(version_id, "null");
                assert_multipart_etag_listings(&bucket, key, &etag, &version_id).await;
                let head = crate::app::test_seams::head_object_gateway(
                    &DefaultObjectUsecase::from_global(),
                    owned_envelope(Method::HEAD),
                    HeadObjectInput {
                        bucket: BucketName::new(bucket.clone()).expect("test bucket name"),
                        key: ObjectKey::new(key).expect("test object key"),
                        version_id: Some(version_id.clone()),
                        ..Default::default()
                    },
                )
                .await
                .expect("HEAD of completed KMS version")
                .output;
                let head_etag = head.etag.expect("completed version has an ETag");
                assert_eq!(head_etag.opaque, etag.opaque_tag());
                assert_eq!(head_etag.weak, etag.is_weak());
                assert_eq!(head.dto.version_id, Some(version_id));
                assert_eq!(head.dto.server_side_encryption.as_ref().map(|value| value.as_str()), Some("aws:kms"));
                assert_eq!(head.dto.ssekms_key_id.as_deref(), Some(kms_key.key_id.as_str()));
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
            .execute_create_multipart_upload_from_s3s(initial_create_req)
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
        let mut input = completion_input(&bucket, object, &first_upload_id, vec![completion_part(1, first_part.etag.as_deref())]);
        input.if_none_match = Some("*".to_owned());
        usecase
            .execute_complete_multipart_upload(
                completion_with_header(http::header::IF_NONE_MATCH, HeaderValue::from_static("*")),
                input,
            )
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
            .execute_create_multipart_upload_from_s3s(second_create_req)
            .await
            .expect_err("create multipart upload with If-None-Match: * on existing key must fail");
        assert_eq!(create_err.code(), &crate::app::storage_api::s3::S3ErrorCode::PreconditionFailed);

        let second_created = usecase
            .execute_create_multipart_upload(
                owned_envelope(Method::POST),
                super::super::CreateMultipartUploadFields::from_legacy(create_input),
                None,
            )
            .await
            .expect("unconditional create multipart upload should succeed");
        let second_upload_id = second_created.upload_id;
        assert!(!second_upload_id.is_empty(), "second create response should contain upload id");
        let mut second_reader = PutObjReader::from_vec(b"replacement multipart body".to_vec());
        let second_part = store
            .put_object_part(&bucket, object, &second_upload_id, 1, &mut second_reader, &ObjectOptions::default())
            .await
            .expect("second multipart part should be staged");
        let second_completion =
            || completion_input(&bucket, object, &second_upload_id, vec![completion_part(1, second_part.etag.as_deref())]);

        // DTO-only If-None-Match: * exercises the early precheck in execute_complete_multipart_upload.
        let mut input = second_completion();
        input.if_none_match = Some("*".to_owned());
        let rejected = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input)).await;
        let dto_complete_err = rejected.expect_err("DTO-only If-None-Match: * on existing key must fail");
        assert_eq!(dto_complete_err.code(), &S3ErrorCode::PreconditionFailed);

        for condition in [format!("W/\"{committed_etag}\""), "\"different-etag\"".to_owned()] {
            let mut input = second_completion();
            input.if_match = Some(condition);
            let rejected = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input)).await;
            let err = rejected.expect_err("weak or nonmatching If-Match must reject the overwrite");
            assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);
        }

        // Write requests reject ETag-valued If-None-Match before checking object state.
        let mut input = second_completion();
        input.if_none_match = Some(format!("W/\"{committed_etag}\""));
        let rejected = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input)).await;
        let weak_complete_err = rejected.expect_err("weak If-None-Match ETag must be rejected as unsupported");
        assert_eq!(weak_complete_err.code(), &S3ErrorCode::NotImplemented);

        let mut input = second_completion();
        input.if_none_match = Some("*".to_owned());
        let complete_err = usecase
            .execute_complete_multipart_upload(
                completion_with_header(http::header::IF_NONE_MATCH, HeaderValue::from_static("*")),
                input,
            )
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
            .execute_create_multipart_upload_from_s3s(dm_create_req)
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
        let marker_completion = || {
            completion_input(
                &versioned_bucket,
                object,
                &dm_upload_id,
                vec![completion_part(1, dm_part.etag.as_deref())],
            )
        };
        let mut input = marker_completion();
        input.if_match = Some("*".to_owned());
        let dm_if_match_err = usecase
            .execute_complete_multipart_upload(
                completion_with_header(http::header::IF_MATCH, HeaderValue::from_static("*")),
                input,
            )
            .await
            .expect_err("If-Match against a current delete marker must fail with NoSuchKey");
        assert_eq!(dm_if_match_err.code(), &S3ErrorCode::NoSuchKey);

        let mut input = marker_completion();
        input.if_none_match = Some("*".to_owned());
        usecase
            .execute_complete_multipart_upload(
                completion_with_header(http::header::IF_NONE_MATCH, HeaderValue::from_static("*")),
                input,
            )
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
            inputs.push(completion_input(
                &bucket,
                object,
                &upload.upload_id,
                vec![completion_part(1, part.etag.as_deref())],
            ));
        }

        let first = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), inputs.remove(0)));
        let second = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), inputs.remove(0)));
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
        let request = || {
            let mut envelope = owned_envelope(Method::POST);
            *envelope.uri_mut() = format!("/{bucket}/{object}").parse().expect("complete URI");
            let input = completion_input(&bucket, object, &upload.upload_id, vec![completion_part(1, part.etag.as_deref())]);
            (envelope, input)
        };
        let probe = crate::app::storage_api::multipart_usecase::helper::MutationCompletionProbe::install(&bucket);
        let (entered, resume) = probe.pause_after_commit();
        let (envelope, input) = request();
        let completion = tokio::spawn(async move {
            DefaultMultipartUsecase::from_global()
                .execute_complete_multipart_upload(envelope, input)
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
        let (envelope, input) = request();
        DefaultMultipartUsecase::from_global()
            .execute_complete_multipart_upload(envelope, input)
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
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

        let usecase = DefaultMultipartUsecase::from_global();
        let mut s3s_input = input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let rejected = match complete_multipart_upload::input_from_s3s(s3s_input) {
            Err(error) => Err(crate::app::dto_values::seam_error(error)),
            Ok(mut owned) => {
                for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                    part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                }
                let taken = std::mem::take(&mut owned.multipart_upload);
                let parts = has_upload.then_some(taken);
                match parts {
                    Some(parts) => {
                        owned.multipart_upload = parts;
                        Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                    }
                    None => {
                        Box::pin(usecase.execute_complete_multipart_upload_parts(owned_envelope(Method::POST), owned, None)).await
                    }
                }
            }
        };
        let err = rejected.expect_err("empty parts list must be rejected");
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
        let input = completion_input(&bucket, object, &upload.upload_id, vec![completion_part(1, Some(&staged_etag))]);
        let usecase = DefaultMultipartUsecase::from_global();
        let completed = Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), input)).await;
        let completed = completed.expect("staged part should remain completable after empty request rejection");
        assert!(completed.e_tag.is_some());
        let completed_info = store
            .get_object_info(
                completed
                    .bucket
                    .as_ref()
                    .expect("complete response should include bucket")
                    .as_str(),
                object,
                &ObjectOptions::default(),
            )
            .await
            .expect("completed object should be readable");
        assert_eq!(completed_info.size, b"staged part".len() as i64);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_allows_duplicate_part_numbers_by_using_last_occurrence() {
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

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
        let mut s3s_input = input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let err = match complete_multipart_upload::input_from_s3s(s3s_input) {
            Err(error) => crate::app::dto_values::seam_error(error),
            Ok(mut owned) => {
                for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                    part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                }
                let taken = std::mem::take(&mut owned.multipart_upload);
                let parts = has_upload.then_some(taken);
                let result = match parts {
                    Some(parts) => {
                        owned.multipart_upload = parts;
                        Box::pin(make_usecase().execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                    }
                    None => {
                        Box::pin(make_usecase().execute_complete_multipart_upload_parts(
                            owned_envelope(Method::POST),
                            owned,
                            None,
                        ))
                        .await
                    }
                };
                match result {
                    Err(error) => error,
                    Ok(_output) => panic!("duplicate part numbers must not succeed before the order check"),
                }
            }
        };
        assert_ne!(err.code(), &S3ErrorCode::InvalidPartOrder);
    }

    #[tokio::test]
    async fn execute_complete_multipart_upload_rejects_out_of_order_parts() {
        use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

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
        let mut s3s_input = input;
        let has_upload = s3s_input.multipart_upload.is_some();
        s3s_input.multipart_upload.get_or_insert_with(Default::default);
        let tags: Vec<Option<String>> = s3s_input
            .multipart_upload
            .iter_mut()
            .flat_map(|upload| upload.parts.iter_mut().flatten())
            .map(|part| {
                part.part_number.get_or_insert(0);
                part.e_tag.take().map(|tag| tag.value().to_owned())
            })
            .collect();
        for checksum in [
            &mut s3s_input.checksum_crc32,
            &mut s3s_input.checksum_crc32c,
            &mut s3s_input.checksum_crc64nvme,
            &mut s3s_input.checksum_md5,
            &mut s3s_input.checksum_sha1,
            &mut s3s_input.checksum_sha256,
            &mut s3s_input.checksum_sha512,
            &mut s3s_input.checksum_xxhash128,
            &mut s3s_input.checksum_xxhash3,
            &mut s3s_input.checksum_xxhash64,
        ] {
            *checksum = None;
        }
        let err = match complete_multipart_upload::input_from_s3s(s3s_input) {
            Err(error) => crate::app::dto_values::seam_error(error),
            Ok(mut owned) => {
                for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                    part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                }
                let taken = std::mem::take(&mut owned.multipart_upload);
                let parts = has_upload.then_some(taken);
                let result = match parts {
                    Some(parts) => {
                        owned.multipart_upload = parts;
                        Box::pin(make_usecase().execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                    }
                    None => {
                        Box::pin(make_usecase().execute_complete_multipart_upload_parts(
                            owned_envelope(Method::POST),
                            owned,
                            None,
                        ))
                        .await
                    }
                };
                match result {
                    Err(error) => error,
                    Ok(_output) => panic!("out-of-order parts must be rejected"),
                }
            }
        };
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
            assert_eq!(err.code(), &S3ErrorCode::InvalidPart);
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

            let err = Box::pin(make_usecase().complete_for_s3s_request(req)).await.unwrap_err();
            assert_eq!(err.code(), &S3ErrorCode::InvalidRequest, "header {header_name} should be rejected");
        }
    }

    #[test]
    #[serial_test::serial]
    fn completion_absent_binding_refusal_order_baseline() {
        crate::app::gating_test_env::run_large_stack_test(
            "completion-absent-binding-refusal-order",
            completion_absent_binding_refusal_order_baseline_inner,
        );
    }

    async fn completion_absent_binding_refusal_order_baseline_inner() {
        use crate::app::storage_api::test::bucket::quota::BucketQuota;
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        for (shape, raw_id) in [("absent", None), ("dot", Some("a/../b")), ("absolute", Some("/upload"))] {
            for (label, full_quota, locked) in [
                ("absent-quota", true, false),
                ("absent", false, false),
                ("absent-lock", false, true),
                ("absent-quota-lock", true, true),
            ] {
                let bucket = format!("{shape}-{label}-{}", Uuid::new_v4().simple());
                let key = "object";
                store
                    .make_bucket(&bucket, &MakeBucketOptions::default())
                    .await
                    .expect("create bucket");
                let upload = store
                    .new_multipart_upload(&bucket, key, &ObjectOptions::default())
                    .await
                    .expect("mint upload");
                store
                    .abort_multipart_upload(&bucket, key, &upload.upload_id, &ObjectOptions::default())
                    .await
                    .expect("abort upload");
                assert!(
                    store
                        .lookup_multipart_completion_binding(&bucket, key, &upload.upload_id, &ObjectOptions::default())
                        .await
                        .expect("observe binding")
                        .is_none()
                );
                if locked {
                    metadata_sys::update(
                        &bucket,
                        crate::app::storage_api::test::bucket::metadata::OBJECT_LOCK_CONFIG,
                        b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled></ObjectLockConfiguration>"
                            .to_vec(),
                    )
                    .await
                    .expect("configure object lock");
                    let mut opts = ObjectOptions::default();
                    opts.user_defined
                        .insert(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER.to_owned(), "ON".to_owned());
                    let mut reader = PutObjReader::from_vec(b"existing".to_vec());
                    store
                        .clone()
                        .put_object(&bucket, key, &mut reader, &opts)
                        .await
                        .expect("write held object");
                }
                crate::app::storage_api::test::data_usage::seed_bucket_usage_memory_for_test(&bucket, 2).await;
                if full_quota {
                    let metadata = DefaultMultipartUsecase::from_global()
                        .bucket_metadata_sys()
                        .expect("metadata context");
                    QuotaChecker::new(metadata)
                        .set_quota_config(
                            &bucket,
                            BucketQuota {
                                quota: Some(1),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("set legacy quota");
                }
                let probe = crate::app::storage_api::multipart_usecase::helper::MutationCompletionProbe::install(&bucket);
                let request_id = raw_id.unwrap_or(&upload.upload_id).to_owned();
                let input = CompleteMultipartUploadInput::builder()
                    .bucket(bucket)
                    .key(key.to_owned())
                    .upload_id(request_id)
                    .multipart_upload(Some(CompletedMultipartUpload {
                        parts: Some(vec![CompletedPart {
                            part_number: Some(1),
                            e_tag: Some(ETag::Strong("part".to_owned())),
                            ..Default::default()
                        }]),
                    }))
                    .build()
                    .expect("complete input");
                use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::complete_multipart_upload;

                let usecase = DefaultMultipartUsecase::from_global();
                let mut s3s_input = input;
                let has_upload = s3s_input.multipart_upload.is_some();
                s3s_input.multipart_upload.get_or_insert_with(Default::default);
                let tags: Vec<Option<String>> = s3s_input
                    .multipart_upload
                    .iter_mut()
                    .flat_map(|upload| upload.parts.iter_mut().flatten())
                    .map(|part| {
                        part.part_number.get_or_insert(0);
                        part.e_tag.take().map(|tag| tag.value().to_owned())
                    })
                    .collect();
                for checksum in [
                    &mut s3s_input.checksum_crc32,
                    &mut s3s_input.checksum_crc32c,
                    &mut s3s_input.checksum_crc64nvme,
                    &mut s3s_input.checksum_md5,
                    &mut s3s_input.checksum_sha1,
                    &mut s3s_input.checksum_sha256,
                    &mut s3s_input.checksum_sha512,
                    &mut s3s_input.checksum_xxhash128,
                    &mut s3s_input.checksum_xxhash3,
                    &mut s3s_input.checksum_xxhash64,
                ] {
                    *checksum = None;
                }
                let rejected = match complete_multipart_upload::input_from_s3s(s3s_input) {
                    Err(error) => Err(crate::app::dto_values::seam_error(error)),
                    Ok(mut owned) => {
                        for (part, tag) in owned.multipart_upload.parts.iter_mut().zip(tags) {
                            part.e_tag = tag.map(rustfs_gateway_types::ETag::from_stored);
                        }
                        let taken = std::mem::take(&mut owned.multipart_upload);
                        let parts = has_upload.then_some(taken);
                        match parts {
                            Some(parts) => {
                                owned.multipart_upload = parts;
                                Box::pin(usecase.execute_complete_multipart_upload(owned_envelope(Method::POST), owned)).await
                            }
                            None => {
                                Box::pin(usecase.execute_complete_multipart_upload_parts(
                                    owned_envelope(Method::POST),
                                    owned,
                                    None,
                                ))
                                .await
                            }
                        }
                    }
                };
                let error = rejected.expect_err("absent upload cannot complete");
                assert_eq!(probe.audits.lock().expect("audits").len(), 0, "{shape}/{label}");
                assert_eq!(probe.events.lock().expect("events").len(), 0, "{shape}/{label}");
                let (code, status, message, storage_cause) = if raw_id.is_some() {
                    (
                        S3ErrorCode::NoSuchUpload,
                        404,
                        "The specified multipart upload does not exist. The upload ID might be invalid, or the multipart upload might have been aborted or completed.",
                        false,
                    )
                } else if full_quota {
                    (
                        S3ErrorCode::InvalidRequest,
                        400,
                        "Bucket quota exceeded. Current usage: 2 bytes, limit: 1 bytes",
                        false,
                    )
                } else if locked {
                    (
                        S3ErrorCode::AccessDenied,
                        403,
                        "Object has a legal hold and cannot be overwritten. Remove the legal hold first.",
                        false,
                    )
                } else {
                    (
                        S3ErrorCode::NoSuchUpload,
                        404,
                        "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
                        true,
                    )
                };
                assert_eq!(error.source().is_some(), storage_cause, "{shape}/{label}");
                assert_eq!(error.code(), &code, "{shape}/{label}");
                assert_eq!(error.status_code().map(|status| status.as_u16()), Some(status), "{label}");
                assert_eq!(error.message(), Some(message), "{label}");
                assert_eq!(
                    error
                        .source()
                        .is_some_and(|cause| cause.downcast_ref::<StorageError>().is_some()),
                    storage_cause,
                    "{label}"
                );
            }
        }
    }
    #[tokio::test]
    #[serial_test::serial]
    async fn live_completion_checks_recorded_checksum_type_before_parts() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("complete-type-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");
        let mut opts = ObjectOptions::default();
        opts.user_defined
            .insert(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_owned(), "COMPOSITE".to_owned());
        let upload = store
            .new_multipart_upload(&bucket, "object", &opts)
            .await
            .expect("active upload");
        let binding = store
            .lookup_multipart_completion_binding(&bucket, "object", &upload.upload_id, &opts)
            .await
            .expect("binding lookup")
            .expect("active binding");
        assert!(binding.active_info().is_some());
        let input = CompleteMultipartUploadInput::builder()
            .bucket(bucket)
            .key("object".to_owned())
            .upload_id(upload.upload_id)
            .multipart_upload(Some(CompletedMultipartUpload {
                parts: Some(vec![CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong("not-uploaded".to_owned())),
                    ..Default::default()
                }]),
            }))
            .build()
            .expect("request");
        let mut request = build_request(input, Method::POST);
        request
            .headers
            .insert(AMZ_CHECKSUM_TYPE, HeaderValue::from_static("FULL_OBJECT"));
        let error = DefaultMultipartUsecase::from_global()
            .complete_for_s3s_request(request)
            .await
            .expect_err("recorded checksum type is checked first");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert_eq!(
            error.message(),
            Some(
                "The upload was created with checksum type COMPOSITE. The complete request must use the same checksum type, got FULL_OBJECT."
            )
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn complete_multipart_upload_maps_checksum_mismatch_to_bad_digest_issue_9272() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};
        use crate::app::storage_api::test::contract::object::ObjectOperations as _;

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        let bucket = format!("complete-checksum-9272-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");

        let usecase = DefaultMultipartUsecase::from_global();
        let key = "object-9272";
        let mut create_input = CreateMultipartUploadInput::builder()
            .bucket(bucket.clone())
            .key(key.to_owned())
            .build()
            .expect("create");
        create_input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
        let mut create = build_request(create_input, Method::POST);
        create
            .headers
            .insert("x-amz-checksum-algorithm", HeaderValue::from_static("SHA256"));
        let upload_id = usecase
            .execute_create_multipart_upload_from_s3s(create)
            .await
            .expect("create sha256 multipart upload")
            .output
            .upload_id
            .expect("upload id");

        let part1_bytes = vec![b'a'; 5 * 1024 * 1024];
        let part2_bytes = b"part-two".to_vec();
        let whole_bytes: Vec<u8> = part1_bytes.iter().chain(part2_bytes.iter()).copied().collect();
        let whole_sha256 = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &whole_bytes)
            .expect("whole sha256")
            .encoded;

        let mut combined_part_digests = Vec::new();
        let mut completed_parts = Vec::new();
        for (part_number, bytes) in [(1_i32, part1_bytes), (2_i32, part2_bytes)] {
            let part_checksum =
                rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &bytes).expect("part sha256");
            combined_part_digests.extend_from_slice(&part_checksum.raw);

            let len = i64::try_from(bytes.len()).expect("part length fits i64");
            let mut part_input = UploadPartInput::builder()
                .bucket(bucket.clone())
                .key(key.to_owned())
                .upload_id(upload_id.clone())
                .part_number(part_number)
                .build()
                .expect("part");
            part_input.body = Some(StreamingBlob::from(Bytes::from(bytes)));
            part_input.content_length = Some(len);
            part_input.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
            part_input.checksum_sha256 = Some(part_checksum.encoded.clone());

            let mut part_req = build_request(part_input, Method::PUT);
            part_req.headers.insert(
                "x-amz-checksum-sha256",
                HeaderValue::from_str(&part_checksum.encoded).expect("header value"),
            );
            let uploaded = usecase.execute_upload_part(part_req).await.expect("upload part");
            let mut part = completion_part(part_number, uploaded.e_tag.as_deref());
            part.checksum_sha256 = Some(part_checksum.encoded);
            completed_parts.push(part);
        }

        let composite_base = rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::SHA256, &combined_part_digests)
            .expect("composite sha256");
        let composite_sha256 = format!("{}-2", composite_base.encoded);

        let input = || completion_input(&bucket, key, &upload_id, completed_parts.clone());
        let mismatch = completion_with_header(
            "x-amz-checksum-sha256",
            HeaderValue::from_str(&whole_sha256).expect("whole sha256 header"),
        );
        let error = usecase
            .execute_complete_multipart_upload(mismatch, input())
            .await
            .expect_err("mismatched completion checksum must fail");
        assert_eq!(error.code(), &S3ErrorCode::BadDigest);
        assert_eq!(error.status_code(), Some(StatusCode::BAD_REQUEST));
        assert_eq!(error.message(), Some("The Content-Md5 you specified did not match what we received."));
        let info_err = store
            .get_object_info(&bucket, key, &ObjectOptions::default())
            .await
            .expect_err("object must not exist after rejected completion");
        assert!(is_err_object_not_found(&info_err), "unexpected lookup error: {info_err:?}");

        let retry = completion_with_header(
            "x-amz-checksum-sha256",
            HeaderValue::from_str(&composite_sha256).expect("composite sha256 header"),
        );
        let output = usecase
            .execute_complete_multipart_upload(retry, input())
            .await
            .expect("matching composite checksum should complete");
        assert_eq!(output.checksum_sha256.as_deref(), Some(composite_sha256.as_str()));
    }
}
