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

//! HeadObject path.

use super::*;
use crate::on_demand_migration::{
    BucketOdmState, HeadPolicy, OdmLookup, OdmOp, OdmOutcome, OnDemandMigrationSys, SourceClient, SourceError, SourceHead,
};

/// Source HEAD seam for the on-demand migration passthrough: production goes
/// through [`SourceClient`], tests script the answers.
pub(super) trait OdmHeadSource {
    async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError>;
}

impl OdmHeadSource for SourceClient {
    async fn head_object(&self, key: &str) -> Result<SourceHead, SourceError> {
        SourceClient::head_object(self, key).await
    }
}

/// What the on-demand migration runtime decided for a HEAD miss before any
/// source traffic.
pub(super) enum OdmHeadVerdict {
    /// Fall through to the local 404.
    Ignore,
    /// Answer with this error without touching the source.
    Fail(S3Error),
    /// Consult the source through `client`.
    Consult {
        state: Arc<BucketOdmState>,
        client: Arc<SourceClient>,
    },
}

/// Applies the bucket policy and the lookup verdict of
/// [`OnDemandMigrationSys::resolve`] to a HEAD miss, recording the outcome
/// for every request that stops here.
pub(super) fn odm_head_verdict(lookup: OdmLookup, miss: OdmLocalMiss) -> OdmHeadVerdict {
    let state = Arc::clone(lookup.state());
    let policy = &state.config().policy;
    if !odm_policy_admits_miss(policy, miss) {
        return OdmHeadVerdict::Ignore;
    }
    let stats = state.stats();
    let unavailable = |error| {
        stats.record_request(OdmOp::Head, OdmOutcome::SourceError);
        OdmHeadVerdict::Fail(odm_source_error_response(policy, odm_state_error_class(error)))
    };
    match &lookup {
        OdmLookup::NegativeCached { .. } => {
            stats.record_request(OdmOp::Head, OdmOutcome::NegativeCached);
            OdmHeadVerdict::Fail(S3Error::new(S3ErrorCode::NoSuchKey))
        }
        OdmLookup::BreakerOpen { .. } => {
            stats.record_request(OdmOp::Head, OdmOutcome::BreakerOpen);
            OdmHeadVerdict::Fail(S3Error::new(S3ErrorCode::NoSuchKey))
        }
        OdmLookup::Unavailable { error, .. } => unavailable(error),
        OdmLookup::Ready { .. } => {
            if policy.head == HeadPolicy::LocalOnly {
                stats.record_request(OdmOp::Head, OdmOutcome::Filtered);
                return OdmHeadVerdict::Fail(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            match state.client() {
                Ok(client) => OdmHeadVerdict::Consult {
                    client: Arc::clone(client),
                    state: Arc::clone(&state),
                },
                Err(error) => unavailable(error),
            }
        }
    }
}

/// Maps the source's HEAD onto the s3s output. Only what the source can
/// vouch for is returned: the ETag and Last-Modified are the source's (the
/// object is not local yet), no version id, no SSE headers and no storage
/// class are reported.
pub(super) fn odm_head_output(head: SourceHead) -> S3Result<HeadObjectOutput> {
    let content_length = i64::try_from(head.size)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "source object size exceeds the content-length range"))?;
    Ok(HeadObjectOutput {
        content_length: Some(content_length),
        content_type: head.content_type.as_deref().and_then(|v| ContentType::from_str(v).ok()),
        content_encoding: head.content_encoding,
        content_disposition: head.content_disposition,
        content_language: head.content_language,
        cache_control: head.cache_control,
        expires: head.expires,
        accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
        e_tag: head.etag.as_deref().map(to_s3s_etag),
        last_modified: head.last_modified.map(OffsetDateTime::from).map(Timestamp::from),
        metadata: (!head.user_metadata.is_empty()).then_some(head.user_metadata),
        ..Default::default()
    })
}

/// One source HEAD: latency, breaker and negative cache go through
/// `observe_source`; the outcome counter and the client-facing error follow
/// the bucket's `source_error` policy. Nothing is written locally.
pub(super) async fn odm_head_from_source<S: OdmHeadSource>(
    state: &BucketOdmState,
    source: &S,
    key: &str,
) -> S3Result<HeadObjectOutput> {
    let started = Instant::now();
    let result = source.head_object(key).await;
    state.observe_source(started.elapsed(), key, result.as_ref().err());
    let stats = state.stats();
    match result {
        Ok(head) => {
            stats.record_request(OdmOp::Head, OdmOutcome::SourceHit);
            odm_head_output(head)
        }
        Err(SourceError::NotFound) => {
            stats.record_request(OdmOp::Head, OdmOutcome::SourceMiss);
            Err(S3Error::new(S3ErrorCode::NoSuchKey))
        }
        Err(err @ SourceError::Unsupported(_)) => {
            stats.record_request(OdmOp::Head, OdmOutcome::Unsupported);
            Err(odm_source_unavailable_error(err.class_label()))
        }
        Err(err) => {
            stats.record_request(OdmOp::Head, OdmOutcome::SourceError);
            Err(odm_source_error_response(&state.config().policy, err.class_label()))
        }
    }
}

impl DefaultObjectUsecase {
    /// On-demand migration HEAD passthrough (rustfs/backlog#2155): consulted
    /// only after the local lookup and the replication proxy both missed.
    /// `None` means the runtime does not intervene and the caller keeps its
    /// original 404. The source answer is never written back or queued.
    async fn on_demand_migration_head(
        req: &S3Request<HeadObjectInput>,
        store: &ECStore,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
        miss: OdmLocalMiss,
    ) -> Option<S3Result<HeadObjectOutput>> {
        if !odm_request_may_consult_source(opts) {
            return None;
        }
        let sys = OnDemandMigrationSys::get();
        if !sys.is_module_enabled() || sys.state(bucket).is_none() {
            return None;
        }
        let expected_incarnation = match odm_read_generation(req, bucket) {
            Ok(Some(incarnation)) => incarnation,
            Ok(None) => return None,
            Err(err) => return Some(Err(err)),
        };
        match store.bucket_incarnation_id(bucket).await {
            Ok(current) if current == expected_incarnation => {}
            Ok(_) => return None,
            Err(err) => return Some(Err(ApiError::from(err).into())),
        }
        let lookup = OnDemandMigrationSys::get().resolve_for_incarnation(bucket, key, expected_incarnation)?;
        match odm_head_verdict(lookup, miss) {
            OdmHeadVerdict::Ignore => None,
            OdmHeadVerdict::Fail(err) => Some(Err(err)),
            OdmHeadVerdict::Consult { state, client } => Some(odm_head_from_source(&state, client.as_ref(), key).await),
        }
    }

    async fn finish_on_demand_migration_head(
        req: &S3Request<HeadObjectInput>,
        bucket: &str,
        helper: OperationHelper,
        output: HeadObjectOutput,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let mut response = wrap_response_with_cors(bucket, &req.method, &req.headers, output).await;
        mark_on_demand_migration_response(&mut response.headers);
        let result = Ok(response);
        let _ = helper.complete(&result);
        result
    }

    /// Serve a HEAD whose local lookup failed with not-found by proxying to
    /// the bucket's replication targets (MinIO `proxyHeadToRepTarget`).
    async fn proxy_head_object_to_replication_targets(
        req: &S3Request<HeadObjectInput>,
        bucket: &str,
        key: &str,
        opts: &ObjectOptions,
    ) -> Option<HeadObjectOutput> {
        let targets = get_read_proxy_targets(bucket, key, opts).await;
        if targets.is_empty() {
            return None;
        }
        let extra_headers = Self::proxy_read_passthrough_headers(&req.headers);
        let range = req
            .headers
            .get(http::header::RANGE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let part_number = req.input.part_number;

        for target in targets {
            match target
                .head_object_for_proxy(
                    &target.bucket,
                    key,
                    opts.version_id.clone(),
                    range.clone(),
                    part_number,
                    extra_headers.clone(),
                )
                .await
            {
                Ok(remote) => {
                    // MinIO-aligned accounting: one total per proxy attempt,
                    // one failed when no target served it.
                    record_replication_proxy(bucket, "HeadObject", false).await;
                    return Some(Self::proxy_sdk_head_output_to_s3s(remote));
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, key, arn = %target.arn, "read proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, key, arn = %target.arn, error = %err, "read proxy: HEAD against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "HeadObject", true).await;
        None
    }

    /// Translate a proxied SDK HEAD response into the s3s output.
    ///
    /// Known gaps: the SDK's HeadObjectOutput does not model 206/Content-Range
    /// for a ranged HEAD (the SDK exposes no content_range member on HEAD),
    /// and s3s' typed HeadObjectOutput has no tag_count field (the local path
    /// injects x-amz-tagging-count as a raw header) — both are dropped for
    /// proxied HEADs.
    fn proxy_sdk_head_output_to_s3s(remote: aws_sdk_s3::operation::head_object::HeadObjectOutput) -> HeadObjectOutput {
        HeadObjectOutput {
            content_length: remote.content_length,
            content_type: remote.content_type.as_deref().and_then(|v| ContentType::from_str(v).ok()),
            content_encoding: remote.content_encoding,
            content_disposition: remote.content_disposition,
            content_language: remote.content_language,
            cache_control: remote.cache_control,
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
            e_tag: remote.e_tag.as_deref().and_then(|v| ETag::from_str(v).ok()),
            last_modified: remote
                .last_modified
                .and_then(|dt| OffsetDateTime::from_unix_timestamp_nanos(dt.as_nanos()).ok())
                .map(Timestamp::from),
            metadata: remote.metadata,
            version_id: remote.version_id,
            server_side_encryption: remote
                .server_side_encryption
                .map(|sse| ServerSideEncryption::from(sse.as_str().to_string())),
            sse_customer_algorithm: remote.sse_customer_algorithm,
            sse_customer_key_md5: remote.sse_customer_key_md5,
            ssekms_key_id: remote.ssekms_key_id,
            parts_count: remote.parts_count,
            storage_class: remote.storage_class.map(|sc| StorageClass::from(sc.as_str().to_string())),
            expiration: remote.expiration,
            restore: remote.restore,
            checksum_crc32: remote.checksum_crc32,
            checksum_crc32c: remote.checksum_crc32_c,
            checksum_crc64nvme: remote.checksum_crc64_nvme,
            checksum_sha1: remote.checksum_sha1,
            checksum_sha256: remote.checksum_sha256,
            checksum_type: remote.checksum_type.map(|ct| ChecksumType::from(ct.as_str().to_string())),
            ..Default::default()
        }
    }

    #[instrument(level = "debug", skip(self, req))]
    pub async fn execute_head_object(&self, mut req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::new(&req, EventName::ObjectAccessedHead, S3Operation::HeadObject).suppress_event();
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

        let rs = range.map(range_to_http_range_spec).transpose()?;

        if rs.is_some() && part_number.is_some() {
            return Err(s3_error!(InvalidArgument, "range and part_number invalid"));
        }

        // Establish bucket existence before any bucket-metadata work (matches
        // PUT/GET): nonexistent buckets fail here instead of paying the
        // versioning lookup in get_opts first. Resolve the store through the
        // request-bound server context (backlog#1052 S6), not the
        // process-global handle.
        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        validate_bucket_exists(&store, &bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, part_number, &req.headers)
            .await
            .map_err(ApiError::from)?;

        prepare_odm_read_generation(&store, &mut req, &bucket).await;

        // Modification Points: Explicitly handles get_object_info errors, distinguishing between object absence and other errors
        let lookup = store.get_object_info(&bucket, &key, &opts).await;
        // Single classification point for the on-demand migration gate
        // (rustfs/backlog#2155): a not-found error or a latest delete marker.
        let odm_miss = odm_local_miss(lookup.as_ref());
        let info = match lookup {
            Ok(info) => info,
            Err(err) => {
                // If the error indicates the object or its version was not found, return 404 (NoSuchKey)
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!(bucket, key, error = %e, "Failed to probe children for prefix");
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    // Active-active replication lag window: an object missing
                    // locally may still be served by proxying the HEAD to a
                    // replication target (backlog#1675 P1-5).
                    if let Some(output) = Self::proxy_head_object_to_replication_targets(&req, &bucket, &key, &opts).await {
                        let response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;
                        let result = Ok(response);
                        let _ = helper
                            .version_id(req.input.version_id.clone().unwrap_or_default())
                            .complete(&result);
                        return result;
                    }
                    if let Some(miss) = odm_miss
                        && let Some(result) = Self::on_demand_migration_head(&req, &store, &bucket, &key, &opts, miss).await
                    {
                        return Self::finish_on_demand_migration_head(&req, &bucket, helper, result?).await;
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                // Other errors, such as insufficient permissions, still return the original error
                return Err(ApiError::from(err).into());
            }
        };
        if info.delete_marker {
            if opts.version_id.is_none() {
                // A latest delete marker is a local miss the source may still
                // answer when the bucket policy says so.
                if let Some(miss) = odm_miss
                    && let Some(result) = Self::on_demand_migration_head(&req, &store, &bucket, &key, &opts, miss).await
                {
                    return Self::finish_on_demand_migration_head(&req, &bucket, helper, result?).await;
                }
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
        // An authorized replication convergence check only needs etag/size/mtime
        // to compare source and replica; it holds no customer key, so the SSE-C
        // read validation is skipped for it (and only it).
        let replication_check = replication_request_authorized(&req)
            && get_header(&req.headers, SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true");
        if !replication_check {
            validate_sse_headers_for_read(&info.user_defined, &req.headers)?;

            // Validate SSE-C: if the object was encrypted with a customer-provided key,
            // the caller must supply the matching key even for HEAD requests (per S3 spec).
            validate_ssec_for_read(
                &info.user_defined,
                req.input.sse_customer_key.as_ref(),
                req.input.sse_customer_key_md5.as_ref(),
            )?;
        }

        // Compute x-amz-expiration header from lifecycle prediction (before info is partially moved)
        let expiration_header = resolve_put_object_expiration(&bucket, &info).await;
        // Clone ObjectInfo for event notification only when an event will
        // actually be built — the clone is expensive for multipart objects.
        let event_info = helper.wants_object_info().then(|| info.clone());
        let content_type = {
            if let Some(content_type) = &info.content_type {
                match ContentType::from_str(content_type) {
                    Ok(res) => Some(res),
                    Err(err) => {
                        error!(content_type = %content_type, error = ?err, "Archive content-type parse failed");
                        //
                        None
                    }
                }
            } else {
                None
            }
        };
        let last_modified = info.mod_time.map(Timestamp::from);

        let content_length = info.get_actual_size().map_err(|e| {
            error!(error = %e, "Failed to resolve actual object size");
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
        let storage_class = response_storage_class(&info, &metadata_map);
        // checksum: classify once; additional algorithms (XXHash3/64/128, SHA-512, MD5)
        // land in `extra` and are emitted as raw headers below (s3s has no typed field).
        let ResponseChecksums {
            crc32: checksum_crc32,
            crc32c: checksum_crc32c,
            sha1: checksum_sha1,
            sha256: checksum_sha256,
            crc64nvme: checksum_crc64nvme,
            checksum_type,
            extra: extra_checksum_headers,
        } = if let Some(checksum_mode) = req.headers.get(AMZ_CHECKSUM_MODE)
            && checksum_mode.to_str().unwrap_or_default() == "ENABLED"
            && rs.is_none()
        {
            let (checksums, is_multipart) = info
                .decrypt_checksums(opts.part_number.unwrap_or(0), &req.headers)
                .map_err(ApiError::from)?;
            classify_response_checksums(checksums, is_multipart)
        } else {
            ResponseChecksums::default()
        };
        // Extract standard HTTP headers from user_defined metadata
        // Note: These headers are stored with lowercase keys by extract_metadata_from_mime
        let cache_control = metadata_map.get("cache-control").cloned();
        let content_disposition = metadata_map.get("content-disposition").cloned();
        let content_language = metadata_map.get("content-language").cloned();
        let website_redirect_location = metadata_map.get(AMZ_WEBSITE_REDIRECT_LOCATION).cloned();
        let expires = info
            .expires
            .map(Timestamp::from)
            .map(|expires| format_expires_header(&expires))
            .transpose()?;

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
            accept_ranges: Some(ACCEPT_RANGES_BYTES.to_string()),
            website_redirect_location,
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
            // x-amz-restore from object metadata
            restore: metadata_map.get(X_AMZ_RESTORE.as_str()).and_then(|v| {
                let rs = parse_restore_obj_status(v).ok()?;
                Some(rs.to_string2())
            }),
            // x-amz-expiration from lifecycle prediction
            expiration: expiration_header,
            // metadata: object_metadata,
            ..Default::default()
        };

        let version_id = req.input.version_id.clone().unwrap_or_default();
        if let Some(event_info) = event_info {
            helper = helper.object(event_info);
        }
        helper = helper.version_id(version_id);

        // NOTE ON CORS:
        // Bucket-level CORS headers are intentionally applied only for object retrieval
        // operations (GET/HEAD) via `wrap_response_with_cors`. Other S3 operations that
        // interact with objects (PUT/POST/DELETE/LIST, etc.) rely on the system-level
        // CORS layer instead. In case both are applicable, this bucket-level CORS logic
        // takes precedence for these read operations.
        let mut response = wrap_response_with_cors(&bucket, &req.method, &req.headers, output).await;

        // Emit additional-checksum headers (XXHash3/64/128, SHA-512) that s3s cannot
        // carry on the typed HeadObjectOutput (#1257).
        inject_additional_checksum_headers(&mut response.headers, &extra_checksum_headers);

        // Add x-amz-tagging-count header if object has tags
        // Per S3 API spec, this header should be present in HEAD object response when tags exist
        if tag_count > 0 {
            let header_name = http::HeaderName::from_static(AMZ_TAG_COUNT);
            if let Ok(header_value) = tag_count.to_string().parse::<HeaderValue>() {
                response.headers.insert(header_name, header_value);
            } else {
                warn!("Failed to parse x-amz-tagging-count header; skipping");
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
        if info.replication_status != ReplicationStatusType::Empty
            && let Ok(header_name) = http::HeaderName::from_bytes(AMZ_BUCKET_REPLICATION_STATUS.to_ascii_lowercase().as_bytes())
            && let Ok(header_value) = HeaderValue::from_str(info.replication_status.as_str())
        {
            response.headers.insert(header_name, header_value);
        }

        let result = Ok(response);
        let _ = helper.complete(&result);

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::on_demand_migration::{
        BREAKER_FAILURE_THRESHOLD, BreakerState, FilterConfig, OdmStateError, OnDemandMigrationConfig, PathStyle, PolicyConfig,
        Provider, SourceConfig, SourceCredentials, SourceErrorPolicy, TlsConfig,
    };
    use http::Method;
    use std::collections::VecDeque;
    use std::time::SystemTime;

    /// A configured, enabled bucket source pointing at an unreachable
    /// endpoint; the client is built (no network) and only the scripted
    /// source below is ever called.
    fn odm_config(policy: PolicyConfig) -> OnDemandMigrationConfig {
        OnDemandMigrationConfig {
            version: 1,
            enabled: true,
            source: SourceConfig {
                provider: Provider::Minio,
                endpoint: Some("https://source.example.invalid:9000".to_string()),
                region: "auto".to_string(),
                bucket: "legacy".to_string(),
                path_style: PathStyle::Auto,
                credentials: Some(SourceCredentials {
                    access_key: "AK".to_string(),
                    secret_key: "SK".to_string(),
                    session_token: None,
                }),
                tls: TlsConfig::default(),
                azure: None,
                gcs: None,
            },
            filter: FilterConfig {
                prefix: None,
                source_prefix: None,
            },
            policy,
        }
    }

    async fn odm_sys(bucket: &str, policy: PolicyConfig) -> OnDemandMigrationSys {
        let sys = OnDemandMigrationSys::new();
        sys.set_module_enabled(true);
        sys.apply(bucket, Some(&odm_config(policy))).await;
        sys
    }

    fn head_count(state: &BucketOdmState, outcome: OdmOutcome) -> u64 {
        state.stats().snapshot(state.breaker().state()).requests_total["head"][outcome.as_str()]
    }

    fn assert_no_head_traffic(state: &BucketOdmState) {
        let snapshot = state.stats().snapshot(state.breaker().state());
        assert!(
            snapshot.requests_total["head"].values().all(|count| *count == 0),
            "HEAD must not have entered the runtime: {:?}",
            snapshot.requests_total["head"]
        );
    }

    struct ScriptedSource {
        responses: Mutex<VecDeque<Result<SourceHead, SourceError>>>,
        calls: AtomicUsize,
    }

    impl ScriptedSource {
        fn new(responses: Vec<Result<SourceHead, SourceError>>) -> Self {
            Self {
                responses: Mutex::new(responses.into_iter().collect()),
                calls: AtomicUsize::new(0),
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl OdmHeadSource for ScriptedSource {
        async fn head_object(&self, _key: &str) -> Result<SourceHead, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.responses
                .lock()
                .expect("scripted source lock should not be poisoned")
                .pop_front()
                .expect("test script must provide a response for every source HEAD")
        }
    }

    fn source_head() -> SourceHead {
        SourceHead {
            etag: Some("d41d8cd98f00b204e9800998ecf8427e-3".to_string()),
            size: 1234,
            last_modified: Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_445_412_480)),
            content_type: Some("text/plain".to_string()),
            content_encoding: Some("gzip".to_string()),
            content_disposition: Some("attachment; filename=\"a.txt\"".to_string()),
            content_language: Some("en".to_string()),
            cache_control: Some("max-age=60".to_string()),
            expires: Some("Thu, 01 Jan 2026 00:00:00 GMT".to_string()),
            user_metadata: HashMap::from([("owner".to_string(), "alice".to_string())]),
            version_id: Some("v1".to_string()),
            storage_class: Some("STANDARD_IA".to_string()),
            sse: Some(crate::on_demand_migration::source_client::SourceSse::Kms {
                key_id: Some("key-1".to_string()),
            }),
            is_multipart_etag: true,
            etag_is_opaque: false,
        }
    }

    fn consult(sys: &OnDemandMigrationSys, bucket: &str, key: &str) -> Arc<BucketOdmState> {
        match odm_head_verdict(sys.resolve(bucket, key).expect("bucket is configured"), OdmLocalMiss::NotFound) {
            OdmHeadVerdict::Consult { state, .. } => state,
            OdmHeadVerdict::Ignore => panic!("expected Consult, got Ignore"),
            OdmHeadVerdict::Fail(err) => panic!("expected Consult, got {err:?}"),
        }
    }

    fn fail(sys: &OnDemandMigrationSys, bucket: &str, key: &str, miss: OdmLocalMiss) -> S3Error {
        match odm_head_verdict(sys.resolve(bucket, key).expect("bucket is configured"), miss) {
            OdmHeadVerdict::Fail(err) => err,
            OdmHeadVerdict::Ignore => panic!("expected Fail, got Ignore"),
            OdmHeadVerdict::Consult { .. } => panic!("expected Fail, got Consult"),
        }
    }

    #[test]
    fn odm_head_output_maps_source_fields_and_hides_local_only_headers() {
        let output = odm_head_output(source_head()).expect("source HEAD maps");
        assert_eq!(output.content_length, Some(1234));
        assert_eq!(output.content_type.as_ref().map(|v| v.to_string()), Some("text/plain".to_string()));
        assert_eq!(output.content_encoding.as_deref(), Some("gzip"));
        assert_eq!(output.content_disposition.as_deref(), Some("attachment; filename=\"a.txt\""));
        assert_eq!(output.content_language.as_deref(), Some("en"));
        assert_eq!(output.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(output.expires.as_deref(), Some("Thu, 01 Jan 2026 00:00:00 GMT"));
        assert_eq!(output.accept_ranges.as_deref(), Some("bytes"));
        assert_eq!(
            output.e_tag,
            Some(ETag::Strong("d41d8cd98f00b204e9800998ecf8427e-3".to_string())),
            "the source ETag is returned as-is"
        );
        let last_modified: OffsetDateTime = output.last_modified.expect("source Last-Modified is returned").into();
        assert_eq!(last_modified.unix_timestamp(), 1_445_412_480);
        assert_eq!(output.metadata, Some(HashMap::from([("owner".to_string(), "alice".to_string())])));
        assert_eq!(output.version_id, None, "no x-amz-version-id for a source answer");
        assert_eq!(output.server_side_encryption, None);
        assert_eq!(output.ssekms_key_id, None);
        assert_eq!(output.sse_customer_algorithm, None);
        assert_eq!(output.storage_class, None);
        assert_eq!(output.replication_status, None);

        let bare = odm_head_output(SourceHead::default()).expect("empty source HEAD maps");
        assert_eq!(bare.content_length, Some(0));
        assert_eq!(bare.e_tag, None);
        assert_eq!(bare.metadata, None, "no metadata header family for an empty map");
        assert_eq!(bare.last_modified, None);
    }

    #[tokio::test]
    async fn odm_head_source_hit_returns_output_and_writes_nothing_back() {
        let sys = odm_sys("b", PolicyConfig::default()).await;
        let state = consult(&sys, "b", "k");
        let source = ScriptedSource::new(vec![Ok(source_head())]);

        let output = odm_head_from_source(&state, &source, "k").await.expect("source hit");
        assert_eq!(output.content_length, Some(1234));
        assert_eq!(source.calls(), 1);
        assert_eq!(head_count(&state, OdmOutcome::SourceHit), 1);
        let snapshot = state.stats().snapshot(state.breaker().state());
        assert_eq!(snapshot.source_latency.count, 1, "source latency is observed once per HEAD");
        assert_eq!(snapshot.queue_depth, 0, "HEAD never queues a pull");
        assert_eq!(snapshot.inflight_pulls, 0);
        assert_eq!(state.inflight_keys(), 0);
        assert_eq!(snapshot.pulled_objects_total.values().sum::<u64>(), 0);
        assert!(
            matches!(sys.resolve("b", "k"), Some(OdmLookup::Ready { .. })),
            "a hit leaves the key consultable"
        );
    }

    #[tokio::test]
    async fn odm_head_source_not_found_is_404_and_negative_cached() {
        let sys = odm_sys("b", PolicyConfig::default()).await;
        let state = consult(&sys, "b", "gone");
        let source = ScriptedSource::new(vec![Err(SourceError::NotFound)]);

        let err = odm_head_from_source(&state, &source, "gone").await.expect_err("source 404");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&state, OdmOutcome::SourceMiss), 1);

        // The second HEAD stops at the negative cache: no source call.
        let err = fail(&sys, "b", "gone", OdmLocalMiss::NotFound);
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&state, OdmOutcome::NegativeCached), 1);
        assert_eq!(source.calls(), 1);
        assert!(
            matches!(sys.resolve("b", "other"), Some(OdmLookup::Ready { .. })),
            "other keys stay consultable"
        );
    }

    #[tokio::test]
    async fn odm_head_source_errors_follow_policy_and_open_the_breaker() {
        let sys = odm_sys("b", PolicyConfig::default()).await;
        let state = consult(&sys, "b", "k");
        let source = ScriptedSource::new(vec![
            Err(SourceError::ServerError(503)),
            Err(SourceError::Timeout),
            Err(SourceError::AccessDenied),
            Err(SourceError::Other("boom".to_string())),
        ]);

        let err = odm_head_from_source(&state, &source, "k").await.expect_err("503 propagates");
        assert_eq!(err.status_code(), Some(StatusCode::FAILED_DEPENDENCY));
        assert_eq!(err.code(), &S3ErrorCode::Custom(ODM_SOURCE_UNAVAILABLE_CODE.into()));
        assert_eq!(err.message(), Some("server_error"), "message carries the class only");
        let err = odm_head_from_source(&state, &source, "k")
            .await
            .expect_err("timeout propagates");
        assert_eq!(err.message(), Some("timeout"));
        let err = odm_head_from_source(&state, &source, "k")
            .await
            .expect_err("access denied propagates");
        assert_eq!(err.message(), Some("access_denied"));
        let err = odm_head_from_source(&state, &source, "k")
            .await
            .expect_err("other propagates");
        assert_eq!(err.message(), Some("other"));
        assert_eq!(head_count(&state, OdmOutcome::SourceError), 4);
        assert_eq!(state.stats().last_source_error().map(|e| e.class), Some("other".to_string()));
        assert_eq!(state.breaker().state(), BreakerState::Closed, "neutral classes do not score");

        let hidden_sys = odm_sys(
            "h",
            PolicyConfig {
                source_error: SourceErrorPolicy::NotFound,
                ..Default::default()
            },
        )
        .await;
        let hidden = consult(&hidden_sys, "h", "k");
        let source = ScriptedSource::new(vec![Err(SourceError::ServerError(503))]);
        let err = odm_head_from_source(&hidden, &source, "k").await.expect_err("503 hidden");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&hidden, OdmOutcome::SourceError), 1);

        // Consecutive scoring failures open the breaker; later HEADs stop
        // before the source.
        let breaker_sys = odm_sys("c", PolicyConfig::default()).await;
        let state = consult(&breaker_sys, "c", "k");
        let source = ScriptedSource::new(
            (0..BREAKER_FAILURE_THRESHOLD)
                .map(|_| Err(SourceError::ServerError(503)))
                .collect(),
        );
        for _ in 0..BREAKER_FAILURE_THRESHOLD {
            let state = consult(&breaker_sys, "c", "k");
            let err = odm_head_from_source(&state, &source, "k").await.expect_err("503");
            assert_eq!(err.status_code(), Some(StatusCode::FAILED_DEPENDENCY));
        }
        assert_eq!(state.breaker().state(), BreakerState::Open);
        let err = fail(&breaker_sys, "c", "k", OdmLocalMiss::NotFound);
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&state, OdmOutcome::BreakerOpen), 1);
        assert_eq!(
            source.calls(),
            BREAKER_FAILURE_THRESHOLD as usize,
            "an open breaker never reaches the source"
        );
    }

    #[tokio::test]
    async fn odm_head_unsupported_source_object_is_424_regardless_of_policy() {
        let sys = odm_sys(
            "b",
            PolicyConfig {
                source_error: SourceErrorPolicy::NotFound,
                ..Default::default()
            },
        )
        .await;
        let state = consult(&sys, "b", "k");
        let source = ScriptedSource::new(vec![Err(SourceError::Unsupported("SSE-C".to_string()))]);

        let err = odm_head_from_source(&state, &source, "k").await.expect_err("unsupported");
        assert_eq!(err.status_code(), Some(StatusCode::FAILED_DEPENDENCY));
        assert_eq!(err.message(), Some("unsupported"));
        assert_eq!(head_count(&state, OdmOutcome::Unsupported), 1);
        assert_eq!(head_count(&state, OdmOutcome::SourceError), 0);
        assert_eq!(state.breaker().state(), BreakerState::Closed);
    }

    #[tokio::test]
    async fn odm_head_local_only_policy_is_404_without_source_traffic() {
        let sys = odm_sys(
            "b",
            PolicyConfig {
                head: HeadPolicy::LocalOnly,
                ..Default::default()
            },
        )
        .await;
        let err = fail(&sys, "b", "k", OdmLocalMiss::NotFound);
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        let state = sys.state("b").expect("configured");
        assert_eq!(head_count(&state, OdmOutcome::Filtered), 1);
        assert_eq!(state.stats().snapshot(state.breaker().state()).source_latency.count, 0);
    }

    #[tokio::test]
    async fn odm_head_verdict_respects_local_delete_marker_by_policy() {
        let sys = odm_sys("b", PolicyConfig::default()).await;
        let lookup = sys.resolve("b", "k").expect("configured");
        assert!(matches!(odm_head_verdict(lookup, OdmLocalMiss::DeleteMarker), OdmHeadVerdict::Ignore));
        assert_no_head_traffic(&sys.state("b").expect("configured"));

        let sys = odm_sys(
            "o",
            PolicyConfig {
                respect_local_delete_marker: false,
                ..Default::default()
            },
        )
        .await;
        let lookup = sys.resolve("o", "k").expect("configured");
        assert!(matches!(
            odm_head_verdict(lookup, OdmLocalMiss::DeleteMarker),
            OdmHeadVerdict::Consult { .. }
        ));
    }

    #[tokio::test]
    async fn odm_head_unavailable_client_follows_source_error_policy() {
        let sys = OnDemandMigrationSys::new();
        sys.set_module_enabled(true);
        let mut cfg = odm_config(PolicyConfig::default());
        cfg.source.credentials = None;
        sys.apply("b", Some(&cfg)).await;
        let state = sys.state("b").expect("configured");
        assert_eq!(state.client().err(), Some(&OdmStateError::AnonymousUnsupported));

        let err = fail(&sys, "b", "k", OdmLocalMiss::NotFound);
        assert_eq!(err.status_code(), Some(StatusCode::FAILED_DEPENDENCY));
        assert_eq!(err.message(), Some("unsupported"));
        assert_eq!(head_count(&state, OdmOutcome::SourceError), 1);

        cfg.policy.source_error = SourceErrorPolicy::NotFound;
        sys.apply("b", Some(&cfg)).await;
        let err = fail(&sys, "b", "k", OdmLocalMiss::NotFound);
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
    }

    fn head_input(bucket: &str, key: &str, version_id: Option<String>) -> S3Request<HeadObjectInput> {
        let input = HeadObjectInput::builder()
            .bucket(bucket.to_string())
            .key(key.to_string())
            .version_id(version_id)
            .build()
            .unwrap();
        build_request(input, Method::HEAD)
    }

    /// Drives `execute_head_object` against a real store with the global
    /// runtime configured as `head = local_only`: any HEAD that wrongly
    /// enters the runtime shows up as a `filtered` count, so a zero counter
    /// proves the gate held.
    #[test]
    #[serial_test::serial]
    fn execute_head_object_odm_gate_against_real_store() {
        crate::app::gating_test_env::run_large_stack_test(
            "execute-head-object-odm-gate",
            execute_head_object_odm_gate_against_real_store_inner,
        );
    }

    async fn execute_head_object_odm_gate_against_real_store_inner() {
        use crate::app::storage_api::test::contract::bucket::{BucketOperations as _, MakeBucketOptions};

        let store = crate::app::gating_test_env::shared_gating_ecstore().await;
        if current_app_context().is_none() {
            crate::app::runtime_sources::install_test_app_context(Arc::clone(&store)).await;
        }
        let usecase = DefaultObjectUsecase::from_global();

        let bucket = format!("odm-head-{}", Uuid::new_v4().simple());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    versioning_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create versioned ODM test bucket");
        let mut reader = PutObjReader::from_vec(b"present".to_vec());
        store
            .put_object(
                &bucket,
                "present",
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("put local object");

        let sys = OnDemandMigrationSys::get();
        sys.set_module_enabled(true);
        let mut cfg = odm_config(PolicyConfig {
            head: HeadPolicy::LocalOnly,
            ..Default::default()
        });
        sys.apply_for_incarnation(
            &bucket,
            store.bucket_incarnation_id(&bucket).await.expect("bucket incarnation"),
            Some(&cfg),
        )
        .await;
        let state = sys.state(&bucket).expect("bucket runtime installed");

        // Local hit: served locally, the runtime is never entered.
        let response = Box::pin(usecase.execute_head_object(head_input(&bucket, "present", None)))
            .await
            .expect("local object is served");
        assert_eq!(response.output.content_length, Some(7));
        assert!(response.headers.get(ON_DEMAND_MIGRATION_HEADER).is_none());
        assert_no_head_traffic(&state);

        // HEAD ?versionId on a missing object never consults the runtime.
        let err = Box::pin(usecase.execute_head_object(head_input(&bucket, "missing", Some(Uuid::new_v4().to_string()))))
            .await
            .expect_err("missing version");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_no_head_traffic(&state);

        // The anti-loop marker (any value) keeps the answer local.
        let mut req = head_input(&bucket, "missing", None);
        req.headers
            .insert("x-minio-source-proxy-request", HeaderValue::from_static("false"));
        let err = Box::pin(usecase.execute_head_object(req)).await.expect_err("missing object");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_no_head_traffic(&state);

        // A plain miss reaches the runtime; local_only answers 404 there.
        let err = Box::pin(usecase.execute_head_object(head_input(&bucket, "missing", None)))
            .await
            .expect_err("missing object");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&state, OdmOutcome::Filtered), 1);

        // Latest delete marker: respected by default, a miss once overridden.
        store
            .delete_object(
                &bucket,
                "present",
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write delete marker");
        let err = Box::pin(usecase.execute_head_object(head_input(&bucket, "present", None)))
            .await
            .expect_err("delete marker hides the object");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(
            head_count(&state, OdmOutcome::Filtered),
            1,
            "respected delete marker never enters the runtime"
        );

        cfg.policy.respect_local_delete_marker = false;
        sys.apply_for_incarnation(
            &bucket,
            store.bucket_incarnation_id(&bucket).await.expect("bucket incarnation"),
            Some(&cfg),
        )
        .await;
        let err = Box::pin(usecase.execute_head_object(head_input(&bucket, "present", None)))
            .await
            .expect_err("local_only still answers 404");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);
        assert_eq!(head_count(&state, OdmOutcome::Filtered), 2, "overridden delete marker is a miss");

        // A bucket without a runtime keeps the plain 404.
        let plain = format!("odm-head-plain-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&plain, &MakeBucketOptions::default())
            .await
            .expect("create plain bucket");
        assert!(sys.state(&plain).is_none());
        let err = Box::pin(usecase.execute_head_object(head_input(&plain, "missing", None)))
            .await
            .expect_err("missing object");
        assert_eq!(err.code(), &S3ErrorCode::NoSuchKey);

        sys.remove(&bucket);
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

        let err = Box::pin(usecase.execute_head_object(req)).await.unwrap_err();
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }
}
