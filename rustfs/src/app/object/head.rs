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

impl DefaultObjectUsecase {
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
    pub async fn execute_head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
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
    use http::Method;

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
}
