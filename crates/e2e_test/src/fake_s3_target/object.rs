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

//! Object operations for the programmable S3 target.

use super::*;
use rustfs_gateway::{ByteStream, ETag, dto};

impl FakeBackend {
    pub(super) async fn put_object(
        &self,
        req: TargetRequest<dto::PutObjectInput>,
    ) -> S3Result<TargetResponse<dto::PutObjectOutput>> {
        let fault = request_fault(&req);
        let _body_permit = timeout(MAX_FAULT_DURATION, Arc::clone(&self.body_limit).acquire_owned())
            .await
            .map_err(|_| s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3_error!(ServiceUnavailable, "fake target body limiter closed"))?;
        let headers = req.headers;
        let input = req.input;
        let (assign_own, drop_unlisted, require_checksum, ignore_source_etags, limits, versioned) = {
            let state = lock(&self.store);
            (
                state.assign_own_version_ids,
                state.drop_unlisted_replication_headers,
                state.require_checksum_for_object_lock,
                state.ignore_source_etags,
                state.limits,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        let transport = TransportSnapshot::from_headers(&headers);
        if require_checksum
            && transport.object_lock_params
            && transport.content_md5.is_none()
            && transport.checksum_headers.is_empty()
        {
            return Err(s3_error!(
                InvalidRequest,
                "Content-MD5 OR x-amz-checksum- HTTP header is required for Put Object requests with Object Lock parameters"
            ));
        }
        let body = collect_stream(
            input.body,
            Some(input.content_length),
            fault.as_ref(),
            &self.control,
            limits.max_object_bytes,
        )
        .await?;
        let mut body_permit = _body_permit;
        if let Some(expected) = &transport.content_md5 {
            let (digest, permit) = md5_digest(body.clone(), body_permit).await?;
            body_permit = permit;
            if BASE64_STANDARD.encode_to_string(digest) != expected.trim() {
                return Err(s3_error!(BadDigest, "The Content-MD5 you specified did not match what we received."));
            }
        }
        let metadata = (!input.metadata.is_empty()).then(|| input.metadata.into_iter().collect());
        validate_stored_metadata(&input.content_type, &metadata)?;
        let standard_headers = StandardHeaders {
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            expires: input.expires.map(|value| value.into_string()),
        };
        standard_headers.validate()?;
        let version_id = if versioned {
            new_version_id(&headers, assign_own)?
        } else {
            NULL_VERSION_ID.to_string()
        };
        let e_tag = match source_etag(&headers)?.filter(|_| !ignore_source_etags) {
            Some(value) => value,
            None => {
                let (digest, permit) = md5_digest(body.clone(), body_permit).await?;
                body_permit = permit;
                hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower)
            }
        };
        // Held until the version is stored, like every other body-bearing op.
        let _body_permit = body_permit;
        let version = ObjectVersion {
            version_id: version_id.clone(),
            body,
            e_tag: e_tag.clone(),
            last_modified: source_mtime(&headers),
            delete_marker: false,
            content_type: input.content_type,
            metadata,
            standard_headers,
            tags: Vec::new(),
            replication_sse_headers: captured_replication_sse_headers(&headers, drop_unlisted),
            lock: VersionLock::from_headers(
                input.object_lock_mode,
                input.object_lock_retain_until_date,
                input.object_lock_legal_hold_status,
            ),
        };
        upsert_version(&mut lock(&self.store), input.bucket.as_str(), input.key.as_str().to_owned(), version)?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::PutObjectOutput {
                e_tag: ETag::from_stored(maybe_wrong_etag(fault.as_ref(), e_tag)),
                version_id: versioned.then_some(version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn get_object(
        &self,
        req: TargetRequest<dto::GetObjectInput>,
    ) -> S3Result<TargetResponse<dto::GetObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let held_get = lock(&self.control)
            .held_get
            .as_ref()
            .filter(|held| held.bucket == input.bucket.as_str() && held.key == input.key.as_str())
            .cloned();
        if let Some(mut held) = held_get {
            held.entered.send_modify(|count| *count += 1);
            // Keep the gate installed when a request is cancelled or times out:
            // a retry must cross the same boundary before returning any bytes.
            let _ = held.released.wait_for(|released| *released).await;
        }
        let (version, versioned) = {
            let state = lock(&self.store);
            (
                find_version(&state, input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())?,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        let sse_customer_algorithm = stored_sse_customer_algorithm(&version);
        let served = resolve_range(input.range.as_ref(), version.body.len())?;
        let body = version.body.slice(served.range.clone());
        let body = match fault.as_ref().map(|fault| &fault.action) {
            Some(FaultAction::TruncateBodyAt(truncate_at)) => truncated_body(body, *truncate_at),
            Some(FaultAction::SlowSendBody { chunk_bytes, delay }) => slow_sent_body(body, *chunk_bytes, *delay),
            _ => ByteStream::from_bytes(body),
        };
        let mut response = TargetResponse::new(dto::GetObjectOutput {
            body: Some(body),
            content_length: Some(served.len() as i64),
            content_range: served.content_range,
            accept_ranges: Some("bytes".to_string()),
            content_type: version.content_type,
            metadata: version.metadata.unwrap_or_default().into_iter().collect(),
            cache_control: version.standard_headers.cache_control,
            content_disposition: version.standard_headers.content_disposition,
            content_encoding: version.standard_headers.content_encoding,
            content_language: version.standard_headers.content_language,
            expires: version.standard_headers.expires.map(Into::into),
            e_tag: Some(ETag::from_stored(version.e_tag)),
            last_modified: Some(version.last_modified),
            version_id: versioned.then_some(version.version_id),
            sse_customer_algorithm,
            object_lock_mode: version
                .lock
                .retention
                .as_ref()
                .map(|(mode, _)| dto::ObjectLockMode::custom(mode.clone())),
            object_lock_retain_until_date: version.lock.retention.as_ref().map(|(_, until)| *until),
            object_lock_legal_hold_status: version.lock.legal_hold_status(),
            ..Default::default()
        });
        response.status = served.status;
        Ok(apply_response_fault(response, fault.as_ref()))
    }

    pub(super) async fn head_object(
        &self,
        req: TargetRequest<dto::HeadObjectInput>,
    ) -> S3Result<TargetResponse<dto::HeadObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let (version, versioned) = {
            let state = lock(&self.store);
            (
                find_version(&state, input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())?,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        let sse_customer_algorithm = stored_sse_customer_algorithm(&version);
        let served = resolve_range(input.range.as_ref(), version.body.len())?;
        let mut response = TargetResponse::new(dto::HeadObjectOutput {
            content_length: Some(served.len() as i64),
            content_range: served.content_range,
            accept_ranges: Some("bytes".to_string()),
            content_type: version.content_type,
            metadata: version.metadata.unwrap_or_default().into_iter().collect(),
            cache_control: version.standard_headers.cache_control,
            content_disposition: version.standard_headers.content_disposition,
            content_encoding: version.standard_headers.content_encoding,
            content_language: version.standard_headers.content_language,
            expires: version.standard_headers.expires.map(Into::into),
            e_tag: Some(ETag::from_stored(version.e_tag)),
            last_modified: Some(version.last_modified),
            version_id: versioned.then_some(version.version_id),
            sse_customer_algorithm,
            object_lock_mode: version
                .lock
                .retention
                .as_ref()
                .map(|(mode, _)| dto::ObjectLockMode::custom(mode.clone())),
            object_lock_retain_until_date: version.lock.retention.as_ref().map(|(_, until)| *until),
            object_lock_legal_hold_status: version.lock.legal_hold_status(),
            ..Default::default()
        });
        response.status = served.status;
        Ok(apply_response_fault(response, fault.as_ref()))
    }

    pub(super) async fn get_object_tagging(
        &self,
        req: TargetRequest<dto::GetObjectTaggingInput>,
    ) -> S3Result<TargetResponse<dto::GetObjectTaggingOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let (version, versioned) = {
            let state = lock(&self.store);
            (
                find_version(&state, input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())?,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        let tag_set = version
            .tags
            .into_iter()
            .map(|(key, value)| dto::Tag {
                key: Some(key),
                value: Some(value),
            })
            .collect();
        Ok(apply_response_fault(
            TargetResponse::new(dto::GetObjectTaggingOutput {
                tag_set,
                version_id: versioned.then_some(version.version_id),
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn put_object_tagging(
        &self,
        req: TargetRequest<dto::PutObjectTaggingInput>,
    ) -> S3Result<TargetResponse<dto::PutObjectTaggingOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let tags = input
            .tagging
            .tag_set
            .into_iter()
            .map(|tag| (tag.key.unwrap_or_default(), tag.value.unwrap_or_default()))
            .collect();
        let (version_id, versioned) = {
            let mut state = lock(&self.store);
            (
                set_version_tags(&mut state, input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref(), tags)?,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        Ok(apply_response_fault(
            TargetResponse::new(dto::PutObjectTaggingOutput {
                version_id: versioned.then_some(version_id),
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn get_object_retention(
        &self,
        req: TargetRequest<dto::GetObjectRetentionInput>,
    ) -> S3Result<TargetResponse<dto::GetObjectRetentionOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let version = find_version(&lock(&self.store), input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::GetObjectRetentionOutput {
                retention: version.lock.retention.map(|(mode, until)| dto::ObjectLockRetention {
                    mode: Some(dto::Mode::custom(mode)),
                    retain_until_date: Some(until),
                    ..Default::default()
                }),
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn put_object_retention(
        &self,
        req: TargetRequest<dto::PutObjectRetentionInput>,
    ) -> S3Result<TargetResponse<dto::PutObjectRetentionOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let retention = input
            .retention
            .mode
            .zip(input.retention.retain_until_date)
            .map(|(mode, until)| (mode.as_str().to_string(), until));
        update_version_lock(
            &mut lock(&self.store),
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id.as_deref(),
            |lock| {
                lock.retention = retention;
            },
        )?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::PutObjectRetentionOutput::default()),
            fault.as_ref(),
        ))
    }

    pub(super) async fn get_object_legal_hold(
        &self,
        req: TargetRequest<dto::GetObjectLegalHoldInput>,
    ) -> S3Result<TargetResponse<dto::GetObjectLegalHoldOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let version = find_version(&lock(&self.store), input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::GetObjectLegalHoldOutput {
                legal_hold: Some(dto::ObjectLockLegalHold {
                    status: Some(if version.lock.legal_hold == Some(true) {
                        dto::Status::ON
                    } else {
                        dto::Status::OFF
                    }),
                }),
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn put_object_legal_hold(
        &self,
        req: TargetRequest<dto::PutObjectLegalHoldInput>,
    ) -> S3Result<TargetResponse<dto::PutObjectLegalHoldOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let legal_hold_on = input
            .legal_hold
            .status
            .is_some_and(|status| status.as_str().eq_ignore_ascii_case("ON"));
        update_version_lock(
            &mut lock(&self.store),
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id.as_deref(),
            |lock| {
                lock.legal_hold = Some(legal_hold_on);
            },
        )?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::PutObjectLegalHoldOutput::default()),
            fault.as_ref(),
        ))
    }

    pub(super) async fn delete_object_tagging(
        &self,
        req: TargetRequest<dto::DeleteObjectTaggingInput>,
    ) -> S3Result<TargetResponse<dto::DeleteObjectTaggingOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let (version_id, versioned) = {
            let mut state = lock(&self.store);
            (
                set_version_tags(
                    &mut state,
                    input.bucket.as_str(),
                    input.key.as_str(),
                    input.version_id.as_deref(),
                    Vec::new(),
                )?,
                bucket_versioned(&state, input.bucket.as_str()),
            )
        };
        Ok(apply_response_fault(
            TargetResponse::new(dto::DeleteObjectTaggingOutput {
                version_id: versioned.then_some(version_id),
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn delete_object(
        &self,
        req: TargetRequest<dto::DeleteObjectInput>,
    ) -> S3Result<TargetResponse<dto::DeleteObjectOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let headers = req.headers;
        let input = req.input;
        let held_delete = lock(&self.control)
            .held_delete
            .as_ref()
            .filter(|held| held.bucket == input.bucket.as_str() && held.key == input.key.as_str())
            .cloned();
        if let Some(mut held) = held_delete {
            held.entered.send_modify(|count| *count += 1);
            let _ = held.released.wait_for(|released| *released).await;
        }
        let mut state = lock(&self.store);
        let Some(versioned) = state.buckets.get(input.bucket.as_str()).map(|bucket| bucket.versioned) else {
            return Err(s3_error!(NoSuchBucket, "bucket does not exist"));
        };
        if !versioned {
            // Unversioned buckets know only the `null` version: DELETE removes
            // the key outright and never writes a delete marker.
            if input
                .version_id
                .as_deref()
                .is_some_and(|version_id| version_id != NULL_VERSION_ID)
            {
                return Err(s3_error!(InvalidArgument, "invalid version id specified"));
            }
            let removed = state
                .buckets
                .get_mut(input.bucket.as_str())
                .expect("bucket existence checked above")
                .objects
                .remove(input.key.as_str())
                .unwrap_or_default();
            state.total_bytes -= removed.iter().map(|version| version.body.len()).sum::<usize>();
            state.total_versions -= removed.len();
            return Ok(apply_response_fault(
                TargetResponse::new(dto::DeleteObjectOutput::default()),
                fault.as_ref(),
            ));
        }
        if let Some(version_id) = input.version_id {
            let reject_unknown = state.reject_unknown_version_deletes;
            let (removed_bytes, removed_versions, delete_marker, remove_key) = {
                let Some(versions) = state
                    .buckets
                    .get_mut(input.bucket.as_str())
                    .expect("bucket existence checked above")
                    .objects
                    .get_mut(input.key.as_str())
                else {
                    if reject_unknown {
                        return Err(s3_error!(NoSuchVersion, "The specified version does not exist."));
                    }
                    return Ok(apply_response_fault(
                        TargetResponse::new(dto::DeleteObjectOutput {
                            version_id: Some(version_id),
                            ..Default::default()
                        }),
                        fault.as_ref(),
                    ));
                };
                if reject_unknown && !versions.iter().any(|version| version.version_id == version_id) {
                    return Err(s3_error!(NoSuchVersion, "The specified version does not exist."));
                }
                let mut removed_bytes = 0usize;
                let mut removed_versions = 0usize;
                let mut delete_marker = None;
                versions.retain(|version| {
                    if version.version_id == version_id {
                        removed_bytes += version.body.len();
                        removed_versions += 1;
                        delete_marker.get_or_insert(version.delete_marker);
                        false
                    } else {
                        true
                    }
                });
                (removed_bytes, removed_versions, delete_marker, versions.is_empty())
            };
            if remove_key {
                state
                    .buckets
                    .get_mut(input.bucket.as_str())
                    .expect("bucket existence checked above")
                    .objects
                    .remove(input.key.as_str());
            }
            state.total_bytes -= removed_bytes;
            state.total_versions -= removed_versions;
            return Ok(apply_response_fault(
                TargetResponse::new(dto::DeleteObjectOutput {
                    delete_marker,
                    version_id: Some(version_id),
                    ..Default::default()
                }),
                fault.as_ref(),
            ));
        }

        // `state` is the live store guard: read the flag from it. Re-locking
        // would self-deadlock (the store mutex is not reentrant).
        let version_id = new_version_id(&headers, state.assign_own_version_ids)?;
        upsert_version(
            &mut state,
            input.bucket.as_str(),
            input.key.as_str().to_owned(),
            ObjectVersion {
                version_id: version_id.clone(),
                body: Bytes::new(),
                e_tag: String::new(),
                last_modified: source_mtime(&headers),
                delete_marker: true,
                content_type: None,
                metadata: None,
                standard_headers: StandardHeaders::default(),
                tags: Vec::new(),
                replication_sse_headers: Vec::new(),
                lock: VersionLock::default(),
            },
        )?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::DeleteObjectOutput {
                delete_marker: Some(true),
                version_id: Some(version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }
}
