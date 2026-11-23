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

//! Multipart staging and publication for the programmable remote S3 target.

use super::*;
use rustfs_gateway::{BucketName, ObjectKey, RecordedUpload, UploadIdClaim, dto};
use rustfs_s3_types::{S3Result, s3_error};

impl RecordedUpload for MultipartState {
    fn bucket(&self) -> &str {
        &self.bucket
    }

    fn key(&self) -> &str {
        &self.key
    }
}

fn resolve_upload<'id, 'store>(
    state: &'store StoreState,
    claim: &'id UploadIdClaim,
    bucket: &BucketName,
    key: &ObjectKey,
) -> S3Result<(&'id str, &'store MultipartState)> {
    let (id, upload) = rustfs_gateway::resolve_upload(claim, bucket, key, |id| state.uploads.get(id))
        .map_err(|_| s3_error!(NoSuchUpload, "multipart upload does not exist"))?;
    Ok((id.id(), upload))
}

impl FakeBackend {
    pub(super) async fn create_multipart_upload(
        &self,
        req: TargetRequest<dto::CreateMultipartUploadInput>,
    ) -> S3Result<TargetResponse<dto::CreateMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let headers = req.headers;
        let input = req.input;
        let mut state = lock(&self.store);
        if !state.buckets.contains_key(input.bucket.as_str()) {
            return Err(s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        ensure_upload_budget(&state)?;
        let metadata = (!input.metadata.is_empty()).then(|| input.metadata.into_iter().collect());
        validate_stored_metadata(&input.content_type, &metadata)?;
        let standard_headers = StandardHeaders {
            cache_control: input.cache_control,
            content_disposition: input.content_disposition,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            expires: input.expires.map(|value| value.as_str().to_owned()),
        };
        standard_headers.validate()?;
        let upload_id = Uuid::new_v4().to_string();
        // Read the flags before the mutable borrow of `state.uploads` below
        // (and never re-lock the store: the mutex is not reentrant).
        let mint_own = state.assign_own_version_ids || state.assign_own_multipart_version_ids;
        let drop_unlisted = state.drop_unlisted_replication_headers;
        let version_id = if bucket_versioned(&state, input.bucket.as_str()) {
            new_version_id(&headers, mint_own)?
        } else {
            NULL_VERSION_ID.to_string()
        };
        state.uploads.insert(
            upload_id.clone(),
            MultipartState {
                bucket: input.bucket.as_str().to_owned(),
                key: input.key.as_str().to_owned(),
                version_id,
                content_type: input.content_type,
                metadata,
                standard_headers,
                replication_sse_headers: captured_replication_sse_headers(&headers, drop_unlisted),
                lock: VersionLock::from_headers(
                    input.object_lock_mode,
                    input.object_lock_retain_until_date,
                    input.object_lock_legal_hold_status,
                ),
                parts: BTreeMap::new(),
            },
        );
        Ok(apply_response_fault(
            TargetResponse::new(dto::CreateMultipartUploadOutput {
                bucket: input.bucket,
                key: input.key,
                upload_id,
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn upload_part(
        &self,
        req: TargetRequest<dto::UploadPartInput>,
    ) -> S3Result<TargetResponse<dto::UploadPartOutput>> {
        let fault = request_fault(&req);
        let declared_actual_size = req
            .headers
            .get("x-rustfs-replication-part-actual-size")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok());
        let _body_permit = timeout(MAX_FAULT_DURATION, Arc::clone(&self.body_limit).acquire_owned())
            .await
            .map_err(|_| s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3_error!(ServiceUnavailable, "fake target body limiter closed"))?;
        let input = req.input;
        if !(1..=10_000).contains(&input.part_number) {
            return Err(s3_error!(InvalidArgument, "part number must be in 1..=10000"));
        }
        let max_object_bytes = lock(&self.store).limits.max_object_bytes;
        let body =
            collect_stream(input.body, Some(input.content_length), fault.as_ref(), &self.control, max_object_bytes).await?;
        let (digest, _body_permit) = md5_digest(body.clone(), _body_permit).await?;
        let e_tag = hex_simd::encode_to_string(digest, hex_simd::AsciiCase::Lower);
        let mut state = lock(&self.store);
        let (upload_id, upload) = resolve_upload(&state, &input.upload_id, &input.bucket, &input.key)?;
        let existing = upload.parts.get(&input.part_number);
        let existing_bytes = existing.map_or(0, |part| part.body.len());
        let adds_part = existing.is_none();
        ensure_part_budget(&state, adds_part)?;
        ensure_store_budget(&state, existing_bytes, body.len(), false)?;
        state.total_bytes = state.total_bytes - existing_bytes + body.len();
        if adds_part {
            state.total_parts += 1;
        }
        let upload = state.uploads.get_mut(upload_id).expect("upload existence checked above");
        upload.parts.insert(
            input.part_number,
            MultipartPart {
                body,
                e_tag: e_tag.clone(),
                digest,
                actual_size: declared_actual_size,
            },
        );
        Ok(apply_response_fault(
            TargetResponse::new(dto::UploadPartOutput {
                e_tag: rustfs_gateway::ETag::from_stored(maybe_wrong_etag(fault.as_ref(), e_tag)),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn complete_multipart_upload(
        &self,
        mut req: TargetRequest<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<TargetResponse<dto::CompleteMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let _body_permits = match req.extensions.remove::<PrebodyPermit>().and_then(PrebodyPermit::take) {
            Some(permit) => permit,
            None => timeout(
                MAX_FAULT_DURATION,
                Arc::clone(&self.body_limit).acquire_many_owned(MAX_CONCURRENT_BODY_REQUESTS as u32),
            )
            .await
            .map_err(|_| s3_error!(RequestTimeout, "fake target body limiter wait exceeded 30 seconds"))?
            .map_err(|_| s3_error!(ServiceUnavailable, "fake target body limiter closed"))?,
        };
        let headers = req.headers;
        let input = req.input;
        let requested = input.multipart_upload.parts;
        if requested.is_empty() {
            return Err(s3_error!(InvalidPart, "completion omitted parts"));
        }
        let mut requested_parts = Vec::with_capacity(requested.len());
        let mut previous = 0;
        for part in &requested {
            let number = part.part_number;
            if number <= previous {
                return Err(s3_error!(InvalidPartOrder, "parts are not strictly ordered"));
            }
            previous = number;
            let requested_etag = part
                .e_tag
                .as_ref()
                .ok_or_else(|| s3_error!(InvalidPart, "part ETag is missing"))?;
            requested_parts.push((number, normalize_etag(requested_etag.opaque_tag())));
        }

        let (upload_id, upload, selected_parts) = {
            let state = lock(&self.store);
            let (upload_id, upload) = resolve_upload(&state, &input.upload_id, &input.bucket, &input.key)?;
            let mut selected = Vec::with_capacity(requested_parts.len());
            for (index, (number, requested_etag)) in requested_parts.iter().enumerate() {
                let stored = upload
                    .parts
                    .get(number)
                    .ok_or_else(|| s3_error!(InvalidPart, "part was not uploaded"))?;
                if requested_etag != &stored.e_tag {
                    return Err(s3_error!(InvalidPart, "part ETag does not match"));
                }
                if index + 1 != requested_parts.len()
                    && stored.actual_size.unwrap_or(stored.body.len()) < MIN_MULTIPART_PART_BYTES
                {
                    return Err(s3_error!(EntityTooSmall, "non-final multipart part is smaller than 5 MiB"));
                }
                selected.push((*number, stored.clone()));
            }
            (
                upload_id,
                MultipartState {
                    bucket: upload.bucket.clone(),
                    key: upload.key.clone(),
                    version_id: upload.version_id.clone(),
                    content_type: upload.content_type.clone(),
                    metadata: upload.metadata.clone(),
                    standard_headers: upload.standard_headers.clone(),
                    replication_sse_headers: upload.replication_sse_headers.clone(),
                    lock: upload.lock.clone(),
                    parts: BTreeMap::new(),
                },
                selected,
            )
        };
        let max_total_bytes = lock(&self.store).limits.max_total_bytes;
        let total_len = selected_parts.iter().try_fold(0usize, |total, (_, part)| {
            total
                .checked_add(part.body.len())
                .filter(|total| *total <= max_total_bytes)
                .ok_or_else(|| s3_error!(EntityTooLarge, "multipart object exceeds {max_total_bytes} bytes"))
        })?;
        let assembly_parts = selected_parts
            .iter()
            .map(|(_, stored)| (stored.body.clone(), stored.digest))
            .collect();
        let (body, digests, _body_permits) = assemble_multipart(assembly_parts, total_len, _body_permits).await?;
        let part_count = requested.len();
        let ignore_source_etags = lock(&self.store).ignore_source_etags;
        let e_tag = source_etag(&headers)?
            .filter(|_| !ignore_source_etags)
            .unwrap_or_else(|| format!("{}-{part_count}", md5_hex(digests)));
        let version = ObjectVersion {
            version_id: upload.version_id.clone(),
            body,
            e_tag: e_tag.clone(),
            last_modified: now_timestamp(),
            delete_marker: false,
            content_type: upload.content_type,
            metadata: upload.metadata,
            standard_headers: upload.standard_headers,
            tags: Vec::new(),
            replication_sse_headers: upload.replication_sse_headers,
            lock: upload.lock,
        };
        let mut state = lock(&self.store);
        let versioned = bucket_versioned(&state, input.bucket.as_str());
        let current = state
            .uploads
            .get(upload_id)
            .filter(|current| current.bucket == input.bucket.as_str() && current.key == input.key.as_str())
            .ok_or_else(|| s3_error!(NoSuchUpload, "multipart upload does not exist"))?;
        for (part, (selected_number, selected_part)) in requested.iter().zip(&selected_parts) {
            let number = part.part_number;
            debug_assert_eq!(number, *selected_number);
            let current_part = current
                .parts
                .get(&number)
                .ok_or_else(|| s3_error!(InvalidPart, "part changed during completion"))?;
            if current_part.e_tag != selected_part.e_tag {
                return Err(s3_error!(InvalidPart, "part changed during completion"));
            }
        }
        let upload_bytes = current.parts.values().map(|part| part.body.len()).sum::<usize>();
        let upload_part_count = current.parts.len();
        let existing = state
            .buckets
            .get(input.bucket.as_str())
            .ok_or_else(|| s3_error!(NoSuchBucket, "bucket does not exist"))?
            .objects
            .get(input.key.as_str())
            .and_then(|versions| versions.iter().find(|candidate| candidate.version_id == version.version_id))
            .map(|version| version.body.len());
        ensure_store_budget(
            &state,
            upload_bytes + existing.unwrap_or_default(),
            version.body.len(),
            existing.is_none(),
        )?;
        state.total_bytes -= upload_bytes;
        state.total_parts -= upload_part_count;
        state.uploads.remove(upload_id);
        upsert_version(&mut state, input.bucket.as_str(), input.key.as_str().to_owned(), version)?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::CompleteMultipartUploadOutput {
                bucket: Some(input.bucket),
                key: Some(input.key),
                e_tag: Some(rustfs_gateway::ETag::from_stored(maybe_wrong_etag(fault.as_ref(), e_tag))),
                version_id: versioned.then_some(upload.version_id),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn abort_multipart_upload(
        &self,
        req: TargetRequest<dto::AbortMultipartUploadInput>,
    ) -> S3Result<TargetResponse<dto::AbortMultipartUploadOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let mut state = lock(&self.store);
        let (upload_id, _) = resolve_upload(&state, &input.upload_id, &input.bucket, &input.key)?;
        let removed = state.uploads.remove(upload_id).expect("upload existence checked above");
        state.total_bytes -= removed.parts.values().map(|part| part.body.len()).sum::<usize>();
        state.total_parts -= removed.parts.len();
        Ok(apply_response_fault(
            TargetResponse::new(dto::AbortMultipartUploadOutput::default()),
            fault.as_ref(),
        ))
    }
}
