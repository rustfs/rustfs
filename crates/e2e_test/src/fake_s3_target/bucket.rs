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
use rustfs_gateway::{ETag, ObjectKey, dto};

impl FakeBackend {
    pub(super) async fn head_bucket(
        &self,
        req: TargetRequest<dto::HeadBucketInput>,
    ) -> S3Result<TargetResponse<dto::HeadBucketOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        if !lock(&self.store).buckets.contains_key(req.input.bucket.as_str()) {
            return Err(s3_error!(NoSuchBucket, "bucket does not exist"));
        }
        Ok(apply_response_fault(
            TargetResponse::new(dto::HeadBucketOutput::default()),
            fault.as_ref(),
        ))
    }

    pub(super) async fn get_bucket_versioning(
        &self,
        req: TargetRequest<dto::GetBucketVersioningInput>,
    ) -> S3Result<TargetResponse<dto::GetBucketVersioningOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let versioned = lock(&self.store)
            .buckets
            .get(req.input.bucket.as_str())
            .map(|bucket| bucket.versioned)
            .ok_or_else(|| s3_error!(NoSuchBucket, "bucket does not exist"))?;
        Ok(apply_response_fault(
            TargetResponse::new(dto::GetBucketVersioningOutput {
                status: versioned.then_some(dto::Status::ENABLED),
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    pub(super) async fn get_object_lock_configuration(
        &self,
        req: TargetRequest<dto::GetObjectLockConfigurationInput>,
    ) -> S3Result<TargetResponse<dto::GetObjectLockConfigurationOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let object_lock = lock(&self.store)
            .buckets
            .get(req.input.bucket.as_str())
            .map(|bucket| bucket.object_lock)
            .ok_or_else(|| s3_error!(NoSuchBucket, "bucket does not exist"))?;
        if !object_lock {
            return Err(s3_error!(
                ObjectLockConfigurationNotFoundError,
                "Object Lock configuration does not exist for this bucket"
            ));
        }
        Ok(apply_response_fault(
            TargetResponse::new(dto::GetObjectLockConfigurationOutput {
                object_lock_configuration: Some(dto::ObjectLockConfiguration {
                    object_lock_enabled: Some(dto::ObjectLockEnabled::ENABLED),
                    ..Default::default()
                }),
            }),
            fault.as_ref(),
        ))
    }

    /// Current versions only (a key whose newest version is a delete marker
    /// is hidden), keys in byte order, `delimiter` folding into common
    /// prefixes that count toward `max-keys`, and `start-after` /
    /// `continuation-token` cursors. `encoding-type` and `fetch-owner` are
    /// accepted but ignored.
    pub(super) async fn list_objects_v2(
        &self,
        req: TargetRequest<dto::ListObjectsV2Input>,
    ) -> S3Result<TargetResponse<dto::ListObjectsV2Output>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let input = req.input;
        let prefix = input.prefix.unwrap_or_default();
        let delimiter = input.delimiter.as_deref().filter(|delimiter| !delimiter.is_empty());
        let max_keys = input.max_keys.unwrap_or(MAX_LIST_KEYS).clamp(0, MAX_LIST_KEYS);
        let resume_after = input
            .continuation_token
            .as_ref()
            .map(|token| decode_continuation_token(token.as_str()))
            .transpose()?;
        let mut cursors = Vec::with_capacity(2);
        if let Some(start_after) = input.start_after.as_deref() {
            cursors.push(ListCursor {
                after: start_after,
                skips_group: false,
            });
        }
        if let Some(resume_after) = resume_after.as_deref() {
            cursors.push(ListCursor {
                after: resume_after,
                skips_group: delimiter.is_some_and(|delimiter| resume_after.ends_with(delimiter)),
            });
        }
        let page = {
            let state = lock(&self.store);
            let bucket_state = state
                .buckets
                .get(input.bucket.as_str())
                .ok_or_else(|| s3_error!(NoSuchBucket, "bucket does not exist"))?;
            list_page(
                bucket_state,
                &prefix,
                delimiter,
                &cursors,
                usize::try_from(max_keys).map_err(S3Error::internal_error)?,
            )
        };
        let next_continuation_token = page
            .truncated
            .then(|| {
                page.entries
                    .last()
                    .map(|entry| encode_continuation_token(entry.name()).into())
            })
            .flatten();
        let key_count = i32::try_from(page.entries.len()).map_err(S3Error::internal_error)?;
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();
        for entry in page.entries {
            match entry {
                ListEntry::Object {
                    key,
                    e_tag,
                    last_modified,
                    size,
                } => contents.push(dto::Object {
                    key: ObjectKey::new(key).map_err(S3Error::internal_error)?,
                    e_tag: Some(ETag::from_stored(e_tag)),
                    last_modified,
                    size: i64::try_from(size).map_err(S3Error::internal_error)?,
                    storage_class: dto::StorageClass::STANDARD,
                    ..Default::default()
                }),
                ListEntry::CommonPrefix(prefix) => common_prefixes.push(dto::CommonPrefix { prefix }),
            }
        }
        Ok(apply_response_fault(
            TargetResponse::new(dto::ListObjectsV2Output {
                name: input.bucket,
                prefix,
                delimiter: input.delimiter,
                max_keys,
                key_count,
                continuation_token: input.continuation_token,
                start_after: input.start_after,
                encoding_type: input.encoding_type,
                is_truncated: page.truncated,
                next_continuation_token,
                contents,
                common_prefixes,
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }

    /// Prefix + max-keys subset only — enough for the replication-check probe
    /// key allocation. No pagination markers or delimiter folding.
    pub(super) async fn list_object_versions(
        &self,
        req: TargetRequest<dto::ListObjectVersionsInput>,
    ) -> S3Result<TargetResponse<dto::ListObjectVersionsOutput>> {
        let fault = request_fault(&req);
        apply_non_body_fault(fault.as_ref(), &self.control).await?;
        let state = lock(&self.store);
        let Some(bucket_state) = state.buckets.get(req.input.bucket.as_str()) else {
            return Err(s3_error!(NoSuchBucket, "bucket does not exist"));
        };
        let prefix = req.input.prefix.as_deref().unwrap_or_default();
        let max_keys = usize::try_from(req.input.max_keys.unwrap_or(MAX_LIST_KEYS).max(0)).map_err(S3Error::internal_error)?;

        let mut keys: Vec<&String> = bucket_state.objects.keys().filter(|key| key.starts_with(prefix)).collect();
        keys.sort();

        let mut versions = Vec::new();
        let mut delete_markers = Vec::new();
        'keys: for key in keys {
            for version in bucket_state.objects[key].iter().rev() {
                if versions.len() + delete_markers.len() >= max_keys {
                    break 'keys;
                }
                if version.delete_marker {
                    delete_markers.push(dto::DeleteMarkerEntry {
                        key: ObjectKey::new(key.clone()).map_err(S3Error::internal_error)?,
                        version_id: version.version_id.clone().into(),
                        last_modified: version.last_modified,
                        ..Default::default()
                    });
                } else {
                    versions.push(dto::ObjectVersion {
                        key: ObjectKey::new(key.clone()).map_err(S3Error::internal_error)?,
                        version_id: version.version_id.clone().into(),
                        last_modified: version.last_modified,
                        e_tag: Some(ETag::from_stored(version.e_tag.clone())),
                        size: i64::try_from(version.body.len()).map_err(S3Error::internal_error)?,
                        ..Default::default()
                    });
                }
            }
        }
        drop(state);

        Ok(apply_response_fault(
            TargetResponse::new(dto::ListObjectVersionsOutput {
                name: req.input.bucket,
                versions,
                delete_markers,
                ..Default::default()
            }),
            fault.as_ref(),
        ))
    }
}
