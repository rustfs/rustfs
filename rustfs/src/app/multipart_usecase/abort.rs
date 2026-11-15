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

//! AbortMultipartUpload.

use super::*;
use crate::app::RequestEnvelope;
#[cfg(any(feature = "gateway-stack", test))]
use crate::app::storage_api::multipart_usecase::contract::multipart::{MultipartAbortAuthorization, MultipartOwner};
use http::StatusCode;
#[cfg(any(feature = "gateway-stack", test))]
use rustfs_gateway_types::{BucketName, ObjectKey, RecordedUpload, UploadIdClaim, resolve_upload, resolve_upload_async};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
#[cfg(any(feature = "gateway-stack", test))]
use std::sync::OnceLock;

pub(crate) enum AbortMultipartUploadTarget {
    Legacy {
        bucket: String,
        key: String,
        upload_id: String,
    },
    #[cfg(any(feature = "gateway-stack", test))]
    Gateway {
        bucket: BucketName,
        key: ObjectKey,
        upload_id: UploadIdClaim,
    },
}

impl AbortMultipartUploadTarget {
    fn resource(&self) -> (&str, &str) {
        match self {
            Self::Legacy { bucket, key, .. } => (bucket, key),
            #[cfg(any(feature = "gateway-stack", test))]
            Self::Gateway { bucket, key, .. } => (bucket.as_str(), key.as_str()),
        }
    }
}

pub(crate) fn reject_abort_capabilities(envelope: &RequestEnvelope) -> S3Result<()> {
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
    Ok(())
}

impl DefaultMultipartUsecase {
    #[instrument(level = "debug", skip(self, envelope, target))]
    pub(crate) async fn execute_abort_multipart_upload(
        &self,
        envelope: RequestEnvelope,
        target: S3Result<AbortMultipartUploadTarget>,
    ) -> S3Result<()> {
        RequestEnvelope::audit_only_operation(
            envelope,
            S3Operation::AbortMultipartUpload,
            StatusCode::OK,
            |envelope| async move {
                let target = target?;
                let (bucket, key) = target.resource();
                let mut opts = ObjectOptions::default();
                apply_bucket_generation_guard(&envelope, bucket, &mut opts)?;
                #[cfg(any(feature = "gateway-stack", test))]
                if matches!(&target, AbortMultipartUploadTarget::Gateway { .. }) && opts.expected_bucket_incarnation_id.is_none()
                {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "bucket generation guard is missing"));
                }

                reject_veeam_sosapi_mutation(|| self.object_store(), bucket, key).await?;
                let Some(store) = self.object_store() else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
                };

                match &target {
                    AbortMultipartUploadTarget::Legacy { upload_id, .. } => store
                        .abort_multipart_upload(bucket, key, upload_id, &opts)
                        .await
                        .map_err(multipart_upload_api_error)?,
                    #[cfg(any(feature = "gateway-stack", test))]
                    AbortMultipartUploadTarget::Gateway { bucket, key, upload_id } => {
                        abort_gateway(&store, bucket, key, upload_id, opts).await?
                    }
                }
                rustfs_scanner::record_dirty_usage_object_from_producer(
                    bucket,
                    key,
                    rustfs_scanner::SegmentInvalidationProducerIdentity::AbortMultipartUpload,
                );
                Ok(())
            },
        )
        .await
    }
}

#[cfg(any(feature = "gateway-stack", test))]
struct AbortOwner(MultipartOwner);

#[cfg(any(feature = "gateway-stack", test))]
impl RecordedUpload for AbortOwner {
    fn bucket(&self) -> &str {
        &self.0.bucket
    }

    fn key(&self) -> &str {
        &self.0.object
    }
}

#[cfg(any(feature = "gateway-stack", test))]
struct AbortAuthorization {
    bucket: BucketName,
    key: ObjectKey,
    upload_id: UploadIdClaim,
    observed: OnceLock<MultipartOwner>,
}

#[cfg(any(feature = "gateway-stack", test))]
impl MultipartAbortAuthorization for AbortAuthorization {
    type Error = StorageError;

    fn authorize(&self, owner: Option<&MultipartOwner>) -> Result<(), Self::Error> {
        let (_, owner) =
            resolve_upload(&self.upload_id, &self.bucket, &self.key, |_| owner.cloned().map(AbortOwner)).map_err(|_| {
                StorageError::InvalidUploadID(self.bucket.as_str().to_owned(), self.key.as_str().to_owned(), String::new())
            })?;
        self.observed.get_or_init(|| owner.0);
        Ok(())
    }
}

#[cfg(any(feature = "gateway-stack", test))]
async fn abort_gateway(
    store: &ECStore,
    bucket: &BucketName,
    key: &ObjectKey,
    upload_id: &UploadIdClaim,
    mut opts: ObjectOptions,
) -> S3Result<()> {
    let authorization = Arc::new(AbortAuthorization {
        bucket: bucket.clone(),
        key: key.clone(),
        upload_id: upload_id.clone(),
        observed: OnceLock::new(),
    });
    opts.multipart_abort_authorization = Some(authorization.clone());
    // The outer exchange checks the claim's shape. Only the inner exchange, on the
    // selected metadata while holding the upload lock, authorizes deletion.
    resolve_upload_async(upload_id, bucket, key, |candidate| async move {
        store
            .abort_multipart_upload(bucket.as_str(), key.as_str(), candidate, &opts)
            .await
            .map_err(multipart_upload_api_error)?;
        let owner = authorization.observed.get().cloned().ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InternalError, "Abort authorization did not observe the upload owner")
        })?;
        Ok::<_, S3Error>(Some(AbortOwner(owner)))
    })
    .await?
    .map_err(|_| {
        // An empty id has the legacy missing-upload response. Other impossible shapes
        // have the malformed-upload response. Neither reaches the storage lookup.
        let error = if upload_id.is_explicitly_empty() {
            StorageError::InvalidUploadID(bucket.as_str().to_owned(), key.as_str().to_owned(), String::new())
        } else {
            StorageError::MalformedUploadID(String::new())
        };
        multipart_upload_api_error(error)
    })?;
    Ok(())
}

#[cfg(test)]
mod tests;
