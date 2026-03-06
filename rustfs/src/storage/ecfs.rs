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

use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::multipart_usecase::DefaultMultipartUsecase;
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::storage::s3_metrics::{S3Operation, record_s3_op};
use rustfs_ecstore::{
    bucket::tagging::decode_tags_to_map,
    error::{is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    store_api::{BucketOperations, BucketOptions, ObjectOperations, ObjectOptions},
};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use std::{fmt::Debug, sync::LazyLock};
use tokio::io::{AsyncRead, AsyncSeek};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const DEFAULT_OWNER_ID: &str = "rustfsadmin";
const DEFAULT_OWNER_DISPLAY_NAME: &str = "RustFS Tester";

pub(crate) static RUSTFS_OWNER: LazyLock<Owner> = LazyLock::new(|| Owner {
    display_name: Some(DEFAULT_OWNER_DISPLAY_NAME.to_owned()),
    id: Some(DEFAULT_OWNER_ID.to_owned()),
});

#[derive(Clone, Debug, Default)]
pub(crate) struct StoredOwner {
    pub(crate) id: String,
}

pub(crate) fn default_owner() -> StoredOwner {
    StoredOwner {
        id: DEFAULT_OWNER_ID.to_string(),
    }
}

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ListObjectUnorderedQuery {
    #[serde(rename = "allow-unordered")]
    pub(crate) allow_unordered: Option<String>,
}

pub(crate) struct InMemoryAsyncReader {
    cursor: std::io::Cursor<Vec<u8>>,
}

impl InMemoryAsyncReader {
    pub(crate) fn new(data: Vec<u8>) -> Self {
        Self {
            cursor: std::io::Cursor::new(data),
        }
    }
}

impl AsyncRead for InMemoryAsyncReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let unfilled = buf.initialize_unfilled();
        let bytes_read = std::io::Read::read(&mut self.cursor, unfilled)?;
        buf.advance(bytes_read);
        std::task::Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for InMemoryAsyncReader {
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        // std::io::Cursor natively supports negative SeekCurrent offsets
        // It will automatically handle validation and return an error if the final position would be negative
        std::io::Seek::seek(&mut self.cursor, position)?;
        Ok(())
    }

    fn poll_complete(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<u64>> {
        std::task::Poll::Ready(Ok(self.cursor.position()))
    }
}

impl FS {
    pub fn new() -> Self {
        use std::sync::Once;
        static DESCRIBE: Once = Once::new();
        DESCRIBE.call_once(crate::storage::s3_metrics::describe_s3_metrics);
        Self {}
    }

    pub async fn get_object_tag_conditions_for_policy(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> S3Result<std::collections::HashMap<String, Vec<String>>> {
        let Some(store) = new_object_layer_fn() else {
            return Ok(std::collections::HashMap::new());
        };
        let opts = ObjectOptions {
            version_id: version_id.map(String::from),
            ..Default::default()
        };
        let tags = match store.get_object_tags(bucket, object, &opts).await {
            Ok(t) => t,
            Err(e) => {
                if is_err_object_not_found(&e) || is_err_version_not_found(&e) {
                    debug!(
                        target: "rustfs::storage::ecfs",
                        bucket = %bucket,
                        object = %object,
                        version_id = ?version_id,
                        error = %e,
                        "object or version not found when fetching tags for policy; treating as no tags"
                    );
                    return Ok(std::collections::HashMap::new());
                }
                if is_err_bucket_not_found(&e) {
                    return Err(s3_error!(NoSuchBucket, "The specified bucket does not exist"));
                }
                warn!(
                    target: "rustfs::storage::ecfs",
                    bucket = %bucket,
                    object = %object,
                    version_id = ?version_id,
                    error = %e,
                    "get_object_tags failed for policy conditions; denying request"
                );
                return Err(s3_error!(AccessDenied, "Access Denied"));
            }
        };
        let map = decode_tags_to_map(&tags);
        let mut out = std::collections::HashMap::new();
        for (k, v) in map {
            out.insert(format!("ExistingObjectTag/{}", k), vec![v]);
        }
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn normalize_delete_objects_version_id(
        &self,
        version_id: Option<String>,
    ) -> std::result::Result<(Option<String>, Option<Uuid>), String> {
        let version_id = version_id.map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
        match version_id {
            Some(id) => {
                if id.eq_ignore_ascii_case("null") {
                    Ok((Some("null".to_string()), Some(Uuid::nil())))
                } else {
                    let uuid = Uuid::parse_str(&id).map_err(|e| e.to_string())?;
                    Ok((Some(id), Some(uuid)))
                }
            }
            None => Ok((None, None)),
        }
    }
}

pub(crate) fn parse_object_version_id(version_id: Option<String>) -> S3Result<Option<Uuid>> {
    if let Some(vid) = version_id {
        let uuid = Uuid::parse_str(&vid).map_err(|e| {
            error!("Invalid version ID: {}", e);
            s3_error!(InvalidArgument, "Invalid version ID")
        })?;
        Ok(Some(uuid))
    } else {
        Ok(None)
    }
}

#[async_trait::async_trait]
impl S3 for FS {
    #[instrument(level = "debug", skip(self))]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        record_s3_op(S3Operation::AbortMultipartUpload, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_abort_multipart_upload(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        record_s3_op(S3Operation::CompleteMultipartUpload, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_complete_multipart_upload(req).await
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        record_s3_op(S3Operation::CopyObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_copy_object(req).await
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        record_s3_op(S3Operation::CreateBucket, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_create_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        record_s3_op(S3Operation::CreateMultipartUpload, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_create_multipart_upload(req).await
    }

    /// Delete a bucket
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        record_s3_op(S3Operation::DeleteBucket, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        record_s3_op(S3Operation::DeleteBucketCors, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_cors(req).await
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        record_s3_op(S3Operation::DeleteBucketEncryption, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        record_s3_op(S3Operation::DeleteBucketLifecycle, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_lifecycle(req).await
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        record_s3_op(S3Operation::DeleteBucketPolicy, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_policy(req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        record_s3_op(S3Operation::DeleteBucketReplication, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        record_s3_op(S3Operation::DeleteBucketTagging, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        record_s3_op(S3Operation::DeletePublicAccessBlock, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_public_access_block(req).await
    }

    /// Delete an object
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        record_s3_op(S3Operation::DeleteObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        record_s3_op(S3Operation::DeleteObjectTagging, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object_tagging(req).await
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        record_s3_op(S3Operation::DeleteObjects, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_objects(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_acl(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        record_s3_op(S3Operation::GetBucketCors, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_cors(req).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        record_s3_op(S3Operation::GetBucketEncryption, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketLifecycleConfiguration, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_lifecycle_configuration(req).await
    }

    /// Get bucket location
    #[instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        record_s3_op(S3Operation::GetBucketLocation, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_location(req).await
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketNotificationConfiguration, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_notification_configuration(req).await
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        record_s3_op(S3Operation::GetBucketPolicy, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_policy(req).await
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        record_s3_op(S3Operation::GetBucketPolicyStatus, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_policy_status(req).await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        record_s3_op(S3Operation::GetBucketReplication, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        record_s3_op(S3Operation::GetBucketTagging, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        record_s3_op(S3Operation::GetPublicAccessBlock, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        record_s3_op(S3Operation::GetBucketVersioning, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_versioning(req).await
    }

    /// Get bucket notification
    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        record_s3_op(S3Operation::GetObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object(req).await
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        record_s3_op(S3Operation::GetObjectAcl, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_acl(req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        record_s3_op(S3Operation::GetObjectAttributes, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_attributes(req).await
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        record_s3_op(S3Operation::GetObjectLegalHold, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_legal_hold(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        record_s3_op(S3Operation::GetObjectLockConfiguration, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_lock_configuration(req).await
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        record_s3_op(S3Operation::GetObjectRetention, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_retention(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        record_s3_op(S3Operation::GetObjectTagging, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_tagging(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn get_object_torrent(&self, req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        record_s3_op(S3Operation::GetObjectTorrent, &req.input.bucket);
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        record_s3_op(S3Operation::HeadBucket, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_head_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        record_s3_op(S3Operation::HeadObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_head_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        record_s3_op(S3Operation::ListBuckets, "");
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_buckets(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        record_s3_op(S3Operation::ListMultipartUploads, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_list_multipart_uploads(req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        record_s3_op(S3Operation::ListObjectVersions, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_object_versions(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        record_s3_op(S3Operation::ListObjects, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        record_s3_op(S3Operation::ListObjectsV2, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects_v2(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        record_s3_op(S3Operation::ListParts, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_list_parts(req).await
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        record_s3_op(S3Operation::PutBucketAcl, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_acl(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        record_s3_op(S3Operation::PutBucketCors, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_cors(req).await
    }

    async fn get_bucket_logging(&self, req: S3Request<GetBucketLoggingInput>) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        record_s3_op(S3Operation::GetBucketLogging, &req.input.bucket);
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;
        Err(s3_error!(NotImplemented, "GetBucketLogging is not implemented yet"))
    }

    async fn put_bucket_logging(&self, req: S3Request<PutBucketLoggingInput>) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        record_s3_op(S3Operation::PutBucketLogging, &req.input.bucket);
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;
        Err(s3_error!(NotImplemented, "PutBucketLogging is not implemented yet"))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        record_s3_op(S3Operation::PutBucketEncryption, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        record_s3_op(S3Operation::PutBucketLifecycleConfiguration, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_lifecycle_configuration(req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::PutBucketNotificationConfiguration, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_notification_configuration(req).await
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        record_s3_op(S3Operation::PutBucketPolicy, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_policy(req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        record_s3_op(S3Operation::PutBucketReplication, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        record_s3_op(S3Operation::PutPublicAccessBlock, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        record_s3_op(S3Operation::PutBucketTagging, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        record_s3_op(S3Operation::PutBucketVersioning, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_versioning(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        record_s3_op(S3Operation::PutObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object(self, req).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        record_s3_op(S3Operation::PutObjectAcl, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_acl(req).await
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        record_s3_op(S3Operation::PutObjectLegalHold, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_legal_hold(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        record_s3_op(S3Operation::PutObjectLockConfiguration, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_lock_configuration(req).await
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        record_s3_op(S3Operation::PutObjectRetention, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_retention(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        record_s3_op(S3Operation::PutObjectTagging, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_tagging(req).await
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        record_s3_op(S3Operation::RestoreObject, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_restore_object(req).await
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        record_s3_op(S3Operation::SelectObjectContent, &req.input.bucket);
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_select_object_content(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        record_s3_op(S3Operation::UploadPart, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_upload_part(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        record_s3_op(S3Operation::UploadPartCopy, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_upload_part_copy(req).await
    }
}
