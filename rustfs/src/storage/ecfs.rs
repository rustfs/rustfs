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
use crate::error::ApiError;
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_opts;
use crate::storage::s3_api::acl;
use crate::storage::validate_bucket_object_lock_enabled;
use metrics::{counter, histogram};
use rustfs_ecstore::{
    bucket::{
        metadata::{BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_WEBSITE_CONFIG},
        metadata_sys,
        tagging::{decode_tags, decode_tags_to_map},
        utils::serialize,
    },
    error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    store_api::{BucketOperations, BucketOptions, ObjectOperations, ObjectOptions},
};
use rustfs_s3_common::{S3Operation, record_s3_op};
use rustfs_targets::EventName;
use rustfs_utils::http::headers::AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use std::fmt::Debug;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FS {
    // pub store: ECStore,
}

#[derive(Debug, Default, serde::Deserialize)]
pub(crate) struct ListObjectUnorderedQuery {
    #[serde(rename = "allow-unordered")]
    pub(crate) allow_unordered: Option<String>,
}

impl Default for FS {
    fn default() -> Self {
        Self::new()
    }
}

impl FS {
    pub fn new() -> Self {
        rustfs_s3_common::init_s3_metrics();
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
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_abort_multipart_upload(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_complete_multipart_upload(req).await
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_copy_object(req).await
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_create_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_create_multipart_upload(req).await
    }

    /// Delete a bucket
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_cors(req).await
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_lifecycle(req).await
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_policy(req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_replication(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_bucket_tagging(req).await
    }

    async fn delete_bucket_website(
        &self,
        req: S3Request<DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<DeleteBucketWebsiteOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        metadata_sys::delete(&req.input.bucket, BUCKET_WEBSITE_CONFIG)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(DeleteBucketWebsiteOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_delete_public_access_block(req).await
    }

    /// Delete an object
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_object_tagging(req).await
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_objects(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_get_bucket_acl(req).await
    }

    async fn get_bucket_accelerate_configuration(
        &self,
        req: S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketAccelerateConfigurationOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match metadata_sys::get_accelerate_config(&req.input.bucket).await {
            Ok((accelerate, _)) => Ok(S3Response::new(GetBucketAccelerateConfigurationOutput {
                status: accelerate.status,
                ..Default::default()
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketAccelerateConfigurationOutput::default())),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
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

    async fn get_bucket_request_payment(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match metadata_sys::get_request_payment_config(&req.input.bucket).await {
            Ok((payment, _)) => Ok(S3Response::new(GetBucketRequestPaymentOutput {
                payer: Some(payment.payer),
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketRequestPaymentOutput {
                payer: Some(Payer::from_static(Payer::BUCKET_OWNER)),
            })),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
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

    async fn get_bucket_website(&self, req: S3Request<GetBucketWebsiteInput>) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match metadata_sys::get_website_config(&req.input.bucket).await {
            Ok((website, _)) => Ok(S3Response::new(GetBucketWebsiteOutput {
                error_document: website.error_document,
                index_document: website.index_document,
                redirect_all_requests_to: website.redirect_all_requests_to,
                routing_rules: website.routing_rules,
            })),
            Err(StorageError::ConfigNotFound) => Err(s3_error!(NoSuchWebsiteConfiguration)),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    /// Get bucket notification
    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object(req).await
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        record_s3_op(S3Operation::GetObjectAcl, &req.input.bucket);
        let GetObjectAclInput {
            bucket, key, version_id, ..
        } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        store.get_object_info(&bucket, &key, &opts).await.map_err(ApiError::from)?;

        Ok(S3Response::new(acl::build_get_object_acl_output()))
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_get_object_attributes(req).await
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetLegalHold, S3Operation::GetObjectLegalHold).suppress_event();
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let _ = store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let legal_hold = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER)
            .map(|v| v.as_str().to_string());

        let status = if let Some(v) = legal_hold {
            v
        } else {
            ObjectLockLegalHoldStatus::OFF.to_string()
        };

        let output = GetObjectLegalHoldOutput {
            legal_hold: Some(ObjectLockLegalHold {
                status: Some(ObjectLockLegalHoldStatus::from(status)),
            }),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
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
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetRetention, S3Operation::GetObjectRetention).suppress_event();
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!("get_object_info failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let mode = object_info
            .user_defined
            .get("x-amz-object-lock-mode")
            .map(|v| ObjectLockRetentionMode::from(v.as_str().to_string()));

        let retain_until_date = object_info
            .user_defined
            .get("x-amz-object-lock-retain-until-date")
            .and_then(|v| OffsetDateTime::parse(v.as_str(), &Rfc3339).ok())
            .map(Timestamp::from);

        let output = GetObjectRetentionOutput {
            retention: Some(ObjectLockRetention { mode, retain_until_date }),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        record_s3_op(S3Operation::GetObjectTagging, &req.input.bucket);
        let start_time = std::time::Instant::now();
        let bucket = req.input.bucket.as_str();
        let object = req.input.key.as_str();

        info!("Starting get_object_tagging for bucket: {}, object: {}", bucket, object);

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        let tags = store.get_object_tags(bucket, object, &opts).await.map_err(|e| {
            if is_err_object_not_found(&e) {
                error!("Object not found: {}", e);
                return s3_error!(NoSuchKey);
            }
            error!("Failed to get object tags: {}", e);
            ApiError::from(e).into()
        })?;

        let tag_set = decode_tags(tags.as_str());
        debug!("Decoded tag set: {:?}", tag_set);

        counter!("rustfs.get_object_tagging.success").increment(1);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "get").record(duration.as_secs_f64());
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: req.input.version_id.clone(),
        }))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn get_object_torrent(&self, req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        // Torrent functionality is not implemented in RustFS
        // Per S3 API test expectations, return 404 NoSuchKey (not 501 Not Implemented)
        // This allows clients to gracefully handle the absence of torrent support
        record_s3_op(S3Operation::GetObjectTorrent, &req.input.bucket);
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_head_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_head_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // List buckets not associated with a bucket, give it bucket label "*" to denote "all".
        record_s3_op(S3Operation::ListBuckets, "*");
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

    async fn list_object_versions_m(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsMOutput>> {
        record_s3_op(S3Operation::ListObjectVersions, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_object_versions_m(req).await
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
    async fn list_objects_v2m(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2MOutput>> {
        record_s3_op(S3Operation::ListObjectsV2, &req.input.bucket);
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_list_objects_v2m(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        record_s3_op(S3Operation::ListParts, &req.input.bucket);
        let usecase = DefaultMultipartUsecase::from_global();
        usecase.execute_list_parts(req).await
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_acl(req).await
    }

    async fn put_bucket_accelerate_configuration(
        &self,
        req: S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketAccelerateConfigurationOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let accelerate_config = serialize(&req.input.accelerate_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        metadata_sys::update(&req.input.bucket, BUCKET_ACCELERATE_CONFIG, accelerate_config)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketAccelerateConfigurationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
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

        match metadata_sys::get_logging_config(&req.input.bucket).await {
            Ok((logging, _)) => Ok(S3Response::new(GetBucketLoggingOutput {
                logging_enabled: logging.logging_enabled,
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketLoggingOutput::default())),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
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

        let logging_config = serialize(&req.input.bucket_logging_status)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        metadata_sys::update(&req.input.bucket, BUCKET_LOGGING_CONFIG, logging_config)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketLoggingOutput::default()))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_lifecycle_configuration(req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_notification_configuration(req).await
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_policy(req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_replication(req).await
    }

    async fn put_bucket_request_payment(
        &self,
        req: S3Request<PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<PutBucketRequestPaymentOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let payment_config = serialize(&req.input.request_payment_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        metadata_sys::update(&req.input.bucket, BUCKET_REQUEST_PAYMENT_CONFIG, payment_config)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketRequestPaymentOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let usecase = DefaultBucketUsecase::from_global();
        usecase.execute_put_bucket_versioning(req).await
    }

    async fn put_bucket_website(&self, req: S3Request<PutBucketWebsiteInput>) -> S3Result<S3Response<PutBucketWebsiteOutput>> {
        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let website_config = serialize(&req.input.website_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        metadata_sys::update(&req.input.bucket, BUCKET_WEBSITE_CONFIG, website_config)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketWebsiteOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
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
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_legal_hold(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_lock_configuration(req).await
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_retention(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_put_object_tagging(req).await
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_restore_object(req).await
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
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
