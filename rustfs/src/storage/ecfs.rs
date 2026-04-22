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
use crate::storage::access::has_bypass_governance_header;
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_opts;
use crate::storage::s3_api::acl;
use crate::storage::{parse_object_lock_legal_hold, parse_object_lock_retention, validate_bucket_object_lock_enabled};
use http::StatusCode;
use metrics::{counter, histogram};
use rustfs_ecstore::{
    bucket::{
        metadata::{
            BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_VERSIONING_CONFIG,
            BUCKET_WEBSITE_CONFIG, OBJECT_LOCK_CONFIG,
        },
        metadata_sys,
        object_lock::objectlock_sys::check_retention_for_modification,
        tagging::{decode_tags, decode_tags_to_map, encode_tags},
        utils::serialize,
        versioning::VersioningApi,
        versioning_sys::BucketVersioningSys,
    },
    error::{StorageError, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found},
    new_object_layer_fn,
    store_api::{BucketOperations, BucketOptions, ObjectLockRetentionOptions, ObjectOperations, ObjectOptions},
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

const MAXIMUM_RETENTION_DAYS: i32 = 36_500;
const MAXIMUM_RETENTION_YEARS: i32 = 100;

fn invalid_object_lock_configuration(message: impl Into<String>) -> S3Error {
    S3Error::with_message(S3ErrorCode::MalformedXML, message.into())
}

fn invalid_retention_period(message: impl Into<String>) -> S3Error {
    let mut err = S3Error::with_message(S3ErrorCode::Custom("InvalidRetentionPeriod".into()), message.into());
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

fn validate_default_retention_configuration(default_retention: &DefaultRetention) -> S3Result<()> {
    let Some(mode) = default_retention.mode.as_ref() else {
        return Err(invalid_object_lock_configuration("retention mode must be specified"));
    };

    match mode.as_str() {
        ObjectLockRetentionMode::COMPLIANCE | ObjectLockRetentionMode::GOVERNANCE => {}
        _ => {
            return Err(invalid_object_lock_configuration(format!("unknown retention mode {}", mode.as_str())));
        }
    }

    match (default_retention.days, default_retention.years) {
        (Some(days), None) => {
            if days <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Days'",
                ));
            }
            if days > MAXIMUM_RETENTION_DAYS {
                return Err(invalid_retention_period(format!("Default retention period too large for 'Days' {days}",)));
            }
        }
        (None, Some(years)) => {
            if years <= 0 {
                return Err(invalid_retention_period(
                    "Default retention period must be a positive integer value for 'Years'",
                ));
            }
            if years > MAXIMUM_RETENTION_YEARS {
                return Err(invalid_retention_period(format!(
                    "Default retention period too large for 'Years' {years}",
                )));
            }
        }
        (Some(_), Some(_)) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified, not both"));
        }
        (None, None) => {
            return Err(invalid_object_lock_configuration("either Days or Years must be specified"));
        }
    }

    Ok(())
}

pub(crate) fn validate_object_lock_configuration_input(input_cfg: &ObjectLockConfiguration) -> S3Result<()> {
    let enabled = input_cfg.object_lock_enabled.as_ref().map(ObjectLockEnabled::as_str);
    if enabled != Some(ObjectLockEnabled::ENABLED) {
        return Err(invalid_object_lock_configuration(
            "only 'Enabled' value is allowed to ObjectLockEnabled element",
        ));
    }

    if let Some(rule) = input_cfg.rule.as_ref() {
        let Some(default_retention) = rule.default_retention.as_ref() else {
            return Err(invalid_object_lock_configuration("Rule must include DefaultRetention"));
        };
        validate_default_retention_configuration(default_retention)?;
    }

    Ok(())
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
        Box::pin(usecase.execute_complete_multipart_upload(req)).await
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        Box::pin(usecase.execute_copy_object(req)).await
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
        Box::pin(usecase.execute_delete_object(req)).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        record_s3_op(S3Operation::DeleteObjectTagging, &req.input.bucket);
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingDelete, S3Operation::DeleteObjectTagging);
        let DeleteObjectTaggingInput {
            bucket,
            key: object,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            error!("Store not initialized");
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id_for_parse = version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id_for_parse)?.map(Into::into),
            ..Default::default()
        };

        store.delete_object_tags(&bucket, &object, &opts).await.map_err(|e| {
            error!("Failed to delete object tags: {}", e);
            ApiError::from(e)
        })?;

        let event_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!(
                    bucket = %bucket,
                    object = %object,
                    version_id = ?version_id,
                    error = %err,
                    "failed to load object info for delete-object-tagging notification; falling back to request context"
                );
                None
            }
        };

        counter!("rustfs.delete_object_tagging.success").increment(1);

        let event_version_id = version_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| {
                event_object_info
                    .as_ref()
                    .and_then(|info| info.version_id.map(|version_id| version_id.to_string()))
            })
            .unwrap_or_default();
        if let Some(event_object_info) = event_object_info {
            helper = helper.object(event_object_info);
        }
        helper = helper.version_id(event_version_id);

        let result = Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "delete").record(duration.as_secs_f64());
        result
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = DefaultObjectUsecase::from_global();
        usecase.execute_delete_objects(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl, &req.input.bucket);
        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        Ok(S3Response::new(acl::build_get_bucket_acl_output()))
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
        Box::pin(usecase.execute_get_object(req)).await
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
        let GetObjectLockConfigurationInput { bucket, .. } = req.input;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let object_lock_configuration = match metadata_sys::get_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ObjectLockConfigurationNotFoundError,
                        "Object Lock configuration does not exist for this bucket".to_string(),
                    ));
                }
                warn!("get_object_lock_config err {:?}", err);
                return Err(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "Failed to load Object Lock configuration".to_string(),
                ));
            }
        };

        Ok(S3Response::new(GetObjectLockConfigurationOutput {
            object_lock_configuration,
        }))
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
        let PutBucketAclInput {
            bucket,
            access_control_policy,
            ..
        } = req.input;
        record_s3_op(S3Operation::PutBucketAcl, &bucket);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        if access_control_policy.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "ACL XML grants are not supported; use canned ACL headers or omit ACL"
            ));
        }

        Ok(S3Response::new(PutBucketAclOutput::default()))
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
        Box::pin(usecase.execute_put_object(self, req)).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        record_s3_op(S3Operation::PutObjectAcl, &req.input.bucket);
        let mut helper = OperationHelper::new(&req, EventName::ObjectAclPut, S3Operation::PutObjectAcl);
        let bucket = &req.input.bucket;
        let key = &req.input.key;
        let version_id = req.input.version_id.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let opts: ObjectOptions = get_opts(bucket, key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let object_info = store.get_object_info(bucket, key, &opts).await.map_err(ApiError::from)?;

        if req.input.access_control_policy.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "ACL XML grants are not supported; use canned ACL headers or omit ACL"
            ));
        }

        let event_version_id = version_id
            .or_else(|| object_info.version_id.map(|version_id| version_id.to_string()))
            .unwrap_or_default();
        helper = helper.object(object_info).version_id(event_version_id);

        let result = Ok(S3Response::new(PutObjectAclOutput::default()));
        let _ = helper.complete(&result);
        result
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectCreatedPutLegalHold, S3Operation::PutObjectLegalHold).suppress_event();
        let PutObjectLegalHoldInput {
            bucket,
            key,
            legal_hold,
            version_id,
            ..
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

        let eval_metadata = parse_object_lock_legal_hold(legal_hold)?;

        let popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id,
            eval_metadata: Some(eval_metadata),
            ..Default::default()
        };

        let info = store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_object_lock_configuration_input(&input_cfg)?;

        match metadata_sys::get_object_lock_config(&bucket).await {
            Ok(_) => {}
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    if !BucketVersioningSys::enabled(&bucket).await {
                        return Err(S3Error::with_message(
                            S3ErrorCode::InvalidBucketState,
                            "Object Lock configuration cannot be enabled on existing buckets".to_string(),
                        ));
                    }
                } else {
                    warn!("get_object_lock_config err {:?}", err);
                    return Err(S3Error::with_message(
                        S3ErrorCode::InternalError,
                        "Failed to get bucket ObjectLockConfiguration".to_string(),
                    ));
                }
            }
        };

        let data = serialize(&input_cfg).map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{err}")))?;

        metadata_sys::update(&bucket, OBJECT_LOCK_CONFIG, data)
            .await
            .map_err(ApiError::from)?;

        // When Object Lock is enabled, automatically enable versioning if not already enabled.
        // This matches S3-compatible behavior.
        let versioning_config = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let enable_versioning_config = VersioningConfiguration {
                status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
                ..Default::default()
            };
            let versioning_data = serialize(&enable_versioning_config)
                .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{err}")))?;
            metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, versioning_data)
                .await
                .map_err(ApiError::from)?;
        }

        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectCreatedPutRetention, S3Operation::PutObjectRetention).suppress_event();
        let PutObjectRetentionInput {
            bucket,
            key,
            retention,
            version_id,
            ..
        } = req.input.clone();

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let new_retain_until = retention
            .as_ref()
            .and_then(|r| r.retain_until_date.as_ref())
            .map(|d| OffsetDateTime::from(d.clone()));
        let new_mode = retention
            .as_ref()
            .and_then(|r| r.mode.as_ref())
            .map(|mode| mode.as_str().to_string());

        let bypass_governance = has_bypass_governance_header(&req.headers);
        // Keep the early check for existing response behavior; put_object_metadata
        // repeats the same check after taking the metadata write lock.
        let check_opts: ObjectOptions = get_opts(&bucket, &key, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        if let Ok(existing_obj_info) = store.get_object_info(&bucket, &key, &check_opts).await
            && let Some(block_reason) = check_retention_for_modification(
                &existing_obj_info.user_defined,
                new_mode.as_deref(),
                new_retain_until,
                bypass_governance,
            )
        {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
        }

        let eval_metadata = parse_object_lock_retention(retention)?;

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        opts.eval_metadata = Some(eval_metadata);
        opts.object_lock_retention = Some(ObjectLockRetentionOptions {
            mode: new_mode,
            retain_until: new_retain_until,
            bypass_governance,
        });

        let object_info = store.put_object_metadata(&bucket, &key, &opts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            S3Error::from(ApiError::from(e))
        })?;

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        record_s3_op(S3Operation::PutObjectTagging, &req.input.bucket);
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        crate::storage::s3_api::tagging::validate_object_tag_set(&tagging.tag_set)?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let tags = encode_tags(tagging.tag_set);
        debug!("Encoded tags: {}", tags);

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        store.put_object_tags(&bucket, &object, &tags, &opts).await.map_err(|e| {
            error!("Failed to put object tags: {}", e);
            counter!("rustfs.put_object_tagging.failure").increment(1);
            ApiError::from(e)
        })?;

        let event_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => Some(info),
            Err(err) => {
                warn!(
                    bucket = %bucket,
                    object = %object,
                    version_id = ?req.input.version_id,
                    error = %err,
                    "failed to load object info for put-object-tagging notification; falling back to request context"
                );
                None
            }
        };

        counter!("rustfs.put_object_tagging.success").increment(1);

        let event_version_id = req
            .input
            .version_id
            .as_deref()
            .filter(|version_id| !version_id.is_empty())
            .map(str::to_string)
            .or_else(|| {
                event_object_info
                    .as_ref()
                    .and_then(|info| info.version_id.map(|version_id| version_id.to_string()))
            })
            .unwrap_or_default();
        if let Some(event_object_info) = event_object_info {
            helper = helper.object(event_object_info);
        }
        helper = helper.version_id(event_version_id);

        let result = Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: req.input.version_id.clone(),
        }));
        let _ = helper.complete(&result);
        let duration = start_time.elapsed();
        histogram!("rustfs.object_tagging.operation.duration.seconds", "operation" => "put").record(duration.as_secs_f64());
        result
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
        Box::pin(usecase.execute_upload_part_copy(req)).await
    }
}
