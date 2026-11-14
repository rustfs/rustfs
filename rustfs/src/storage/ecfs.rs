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

use super::{StorageError, decode_tags_to_map, is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found};
use crate::app::{Answered, RequestEnvelope};
use crate::error::ApiErrorDiagnostic;
use crate::storage::s3_api;
use crate::storage::storage_api::ecfs_consumer::contract::object::ObjectOperations as _;
use crate::storage::storage_api::runtime_sources_consumer::{ECStore, runtime_sources};
use http::StatusCode;
use rustfs_gateway_types::compat::ConversionError;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::list_buckets;
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use std::fmt::Debug;
use std::future::Future;
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_OBJECT: &str = "object";
const EVENT_OBJECT_INTERNAL_ERROR: &str = "object_internal_error";

fn log_object_internal_error(operation: &'static str, request_id: Option<&str>, error: &S3Error) {
    if error.code() != &S3ErrorCode::InternalError {
        return;
    }
    // Generated S3 adapters serialize handler errors without retaining their
    // typed sources in logs. Keep only bounded classifications at this boundary.
    let diagnostic = ApiErrorDiagnostic::from_error(error);
    error!(
        event = EVENT_OBJECT_INTERNAL_ERROR,
        component = LOG_COMPONENT_STORAGE,
        subsystem = LOG_SUBSYSTEM_OBJECT,
        result = "error",
        operation,
        request_id = request_id.unwrap_or("unavailable"),
        storage_error_code = ?diagnostic.storage_code,
        io_error_kind = ?diagnostic.io_kind,
        io_os_error = diagnostic.io_os_error,
        rpc_error_code = ?diagnostic.rpc_code,
        pool_metadata_reason = diagnostic.pool_metadata.map(|context| context.reason),
        pool_metadata_phase = diagnostic.pool_metadata.map(|context| context.phase),
        source_chain_truncated = diagnostic.truncated,
        "Object request returned an internal error"
    );
}

use crate::storage::storage_api::ecfs_consumer::StorageObjectOptions as ObjectOptions;

#[derive(Debug, Clone)]
pub struct FS {
    /// This server's late-bound application-context slot (backlog#1052 S2).
    ///
    /// Handlers resolve the object store through this slot instead of the
    /// ambient process singleton, so each server dispatches requests to its
    /// own store once contexts become per-server. An uninstalled slot falls
    /// back to the global `AppContext` — the single-instance legacy default.
    server_ctx: std::sync::Arc<runtime_sources::ServerContextSlot>,
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
        Self::with_server_ctx(runtime_sources::ServerContextSlot::new())
    }

    /// Build the service bound to an explicit per-server context slot
    /// (backlog#1052 S2). [`FS::new`] hands out a fresh, never-installed slot,
    /// which resolves through the global fallback — the legacy behavior.
    pub(crate) fn with_server_ctx(server_ctx: std::sync::Arc<runtime_sources::ServerContextSlot>) -> Self {
        rustfs_io_metrics::init_s3_metrics();
        rustfs_io_metrics::init_list_objects_metrics();
        Self { server_ctx }
    }

    /// This server's request-path context slot (backlog#1052 S2/S6).
    pub(crate) fn server_ctx(&self) -> &std::sync::Arc<runtime_sources::ServerContextSlot> {
        &self.server_ctx
    }

    pub async fn get_object_tag_conditions_for_policy(
        &self,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> S3Result<std::collections::HashMap<String, Vec<String>>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Ok(std::collections::HashMap::new());
        };
        Self::get_object_tag_conditions_for_policy_from_store(store.as_ref(), bucket, object, version_id).await
    }

    pub(crate) async fn get_object_tag_conditions_for_policy_from_store(
        store: &ECStore,
        bucket: &str,
        object: &str,
        version_id: Option<&str>,
    ) -> S3Result<std::collections::HashMap<String, Vec<String>>> {
        let opts = ObjectOptions {
            version_id: version_id.map(String::from),
            ..Default::default()
        };
        let tags = match store.get_object_tags(bucket, object, &opts).await {
            Ok(t) => t,
            Err(e) => {
                if is_err_object_not_found(&e) || is_err_version_not_found(&e) || is_err_bucket_not_found(&e) {
                    debug!(
                        target: "rustfs::storage::ecfs",
                        bucket = %bucket,
                        object = %object,
                        version_id = ?version_id,
                        error = %e,
                        "object, version, or bucket not found when fetching tags for policy; treating as no tags"
                    );
                    return Ok(std::collections::HashMap::new());
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
        if vid == "null" {
            // A nil UUID selects the stored null version; None selects latest.
            return Ok(Some(Uuid::nil()));
        }
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

pub(crate) fn propagate_object_lock_peer_reload(result: std::result::Result<(), StorageError>) -> S3Result<()> {
    result.map_err(|err| {
        S3Error::with_message(S3ErrorCode::InternalError, format!("Failed to publish Object Lock metadata: {err}"))
    })
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

/// Runs a use case that speaks the request envelope and the gateway's types behind this edge: the
/// input and the output cross the gateway's compatibility seam, and nothing else is converted.
pub(in crate::storage) async fn through_gateway_seam<I, GatewayInput, GatewayOutput, O, F>(
    req: S3Request<I>,
    input_from_s3s: fn(I) -> Result<GatewayInput, ConversionError>,
    output_to_s3s: fn(GatewayOutput) -> Result<O, ConversionError>,
    use_case: impl FnOnce(RequestEnvelope, GatewayInput) -> F,
) -> S3Result<S3Response<O>>
where
    F: Future<Output = rustfs_s3_types::S3Result<GatewayOutput>>,
{
    through_gateway_seam_answered(req, input_from_s3s, output_to_s3s, |envelope, input| {
        let answer = use_case(envelope, input);
        async move { answer.await.map(Answered::new) }
    })
    .await
}

/// [`through_gateway_seam`] for a use case that answers a status or response headers of its own:
/// both are written onto the response unchanged.
pub(in crate::storage) async fn through_gateway_seam_answered<I, GatewayInput, GatewayOutput, O, F>(
    req: S3Request<I>,
    input_from_s3s: fn(I) -> Result<GatewayInput, ConversionError>,
    output_to_s3s: fn(GatewayOutput) -> Result<O, ConversionError>,
    use_case: impl FnOnce(RequestEnvelope, GatewayInput) -> F,
) -> S3Result<S3Response<O>>
where
    F: Future<Output = rustfs_s3_types::S3Result<Answered<GatewayOutput>>>,
{
    let envelope = RequestEnvelope::from_s3s(&req);
    let input = input_from_s3s(req.input).map_err(seam_error)?;
    let Answered { output, status, headers } = use_case(envelope, input).await?;
    let mut response = output_to_s3s(output).map(S3Response::new).map_err(seam_error)?;
    response.status = status;
    response.headers = headers;
    Ok(response)
}

fn seam_error(error: ConversionError) -> S3Error {
    crate::app::dto_values::seam_error(error).into()
}

// The bodies of the methods below, one file per migration batch (rustfs/backlog#2765).
#[path = "legacy_edge/mod.rs"]
mod legacy_edge;
#[cfg(test)]
pub(crate) use legacy_edge::list_objects_v2_through_legacy_edge;

#[async_trait::async_trait]
impl S3 for FS {
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.abort_multipart_upload_edge(req).await
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.complete_multipart_upload_edge(req).await
    }

    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        self.copy_object_edge(req).await
    }

    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        self.create_bucket_edge(req).await
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        self.create_multipart_upload_edge(req).await
    }

    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.delete_bucket_edge(req).await
    }

    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        self.delete_bucket_cors_edge(req).await
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        self.delete_bucket_encryption_edge(req).await
    }

    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        self.delete_bucket_lifecycle_edge(req).await
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        self.delete_bucket_policy_edge(req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        self.delete_bucket_replication_edge(req).await
    }

    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        self.delete_bucket_tagging_edge(req).await
    }

    async fn delete_bucket_website(
        &self,
        req: S3Request<DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<DeleteBucketWebsiteOutput>> {
        self.delete_bucket_website_edge(req).await
    }

    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        self.delete_public_access_block_edge(req).await
    }

    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.delete_object_edge(req).await
    }

    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        self.delete_object_tagging_edge(req).await
    }

    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.delete_objects_edge(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        self.get_bucket_acl_edge(req).await
    }

    async fn get_bucket_accelerate_configuration(
        &self,
        req: S3Request<GetBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketAccelerateConfigurationOutput>> {
        self.get_bucket_accelerate_configuration_edge(req).await
    }

    async fn get_bucket_cors(&self, req: S3Request<GetBucketCorsInput>) -> S3Result<S3Response<GetBucketCorsOutput>> {
        self.get_bucket_cors_edge(req).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        self.get_bucket_encryption_edge(req).await
    }

    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        self.get_bucket_lifecycle_configuration_edge(req).await
    }

    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        self.get_bucket_location_edge(req).await
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        self.get_bucket_notification_configuration_edge(req).await
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        self.get_bucket_policy_edge(req).await
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        self.get_bucket_policy_status_edge(req).await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        self.get_bucket_replication_edge(req).await
    }

    async fn get_bucket_request_payment(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        self.get_bucket_request_payment_edge(req).await
    }

    async fn get_bucket_tagging(&self, req: S3Request<GetBucketTaggingInput>) -> S3Result<S3Response<GetBucketTaggingOutput>> {
        self.get_bucket_tagging_edge(req).await
    }

    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        self.get_public_access_block_edge(req).await
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        self.get_bucket_versioning_edge(req).await
    }

    async fn get_bucket_website(&self, req: S3Request<GetBucketWebsiteInput>) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        self.get_bucket_website_edge(req).await
    }

    async fn get_object(&self, req: S3Request<GetObjectInput>) -> S3Result<S3Response<GetObjectOutput>> {
        self.get_object_edge(req).await
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        self.get_object_acl_edge(req).await
    }

    async fn get_object_attributes(
        &self,
        req: S3Request<GetObjectAttributesInput>,
    ) -> S3Result<S3Response<GetObjectAttributesOutput>> {
        self.get_object_attributes_edge(req).await
    }

    async fn get_object_legal_hold(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        self.get_object_legal_hold_edge(req).await
    }

    async fn get_object_lock_configuration(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        self.get_object_lock_configuration_edge(req).await
    }

    async fn get_object_retention(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        self.get_object_retention_edge(req).await
    }

    async fn get_object_tagging(&self, req: S3Request<GetObjectTaggingInput>) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        self.get_object_tagging_edge(req).await
    }

    async fn get_object_torrent(&self, _req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        self.get_object_torrent_edge(_req).await
    }

    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        self.head_bucket_edge(req).await
    }

    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        self.head_object_edge(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // List buckets not associated with a bucket, give it bucket label "*" to denote "all".
        record_s3_op(S3Operation::ListBuckets);
        let usecase = s3_api::bucket_usecase_for(self);
        let mut response =
            through_gateway_seam(req, list_buckets::input_from_s3s, list_buckets::output_to_s3s, |envelope, input| {
                usecase.execute_list_buckets(envelope, input)
            })
            .await?;
        // The seam writes a listing without buckets as an absent member. This edge has always
        // answered the element, empty or not, and clients index the answer by it.
        response.output.buckets.get_or_insert_with(Vec::new);
        Ok(response)
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        self.list_multipart_uploads_edge(req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        self.list_object_versions_edge(req).await
    }

    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        self.list_objects_edge(req).await
    }

    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.list_objects_v2_edge(req).await
    }

    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        self.list_parts_edge(req).await
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        self.put_bucket_acl_edge(req).await
    }

    async fn put_bucket_accelerate_configuration(
        &self,
        req: S3Request<PutBucketAccelerateConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketAccelerateConfigurationOutput>> {
        self.put_bucket_accelerate_configuration_edge(req).await
    }

    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        self.put_bucket_cors_edge(req).await
    }

    async fn get_bucket_logging(&self, req: S3Request<GetBucketLoggingInput>) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        self.get_bucket_logging_edge(req).await
    }

    async fn put_bucket_logging(&self, req: S3Request<PutBucketLoggingInput>) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        self.put_bucket_logging_edge(req).await
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        self.put_bucket_encryption_edge(req).await
    }

    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        self.put_bucket_lifecycle_configuration_edge(req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        self.put_bucket_notification_configuration_edge(req).await
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        self.put_bucket_policy_edge(req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        self.put_bucket_replication_edge(req).await
    }

    async fn put_bucket_request_payment(
        &self,
        req: S3Request<PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<PutBucketRequestPaymentOutput>> {
        self.put_bucket_request_payment_edge(req).await
    }

    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        self.put_public_access_block_edge(req).await
    }

    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        self.put_bucket_tagging_edge(req).await
    }

    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        self.put_bucket_versioning_edge(req).await
    }

    async fn put_bucket_website(&self, req: S3Request<PutBucketWebsiteInput>) -> S3Result<S3Response<PutBucketWebsiteOutput>> {
        self.put_bucket_website_edge(req).await
    }

    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        self.put_object_edge(req).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        self.put_object_acl_edge(req).await
    }

    async fn put_object_legal_hold(
        &self,
        req: S3Request<PutObjectLegalHoldInput>,
    ) -> S3Result<S3Response<PutObjectLegalHoldOutput>> {
        self.put_object_legal_hold_edge(req).await
    }

    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        self.put_object_lock_configuration_edge(req).await
    }

    async fn put_object_retention(
        &self,
        req: S3Request<PutObjectRetentionInput>,
    ) -> S3Result<S3Response<PutObjectRetentionOutput>> {
        self.put_object_retention_edge(req).await
    }

    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        self.put_object_tagging_edge(req).await
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        self.restore_object_edge(req).await
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        self.select_object_content_edge(req).await
    }

    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        self.upload_part_edge(req).await
    }

    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        self.upload_part_copy_edge(req).await
    }
}

/// Every mutating S3 operation that has no bucket notification event is audited at exactly one
/// place: either its legacy edge wraps it (`audit_only_operation(req, S3Operation::X, …)` in this
/// file or in `legacy_edge/`), or, once it is migrated, its use case under `src/app/` opens the
/// owned audit (`audit_only_operation(envelope, S3Operation::X, …)` on `RequestEnvelope`, or
/// `OperationHelper::audit_only_from_view(&envelope.audit_view(), S3Operation::X)`), so both
/// stacks write the same record. Neither place leaves the operation unaudited; both would audit
/// it twice. The calls are matched as text with whitespace ignored.
#[cfg(test)]
mod audit_coverage_tests {
    use crate::guard_sources::SourceSet;
    use rustfs_s3_ops::S3Operation;

    fn is_read_only(op: S3Operation) -> bool {
        let name = op.as_str().trim_start_matches("s3:");
        ["Get", "Head", "List", "Select"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
    }

    /// The legacy wrappers and the owned audit calls of `op`, in that order.
    fn audit_sites(legacy_edge: &SourceSet, use_cases: &SourceSet, op: S3Operation) -> (usize, usize) {
        let legacy = legacy_edge.count_ignoring_whitespace(&format!("audit_only_operation(req, S3Operation::{op:?},"));
        // The helper's last argument is followed by `)`, or by `,` when rustfmt lays it out vertically.
        let helper = format!("OperationHelper::audit_only_from_view(&envelope.audit_view(), S3Operation::{op:?}");
        let owned = use_cases.count_ignoring_whitespace(&format!("audit_only_operation(envelope, S3Operation::{op:?},"))
            + use_cases.count_ignoring_whitespace(&format!("{helper})"))
            + use_cases.count_ignoring_whitespace(&format!("{helper},)"));
        (legacy, owned)
    }

    #[test]
    fn mutating_operations_without_a_notification_event_are_audited_once() {
        let mut expected: Vec<S3Operation> = S3Operation::ALL
            .into_iter()
            .filter(|op| !is_read_only(*op) && op.to_event_name().is_none())
            .collect();
        // AbortMultipartUpload has an internal event but no handler-built helper.
        expected.push(S3Operation::AbortMultipartUpload);
        assert_eq!(expected.len(), 22, "an operation was added or removed: {expected:?}");

        let legacy_edge = SourceSet::production("src/storage/ecfs.rs", "src/storage/legacy_edge");
        let use_cases = SourceSet::production("src/app/mod.rs", "src/app");
        for op in expected {
            let (legacy, owned) = audit_sites(&legacy_edge, &use_cases, op);
            assert_eq!(
                legacy + owned,
                1,
                "{} must be audited at exactly one place; legacy edge wrappers: {legacy} in {}, owned use-case calls: {owned} in {}",
                op.as_str(),
                legacy_edge.describe(),
                use_cases.describe()
            );
        }
    }

    /// A package root holding one legacy edge file and one use-case file.
    fn fixture(edge: &str, use_case: &str) -> (tempfile::TempDir, SourceSet, SourceSet) {
        let root = tempfile::tempdir().expect("fixture root");
        for (path, text) in [
            ("src/storage/ecfs.rs", edge),
            ("src/app/mod.rs", ""),
            ("src/app/upload_part.rs", use_case),
        ] {
            let path = root.path().join(path);
            std::fs::create_dir_all(path.parent().expect("parent")).expect("fixture dir");
            std::fs::write(path, text).expect("fixture file");
        }
        let edge = SourceSet::production_under(root.path(), "src/storage/ecfs.rs", "src/storage/legacy_edge");
        let use_cases = SourceSet::production_under(root.path(), "src/app/mod.rs", "src/app");
        (root, edge, use_cases)
    }

    const LEGACY: &str = "audit_only_operation(req, S3Operation::UploadPart, |req| usecase.run(req)).await";
    const OWNED_HELPER: &str =
        "let helper = OperationHelper::audit_only_from_view(\n    &envelope.audit_view(),\n    S3Operation::UploadPart,\n);";
    const OWNED_WRAPPER: &str =
        "RequestEnvelope::audit_only_operation(\n    envelope,\n    S3Operation::UploadPart,\n    StatusCode::OK,\n    run,\n)";

    #[test]
    fn the_guard_accepts_exactly_one_audit_place() {
        for (edge, use_case) in [(LEGACY, ""), ("", OWNED_HELPER), ("", OWNED_WRAPPER)] {
            let (_root, edge, use_cases) = fixture(edge, use_case);
            let (legacy, owned) = audit_sites(&edge, &use_cases, S3Operation::UploadPart);
            assert_eq!(legacy + owned, 1, "legacy {legacy}, owned {owned}");
        }
    }

    #[test]
    fn the_guard_refuses_an_operation_audited_nowhere_or_twice() {
        let wrapper_in_test_only = format!("#[cfg(test)]\nfn t() {{\n    {OWNED_HELPER}\n}}\n");
        for (edge, use_case) in [
            ("", ""),
            (LEGACY, OWNED_HELPER),
            (LEGACY, OWNED_WRAPPER),
            ("", wrapper_in_test_only.as_str()),
            ("audit_only_operation(req, S3Operation::UploadPartCopy, run)", ""),
            (
                "",
                "OperationHelper::audit_only_from_view(&envelope.audit_view(), S3Operation::UploadPartCopy)",
            ),
        ] {
            let (_root, edge, use_cases) = fixture(edge, use_case);
            let (legacy, owned) = audit_sites(&edge, &use_cases, S3Operation::UploadPart);
            assert_ne!(legacy + owned, 1, "legacy {legacy}, owned {owned}");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::validate_website_configuration;
    use http::StatusCode;
    use rustfs_gateway_types::persistence::{
        PersistedIndexDocument as IndexDocument, PersistedRedirect as Redirect, PersistedRoutingRule as RoutingRule,
        PersistedRoutingRuleCondition as Condition, PersistedWebsiteConfiguration as WebsiteConfiguration,
    };
    use s3s::{S3Error, S3ErrorCode};

    #[test]
    fn object_internal_error_logs_only_correlated_bounded_classifications() {
        #[derive(Clone, Default)]
        struct Writer(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Writer {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().expect("log buffer").extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Writer {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }
        let writer = Writer::default();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_writer(writer.clone())
            .finish();
        let mut status = tonic::Status::internal("secret source message");
        status
            .metadata_mut()
            .insert("authorization", "secret-token".parse().expect("metadata"));
        let error: S3Error = crate::error::ApiError::from(crate::storage::StorageError::from(status)).into();
        let original_code = error.code().clone();
        let original_status = error.status_code();
        let original_message = error.message().map(str::to_owned);
        tracing::subscriber::with_default(subscriber, || {
            super::log_object_internal_error("GetObject", Some("server-owned-request"), &error);
            let mut opaque = S3Error::with_message(S3ErrorCode::InternalError, "secret outer message");
            opaque.set_source(Box::new(std::io::Error::other("secret inner message")));
            super::log_object_internal_error("DeleteObject", None, &opaque);
            let unavailable: S3Error = crate::error::ApiError::from(crate::storage::StorageError::from(
                tonic::Status::unavailable("secret transport message"),
            ))
            .into();
            assert_eq!(unavailable.code(), &S3ErrorCode::ServiceUnavailable);
            assert_eq!(unavailable.status_code(), Some(StatusCode::SERVICE_UNAVAILABLE));
            super::log_object_internal_error("GetObject", None, &unavailable);
        });
        let output = String::from_utf8(writer.0.lock().expect("log buffer").clone()).expect("UTF8 log");
        assert!(!output.contains("secret"));
        assert!(!output.contains("authorization"));
        let rows = output
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("JSON log"))
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert_eq!(row["fields"]["event"], "object_internal_error");
            assert_eq!(row["fields"]["source_chain_truncated"], false);
        }
        assert_eq!(rows[0]["fields"]["request_id"], "server-owned-request");
        assert_eq!(rows[0]["fields"]["rpc_error_code"], "Some(Internal)");
        assert_eq!(rows[1]["fields"]["request_id"], "unavailable");
        assert_eq!(rows[1]["fields"]["io_error_kind"], "Some(Other)");
        assert_eq!(error.code(), &original_code);
        assert_eq!(error.status_code(), original_status);
        assert_eq!(error.message(), original_message.as_deref());
    }

    #[test]
    fn website_config_rejects_missing_index_and_invalid_redirect_rules() {
        assert!(validate_website_configuration(&WebsiteConfiguration::default()).is_err());
        let config = WebsiteConfiguration {
            index_document: Some(IndexDocument {
                suffix: "index.html".to_owned(),
            }),
            ..Default::default()
        };
        assert!(validate_website_configuration(&config).is_ok());
        let mut bad = config;
        bad.routing_rules = Some(vec![RoutingRule {
            condition: Some(Condition::default()),
            redirect: Redirect {
                host_name: Some("other.test".to_owned()),
                ..Default::default()
            },
        }]);
        assert!(validate_website_configuration(&bad).is_err());
        bad.routing_rules = Some(vec![RoutingRule {
            condition: Some(Condition {
                key_prefix_equals: Some("old/".to_owned()),
                ..Default::default()
            }),
            redirect: Redirect {
                protocol: Some("ftp".to_owned()),
                ..Default::default()
            },
        }]);
        assert!(validate_website_configuration(&bad).is_err());
    }
}
