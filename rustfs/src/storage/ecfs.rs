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

use super::StorageVersioningConfigExt as _;
use super::{
    BUCKET_ACCELERATE_CONFIG, BUCKET_LOGGING_CONFIG, BUCKET_REQUEST_PAYMENT_CONFIG, BUCKET_VERSIONING_CONFIG,
    BUCKET_WEBSITE_CONFIG, BucketVersioningSys, OBJECT_LOCK_CONFIG, StorageError, check_retention_for_modification, decode_tags,
    decode_tags_to_map, delete_bucket_metadata_config_if_incarnation, encode_tags, get_bucket_accelerate_config,
    get_bucket_logging_config, get_bucket_object_lock_config, get_bucket_request_payment_config, get_bucket_website_config,
    is_err_bucket_not_found, is_err_object_not_found, is_err_version_not_found, record_replication_proxy, serialize,
    update_bucket_metadata_config_if_incarnation,
};
use crate::admin::handlers::site_replication::site_replication_bucket_meta_hook;
use crate::error::ApiError;
use crate::storage::access::{apply_bucket_generation_guard, bucket_config_mutation_incarnation, has_bypass_governance_header};
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_opts;
use crate::storage::s3_api::{self, acl};
use crate::storage::storage_api::ecfs_consumer::contract::{
    bucket::{BucketOperations, BucketOptions},
    object::{ObjectLockRetentionOptions, ObjectOperations as _},
};
use crate::storage::storage_api::ecfs_consumer::object_lock::{
    parse_object_lock_legal_hold, parse_object_lock_retention, validate_bucket_object_lock_enabled,
};
use crate::storage::storage_api::runtime_sources_consumer::{ECStore, runtime_sources};
use crate::table_catalog;
use http::StatusCode;
use metrics::{counter, histogram};
use rustfs_io_metrics::record_s3_op;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_s3_ops::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::http::headers::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER, AMZ_OBJECT_LOCK_MODE_LOWER, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER,
};
use rustfs_utils::http::{SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, insert_str};
use s3s::{S3, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, dto::*, s3_error};
use std::collections::HashMap;
use std::fmt::Debug;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const LOG_COMPONENT_STORAGE: &str = "storage";
const LOG_SUBSYSTEM_OBJECT: &str = "object";
const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";
const LOG_SUBSYSTEM_TAGGING: &str = "tagging";

use crate::app::storage_api::object_usecase::bucket::replication::{
    OperatorRuleContract, ReplicateDecision, get_read_proxy_targets, must_replicate_metadata, schedule_metadata_replication,
};
use crate::storage::storage_api::ecfs_consumer::StorageObjectOptions as ObjectOptions;

#[cfg(test)]
static SITE_REPLICATION_GATE_TEST_OVERRIDE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);
#[cfg(test)]
const SITE_REPLICATION_GATE_FORCE_DISABLED: u8 = 1;
#[cfg(test)]
const SITE_REPLICATION_GATE_FORCE_ENABLED: u8 = 2;

async fn site_replication_gate_enabled() -> S3Result<bool> {
    #[cfg(test)]
    match SITE_REPLICATION_GATE_TEST_OVERRIDE.load(std::sync::atomic::Ordering::SeqCst) {
        SITE_REPLICATION_GATE_FORCE_DISABLED => return Ok(false),
        SITE_REPLICATION_GATE_FORCE_ENABLED => return Ok(true),
        _ => {}
    }
    crate::admin::handlers::site_replication::site_replication_enabled().await
}

/// Remote site-replication peer deployment ids and the operator-rule contract
/// the peers support, handed to the bucket usecase so an S3 replication-config
/// edit keeps exactly the reconciler-owned rules and merges the way every
/// peer will (issue #1948). Read here, in the interface layer, because the
/// usecase must not import the admin handlers (layer guard); a state-read
/// failure propagates so the edit fails closed.
async fn site_replication_edit_context() -> S3Result<(std::collections::HashSet<String>, OperatorRuleContract)> {
    // While the gate override is in effect the test exercises the deny/allow
    // branch, not the peer set; there is no persisted state to read.
    #[cfg(test)]
    if SITE_REPLICATION_GATE_TEST_OVERRIDE.load(std::sync::atomic::Ordering::SeqCst) != 0 {
        return Ok((std::collections::HashSet::new(), OperatorRuleContract::Derived));
    }
    crate::admin::handlers::site_replication::site_replication_edit_context().await
}

/// MinIO `ErrReplicationDenyEditError`.
fn replication_deny_edit_error() -> S3Error {
    let mut err = S3Error::with_message(
        S3ErrorCode::Custom("XMinioReplicationDenyEdit".into()),
        "Sub-User is not allowed to edit Replication configuration",
    );
    err.set_status_code(StatusCode::BAD_REQUEST);
    err
}

/// Site-replication gate for S3 replication-config edits (issue #1948).
///
/// On a site-replication deployment the bucket's replication config carries
/// the operator-managed `site-repl-*` rules that keep every peer in sync, and
/// a successful edit is broadcast to all peers — so a user holding only
/// bucket-scoped `s3:PutReplicationConfiguration` could rewrite or erase
/// replication net-wide. MinIO parity (`ErrReplicationDenyEditError`): only
/// owner credentials (root or root-parented) may edit. Runs after the policy
/// authorization in the access layer and only on the external S3 path — the
/// reconciler and peer bucket-meta ingestion never route through these
/// handlers.
async fn deny_replication_config_edit_for_non_owner<T>(req: &S3Request<T>) -> S3Result<()> {
    if crate::storage::access::req_info_ref(req)?.is_owner {
        return Ok(());
    }
    if site_replication_gate_enabled().await? {
        return Err(replication_deny_edit_error());
    }
    Ok(())
}

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

    /// Not-found classifier for proxied SDK tagging calls: a raw 404 covers
    /// NoSuchKey and NoSuchVersion alike; the caller silently tries the next
    /// replication target.
    fn proxy_sdk_error_is_not_found<E>(err: &aws_sdk_s3::error::SdkError<E>) -> bool {
        err.raw_response().is_some_and(|resp| resp.status().as_u16() == 404)
    }

    /// Selector options for a tagging proxy. Reuses `get_opts` so the
    /// anti-loop `source-proxy-request` header family and the bucket's
    /// version-suspension state gate proxying exactly like GET/HEAD.
    async fn tagging_proxy_opts(
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        headers: &http::HeaderMap,
    ) -> Option<ObjectOptions> {
        get_opts(bucket, object, version_id, None, headers).await.ok()
    }

    /// Serve a GetObjectTagging for an object missing locally by proxying to
    /// the bucket's replication targets (MinIO `proxyGetTaggingToRepTarget`,
    /// backlog#1675 P1-5). None means no target had the object.
    async fn proxy_get_object_tagging(
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        headers: &http::HeaderMap,
    ) -> Option<TagSet> {
        let opts = Self::tagging_proxy_opts(bucket, object, version_id, headers).await?;
        let targets = get_read_proxy_targets(bucket, object, &opts).await;
        if targets.is_empty() {
            return None;
        }
        for target in targets {
            match target
                .get_object_tagging(&target.bucket, object, opts.version_id.clone())
                .await
            {
                Ok(remote) => {
                    // MinIO-aligned accounting: one total per proxy attempt,
                    // one failed when no target served it.
                    record_replication_proxy(bucket, "GetObjectTagging", false).await;
                    return Some(
                        remote
                            .tag_set
                            .into_iter()
                            .map(|tag| Tag {
                                key: Some(tag.key),
                                value: Some(tag.value),
                            })
                            .collect(),
                    );
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: GetObjectTagging against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "GetObjectTagging", true).await;
        None
    }

    /// Apply a PutObjectTagging for an object missing locally on a
    /// replication target (MinIO `proxyTaggingToRepTarget`).
    async fn proxy_put_object_tagging(
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        headers: &http::HeaderMap,
        tag_set: &TagSet,
    ) -> Option<()> {
        let opts = Self::tagging_proxy_opts(bucket, object, version_id, headers).await?;
        let mut tagging = aws_sdk_s3::types::Tagging::builder();
        for tag in tag_set {
            let sdk_tag = aws_sdk_s3::types::Tag::builder()
                .key(tag.key.clone().unwrap_or_default())
                .value(tag.value.clone().unwrap_or_default())
                .build()
                .ok()?;
            tagging = tagging.tag_set(sdk_tag);
        }
        let tagging = tagging.build().ok()?;
        let targets = get_read_proxy_targets(bucket, object, &opts).await;
        if targets.is_empty() {
            return None;
        }
        for target in targets {
            match target
                .put_object_tagging(&target.bucket, object, opts.version_id.clone(), tagging.clone())
                .await
            {
                Ok(_) => {
                    // MinIO-aligned accounting: one total per proxy attempt,
                    // one failed when no target served it.
                    record_replication_proxy(bucket, "PutObjectTagging", false).await;
                    return Some(());
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: PutObjectTagging against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "PutObjectTagging", true).await;
        None
    }

    /// Apply a DeleteObjectTagging for an object missing locally on a
    /// replication target (MinIO `proxyTaggingToRepTarget`).
    async fn proxy_delete_object_tagging(
        bucket: &str,
        object: &str,
        version_id: Option<String>,
        headers: &http::HeaderMap,
    ) -> Option<()> {
        let opts = Self::tagging_proxy_opts(bucket, object, version_id, headers).await?;
        let targets = get_read_proxy_targets(bucket, object, &opts).await;
        if targets.is_empty() {
            return None;
        }
        for target in targets {
            match target
                .delete_object_tagging(&target.bucket, object, opts.version_id.clone())
                .await
            {
                Ok(_) => {
                    // MinIO-aligned accounting: one total per proxy attempt,
                    // one failed when no target served it.
                    record_replication_proxy(bucket, "DeleteObjectTagging", false).await;
                    return Some(());
                }
                Err(err) if Self::proxy_sdk_error_is_not_found(&err) => {
                    debug!(bucket, object, arn = %target.arn, "tagging proxy: target does not have the object");
                }
                Err(err) => {
                    warn!(bucket, object, arn = %target.arn, error = %err, "tagging proxy: DeleteObjectTagging against replication target failed");
                }
            }
        }
        record_replication_proxy(bucket, "DeleteObjectTagging", true).await;
        None
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
        let uuid = Uuid::parse_str(&vid).map_err(|e| {
            error!("Invalid version ID: {}", e);
            s3_error!(InvalidArgument, "Invalid version ID")
        })?;
        Ok(Some(uuid))
    } else {
        Ok(None)
    }
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
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

#[async_trait::async_trait]
impl S3 for FS {
    #[instrument(level = "debug", skip(self))]
    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_abort_multipart_upload(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        crate::hp_guard!("S3::complete_multipart_upload");
        let usecase = s3_api::multipart_usecase_for(self);
        Box::pin(usecase.execute_complete_multipart_upload(req)).await
    }

    /// Copy an object from one location to another
    #[instrument(level = "debug", skip(self, req))]
    async fn copy_object(&self, req: S3Request<CopyObjectInput>) -> S3Result<S3Response<CopyObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_copy_object(req).await
    }

    #[instrument(
        level = "debug",
        skip(self, req),
        fields(start_time=?time::OffsetDateTime::now_utc())
    )]
    async fn create_bucket(&self, req: S3Request<CreateBucketInput>) -> S3Result<S3Response<CreateBucketOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_create_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        crate::hp_guard!("S3::create_multipart_upload");
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_create_multipart_upload(req).await
    }

    /// Delete a bucket
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_bucket(&self, req: S3Request<DeleteBucketInput>) -> S3Result<S3Response<DeleteBucketOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_cors(&self, req: S3Request<DeleteBucketCorsInput>) -> S3Result<S3Response<DeleteBucketCorsOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_cors(req).await
    }

    async fn delete_bucket_encryption(
        &self,
        req: S3Request<DeleteBucketEncryptionInput>,
    ) -> S3Result<S3Response<DeleteBucketEncryptionOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_lifecycle(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_lifecycle(req).await
    }

    async fn delete_bucket_policy(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_policy(req).await
    }

    async fn delete_bucket_replication(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        deny_replication_config_edit_for_non_owner(&req).await?;
        let (site_peers, contract) = site_replication_edit_context().await?;
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_replication(req, site_peers, contract).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_bucket_tagging(
        &self,
        req: S3Request<DeleteBucketTaggingInput>,
    ) -> S3Result<S3Response<DeleteBucketTaggingOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_tagging(req).await
    }

    async fn delete_bucket_website(
        &self,
        req: S3Request<DeleteBucketWebsiteInput>,
    ) -> S3Result<S3Response<DeleteBucketWebsiteOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        delete_bucket_metadata_config_if_incarnation(&req.input.bucket, BUCKET_WEBSITE_CONFIG, expected_incarnation_id)
            .await
            .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(DeleteBucketWebsiteOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_public_access_block(
        &self,
        req: S3Request<DeletePublicAccessBlockInput>,
    ) -> S3Result<S3Response<DeletePublicAccessBlockOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_public_access_block(req).await
    }

    /// Delete an object
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_object(&self, req: S3Request<DeleteObjectInput>) -> S3Result<S3Response<DeleteObjectOutput>> {
        crate::hp_guard!("S3::delete_object");
        let usecase = s3_api::object_usecase_for(self);
        Box::pin(usecase.execute_delete_object(req)).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn delete_object_tagging(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingDelete, S3Operation::DeleteObjectTagging);
        let DeleteObjectTaggingInput {
            bucket,
            key: object,
            version_id,
            ..
        } = req.input.clone();

        validate_table_catalog_object_mutation(&bucket, &object).await?;

        let Some(store) = self.server_ctx.object_store() else {
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_TAGGING,
                event = "object_tagging_store_uninitialized",
                operation = "delete",
                "Object tagging operation failed because storage is not initialized"
            );
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let mut opts = get_opts(&bucket, &object, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let existing_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => info,
            Err(e) => {
                // Replication lag window: apply the tagging delete on a
                // replication target that already has the object
                // (backlog#1675 P1-5). No local object exists, so no bucket
                // notification event is emitted for the proxied write.
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && Self::proxy_delete_object_tagging(&bucket, &object, version_id.clone(), &req.headers)
                        .await
                        .is_some()
                {
                    counter!("rustfs_delete_object_tagging_success").increment(1);
                    let duration = start_time.elapsed();
                    histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "delete")
                        .record(duration.as_secs_f64());
                    return Ok(S3Response::new(DeleteObjectTaggingOutput { version_id }));
                }
                return Err(ApiError::from(e).into());
            }
        };
        let dsc = must_replicate_metadata(
            &bucket,
            &object,
            &existing_object_info.user_defined,
            String::new(),
            existing_object_info.replication_status.clone(),
            opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            let mut eval_metadata = HashMap::new();
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
            insert_str(
                &mut eval_metadata,
                SUFFIX_TAGGING_TIMESTAMP,
                OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default(),
            );
            opts.eval_metadata = Some(eval_metadata);
        }

        let delete_tags_result = store.delete_object_tags(&bucket, &object, &opts).await;
        let object_info = delete_tags_result.map_err(|e| {
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_TAGGING,
                event = "object_tagging_delete_failed",
                bucket = %bucket,
                object = %object,
                error = %e,
                "Failed to delete object tags"
            );
            ApiError::from(e)
        })?;

        let event_object_info = Some(object_info.clone());
        if dsc.replicate_any() {
            schedule_metadata_replication(object_info, store.clone(), dsc).await;
        }

        counter!("rustfs_delete_object_tagging_success").increment(1);

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
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
        let duration = start_time.elapsed();
        histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "delete").record(duration.as_secs_f64());
        result
    }

    /// Delete multiple objects
    #[instrument(level = "debug", skip(self, req))]
    async fn delete_objects(&self, req: S3Request<DeleteObjectsInput>) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_delete_objects(req).await
    }

    async fn get_bucket_acl(&self, req: S3Request<GetBucketAclInput>) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl);
        let GetBucketAclInput { bucket, .. } = req.input;

        let Some(store) = self.server_ctx.object_store() else {
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
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_accelerate_config(&req.input.bucket).await {
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
        record_s3_op(S3Operation::GetBucketCors);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_cors(req).await
    }

    async fn get_bucket_encryption(
        &self,
        req: S3Request<GetBucketEncryptionInput>,
    ) -> S3Result<S3Response<GetBucketEncryptionOutput>> {
        record_s3_op(S3Operation::GetBucketEncryption);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_lifecycle_configuration(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketLifecycleConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_lifecycle_configuration(req).await
    }

    /// Get bucket location
    #[instrument(level = "debug", skip(self, req))]
    async fn get_bucket_location(&self, req: S3Request<GetBucketLocationInput>) -> S3Result<S3Response<GetBucketLocationOutput>> {
        record_s3_op(S3Operation::GetBucketLocation);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_location(req).await
    }

    async fn get_bucket_notification_configuration(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketNotificationConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_notification_configuration(req).await
    }

    async fn get_bucket_policy(&self, req: S3Request<GetBucketPolicyInput>) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        record_s3_op(S3Operation::GetBucketPolicy);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_policy(req).await
    }

    async fn get_bucket_policy_status(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        record_s3_op(S3Operation::GetBucketPolicyStatus);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_policy_status(req).await
    }

    async fn get_bucket_replication(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        record_s3_op(S3Operation::GetBucketReplication);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_replication(req).await
    }

    async fn get_bucket_request_payment(
        &self,
        req: S3Request<GetBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<GetBucketRequestPaymentOutput>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_request_payment_config(&req.input.bucket).await {
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
        record_s3_op(S3Operation::GetBucketTagging);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_public_access_block(
        &self,
        req: S3Request<GetPublicAccessBlockInput>,
    ) -> S3Result<S3Response<GetPublicAccessBlockOutput>> {
        record_s3_op(S3Operation::GetPublicAccessBlock);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        record_s3_op(S3Operation::GetBucketVersioning);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_get_bucket_versioning(req).await
    }

    async fn get_bucket_website(&self, req: S3Request<GetBucketWebsiteInput>) -> S3Result<S3Response<GetBucketWebsiteOutput>> {
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };

        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_website_config(&req.input.bucket).await {
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
        crate::hp_guard!("S3::get_object");
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_get_object(req).await
    }

    async fn get_object_acl(&self, req: S3Request<GetObjectAclInput>) -> S3Result<S3Response<GetObjectAclOutput>> {
        record_s3_op(S3Operation::GetObjectAcl);
        let GetObjectAclInput {
            bucket, key, version_id, ..
        } = req.input;

        let Some(store) = self.server_ctx.object_store() else {
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
        let usecase = s3_api::object_usecase_for(self);
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

        let Some(store) = self.server_ctx.object_store() else {
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
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_OBJECT,
                event = "object_info_load_failed",
                bucket = %bucket,
                object = %key,
                error = %e,
                "Failed to load object info"
            );
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
        record_s3_op(S3Operation::GetObjectLockConfiguration);
        let GetObjectLockConfigurationInput { bucket, .. } = req.input;

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        let object_lock_configuration = match get_bucket_object_lock_config(&bucket).await {
            Ok((cfg, _created)) => Some(cfg),
            Err(err) => {
                if err == StorageError::ConfigNotFound {
                    return Err(S3Error::with_message(
                        S3ErrorCode::ObjectLockConfigurationNotFoundError,
                        "Object Lock configuration does not exist for this bucket".to_string(),
                    ));
                }
                warn!(
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                    event = "object_lock_config_load_failed",
                    bucket = %bucket,
                    error = ?err,
                    "Failed to load bucket object lock configuration"
                );
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

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        validate_bucket_object_lock_enabled(&bucket).await?;

        let opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;

        let object_info = store.get_object_info(&bucket, &key, &opts).await.map_err(|e| {
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_OBJECT,
                event = "object_info_load_failed",
                bucket = %bucket,
                object = %key,
                error = %e,
                "Failed to load object info"
            );
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        let mode = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_MODE_LOWER)
            .map(|v| ObjectLockRetentionMode::from(v.as_str().to_string()));

        let retain_until_date = object_info
            .user_defined
            .get(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE_LOWER)
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
        record_s3_op(S3Operation::GetObjectTagging);
        let start_time = std::time::Instant::now();
        let bucket = req.input.bucket.as_str();
        let object = req.input.key.as_str();

        let Some(store) = self.server_ctx.object_store() else {
            error!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_TAGGING,
                event = "object_tagging_store_uninitialized",
                operation = "get",
                bucket = %bucket,
                object = %object,
                "Object tagging operation failed because storage is not initialized"
            );
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let version_id = req.input.version_id.clone();
        let opts = ObjectOptions {
            version_id: parse_object_version_id(version_id)?.map(Into::into),
            ..Default::default()
        };

        let tags = match store.get_object_tags(bucket, object, &opts).await {
            Ok(tags) => tags,
            Err(e) => {
                // Replication lag window: the object may exist on a
                // replication target even though it is missing locally —
                // proxy the tagging read there (backlog#1675 P1-5).
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && let Some(tag_set) =
                        Self::proxy_get_object_tagging(bucket, object, req.input.version_id.clone(), &req.headers).await
                {
                    counter!("rustfs_get_object_tagging_success").increment(1);
                    let duration = start_time.elapsed();
                    histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "get")
                        .record(duration.as_secs_f64());
                    return Ok(S3Response::new(GetObjectTaggingOutput {
                        tag_set,
                        version_id: req.input.version_id.clone(),
                    }));
                }
                if is_err_object_not_found(&e) {
                    debug!(
                        component = LOG_COMPONENT_STORAGE,
                        subsystem = LOG_SUBSYSTEM_TAGGING,
                        event = "object_tagging_not_found",
                        bucket = %bucket,
                        object = %object,
                        error = %e,
                        "Object tags not found"
                    );
                    return Err(s3_error!(NoSuchKey));
                }
                error!(
                    component = LOG_COMPONENT_STORAGE,
                    subsystem = LOG_SUBSYSTEM_TAGGING,
                    event = "object_tagging_get_failed",
                    bucket = %bucket,
                    object = %object,
                    error = %e,
                    "Failed to load object tags"
                );
                return Err(ApiError::from(e).into());
            }
        };

        let tag_set = decode_tags(tags.as_str());
        debug!(
            component = LOG_COMPONENT_STORAGE,
            subsystem = LOG_SUBSYSTEM_TAGGING,
            event = "object_tagging_decoded",
            bucket = %bucket,
            object = %object,
            tag_count = tag_set.len(),
            "Decoded object tags"
        );

        counter!("rustfs_get_object_tagging_success").increment(1);
        let duration = start_time.elapsed();
        histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "get").record(duration.as_secs_f64());
        Ok(S3Response::new(GetObjectTaggingOutput {
            tag_set,
            version_id: req.input.version_id.clone(),
        }))
    }

    #[instrument(level = "debug", skip(self, _req))]
    async fn get_object_torrent(&self, _req: S3Request<GetObjectTorrentInput>) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        // Torrent functionality is not implemented in RustFS
        // Per S3 API test expectations, return 404 NoSuchKey (not 501 Not Implemented)
        // This allows clients to gracefully handle the absence of torrent support
        record_s3_op(S3Operation::GetObjectTorrent);
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_bucket(&self, req: S3Request<HeadBucketInput>) -> S3Result<S3Response<HeadBucketOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_head_bucket(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn head_object(&self, req: S3Request<HeadObjectInput>) -> S3Result<S3Response<HeadObjectOutput>> {
        crate::hp_guard!("S3::head_object");
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_head_object(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn list_buckets(&self, req: S3Request<ListBucketsInput>) -> S3Result<S3Response<ListBucketsOutput>> {
        // List buckets not associated with a bucket, give it bucket label "*" to denote "all".
        record_s3_op(S3Operation::ListBuckets);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_buckets(req).await
    }

    async fn list_multipart_uploads(
        &self,
        req: S3Request<ListMultipartUploadsInput>,
    ) -> S3Result<S3Response<ListMultipartUploadsOutput>> {
        record_s3_op(S3Operation::ListMultipartUploads);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_list_multipart_uploads(req).await
    }

    async fn list_object_versions(
        &self,
        req: S3Request<ListObjectVersionsInput>,
    ) -> S3Result<S3Response<ListObjectVersionsOutput>> {
        record_s3_op(S3Operation::ListObjectVersions);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_object_versions(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects(&self, req: S3Request<ListObjectsInput>) -> S3Result<S3Response<ListObjectsOutput>> {
        record_s3_op(S3Operation::ListObjects);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_objects(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_objects_v2(&self, req: S3Request<ListObjectsV2Input>) -> S3Result<S3Response<ListObjectsV2Output>> {
        crate::hp_guard!("S3::list_objects_v2");
        record_s3_op(S3Operation::ListObjectsV2);
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_list_objects_v2(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn list_parts(&self, req: S3Request<ListPartsInput>) -> S3Result<S3Response<ListPartsOutput>> {
        record_s3_op(S3Operation::ListParts);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_list_parts(req).await
    }

    async fn put_bucket_acl(&self, req: S3Request<PutBucketAclInput>) -> S3Result<S3Response<PutBucketAclOutput>> {
        let PutBucketAclInput {
            bucket,
            access_control_policy,
            ..
        } = req.input;
        record_s3_op(S3Operation::PutBucketAcl);

        let Some(store) = self.server_ctx.object_store() else {
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
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let accelerate_config = serialize(&req.input.accelerate_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_ACCELERATE_CONFIG,
            accelerate_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketAccelerateConfigurationOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_cors(&self, req: S3Request<PutBucketCorsInput>) -> S3Result<S3Response<PutBucketCorsOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_cors(req).await
    }

    async fn get_bucket_logging(&self, req: S3Request<GetBucketLoggingInput>) -> S3Result<S3Response<GetBucketLoggingOutput>> {
        record_s3_op(S3Operation::GetBucketLogging);
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        match get_bucket_logging_config(&req.input.bucket).await {
            Ok((logging, _)) => Ok(S3Response::new(GetBucketLoggingOutput {
                logging_enabled: logging.logging_enabled,
            })),
            Err(StorageError::ConfigNotFound) => Ok(S3Response::new(GetBucketLoggingOutput::default())),
            Err(err) => Err(crate::error::ApiError::from(err).into()),
        }
    }

    async fn put_bucket_logging(&self, req: S3Request<PutBucketLoggingInput>) -> S3Result<S3Response<PutBucketLoggingOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        record_s3_op(S3Operation::PutBucketLogging);
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let logging_config = serialize(&req.input.bucket_logging_status)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_LOGGING_CONFIG,
            logging_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketLoggingOutput::default()))
    }

    async fn put_bucket_encryption(
        &self,
        req: S3Request<PutBucketEncryptionInput>,
    ) -> S3Result<S3Response<PutBucketEncryptionOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_encryption(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_lifecycle_configuration(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_lifecycle_configuration(req).await
    }

    async fn put_bucket_notification_configuration(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_notification_configuration(req).await
    }

    async fn put_bucket_policy(&self, req: S3Request<PutBucketPolicyInput>) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_policy(req).await
    }

    async fn put_bucket_replication(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        deny_replication_config_edit_for_non_owner(&req).await?;
        let (site_peers, contract) = site_replication_edit_context().await?;
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_replication(req, site_peers, contract).await
    }

    async fn put_bucket_request_payment(
        &self,
        req: S3Request<PutBucketRequestPaymentInput>,
    ) -> S3Result<S3Response<PutBucketRequestPaymentOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let payment_config = serialize(&req.input.request_payment_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_REQUEST_PAYMENT_CONFIG,
            payment_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketRequestPaymentOutput::default()))
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_public_access_block(
        &self,
        req: S3Request<PutPublicAccessBlockInput>,
    ) -> S3Result<S3Response<PutPublicAccessBlockOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_public_access_block(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_tagging(&self, req: S3Request<PutBucketTaggingInput>) -> S3Result<S3Response<PutBucketTaggingOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_tagging(req).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_bucket_versioning(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_put_bucket_versioning(req).await
    }

    async fn put_bucket_website(&self, req: S3Request<PutBucketWebsiteInput>) -> S3Result<S3Response<PutBucketWebsiteOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let Some(store) = self.server_ctx.object_store() else {
            return Err(s3_error!(InternalError, "Not init"));
        };
        store
            .get_bucket_info(&req.input.bucket, &BucketOptions::default())
            .await
            .map_err(crate::error::ApiError::from)?;

        let website_config = serialize(&req.input.website_configuration)
            .map_err(|err| S3Error::with_message(S3ErrorCode::MalformedXML, format!("{err}")))?;
        update_bucket_metadata_config_if_incarnation(
            &req.input.bucket,
            BUCKET_WEBSITE_CONFIG,
            website_config,
            expected_incarnation_id,
        )
        .await
        .map_err(crate::error::ApiError::from)?;

        Ok(S3Response::new(PutBucketWebsiteOutput::default()))
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object(&self, req: S3Request<PutObjectInput>) -> S3Result<S3Response<PutObjectOutput>> {
        crate::hp_guard!("S3::put_object");
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_put_object(self, req).await
    }

    async fn put_object_acl(&self, req: S3Request<PutObjectAclInput>) -> S3Result<S3Response<PutObjectAclOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAclPut, S3Operation::PutObjectAcl);
        let bucket = &req.input.bucket;
        let key = &req.input.key;
        let version_id = req.input.version_id.clone();

        let Some(store) = self.server_ctx.object_store() else {
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

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.server_ctx.object_store() else {
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

        let mut popts = ObjectOptions {
            mod_time: opts.mod_time,
            version_id: opts.version_id.clone(),
            ..Default::default()
        };
        apply_bucket_generation_guard(&req, &bucket, &mut popts)?;

        // PutObjectLegalHold only rewrites metadata, so replication is not scheduled by the
        // object PUT path. Schedule it explicitly, otherwise the legal hold never reaches the
        // replica and the peer copy remains deletable.
        //
        // Mirror the PUT path ordering (see object_usecase::put_object): compute the decision
        // once BEFORE the commit and persist the pending marker with it, so a restart between
        // the commit and the worker write-back leaves a Pending status on disk for the scanner
        // to re-drive. Scheduling without that marker would lose the task silently.
        let dsc = match store.get_object_info(&bucket, &key, &opts).await.ok() {
            Some(info) => {
                must_replicate_metadata(
                    &bucket,
                    &key,
                    &info.user_defined,
                    info.user_tags.as_ref().clone(),
                    popts.delete_marker_replication_status(),
                    popts.clone(),
                )
                .await
            }
            None => ReplicateDecision::new(),
        };

        let mut eval_metadata = parse_object_lock_legal_hold(legal_hold)?;
        if dsc.replicate_any() {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }
        popts.eval_metadata = Some(eval_metadata);

        let info = store.put_object_metadata(&bucket, &key, &popts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            s3_error!(InternalError, "{}", e.to_string())
        })?;

        // The current target transport carries the updated metadata in a full-object PUT.
        if dsc.replicate_any() {
            schedule_metadata_replication(info.clone(), store.clone(), dsc).await;
        }

        let output = PutObjectLegalHoldOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
        result
    }

    #[instrument(level = "debug", skip(self))]
    async fn put_object_lock_configuration(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        let expected_incarnation_id = bucket_config_mutation_incarnation(&req, &req.input.bucket)?;
        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;

        let Some(input_cfg) = object_lock_configuration else { return Err(s3_error!(InvalidArgument)) };

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store
            .get_bucket_info(&bucket, &BucketOptions::default())
            .await
            .map_err(ApiError::from)?;

        validate_object_lock_configuration_input(&input_cfg)?;

        match get_bucket_object_lock_config(&bucket).await {
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
        let object_lock_config =
            String::from_utf8(data.clone()).map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{err}")))?;

        let updated_at = update_bucket_metadata_config_if_incarnation(&bucket, OBJECT_LOCK_CONFIG, data, expected_incarnation_id)
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
            update_bucket_metadata_config_if_incarnation(
                &bucket,
                BUCKET_VERSIONING_CONFIG,
                versioning_data,
                expected_incarnation_id,
            )
            .await
            .map_err(ApiError::from)?;
        }

        if let Some(notification_sys) =
            runtime_sources::current_notification_system_for_context(self.server_ctx.app_context().as_deref())
        {
            propagate_object_lock_peer_reload(notification_sys.load_bucket_metadata(&bucket).await)?;
        }

        if let Err(err) = site_replication_bucket_meta_hook(SRBucketMeta {
            bucket: bucket.clone(),
            r#type: "object-lock-config".to_string(),
            object_lock_config: Some(object_lock_config),
            updated_at: Some(updated_at),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
        .await
        {
            warn!(
                component = LOG_COMPONENT_STORAGE,
                subsystem = LOG_SUBSYSTEM_OBJECT_LOCK,
                event = "put_object_lock_configuration",
                bucket = %bucket,
                result = "site_replication_hook_failed",
                error = ?err,
                "storage object lock state"
            );
        }

        rustfs_scanner::record_dirty_usage_bucket(&bucket);
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

        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.server_ctx.object_store() else {
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

        let existing_obj_info = store.get_object_info(&bucket, &key, &check_opts).await.ok();

        if let Some(existing_obj_info) = existing_obj_info.as_ref()
            && let Some(block_reason) = check_retention_for_modification(
                &existing_obj_info.user_defined,
                new_mode.as_deref(),
                new_retain_until,
                bypass_governance,
            )
        {
            return Err(S3Error::with_message(S3ErrorCode::AccessDenied, block_reason.error_message()));
        }

        let mut opts: ObjectOptions = get_opts(&bucket, &key, version_id, None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        apply_bucket_generation_guard(&req, &bucket, &mut opts)?;
        opts.object_lock_retention = Some(ObjectLockRetentionOptions {
            mode: new_mode,
            retain_until: new_retain_until,
            bypass_governance,
        });

        // PutObjectRetention only rewrites metadata, so the object PUT path that normally
        // computes a replication decision and schedules replication never runs for it. Without
        // scheduling here the peer keeps the previous, unprotected lock state and a
        // WORM-protected object stays deletable on the replica.
        //
        // Mirror the PUT path ordering (see object_usecase::put_object): compute the decision
        // once BEFORE the commit and persist the pending marker with it, so a restart between
        // the commit and the worker write-back leaves a Pending status on disk for the scanner
        // to re-drive. Scheduling without that marker would lose the task silently.
        let dsc = match existing_obj_info.as_ref() {
            Some(info) => {
                must_replicate_metadata(
                    &bucket,
                    &key,
                    &info.user_defined,
                    info.user_tags.as_ref().clone(),
                    opts.delete_marker_replication_status(),
                    opts.clone(),
                )
                .await
            }
            None => ReplicateDecision::new(),
        };

        let mut eval_metadata = parse_object_lock_retention(retention)?;
        if dsc.replicate_any() {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
        }
        opts.eval_metadata = Some(eval_metadata);

        let object_info = store.put_object_metadata(&bucket, &key, &opts).await.map_err(|e| {
            error!("put_object_metadata failed, {}", e.to_string());
            S3Error::from(ApiError::from(e))
        })?;

        // The current target transport carries the updated metadata in a full-object PUT.
        if dsc.replicate_any() {
            schedule_metadata_replication(object_info.clone(), store.clone(), dsc).await;
        }

        let output = PutObjectRetentionOutput {
            request_charged: Some(RequestCharged::from_static(RequestCharged::REQUESTER)),
        };

        let version_id = req.input.version_id.clone().unwrap_or_else(|| Uuid::new_v4().to_string());
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
        result
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn put_object_tagging(&self, req: S3Request<PutObjectTaggingInput>) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        validate_table_catalog_object_mutation(&bucket, &object).await?;

        crate::storage::s3_api::tagging::validate_object_tag_set(&tagging.tag_set)?;

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let tags = encode_tags(tagging.tag_set.clone());
        debug!("Encoded tags: {}", tags);

        let version_id = req.input.version_id.clone();
        let mut opts = get_opts(&bucket, &object, version_id.clone(), None, &req.headers)
            .await
            .map_err(ApiError::from)?;
        let existing_object_info = match store.get_object_info(&bucket, &object, &opts).await {
            Ok(info) => info,
            Err(e) => {
                // Replication lag window: apply the tagging update on a
                // replication target that already has the object
                // (backlog#1675 P1-5). No local object exists, so no bucket
                // notification event is emitted for the proxied write.
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && Self::proxy_put_object_tagging(&bucket, &object, version_id.clone(), &req.headers, &tagging.tag_set)
                        .await
                        .is_some()
                {
                    counter!("rustfs_put_object_tagging_success").increment(1);
                    let duration = start_time.elapsed();
                    histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "put")
                        .record(duration.as_secs_f64());
                    return Ok(S3Response::new(PutObjectTaggingOutput {
                        version_id: req.input.version_id.clone(),
                    }));
                }
                return Err(ApiError::from(e).into());
            }
        };
        let dsc = must_replicate_metadata(
            &bucket,
            &object,
            &existing_object_info.user_defined,
            tags.clone(),
            existing_object_info.replication_status.clone(),
            opts.clone(),
        )
        .await;
        if dsc.replicate_any() {
            let mut eval_metadata = HashMap::new();
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_TIMESTAMP, jiff::Zoned::now().to_string());
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_STATUS, dsc.pending_status().unwrap_or_default());
            insert_str(
                &mut eval_metadata,
                SUFFIX_TAGGING_TIMESTAMP,
                OffsetDateTime::now_utc().format(&Rfc3339).unwrap_or_default(),
            );
            opts.eval_metadata = Some(eval_metadata);
        }

        let put_tags_result = store.put_object_tags(&bucket, &object, &tags, &opts).await;
        let object_info = put_tags_result.map_err(|e| {
            error!("Failed to put object tags: {}", e);
            counter!("rustfs_put_object_tagging_failure").increment(1);
            ApiError::from(e)
        })?;

        let event_object_info = Some(object_info.clone());
        if dsc.replicate_any() {
            schedule_metadata_replication(object_info, store.clone(), dsc).await;
        }

        counter!("rustfs_put_object_tagging_success").increment(1);

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
        rustfs_scanner::record_dirty_usage_bucket(&bucket);
        let duration = start_time.elapsed();
        histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "put").record(duration.as_secs_f64());
        result
    }

    async fn restore_object(&self, req: S3Request<RestoreObjectInput>) -> S3Result<S3Response<RestoreObjectOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_restore_object(req).await
    }

    async fn select_object_content(
        &self,
        req: S3Request<SelectObjectContentInput>,
    ) -> S3Result<S3Response<SelectObjectContentOutput>> {
        let usecase = s3_api::object_usecase_for(self);
        usecase.execute_select_object_content(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part(&self, req: S3Request<UploadPartInput>) -> S3Result<S3Response<UploadPartOutput>> {
        crate::hp_guard!("S3::upload_part");
        record_s3_op(S3Operation::UploadPart);
        let usecase = s3_api::multipart_usecase_for(self);
        usecase.execute_upload_part(req).await
    }

    #[instrument(level = "debug", skip(self, req))]
    async fn upload_part_copy(&self, req: S3Request<UploadPartCopyInput>) -> S3Result<S3Response<UploadPartCopyOutput>> {
        record_s3_op(S3Operation::UploadPartCopy);
        let usecase = s3_api::multipart_usecase_for(self);
        Box::pin(usecase.execute_upload_part_copy(req)).await
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FS, SITE_REPLICATION_GATE_FORCE_DISABLED, SITE_REPLICATION_GATE_FORCE_ENABLED, SITE_REPLICATION_GATE_TEST_OVERRIDE,
    };
    use crate::storage::access::ReqInfo;
    use http::Method;
    use http::StatusCode;
    use s3s::dto::{DeleteBucketReplicationInput, PutBucketReplicationInput, ReplicationConfiguration};
    use s3s::{S3, S3Error, S3ErrorCode, S3Request};
    use std::sync::atomic::Ordering;

    fn replication_config_edit_request<T>(input: T, is_owner: bool) -> S3Request<T> {
        let mut req = S3Request {
            input,
            method: Method::PUT,
            uri: http::Uri::from_static("/"),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        req.extensions.insert(ReqInfo {
            is_owner,
            ..Default::default()
        });
        req
    }

    fn put_bucket_replication_input() -> PutBucketReplicationInput {
        PutBucketReplicationInput {
            bucket: "test-bucket".to_string(),
            checksum_algorithm: None,
            content_md5: None,
            expected_bucket_owner: None,
            replication_configuration: ReplicationConfiguration {
                role: String::new(),
                rules: Vec::new(),
            },
            token: None,
        }
    }

    fn delete_bucket_replication_input() -> DeleteBucketReplicationInput {
        DeleteBucketReplicationInput {
            bucket: "test-bucket".to_string(),
            expected_bucket_owner: None,
        }
    }

    fn assert_replication_deny_edit(err: &S3Error) {
        match err.code() {
            S3ErrorCode::Custom(code) => assert_eq!(code, "XMinioReplicationDenyEdit"),
            other => panic!("expected XMinioReplicationDenyEdit, got {other:?}"),
        }
        assert_eq!(err.status_code(), Some(StatusCode::BAD_REQUEST));
    }

    /// Single test on purpose: the branches share the process-wide gate
    /// override, and parallel tests would race it.
    #[tokio::test]
    async fn replication_config_edit_gate_denies_only_non_owner_under_site_replication() {
        let fs = FS::new();
        SITE_REPLICATION_GATE_TEST_OVERRIDE.store(SITE_REPLICATION_GATE_FORCE_ENABLED, Ordering::SeqCst);

        // Non-owner PUT/DELETE through the real S3 handlers: denied by the
        // gate before the usecase (and thus the store) is ever touched.
        let err = fs
            .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), false))
            .await
            .expect_err("non-owner PutBucketReplication must be denied while site replication is enabled");
        assert_replication_deny_edit(&err);
        let err = fs
            .delete_bucket_replication(replication_config_edit_request(delete_bucket_replication_input(), false))
            .await
            .expect_err("non-owner DeleteBucketReplication must be denied while site replication is enabled");
        assert_replication_deny_edit(&err);

        // Owner passes the gate (the usecase's empty-rules structure error
        // proves the request reached the usecase instead of the deny path).
        let err = fs
            .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), true))
            .await
            .expect_err("owner request should pass the gate and fail later on config validation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        // Without site replication the policy check alone still governs the edit.
        SITE_REPLICATION_GATE_TEST_OVERRIDE.store(SITE_REPLICATION_GATE_FORCE_DISABLED, Ordering::SeqCst);
        let err = fs
            .put_bucket_replication(replication_config_edit_request(put_bucket_replication_input(), false))
            .await
            .expect_err("non-owner request should pass the gate and fail later on config validation");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);

        SITE_REPLICATION_GATE_TEST_OVERRIDE.store(0, Ordering::SeqCst);
    }
}
