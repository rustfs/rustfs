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

//! Legacy s3s edge of object ACL, tagging, legal hold, retention and torrent: batch B1 of the
//! gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::app::storage_api::object_usecase::bucket::replication::{
    ReplicateDecision, get_read_proxy_targets, must_replicate_metadata, schedule_metadata_replication,
};
use crate::error::ApiError;
use crate::storage::access::{apply_bucket_generation_guard, has_bypass_governance_header, replication_proxy_allowed};
use crate::storage::ecfs::*;
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_opts;
use crate::storage::s3_api::{self, acl};
use crate::storage::storage_api::ecfs_consumer::StorageObjectOptions as ObjectOptions;
use crate::storage::storage_api::ecfs_consumer::contract::{
    bucket::{BucketOperations, BucketOptions},
    object::ObjectLockRetentionOptions,
};
use crate::storage::storage_api::ecfs_consumer::object_lock::{
    parse_object_lock_legal_hold, parse_object_lock_retention, validate_bucket_object_lock_enabled,
};
use crate::storage::{
    BucketVersioningSys, check_retention_for_modification, decode_tags, encode_tags, is_err_object_not_found,
    is_err_version_not_found, record_replication_proxy,
};
use crate::table_catalog;
use metrics::{counter, histogram};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::http::headers::AMZ_OBJECT_LOCK_LEGAL_HOLD_LOWER;
use rustfs_utils::http::{
    SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP, insert_str,
};
use std::collections::HashMap;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tracing::{debug, error, instrument, warn};
use uuid::Uuid;

const LOG_SUBSYSTEM_TAGGING: &str = "tagging";

async fn reject_veeam_sosapi_mutation(fs: &FS, bucket: &str, key: &str) -> S3Result<()> {
    s3_api::reject_veeam_sosapi_mutation(|| fs.server_ctx.object_store(), bucket, key).await
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

impl FS {
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
    ) -> Option<GetObjectTaggingOutput> {
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
                    return Some(GetObjectTaggingOutput {
                        tag_set: remote
                            .tag_set
                            .into_iter()
                            .map(|tag| Tag {
                                key: Some(tag.key),
                                value: Some(tag.value),
                            })
                            .collect(),
                        version_id: remote.version_id,
                    });
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
}

impl FS {
    #[instrument(name = "delete_object_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_object_tagging_edge(
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

        reject_veeam_sosapi_mutation(self, &bucket, &object).await?;
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
                    && replication_proxy_allowed(&req.extensions)
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
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
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
        rustfs_scanner::record_dirty_usage_object_from_producer(
            &bucket,
            &object,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        let duration = start_time.elapsed();
        histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "delete").record(duration.as_secs_f64());
        result
    }

    pub(in crate::storage::ecfs) async fn get_object_acl_edge(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
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

    pub(in crate::storage::ecfs) async fn get_object_legal_hold_edge(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetLegalHold, S3Operation::GetObjectLegalHold).suppress_event();
        let GetObjectLegalHoldInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        reject_veeam_sosapi_mutation(self, &bucket, &key).await?;

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

    pub(in crate::storage::ecfs) async fn get_object_retention_edge(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        let mut helper =
            OperationHelper::new(&req, EventName::ObjectAccessedGetRetention, S3Operation::GetObjectRetention).suppress_event();
        let GetObjectRetentionInput {
            bucket, key, version_id, ..
        } = req.input.clone();

        reject_veeam_sosapi_mutation(self, &bucket, &key).await?;

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

        let output = GetObjectRetentionOutput {
            retention: Some(object_lock_retention_from_metadata(&object_info.user_defined)?),
        };
        let version_id = req.input.version_id.clone().unwrap_or_default();
        helper = helper.object(object_info).version_id(version_id);

        let result = Ok(S3Response::new(output));
        let _ = helper.complete(&result);
        result
    }

    #[instrument(name = "get_object_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_object_tagging_edge(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        use crate::storage::storage_api::ecstore_bucket::versioning::VersioningApi as _;

        record_s3_op(S3Operation::GetObjectTagging);
        let start_time = std::time::Instant::now();
        let bucket = req.input.bucket.as_str();
        let object = req.input.key.as_str();

        reject_veeam_sosapi_mutation(self, bucket, object).await?;

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

        let version_id = parse_object_version_id(req.input.version_id.clone())?.map(Into::into);
        let versioning = BucketVersioningSys::get(bucket).await.map_err(ApiError::from)?;
        let opts = ObjectOptions {
            version_id,
            versioned: versioning.prefix_enabled(object),
            version_suspended: versioning.prefix_suspended(object),
            ..Default::default()
        };

        // Tags and response identity must come from the same locked metadata snapshot.
        let info = match store.get_object_info(bucket, object, &opts).await {
            Ok(info) if info.delete_marker => {
                return Err(S3Error::new(if opts.version_id.is_some() {
                    S3ErrorCode::MethodNotAllowed
                } else {
                    S3ErrorCode::NoSuchKey
                }));
            }
            Ok(info) => info,
            Err(e) => {
                // Replication lag window: the object may exist on a
                // replication target even though it is missing locally —
                // proxy the tagging read there (backlog#1675 P1-5).
                if (is_err_object_not_found(&e) || is_err_version_not_found(&e))
                    && replication_proxy_allowed(&req.extensions)
                    && let Some(output) =
                        Self::proxy_get_object_tagging(bucket, object, req.input.version_id.clone(), &req.headers).await
                {
                    counter!("rustfs_get_object_tagging_success").increment(1);
                    let duration = start_time.elapsed();
                    histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "get")
                        .record(duration.as_secs_f64());
                    return Ok(S3Response::new(output));
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
                    return Err(S3Error::new(if opts.version_id.is_some() {
                        S3ErrorCode::NoSuchVersion
                    } else {
                        S3ErrorCode::NoSuchKey
                    }));
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

        let tag_set = decode_tags(info.user_tags.as_str());
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
            version_id: s3_api::s3_response_version_id(info.version_id),
        }))
    }

    #[instrument(name = "get_object_torrent", level = "debug", skip(self, _req))]
    pub(in crate::storage::ecfs) async fn get_object_torrent_edge(
        &self,
        _req: S3Request<GetObjectTorrentInput>,
    ) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        // Torrent functionality is not implemented in RustFS
        // Per S3 API test expectations, return 404 NoSuchKey (not 501 Not Implemented)
        // This allows clients to gracefully handle the absence of torrent support
        record_s3_op(S3Operation::GetObjectTorrent);
        Err(S3Error::new(S3ErrorCode::NoSuchKey))
    }

    pub(in crate::storage::ecfs) async fn put_object_acl_edge(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        let mut helper = OperationHelper::new(&req, EventName::ObjectAclPut, S3Operation::PutObjectAcl);
        let bucket = &req.input.bucket;
        let key = &req.input.key;
        let version_id = req.input.version_id.clone();

        reject_veeam_sosapi_mutation(self, bucket, key).await?;

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

    pub(in crate::storage::ecfs) async fn put_object_legal_hold_edge(
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

        reject_veeam_sosapi_mutation(self, &bucket, &key).await?;
        validate_table_catalog_object_mutation(&bucket, &key).await?;

        let Some(store) = self.server_ctx.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        if let Err(err) = validate_bucket_object_lock_enabled(&bucket).await {
            let _ = store
                .get_bucket_info(&bucket, &BucketOptions::default())
                .await
                .map_err(ApiError::from)?;
            return Err(err);
        }

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
        let dsc = if crate::app::storage_api::bucket::metadata_sys::get_replication_config(&bucket)
            .await
            .is_ok()
            && let Ok(info) = store.get_object_info(&bucket, &key, &opts).await
        {
            must_replicate_metadata(
                &bucket,
                &key,
                &info.user_defined,
                info.user_tags.as_ref().clone(),
                popts.delete_marker_replication_status(),
                popts.clone(),
            )
            .await
        } else {
            ReplicateDecision::new()
        };

        let mut eval_metadata = parse_object_lock_legal_hold(legal_hold)?;
        if dsc.replicate_any() {
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
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
        rustfs_scanner::record_dirty_usage_object_from_producer(
            &bucket,
            &key,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        result
    }

    pub(in crate::storage::ecfs) async fn put_object_retention_edge(
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

        reject_veeam_sosapi_mutation(self, &bucket, &key).await?;
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
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
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
        rustfs_scanner::record_dirty_usage_object_from_producer(
            &bucket,
            &key,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        result
    }

    #[instrument(name = "put_object_tagging", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_object_tagging_edge(
        &self,
        req: S3Request<PutObjectTaggingInput>,
    ) -> S3Result<S3Response<PutObjectTaggingOutput>> {
        let start_time = std::time::Instant::now();
        let mut helper = OperationHelper::new(&req, EventName::ObjectTaggingPut, S3Operation::PutObjectTagging);
        let PutObjectTaggingInput {
            bucket,
            key: object,
            tagging,
            ..
        } = req.input.clone();

        reject_veeam_sosapi_mutation(self, &bucket, &object).await?;
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
                    && replication_proxy_allowed(&req.extensions)
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
            insert_str(&mut eval_metadata, SUFFIX_REPLICATION_GENERATION, Uuid::new_v4().to_string());
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
        rustfs_scanner::record_dirty_usage_object_from_producer(
            &bucket,
            &object,
            rustfs_scanner::SegmentInvalidationProducerIdentity::ObjectMetadata,
        );
        let duration = start_time.elapsed();
        histogram!("rustfs_object_tagging_operation_duration_seconds", "operation" => "put").record(duration.as_secs_f64());
        result
    }
}
