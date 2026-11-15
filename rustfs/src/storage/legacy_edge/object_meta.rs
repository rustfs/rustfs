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

use crate::app::object_meta::ObjectMetaUsecase;
use crate::app::object_meta::torrent::answer_get_object_torrent;
use crate::app::storage_api::object_usecase::bucket::replication::{
    ReplicateDecision, must_replicate_metadata, schedule_metadata_replication,
};
use crate::error::ApiError;
use crate::storage::access::{apply_bucket_generation_guard, has_bypass_governance_header};
use crate::storage::check_retention_for_modification;
use crate::storage::ecfs::*;
use crate::storage::helper::OperationHelper;
use crate::storage::options::get_opts;
use crate::storage::s3_api;
use crate::storage::storage_api::ecfs_consumer::StorageObjectOptions as ObjectOptions;
use crate::storage::storage_api::ecfs_consumer::contract::{
    bucket::{BucketOperations, BucketOptions},
    object::ObjectLockRetentionOptions,
};
use crate::storage::storage_api::ecfs_consumer::object_lock::{
    parse_object_lock_legal_hold, parse_object_lock_retention, validate_bucket_object_lock_enabled,
};
use crate::table_catalog;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    delete_object_tagging, get_object_acl, get_object_legal_hold, get_object_retention, get_object_tagging, get_object_torrent,
    put_object_acl,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_targets::EventName;
use rustfs_utils::http::{SUFFIX_REPLICATION_GENERATION, SUFFIX_REPLICATION_STATUS, SUFFIX_REPLICATION_TIMESTAMP, insert_str};
use time::OffsetDateTime;
use tracing::{error, instrument};
use uuid::Uuid;

/// Preserve every tag and its optional fields for validation by the shared use case.
fn gateway_tags(tag_set: &TagSet) -> Vec<rustfs_gateway_types::dto::Tag> {
    tag_set
        .iter()
        .map(|tag| rustfs_gateway_types::dto::Tag {
            key: tag.key.clone(),
            value: tag.value.clone(),
        })
        .collect()
}

fn object_meta_usecase(fs: &FS) -> ObjectMetaUsecase {
    ObjectMetaUsecase::with_context(fs.server_ctx().app_context())
}

async fn reject_veeam_sosapi_mutation(fs: &FS, bucket: &str, key: &str) -> S3Result<()> {
    s3_api::reject_veeam_sosapi_mutation(|| fs.server_ctx.object_store(), bucket, key).await
}

async fn validate_table_catalog_object_mutation(bucket: &str, key: &str) -> S3Result<()> {
    table_catalog::validate_bucket_object_mutation(bucket, key)
        .await
        .map_err(|_| s3_error!(InvalidRequest, "{}", table_catalog::RESERVED_CATALOG_OBJECT_MESSAGE))
}

impl FS {
    pub(in crate::storage::ecfs) async fn delete_object_tagging_edge(
        &self,
        req: S3Request<DeleteObjectTaggingInput>,
    ) -> S3Result<S3Response<DeleteObjectTaggingOutput>> {
        record_s3_op(S3Operation::DeleteObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .delete_object_tagging_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        delete_object_tagging::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_acl_edge(
        &self,
        req: S3Request<GetObjectAclInput>,
    ) -> S3Result<S3Response<GetObjectAclOutput>> {
        record_s3_op(S3Operation::GetObjectAcl);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_acl_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_acl::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_legal_hold_edge(
        &self,
        req: S3Request<GetObjectLegalHoldInput>,
    ) -> S3Result<S3Response<GetObjectLegalHoldOutput>> {
        record_s3_op(S3Operation::GetObjectLegalHold);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_legal_hold_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_legal_hold::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_retention_edge(
        &self,
        req: S3Request<GetObjectRetentionInput>,
    ) -> S3Result<S3Response<GetObjectRetentionOutput>> {
        record_s3_op(S3Operation::GetObjectRetention);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_retention_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_retention::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_tagging_edge(
        &self,
        req: S3Request<GetObjectTaggingInput>,
    ) -> S3Result<S3Response<GetObjectTaggingOutput>> {
        record_s3_op(S3Operation::GetObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .get_object_tagging_fields(envelope, &req.input.bucket, &req.input.key, req.input.version_id)
            .await?;
        get_object_tagging::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn get_object_torrent_edge(
        &self,
        _req: S3Request<GetObjectTorrentInput>,
    ) -> S3Result<S3Response<GetObjectTorrentOutput>> {
        record_s3_op(S3Operation::GetObjectTorrent);
        let output = answer_get_object_torrent()?;
        get_object_torrent::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
    }

    pub(in crate::storage::ecfs) async fn put_object_acl_edge(
        &self,
        req: S3Request<PutObjectAclInput>,
    ) -> S3Result<S3Response<PutObjectAclOutput>> {
        record_s3_op(S3Operation::PutObjectAcl);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let output = object_meta_usecase(self)
            .put_object_acl_fields(
                envelope,
                &req.input.bucket,
                &req.input.key,
                req.input.version_id,
                req.input.access_control_policy.is_some(),
            )
            .await?;
        put_object_acl::output_to_s3s(output)
            .map(S3Response::new)
            .map_err(|error| crate::app::dto_values::seam_error(error).into())
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
        record_s3_op(S3Operation::PutObjectTagging);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let PutObjectTaggingInput {
            bucket,
            key,
            version_id,
            tagging,
            ..
        } = req.input;
        let output = object_meta_usecase(self)
            .put_object_tagging_fields(envelope, &bucket, &key, version_id, gateway_tags(&tagging.tag_set))
            .await?;
        Ok(S3Response::new(PutObjectTaggingOutput {
            version_id: output.version_id,
        }))
    }
}
