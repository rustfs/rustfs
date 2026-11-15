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

//! Legacy s3s edge of the bucket lifecycle, replication and Object Lock configurations: batch A3 of
//! the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::admin::handlers::site_replication::site_replication_bucket_meta_hook;
use crate::app::RequestEnvelope;
use crate::error::ApiError;
use crate::storage::StorageVersioningConfigExt as _;
use crate::storage::access::access_owned::bucket_replication_lifecycle::prepare_replication_config_edit;
use crate::storage::access::bucket_config_mutation_incarnation;
use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::persisted_from_s3s;
use crate::storage::s3_api::{self, PreparedLifecycleConfiguration, validate_lifecycle_rule_status};
use crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s;
use crate::storage::s3_compat::replication::replication_configuration_from_s3s;
use crate::storage::storage_api::ecfs_consumer::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::storage_api::ecstore_bucket::utils::serialize as serialize_bucket_config;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::{
    BUCKET_VERSIONING_CONFIG, BucketVersioningSys, OBJECT_LOCK_CONFIG, StorageError, get_bucket_object_lock_config,
    update_bucket_metadata_config_if_incarnation,
};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    get_bucket_lifecycle_configuration, get_bucket_replication, get_object_lock_configuration,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_s3_ops::S3Operation;
use tracing::{instrument, warn};

const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

/// Handler body that runs through `audit_only_operation`.
impl FS {
    async fn put_object_lock_configuration_inner(
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

        let persisted_cfg =
            persisted_from_s3s::<_, rustfs_gateway_types::persistence::PersistedObjectLockConfiguration>(&input_cfg)?;
        let data = serialize_bucket_config(&persisted_cfg)
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{err}")))?;
        let object_lock_config =
            String::from_utf8(data.clone()).map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("{err}")))?;

        let updated_at = update_bucket_metadata_config_if_incarnation(&bucket, OBJECT_LOCK_CONFIG, data, expected_incarnation_id)
            .await
            .map_err(ApiError::from)?;

        // When Object Lock is enabled, automatically enable versioning if not already enabled.
        // This matches S3-compatible behavior.
        let versioning_config = BucketVersioningSys::get(&bucket).await.map_err(ApiError::from)?;
        if !versioning_config.enabled() {
            let enable_versioning_config = rustfs_gateway_types::persistence::PersistedVersioningConfiguration {
                status: Some("Enabled".to_owned()),
                ..Default::default()
            };
            let versioning_data = serialize_bucket_config(&enable_versioning_config)
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

        rustfs_scanner::record_dirty_usage_bucket_from_producer(
            &bucket,
            rustfs_scanner::SegmentInvalidationProducerIdentity::BucketMetadata,
        );
        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }
}

impl FS {
    #[instrument(name = "delete_bucket_lifecycle", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn delete_bucket_lifecycle_edge(
        &self,
        req: S3Request<DeleteBucketLifecycleInput>,
    ) -> S3Result<S3Response<DeleteBucketLifecycleOutput>> {
        record_s3_op(S3Operation::DeleteBucketLifecycle);
        let envelope = crate::app::RequestEnvelope::from_s3s(&req);
        let bucket = req.input.bucket;
        s3_api::bucket_usecase_for(self)
            .execute_delete_bucket_lifecycle(envelope, &bucket)
            .await?;
        Ok(S3Response::new(DeleteBucketLifecycleOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn delete_bucket_replication_edge(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let prepare = prepare_replication_config_edit(&envelope);
        s3_api::bucket_usecase_for(self)
            .execute_delete_bucket_replication(envelope, &req.input.bucket, prepare)
            .await?;
        Ok(S3Response::new(DeleteBucketReplicationOutput::default()))
    }

    #[instrument(name = "get_bucket_lifecycle_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_lifecycle_configuration_edge(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketLifecycleConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_lifecycle_configuration::input_from_s3s,
            get_bucket_lifecycle_configuration::output_to_s3s,
            |_, input| usecase.execute_get_bucket_lifecycle_configuration(input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_replication_edge(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        record_s3_op(S3Operation::GetBucketReplication);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_replication::input_from_s3s,
            get_bucket_replication::output_to_s3s,
            |_, input| usecase.execute_get_bucket_replication(input),
        )
        .await
    }

    #[instrument(name = "get_object_lock_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_object_lock_configuration_edge(
        &self,
        req: S3Request<GetObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<GetObjectLockConfigurationOutput>> {
        record_s3_op(S3Operation::GetObjectLockConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_object_lock_configuration::input_from_s3s,
            get_object_lock_configuration::output_to_s3s,
            |_, input| usecase.execute_get_object_lock_configuration(input),
        )
        .await
    }

    #[instrument(name = "put_bucket_lifecycle_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_lifecycle_configuration_edge(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        let envelope = RequestEnvelope::from_s3s(&req);
        let PutBucketLifecycleConfigurationInput {
            bucket,
            lifecycle_configuration,
            ..
        } = req.input;
        let prepared = match lifecycle_configuration {
            None => PreparedLifecycleConfiguration::Missing,
            Some(configuration) => {
                if validate_lifecycle_rule_status(configuration.rules.iter().map(|rule| rule.status.as_str())).is_err() {
                    PreparedLifecycleConfiguration::InvalidStatus
                } else {
                    PreparedLifecycleConfiguration::Prepared(lifecycle_configuration_from_s3s(configuration))
                }
            }
        };
        record_s3_op(S3Operation::PutBucketLifecycleConfiguration);
        usecase
            .execute_put_bucket_lifecycle_configuration(envelope, &bucket, prepared)
            .await?;
        Ok(S3Response::new(PutBucketLifecycleConfigurationOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_replication_edge(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        let envelope = RequestEnvelope::from_s3s(&req);
        let prepare = prepare_replication_config_edit(&envelope);
        let PutBucketReplicationInput {
            bucket,
            replication_configuration,
            ..
        } = req.input;
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_replication(envelope, &bucket, async move {
                let (site_peers, contract) = prepare.await?;
                Ok((replication_configuration_from_s3s(replication_configuration), site_peers, contract))
            })
            .await?;
        Ok(S3Response::new(PutBucketReplicationOutput::default()))
    }

    #[instrument(name = "put_object_lock_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_object_lock_configuration_edge(
        &self,
        req: S3Request<PutObjectLockConfigurationInput>,
    ) -> S3Result<S3Response<PutObjectLockConfigurationOutput>> {
        audit_only_operation(req, S3Operation::PutObjectLockConfiguration, |req| {
            self.put_object_lock_configuration_inner(req)
        })
        .await
    }
}
