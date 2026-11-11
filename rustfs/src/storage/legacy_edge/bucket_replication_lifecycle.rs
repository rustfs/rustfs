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
use crate::app::storage_api::object_usecase::bucket::replication::OperatorRuleContract;
use crate::error::ApiError;
use crate::storage::StorageVersioningConfigExt as _;
use crate::storage::access::bucket_config_mutation_incarnation;
use crate::storage::ecfs::*;
use crate::storage::helper::audit_only_operation;
use crate::storage::s3_api;
use crate::storage::s3_compat::lifecycle::{lifecycle_configuration_from_s3s, lifecycle_configuration_to_s3s};
use crate::storage::s3_compat::replication::{replication_configuration_from_s3s, replication_configuration_to_s3s};
use crate::storage::storage_api::ecfs_consumer::contract::bucket::{BucketOperations, BucketOptions};
use crate::storage::storage_api::ecstore_bucket::utils::serialize as serialize_bucket_config;
use crate::storage::storage_api::runtime_sources_consumer::runtime_sources;
use crate::storage::{
    BUCKET_VERSIONING_CONFIG, BucketVersioningSys, OBJECT_LOCK_CONFIG, StorageError, get_bucket_object_lock_config,
    update_bucket_metadata_config_if_incarnation,
};
use crate::storage::{persisted_from_s3s, persisted_to_s3s};
use http::StatusCode;
use rustfs_io_metrics::record_s3_op;
use rustfs_madmin::{SITE_REPL_API_VERSION, SRBucketMeta};
use rustfs_s3_ops::S3Operation;
use tracing::{instrument, warn};

const LOG_SUBSYSTEM_OBJECT_LOCK: &str = "object_lock";

#[cfg(test)]
pub(super) static SITE_REPLICATION_GATE_TEST_OVERRIDE: std::sync::atomic::AtomicU8 = std::sync::atomic::AtomicU8::new(0);

#[cfg(test)]
pub(super) const SITE_REPLICATION_GATE_FORCE_DISABLED: u8 = 1;

#[cfg(test)]
pub(super) const SITE_REPLICATION_GATE_FORCE_ENABLED: u8 = 2;

async fn site_replication_gate_enabled() -> S3Result<bool> {
    #[cfg(test)]
    match SITE_REPLICATION_GATE_TEST_OVERRIDE.load(std::sync::atomic::Ordering::SeqCst) {
        SITE_REPLICATION_GATE_FORCE_DISABLED => return Ok(false),
        SITE_REPLICATION_GATE_FORCE_ENABLED => return Ok(true),
        _ => {}
    }
    crate::admin::handlers::site_replication::site_replication_enabled()
        .await
        .map_err(Into::into)
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
    crate::admin::handlers::site_replication::site_replication_edit_context()
        .await
        .map_err(Into::into)
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

/// Handler bodies that [`S3`] runs through `audit_only_operation`.
impl FS {
    async fn delete_bucket_replication_inner(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        deny_replication_config_edit_for_non_owner(&req).await?;
        let (site_peers, contract) = site_replication_edit_context().await?;
        let usecase = s3_api::bucket_usecase_for(self);
        usecase.execute_delete_bucket_replication(req, site_peers, contract).await
    }

    async fn put_bucket_replication_inner(
        &self,
        mut req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        deny_replication_config_edit_for_non_owner(&req).await?;
        let (site_peers, contract) = site_replication_edit_context().await?;
        let replication_configuration =
            replication_configuration_from_s3s(std::mem::take(&mut req.input.replication_configuration));
        let usecase = s3_api::bucket_usecase_for(self);
        usecase
            .execute_put_bucket_replication(req, replication_configuration, site_peers, contract)
            .await
    }

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
        let usecase = s3_api::bucket_usecase_for(self);
        audit_only_operation(req, S3Operation::DeleteBucketLifecycle, |req| {
            usecase.execute_delete_bucket_lifecycle(req)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn delete_bucket_replication_edge(
        &self,
        req: S3Request<DeleteBucketReplicationInput>,
    ) -> S3Result<S3Response<DeleteBucketReplicationOutput>> {
        audit_only_operation(req, S3Operation::DeleteBucketReplication, |req| self.delete_bucket_replication_inner(req)).await
    }

    #[instrument(name = "get_bucket_lifecycle_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_lifecycle_configuration_edge(
        &self,
        req: S3Request<GetBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketLifecycleConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketLifecycleConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        let config = usecase.execute_get_bucket_lifecycle_configuration(req).await?;
        let config = lifecycle_configuration_to_s3s(config)
            .map_err(|error| S3Error::with_message(S3ErrorCode::InternalError, error.to_string()))?;
        Ok(S3Response::new(GetBucketLifecycleConfigurationOutput {
            rules: Some(config.rules),
            ..Default::default()
        }))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_replication_edge(
        &self,
        req: S3Request<GetBucketReplicationInput>,
    ) -> S3Result<S3Response<GetBucketReplicationOutput>> {
        record_s3_op(S3Operation::GetBucketReplication);
        let usecase = s3_api::bucket_usecase_for(self);
        let replication_configuration = usecase.execute_get_bucket_replication(req).await?;
        Ok(S3Response::new(GetBucketReplicationOutput {
            replication_configuration: Some(replication_configuration_to_s3s(replication_configuration)),
        }))
    }

    #[instrument(name = "get_object_lock_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_object_lock_configuration_edge(
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
            Ok((cfg, _created)) => Some(persisted_to_s3s::<_, ObjectLockConfiguration>(&cfg)?),
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

    #[instrument(name = "put_bucket_lifecycle_configuration", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_lifecycle_configuration_edge(
        &self,
        req: S3Request<PutBucketLifecycleConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketLifecycleConfigurationOutput>> {
        let usecase = s3_api::bucket_usecase_for(self);
        let prepared = req
            .input
            .lifecycle_configuration
            .clone()
            .map(lifecycle_configuration_from_s3s)
            .transpose();
        audit_only_operation(req, S3Operation::PutBucketLifecycleConfiguration, |req| {
            usecase.execute_put_bucket_lifecycle_configuration(req, prepared)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_replication_edge(
        &self,
        req: S3Request<PutBucketReplicationInput>,
    ) -> S3Result<S3Response<PutBucketReplicationOutput>> {
        audit_only_operation(req, S3Operation::PutBucketReplication, |req| self.put_bucket_replication_inner(req)).await
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
