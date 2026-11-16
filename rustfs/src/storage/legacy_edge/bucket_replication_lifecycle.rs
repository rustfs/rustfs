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

use crate::app::RequestEnvelope;
use crate::storage::access::access_owned::bucket_replication_lifecycle::prepare_replication_config_edit;
use crate::storage::ecfs::*;
use crate::storage::s3_api::{self, PreparedLifecycleConfiguration, validate_lifecycle_rule_status};
use crate::storage::s3_compat::lifecycle::lifecycle_configuration_from_s3s;
use crate::storage::s3_compat::replication::replication_configuration_from_s3s;
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    get_bucket_lifecycle_configuration, get_bucket_replication, get_object_lock_configuration,
};
use rustfs_gateway_types::persistence::{PersistedDefaultRetention, PersistedObjectLockConfiguration, PersistedObjectLockRule};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

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
        let envelope = RequestEnvelope::from_s3s(&req);
        let PutObjectLockConfigurationInput {
            bucket,
            object_lock_configuration,
            ..
        } = req.input;
        let config = object_lock_configuration.map(|config| PersistedObjectLockConfiguration {
            object_lock_enabled: config.object_lock_enabled.map(|enabled| enabled.as_str().to_owned()),
            rule: config.rule.map(|rule| PersistedObjectLockRule {
                default_retention: rule.default_retention.map(|retention| PersistedDefaultRetention {
                    mode: retention.mode.map(|mode| mode.as_str().to_owned()),
                    days: retention.days,
                    years: retention.years,
                }),
            }),
        });
        s3_api::bucket_usecase_for(self)
            .execute_put_object_lock_configuration(envelope, &bucket, config)
            .await?;
        Ok(S3Response::new(PutObjectLockConfigurationOutput::default()))
    }
}
