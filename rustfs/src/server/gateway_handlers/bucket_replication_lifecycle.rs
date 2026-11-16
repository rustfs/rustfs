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

//! The gateway operations of migration batch A3 (rustfs/backlog#2765): bucket lifecycle, replication and object lock configuration.

use super::access::authorize_bucket_config_mutation;
use super::lifecycle_input::prepare_lifecycle_configuration;
use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::{
    access_owned::bucket_replication_lifecycle::prepare_replication_config_edit, authorize_bucket_request,
};
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::persistence::{
    PersistedDefaultRetention, PersistedObjectLockConfiguration, PersistedObjectLockRule, parse_replication,
    serialize_replication_dto,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetBucketLifecycleConfiguration>(),
    row::<dto::PutBucketLifecycleConfiguration>(),
    row::<dto::DeleteBucketLifecycle>(),
    row::<dto::GetBucketReplication>(),
    row::<dto::PutBucketReplication>(),
    row::<dto::DeleteBucketReplication>(),
    row::<dto::GetObjectLockConfiguration>(),
    row::<dto::PutObjectLockConfiguration>(),
];

impl Served for dto::GetBucketLifecycleConfiguration {
    const METRIC: S3Operation = S3Operation::GetBucketLifecycleConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketLifecycleAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_lifecycle_configuration(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::GetBucketReplication {
    const METRIC: S3Operation = S3Operation::GetBucketReplication;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetReplicationConfigurationAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_replication(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::PutBucketReplication {
    const METRIC: S3Operation = S3Operation::PutBucketReplication;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutReplicationConfigurationAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let prepare = prepare_replication_config_edit(&envelope);
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_replication(envelope, input.bucket.as_str(), async move {
                let (site_peers, contract) = prepare.await?;
                let bytes = serialize_replication_dto(&input.replication_configuration)
                    .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))?;
                let configuration = parse_replication(&bytes).map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))?;
                Ok((configuration, site_peers, contract))
            })
            .await?;
        Ok(Resp::new(dto::PutBucketReplicationOutput::default()))
    }
}

impl Served for dto::DeleteBucketReplication {
    const METRIC: S3Operation = S3Operation::DeleteBucketReplication;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutReplicationConfigurationAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let prepare = prepare_replication_config_edit(&envelope);
        DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_replication(envelope, request.input().bucket.as_str(), prepare)
            .await?;
        Ok(Resp::new(dto::DeleteBucketReplicationOutput::default()))
    }
}

impl Served for dto::PutObjectLockConfiguration {
    const METRIC: S3Operation = S3Operation::PutObjectLockConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketObjectLockConfigurationAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let config = input.object_lock_configuration;
        let config = PersistedObjectLockConfiguration {
            object_lock_enabled: config.object_lock_enabled.map(|enabled| enabled.as_str().to_owned()),
            rule: config.rule.map(|rule| PersistedObjectLockRule {
                default_retention: rule.default_retention.map(|retention| PersistedDefaultRetention {
                    mode: retention.mode.map(|mode| mode.as_str().to_owned()),
                    days: retention.days,
                    years: retention.years,
                }),
            }),
        };
        DefaultBucketUsecase::with_context(context)
            .execute_put_object_lock_configuration(envelope, input.bucket.as_str(), Some(config))
            .await?;
        Ok(Resp::new(dto::PutObjectLockConfigurationOutput::default()))
    }
}

impl Served for dto::GetObjectLockConfiguration {
    const METRIC: S3Operation = S3Operation::GetObjectLockConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketObjectLockConfigurationAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_object_lock_configuration(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::PutBucketLifecycleConfiguration {
    const METRIC: S3Operation = S3Operation::PutBucketLifecycleConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketLifecycleAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let dates = request.context().lifecycle_request_dates().cloned();
        let input = request.into_input();
        let prepared = prepare_lifecycle_configuration(input.lifecycle_configuration, dates.as_deref());
        let output = DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_lifecycle_configuration(envelope, input.bucket.as_str(), prepared)
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::DeleteBucketLifecycle {
    const METRIC: S3Operation = S3Operation::DeleteBucketLifecycle;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketLifecycleAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let bucket = request.input().bucket.as_str();
        let output = DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_lifecycle(envelope, bucket)
            .await?;
        Ok(Resp::new(output))
    }
}
