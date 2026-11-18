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

//! Legacy s3s edge of bucket policy, policy status, versioning, notification and ACL: batch A2 of
//! the gateway migration (rustfs/backlog#2765).
//!
//! `impl S3 for FS` in `ecfs.rs` calls the `*_edge` fn of each operation here. The bodies are the
//! former trait method bodies, unchanged; the batch that moves these operations onto the gateway
//! owns this file and is the only one that edits it.

use crate::app::RequestEnvelope;
use crate::storage::ecfs::*;
use crate::storage::persisted_from_s3s;
use crate::storage::s3_api::{self, NotificationDestination};
use rustfs_gateway_types::compat::s3s_5761ddfe::generated::ops::{
    get_bucket_acl, get_bucket_notification_configuration, get_bucket_policy, get_bucket_policy_status, get_bucket_versioning,
};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use tracing::instrument;

impl FS {
    pub(in crate::storage::ecfs) async fn delete_bucket_policy_edge(
        &self,
        req: S3Request<DeleteBucketPolicyInput>,
    ) -> S3Result<S3Response<DeleteBucketPolicyOutput>> {
        record_s3_op(S3Operation::DeleteBucketPolicy);
        s3_api::bucket_usecase_for(self)
            .execute_delete_bucket_policy(RequestEnvelope::from_s3s(&req), &req.input.bucket)
            .await?;
        Ok(S3Response::new(DeleteBucketPolicyOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn get_bucket_acl_edge(
        &self,
        req: S3Request<GetBucketAclInput>,
    ) -> S3Result<S3Response<GetBucketAclOutput>> {
        record_s3_op(S3Operation::GetBucketAcl);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, get_bucket_acl::input_from_s3s, get_bucket_acl::output_to_s3s, |_, input| {
            usecase.execute_get_bucket_acl(input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_notification_configuration_edge(
        &self,
        req: S3Request<GetBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<GetBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::GetBucketNotificationConfiguration);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_notification_configuration::input_from_s3s,
            get_bucket_notification_configuration::output_to_s3s,
            |_, input| usecase.execute_get_bucket_notification_configuration(input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_policy_edge(
        &self,
        req: S3Request<GetBucketPolicyInput>,
    ) -> S3Result<S3Response<GetBucketPolicyOutput>> {
        record_s3_op(S3Operation::GetBucketPolicy);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(req, get_bucket_policy::input_from_s3s, get_bucket_policy::output_to_s3s, |_, input| {
            usecase.execute_get_bucket_policy(input)
        })
        .await
    }

    pub(in crate::storage::ecfs) async fn get_bucket_policy_status_edge(
        &self,
        req: S3Request<GetBucketPolicyStatusInput>,
    ) -> S3Result<S3Response<GetBucketPolicyStatusOutput>> {
        record_s3_op(S3Operation::GetBucketPolicyStatus);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_policy_status::input_from_s3s,
            get_bucket_policy_status::output_to_s3s,
            |envelope, input| usecase.execute_get_bucket_policy_status(envelope, input),
        )
        .await
    }

    #[instrument(name = "get_bucket_versioning", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn get_bucket_versioning_edge(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        record_s3_op(S3Operation::GetBucketVersioning);
        let usecase = s3_api::bucket_usecase_for(self);
        through_gateway_seam(
            req,
            get_bucket_versioning::input_from_s3s,
            get_bucket_versioning::output_to_s3s,
            |_, input| usecase.execute_get_bucket_versioning(input),
        )
        .await
    }

    pub(in crate::storage::ecfs) async fn put_bucket_acl_edge(
        &self,
        req: S3Request<PutBucketAclInput>,
    ) -> S3Result<S3Response<PutBucketAclOutput>> {
        record_s3_op(S3Operation::PutBucketAcl);
        let envelope = RequestEnvelope::from_s3s(&req);
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_acl(envelope, &req.input.bucket, req.input.access_control_policy.is_some())
            .await?;
        Ok(S3Response::new(PutBucketAclOutput::default()))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_notification_configuration_edge(
        &self,
        req: S3Request<PutBucketNotificationConfigurationInput>,
    ) -> S3Result<S3Response<PutBucketNotificationConfigurationOutput>> {
        record_s3_op(S3Operation::PutBucketNotificationConfiguration);
        let prepared = persisted_from_s3s(&req.input.notification_configuration).map_err(rustfs_s3_types::S3Error::from);
        let envelope = RequestEnvelope::from_s3s(&req);
        let destinations = notification_destinations(req.input.notification_configuration);
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_notification_configuration(envelope, &req.input.bucket, destinations, prepared)
            .await?;
        Ok(S3Response::new(PutBucketNotificationConfigurationOutput {}))
    }

    pub(in crate::storage::ecfs) async fn put_bucket_policy_edge(
        &self,
        req: S3Request<PutBucketPolicyInput>,
    ) -> S3Result<S3Response<PutBucketPolicyOutput>> {
        record_s3_op(S3Operation::PutBucketPolicy);
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_policy(RequestEnvelope::from_s3s(&req), &req.input.bucket, &req.input.policy)
            .await?;
        Ok(S3Response::new(PutBucketPolicyOutput {}))
    }

    #[instrument(name = "put_bucket_versioning", level = "debug", skip(self, req))]
    pub(in crate::storage::ecfs) async fn put_bucket_versioning_edge(
        &self,
        req: S3Request<PutBucketVersioningInput>,
    ) -> S3Result<S3Response<PutBucketVersioningOutput>> {
        record_s3_op(S3Operation::PutBucketVersioning);
        let prepared = persisted_from_s3s(&req.input.versioning_configuration).map_err(rustfs_s3_types::S3Error::from);
        let envelope = RequestEnvelope::from_s3s(&req);
        s3_api::bucket_usecase_for(self)
            .execute_put_bucket_versioning(envelope, &req.input.bucket, prepared)
            .await?;
        Ok(S3Response::new(PutBucketVersioningOutput::default()))
    }
}

fn notification_destinations(configuration: NotificationConfiguration) -> Vec<NotificationDestination> {
    let mut destinations = Vec::new();
    for cfg in configuration.queue_configurations.into_iter().flatten() {
        destinations.push(NotificationDestination {
            kind: "QueueConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_ref().to_owned()).collect(),
            target_arn: cfg.queue_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    for cfg in configuration.topic_configurations.into_iter().flatten() {
        destinations.push(NotificationDestination {
            kind: "TopicConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_ref().to_owned()).collect(),
            target_arn: cfg.topic_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    for cfg in configuration.lambda_function_configurations.into_iter().flatten() {
        destinations.push(NotificationDestination {
            kind: "LambdaFunctionConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_ref().to_owned()).collect(),
            target_arn: cfg.lambda_function_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    destinations
}

fn notification_filters(filter: Option<NotificationConfigurationFilter>) -> Vec<(Option<String>, Option<String>)> {
    filter
        .and_then(|filter| filter.key)
        .and_then(|key| key.filter_rules)
        .unwrap_or_default()
        .into_iter()
        .map(|rule| (rule.name.map(|name| name.as_str().to_owned()), rule.value))
        .collect()
}
