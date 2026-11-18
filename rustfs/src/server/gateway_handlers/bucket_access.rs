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

//! The gateway operations of migration batch A2 (rustfs/backlog#2765): bucket policy, policy status, versioning, notification and ACL.

use super::access::authorize_bucket_config_mutation;
use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::{DefaultBucketUsecase, NotificationDestination};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::persistence::{
    parse_notification, parse_versioning, serialize_notification_dto, serialize_versioning_dto,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetBucketVersioning>(),
    row::<dto::PutBucketVersioning>(),
    row::<dto::GetBucketPolicy>(),
    row::<dto::PutBucketPolicy>(),
    row::<dto::DeleteBucketPolicy>(),
    row::<dto::GetBucketPolicyStatus>(),
    row::<dto::GetBucketAcl>(),
    row::<dto::PutBucketAcl>(),
    row::<dto::GetBucketNotificationConfiguration>(),
    row::<dto::PutBucketNotificationConfiguration>(),
];

impl Served for dto::GetBucketVersioning {
    const METRIC: S3Operation = S3Operation::GetBucketVersioning;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketVersioningAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_versioning(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::PutBucketVersioning {
    const METRIC: S3Operation = S3Operation::PutBucketVersioning;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketVersioningAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let prepared = parse_versioning(&serialize_versioning_dto(&input.versioning_configuration))
            .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"));
        let output = DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_versioning(envelope, input.bucket.as_str(), prepared)
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::DeleteBucketPolicy {
    const METRIC: S3Operation = S3Operation::DeleteBucketPolicy;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::DeleteBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_policy(envelope, request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::DeleteBucketPolicyOutput::default()))
    }
}

impl Served for dto::PutBucketPolicy {
    const METRIC: S3Operation = S3Operation::PutBucketPolicy;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_policy(envelope, input.bucket.as_str(), &input.policy)
            .await?;
        Ok(Resp::new(dto::PutBucketPolicyOutput::default()))
    }
}

impl Served for dto::GetBucketPolicy {
    const METRIC: S3Operation = S3Operation::GetBucketPolicy;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPolicyAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_policy(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::GetBucketPolicyStatus {
    const METRIC: S3Operation = S3Operation::GetBucketPolicyStatus;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPolicyStatusAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_policy_status(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::GetBucketAcl {
    const METRIC: S3Operation = S3Operation::GetBucketAcl;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketAclAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_acl(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::PutBucketAcl {
    const METRIC: S3Operation = S3Operation::PutBucketAcl;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketAclAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_acl(envelope, input.bucket.as_str(), input.access_control_policy.is_some())
            .await?;
        Ok(Resp::new(dto::PutBucketAclOutput::default()))
    }
}

impl Served for dto::GetBucketNotificationConfiguration {
    const METRIC: S3Operation = S3Operation::GetBucketNotificationConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketNotificationAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_notification_configuration(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::PutBucketNotificationConfiguration {
    const METRIC: S3Operation = S3Operation::PutBucketNotificationConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketNotificationAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let prepared = parse_notification(&serialize_notification_dto(&input.notification_configuration))
            .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"));
        let destinations = notification_destinations(input.notification_configuration);
        let output = DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_notification_configuration(envelope, input.bucket.as_str(), destinations, prepared)
            .await?;
        Ok(Resp::new(output))
    }
}

fn notification_destinations(configuration: dto::NotificationConfiguration) -> Vec<NotificationDestination> {
    let mut destinations = Vec::new();
    for cfg in configuration.queue_configurations {
        destinations.push(NotificationDestination {
            kind: "QueueConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_str().to_owned()).collect(),
            target_arn: cfg.queue_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    for cfg in configuration.topic_configurations {
        destinations.push(NotificationDestination {
            kind: "TopicConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_str().to_owned()).collect(),
            target_arn: cfg.topic_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    for cfg in configuration.lambda_function_configurations {
        destinations.push(NotificationDestination {
            kind: "LambdaFunctionConfiguration",
            id: cfg.id,
            events: cfg.events.into_iter().map(|event| event.as_str().to_owned()).collect(),
            target_arn: cfg.lambda_function_arn,
            filters: notification_filters(cfg.filter),
        });
    }
    destinations
}

fn notification_filters(filter: Option<dto::NotificationConfigurationFilter>) -> Vec<(Option<String>, Option<String>)> {
    filter
        .and_then(|filter| filter.key)
        .into_iter()
        .flat_map(|key| key.filter_rules)
        .map(|rule| (rule.name.map(|name| name.as_str().to_owned()), rule.value))
        .collect()
}
