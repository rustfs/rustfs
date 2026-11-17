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

//! The gateway operations of migration batch A4 (rustfs/backlog#2765): bucket create, delete,
//! head and location, website, accelerate, logging and request payment; and ListBuckets.

use super::{OpRow, Served, access::authorize_bucket_config_mutation, row};
use crate::app::bucket_usecase::{DefaultBucketUsecase, requested_location_constraint};
use crate::app::{RequestEnvelope, dto_values};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::bucket_core::{authorize_create_bucket, authorize_delete_bucket};
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::persistence::{
    parse_accelerate, parse_bucket_logging_dto, parse_request_payment, parse_website, parse_website_dto,
    serialize_accelerate_dto, serialize_bucket_logging_dto, serialize_request_payment_dto, serialize_website_dto,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::ListBuckets>(),
    row::<dto::HeadBucket>(),
    row::<dto::CreateBucket>(),
    row::<dto::DeleteBucket>(),
    row::<dto::GetBucketLocation>(),
    row::<dto::GetBucketAccelerateConfiguration>(),
    row::<dto::GetBucketRequestPayment>(),
    row::<dto::GetBucketWebsite>(),
    row::<dto::GetBucketLogging>(),
    row::<dto::PutBucketLogging>(),
    row::<dto::PutBucketAccelerateConfiguration>(),
    row::<dto::PutBucketRequestPayment>(),
    row::<dto::PutBucketWebsite>(),
    row::<dto::DeleteBucketWebsite>(),
];

impl Served for dto::ListBuckets {
    const METRIC: S3Operation = S3Operation::ListBuckets;

    /// The legacy hook checks nothing: the use case decides per caller which buckets it sees.
    async fn authorize(_envelope: &mut RequestEnvelope, _request: &Req<Self>) -> S3Result<()> {
        Ok(())
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_list_buckets(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::HeadBucket {
    const METRIC: S3Operation = S3Operation::HeadBucket;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::ListBucketAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_head_bucket(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::CreateBucket {
    const METRIC: S3Operation = S3Operation::CreateBucket;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let location_constraint = requested_location_constraint(input).map(str::to_owned);
        let object_lock_enabled = input.object_lock_enabled_for_bucket.is_some_and(|enabled| enabled);
        authorize_create_bucket(envelope, input.bucket.as_str(), location_constraint, object_lock_enabled).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_create_bucket(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::DeleteBucket {
    const METRIC: S3Operation = S3Operation::DeleteBucket;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        authorize_delete_bucket(envelope, request.input().bucket.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::GetBucketLocation {
    const METRIC: S3Operation = S3Operation::GetBucketLocation;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketLocationAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_location(request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::GetBucketAccelerateConfiguration {
    const METRIC: S3Operation = S3Operation::GetBucketAccelerateConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPolicyAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let config = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_accelerate_configuration(request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::GetBucketAccelerateConfigurationOutput {
            status: config.status.map(dto::Status::custom),
            ..Default::default()
        }))
    }
}

impl Served for dto::GetBucketRequestPayment {
    const METRIC: S3Operation = S3Operation::GetBucketRequestPayment;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPolicyAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let config = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_request_payment(request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::GetBucketRequestPaymentOutput {
            payer: Some(dto::Payer::custom(config.payer)),
        }))
    }
}

impl Served for dto::GetBucketWebsite {
    const METRIC: S3Operation = S3Operation::GetBucketWebsite;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPolicyAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let config = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_website(request.input().bucket.as_str())
            .await?;
        let config = dto_values::stored_config(&config, parse_website_dto)?;
        Ok(Resp::new(dto::GetBucketWebsiteOutput {
            error_document: config.error_document,
            index_document: config.index_document,
            redirect_all_requests_to: config.redirect_all_requests_to,
            routing_rules: config.routing_rules,
        }))
    }
}

impl Served for dto::GetBucketLogging {
    const METRIC: S3Operation = S3Operation::GetBucketLogging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketLoggingAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let config = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_logging(request.input().bucket.as_str())
            .await?;
        let config = dto_values::stored_config(&config, parse_bucket_logging_dto)?;
        Ok(Resp::new(dto::GetBucketLoggingOutput {
            logging_enabled: config.logging_enabled,
        }))
    }
}

impl Served for dto::PutBucketLogging {
    const METRIC: S3Operation = S3Operation::PutBucketLogging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketLoggingAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_logging(envelope, input.bucket.as_str(), || {
                serialize_bucket_logging_dto(&input.bucket_logging_status)
                    .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))
            })
            .await?;
        Ok(Resp::new(dto::PutBucketLoggingOutput::default()))
    }
}

impl Served for dto::PutBucketAccelerateConfiguration {
    const METRIC: S3Operation = S3Operation::PutBucketAccelerateConfiguration;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_accelerate_configuration(envelope, input.bucket.as_str(), || {
                parse_accelerate(&serialize_accelerate_dto(&input.accelerate_configuration))
                    .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))
            })
            .await?;
        Ok(Resp::new(dto::PutBucketAccelerateConfigurationOutput::default()))
    }
}

impl Served for dto::PutBucketRequestPayment {
    const METRIC: S3Operation = S3Operation::PutBucketRequestPayment;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_request_payment(envelope, input.bucket.as_str(), || {
                parse_request_payment(&serialize_request_payment_dto(&input.request_payment_configuration))
                    .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))
            })
            .await?;
        Ok(Resp::new(dto::PutBucketRequestPaymentOutput::default()))
    }
}

impl Served for dto::PutBucketWebsite {
    const METRIC: S3Operation = S3Operation::PutBucketWebsite;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_website(envelope, input.bucket.as_str(), || {
                parse_website(&serialize_website_dto(&input.website_configuration))
                    .map_err(|err| rustfs_s3_types::s3_error!(MalformedXML, "{err}"))
            })
            .await?;
        Ok(Resp::new(dto::PutBucketWebsiteOutput::default()))
    }
}

impl Served for dto::DeleteBucketWebsite {
    const METRIC: S3Operation = S3Operation::DeleteBucketWebsite;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPolicyAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_website(envelope, input.bucket.as_str())
            .await?;
        Ok(Resp::new(dto::DeleteBucketWebsiteOutput::default()))
    }
}
