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

//! The gateway operations of migration batch A1 (rustfs/backlog#2765): bucket CORS, tagging, public access block and encryption.
//! The stored CORS document is applied to requests by `ConditionalCorsLayer`, not by the gateway.

use super::access::authorize_bucket_config_mutation;
use super::{OpRow, Served, row};
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::{RequestEnvelope, dto_values};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::persistence::{parse_bucket_encryption_dto, parse_cors_dto, parse_tagging_dto};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetBucketCors>(),
    row::<dto::GetBucketEncryption>(),
    row::<dto::GetBucketTagging>(),
    row::<dto::PutBucketCors>(),
    row::<dto::DeleteBucketCors>(),
];

impl Served for dto::GetBucketCors {
    const METRIC: S3Operation = S3Operation::GetBucketCors;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketCorsAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let stored = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_cors(request.input().bucket.as_str())
            .await?;
        let config = dto_values::stored_config(&stored, parse_cors_dto)?;
        Ok(Resp::new(dto::GetBucketCorsOutput {
            cors_rules: config.cors_rules,
        }))
    }
}

impl Served for dto::GetBucketEncryption {
    const METRIC: S3Operation = S3Operation::GetBucketEncryption;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketEncryptionAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let stored = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_encryption(request.input().bucket.as_str())
            .await?;
        let config = dto_values::stored_config(&stored, parse_bucket_encryption_dto)?;
        Ok(Resp::new(dto::GetBucketEncryptionOutput {
            server_side_encryption_configuration: Some(config),
        }))
    }
}

impl Served for dto::GetBucketTagging {
    const METRIC: S3Operation = S3Operation::GetBucketTagging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketTaggingAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let stored = DefaultBucketUsecase::with_context(context)
            .execute_get_bucket_tagging(request.input().bucket.as_str())
            .await?;
        let config = dto_values::stored_config(&stored, parse_tagging_dto)?;
        Ok(Resp::new(dto::GetBucketTaggingOutput { tag_set: config.tag_set }))
    }
}

impl Served for dto::PutBucketCors {
    const METRIC: S3Operation = S3Operation::PutBucketCors;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketCorsAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_cors(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::DeleteBucketCors {
    const METRIC: S3Operation = S3Operation::DeleteBucketCors;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::DeleteBucketCorsAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_cors(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}
