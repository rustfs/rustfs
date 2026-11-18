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

use super::access::{authorize_bucket_config_mutation, authorize_bucket_tag_deletion};
use super::{OpRow, Served, row};
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::{RequestEnvelope, dto_values};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::cors_tagging::{PersistedTag, PersistedTagging};
use rustfs_gateway_types::persistence::{
    PersistedPublicAccessBlockConfiguration, parse_bucket_encryption, parse_bucket_encryption_dto, parse_cors_dto,
    parse_tagging_dto, serialize_bucket_encryption_dto,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetBucketCors>(),
    row::<dto::GetBucketEncryption>(),
    row::<dto::PutBucketEncryption>(),
    row::<dto::DeleteBucketEncryption>(),
    row::<dto::GetBucketTagging>(),
    row::<dto::PutBucketTagging>(),
    row::<dto::DeleteBucketTagging>(),
    row::<dto::PutBucketCors>(),
    row::<dto::DeleteBucketCors>(),
    row::<dto::GetPublicAccessBlock>(),
    row::<dto::PutPublicAccessBlock>(),
    row::<dto::DeletePublicAccessBlock>(),
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

impl Served for dto::DeleteBucketEncryption {
    const METRIC: S3Operation = S3Operation::DeleteBucketEncryption;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketEncryptionAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_encryption(envelope, request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::DeleteBucketEncryptionOutput::default()))
    }
}

impl Served for dto::PutBucketEncryption {
    const METRIC: S3Operation = S3Operation::PutBucketEncryption;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketEncryptionAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let prepared = serialize_bucket_encryption_dto(&input.server_side_encryption_configuration)
            .map_err(|err| s3_error!(MalformedXML, "{err}"))
            .and_then(|bytes| parse_bucket_encryption(&bytes).map_err(|err| s3_error!(MalformedXML, "{err}")));
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_encryption(envelope, input.bucket.as_str(), prepared)
            .await?;
        Ok(Resp::new(dto::PutBucketEncryptionOutput::default()))
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

impl Served for dto::DeleteBucketTagging {
    const METRIC: S3Operation = S3Operation::DeleteBucketTagging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        authorize_bucket_tag_deletion(envelope, request.input().bucket.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        DefaultBucketUsecase::with_context(context)
            .execute_delete_bucket_tagging(envelope, request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::DeleteBucketTaggingOutput::default()))
    }
}

impl Served for dto::PutBucketTagging {
    const METRIC: S3Operation = S3Operation::PutBucketTagging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketTaggingAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let tagging = PersistedTagging {
            tag_set: input
                .tagging
                .tag_set
                .into_iter()
                .map(|tag| PersistedTag {
                    key: tag.key,
                    value: tag.value,
                })
                .collect(),
        };
        DefaultBucketUsecase::with_context(context)
            .execute_put_bucket_tagging(envelope, input.bucket.as_str(), tagging)
            .await?;
        Ok(Resp::new(dto::PutBucketTaggingOutput::default()))
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

impl Served for dto::GetPublicAccessBlock {
    const METRIC: S3Operation = S3Operation::GetPublicAccessBlock;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::GetBucketPublicAccessBlockAction);
        Ok(authorize_bucket_request(envelope, request.input().bucket.as_str(), action).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let stored = DefaultBucketUsecase::with_context(context)
            .execute_get_public_access_block(request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::GetPublicAccessBlockOutput {
            public_access_block_configuration: Some(dto::PublicAccessBlockConfiguration {
                block_public_acls: stored.block_public_acls,
                ignore_public_acls: stored.ignore_public_acls,
                block_public_policy: stored.block_public_policy,
                restrict_public_buckets: stored.restrict_public_buckets,
            }),
        }))
    }
}

impl Served for dto::PutPublicAccessBlock {
    const METRIC: S3Operation = S3Operation::PutPublicAccessBlock;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::PutBucketPublicAccessBlockAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let config = input.public_access_block_configuration;
        let prepared = PersistedPublicAccessBlockConfiguration {
            block_public_acls: config.block_public_acls,
            ignore_public_acls: config.ignore_public_acls,
            block_public_policy: config.block_public_policy,
            restrict_public_buckets: config.restrict_public_buckets,
        };
        DefaultBucketUsecase::with_context(context)
            .execute_put_public_access_block(envelope, input.bucket.as_str(), Ok(prepared))
            .await?;
        Ok(Resp::new(dto::PutPublicAccessBlockOutput::default()))
    }
}

impl Served for dto::DeletePublicAccessBlock {
    const METRIC: S3Operation = S3Operation::DeletePublicAccessBlock;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let action = Action::S3Action(S3Action::DeleteBucketPublicAccessBlockAction);
        authorize_bucket_config_mutation(envelope, request.input().bucket.as_str(), action).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        DefaultBucketUsecase::with_context(context)
            .execute_delete_public_access_block(envelope, request.input().bucket.as_str())
            .await?;
        Ok(Resp::new(dto::DeletePublicAccessBlockOutput::default()))
    }
}
