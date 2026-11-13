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
//! None is served yet; the batch appends its rows here.

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetBucketVersioning>(),
    row::<dto::GetBucketPolicy>(),
    row::<dto::GetBucketPolicyStatus>(),
    row::<dto::GetBucketAcl>(),
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
