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

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::{DefaultBucketUsecase, requested_location_constraint};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::bucket_core::{authorize_create_bucket, authorize_delete_bucket};
use crate::storage_api::server::http::access::authorize_bucket_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::ListBuckets>(),
    row::<dto::HeadBucket>(),
    row::<dto::CreateBucket>(),
    row::<dto::DeleteBucket>(),
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
