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

//! The gateway operations of migration batch B1 (rustfs/backlog#2766): object ACL, tagging, legal
//! hold, retention and torrent. Tagging and the three body-driven writes are appended here.

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::object_meta::ObjectMetaUsecase;
use crate::app::object_meta::torrent::execute_get_object_torrent;
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_meta::authorize_object_tagging_request;
use crate::storage_api::server::http::access::authorize_object_request;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::{BucketName, ObjectKey};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::S3Result;
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetObjectTorrent>(),
    row::<dto::GetObjectAcl>(),
    row::<dto::PutObjectAcl>(),
    row::<dto::GetObjectLegalHold>(),
    row::<dto::GetObjectRetention>(),
    row::<dto::GetObjectTagging>(),
    row::<dto::DeleteObjectTagging>(),
];

/// What the legacy hooks of the ACL, legal-hold and retention reads make: the decoded object, its
/// named version, and one action with no version swap.
async fn authorize_object(
    envelope: &mut RequestEnvelope,
    (bucket, key, version_id): (&BucketName, &ObjectKey, Option<&str>),
    action: S3Action,
) -> S3Result<()> {
    Ok(authorize_object_request(envelope, bucket.as_str(), key.as_str(), version_id, Action::S3Action(action)).await?)
}

impl Served for dto::GetObjectTorrent {
    const METRIC: S3Operation = S3Operation::GetObjectTorrent;

    /// Every caller receives the same `NoSuchKey` answer.
    async fn authorize(_envelope: &mut RequestEnvelope, _request: &Req<Self>) -> S3Result<()> {
        Ok(())
    }

    async fn execute(_context: Option<Arc<AppContext>>, _envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        execute_get_object_torrent(request.into_input()).map(Resp::new)
    }
}

impl Served for dto::GetObjectAcl {
    const METRIC: S3Operation = S3Operation::GetObjectAcl;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let object = (&input.bucket, &input.key, input.version_id.as_deref());
        authorize_object(envelope, object, S3Action::GetObjectAclAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_get_object_acl(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}

impl Served for dto::PutObjectAcl {
    const METRIC: S3Operation = S3Operation::PutObjectAcl;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let object = (&input.bucket, &input.key, input.version_id.as_deref());
        authorize_object(envelope, object, S3Action::PutObjectAclAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_put_object_acl(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}

impl Served for dto::GetObjectLegalHold {
    const METRIC: S3Operation = S3Operation::GetObjectLegalHold;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let object = (&input.bucket, &input.key, input.version_id.as_deref());
        authorize_object(envelope, object, S3Action::GetObjectLegalHoldAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_get_object_legal_hold(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}

impl Served for dto::GetObjectRetention {
    const METRIC: S3Operation = S3Operation::GetObjectRetention;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let object = (&input.bucket, &input.key, input.version_id.as_deref());
        authorize_object(envelope, object, S3Action::GetObjectRetentionAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_get_object_retention(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}

impl Served for dto::GetObjectTagging {
    const METRIC: S3Operation = S3Operation::GetObjectTagging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let (bucket, key, version_id) = (input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref());
        Ok(authorize_object_tagging_request(envelope, bucket, key, version_id, S3Action::GetObjectTaggingAction).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_get_object_tagging(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}

impl Served for dto::DeleteObjectTagging {
    const METRIC: S3Operation = S3Operation::DeleteObjectTagging;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let (bucket, key, version_id) = (input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref());
        Ok(authorize_object_tagging_request(envelope, bucket, key, version_id, S3Action::DeleteObjectTaggingAction).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let usecase = ObjectMetaUsecase::with_context(context);
        usecase
            .execute_delete_object_tagging(envelope, request.into_input())
            .await
            .map(Resp::new)
    }
}
