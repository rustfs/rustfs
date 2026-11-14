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

//! The gateway operations of migration batch B2 (rustfs/backlog#2766): object listings, multipart
//! listings, and multipart create and abort.

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::multipart_usecase::DefaultMultipartUsecase;
use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::storage_api::server::http::access::access_owned::listing_multipart::{
    authorize_bucket_listing, authorize_list_multipart_uploads, authorize_list_objects_v2,
};
use rustfs_gateway::{Req, Resp, dto};
use rustfs_policy::policy::action::S3Action;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::ListObjectsV2>(),
    row::<dto::ListObjects>(),
    row::<dto::ListObjectVersions>(),
    row::<dto::ListMultipartUploads>(),
];

impl Served for dto::ListObjectsV2 {
    const METRIC: S3Operation = S3Operation::ListObjectsV2;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server_ctx = server_context(envelope)?;
        authorize_list_objects_v2(&server_ctx, envelope, request.input().bucket.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_list_objects_v2(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::ListObjects {
    const METRIC: S3Operation = S3Operation::ListObjects;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        authorize_bucket_listing(envelope, request.input().bucket.as_str(), S3Action::ListBucketAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_list_objects(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::ListObjectVersions {
    const METRIC: S3Operation = S3Operation::ListObjectVersions;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        authorize_bucket_listing(envelope, request.input().bucket.as_str(), S3Action::ListBucketVersionsAction).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultBucketUsecase::with_context(context)
            .execute_list_object_versions(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::ListMultipartUploads {
    const METRIC: S3Operation = S3Operation::ListMultipartUploads;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server_ctx = server_context(envelope)?;
        authorize_list_multipart_uploads(&server_ctx, envelope, request.input().bucket.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_list_multipart_uploads(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

/// The server context a generation guard is read through; the gateway envelope always has one.
fn server_context(envelope: &RequestEnvelope) -> S3Result<Arc<ServerContextSlot>> {
    envelope
        .server_context()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "the gateway envelope carries no server context"))
}
