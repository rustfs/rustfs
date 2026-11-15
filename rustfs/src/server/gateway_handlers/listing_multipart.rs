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

use super::{
    OpRow, Served,
    object_write::{request_version_id, retain_until},
    row,
};
use crate::app::RequestEnvelope;
use crate::app::bucket_usecase::DefaultBucketUsecase;
use crate::app::multipart_usecase::{CreateMultipartUploadFields, DefaultMultipartUsecase};
use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::storage_api::server::http::access::access_owned::listing_multipart::{
    CreateMultipartAuthorization, authorize_bucket_listing, authorize_create_multipart_upload, authorize_list_multipart_uploads,
    authorize_list_objects_v2,
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
    row::<dto::CreateMultipartUpload>(),
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

impl Served for dto::CreateMultipartUpload {
    const METRIC: S3Operation = S3Operation::CreateMultipartUpload;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = server_context(envelope)?;
        let input = request.input();
        let version = request_version_id(envelope);
        let facts = CreateMultipartAuthorization {
            bucket: input.bucket.as_str(),
            key: input.key.as_str(),
            query_version: version.as_deref(),
            retain_until: retain_until(input.object_lock_retain_until_date.as_ref())?,
            legal_hold_present: input.object_lock_legal_hold_status.is_some(),
            retention_present: input.object_lock_mode.is_some() || input.object_lock_retain_until_date.is_some(),
        };
        authorize_create_multipart_upload(&server, envelope, facts).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let version = request_version_id(&envelope);
        let input = request.into_input();
        let fields = CreateMultipartUploadFields {
            bucket: input.bucket.as_str().to_owned(),
            key: input.key.as_str().to_owned(),
            metadata: Some(input.metadata.into_iter().collect()),
            tagging: input.tagging,
            storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
            server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
            sse_customer_key_md5: input.sse_customer_key_md5,
            ssekms_key_id: input.ssekms_key_id,
            object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
            object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
            object_lock_retain_until_date: retain_until(input.object_lock_retain_until_date.as_ref())?,
        };
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_create_multipart_upload(envelope, fields, version)
            .await?;
        Ok(Resp::new(dto::CreateMultipartUploadOutput {
            bucket: input.bucket,
            key: input.key,
            upload_id: output.upload_id,
            server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
            sse_customer_algorithm: output.sse_customer_algorithm,
            sse_customer_key_md5: output.sse_customer_key_md5,
            ssekms_key_id: output.ssekms_key_id,
            checksum_algorithm: output.checksum_algorithm.map(dto::ChecksumAlgorithm::custom),
            checksum_type: output.checksum_type.map(dto::ChecksumType::custom),
            ..Default::default()
        }))
    }
}

/// The server context a generation guard is read through; the gateway envelope always has one.
fn server_context(envelope: &RequestEnvelope) -> S3Result<Arc<ServerContextSlot>> {
    envelope
        .server_context()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "the gateway envelope carries no server context"))
}
