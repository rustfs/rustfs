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
use crate::app::dto_values;
use crate::app::multipart_usecase::{
    AbortMultipartUploadTarget, CreateMultipartUploadFields, DefaultMultipartUsecase, ListPartsFields, ListPartsInfo,
    ListPartsTarget, reject_abort_capabilities,
};
use crate::app::storage_api::storage_class::effective_class;
use crate::runtime_sources::{AppContext, ServerContextSlot};
use crate::storage_api::server::http::access::access_owned::listing_multipart::{
    CreateMultipartAuthorization, authorize_abort_multipart_upload, authorize_bucket_listing, authorize_create_multipart_upload,
    authorize_list_multipart_uploads, authorize_list_objects_v2, authorize_list_parts,
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
    row::<dto::ListParts>(),
    row::<dto::CreateMultipartUpload>(),
    row::<dto::AbortMultipartUpload>(),
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

impl Served for dto::ListParts {
    const METRIC: S3Operation = S3Operation::ListParts;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server_ctx = server_context(envelope)?;
        let input = request.input();
        authorize_list_parts(&server_ctx, envelope, input.bucket.as_str(), input.key.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let fields = ListPartsFields {
            target: ListPartsTarget::Gateway {
                bucket: input.bucket,
                key: input.key,
                upload_id: input.upload_id,
            },
            part_number_marker: input.part_number_marker,
            max_parts: input.max_parts,
        };
        let page = DefaultMultipartUsecase::with_context(context)
            .execute_list_parts(envelope, fields)
            .await?;
        Ok(Resp::new(list_parts_output(page)?))
    }
}

fn list_parts_output(page: ListPartsInfo) -> S3Result<dto::ListPartsOutput> {
    let owner = dto_values::owner();
    let initiator = dto::Initiator {
        id: owner.id.clone(),
        display_name: owner.display_name.clone(),
    };
    let transformed_parts = rustfs_utils::http::contains_key_str(&page.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION)
        || page
            .user_defined
            .keys()
            .any(|key| rustfs_utils::http::is_object_encryption_marker(key));
    let parts = page
        .parts
        .into_iter()
        .map(|part| {
            let tag = part
                .etag
                .as_deref()
                .ok_or_else(|| s3_error!(InternalError, "a stored part is missing its ETag"))?;
            let part_number = i32::try_from(part.part_num)
                .map_err(|_| s3_error!(InternalError, "a stored part number is outside the ListParts response range"))?;
            let size = if part.actual_size > 0 || (transformed_parts && part.actual_size == 0) {
                part.actual_size
            } else {
                i64::try_from(part.size)
                    .map_err(|_| s3_error!(InternalError, "a stored part size is outside the ListParts response range"))?
            };
            Ok(dto::Part {
                e_tag: dto_values::stored_etag(tag),
                last_modified: part.last_mod.map(dto_values::timestamp),
                part_number,
                size,
                ..Default::default()
            })
        })
        .collect::<S3Result<_>>()?;
    Ok(dto::ListPartsOutput {
        bucket: dto_values::stored_bucket_name(&page.bucket)?,
        key: dto_values::stored_object_key(&page.object)?,
        upload_id: page.upload_id,
        parts,
        owner: Some(owner),
        initiator: Some(initiator),
        is_truncated: page.is_truncated,
        next_part_number_marker: page.next_part_number_marker.map(|marker| marker.to_string()),
        max_parts: i32::try_from(page.max_parts)
            .map_err(|_| s3_error!(InternalError, "the ListParts page size is outside the response range"))?,
        part_number_marker: Some(page.part_number_marker.to_string()),
        storage_class: Some(dto::StorageClass::custom(
            effective_class(Some(page.storage_class.as_str()), None).to_owned(),
        )),
        ..Default::default()
    })
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

impl Served for dto::AbortMultipartUpload {
    const METRIC: S3Operation = S3Operation::AbortMultipartUpload;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = server_context(envelope)?;
        let input = request.input();
        authorize_abort_multipart_upload(&server, envelope, input.bucket.as_str(), input.key.as_str()).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let target = reject_abort_capabilities(&envelope).map(|()| AbortMultipartUploadTarget::Gateway {
            bucket: input.bucket,
            key: input.key,
            upload_id: input.upload_id,
        });
        DefaultMultipartUsecase::with_context(context)
            .execute_abort_multipart_upload(envelope, target)
            .await?;
        Ok(Resp::new(dto::AbortMultipartUploadOutput::default()))
    }
}

/// The server context a generation guard is read through; the gateway envelope always has one.
fn server_context(envelope: &RequestEnvelope) -> S3Result<Arc<ServerContextSlot>> {
    envelope
        .server_context()
        .cloned()
        .ok_or_else(|| s3_error!(InternalError, "the gateway envelope carries no server context"))
}
