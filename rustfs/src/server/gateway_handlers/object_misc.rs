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

//! The gateway operations of migration batch E2 (rustfs/backlog#2765): DeleteObject, DeleteObjects, RestoreObject and SelectObjectContent.
//! DeleteObject, RestoreObject and SelectObjectContent are served here.

use super::{OpRow, Served, resp, row};
use crate::app::RequestEnvelope;
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::app::storage_api::object_usecase::bucket::{
    RestoreOutputLocation, RestoreRequest, RestoreS3Location, RestoreTag, RestoreTagging,
};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_misc::{
    authorize_delete_object, authorize_restore_object, authorize_select_object_content,
};
use http::StatusCode;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::DeleteObject>(),
    row::<dto::RestoreObject>(),
    row::<dto::SelectObjectContent>(),
];

impl Served for dto::DeleteObject {
    const METRIC: S3Operation = S3Operation::DeleteObject;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        // Cloned out first: the decision borrows the envelope mutably.
        let server_ctx = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is missing"))?;
        let version_id = input.version_id.as_deref();
        Ok(authorize_delete_object(&server_ctx, envelope, input.bucket.as_str(), input.key.as_str(), version_id).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultObjectUsecase::with_context(context)
            .execute_delete_object(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::RestoreObject {
    const METRIC: S3Operation = S3Operation::RestoreObject;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let server_ctx = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is missing"))?;
        Ok(authorize_restore_object(
            &server_ctx,
            envelope,
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id.as_deref(),
        )
        .await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let output = DefaultObjectUsecase::with_context(context)
            .execute_restore_object_owned(
                envelope,
                input.bucket.as_str().to_owned(),
                input.key.as_str().to_owned(),
                input.version_id,
                Some(restore_request(input.restore_request)),
            )
            .await?;
        // RustFS answers 200 for both a new restore and a renewal.
        Ok(Resp::with_status(output, StatusCode::OK.as_u16()))
    }
}

fn restore_request(request: dto::RestoreRequest) -> RestoreRequest {
    RestoreRequest {
        days: request.days,
        description: request.description,
        glacier_job_parameters: request.glacier_job_parameters,
        output_location: request.output_location.map(|output| RestoreOutputLocation {
            s3: output.s3.map(|location| RestoreS3Location {
                access_control_list: location.access_control_list,
                bucket_name: location.bucket_name.as_str().to_owned(),
                canned_acl: location.canned_acl,
                encryption: location.encryption,
                prefix: location.prefix,
                storage_class: location.storage_class,
                tagging: location.tagging.map(|tagging| RestoreTagging {
                    tag_set: tagging
                        .tag_set
                        .into_iter()
                        .map(|tag| RestoreTag {
                            key: tag.key,
                            value: tag.value,
                        })
                        .collect(),
                }),
                user_metadata: location.user_metadata,
            }),
        }),
        select_parameters: request.select_parameters,
        tier: request.tier,
        type_: request.r#type.map(|value| value.as_str().to_owned()),
    }
}

impl Served for dto::SelectObjectContent {
    const METRIC: S3Operation = S3Operation::SelectObjectContent;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        Ok(authorize_select_object_content(envelope, input.bucket.as_str(), input.key.as_str()).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = crate::app::select_request::select_input_from_gateway(request.into_input());
        let answered = DefaultObjectUsecase::with_context(context)
            .execute_select_object_content_owned(envelope, input)
            .await?;
        Ok(resp::<Self>(answered))
    }
}
