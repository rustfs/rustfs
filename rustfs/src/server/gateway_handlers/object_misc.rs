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

use super::{OpRow, Served, resp, row};
use crate::app::RequestEnvelope;
use crate::app::dto_values::stored_object_key;
use crate::app::object_usecase::{DefaultObjectUsecase, DeleteBatchETag, DeleteBatchEntry, DeleteBatchInput, DeleteBatchOutput};
use crate::app::storage_api::object_usecase::bucket::{
    RestoreOutputLocation, RestoreRequest, RestoreS3Location, RestoreTag, RestoreTagging,
};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_misc::{
    authorize_delete_object, authorize_delete_objects, authorize_restore_object, authorize_select_object_content,
};
use http::StatusCode;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::DeleteObject>(),
    row::<dto::DeleteObjects>(),
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

impl Served for dto::DeleteObjects {
    const METRIC: S3Operation = S3Operation::DeleteObjects;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        Ok(authorize_delete_objects(envelope, request.input().bucket.as_str())?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = delete_batch_input(&request)?;
        let output = DefaultObjectUsecase::with_context(context)
            .execute_delete_objects(envelope, input)
            .await?;
        Ok(Resp::new(output.try_into()?))
    }
}

fn delete_batch_input(request: &Req<dto::DeleteObjects>) -> S3Result<DeleteBatchInput> {
    Ok(DeleteBatchInput {
        bucket: request.input().bucket.as_str().to_owned(),
        quiet: request.input().delete.quiet,
        objects: request
            .resources()
            .resolve_objects(request.read_proof())
            .ok_or_else(|| s3_error!(InternalError, "DeleteObjects resources could not be resolved"))?
            .map(Into::into)
            .collect(),
    })
}

impl From<&dto::ObjectIdentifier> for DeleteBatchEntry {
    fn from(object: &dto::ObjectIdentifier) -> Self {
        Self {
            key: object.key.as_str().to_owned(),
            key_too_long: object.key.rejection() == Some(dto::DeleteObjectKeyRejection::TooLong),
            version_id: object.version_id.clone(),
            e_tag: object.e_tag.as_ref().map(|condition| match condition.etag() {
                Some(tag) => DeleteBatchETag {
                    value: tag.opaque_tag().to_owned(),
                    weak: tag.is_weak(),
                },
                None => DeleteBatchETag {
                    value: String::new(),
                    weak: false,
                },
            }),
            _last_modified_time: object
                .last_modified_time
                .as_ref()
                .map(|value| (value.secs(), value.subsec_nanos())),
            _size: object.size,
        }
    }
}

impl TryFrom<DeleteBatchOutput> for dto::DeleteObjectsOutput {
    type Error = rustfs_s3_types::S3Error;

    fn try_from(output: DeleteBatchOutput) -> S3Result<Self> {
        // Keep the original decoded key on both successes and per-key errors.
        Ok(Self {
            deleted: output
                .deleted
                .unwrap_or_default()
                .into_iter()
                .map(|entry| {
                    Ok(dto::DeletedObject {
                        delete_marker: entry.delete_marker,
                        delete_marker_version_id: entry.delete_marker_version_id,
                        key: entry.key.as_deref().map(stored_object_key).transpose()?,
                        version_id: entry.version_id,
                    })
                })
                .collect::<S3Result<_>>()?,
            errors: output
                .errors
                .unwrap_or_default()
                .into_iter()
                .map(|entry| {
                    Ok(dto::Error {
                        code: entry.code,
                        key: entry
                            .key
                            .map(dto::DeleteKeyEcho::new)
                            .transpose()
                            .map_err(|error| s3_error!(InternalError, "DeleteObjects error key cannot be encoded: {error}"))?,
                        message: entry.message,
                        version_id: entry.version_id,
                    })
                })
                .collect::<S3Result<_>>()?,
            request_charged: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::RequestHead;
    use crate::server::gateway_handlers::test_support::{Signed, signed_request};
    use crate::storage_api::server::http::access::test_support::ReqInfo;
    use bytes::Bytes;
    use http::{Extensions, HeaderMap, Method, StatusCode};
    use http_body_util::{BodyExt as _, Full};
    use rustfs_gateway::{
        Credentials, Handler, HandlerResult, RegionSet, ServiceBuilder, SigV4Authenticator, StaticCredentials, allow_when,
    };

    struct BatchProbe;

    impl Handler<dto::DeleteObjects> for BatchProbe {
        async fn call(&self, request: Req<dto::DeleteObjects>) -> HandlerResult<dto::DeleteObjects> {
            let mut envelope = RequestEnvelope::from_parts(
                RequestHead {
                    method: Method::POST,
                    uri: "/fixture-bucket?delete".parse().unwrap(),
                    headers: HeaderMap::new(),
                    credentials: None,
                    region: None,
                    service: None,
                },
                Extensions::new(),
            );
            envelope.install_req_info(ReqInfo {
                bucket: Some("previous-bucket".to_owned()),
                object: Some("previous-key".to_owned()),
                version_id: Some("previous-version".to_owned()),
                ..ReqInfo::default()
            });

            <dto::DeleteObjects as Served>::authorize(&mut envelope, &request)
                .await
                .unwrap();
            let info = envelope.req_info().unwrap();
            assert_eq!(info.bucket.as_deref(), Some("fixture-bucket"));
            assert_eq!((info.object.as_deref(), info.version_id.as_deref()), (None, None));

            let input = delete_batch_input(&request).unwrap();
            assert_eq!(input.bucket, "fixture-bucket");
            assert_eq!(input.quiet, Some(true));
            let first = DeleteBatchEntry {
                key: "first".to_owned(),
                key_too_long: false,
                version_id: Some("null".to_owned()),
                e_tag: Some(DeleteBatchETag {
                    value: "weak-tag".to_owned(),
                    weak: true,
                }),
                _last_modified_time: Some((946_684_800, 0)),
                _size: Some(-19),
            };
            let second = DeleteBatchEntry {
                key: "second".to_owned(),
                e_tag: Some(DeleteBatchETag {
                    value: "*".to_owned(),
                    weak: false,
                }),
                _size: Some(0),
                ..Default::default()
            };
            assert_eq!(input.objects, [first.clone(), second, first]);
            Ok(Resp::new(dto::DeleteObjectsOutput::default()))
        }
    }

    #[tokio::test]
    async fn delete_objects_sets_the_batch_target_and_carries_frozen_entries() {
        let access = "batch-fixture";
        let secret = "fixture-delete-batch-secret";
        let credentials = StaticCredentials::new().with(Credentials::new(access, secret.as_bytes()).unwrap());
        let regions = RegionSet::new(["us-east-1"]).unwrap();
        let service = ServiceBuilder::new()
            .rustfs_profile()
            .authenticator(SigV4Authenticator::new(Arc::new(credentials), regions).rustfs_profile())
            .authorizer(allow_when(|_| true))
            .register::<dto::DeleteObjects, _>(Arc::new(BatchProbe))
            .build()
            .unwrap();

        let first = concat!(
            "<Object><Key>first</Key><VersionId>null</VersionId><ETag>W/&quot;weak-tag&quot;</ETag>",
            "<LastModifiedTime>Sat, 01 Jan 2000 00:00:00 GMT</LastModifiedTime><Size>-19</Size></Object>"
        );
        let second = "<Object><Key>second</Key><ETag>*</ETag><Size>0</Size></Object>";
        let body = format!("<Delete>{first}{second}{first}<Quiet>true</Quiet></Delete>");
        let request = signed_request(Signed::Header, Method::POST, "/fixture-bucket", "delete", access, secret)
            .map(|_| Full::new(Bytes::from(body)));
        let response = service.call(request).await;
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    }
}
