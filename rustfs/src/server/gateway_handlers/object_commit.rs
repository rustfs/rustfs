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

//! Gateway registrations for object copy and multipart completion.

use super::object_write::{request_version_id, retain_until};
use super::{OpRow, Served, row};
use crate::app::multipart_usecase::{UploadPartCopyWriteInput, UploadPartTarget};
use crate::app::object_usecase::{CopyObjectWriteInput, CopyObjectWriteOutput, CopySourceCondition, DefaultObjectUsecase};
use crate::app::{RequestEnvelope, dto_values, multipart_usecase::DefaultMultipartUsecase};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_commit::{
    CopyObjectAuthorization, CopySourceObject, authorize_complete_multipart_upload, authorize_copy_object,
    authorize_upload_part_copy, check_copy_source_owner,
};
use rustfs_gateway::{CopySourceForm, Req, Resp, dto};
use rustfs_gateway_types::{CopySourceValue, ETag};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::CompleteMultipartUpload>(),
    row::<dto::CopyObject>(),
    row::<dto::UploadPartCopy>(),
];

impl Served for dto::CompleteMultipartUpload {
    const METRIC: S3Operation = S3Operation::CompleteMultipartUpload;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
        let input = request.input();
        let specific_etag = input.if_match.as_deref().is_some_and(|value| value.trim() != "*");
        authorize_complete_multipart_upload(envelope, &server, input.bucket.as_str(), input.key.as_str(), specific_etag).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_complete_multipart_upload(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}

impl Served for dto::CopyObject {
    const METRIC: S3Operation = S3Operation::CopyObject;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
        let input = request.input();
        let source = copy_source(request)?;
        let source = match &source {
            Some(CopySourceValue::Bucket { bucket, key, version_id }) => Some(CopySourceObject {
                bucket,
                key,
                version_id: version_id.as_deref(),
            }),
            _ => None,
        };
        let version_id = request_version_id(envelope);
        let facts = CopyObjectAuthorization {
            bucket: input.bucket.as_str(),
            key: input.key.as_str(),
            source,
            query_version: version_id.as_deref(),
            tagging: input.tagging.as_deref(),
            tagging_directive: input.tagging_directive.as_ref().map(|value| value.as_str()),
            retain_until: retain_until(input.object_lock_retain_until_date.as_ref())?,
            legal_hold: input.object_lock_legal_hold_status.is_some(),
            retention: input.object_lock_mode.is_some() || input.object_lock_retain_until_date.is_some(),
        };
        authorize_copy_object(envelope, &server, facts).await
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let source = copy_source(&request)?;
        let input = copy_input(request.into_input(), source, request_version_id(&envelope))?;
        let output = DefaultObjectUsecase::with_context(context)
            .execute_copy_object_owned(envelope, input)
            .await?;
        Ok(Resp::new(copy_output(output)))
    }
}

impl Served for dto::UploadPartCopy {
    const METRIC: S3Operation = S3Operation::UploadPartCopy;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
        let source = copy_source(request)?;
        let source = match &source {
            Some(CopySourceValue::Bucket { bucket, key, version_id }) => Some(CopySourceObject {
                bucket,
                key,
                version_id: version_id.as_deref(),
            }),
            _ => None,
        };
        let input = request.input();
        authorize_upload_part_copy(envelope, &server, input.bucket.as_str(), input.key.as_str(), source).await?;
        check_copy_source_owner(envelope.headers())
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let source = copy_source(&request)?.ok_or_else(|| s3_error!(NotImplemented))?;
        let input = request.into_input();
        let input = UploadPartCopyWriteInput {
            target: UploadPartTarget::Gateway {
                bucket: input.bucket,
                key: input.key,
                upload_id: input.upload_id,
            },
            part_number: input.part_number,
            copy_source: source,
            copy_source_range: input.copy_source_range,
            copy_source_if_match: copy_condition(input.copy_source_if_match.as_deref())?,
            copy_source_if_none_match: copy_condition(input.copy_source_if_none_match.as_deref())?,
            sse_customer_algorithm: input.sse_customer_algorithm,
            sse_customer_key: input.sse_customer_key.map(|key| key.expose_secret().to_owned()),
            sse_customer_key_md5: input.sse_customer_key_md5,
            copy_source_sse_customer_algorithm: input.copy_source_sse_customer_algorithm,
            copy_source_sse_customer_key: input.copy_source_sse_customer_key.map(|key| key.expose_secret().to_owned()),
            copy_source_sse_customer_key_md5: input.copy_source_sse_customer_key_md5,
        };
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_upload_part_copy_owned(envelope, input)
            .await?;
        let tag = output
            .e_tag
            .as_deref()
            .ok_or_else(|| s3_error!(InternalError, "copy part result is missing its ETag"))?;
        Ok(Resp::new(dto::UploadPartCopyOutput {
            e_tag: dto_values::stored_etag(tag),
            last_modified: output.last_modified.map(dto_values::timestamp),
            copy_source_version_id: output.copy_source_version_id,
            server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
            ssekms_key_id: output.ssekms_key_id,
            sse_customer_algorithm: output.sse_customer_algorithm,
            sse_customer_key_md5: output.sse_customer_key_md5,
            checksum_crc32: output.checksum_crc32,
            checksum_crc32c: output.checksum_crc32c,
            checksum_sha1: output.checksum_sha1,
            checksum_sha256: output.checksum_sha256,
            checksum_crc64nvme: output.checksum_crc64nvme,
            ..Default::default()
        }))
    }
}

fn copy_source<O: rustfs_gateway::Operation<DerivedResources = rustfs_gateway::CopySourceResources>>(
    request: &Req<O>,
) -> S3Result<Option<CopySourceValue>> {
    // The gateway clears the source in the DTO once it binds the authorized resources.
    let source = request
        .resources()
        .source()
        .resolve(request.read_proof())
        .ok_or_else(|| s3_error!(AccessDenied))?;
    if source.form() != CopySourceForm::Path {
        return Ok(None);
    }
    let bucket = source.bucket().ok_or_else(|| s3_error!(AccessDenied))?;
    Ok(Some(CopySourceValue::Bucket {
        bucket: bucket.as_str().to_owned(),
        key: source.key().as_str().to_owned(),
        version_id: source.version_id().map(str::to_owned),
    }))
}

fn copy_condition(value: Option<&str>) -> S3Result<Option<CopySourceCondition>> {
    value
        .map(|value| {
            let tag =
                ETag::parse_http_header(value).map_err(|_| s3_error!(InvalidArgument, "Invalid copy source ETag condition"))?;
            Ok(if tag.is_any() {
                CopySourceCondition::Any
            } else if tag.is_weak() {
                CopySourceCondition::Weak
            } else {
                CopySourceCondition::Strong(tag.opaque_tag().to_owned())
            })
        })
        .transpose()
}

fn copy_input(
    input: dto::CopyObjectInput,
    copy_source: Option<CopySourceValue>,
    version_id: Option<String>,
) -> S3Result<CopyObjectWriteInput> {
    Ok(CopyObjectWriteInput {
        copy_source,
        bucket: input.bucket.as_str().to_owned(),
        key: input.key.as_str().to_owned(),
        version_id,
        specific_if_none_match: input.if_none_match.as_deref().is_some_and(|value| value.trim() != "*"),
        server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
        ssekms_key_id: input.ssekms_key_id,
        sse_customer_algorithm: input.sse_customer_algorithm,
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: input.sse_customer_key_md5,
        copy_source_sse_customer_algorithm: input.copy_source_sse_customer_algorithm,
        copy_source_sse_customer_key: input
            .copy_source_sse_customer_key
            .map(|value| value.expose_secret().to_owned()),
        copy_source_sse_customer_key_md5: input.copy_source_sse_customer_key_md5,
        metadata_directive: input.metadata_directive.map(|value| value.as_str().to_owned()),
        metadata: Some(input.metadata.into_iter().collect()),
        tagging: input.tagging,
        tagging_directive: input.tagging_directive.map(|value| value.as_str().to_owned()),
        copy_source_if_match: copy_condition(input.copy_source_if_match.as_deref())?,
        copy_source_if_none_match: copy_condition(input.copy_source_if_none_match.as_deref())?,
        cache_control: input.cache_control,
        content_disposition: input.content_disposition,
        content_encoding: input.content_encoding,
        content_language: input.content_language,
        content_type: input.content_type,
        expires: input.expires.map(|value| value.as_str().to_owned()),
        website_redirect_location: input.website_redirect_location,
        object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
        object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
        object_lock_retain_until_date: retain_until(input.object_lock_retain_until_date.as_ref())?,
        storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
        checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
    })
}

fn copy_output(output: CopyObjectWriteOutput) -> dto::CopyObjectOutput {
    dto::CopyObjectOutput {
        e_tag: output.e_tag.as_deref().map(dto_values::stored_etag),
        last_modified: output.last_modified.map(dto_values::timestamp),
        copy_source_version_id: output.copy_source_version_id,
        version_id: output.version_id,
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        checksum_crc32: output.checksum_crc32,
        checksum_crc32c: output.checksum_crc32c,
        checksum_sha1: output.checksum_sha1,
        checksum_sha256: output.checksum_sha256,
        checksum_crc64nvme: output.checksum_crc64nvme,
        checksum_md5: output.checksum_md5,
        checksum_sha512: output.checksum_sha512,
        checksum_xxhash3: output.checksum_xxhash3,
        checksum_xxhash64: output.checksum_xxhash64,
        checksum_xxhash128: output.checksum_xxhash128,
        checksum_type: output.checksum_type,
        ..Default::default()
    }
}

#[cfg(test)]
mod source_owner_tests {
    use super::*;
    use crate::app::RequestHead;
    use http::{Extensions, HeaderMap, HeaderValue, Method, Uri};

    #[test]
    fn source_owner_header_accepts_only_the_reported_owner() {
        let owner = dto_values::owner().id.expect("reported owner");
        for (values, expected) in [
            (vec![], None),
            (vec![HeaderValue::from_static("")], None),
            (vec![HeaderValue::from_str(&owner).expect("owner header")], None),
            (vec![HeaderValue::from_static("different")], Some("AccessDenied")),
            (
                vec![
                    HeaderValue::from_str(&owner).expect("owner header"),
                    HeaderValue::from_static("different"),
                ],
                Some("InvalidRequest"),
            ),
            (vec![HeaderValue::from_bytes(&[0xff]).expect("opaque header")], Some("InvalidRequest")),
        ] {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append("x-amz-source-expected-bucket-owner", value);
            }
            let envelope = RequestEnvelope::from_parts(
                RequestHead {
                    method: Method::PUT,
                    uri: Uri::from_static("/bucket/key"),
                    headers,
                    credentials: None,
                    region: None,
                    service: None,
                },
                Extensions::new(),
            );
            let actual = check_copy_source_owner(envelope.headers());
            assert_eq!(actual.as_ref().err().map(|error| error.code().as_str()), expected);
        }
    }
}
