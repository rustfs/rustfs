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

//! PutObject gateway registration and native request/response projection.

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::multipart_usecase::{DefaultMultipartUsecase, UploadPartTarget, UploadPartWriteInput, UploadPartWriteOutput};
use crate::app::object::FrameTrailers;
use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput, PutObjectWriteOutput};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_write::{
    PutObjectAuthorization, authorize_put_object, authorize_upload_part,
};
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::{ChecksumAlgorithm, Timestamp};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[row::<dto::PutObject>(), row::<dto::UploadPart>()];

impl Served for dto::PutObject {
    const METRIC: S3Operation = S3Operation::PutObject;

    fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> impl std::future::Future<Output = S3Result<()>> + Send {
        let input = request.input();
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let retain_until = put_object_retain_until(request);
        let version_id = request_version_id(envelope);
        let specific_etag = input.if_match.as_deref().is_some_and(|value| value.trim() != "*");
        let legal_hold = input.object_lock_legal_hold_status.is_some();
        let retention = input.object_lock_mode.is_some() || input.object_lock_retain_until_date.is_some();
        async move {
            let server = envelope
                .server_context()
                .cloned()
                .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
            let facts = PutObjectAuthorization {
                bucket: &bucket,
                key: &key,
                version_id: version_id.as_deref(),
                retain_until: retain_until?,
                specific_etag,
                legal_hold,
                retention,
                post: false,
            };
            Ok(authorize_put_object(envelope, &server, facts, |_| Ok(())).await?)
        }
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let retain_until = put_object_retain_until(&request)?;
        let mut input = native_input(request.into_input(), retain_until);
        input.version_id = request_version_id(&envelope);
        if let Some(algorithm) = trailer_checksum_algorithm(&envelope)? {
            input.checksum_algorithm = Some(algorithm.wire_name().to_owned());
            input.capture_frame_trailers();
        }
        let output = DefaultObjectUsecase::with_context(context)
            .execute_put_object_owned(envelope, input)
            .await?;
        native_output(output)
    }
}

impl Served for dto::UploadPart {
    const METRIC: S3Operation = S3Operation::UploadPart;

    fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> impl std::future::Future<Output = S3Result<()>> + Send {
        let input = request.input();
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        async move {
            let server = envelope
                .server_context()
                .cloned()
                .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
            Ok(authorize_upload_part(envelope, &server, &bucket, &key).await?)
        }
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = native_part_input(request.into_input(), trailer_checksum_algorithm(&envelope)?);
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_upload_part_owned(envelope, input)
            .await?;
        native_part_output(output)
    }
}

fn trailer_checksum_algorithm(envelope: &RequestEnvelope) -> S3Result<Option<ChecksumAlgorithm>> {
    envelope
        .headers()
        .get("x-amz-trailer")
        .filter(|value| !value.is_empty())
        .map(|declaration| {
            // The gateway verifies the declared trailer independently of the SDK algorithm.
            declaration
                .to_str()
                .ok()
                .and_then(|name| ChecksumAlgorithm::from_header_name(name.trim_matches([' ', '\t'])))
                .ok_or_else(|| s3_error!(InvalidRequest, "Invalid checksum trailer declaration"))
        })
        .transpose()
}

fn native_part_input(input: dto::UploadPartInput, trailer_algorithm: Option<ChecksumAlgorithm>) -> UploadPartWriteInput {
    let checksum = |algorithm| {
        input
            .checksum_spec
            .as_ref()
            .filter(|value| value.algorithm() == algorithm)
            .map(|value| value.render_base64().to_owned())
    };
    let frame_trailers = trailer_algorithm.map(|_| Arc::new(FrameTrailers::default()));
    UploadPartWriteInput {
        checksum_crc32: checksum(ChecksumAlgorithm::Crc32),
        checksum_crc32c: checksum(ChecksumAlgorithm::Crc32c),
        checksum_crc64nvme: checksum(ChecksumAlgorithm::Crc64Nvme),
        checksum_sha1: checksum(ChecksumAlgorithm::Sha1),
        checksum_sha256: checksum(ChecksumAlgorithm::Sha256),
        target: UploadPartTarget::Gateway {
            bucket: input.bucket,
            key: input.key,
            upload_id: input.upload_id,
        },
        part_number: input.part_number,
        content_length: Some(input.content_length),
        body: input.body.map(|body| body.into_body()),
        trailing_headers: frame_trailers.clone().map(|source| source as rustfs_rio::SharedTrailerSource),
        frame_trailers,
        content_md5: input.content_md5,
        checksum_algorithm: trailer_algorithm
            .map(|value| value.wire_name().to_owned())
            .or_else(|| input.checksum_algorithm.map(|value| value.as_str().to_owned())),
        sse_customer_algorithm: input.sse_customer_algorithm,
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: input.sse_customer_key_md5,
    }
}

fn native_part_output(output: UploadPartWriteOutput) -> S3Result<Resp<dto::UploadPart>> {
    let mut headers = output.headers;
    insert_checksum_headers(
        &mut headers,
        [
            output.checksum_crc32,
            output.checksum_crc32c,
            output.checksum_sha1,
            output.checksum_sha256,
            output.checksum_crc64nvme,
        ],
    )?;
    let dto = dto::UploadPartOutput {
        e_tag: crate::app::dto_values::stored_etag(
            output
                .e_tag
                .as_deref()
                .ok_or_else(|| s3_error!(InternalError, "UploadPart completed without an ETag"))?,
        ),
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ssekms_key_id: output.ssekms_key_id,
        ..Default::default()
    };
    Ok(Resp::new(dto).with_extra_headers(headers))
}

// The destination version is a RustFS replication extension absent from the AWS DTOs.
pub(super) fn request_version_id(envelope: &RequestEnvelope) -> Option<String> {
    url::form_urlencoded::parse(envelope.uri().query()?.as_bytes())
        .find(|(name, _)| name == "versionId")
        .map(|(_, value)| value.into_owned())
}

fn put_object_retain_until(request: &Req<dto::PutObject>) -> S3Result<Option<time::OffsetDateTime>> {
    retain_until_with_offset(
        request.input().object_lock_retain_until_date.as_ref(),
        request.context().object_lock_retain_until_offset_seconds(),
    )
}

pub(super) fn retain_until(value: Option<&Timestamp>) -> S3Result<Option<time::OffsetDateTime>> {
    retain_until_with_offset(value, None)
}

pub(super) fn retain_until_with_offset(
    value: Option<&Timestamp>,
    offset_seconds: Option<i32>,
) -> S3Result<Option<time::OffsetDateTime>> {
    value
        .map(|value| {
            let invalid_date = || s3_error!(InvalidArgument, "Invalid retain-until date");
            let instant = time::OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(value.secs()) * 1_000_000_000 + i128::from(value.subsec_nanos()),
            )
            .map_err(|_| invalid_date())?;
            let Some(seconds) = offset_seconds else {
                return Ok(instant);
            };
            let offset = time::UtcOffset::from_whole_seconds(seconds).map_err(|_| invalid_date())?;
            instant.checked_to_offset(offset).ok_or_else(invalid_date)
        })
        .transpose()
}

fn native_input(input: dto::PutObjectInput, retain_until: Option<time::OffsetDateTime>) -> PutObjectWriteInput {
    let checksum = |algorithm| {
        input
            .checksum_spec
            .as_ref()
            .filter(|value| value.algorithm() == algorithm)
            .map(|value| value.render_base64().to_owned())
    };
    PutObjectWriteInput {
        checksum_crc32: checksum(ChecksumAlgorithm::Crc32),
        checksum_crc32c: checksum(ChecksumAlgorithm::Crc32c),
        checksum_sha1: checksum(ChecksumAlgorithm::Sha1),
        checksum_sha256: checksum(ChecksumAlgorithm::Sha256),
        checksum_crc64nvme: checksum(ChecksumAlgorithm::Crc64Nvme),
        bucket: input.bucket.as_str().to_owned(),
        key: input.key.as_str().to_owned(),
        body: input.body.map(|body| body.into_body()),
        trailing_headers: None,
        frame_trailers: None,
        content_length: Some(input.content_length),
        metadata: Some(input.metadata.into_iter().collect()),
        specific_if_none_match: input.if_none_match.as_deref().is_some_and(|value| value.trim() != "*"),
        object_lock_retain_until_date: retain_until,
        cache_control: input.cache_control,
        content_disposition: input.content_disposition,
        content_encoding: input.content_encoding,
        content_language: input.content_language,
        content_type: input.content_type,
        expires: input.expires.map(|value| value.as_str().to_owned()),
        tagging: input.tagging,
        version_id: None,
        server_side_encryption: input.server_side_encryption.map(|value| value.as_str().to_owned()),
        sse_customer_algorithm: input.sse_customer_algorithm,
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: input.sse_customer_key_md5,
        ssekms_key_id: input.ssekms_key_id,
        content_md5: input.content_md5,
        object_lock_legal_hold_status: input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
        object_lock_mode: input.object_lock_mode.map(|value| value.as_str().to_owned()),
        storage_class: input.storage_class.map(|value| value.as_str().to_owned()),
        website_redirect_location: input.website_redirect_location,
        checksum_algorithm: input.checksum_algorithm.map(|value| value.as_str().to_owned()),
    }
}

fn native_output(output: PutObjectWriteOutput) -> S3Result<Resp<dto::PutObject>> {
    let mut headers = output.headers;
    insert_checksum_headers(
        &mut headers,
        [
            output.checksum_crc32,
            output.checksum_crc32c,
            output.checksum_sha1,
            output.checksum_sha256,
            output.checksum_crc64nvme,
        ],
    )?;
    let dto = dto::PutObjectOutput {
        e_tag: crate::app::dto_values::stored_etag(
            output
                .e_tag
                .as_deref()
                .ok_or_else(|| s3_error!(InternalError, "PutObject completed without an ETag"))?,
        ),
        version_id: output.version_id,
        expiration: output.expiration.map(Into::into),
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ssekms_key_id: output.ssekms_key_id,
        ..Default::default()
    };
    Ok(match output.status {
        Some(status) => Resp::with_status(dto, status.as_u16()),
        None => Resp::new(dto),
    }
    .with_extra_headers(headers))
}

fn insert_checksum_headers(
    headers: &mut http::HeaderMap,
    [crc32, crc32c, sha1, sha256, crc64nvme]: [Option<String>; 5],
) -> S3Result<()> {
    for (name, value) in [
        ("x-amz-checksum-crc32", crc32),
        ("x-amz-checksum-crc32c", crc32c),
        ("x-amz-checksum-sha1", sha1),
        ("x-amz-checksum-sha256", sha256),
        ("x-amz-checksum-crc64nvme", crc64nvme),
    ] {
        if let Some(value) = value {
            headers.insert(
                name,
                value
                    .parse()
                    .map_err(|_| s3_error!(InternalError, "Invalid stored checksum"))?,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "object_write_test.rs"]
mod tests;
