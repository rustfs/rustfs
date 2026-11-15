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
use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput, PutObjectWriteOutput};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_write::{PutObjectAuthorization, authorize_put_object};
use rustfs_gateway::{Req, Resp, dto};
use rustfs_gateway_types::{ChecksumAlgorithm, Timestamp};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[row::<dto::PutObject>()];

impl Served for dto::PutObject {
    const METRIC: S3Operation = S3Operation::PutObject;

    fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> impl std::future::Future<Output = S3Result<()>> + Send {
        let input = request.input();
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let retain_until = retain_until(input.object_lock_retain_until_date.as_ref());
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
        let mut input = native_input(request.into_input())?;
        input.version_id = request_version_id(&envelope);
        if let Some(declaration) = envelope.headers().get("x-amz-trailer").filter(|value| !value.is_empty()) {
            // The gateway verifies the declared trailer independently of the SDK algorithm.
            let algorithm = declaration
                .to_str()
                .ok()
                .and_then(|name| ChecksumAlgorithm::from_header_name(name.trim_matches([' ', '\t'])))
                .ok_or_else(|| s3_error!(InvalidRequest, "Invalid checksum trailer declaration"))?;
            input.checksum_algorithm = Some(algorithm.wire_name().to_owned());
            input.capture_frame_trailers();
        }
        let output = DefaultObjectUsecase::with_context(context)
            .execute_put_object_owned(envelope, input)
            .await?;
        native_output(output)
    }
}

// PutObject's destination version is a RustFS replication extension, absent from the AWS DTO.
fn request_version_id(envelope: &RequestEnvelope) -> Option<String> {
    url::form_urlencoded::parse(envelope.uri().query()?.as_bytes())
        .find(|(name, _)| name == "versionId")
        .map(|(_, value)| value.into_owned())
}

fn retain_until(value: Option<&Timestamp>) -> S3Result<Option<time::OffsetDateTime>> {
    value
        .map(|value| {
            time::OffsetDateTime::from_unix_timestamp_nanos(
                i128::from(value.secs()) * 1_000_000_000 + i128::from(value.subsec_nanos()),
            )
            .map_err(|_| s3_error!(InvalidArgument, "Invalid retain-until date"))
        })
        .transpose()
}

fn native_input(input: dto::PutObjectInput) -> S3Result<PutObjectWriteInput> {
    let checksum = |algorithm| {
        input
            .checksum_spec
            .as_ref()
            .filter(|value| value.algorithm() == algorithm)
            .map(|value| value.render_base64().to_owned())
    };
    Ok(PutObjectWriteInput {
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
        object_lock_retain_until_date: retain_until(input.object_lock_retain_until_date.as_ref())?,
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
    })
}

fn native_output(output: PutObjectWriteOutput) -> S3Result<Resp<dto::PutObject>> {
    let mut headers = output.headers;
    for (name, value) in [
        ("x-amz-checksum-crc32", output.checksum_crc32),
        ("x-amz-checksum-crc32c", output.checksum_crc32c),
        ("x-amz-checksum-sha1", output.checksum_sha1),
        ("x-amz-checksum-sha256", output.checksum_sha256),
        ("x-amz-checksum-crc64nvme", output.checksum_crc64nvme),
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

#[cfg(test)]
#[path = "object_write_test.rs"]
mod tests;
