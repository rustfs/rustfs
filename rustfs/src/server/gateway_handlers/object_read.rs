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

//! The gateway operations of migration batch C (rustfs/backlog#2765): GetObject, HeadObject and GetObjectAttributes.

use super::{OpRow, Served, resp, row};
use crate::app::object_usecase::{
    DefaultObjectUsecase, GetObjectAttributesFields, GetObjectAttributesOutputFacts, GetObjectBody, GetObjectOutputFacts,
    HeadObjectOutputFacts, ResponseEtag,
};
use crate::app::{Answered, RequestEnvelope};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_read::{
    authorize_get_object, authorize_get_object_attributes, authorize_head_object,
};
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3ErrorCode, S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[
    row::<dto::GetObject>(),
    row::<dto::HeadObject>(),
    row::<dto::GetObjectAttributes>(),
];

impl Served for dto::HeadObject {
    const METRIC: S3Operation = S3Operation::HeadObject;
    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let server_ctx = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is missing"))?;
        Ok(authorize_head_object(
            &server_ctx,
            envelope,
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id.as_deref(),
        )
        .await?)
    }
    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let result = DefaultObjectUsecase::with_context(context)
            .execute_head_object(envelope, request.into_input())
            .await;
        head_result(result)
    }
}

impl Served for dto::GetObject {
    const METRIC: S3Operation = S3Operation::GetObject;
    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        let server_ctx = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is missing"))?;
        Ok(authorize_get_object(
            &server_ctx,
            envelope,
            input.bucket.as_str(),
            input.key.as_str(),
            input.version_id.as_deref(),
        )
        .await?)
    }
    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        get_result(
            DefaultObjectUsecase::with_context(context)
                .execute_get_object(envelope, request.into_input())
                .await,
        )
    }
}

impl Served for dto::GetObjectAttributes {
    const METRIC: S3Operation = S3Operation::GetObjectAttributes;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        Ok(
            authorize_get_object_attributes(envelope, input.bucket.as_str(), input.key.as_str(), input.version_id.as_deref())
                .await?,
        )
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let input = request.into_input();
        let fields = GetObjectAttributesFields {
            bucket: input.bucket.as_str().to_owned(),
            key: input.key.as_str().to_owned(),
            version_id: input.version_id,
            object_attributes: input
                .object_attributes
                .into_iter()
                .map(|name| name.as_str().to_owned())
                .collect(),
            max_parts: input.max_parts,
            part_number_marker: input.part_number_marker,
            sse_customer_key: input.sse_customer_key.map(|key| key.expose_secret().to_owned()),
            sse_customer_key_md5: input.sse_customer_key_md5,
        };
        let Answered { output, status, headers } = DefaultObjectUsecase::with_context(context)
            .execute_get_object_attributes(envelope, fields)
            .await?;
        Ok(resp::<Self>(Answered {
            output: attributes_output(output)?,
            status,
            headers,
        }))
    }
}

fn attributes_output(output: GetObjectAttributesOutputFacts) -> S3Result<dto::GetObjectAttributesOutput> {
    let object_parts = output
        .object_parts
        .map(|parts| {
            let parts_output = parts
                .parts
                .into_iter()
                .map(|part| {
                    Ok(dto::ObjectPart {
                        part_number: part
                            .part_number
                            .ok_or_else(|| s3_error!(InternalError, "Stored part number is out of range"))?,
                        size: part.size,
                        checksum_crc32: part.checksums.crc32,
                        checksum_crc32c: part.checksums.crc32c,
                        checksum_crc64nvme: part.checksums.crc64nvme,
                        checksum_sha1: part.checksums.sha1,
                        checksum_sha256: part.checksums.sha256,
                        ..Default::default()
                    })
                })
                .collect::<S3Result<_>>()?;
            Ok::<_, rustfs_s3_types::S3Error>(dto::GetObjectAttributesParts {
                is_truncated: parts.is_truncated,
                max_parts: parts.max_parts,
                next_part_number_marker: parts.next_part_number_marker.map(|marker| marker.to_string()),
                part_number_marker: parts.part_number_marker.map(|marker| marker.to_string()),
                total_parts_count: parts.total_parts_count,
                parts: parts_output,
            })
        })
        .transpose()?;
    Ok(dto::GetObjectAttributesOutput {
        e_tag: output.e_tag.map(attributes_etag),
        last_modified: output.last_modified.map(crate::app::dto_values::timestamp),
        version_id: output.version_id,
        storage_class: output.storage_class.map(dto::StorageClass::custom),
        object_size: output.object_size,
        checksum: output.checksum.map(|checksum| dto::Checksum {
            checksum_crc32: checksum.crc32,
            checksum_crc32c: checksum.crc32c,
            checksum_crc64nvme: checksum.crc64nvme,
            checksum_sha1: checksum.sha1,
            checksum_sha256: checksum.sha256,
            checksum_type: checksum.checksum_type.map(dto::ChecksumType::custom),
            ..Default::default()
        }),
        object_parts,
        ..Default::default()
    })
}

fn attributes_etag(tag: ResponseEtag) -> rustfs_gateway_types::ETag {
    if tag.weak
        && let Ok(weak) = rustfs_gateway_types::ETag::new_weak(tag.opaque.as_str().to_owned())
        && weak.opaque_tag() == tag.opaque
    {
        return weak;
    }
    rustfs_gateway_types::ETag::from_stored(tag.opaque)
}

fn get_result(result: S3Result<Answered<GetObjectOutputFacts>>) -> S3Result<Resp<dto::GetObject>> {
    match result {
        Ok(answered) => get_response(answered),
        Err(error) if is_not_modified(&error) => Ok(Resp::with_status(dto::GetObjectOutput::default(), 304)
            .with_extra_headers(error.into_parts().headers.unwrap_or_default())),
        Err(error) => Err(error),
    }
}

fn is_not_modified(error: &rustfs_s3_types::S3Error) -> bool {
    error.code() == &S3ErrorCode::NotModified && error.status_code() == Some(http::StatusCode::NOT_MODIFIED)
}

pub(super) fn get_response(answered: Answered<GetObjectOutputFacts>) -> S3Result<Resp<dto::GetObject>> {
    let Answered {
        mut output,
        status,
        mut headers,
    } = answered;
    insert_etag(&mut headers, output.etag)
        .map_err(|_| s3_error!(InternalError, "We encountered an internal error. Please try again."))?;
    output.dto.body = output
        .body
        .map(|body| match body {
            GetObjectBody::Bytes(bytes) => Ok(rustfs_gateway_stream::ByteStream::from_bytes(bytes)),
            GetObjectBody::Stream(stream) => rustfs_gateway_stream::ByteStream::new(stream)
                .map_err(|_| s3_error!(InternalError, "Invalid GetObject body capabilities")),
        })
        .transpose()?;
    // Range encoding follows use-case completion; the legacy typed answer has no override.
    let status = status.or_else(|| output.dto.content_range.as_ref().map(|_| http::StatusCode::PARTIAL_CONTENT));
    Ok(resp::<dto::GetObject>(Answered {
        output: output.dto,
        status,
        headers,
    }))
}

fn insert_etag(headers: &mut http::HeaderMap, etag: Option<ResponseEtag>) -> S3Result<()> {
    if let Some(ResponseEtag { opaque, weak }) = etag {
        let value = if weak {
            format!("W/\"{}\"", opaque)
        } else {
            format!("\"{}\"", opaque)
        };
        headers.insert(
            http::header::ETAG,
            http::HeaderValue::from_str(&value).map_err(|_| s3_error!(InternalError, "Invalid stored ETag header"))?,
        );
    }
    Ok(())
}

/// The use case has already recorded this conditional refusal as an error. Preserve its
/// completed response without re-parsing the stored headers through the generic error codec.
fn head_result(result: S3Result<Answered<HeadObjectOutputFacts>>) -> S3Result<Resp<dto::HeadObject>> {
    match result {
        Ok(answered) => head_response(answered),
        Err(error) if is_not_modified(&error) => {
            let parts = error.into_parts();
            Ok(Resp::with_status(dto::HeadObjectOutput::default(), 304).with_extra_headers(parts.headers.unwrap_or_default()))
        }
        Err(error) => Err(error),
    }
}

/// Header encoding stays after use-case completion, just as on the legacy edge.
pub(super) fn head_response(answered: Answered<HeadObjectOutputFacts>) -> S3Result<Resp<dto::HeadObject>> {
    let Answered {
        output,
        status,
        mut headers,
    } = answered;
    insert_etag(&mut headers, output.etag)?;
    Ok(resp::<dto::HeadObject>(Answered {
        output: output.dto,
        status,
        headers,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_terminal_304_settles_without_a_body() {
        for status in [None, Some(http::StatusCode::NOT_MODIFIED)] {
            let mut error = rustfs_s3_types::S3Error::new(S3ErrorCode::NotModified);
            if let Some(status) = status {
                error.set_status_code(status);
            }
            let mut headers = http::HeaderMap::new();
            headers.insert(http::header::ETAG, http::HeaderValue::from_static("\"stored\""));
            error.set_headers(headers.clone());
            let (answer, status, actual) = get_result(Err(error)).unwrap().into_parts();
            let rustfs_gateway::Answer::Settled(output) = answer else {
                panic!("a conditional GET must settle before body handoff");
            };
            assert_eq!(status, 304);
            assert_eq!(actual, headers);
            assert!(output.body.is_none());
        }
    }

    #[test]
    fn get_terminal_304_requires_both_code_and_status() {
        for (code, status) in [
            (S3ErrorCode::NotModified, http::StatusCode::BAD_REQUEST),
            (S3ErrorCode::AccessDenied, http::StatusCode::NOT_MODIFIED),
            (S3ErrorCode::InternalError, http::StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let mut error = rustfs_s3_types::S3Error::new(code.clone());
            error.set_status_code(status);
            let Err(error) = get_result(Err(error)) else {
                panic!("unrelated refusal stays an error");
            };
            assert_eq!(error.code(), &code);
            assert_eq!(error.status_code(), Some(status));
        }
    }

    #[tokio::test]
    async fn get_response_handoff_retains_bytes_for_both_body_shapes() {
        use http_body_util::BodyExt as _;
        use rustfs_gateway_stream::ByteStream;
        for streaming in [false, true] {
            let bytes = bytes::Bytes::from(vec![b'x'; 8193]);
            let address = bytes.as_ptr();
            let body = if streaming {
                GetObjectBody::Stream(Box::pin(ByteStream::from_bytes(bytes.clone())))
            } else {
                GetObjectBody::Bytes(bytes.clone())
            };
            let response = get_response(Answered::new(GetObjectOutputFacts {
                dto: dto::GetObjectOutput {
                    content_length: Some(bytes.len() as i64),
                    ..Default::default()
                },
                body: Some(body),
                ..Default::default()
            }))
            .unwrap();
            let (output, status, _) = response.into_parts();
            assert_eq!(status, 200);
            let rustfs_gateway::Answer::Settled(output) = output else {
                panic!("GET must settle its response before body handoff");
            };
            let mut body = output.body.unwrap().into_body();
            let frame = body.frame().await.unwrap().unwrap().into_data().unwrap();
            assert_eq!(frame.as_ptr(), address);
            assert_eq!(frame, bytes);
            assert!(body.frame().await.is_none());
        }
    }

    #[test]
    fn get_range_status_changes_only_at_the_wire_edge() {
        for status in [None, Some(http::StatusCode::ACCEPTED)] {
            let mut answered = Answered::new(GetObjectOutputFacts {
                dto: dto::GetObjectOutput {
                    content_range: Some("bytes 1-3/7".to_owned()),
                    ..Default::default()
                },
                ..Default::default()
            });
            answered.status = status;
            let (_, actual, _) = get_response(answered).unwrap().into_parts();
            assert_eq!(actual, status.map_or(206, |status| status.as_u16()));
        }
    }

    #[test]
    fn head_terminal_304_uses_the_resolved_status() {
        for status in [None, Some(http::StatusCode::NOT_MODIFIED)] {
            let mut error = rustfs_s3_types::S3Error::new(S3ErrorCode::NotModified);
            if let Some(status) = status {
                error.set_status_code(status);
            }
            let mut headers = http::HeaderMap::new();
            headers.insert(http::header::ETAG, http::HeaderValue::from_bytes(b"\"\xc3\xa9\"").unwrap());
            error.set_headers(headers.clone());
            let (_, status, actual) = head_result(Err(error)).unwrap().into_parts();
            assert_eq!(status, 304);
            assert_eq!(actual, headers);
        }
    }

    #[test]
    fn head_terminal_304_requires_both_code_and_status() {
        for (code, status) in [
            (S3ErrorCode::NotModified, http::StatusCode::BAD_REQUEST),
            (S3ErrorCode::AccessDenied, http::StatusCode::NOT_MODIFIED),
            (S3ErrorCode::InternalError, http::StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            let mut error = rustfs_s3_types::S3Error::new(code.clone());
            error.set_status_code(status);
            let Err(error) = head_result(Err(error)) else {
                panic!("unrelated refusal stays an error");
            };
            assert_eq!(error.code(), &code);
            assert_eq!(error.status_code(), Some(status));
        }
    }
}
