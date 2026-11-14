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
//! HeadObject is served; the batch appends the remaining rows here.

use super::{OpRow, Served, resp, row};
use crate::app::object_usecase::{DefaultObjectUsecase, HeadObjectOutputFacts, ResponseEtag};
use crate::app::{Answered, RequestEnvelope};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_read::authorize_head_object;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3ErrorCode, S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[row::<dto::HeadObject>()];

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

/// The use case has already recorded this conditional refusal as an error. Preserve its
/// completed response without re-parsing the stored headers through the generic error codec.
fn head_result(result: S3Result<Answered<HeadObjectOutputFacts>>) -> S3Result<Resp<dto::HeadObject>> {
    match result {
        Ok(answered) => head_response(answered),
        Err(error)
            if error.code() == &S3ErrorCode::NotModified && error.status_code() == Some(http::StatusCode::NOT_MODIFIED) =>
        {
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
    if let Some(ResponseEtag { opaque, weak }) = output.etag {
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
