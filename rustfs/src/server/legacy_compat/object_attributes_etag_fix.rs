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
//! `ObjectAttributesEtagFixLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s renders the `GetObjectAttributes` `<ETag>` quoted, unlike every
//! other operation, so the layer parses the response XML to strip the quotes.
//! Introduced in rustfs/rustfs#2002.
//! Replaced by: the gateway quirk table (`q-mpu-attributes-etag-0036`) first,
//! with `OpLayer<GetObjectAttributes>` to override (gateway
//! `docs/middleware.md`, "Level 3: `OpLayer<O>`"; "The nine RustFS tower patch
//! layers", row 7).
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::is_xml_response;
use crate::server::hybrid::HybridBody;
use crate::server::{
    ADMIN_PREFIX, MINIO_ADMIN_PREFIX, MINIO_ADMIN_V3_PREFIX, RPC_PREFIX, RUSTFS_ADMIN_PREFIX, console_prefix, has_path_prefix,
    is_table_catalog_path,
};
use bytes::Bytes;
use http::{Method, Request as HttpRequest, Response};
use http_body::Body;
use http_body_util::BodyExt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct ObjectAttributesEtagFixLayer;

impl<S> Layer<S> for ObjectAttributesEtagFixLayer {
    type Service = ObjectAttributesEtagFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ObjectAttributesEtagFixService { inner }
    }
}

#[derive(Clone)]
pub struct ObjectAttributesEtagFixService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for ObjectAttributesEtagFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    RestBody::Error: Into<S::Error> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let is_target = is_object_attributes_request(&req);
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !is_target || !response.status().is_success() || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let rest_body = fix_object_attributes_etag_in_xml(rest_body).await.map_err(Into::into)?;

                    let mut parts = parts;
                    parts.headers.remove(http::header::CONTENT_LENGTH);

                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

async fn fix_object_attributes_etag_in_xml<RestBody>(body: RestBody) -> Result<RestBody, RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let xml = String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
    let fixed = strip_quotes_from_first_etag(xml);
    Ok(RestBody::from(Bytes::from(fixed)))
}

fn strip_quotes_from_first_etag(xml: String) -> String {
    let Some(start) = xml.find("<ETag>") else {
        return xml;
    };
    let value_start = start + "<ETag>".len();
    let value_rest = &xml[value_start..];
    let Some(end_offset) = value_rest.find("</ETag>") else {
        return xml;
    };
    let value_end = value_start + end_offset;
    let raw = &xml[value_start..value_end];

    let Some(trimmed) = raw.strip_prefix('"').and_then(|v| v.strip_suffix('"')) else {
        return xml;
    };

    let mut fixed = String::with_capacity(xml.len() - 2);
    fixed.push_str(&xml[..value_start]);
    fixed.push_str(trimmed);
    fixed.push_str(&xml[value_end..]);
    fixed
}

pub(in crate::server) fn is_object_attributes_request<B>(req: &HttpRequest<B>) -> bool {
    if req.method() != Method::GET {
        return false;
    }

    let path = req.uri().path();
    if has_path_prefix(path, ADMIN_PREFIX)
        || has_path_prefix(path, MINIO_ADMIN_PREFIX)
        || has_path_prefix(path, RUSTFS_ADMIN_PREFIX)
        || has_path_prefix(path, MINIO_ADMIN_V3_PREFIX)
        || is_table_catalog_path(path)
        || has_path_prefix(path, console_prefix())
        || has_path_prefix(path, RPC_PREFIX)
    {
        return false;
    }

    let has_object_attributes_query = req.uri().query().is_some_and(|query| {
        query.split('&').any(|part| {
            let (name, _value) = part.split_once('=').unwrap_or((part, ""));
            matches!(
                name.to_ascii_lowercase().as_str(),
                "attributes" | "object-attributes" | "x-amz-object-attributes"
            )
        })
    });
    let has_object_attributes_header = req
        .headers()
        .get(http::header::HeaderName::from_static("x-amz-object-attributes"))
        .is_some();

    has_object_attributes_query || has_object_attributes_header
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::legacy_compat::test_support::{FixedHybridResponse, collect_hybrid_response};
    use http::Request;
    use http::StatusCode;
    use http_body_util::Full;

    #[test]
    fn test_strip_quotes_from_first_etag_removes_quotes() {
        let input = String::from("<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input);

        assert_eq!(output, "<GetObjectAttributesOutput><ETag>abc</ETag></GetObjectAttributesOutput>");
    }

    #[test]
    fn test_strip_quotes_from_first_etag_keeps_non_quoted_value() {
        let input = String::from("<GetObjectAttributesOutput><ETag>abc</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input.clone());

        assert_eq!(output, input);
    }

    #[test]
    fn test_strip_quotes_from_first_etag_only_first_occurrence() {
        let input =
            String::from("<GetObjectAttributesOutput><ETag>\"first\"</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>");
        let output = strip_quotes_from_first_etag(input);

        assert_eq!(
            output,
            "<GetObjectAttributesOutput><ETag>first</ETag><ETag>\"second\"</ETag></GetObjectAttributesOutput>"
        );
    }

    #[tokio::test]
    async fn test_fix_object_attributes_etag_in_xml() {
        let body = Full::from(Bytes::from(
            "<GetObjectAttributesOutput><ETag>\"abc\"</ETag><Checksum>CRC32C</Checksum></GetObjectAttributesOutput>",
        ));
        let fixed = fix_object_attributes_etag_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert_eq!(
            bytes,
            Bytes::from_static(
                b"<GetObjectAttributesOutput><ETag>abc</ETag><Checksum>CRC32C</Checksum></GetObjectAttributesOutput>",
            ),
        );
    }

    #[tokio::test]
    async fn object_attributes_etag_fix_rewrites_target_response() {
        let mut service = ObjectAttributesEtagFixLayer.layer(FixedHybridResponse {
            status: StatusCode::OK,
            body: Bytes::from_static(b"<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>"),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object?attributes")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (_status, headers, body) = collect_hybrid_response(response).await;

        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("<ETag>abc</ETag>"));
    }

    #[tokio::test]
    async fn object_attributes_etag_fix_leaves_regular_get_unchanged() {
        let input = Bytes::from_static(b"<GetObjectAttributesOutput><ETag>\"abc\"</ETag></GetObjectAttributesOutput>");
        let mut service = ObjectAttributesEtagFixLayer.layer(FixedHybridResponse {
            status: StatusCode::OK,
            body: input.clone(),
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (_status, headers, body) = collect_hybrid_response(response).await;

        let expected_len = input.len().to_string();
        assert_eq!(
            headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok()),
            Some(expected_len.as_str())
        );
        assert_eq!(body.as_bytes(), input.as_ref());
    }
}
