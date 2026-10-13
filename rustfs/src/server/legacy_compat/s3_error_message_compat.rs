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
//! `S3ErrorMessageCompatLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: s3s renders `SignatureDoesNotMatch` without the `<Message>`
//! element that MinIO and AWS clients (the GitLab registry among them) expect.
//! Introduced in rustfs/rustfs#2596.
//! Replaced by: the gateway dialect's error-render policy first, with
//! `StageFilter::on_response` as the per-message escape hatch (gateway
//! `docs/middleware.md`, "The nine RustFS tower patch layers", row 6).
//! Removed by: T2.12 (rustfs/backlog#2771).

use super::{StsQueryRequest, is_xml_response};
use crate::error::ApiError;
use crate::server::hybrid::HybridBody;
use crate::storage_api::server::legacy_compat::S3ErrorCode;
use bytes::Bytes;
use http::{Method, Request as HttpRequest, Response, StatusCode};
use http_body::Body;
use http_body_util::BodyExt;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

#[derive(Clone)]
pub struct S3ErrorMessageCompatLayer;

impl<S> Layer<S> for S3ErrorMessageCompatLayer {
    type Service = S3ErrorMessageCompatService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        S3ErrorMessageCompatService { inner }
    }
}

#[derive(Clone)]
pub struct S3ErrorMessageCompatService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for S3ErrorMessageCompatService<S>
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
        let is_sts_query =
            req.method() == Method::POST && req.uri().path() == "/" && req.extensions().get::<StsQueryRequest>().is_some();
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if is_sts_query || response.status() != StatusCode::FORBIDDEN || !is_xml_response(response.headers()) {
                return Ok(response);
            }

            let (parts, body) = response.into_parts();

            let response = match body {
                HybridBody::Rest { rest_body } => {
                    let (rest_body, changed) = fix_s3_error_message_in_xml(rest_body).await.map_err(Into::into)?;
                    let mut parts = parts;
                    if changed {
                        parts.headers.remove(http::header::CONTENT_LENGTH);
                    }
                    Response::from_parts(parts, HybridBody::Rest { rest_body })
                }
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

async fn fix_s3_error_message_in_xml<RestBody>(body: RestBody) -> Result<(RestBody, bool), RestBody::Error>
where
    RestBody: Body<Data = Bytes> + From<Bytes>,
{
    let bytes = BodyExt::collect(body).await?.to_bytes();
    let xml = String::from_utf8(bytes.to_vec()).unwrap_or_else(|_| String::from_utf8_lossy(&bytes).into_owned());
    let (fixed, changed) = insert_missing_signature_error_message(xml);
    Ok((RestBody::from(Bytes::from(fixed)), changed))
}

fn insert_missing_signature_error_message(mut xml: String) -> (String, bool) {
    if !xml.contains("<Code>SignatureDoesNotMatch</Code>") || xml.contains("<Message>") {
        return (xml, false);
    }

    let Some(code_end) = xml.find("</Code>") else {
        return (xml, false);
    };

    let message = ApiError::error_code_to_message(&S3ErrorCode::SignatureDoesNotMatch);
    xml.insert_str(code_end + "</Code>".len(), &format!("<Message>{message}</Message>"));
    (xml, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::legacy_compat::test_support::{FixedHybridResponse, collect_hybrid_response};
    use http::Request;
    use http_body_util::Full;

    #[tokio::test]
    async fn test_fix_s3_error_message_in_xml_reports_changed_body() {
        let body = Full::from(Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>"));

        let (fixed, changed) = fix_s3_error_message_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert!(changed);
        assert!(bytes.starts_with(b"<Error><Code>SignatureDoesNotMatch</Code><Message>"));
        assert!(bytes.ends_with(b"</Message></Error>"));
    }

    #[tokio::test]
    async fn test_fix_s3_error_message_in_xml_reports_unchanged_body() {
        let input = Bytes::from_static(b"<Error><Code>AccessDenied</Code></Error>");
        let body = Full::from(input.clone());

        let (fixed, changed) = fix_s3_error_message_in_xml(body).await.unwrap();
        let bytes = BodyExt::collect(fixed).await.unwrap().to_bytes();

        assert!(!changed);
        assert_eq!(bytes, input);
    }

    #[tokio::test]
    async fn s3_error_message_compat_fixes_regular_forbidden_xml() {
        let body = Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>");
        let mut service = S3ErrorMessageCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::FORBIDDEN,
            body,
            content_type: "application/xml",
        });
        let request = Request::builder()
            .method(Method::GET)
            .uri("/bucket/object")
            .body(())
            .expect("request");

        let response = service.call(request).await.expect("service response");
        let (status, headers, body) = collect_hybrid_response(response).await;

        assert_eq!(status, StatusCode::FORBIDDEN);
        assert!(headers.get(http::header::CONTENT_LENGTH).is_none());
        assert!(body.contains("<Message>"));
    }

    #[tokio::test]
    async fn s3_error_message_compat_leaves_sts_query_response_unchanged() {
        let input = Bytes::from_static(b"<Error><Code>SignatureDoesNotMatch</Code></Error>");
        let mut service = S3ErrorMessageCompatLayer.layer(FixedHybridResponse {
            status: StatusCode::FORBIDDEN,
            body: input.clone(),
            content_type: "application/xml",
        });
        let mut request = Request::builder().method(Method::POST).uri("/").body(()).expect("request");
        request.extensions_mut().insert(StsQueryRequest);

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

    #[test]
    fn test_insert_missing_signature_error_message() {
        let (fixed, changed) =
            insert_missing_signature_error_message("<Error><Code>SignatureDoesNotMatch</Code></Error>".to_string());

        assert!(changed);
        assert!(fixed.contains("<Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match the signature you provided."));
    }

    #[test]
    fn test_insert_missing_signature_error_message_preserves_existing_message() {
        let input = "<Error><Code>SignatureDoesNotMatch</Code><Message>custom</Message></Error>".to_string();
        let (fixed, changed) = insert_missing_signature_error_message(input.clone());

        assert!(!changed);
        assert_eq!(fixed, input);
    }
}
