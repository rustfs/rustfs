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
//! `HeadRequestBodyFixLayer`: an s3s compatibility patch, installed by
//! `server::http::external_service_stack!` and pinned there by `stack_census`.
//!
//! Patches: a refused `HEAD` carries the s3s error document as a body, which
//! h2 clients reject as a protocol error. Introduced in rustfs/rustfs#2648.
//! Replaced by: the same response invariants as `BodylessStatusFixLayer`, on
//! the refusal path too (gateway `docs/middleware.md`, "The nine RustFS tower
//! patch layers", row 2).
//! Removed by: T2.12 (rustfs/backlog#2771).

use crate::server::hybrid::HybridBody;
use bytes::Bytes;
use http::{Method, Request as HttpRequest, Response};
use http_body::Body;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};

/// Tower middleware that strips the actual response body for `HEAD` requests
/// while preserving metadata headers such as `Content-Length`.
///
/// The inner s3s layer may serialize S3 errors as XML bodies. That is valid for
/// regular requests, but for `HEAD` the HTTP layer must suppress the response
/// body entirely. If we forward the serialized error body over HTTP/2, clients
/// observe DATA frames on a `HEAD` response and fail the exchange with a
/// protocol error.
#[derive(Clone)]
pub struct HeadRequestBodyFixLayer;

impl<S> Layer<S> for HeadRequestBodyFixLayer {
    type Service = HeadRequestBodyFixService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HeadRequestBodyFixService { inner }
    }
}

#[derive(Clone)]
pub struct HeadRequestBodyFixService<S> {
    inner: S,
}

impl<S, ReqBody, RestBody, GrpcBody> Service<HttpRequest<ReqBody>> for HeadRequestBodyFixService<S>
where
    S: Service<HttpRequest<ReqBody>, Response = Response<HybridBody<RestBody, GrpcBody>>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    RestBody: Body<Data = Bytes> + From<Bytes> + Send + 'static,
    GrpcBody: Send + 'static,
{
    type Response = Response<HybridBody<RestBody, GrpcBody>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: HttpRequest<ReqBody>) -> Self::Future {
        let is_head = req.method() == Method::HEAD;
        let mut inner = self.inner.clone();

        Box::pin(async move {
            let response = inner.call(req).await?;
            if !is_head {
                return Ok(response);
            }

            let (mut parts, body) = response.into_parts();
            parts.headers.remove(http::header::TRANSFER_ENCODING);

            let response = match body {
                HybridBody::Rest { .. } => Response::from_parts(
                    parts,
                    HybridBody::Rest {
                        rest_body: RestBody::from(Bytes::new()),
                    },
                ),
                HybridBody::Grpc { grpc_body } => Response::from_parts(parts, HybridBody::Grpc { grpc_body }),
            };

            Ok(response)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;
    use http::StatusCode;
    use http_body_util::Empty;
    use http_body_util::{BodyExt, Full};
    use std::convert::Infallible;

    #[derive(Clone)]
    struct FixedResponse {
        status: StatusCode,
        body: Bytes,
        content_type: Option<&'static str>,
    }

    impl<B: Send + 'static> Service<Request<B>> for FixedResponse {
        type Response = Response<HybridBody<Full<Bytes>, Empty<Bytes>>>;
        type Error = Infallible;
        type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<B>) -> Self::Future {
            let this = self.clone();
            Box::pin(async move {
                let body = this.body.clone();
                let len = body.len();
                let mut builder = Response::builder().status(this.status);
                builder = builder.header(http::header::CONTENT_LENGTH, len.to_string());
                builder = builder.header(http::header::TRANSFER_ENCODING, "chunked");
                if let Some(ct) = this.content_type {
                    builder = builder.header(http::header::CONTENT_TYPE, ct);
                }
                Ok(builder
                    .body(HybridBody::Rest {
                        rest_body: Full::from(body),
                    })
                    .expect("build response"))
            })
        }
    }

    fn request_with_method(method: Method) -> Request<()> {
        Request::builder()
            .method(method)
            .uri("/bucket/object")
            .body(())
            .expect("request")
    }

    async fn collect_body<B: Body<Data = Bytes>>(body: B) -> Bytes
    where
        B::Error: std::fmt::Debug,
    {
        BodyExt::collect(body).await.expect("collect body").to_bytes()
    }

    #[tokio::test]
    async fn strips_body_for_head_errors_but_preserves_metadata_headers() {
        let payload = Bytes::from_static(b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>");
        let mut svc = HeadRequestBodyFixLayer.layer(FixedResponse {
            status: StatusCode::NOT_FOUND,
            body: payload.clone(),
            content_type: Some("application/xml"),
        });

        let res = svc.call(request_with_method(Method::HEAD)).await.expect("service call");
        let (parts, body) = res.into_parts();

        assert_eq!(parts.status, StatusCode::NOT_FOUND);
        assert_eq!(
            parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
            payload.len().to_string().as_str()
        );
        assert_eq!(parts.headers.get(http::header::CONTENT_TYPE).unwrap(), "application/xml");
        assert!(parts.headers.get(http::header::TRANSFER_ENCODING).is_none());

        let bytes = collect_body(body).await;
        assert!(bytes.is_empty(), "HEAD response body must be empty");
    }

    #[tokio::test]
    async fn preserves_body_for_get_errors() {
        let payload = Bytes::from_static(b"<?xml version=\"1.0\"?><Error><Code>NoSuchKey</Code></Error>");
        let mut svc = HeadRequestBodyFixLayer.layer(FixedResponse {
            status: StatusCode::NOT_FOUND,
            body: payload.clone(),
            content_type: Some("application/xml"),
        });

        let res = svc.call(request_with_method(Method::GET)).await.expect("service call");
        let (parts, body) = res.into_parts();

        assert_eq!(parts.status, StatusCode::NOT_FOUND);
        assert_eq!(
            parts.headers.get(http::header::CONTENT_LENGTH).unwrap(),
            payload.len().to_string().as_str()
        );
        assert_eq!(parts.headers.get(http::header::TRANSFER_ENCODING).unwrap(), "chunked");

        let bytes = collect_body(body).await;
        assert_eq!(bytes, payload);
    }
}
